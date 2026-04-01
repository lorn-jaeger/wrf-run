#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
import shutil
import subprocess
import sys
import tempfile
import concurrent.futures

import yaml


AWS_BASE = "https://noaa-hrrr-bdp-pds.s3.amazonaws.com"
GC_BASE = "https://storage.googleapis.com/high-resolution-rapid-refresh"


@dataclass
class FireConfig:
    sim_hrs: int
    icbc_model: str
    icbc_source: str
    icbc_analysis: bool
    hrrr_native: bool
    icbc_fc_dt: int
    template_dir: Path
    exp_name: str | None
    exp_wrf_only: bool
    grib_dir: Path


@dataclass
class HrrrTarget:
    url: str
    dest: Path
    valid_time: datetime
    kind: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check HRRR URLs for all runs; interpolate missing files using neighbor hours."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--indices-file", type=Path, help="File with day indices (one per line).")
    group.add_argument("--all-days", action="store_true", help="Process all day indices in runs file.")
    parser.add_argument(
        "--runs-file",
        type=Path,
        default=Path("fires/output_budget.csv"),
        help="CSV listing per-day runs (default: fires/output_budget.csv).",
    )
    parser.add_argument(
        "--config-root",
        type=Path,
        default=Path("configs/built/workflow"),
        help="Directory containing per-fire workflow YAML files.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=10,
        help="Seconds to wait per HEAD request (default: 10).",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of worker threads for interpolation (default: 4).",
    )
    parser.add_argument(
        "--use-existing-neighbors",
        action="store_true",
        help="Use existing neighbor files on disk if present; otherwise download.",
    )
    parser.add_argument(
        "--force-download-neighbors",
        action="store_true",
        help="Always download neighbor files even if they exist locally.",
    )
    parser.add_argument(
        "--delete-existing-neighbors",
        action="store_true",
        help="Delete neighbor files even if they existed before this script ran.",
    )
    parser.add_argument(
        "--tmp-dir",
        type=Path,
        default=Path("/glade/derecho/scratch/ljaeger/tmp/hrrr_interp"),
        help="Temporary directory for neighbor downloads.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing interpolated output files.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only report missing URLs; do not download or interpolate.",
    )
    return parser.parse_args()


def compute_sim_start(date_str: str) -> str:
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    shifted = dt - timedelta(hours=6)
    return shifted.strftime("%Y%m%d_%H")


def load_rows(csv_path: Path) -> List[Dict[str, str]]:
    if not csv_path.exists():
        raise SystemExit(f"Runs file not found: {csv_path}")
    with csv_path.open("r", newline="") as handle:
        reader = csv.DictReader(handle)
        required = {"date", "fire_id"}
        if not reader.fieldnames or not required.issubset(reader.fieldnames):
            raise SystemExit(f"{csv_path} must include columns {sorted(required)}")
        return [row for row in reader]


def group_rows_by_date(rows: Iterable[Dict[str, str]]) -> List[Tuple[str, List[Dict[str, str]]]]:
    order: List[str] = []
    grouped: Dict[str, List[Dict[str, str]]] = {}
    for row in rows:
        date = row["date"]
        if date not in grouped:
            order.append(date)
            grouped[date] = []
        grouped[date].append(row)
    return [(date, grouped[date]) for date in order]


def load_config(config_root: Path, fire_id: str) -> FireConfig:
    cfg_path = (config_root / f"{fire_id}.yaml").resolve()
    if not cfg_path.exists():
        raise FileNotFoundError(f"Config not found for {fire_id}: {cfg_path}")
    with cfg_path.open("r") as handle:
        cfg = yaml.safe_load(handle) or {}
    return FireConfig(
        sim_hrs=int(cfg.get("sim_hrs", 24)),
        icbc_model=str(cfg.get("icbc_model", "HRRR")),
        icbc_source=str(cfg.get("icbc_source", "AWS")),
        icbc_analysis=bool(cfg.get("icbc_analysis", False)),
        hrrr_native=bool(cfg.get("hrrr_native", True)),
        icbc_fc_dt=int(cfg.get("icbc_fc_dt", 0)),
        template_dir=Path(cfg.get("template_dir", ".")),
        exp_name=cfg.get("exp_name"),
        exp_wrf_only=bool(cfg.get("exp_wrf_only", False)),
        grib_dir=Path(cfg.get("grib_dir", ".")),
    )


def detect_interval_hours(cfg: FireConfig) -> int:
    icbc_model = cfg.icbc_model.lower()
    wps_nml_tmp = f"namelist.wps.{icbc_model}"
    if cfg.exp_name and not cfg.exp_wrf_only:
        wps_nml_tmp = f"{wps_nml_tmp}.{cfg.exp_name}"
    nml_path = cfg.template_dir / wps_nml_tmp
    if not nml_path.exists():
        raise FileNotFoundError(f"Missing namelist.wps template: {nml_path}")
    int_sec = None
    with nml_path.open() as handle:
        for line in handle:
            if line.strip().startswith("interval_seconds"):
                int_sec = int(line.split("=", 1)[1].strip().split(",")[0])
                break
    if int_sec is None:
        raise ValueError(f"interval_seconds not found in {nml_path}")
    return int_sec // 3600


def build_hrrr_targets(cfg: FireConfig, sim_start: str, int_h: int) -> List[HrrrTarget]:
    base = GC_BASE if cfg.icbc_source.lower().startswith("google") else AWS_BASE
    targets: List[HrrrTarget] = []
    cycle_dt = datetime.strptime(sim_start, "%Y%m%d_%H")

    if cfg.icbc_analysis:
        valid_dt = cycle_dt
        end_dt = cycle_dt + timedelta(hours=cfg.sim_hrs)
        while valid_dt <= end_dt:
            valid_date = valid_dt.strftime("%Y%m%d")
            valid_hour = valid_dt.strftime("%H")
            host_dir = f"{base}/hrrr.{valid_date}/conus"
            if cfg.hrrr_native:
                fname = f"hrrr.t{valid_hour}z.wrfnatf00.grib2"
                targets.append(
                    HrrrTarget(
                        f"{host_dir}/{fname}",
                        cfg.grib_dir / f"hrrr.{valid_date}" / "conus" / fname,
                        valid_dt,
                        "wrfnatf00",
                    )
                )
            fname = f"hrrr.t{valid_hour}z.wrfprsf00.grib2"
            targets.append(
                HrrrTarget(
                    f"{host_dir}/{fname}",
                    cfg.grib_dir / f"hrrr.{valid_date}" / "conus" / fname,
                    valid_dt,
                    "wrfprsf00",
                )
            )
            valid_dt += timedelta(hours=int_h)
    else:
        cycle_date = cycle_dt.strftime("%Y%m%d")
        cycle_hour = cycle_dt.strftime("%H")
        host_dir = f"{base}/hrrr.{cycle_date}/conus"
        lead = cfg.icbc_fc_dt
        while lead <= cfg.sim_hrs + cfg.icbc_fc_dt:
            lead_str = str(int(lead)).zfill(2)
            valid_dt = cycle_dt + timedelta(hours=lead)
            if cfg.hrrr_native:
                fname = f"hrrr.t{cycle_hour}z.wrfnatf{lead_str}.grib2"
                targets.append(
                    HrrrTarget(
                        f"{host_dir}/{fname}",
                        cfg.grib_dir / f"hrrr.{cycle_date}" / "conus" / fname,
                        valid_dt,
                        f"wrfnatf{lead_str}",
                    )
                )
            fname = f"hrrr.t{cycle_hour}z.wrfprsf{lead_str}.grib2"
            targets.append(
                HrrrTarget(
                    f"{host_dir}/{fname}",
                    cfg.grib_dir / f"hrrr.{cycle_date}" / "conus" / fname,
                    valid_dt,
                    f"wrfprsf{lead_str}",
                )
            )
            lead += int_h
    return targets


def head_ok(url: str, timeout: int) -> bool:
    req = Request(url, method="HEAD")
    try:
        with urlopen(req, timeout=timeout) as resp:
            return resp.status < 400
    except (HTTPError, URLError):
        return False


def download_url(url: str, dest: Path, timeout: int) -> bool:
    dest.parent.mkdir(parents=True, exist_ok=True)
    try:
        with urlopen(url, timeout=timeout) as resp, dest.open("wb") as out_f:
            shutil.copyfileobj(resp, out_f)
        return True
    except (HTTPError, URLError, OSError):
        return False


def protect_file(path: Path) -> None:
    try:
        result = subprocess.run(["chattr", "+i", str(path)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if result.returncode == 0:
            return
    except OSError:
        pass
    try:
        path.chmod(0o444)
    except OSError:
        print(f"[WARN] Unable to protect {path}", file=sys.stderr)


def interpolate_with_script(prev_path: Path, next_path: Path, out_path: Path, valid_time: datetime, overwrite: bool) -> bool:
    interp_script = Path("scripts/interp_hrrr_manual.py")
    if not interp_script.exists():
        print(f"[ERROR] Missing interpolation script: {interp_script}", file=sys.stderr)
        return False
    cmd = [
        sys.executable,
        str(interp_script),
        "--prev", str(prev_path),
        "--next", str(next_path),
        "--out", str(out_path),
        "--valid-time", valid_time.strftime("%Y%m%d%H"),
    ]
    if overwrite:
        cmd.append("--overwrite")
    result = subprocess.run(cmd)
    return result.returncode == 0


def handle_missing_target(
    target: HrrrTarget,
    prev_target: HrrrTarget,
    next_target: HrrrTarget,
    args: argparse.Namespace,
) -> bool:
    task_dir = args.tmp_dir / f"{target.dest.stem}_tmp"
    task_dir.mkdir(parents=True, exist_ok=True)
    prev_path = prev_target.dest
    next_path = next_target.dest
    prev_downloaded = False
    next_downloaded = False

    if args.force_download_neighbors or not args.use_existing_neighbors or not prev_path.exists():
        prev_tmp = task_dir / prev_target.dest.name
        if download_url(prev_target.url, prev_tmp, args.timeout):
            prev_path = prev_tmp
            prev_downloaded = True
        else:
            print(f"[WARN] Unable to download neighbor {prev_target.url}", file=sys.stderr)
            return False

    if args.force_download_neighbors or not args.use_existing_neighbors or not next_path.exists():
        next_tmp = task_dir / next_target.dest.name
        if download_url(next_target.url, next_tmp, args.timeout):
            next_path = next_tmp
            next_downloaded = True
        else:
            print(f"[WARN] Unable to download neighbor {next_target.url}", file=sys.stderr)
            return False

    if target.dest.exists() and not args.overwrite:
        print(f"[SKIP] {target.dest} exists (use --overwrite)")
        return True

    ok = interpolate_with_script(prev_path, next_path, target.dest, target.valid_time, args.overwrite)
    if not ok:
        print(f"[ERROR] Interpolation failed for {target.dest}", file=sys.stderr)
        return False

    protect_file(target.dest)

    if prev_downloaded or args.delete_existing_neighbors:
        try:
            prev_path.unlink()
        except OSError:
            pass
    if next_downloaded or args.delete_existing_neighbors:
        try:
            next_path.unlink()
        except OSError:
            pass
    try:
        if task_dir.exists():
            shutil.rmtree(task_dir, ignore_errors=True)
    except OSError:
        pass
    return True


def main() -> None:
    args = parse_args()
    rows = load_rows(args.runs_file)
    grouped = group_rows_by_date(rows)
    if not grouped:
        raise SystemExit("No runs found in CSV.")

    if args.all_days:
        indices = list(range(1, len(grouped) + 1))
    else:
        indices = [int(x) for x in args.indices_file.read_text().split() if x.strip()]

    config_cache: Dict[str, FireConfig] = {}
    interval_cache: Dict[str, int] = {}
    url_cache: Dict[str, bool] = {}

    args.tmp_dir.mkdir(parents=True, exist_ok=True)

    tasks: List[Tuple[HrrrTarget, HrrrTarget, HrrrTarget]] = []
    seen_targets: set[Path] = set()

    for day_index in indices:
        if day_index < 1 or day_index > len(grouped):
            print(f"[WARN] day-index {day_index} out of range; skipping")
            continue
        date_str, day_rows = grouped[day_index - 1]
        sim_start = compute_sim_start(date_str)
        print(f"[DAY] {date_str} (day-index {day_index})")

        for row in day_rows:
            fire_id = row["fire_id"]
            if fire_id not in config_cache:
                config_cache[fire_id] = load_config(args.config_root, fire_id)
            cfg = config_cache[fire_id]
            if cfg.icbc_model.lower() != "hrrr":
                continue
            if fire_id not in interval_cache:
                interval_cache[fire_id] = detect_interval_hours(cfg)
            int_h = interval_cache[fire_id]

            targets = build_hrrr_targets(cfg, sim_start, int_h)
            kind_groups: Dict[str, List[HrrrTarget]] = {}
            for t in targets:
                kind_groups.setdefault(t.kind, []).append(t)

            for kind, group in kind_groups.items():
                for idx, target in enumerate(group):
                    if target.url not in url_cache:
                        url_cache[target.url] = head_ok(target.url, args.timeout)
                    if url_cache[target.url]:
                        continue

                    prev_target = group[idx - 1] if idx - 1 >= 0 else None
                    next_target = group[idx + 1] if idx + 1 < len(group) else None
                    if not prev_target or not next_target:
                        print(f"[MISS] {target.url} (no neighbors)")
                        continue

                    if target.dest in seen_targets:
                        continue
                    seen_targets.add(target.dest)

                    print(f"[MISS] {target.url} -> interpolate from {prev_target.dest.name} & {next_target.dest.name}")
                    if args.dry_run:
                        continue
                    tasks.append((target, prev_target, next_target))

    if args.dry_run:
        return

    if not tasks:
        print("[INFO] No missing targets to interpolate.")
        return

    workers = max(1, args.workers)
    print(f"[INFO] Starting interpolation with {workers} worker(s) on {len(tasks)} file(s).")
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(handle_missing_target, t, p, n, args) for t, p, n in tasks]
        for future in concurrent.futures.as_completed(futures):
            _ = future.result()


if __name__ == "__main__":
    main()
