#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Tuple
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Precheck HRRR URLs for days in a day-index list (no downloads)."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--indices-file",
        type=Path,
        help="File with day indices (one per line).",
    )
    group.add_argument(
        "--all-days",
        action="store_true",
        help="Check all day indices present in the runs file.",
    )
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
        "--output-ok",
        type=Path,
        default=Path("runs_hrrr_ok_indices.txt"),
        help="Write day indices with all HRRR URLs present.",
    )
    parser.add_argument(
        "--output-bad",
        type=Path,
        default=Path("runs_hrrr_missing_indices.txt"),
        help="Write day indices with missing HRRR URLs.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=10,
        help="Seconds to wait per HEAD request (default: 10).",
    )
    parser.add_argument(
        "--max-missing-per-day",
        type=int,
        default=1,
        help="Stop checking a day after this many missing URLs (default: 1).",
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


def build_hrrr_urls(cfg: FireConfig, sim_start: str, int_h: int) -> List[Tuple[str, Path]]:
    if cfg.icbc_source.lower().startswith("google"):
        base = GC_BASE
    else:
        base = AWS_BASE

    urls: List[Tuple[str, Path]] = []
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
                urls.append(
                    (f"{host_dir}/{fname}", cfg.grib_dir / f"hrrr.{valid_date}" / "conus" / fname)
                )
            fname = f"hrrr.t{valid_hour}z.wrfprsf00.grib2"
            urls.append(
                (f"{host_dir}/{fname}", cfg.grib_dir / f"hrrr.{valid_date}" / "conus" / fname)
            )
            valid_dt += timedelta(hours=int_h)
    else:
        cycle_date = cycle_dt.strftime("%Y%m%d")
        cycle_hour = cycle_dt.strftime("%H")
        host_dir = f"{base}/hrrr.{cycle_date}/conus"
        lead = cfg.icbc_fc_dt
        while lead <= cfg.sim_hrs + cfg.icbc_fc_dt:
            lead_str = str(int(lead)).zfill(2)
            if cfg.hrrr_native:
                fname = f"hrrr.t{cycle_hour}z.wrfnatf{lead_str}.grib2"
                urls.append(
                    (f"{host_dir}/{fname}", cfg.grib_dir / f"hrrr.{cycle_date}" / "conus" / fname)
                )
            fname = f"hrrr.t{cycle_hour}z.wrfprsf{lead_str}.grib2"
            urls.append(
                (f"{host_dir}/{fname}", cfg.grib_dir / f"hrrr.{cycle_date}" / "conus" / fname)
            )
            lead += int_h
    return urls


def head_ok(url: str, timeout: int) -> bool:
    req = Request(url, method="HEAD")
    try:
        with urlopen(req, timeout=timeout) as resp:
            return resp.status < 400
    except (HTTPError, URLError):
        return False


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

    ok_indices: List[int] = []
    bad_indices: List[int] = []

    for day_index in indices:
        if day_index < 1 or day_index > len(grouped):
            print(f"[WARN] day-index {day_index} out of range; skipping")
            continue
        date_str, day_rows = grouped[day_index - 1]
        sim_start = compute_sim_start(date_str)

        missing = 0
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

            for url, dest in build_hrrr_urls(cfg, sim_start, int_h):
                if url not in url_cache:
                    url_cache[url] = head_ok(url, args.timeout)
                if not url_cache[url]:
                    missing += 1
                    if missing >= args.max_missing_per_day:
                        break
            if missing >= args.max_missing_per_day:
                break

        if missing:
            bad_indices.append(day_index)
            print(f"[MISSING] {date_str} (day-index {day_index}) missing={missing}")
        else:
            ok_indices.append(day_index)
            print(f"[OK] {date_str} (day-index {day_index})")

    args.output_ok.write_text("\\n".join(str(i) for i in ok_indices) + "\\n")
    args.output_bad.write_text("\\n".join(str(i) for i in bad_indices) + "\\n")
    print(f"Wrote OK indices: {args.output_ok} ({len(ok_indices)})")
    print(f"Wrote BAD indices: {args.output_bad} ({len(bad_indices)})")


if __name__ == "__main__":
    main()
