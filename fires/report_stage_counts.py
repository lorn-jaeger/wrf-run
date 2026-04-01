#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, Tuple

import yaml

UNGRIB_PATTERNS = [
    "FILE:*",
    "PFILE:*",
    "GFS:*",
    "GFS_FNL:*",
    "GEFS_*:*",
    "HRRR_*:*",
    "HRRR_soil*",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Report how many budget CSV runs have completed each workflow stage."
        )
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
        "--wrfout-threshold",
        type=int,
        default=12,
        help="Minimum wrfout file count to treat WRF as complete (default: 12).",
    )
    parser.add_argument(
        "--fire-id",
        type=str,
        default=None,
        help="Filter to a single fire_id (for testing).",
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Filter to a single date in YYYY-MM-DD format (for testing).",
    )
    parser.add_argument(
        "--sim-start",
        type=str,
        default=None,
        help="Filter to a single cycle start in YYYYMMDD_HH format (for testing).",
    )
    parser.add_argument(
        "--config-file",
        type=Path,
        default=None,
        help="Override per-fire config with a specific YAML file (for testing).",
    )
    return parser.parse_args()


def compute_sim_start(date_str: str) -> str:
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    shifted = dt - timedelta(hours=6)
    return shifted.strftime("%Y%m%d_%H")


def _has_glob(path: Path, pattern: str) -> bool:
    if not path.exists():
        return False
    return any(path.glob(pattern))


def _has_any(path: Path, patterns: Iterable[str]) -> bool:
    if not path.exists():
        return False
    for pattern in patterns:
        if any(path.glob(pattern)):
            return True
    return False


def _count_glob(path: Path, pattern: str) -> int:
    if not path.exists():
        return 0
    return sum(1 for _ in path.glob(pattern))


def load_rows(csv_path: Path) -> list[dict[str, str]]:
    if not csv_path.exists():
        raise SystemExit(f"Runs file not found: {csv_path}")
    with csv_path.open("r", newline="") as handle:
        reader = csv.DictReader(handle)
        required = {"date", "fire_id"}
        if not reader.fieldnames or not required.issubset(reader.fieldnames):
            raise SystemExit(f"{csv_path} must include columns {sorted(required)}")
        return [row for row in reader]


def load_config(config_root: Path, fire_id: str) -> Dict[str, object]:
    cfg_path = (config_root / f"{fire_id}.yaml").resolve()
    if not cfg_path.exists():
        raise FileNotFoundError(f"Config not found for {fire_id}: {cfg_path}")
    with cfg_path.open("r") as handle:
        return yaml.safe_load(handle) or {}


def resolve_run_dirs(cfg: Dict[str, object], sim_start: str) -> Tuple[Path, Path, Path]:
    wps_run_dir_parent = Path(cfg["wps_run_dir"])
    wrf_run_dir_parent = Path(cfg["wrf_run_dir"])
    exp_name = cfg.get("exp_name")
    exp_wrf_only = bool(cfg.get("exp_wrf_only", False))

    if exp_name:
        if exp_wrf_only:
            wps_run_dir = wps_run_dir_parent / sim_start
        else:
            wps_run_dir = wps_run_dir_parent / sim_start / str(exp_name)
        wrf_run_dir = wrf_run_dir_parent / sim_start / str(exp_name)
    else:
        wps_run_dir = wps_run_dir_parent / sim_start
        wrf_run_dir = wrf_run_dir_parent / sim_start

    geogrid_dir = wps_run_dir_parent / "geogrid"
    return wps_run_dir, wrf_run_dir, geogrid_dir


def main() -> None:
    args = parse_args()
    rows = load_rows(args.runs_file)
    detailed = any([args.fire_id, args.date, args.sim_start])

    config_cache: Dict[str, Dict[str, object]] = {}
    geogrid_cache: Dict[str, bool] = {}

    status_counts: Counter[str] = Counter()
    inclusive_counts: Counter[str] = Counter()
    missing_configs = 0
    matched_rows = 0

    config_override: Dict[str, object] | None = None
    if args.config_file is not None:
        if not args.config_file.exists():
            raise SystemExit(f"Config file not found: {args.config_file}")
        with args.config_file.open("r") as handle:
            config_override = yaml.safe_load(handle) or {}

    for row in rows:
        fire_id = row["fire_id"]
        date_str = row["date"]
        sim_start = compute_sim_start(date_str)

        if args.fire_id and fire_id != args.fire_id:
            continue
        if args.date and date_str != args.date:
            continue
        if args.sim_start and sim_start != args.sim_start:
            continue

        matched_rows += 1

        if fire_id not in config_cache:
            if config_override is not None:
                config_cache[fire_id] = config_override
            else:
                try:
                    config_cache[fire_id] = load_config(args.config_root, fire_id)
                except FileNotFoundError:
                    missing_configs += 1
                    continue

        cfg = config_cache[fire_id]
        wps_run_dir, wrf_run_dir, geogrid_dir = resolve_run_dirs(cfg, sim_start)
        ungrib_dir = wps_run_dir / "ungrib"
        metgrid_dir = wps_run_dir / "metgrid"

        if fire_id not in geogrid_cache:
            geogrid_cache[fire_id] = _has_glob(geogrid_dir, "geo_em.d0*")

        geogrid_done = geogrid_cache[fire_id]
        ungrib_done = _has_any(ungrib_dir, UNGRIB_PATTERNS)
        metgrid_done = _has_glob(metgrid_dir, "met_em.d0*")
        real_done = _has_glob(wrf_run_dir, "wrfinput_d0*") and _has_glob(
            wrf_run_dir, "wrfbdy_d0*"
        )
        wrfout_count = _count_glob(wrf_run_dir, "wrfout_d0*")
        wrf_done = wrfout_count >= args.wrfout_threshold

        if geogrid_done:
            inclusive_counts["geogrid"] += 1
        if ungrib_done:
            inclusive_counts["ungrib"] += 1
        if metgrid_done:
            inclusive_counts["metgrid"] += 1
        if real_done:
            inclusive_counts["real"] += 1
        if wrf_done:
            inclusive_counts["wrf"] += 1

        if wrf_done:
            status = "wrf"
        elif real_done:
            status = "real"
        elif metgrid_done:
            status = "metgrid"
        elif ungrib_done:
            status = "ungrib"
        elif geogrid_done:
            status = "geogrid"
        else:
            status = "not_started"
        status_counts[status] += 1

        if detailed:
            print(f"{fire_id} {date_str} sim_start={sim_start}")
            print(f"  wps_run_dir: {wps_run_dir}")
            print(f"  wrf_run_dir: {wrf_run_dir}")
            print(
                "  stages: "
                f"geogrid={geogrid_done} "
                f"ungrib={ungrib_done} "
                f"metgrid={metgrid_done} "
                f"real={real_done} "
                f"wrf={wrf_done} (wrfout={wrfout_count})"
            )
            print(f"  latest_stage: {status}")

    total_runs = matched_rows if detailed else len(rows)
    if detailed and matched_rows == 0:
        print("No matching runs found.")
        return
    print(f"Total runs considered: {total_runs}")
    if missing_configs:
        print(f"Missing configs: {missing_configs}")

    print("Counts by latest completed stage:")
    for key in ("wrf", "real", "metgrid", "ungrib", "geogrid", "not_started"):
        print(f"  {key}: {status_counts.get(key, 0)}")

    print("Inclusive stage completion counts:")
    for key in ("geogrid", "ungrib", "metgrid", "real", "wrf"):
        print(f"  {key}: {inclusive_counts.get(key, 0)}")


if __name__ == "__main__":
    main()
