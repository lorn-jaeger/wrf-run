#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import yaml


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Clean up HRRR/WPS outputs for a given day once runs are complete."
    )
    parser.add_argument(
        "--date",
        type=str,
        help="Target date in YYYY-MM-DD format (matches CSV date column).",
    )
    parser.add_argument(
        "--day-index",
        type=int,
        help="1-based index of the day in the runs CSV (same order as run_budget_day.py).",
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
        "--delete-hrrr",
        action="store_true",
        help="Delete HRRR grib directories for the day window.",
    )
    parser.add_argument(
        "--delete-wrfrst",
        action="store_true",
        help="Delete WRF restart files (wrfrst_d0*).",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Perform deletions (default is dry-run).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Require all runs to be complete before cleaning (override the default).",
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


def count_wrfouts(run_dir: Path) -> int:
    if not run_dir.exists():
        return 0
    return sum(1 for _ in run_dir.glob("wrfout_d0*"))


def remove_path(path: Path, execute: bool) -> None:
    if not path.exists() and not path.is_symlink():
        return
    if execute:
        if path.is_symlink() or path.is_file():
            path.unlink()
        else:
            for child in path.glob("*"):
                remove_path(child, execute=True)
            path.rmdir()
    else:
        print(f"[DRY-RUN] remove {path}")


def main() -> None:
    args = parse_args()
    if not args.date and not args.day_index:
        raise SystemExit("Specify --date or --day-index.")

    rows = load_rows(args.runs_file)
    grouped = group_rows_by_date(rows)
    if not grouped:
        raise SystemExit("No runs found in CSV.")

    if args.day_index:
        if args.day_index < 1 or args.day_index > len(grouped):
            raise SystemExit(
                f"--day-index must be between 1 and {len(grouped)} (got {args.day_index})."
            )
        date_str, day_rows = grouped[args.day_index - 1]
    else:
        date_str = args.date
        day_rows = [row for row in rows if row["date"] == date_str]
        if not day_rows:
            raise SystemExit(f"No runs found for date {date_str}.")

    sim_start = compute_sim_start(date_str)

    config_cache: Dict[str, Dict[str, object]] = {}
    wrf_done_by_fire: Dict[str, bool] = {}
    ungrib_paths: List[Path] = []
    metgrid_paths: List[Path] = []
    wrfrst_paths: List[Path] = []
    hrrr_dirs: Dict[Path, None] = {}

    max_sim_hrs = 0
    for row in day_rows:
        fire_id = row["fire_id"]
        if fire_id not in config_cache:
            config_cache[fire_id] = load_config(args.config_root, fire_id)
        cfg = config_cache[fire_id]
        wps_run_dir, wrf_run_dir, _ = resolve_run_dirs(cfg, sim_start)
        wrf_done_by_fire[fire_id] = count_wrfouts(wrf_run_dir) >= args.wrfout_threshold

        if wps_run_dir.exists():
            ungrib_paths.extend(sorted(wps_run_dir.glob("ungrib*")))
        metgrid_paths.append(wps_run_dir / "metgrid")
        if args.delete_wrfrst and wrf_run_dir.exists():
            wrfrst_paths.extend(sorted(wrf_run_dir.glob("wrfrst_d0*")))

        sim_hrs = int(cfg.get("sim_hrs", 24))
        if sim_hrs > max_sim_hrs:
            max_sim_hrs = sim_hrs

    all_done = all(wrf_done_by_fire.values())
    if not all_done and args.force:
        incomplete = [k for k, v in wrf_done_by_fire.items() if not v]
        raise SystemExit(
            f"Refusing to clean: {len(incomplete)} runs not complete (force requires all done)."
        )

    print(f"Cleaning day {date_str} (sim_start {sim_start})")
    print(f"Runs in day: {len(day_rows)}; all_done={all_done}")

    # HRRR directories for the window of sim_hrs (use max across fires)
    if args.delete_hrrr:
        start_dt = datetime.strptime(sim_start, "%Y%m%d_%H")
        end_dt = start_dt + timedelta(hours=max_sim_hrs)
        day_cursor = datetime(start_dt.year, start_dt.month, start_dt.day)
        while day_cursor <= end_dt:
            day_str = day_cursor.strftime("%Y%m%d")
            hrrr_dirs[Path(config_cache[day_rows[0]["fire_id"]]["grib_dir"]) / f"hrrr.{day_str}"] = None
            hrrr_dirs[Path(config_cache[day_rows[0]["fire_id"]]["grib_dir"]) / f"hrrr.{day_str}.subset"] = None
            day_cursor += timedelta(days=1)

    # Remove ungrib/metgrid paths (dedup)
    seen: Dict[Path, None] = {}
    for path in ungrib_paths + metgrid_paths:
        if path in seen:
            continue
        seen[path] = None
        remove_path(path, execute=args.execute)

    if args.delete_wrfrst:
        for path in wrfrst_paths:
            remove_path(path, execute=args.execute)

    for path in hrrr_dirs:
        remove_path(path, execute=args.execute)

    if not args.execute:
        print("Dry-run only. Re-run with --execute to delete.")


if __name__ == "__main__":
    main()
