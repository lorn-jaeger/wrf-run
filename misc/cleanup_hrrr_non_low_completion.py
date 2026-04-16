#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import yaml


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Delete HRRR grib directories for all day indices not in runs_low_completion list."
    )
    parser.add_argument(
        "--low-completion-file",
        type=Path,
        default=Path("runs_low_completion_indices.txt"),
        help="File with day indices to keep (default: runs_low_completion_indices.txt).",
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
        "--execute",
        action="store_true",
        help="Perform deletions (default is dry-run).",
    )
    parser.add_argument(
        "--keep-ungrib",
        action="store_true",
        help="Do not delete ungrib outputs (default deletes ungrib).",
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


def resolve_wps_run_dir(cfg: Dict[str, object], sim_start: str) -> Path:
    wps_run_dir_parent = Path(cfg["wps_run_dir"])
    exp_name = cfg.get("exp_name")
    exp_wrf_only = bool(cfg.get("exp_wrf_only", False))

    if exp_name:
        if exp_wrf_only:
            return wps_run_dir_parent / sim_start
        return wps_run_dir_parent / sim_start / str(exp_name)
    return wps_run_dir_parent / sim_start


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
    rows = load_rows(args.runs_file)
    grouped = group_rows_by_date(rows)
    if not grouped:
        raise SystemExit("No runs found in CSV.")

    if not args.low_completion_file.exists():
        raise SystemExit(f"Missing low completion list: {args.low_completion_file}")
    keep_indices = {
        int(x) for x in args.low_completion_file.read_text().split() if x.strip().isdigit()
    }

    all_indices = list(range(1, len(grouped) + 1))
    cleanup_indices = [i for i in all_indices if i not in keep_indices]
    print(f"Keeping {len(keep_indices)} day indices from {args.low_completion_file}")
    print(f"Cleaning HRRR for {len(cleanup_indices)} day indices.")
    if not cleanup_indices:
        return

    config_cache: Dict[str, Dict[str, object]] = {}
    ungrib_paths: Dict[Path, None] = {}
    for day_index in cleanup_indices:
        date_str, day_rows = grouped[day_index - 1]
        sim_start = compute_sim_start(date_str)
        max_sim_hrs = 0
        grib_dir: Path | None = None

        for row in day_rows:
            fire_id = row["fire_id"]
            if fire_id not in config_cache:
                config_cache[fire_id] = load_config(args.config_root, fire_id)
            cfg = config_cache[fire_id]
            if grib_dir is None:
                grib_dir = Path(cfg.get("grib_dir", ""))
            sim_hrs = int(cfg.get("sim_hrs", 24))
            if sim_hrs > max_sim_hrs:
                max_sim_hrs = sim_hrs
            if not args.keep_ungrib:
                wps_run_dir = resolve_wps_run_dir(cfg, sim_start)
                if wps_run_dir.exists():
                    for path in wps_run_dir.glob("ungrib*"):
                        ungrib_paths[path] = None

        if grib_dir is None or not str(grib_dir):
            print(f"[WARN] Missing grib_dir for day-index {day_index}; skipping")
            continue

        start_dt = datetime.strptime(sim_start, "%Y%m%d_%H")
        end_dt = start_dt + timedelta(hours=max_sim_hrs)
        day_cursor = datetime(start_dt.year, start_dt.month, start_dt.day)
        print(f"[DAY] {date_str} (day-index {day_index}) -> HRRR window {start_dt} to {end_dt}")
        while day_cursor <= end_dt:
            day_str = day_cursor.strftime("%Y%m%d")
            remove_path(grib_dir / f"hrrr.{day_str}", execute=args.execute)
            remove_path(grib_dir / f"hrrr.{day_str}.subset", execute=args.execute)
            day_cursor += timedelta(days=1)

    if not args.keep_ungrib and ungrib_paths:
        print(f"Cleaning {len(ungrib_paths)} ungrib path(s).")
        for path in ungrib_paths:
            remove_path(path, execute=args.execute)

    if not args.execute:
        print("Dry-run only. Re-run with --execute to delete.")


if __name__ == "__main__":
    main()
