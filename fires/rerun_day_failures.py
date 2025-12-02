#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Sequence, Tuple

import yaml


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Handle common daily workflow failures (missing HRRR or V_CFL) and rerun as needed."
    )
    parser.add_argument(
        "--day-index",
        type=int,
        required=True,
        help="1-based index of the target day in the runs CSV.",
    )
    parser.add_argument(
        "--runs-file",
        type=Path,
        default=Path("fires/output_budget.csv"),
        help="CSV containing the per-day runs (default: fires/output_budget.csv).",
    )
    parser.add_argument(
        "--config-root",
        type=Path,
        default=Path("configs/built/workflow"),
        help="Directory with per-fire workflow configs.",
    )
    parser.add_argument(
        "--logs-dir",
        type=Path,
        default=Path("logs/budget_runs"),
        help="Directory containing per-fire workflow logs.",
    )
    parser.add_argument(
        "--run-day-script",
        type=Path,
        default=Path("fires/run_budget_day.py"),
        help="Script used to execute a full day (default: fires/run_budget_day.py).",
    )
    parser.add_argument(
        "--download-script",
        type=Path,
        default=Path("fires/download_hrrr_day.py"),
        help="Utility script used to backfill/interpolate HRRR data (default: fires/download_hrrr_day.py).",
    )
    parser.add_argument(
        "--runs-csv-delimiter",
        default=",",
        help="Delimiter for the runs CSV (default: ',').",
    )
    return parser.parse_args()


def load_runs(csv_path: Path, delimiter: str) -> List[Dict[str, str]]:
    import csv

    if not csv_path.exists():
        raise SystemExit(f"Runs file not found: {csv_path}")
    with csv_path.open("r", newline="") as handle:
        reader = csv.DictReader(handle, delimiter=delimiter)
        required = {"date", "fire_id"}
        if not reader.fieldnames or not required.issubset(reader.fieldnames):
            raise SystemExit(f"{csv_path} must include columns {sorted(required)}")
        return [row for row in reader]


def group_rows_by_date(rows: Sequence[Dict[str, str]]) -> List[Tuple[str, List[Dict[str, str]]]]:
    grouped: Dict[str, List[Dict[str, str]]] = {}
    ordered: List[str] = []
    for row in rows:
        date = row["date"]
        if date not in grouped:
            grouped[date] = []
            ordered.append(date)
        grouped[date].append(row)
    return [(date, grouped[date]) for date in ordered]


def compute_sim_start(date_str: str) -> str:
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    shifted = dt - timedelta(hours=6)
    return shifted.strftime("%Y%m%d_%H")


def read_config(fire_id: str, config_root: Path) -> Dict[str, object]:
    cfg_path = (config_root / f"{fire_id}.yaml").resolve()
    if not cfg_path.exists():
        raise SystemExit(f"Missing config for {fire_id}: {cfg_path}")
    with cfg_path.open("r") as handle:
        return yaml.safe_load(handle) or {}


def detect_missing_hrrr(day_rows: Sequence[Dict[str, str]], logs_dir: Path) -> bool:
    patterns = (
        "HTTP error while downloading",
        "Unable to interpolate missing file",
        "Unable to synthesize",
        "Interpolation failed",
    )
    for row in day_rows:
        log_path = logs_dir / f"{row['fire_id']}_{row['date']}.log"
        if not log_path.exists():
            continue
        try:
            text = log_path.read_text(errors="ignore")
        except OSError:
            continue
        if any(pattern in text for pattern in patterns):
            print(f"[DETECTED] Missing HRRR data referenced in {log_path}")
            return True
    return False


def detect_vcfl(
    day_rows: Sequence[Dict[str, str]],
    sim_start: str,
    config_cache: Dict[str, Dict[str, object]],
) -> List[str]:
    vcfl_fires: List[str] = []
    for row in day_rows:
        fire_id = row["fire_id"]
        cfg = config_cache[fire_id]
        wrf_dir = Path(cfg["wrf_run_dir"]).joinpath(sim_start)
        wrfouts = list(wrf_dir.glob("wrfout_d0*"))
        if len(wrfouts) == 1:
            vcfl_fires.append(fire_id)
    if vcfl_fires:
        print(f"[DETECTED] V_CFL failures for: {', '.join(vcfl_fires)}")
    return vcfl_fires


def interpolate_hrrr(
    cycle_start: datetime,
    sim_hrs: int,
    grib_root: Path,
    native: bool,
    source: str,
    download_script: Path,
) -> None:
    unique_dates = set()
    for hour in range(sim_hrs + 1):
        unique_dates.add((cycle_start + timedelta(hours=hour)).strftime("%Y%m%d"))
    for date_str in sorted(unique_dates):
        cmd = [
            sys.executable,
            str(download_script),
            "--date",
            date_str,
            "--output-dir",
            str(grib_root),
            "--source",
            source,
        ]
        if native:
            cmd.append("--native-grid")
        print(f"[RUN] {' '.join(cmd)}")
        result = subprocess.run(cmd)
        if result.returncode != 0:
            raise SystemExit(f"HRRR interpolation/downloading failed for date {date_str}.")


def rerun_full_day(run_script: Path, day_index: int) -> None:
    cmd = [sys.executable, str(run_script), "--day-index", str(day_index)]
    print(f"[RUN] {' '.join(cmd)}")
    result = subprocess.run(cmd)
    if result.returncode != 0:
        raise SystemExit("Full-day rerun failed.")


def rerun_vcfl(run_script: Path, day_index: int) -> None:
    cmd = [sys.executable, str(run_script), "--day-index", str(day_index), "--retry-vcfl"]
    print(f"[RUN] {' '.join(cmd)}")
    result = subprocess.run(cmd)
    if result.returncode != 0:
        raise SystemExit("V_CFL retry run failed.")


def main() -> None:
    args = parse_args()
    rows = load_runs(args.runs_file, args.runs_csv_delimiter)
    grouped = group_rows_by_date(rows)
    if not grouped:
        raise SystemExit("Runs file is empty.")

    if not (1 <= args.day_index <= len(grouped)):
        raise SystemExit(f"--day-index must be between 1 and {len(grouped)}.")

    target_date, day_rows = grouped[args.day_index - 1]
    print(f"Selected day #{args.day_index}: {target_date} ({len(day_rows)} fires)")

    sim_start = compute_sim_start(target_date)
    cycle_start_dt = datetime.strptime(sim_start, "%Y%m%d_%H")

    config_cache: Dict[str, Dict[str, object]] = {
        row["fire_id"]: read_config(row["fire_id"], args.config_root) for row in day_rows
    }

    if detect_missing_hrrr(day_rows, args.logs_dir):
        # Use the first fire as the reference for grib location/settings.
        first_cfg = config_cache[day_rows[0]["fire_id"]]
        grib_root = Path(first_cfg["grib_dir"])
        sim_hrs = int(first_cfg.get("sim_hrs", 24))
        native = bool(first_cfg.get("hrrr_native", False))
        source = str(first_cfg.get("icbc_source", "AWS"))
        interpolate_hrrr(cycle_start_dt, sim_hrs, grib_root, native, source, args.download_script)
        rerun_full_day(args.run_day_script, args.day_index)
        return

    vcfl_fires = detect_vcfl(day_rows, sim_start, config_cache)
    if vcfl_fires:
        rerun_vcfl(args.run_day_script, args.day_index)
        return

    print("No actionable failures detected for this day.")


if __name__ == "__main__":
    main()
