#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import shlex
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import yaml


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Retry V_CDL failures by bumping EPSSM and rerunning simulations for a given date."
    )
    parser.add_argument(
        "--date",
        required=True,
        help="Target simulation day in YYYY-MM-DD format (matches the 'current' column).",
    )
    parser.add_argument(
        "--runs-file",
        type=Path,
        default=Path("fires/output_budget.csv"),
        help="CSV file containing run metadata (default: fires/output_budget.csv).",
    )
    parser.add_argument(
        "--logs-dir",
        type=Path,
        default=Path("logs"),
        help="Directory containing log files (default: ./logs).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print actions without modifying configs or rerunning commands.",
    )
    return parser.parse_args()


def validate_date(date_str: str) -> str:
    try:
        parsed = datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError as exc:
        raise SystemExit(f"Invalid --date '{date_str}': {exc}")
    return parsed.strftime("%Y-%m-%d")


def load_runs(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        raise SystemExit(f"Runs file not found: {path}")
    with path.open("r", newline="") as handle:
        reader = csv.DictReader(handle)
        required = {"key", "current", "command", "sim_start"}
        if not reader.fieldnames or not required.issubset(reader.fieldnames):
            raise SystemExit(f"{path} must include columns {sorted(required)}")
        return [row for row in reader]


def filter_runs_by_date(runs: List[Dict[str, str]], target: str) -> List[Dict[str, str]]:
    selected = [row for row in runs if row["current"] == target]
    if not selected:
        raise SystemExit(f"No runs found in CSV for date {target}.")
    return selected


def parse_config_from_command(command: str) -> Optional[Path]:
    tokens = shlex.split(command)
    for idx, token in enumerate(tokens):
        if token in {"-c", "--config"} and idx + 1 < len(tokens):
            return Path(tokens[idx + 1])
    return None


def read_wrf_run_dir(config_path: Path) -> Path:
    if not config_path.exists():
        raise SystemExit(f"Config {config_path} not found.")
    with config_path.open("r") as handle:
        data = yaml.safe_load(handle) or {}
    wrf_run_dir = data.get("wrf_run_dir")
    if not wrf_run_dir:
        raise SystemExit(f"'wrf_run_dir' missing in {config_path}")
    return Path(wrf_run_dir)


def count_wrfout_files(directory: Path) -> int:
    if not directory.exists():
        return 0
    return len(list(directory.glob("wrfout_d01*")))


def increment_epssm(fire_id: str, sim_start: str, dry_run: bool) -> None:
    dt_obj = datetime.strptime(sim_start, "%Y%m%d_%H")
    formatted = dt_obj.strftime("%Y-%m-%d_%H:%M:%S")
    cmd = [
        sys.executable,
        "fires/adjust_epssm.py",
        "--fire-id",
        fire_id,
        "--date",
        formatted,
        "--direction",
        "increase",
    ]
    if dry_run:
        print("[DRY RUN] Would increment EPSSM:", " ".join(cmd))
        return
    subprocess.run(cmd, check=True)


def rerun_command(command: str, log_path: Path, dry_run: bool) -> int:
    if dry_run:
        print("[DRY RUN] Would rerun:", command)
        return 0
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a") as log_file:
        result = subprocess.run(shlex.split(command), stdout=log_file, stderr=log_file)
    return result.returncode


def main() -> None:
    args = parse_args()
    target_date = validate_date(args.date)
    runs = filter_runs_by_date(load_runs(args.runs_file), target_date)

    for row in runs:
        fire_id = row["key"]
        command = row["command"]
        sim_start = row["sim_start"]
        config_path = parse_config_from_command(command)
        if not config_path:
            print(f"[WARN] Unable to locate config path in command for {fire_id}. Skipping.")
            continue

        wrf_run_dir = read_wrf_run_dir(config_path)
        wrfout_count = count_wrfout_files(wrf_run_dir)

        if wrfout_count == 0:
            print(f"[INFO] No wrfout files present for {fire_id}. Investigate manually.")
            continue
        if wrfout_count > 1:
            print(f"[OK] {fire_id} produced {wrfout_count} wrfout files. Assuming success.")
            continue

        print(f"[V_CDL] {fire_id} has only one wrfout file in {wrf_run_dir}. Incrementing EPSSM.")
        try:
            increment_epssm(fire_id, sim_start, args.dry_run)
        except subprocess.CalledProcessError as exc:
            print(f"[ERROR] Failed to adjust EPSSM for {fire_id}: {exc}")
            continue

        log_path = args.logs_dir / f"{fire_id}_{row['current']}.log"
        ret = rerun_command(command, log_path, args.dry_run)
        if ret != 0:
            print(f"[FAILURE] Rerun for {fire_id} exited with code {ret}. See {log_path}.")
            continue

        new_count = count_wrfout_files(wrf_run_dir)
        if new_count > 1:
            print(f"[SUCCESS] {fire_id} rerun succeeded with {new_count} wrfout files.")
        else:
            print(f"[WARN] {fire_id} rerun still has {new_count} wrfout files. Additional debugging required.")


if __name__ == "__main__":
    main()
