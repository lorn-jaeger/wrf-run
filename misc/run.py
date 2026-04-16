#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import shlex
import subprocess
from datetime import datetime
from pathlib import Path
from typing import List, Dict


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run all fire simulations for a given day.")
    parser.add_argument(
        "--date",
        required=True,
        help="Target simulation day in YYYY-MM-DD format (matches the 'current' column).",
    )
    parser.add_argument(
        "--runs-file",
        type=Path,
        default=Path("fires/output_budget.csv"),
        help="CSV file containing per-day run commands (default: fires/output_budget.csv).",
    )
    parser.add_argument(
        "--logs-dir",
        type=Path,
        default=Path("logs"),
        help="Directory to store per-run log files (default: ./logs).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print matching commands without executing them.",
    )
    return parser.parse_args()


def validate_date(date_str: str) -> str:
    try:
        parsed = datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError as exc:
        raise SystemExit(f"Invalid --date '{date_str}': {exc}")
    return parsed.strftime("%Y-%m-%d")


def load_runs(runs_file: Path) -> List[Dict[str, str]]:
    if not runs_file.exists():
        raise SystemExit(f"Runs file not found: {runs_file}")

    with runs_file.open("r", newline="") as handle:
        reader = csv.DictReader(handle)
        required = {"key", "current", "command"}
        if not reader.fieldnames or not required.issubset(reader.fieldnames):
            raise SystemExit(f"{runs_file} must include columns {sorted(required)}")
        return [row for row in reader]


def filter_runs(runs: List[Dict[str, str]], target_date: str) -> List[Dict[str, str]]:
    filtered = [row for row in runs if row["current"] == target_date]
    if not filtered:
        raise SystemExit(f"No runs found in CSV for date {target_date}.")
    return filtered


def run_commands(runs: List[Dict[str, str]], logs_dir: Path, dry_run: bool) -> None:
    logs_dir.mkdir(parents=True, exist_ok=True)

    for row in runs:
        key = row["key"]
        command = row["command"].strip()
        log_path = logs_dir / f"{key}_{row['current']}.log"

        print(f"[RUN] {key} ({row['current']}): {command}")
        if dry_run:
            continue

        with log_path.open("a") as log_file:
            result = subprocess.run(
                shlex.split(command),
                stdout=log_file,
                stderr=log_file,
            )
            if result.returncode == 0:
                print(f"[SUCCESS] {key} completed.")
            else:
                print(f"[FAILURE] {key} exited with code {result.returncode}. See {log_path}.")


def main() -> None:
    args = parse_args()
    target_date = validate_date(args.date)
    runs = load_runs(args.runs_file)
    todays_runs = filter_runs(runs, target_date)
    run_commands(todays_runs, args.logs_dir, args.dry_run)


if __name__ == "__main__":
    main()
