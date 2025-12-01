#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from collections import deque
from datetime import datetime, timedelta
import os
import re
import subprocess
from pathlib import Path
from typing import Dict, List, Sequence, Tuple

import yaml


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Summarize HRRR/ungrib/WRF status for all fires scheduled on a given day."
    )
    parser.add_argument(
        "--runs-file",
        type=Path,
        default=Path("fires/output_budget.csv"),
        help="CSV listing day-by-day fires (default: fires/output_budget.csv).",
    )
    parser.add_argument(
        "--config-root",
        type=Path,
        default=Path("configs/built/workflow"),
        help="Directory containing per-fire workflow YAML configs.",
    )
    parser.add_argument(
        "--logs-dir",
        type=Path,
        default=Path("logs/budget_runs"),
        help="Directory containing workflow log files (default: logs/budget_runs).",
    )
    parser.add_argument(
        "--day-index",
        type=int,
        help="1-based index of the day to report (use --list-days to inspect ordering).",
    )
    parser.add_argument(
        "--list-days",
        action="store_true",
        help="List available days and exit.",
    )
    return parser.parse_args()


def load_rows(csv_path: Path) -> List[Dict[str, str]]:
    if not csv_path.exists():
        raise SystemExit(f"Runs file not found: {csv_path}")
    with csv_path.open("r", newline="") as handle:
        reader = csv.DictReader(handle)
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


def list_days(grouped: Sequence[Tuple[str, Sequence[Dict[str, str]]]]) -> None:
    print("Available days:")
    for idx, (date, runs) in enumerate(grouped, start=1):
        names = ", ".join(row["fire_id"] for row in runs)
        print(f" {idx:3d}: {date} ({len(runs)} fires) -> {names}")


def compute_sim_start(date_str: str) -> str:
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    shifted = dt - timedelta(hours=6)
    return shifted.strftime("%Y%m%d_%H")


def read_config(fire_id: str, config_root: Path) -> Dict[str, object]:
    cfg_path = config_root / f"{fire_id}.yaml"
    if not cfg_path.exists():
        raise SystemExit(f"Missing config for {fire_id}: {cfg_path}")
    with cfg_path.open("r") as handle:
        data = yaml.safe_load(handle) or {}
    data["_config_path"] = cfg_path
    return data


def check_hrrr(grib_root: Path, cycle_dt: datetime) -> Tuple[bool, Path, int]:
    date_str = cycle_dt.strftime("%Y%m%d")
    target = grib_root / f"hrrr.{date_str}" / "conus"
    if not target.is_dir():
        return False, target, 0
    count = sum(1 for _ in target.glob("*.grib2"))
    return count > 0, target, count


def check_ungrib(paths: Sequence[Tuple[str, Path]]) -> Tuple[bool, List[Tuple[str, Path, int]]]:
    reports: List[Tuple[str, Path, int]] = []
    ok = True
    for fire_id, path in paths:
        if not path.exists():
            reports.append((fire_id, path, -1))
            ok = False
            continue
        try:
            count = sum(1 for _ in path.iterdir())
        except PermissionError:
            count = -2
            ok = False
        else:
            if count == 0:
                ok = False
        reports.append((fire_id, path, count))
    return ok, reports


def fetch_qstat_jobs() -> Tuple[Dict[str, str], bool]:
    user = os.environ.get("USER")
    if not user:
        return {}, False
    try:
        result = subprocess.run(
            ["qstat", "-u", user],
            check=False,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        return {}, False
    jobs: Dict[str, str] = {}
    for line in result.stdout.splitlines():
        parts = line.split()
        if len(parts) >= 5 and parts[0].isdigit():
            jobs[parts[0]] = parts[4]
    return jobs, True


def tail_lines(path: Path, limit: int = 200) -> List[str]:
    dq: deque[str] = deque(maxlen=limit)
    with path.open("r") as handle:
        for line in handle:
            dq.append(line.rstrip())
    return list(dq)


def extract_job_info(log_path: Path) -> Tuple[str | None, str]:
    if not log_path.exists():
        return None, "log not found"
    tail = tail_lines(log_path, limit=200)
    job_id = None
    for line in tail:
        match = re.search(r"Submitted batch job (\d+)", line)
        if match:
            job_id = match.group(1)
    last_line = tail[-1] if tail else ""
    return job_id, last_line


def summarize_wrf(
    fire_id: str,
    wrf_cycle_dir: Path,
    log_path: Path,
    qstat_jobs: Dict[str, str],
    qstat_available: bool,
) -> Tuple[str, int]:
    if not wrf_cycle_dir.exists():
        return ("MISSING RUN DIR", 0)
    wrfouts = sorted(wrf_cycle_dir.glob("wrfout_d0*"))
    count = len(wrfouts)
    if count == 0:
        job_id, last_line = extract_job_info(log_path)
        if job_id and qstat_jobs.get(job_id):
            state = qstat_jobs[job_id]
            msg = f"WRF QUEUED/RUNNING (job {job_id}, state {state})"
        elif job_id:
            msg = f"WRF JOB SUBMITTED ({job_id}) BUT NO OUTPUT"
        else:
            msg = "WRF NOT STARTED (no job submission found)"
        if last_line and last_line != "log not found":
            msg += f" | last log: {last_line}"
        if not qstat_available:
            msg += " | qstat unavailable"
        return (msg, 0)
    if count == 1:
        job_id, last_line = extract_job_info(log_path)
        if job_id and qstat_jobs.get(job_id):
            state = qstat_jobs[job_id]
            msg = f"V_CFL (single wrfout) | retry running (job {job_id}, state {state})"
        else:
            msg = "V_CFL (single wrfout)"
        if last_line and last_line != "log not found":
            msg += f" | last log: {last_line}"
        if not qstat_available:
            msg += " | qstat unavailable"
        return (msg, count)
    if count in (30, 31):
        return ("SUCCESS", count)
    return (f"UNEXPECTED COUNT ({count})", count)


def main() -> None:
    args = parse_args()
    rows = load_rows(args.runs_file)
    grouped = group_rows_by_date(rows)

    if not grouped:
        raise SystemExit(f"No runs found in {args.runs_file}")

    if args.list_days or args.day_index is None:
        list_days(grouped)
        if args.day_index is None:
            return

    if not (1 <= args.day_index <= len(grouped)):
        raise SystemExit(f"--day-index must be between 1 and {len(grouped)}")

    target_date, day_rows = grouped[args.day_index - 1]
    print(f"Selected day #{args.day_index}: {target_date} ({len(day_rows)} fires)")

    sim_start = compute_sim_start(target_date)
    cycle_dt = datetime.strptime(sim_start, "%Y%m%d_%H")

    config_cache: Dict[str, Dict[str, object]] = {}

    def get_cfg(fire_id: str) -> Dict[str, object]:
        if fire_id not in config_cache:
            config_cache[fire_id] = read_config(fire_id, args.config_root)
        return config_cache[fire_id]

    first_cfg = get_cfg(day_rows[0]["fire_id"])
    grib_root = Path(first_cfg["grib_dir"])
    hrrr_ok, hrrr_path, hrrr_count = check_hrrr(grib_root, cycle_dt)
    status = "OK" if hrrr_ok else "MISSING"
    print("\n[HRRR]")
    print(f" Path : {hrrr_path}")
    print(f" Files: {hrrr_count}")
    print(f" Status: {status}")
    if not hrrr_ok:
        raise SystemExit("HRRR data not available for this day. Aborting report.")

    ungrib_paths = []
    for row in day_rows:
        cfg = get_cfg(row["fire_id"])
        wps_dir = Path(cfg["wps_run_dir"])
        ungrib_paths.append((row["fire_id"], wps_dir / sim_start / "ungrib"))

    ungrib_ok, ungrib_reports = check_ungrib(ungrib_paths)
    print("\n[UNGRIB]")
    for fire_id, path, count in ungrib_reports:
        if count < 0:
            msg = "MISSING" if count == -1 else "UNREADABLE"
        elif count == 0:
            msg = "EMPTY"
        else:
            msg = f"{count} files"
        print(f" {fire_id}: {path} -> {msg}")
    if not ungrib_ok:
        raise SystemExit("Ungrib outputs missing or empty. Aborting report.")

    qstat_jobs, qstat_available = fetch_qstat_jobs()
    logs_dir = args.logs_dir

    print("\n[FIRES]")
    for row in day_rows:
        fire_id = row["fire_id"]
        cfg = get_cfg(fire_id)
        wrf_dir = Path(cfg["wrf_run_dir"]) / sim_start
        log_path = logs_dir / f"{fire_id}_{row['date']}.log"
        result, count = summarize_wrf(fire_id, wrf_dir, log_path, qstat_jobs, qstat_available)
        print(f" {fire_id}: {result} ({count} wrfout files) -> {wrf_dir}")


if __name__ == "__main__":
    main()
