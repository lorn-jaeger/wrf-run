#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import shutil
import shlex
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Sequence, Tuple

import yaml


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Execute all fire workflows for a single day from output_budget.csv.\n"
            "Days are referenced by their order of appearance in the CSV."
        )
    )
    parser.add_argument(
        "--runs-file",
        type=Path,
        default=Path("fires/output_budget.csv"),
        help="CSV listing budgeted runs (default: fires/output_budget.csv).",
    )
    parser.add_argument(
        "--day-index",
        type=int,
        help="1-based index of the day to run (use --list-days to inspect ordering).",
    )
    parser.add_argument(
        "--list-days",
        action="store_true",
        help="List available days with their index and exit.",
    )
    parser.add_argument(
        "--start-fire",
        type=int,
        default=1,
        help="1-based position within the selected day to start running (default: 1).",
    )
    parser.add_argument(
        "--workflow-script",
        type=Path,
        default=Path("wps_wrf_workflow/setup_wps_wrf.py"),
        help="Path to setup_wps_wrf.py (default: wps_wrf_workflow/setup_wps_wrf.py).",
    )
    parser.add_argument(
        "--config-root",
        type=Path,
        default=Path("configs/built/workflow"),
        help="Directory containing per-fire workflow YAML files.",
    )
    parser.add_argument(
        "--logs-dir",
        type=Path,
        default=Path("logs/budget_runs"),
        help="Directory to write per-run logs (default: logs/budget_runs).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the selected runs without executing setup_wps_wrf.py.",
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
    order: List[str] = []
    grouped: Dict[str, List[Dict[str, str]]] = {}
    for row in rows:
        date = row["date"]
        if date not in grouped:
            order.append(date)
            grouped[date] = []
        grouped[date].append(row)
    return [(date, grouped[date]) for date in order]


def list_days(grouped: Sequence[Tuple[str, Sequence[Dict[str, str]]]]) -> None:
    print("Available days:")
    for idx, (date, runs) in enumerate(grouped, start=1):
        fire_list = ", ".join(row["fire_id"] for row in runs)
        print(f" {idx:3d}: {date} ({len(runs)} runs) -> {fire_list}")


def compute_sim_start(date_str: str) -> str:
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    shifted = dt - timedelta(hours=6)
    return shifted.strftime("%Y%m%d_%H")


def ensure_ungrib_link(target: Path, source: Path) -> None:
    if not source.exists():
        raise SystemExit(f"Shared ungrib directory not found: {source}")
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists() or target.is_symlink():
        try:
            if target.resolve() == source.resolve():
                return
        except FileNotFoundError:
            pass
        if target.is_symlink() or target.is_file():
            target.unlink()
        else:
            shutil.rmtree(target)
    target.symlink_to(source, target_is_directory=True)


def build_command(
    workflow_script: Path,
    sim_start: str,
    config_path: Path,
    config_data: Dict[str, object],
    disable_ungrib: bool,
) -> Tuple[List[str], Path, Path | None]:
    cfg_path = config_path
    temp_path: Path | None = None
    if disable_ungrib and config_data.get("do_ungrib"):
        patched = dict(config_data)
        patched["do_ungrib"] = False
        temp = tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False)
        with temp:
            yaml.safe_dump(patched, temp, sort_keys=False)
        cfg_path = Path(temp.name)
        temp_path = cfg_path
    cmd = [
        sys.executable,
        str(workflow_script),
        "-b",
        sim_start,
        "-c",
        str(cfg_path),
    ]
    return cmd, cfg_path, temp_path


def run_command(
    command: Sequence[str], log_path: Path, dry_run: bool, workdir: Path | None
) -> int:
    cmd_str = " ".join(shlex.quote(part) for part in command)
    cwd_note = f" (cwd={workdir})" if workdir else ""
    print(f"[RUN]{cwd_note} {cmd_str}")
    if dry_run:
        return 0
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a") as log_file:
        result = subprocess.run(
            command,
            stdout=log_file,
            stderr=log_file,
            cwd=str(workdir) if workdir else None,
        )
    if result.returncode == 0:
        print(f"[SUCCESS] See {log_path} for logs.")
    else:
        print(f"[FAILURE] Exit code {result.returncode}. Check {log_path}.")
    return result.returncode


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

    if args.day_index < 1 or args.day_index > len(grouped):
        raise SystemExit(
            f"--day-index must be between 1 and {len(grouped)} (got {args.day_index})."
        )

    target_date, day_rows = grouped[args.day_index - 1]
    print(f"Selected day #{args.day_index}: {target_date} ({len(day_rows)} runs)")

    args.workflow_script = args.workflow_script.resolve()
    if not args.workflow_script.exists():
        raise SystemExit(f"Workflow script not found: {args.workflow_script}")
    workflow_cwd = args.workflow_script.parent

    reference_ungrib: Path | None = None

    temp_files: List[Path] = []
    try:
        for idx, row in enumerate(day_rows):
            fire_id = row["fire_id"]
            config_path = (args.config_root / f"{fire_id}.yaml").resolve()
            if not config_path.exists():
                raise SystemExit(f"Config not found for {fire_id}: {config_path}")

            with config_path.open("r") as handle:
                config_data = yaml.safe_load(handle) or {}

            sim_start = compute_sim_start(row["date"])
            cycle_ungrib_dir = (Path(config_data["wps_run_dir"]) / sim_start / "ungrib").resolve()
            disable_ungrib = idx > 0
            if disable_ungrib:
                if reference_ungrib is None:
                    raise SystemExit("First run did not establish a shared ungrib directory.")
                ensure_ungrib_link(cycle_ungrib_dir, reference_ungrib)
            cmd, cfg_used, temp_path = build_command(
                args.workflow_script, sim_start, config_path, config_data, disable_ungrib
            )
            if temp_path:
                temp_files.append(temp_path)
            state = "FIRST" if idx == 0 else "SUBSEQUENT (do_ungrib=False)"
            print(f"[INFO] {row['date']} {fire_id} ({state}) -> {cfg_used}")
            log_path = args.logs_dir / f"{fire_id}_{row['date']}.log"
            ret = run_command(cmd, log_path, args.dry_run, workflow_cwd)
            if ret != 0:
                raise SystemExit(ret)
            if reference_ungrib is None:
                if not cycle_ungrib_dir.exists():
                    raise SystemExit(f"Expected ungrib output missing: {cycle_ungrib_dir}")
                reference_ungrib = cycle_ungrib_dir.resolve()
    finally:
        for tmp in temp_files:
            try:
                tmp.unlink()
            except FileNotFoundError:
                pass


if __name__ == "__main__":
    main()
