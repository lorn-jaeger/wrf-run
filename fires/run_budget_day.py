#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import re
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
        "--retry-vcfl",
        action="store_true",
        help="Only rerun WRF for fires that previously failed with a V_CFL error.",
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


def detect_namelist_name(template_dir: Path) -> str:
    default = template_dir / "namelist.input.hrrr.hybr"
    if default.exists():
        return default.name
    candidates = sorted(template_dir.glob("namelist.input*"))
    if not candidates:
        raise SystemExit(f"No namelist.input* files found in {template_dir}")
    return candidates[0].name


def _parse_epssm_value(line: str) -> float:
    match = re.search(r"([-+]?\d*\.?\d+)", line)
    return float(match.group(1)) if match else 0.1


def update_epssm(namelist_path: Path) -> float:
    if not namelist_path.exists():
        raise SystemExit(f"Namelist not found: {namelist_path}")
    lines = namelist_path.read_text().splitlines()
    for idx, line in enumerate(lines):
        if "epssm" in line.lower():
            current = _parse_epssm_value(line)
            new_value = round(min(current + 0.1, 0.7), 2)
            lines[idx] = f" epssm                               = {new_value:.1f},   {new_value:.1f},   {new_value:.1f},"
            namelist_path.write_text("\n".join(lines) + "\n")
            return new_value
    insert_idx = next((i for i, line in enumerate(lines) if line.strip().lower().startswith("&dynamics")), None)
    new_value = 0.2
    new_line = " epssm                               = 0.2,   0.2,   0.2,"
    if insert_idx is None:
        lines.append(new_line)
    else:
        lines.insert(insert_idx + 1, new_line)
    namelist_path.write_text("\n".join(lines) + "\n")
    return new_value


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

    sim_start = compute_sim_start(target_date)

    config_cache: Dict[str, Dict[str, object]] = {}

    def get_config(fire_id: str) -> Dict[str, object]:
        if fire_id not in config_cache:
            cfg_path = (args.config_root / f"{fire_id}.yaml").resolve()
            if not cfg_path.exists():
                raise SystemExit(f"Config not found for {fire_id}: {cfg_path}")
            with cfg_path.open("r") as handle:
                config_cache[fire_id] = yaml.safe_load(handle) or {}
        return config_cache[fire_id]

    if args.retry_vcfl:
        run_wrf_script = Path("wps_wrf_workflow/run_wrf.py").resolve()
        if not run_wrf_script.exists():
            raise SystemExit(f"run_wrf.py not found: {run_wrf_script}")
        retry_vcfl(
            day_rows,
            sim_start,
            args.start_fire,
            run_wrf_script,
            get_config,
            args.logs_dir,
            args.dry_run,
        )
        return

    args.workflow_script = args.workflow_script.resolve()
    if not args.workflow_script.exists():
        raise SystemExit(f"Workflow script not found: {args.workflow_script}")
    workflow_cwd = args.workflow_script.parent

    reference_ungrib: Path | None = None
    start_idx = max(1, args.start_fire)
    if start_idx > 1:
        first_cfg = get_config(day_rows[0]["fire_id"])
        candidate = (Path(first_cfg["wps_run_dir"]) / sim_start / "ungrib").resolve()
        if candidate.exists():
            reference_ungrib = candidate
        else:
            raise SystemExit(
                f"Unable to locate shared ungrib directory {candidate} required for --start-fire={args.start_fire}."
            )

    temp_files: List[Path] = []
    try:
        if start_idx > len(day_rows):
            print(
                f"[INFO] --start-fire={args.start_fire} exceeds number of runs ({len(day_rows)}) for {target_date}. Nothing to do."
            )
            return

        for pos, row in enumerate(day_rows, start=1):
            if pos < start_idx:
                print(f"[SKIP] {row['fire_id']} ({row['date']}) before start-fire={args.start_fire}")
                continue
            fire_id = row["fire_id"]
            config_data = get_config(fire_id)

            cycle_ungrib_dir = (Path(config_data["wps_run_dir"]) / sim_start / "ungrib").resolve()
            disable_ungrib = reference_ungrib is not None
            if disable_ungrib:
                ensure_ungrib_link(cycle_ungrib_dir, reference_ungrib)
            cfg_path = (args.config_root / f"{fire_id}.yaml").resolve()
            cmd, cfg_used, temp_path = build_command(
                args.workflow_script, sim_start, cfg_path, config_data, disable_ungrib
            )
            if temp_path:
                temp_files.append(temp_path)
            state = "FIRST" if reference_ungrib is None else "SUBSEQUENT (do_ungrib=False)"
            print(f"[INFO] {row['date']} {fire_id} ({state}) -> {cfg_used}")
            log_path = args.logs_dir / f"{fire_id}_{row['date']}.log"
            ret = run_command(cmd, log_path, args.dry_run, workflow_cwd)
            if ret != 0:
                raise SystemExit(ret)
            if reference_ungrib is None:
                if not cycle_ungrib_dir.exists():
                    raise SystemExit(f"Expected ungrib output missing: {cycle_ungrib_dir}")
                reference_ungrib = cycle_ungrib_dir
    finally:
        for tmp in temp_files:
            try:
                tmp.unlink()
            except FileNotFoundError:
                pass


def count_wrfouts(run_dir: Path) -> int:
    if not run_dir.exists():
        return 0
    return len(list(run_dir.glob("wrfout_d0*")))


def retry_vcfl(
    day_rows: Sequence[Dict[str, str]],
    sim_start: str,
    start_fire: int,
    run_wrf_script: Path,
    get_config,
    logs_dir: Path,
    dry_run: bool,
) -> None:
    workflow_cwd = run_wrf_script.parent
    start_idx = max(1, start_fire)
    qstat_jobs, qstat_available = fetch_qstat_jobs()
    for pos, row in enumerate(day_rows, start=1):
        fire_id = row["fire_id"]
        if pos < start_idx:
            print(f"[SKIP] {fire_id} ({row['date']}) before start-fire={start_fire}")
            continue
        cfg = get_config(fire_id)
        wrf_cycle_dir = Path(cfg["wrf_run_dir"]) / sim_start
        log_path = logs_dir / f"{fire_id}_{row['date']}_retry.log"
        job_id, _ = extract_job_info(log_path)
        if job_id and qstat_available and job_id in qstat_jobs:
            print(f"[SKIP] {fire_id}: job {job_id} currently {qstat_jobs[job_id]} (retry running).")
            continue
        wrfout_count = count_wrfouts(wrf_cycle_dir)
        if wrfout_count != 1:
            print(f"[SKIP] {fire_id}: wrfout count = {wrfout_count} (only rerunning V_CFL cases).")
            continue

        template_dir = Path(cfg["template_dir"])
        namelist_name = detect_namelist_name(template_dir)
        namelist_path = template_dir / namelist_name
        new_eps = update_epssm(namelist_path)
        print(f"[INFO] {fire_id}: Updated EPSSM to {new_eps:.1f} in {namelist_path}")

        cmd = [
            sys.executable,
            str(run_wrf_script),
            "-b",
            sim_start,
            "-s",
            str(cfg.get("sim_hrs", 24)),
            "-w",
            str(cfg["wrf_ins_dir"]),
            "-r",
            str(wrf_cycle_dir),
            "-t",
            str(template_dir),
            "-i",
            cfg.get("icbc_model", "HRRR"),
            "-n",
            namelist_name,
            "-q",
            "pbs",
            "-a",
            "derecho",
        ]
        log_path = logs_dir / f"{fire_id}_{row['date']}_retry.log"
        ret = run_command(cmd, log_path, dry_run, workflow_cwd)
        if ret != 0:
            raise SystemExit(ret)


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
        if len(parts) >= 5:
            job_token = parts[0]
            match = re.match(r"(\d+)", job_token)
            if match:
                jobs[match.group(1)] = parts[4]
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


if __name__ == "__main__":
    main()
