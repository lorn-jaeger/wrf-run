#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import re
import subprocess
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


TIME_RE = re.compile(r"^(?:(\d+)-)?(\d+):(\d{2}):(\d{2})$")


def parse_duration(value: str) -> Optional[float]:
    value = value.strip()
    if not value:
        return None
    match = TIME_RE.match(value)
    if not match:
        return None
    days = int(match.group(1) or 0)
    hours = int(match.group(2))
    minutes = int(match.group(3))
    seconds = int(match.group(4))
    total = timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds).total_seconds()
    return total / 3600.0


@dataclass
class JobUsage:
    wall_hours: Optional[float] = None
    cpu_hours: Optional[float] = None
    cores: Optional[int] = None


def parse_keyvalue_blocks(lines: Iterable[str]) -> List[JobUsage]:
    jobs: List[JobUsage] = []
    current = JobUsage()

    def flush():
        nonlocal current
        if current.wall_hours or current.cpu_hours or current.cores:
            jobs.append(current)
        current = JobUsage()

    for raw in lines:
        line = raw.strip()
        if not line:
            continue
        if line.lower().startswith("job id") or line.lower().startswith("job_id"):
            flush()
            continue
        if "=" not in line:
            continue
        key, value = [part.strip() for part in line.split("=", 1)]
        if key in ("resources_used.walltime", "resources_used.walltime_total"):
            current.wall_hours = parse_duration(value) or current.wall_hours
        elif key in ("resources_used.cput", "resources_used.cpupercent", "resources_used.cpu_time"):
            current.cpu_hours = parse_duration(value) or current.cpu_hours
        elif key in ("Resource_List.ncpus", "resources_used.ncpus"):
            try:
                current.cores = int(value)
            except ValueError:
                pass
        elif key in ("Resource_List.nodect", "resources_used.nodect"):
            # leave nodect available if ncpus missing
            try:
                if current.cores is None:
                    current.cores = int(value)
            except ValueError:
                pass
    flush()
    return jobs


def parse_table(lines: Iterable[str]) -> List[JobUsage]:
    headers: List[str] = []
    jobs: List[JobUsage] = []

    for raw in lines:
        line = raw.rstrip("\n")
        if not line.strip():
            continue
        if not headers:
            if "job" in line.lower() and "user" in line.lower():
                headers = re.split(r"\s+", line.strip())
            continue
        parts = re.split(r"\s+", line.strip())
        if len(parts) < len(headers):
            continue
        row = dict(zip(headers, parts))

        wall_val = None
        cpu_val = None
        cores = None

        for key in row:
            lk = key.lower()
            if lk in ("walltime", "wall", "elapsed", "elap"):
                wall_val = row[key]
            elif lk in ("cput", "cpu", "cputime", "cpu_time", "cpuhrs", "corehrs", "corehours"):
                cpu_val = row[key]
            elif lk in ("ncpus", "cores", "nprocs", "tasks", "tks", "tksk", "tsks"):
                try:
                    cores = int(row[key])
                except ValueError:
                    pass
            elif lk in ("nodect", "nodes", "nds"):
                try:
                    if cores is None:
                        cores = int(row[key])
                except ValueError:
                    pass

        usage = JobUsage(
            wall_hours=parse_duration(wall_val) if wall_val else None,
            cpu_hours=parse_duration(cpu_val) if cpu_val else None,
            cores=cores,
        )
        if usage.wall_hours or usage.cpu_hours or usage.cores:
            jobs.append(usage)
    return jobs


def summarize(jobs: List[JobUsage]) -> Tuple[float, float]:
    total_wall = 0.0
    total_core = 0.0
    for job in jobs:
        if job.cpu_hours is not None:
            total_core += job.cpu_hours
        elif job.wall_hours is not None and job.cores is not None:
            total_core += job.wall_hours * job.cores
        if job.wall_hours is not None:
            total_wall += job.wall_hours
    return total_wall, total_core


def main() -> None:
    parser = argparse.ArgumentParser(description="Summarize qhist resource usage.")
    parser.add_argument("--input", type=Path, default=None, help="Read qhist output from a file.")
    parser.add_argument("--user", default=os.environ.get("USER", ""), help="User for qhist (default: $USER).")
    parser.add_argument("--qhist-cmd", default="qhist", help="qhist command (default: qhist).")
    parser.add_argument("--qhist-args", nargs="*", default=None, help="Extra args to pass to qhist.")
    parser.add_argument(
        "--assume-node-cores",
        type=int,
        default=None,
        help="If set, bill as Nodes * this value * Elap hours (e.g., 128).",
    )
    args = parser.parse_args()

    if args.input:
        content = args.input.read_text(errors="ignore").splitlines()
    else:
        cmd = [args.qhist_cmd]
        if args.user:
            cmd.extend(["-u", args.user])
        if args.qhist_args:
            cmd.extend(args.qhist_args)
        result = subprocess.run(cmd, check=False, capture_output=True, text=True)
        if result.returncode != 0:
            raise SystemExit(result.stderr.strip() or f"qhist failed with code {result.returncode}")
        content = result.stdout.splitlines()

    # Detect format
    keyvalue = any("resources_used." in line or "Resource_List." in line for line in content)
    if keyvalue:
        jobs = parse_keyvalue_blocks(content)
    else:
        jobs = parse_table(content)

    total_wall, total_core = summarize(jobs)
    billed_core_hours = None

    if args.assume_node_cores is not None:
        billed_core_hours = 0.0
        for raw in content:
            line = raw.strip()
            if not line or line.startswith("-") or line.lower().startswith("job id"):
                continue
            parts = re.split(r"\s+", line)
            if len(parts) < 10:
                continue
            try:
                nodes = int(parts[3])
            except ValueError:
                continue
            elap = parts[-1]
            elap_hours = parse_duration(elap)
            if elap_hours is None:
                try:
                    elap_hours = float(elap)
                except ValueError:
                    elap_hours = 0.0
            billed_core_hours += nodes * args.assume_node_cores * elap_hours
    print(f"Jobs parsed: {len(jobs)}")
    print(f"Total wall hours: {total_wall:,.2f}")
    print(f"Total core hours (estimated): {total_core:,.2f}")
    if billed_core_hours is not None:
        print(
            f"Total billed core hours (nodes*{args.assume_node_cores}*elap): "
            f"{billed_core_hours:,.2f}"
        )


if __name__ == "__main__":
    main()
