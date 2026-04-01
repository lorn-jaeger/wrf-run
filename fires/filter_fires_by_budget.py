#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple


@dataclass
class FireRun:
    fire_id: str
    rows: List[List[str]]
    cost: float


def build_fire_runs(
    fire_rows: Dict[str, List[List[str]]],
    hours_per_day: float,
    cpu_cores: int,
    billing_multiplier: float,
) -> List[FireRun]:
    fire_runs: List[FireRun] = []
    for fire_id, rows in fire_rows.items():
        days = len(rows)
        cost = days * hours_per_day * cpu_cores * billing_multiplier
        fire_runs.append(FireRun(fire_id=fire_id, rows=rows, cost=cost))
    return fire_runs


def trim_to_budget(
    fire_runs: Sequence[FireRun], budget_hours: float, seed: Optional[int] = None
) -> Tuple[List[FireRun], List[FireRun], float]:
    rng = random.Random(seed)
    shuffled_runs = list(fire_runs)
    rng.shuffle(shuffled_runs)
    total_cost = sum(run.cost for run in shuffled_runs)

    removed = []
    keep_from = 0
    while total_cost > budget_hours and keep_from < len(shuffled_runs):
        removed_run = shuffled_runs[keep_from]
        removed.append(removed_run)
        total_cost -= removed_run.cost
        keep_from += 1

    kept_runs = shuffled_runs[keep_from:]
    return kept_runs, removed, total_cost


def write_filtered_rows(
    output_path: Path, header: List[str], rows: Sequence[List[str]], keep_ids: Sequence[str]
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    keep_set = set(keep_ids)
    with output_path.open("w", newline="") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(header)
        for row in rows:
            if row[-1] in keep_set:
                writer.writerow(row)


def parse_args() -> argparse.Namespace:
    default_csv = Path(__file__).resolve().parent / "output.csv"
    parser = argparse.ArgumentParser(
        description="Filter fire runs until the aggregate CPU-hour cost fits within budget."
    )
    parser.add_argument("--input", type=Path, default=default_csv, help="Fire CSV to read.")
    parser.add_argument(
        "--output",
        type=Path,
        default=default_csv.with_name("output_budget.csv"),
        help="Filtered CSV to write (default: fires/output_budget.csv).",
    )
    parser.add_argument(
        "--budget-hours",
        type=float,
        default=1_000_000,
        help="Total available CPU-hours.",
    )
    parser.add_argument(
        "--cpu-cores",
        type=int,
        default=128,
        help="Number of CPU cores per fire run.",
    )
    parser.add_argument(
        "--hours-per-day",
        type=float,
        default=1.5,
        help="Wall-clock hours required per fire day.",
    )
    parser.add_argument(
        "--billing-multiplier",
        type=float,
        default=0.7,
        help="Billing multiplier applied to core-hours.",
    )
    parser.add_argument(
        "--random-seed",
        type=int,
        default=None,
        help="Optional seed for random fire removal ordering.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not args.input.exists():
        raise SystemExit(f"Input CSV {args.input} does not exist.")

    with args.input.open("r", newline="") as csv_file:
        reader = csv.reader(csv_file)
        header = next(reader, None)
        if not header:
            raise SystemExit(f"{args.input} is empty.")
        rows = [row for row in reader if row]

    # Re-read rows grouped by fire for counting.
    fire_rows: Dict[str, List[List[str]]] = {}
    for row in rows:
        fire_rows.setdefault(row[-1], []).append(row)

    fire_runs = build_fire_runs(
        fire_rows,
        hours_per_day=args.hours_per_day,
        cpu_cores=args.cpu_cores,
        billing_multiplier=args.billing_multiplier,
    )

    kept_runs, removed_runs, remaining_hours = trim_to_budget(
        fire_runs, args.budget_hours, args.random_seed
    )

    print(f"Total budget hours: {args.budget_hours:,.0f}")
    print(f"Remaining hours after trimming: {remaining_hours:,.2f}")
    removed_ids = ", ".join(run.fire_id for run in removed_runs) or "none"
    print(f"Fires removed ({len(removed_runs)}): {removed_ids}")
    print(f"Fires kept ({len(kept_runs)}).")

    keep_ids = [run.fire_id for run in kept_runs]
    write_filtered_rows(args.output, header, rows, keep_ids)


if __name__ == "__main__":
    main()
