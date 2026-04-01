#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import yaml


@dataclass
class DayStat:
    index: int
    date: str
    total: int
    complete: int

    @property
    def ratio(self) -> float:
        return self.complete / self.total if self.total else 0.0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Report day ranges that are not really run yet (low completion), with "
            "special attention to high-volume days."
        )
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
        help="Minimum wrfout file count to treat a run as complete (default: 12).",
    )
    parser.add_argument(
        "--low-ratio",
        type=float,
        default=0.2,
        help="Completion ratio below which a day is considered not really run (default: 0.2).",
    )
    parser.add_argument(
        "--high-volume-quantile",
        type=float,
        default=0.75,
        help="Quantile for high-volume day cutoff (default: 0.75).",
    )
    parser.add_argument(
        "--out-file",
        type=Path,
        default=Path("runs_unrun_areas_report.txt"),
        help="Output report file path.",
    )
    parser.add_argument(
        "--indices-out",
        type=Path,
        default=Path("runs_unrun_areas_indices.txt"),
        help="Output file with day indices (low completion).",
    )
    parser.add_argument(
        "--indices-high-volume-out",
        type=Path,
        default=Path("runs_unrun_areas_high_volume_indices.txt"),
        help="Output file with day indices (low completion + high volume).",
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


def resolve_wrf_dir(cfg: Dict[str, object], sim_start: str) -> Path:
    wrf_run_dir_parent = Path(cfg["wrf_run_dir"])
    exp_name = cfg.get("exp_name")
    exp_wrf_only = bool(cfg.get("exp_wrf_only", False))
    if exp_name:
        if exp_wrf_only:
            return wrf_run_dir_parent / sim_start
        return wrf_run_dir_parent / sim_start / str(exp_name)
    return wrf_run_dir_parent / sim_start


def count_wrfouts(run_dir: Path) -> int:
    if not run_dir.exists():
        return 0
    return sum(1 for _ in run_dir.glob("wrfout_d0*"))


def quantile(values: List[int], q: float) -> int:
    if not values:
        return 0
    values_sorted = sorted(values)
    idx = int(round((len(values_sorted) - 1) * q))
    return values_sorted[idx]


def build_ranges(days: List[DayStat], predicate) -> List[List[DayStat]]:
    ranges: List[List[DayStat]] = []
    current: List[DayStat] = []
    for day in days:
        if predicate(day):
            if current and (current[-1].index + 1 != day.index):
                ranges.append(current)
                current = []
            current.append(day)
        else:
            if current:
                ranges.append(current)
                current = []
    if current:
        ranges.append(current)
    return ranges


def main() -> None:
    args = parse_args()
    rows = load_rows(args.runs_file)
    grouped = group_rows_by_date(rows)
    if not grouped:
        raise SystemExit("No runs found in CSV.")

    config_cache: Dict[str, Dict[str, object]] = {}
    day_stats: List[DayStat] = []
    for idx, (date_str, day_rows) in enumerate(grouped, start=1):
        sim_start = compute_sim_start(date_str)
        total = len(day_rows)
        complete = 0
        for row in day_rows:
            fire_id = row["fire_id"]
            if fire_id not in config_cache:
                config_cache[fire_id] = load_config(args.config_root, fire_id)
            cfg = config_cache[fire_id]
            wrf_dir = resolve_wrf_dir(cfg, sim_start)
            if count_wrfouts(wrf_dir) >= args.wrfout_threshold:
                complete += 1
        day_stats.append(DayStat(index=idx, date=date_str, total=total, complete=complete))

    totals = [d.total for d in day_stats]
    hv_cutoff = quantile(totals, args.high_volume_quantile)

    low_ranges = build_ranges(day_stats, lambda d: d.ratio < args.low_ratio)
    low_high_ranges = build_ranges(
        day_stats, lambda d: d.ratio < args.low_ratio and d.total >= hv_cutoff
    )
    low_indices = [str(d.index) for d in day_stats if d.ratio < args.low_ratio]
    low_high_indices = [
        str(d.index)
        for d in day_stats
        if d.ratio < args.low_ratio and d.total >= hv_cutoff
    ]

    out_lines: List[str] = []
    out_lines.append(f"Low completion threshold: ratio < {args.low_ratio}")
    out_lines.append(f"High-volume cutoff (quantile {args.high_volume_quantile}): >= {hv_cutoff} runs/day")
    out_lines.append("")

    def render_ranges(title: str, ranges: List[List[DayStat]]) -> None:
        out_lines.append(title)
        if not ranges:
            out_lines.append("  (none)")
            out_lines.append("")
            return
        for r in ranges:
            start = r[0]
            end = r[-1]
            total_runs = sum(d.total for d in r)
            total_done = sum(d.complete for d in r)
            ratio = total_done / total_runs if total_runs else 0.0
            out_lines.append(
                f"  day-index {start.index}-{end.index} "
                f"({start.date} to {end.date}) "
                f"runs={total_runs} complete={total_done} ratio={ratio:.2f}"
            )
        out_lines.append("")

    render_ranges("Low completion ranges:", low_ranges)
    render_ranges("Low completion + high volume ranges:", low_high_ranges)

    out_lines.append("Per-day stats (day_index,date,total,complete,ratio):")
    for d in day_stats:
        out_lines.append(f"{d.index},{d.date},{d.total},{d.complete},{d.ratio:.2f}")

    args.out_file.write_text("\n".join(out_lines))
    args.indices_out.write_text("\n".join(low_indices) + ("\n" if low_indices else ""))
    args.indices_high_volume_out.write_text(
        "\n".join(low_high_indices) + ("\n" if low_high_indices else "")
    )
    print(f"Wrote report to {args.out_file}")
    print(f"Wrote low completion indices to {args.indices_out}")
    print(f"Wrote low completion high-volume indices to {args.indices_high_volume_out}")


if __name__ == "__main__":
    main()
