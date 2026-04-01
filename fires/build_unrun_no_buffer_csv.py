#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import yaml


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create a CSV of per-day fire runs without buffer days, removing fires that "
            "already have WRF outputs."
        )
    )
    parser.add_argument(
        "--runs-file",
        type=Path,
        default=Path("fires/output_budget.csv"),
        help="Input CSV with fire_id/latitude/longitude (default: fires/output_budget.csv).",
    )
    parser.add_argument(
        "--fires-glob",
        type=str,
        default="fires/us_fire_*_1e7.yml",
        help="Glob for fire YAML files (default: fires/us_fire_*_1e7.yml).",
    )
    parser.add_argument(
        "--workflow-root",
        type=Path,
        default=Path("/glade/derecho/scratch/ljaeger/workflow"),
        help="Root directory containing fire_*/wrf outputs.",
    )
    parser.add_argument(
        "--wrfout-threshold",
        type=int,
        default=12,
        help="Minimum count of wrfout files to consider a fire already run (default: 12).",
    )
    parser.add_argument(
        "--output-csv",
        type=Path,
        default=Path("fires/output_budget_unrun.csv"),
        help="Path to write filtered CSV (default: fires/output_budget_unrun.csv).",
    )
    return parser.parse_args()


def _coerce_date(value: object) -> date:
    if isinstance(value, date):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        return datetime.fromisoformat(value).date()
    raise ValueError(f"Unsupported date value: {value!r}")


def load_fire_dates(paths: Iterable[Path]) -> Dict[str, Tuple[date, date]]:
    fire_dates: Dict[str, Tuple[date, date]] = {}
    for path in paths:
        with path.open() as handle:
            data = yaml.safe_load(handle) or {}
        if not isinstance(data, dict):
            continue
        for key, value in data.items():
            if not isinstance(key, str) or not key.startswith("fire_"):
                continue
            if not isinstance(value, dict):
                continue
            if "start" not in value or "end" not in value:
                continue
            fire_dates[key] = (_coerce_date(value["start"]), _coerce_date(value["end"]))
    return fire_dates


def load_fire_order_and_coords(csv_path: Path) -> Tuple[List[str], Dict[str, Tuple[str, str]]]:
    if not csv_path.exists():
        raise SystemExit(f"Runs file not found: {csv_path}")
    order: List[str] = []
    coords: Dict[str, Tuple[str, str]] = {}
    with csv_path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames or "fire_id" not in reader.fieldnames:
            raise SystemExit(f"{csv_path} must include a fire_id column.")
        for row in reader:
            fire_id = row["fire_id"]
            if fire_id in coords:
                continue
            order.append(fire_id)
            coords[fire_id] = (row.get("latitude", ""), row.get("longitude", ""))
    return order, coords


def fire_has_outputs(workflow_root: Path, fire_id: str, threshold: int) -> Tuple[bool, int]:
    wrf_dir = workflow_root / fire_id / "wrf"
    if not wrf_dir.exists():
        return False, 0
    count = 0
    for path in wrf_dir.rglob("wrfout*"):
        if path.is_file():
            count += 1
            if count > threshold:
                return True, count
    return False, count


def build_rows(
    fire_id: str, start: date, end: date, latitude: str, longitude: str
) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    current = start
    while current <= end:
        rows.append(
            {
                "date": current.isoformat(),
                "latitude": latitude,
                "longitude": longitude,
                "fire_id": fire_id,
            }
        )
        current += timedelta(days=1)
    return rows


def main() -> None:
    args = parse_args()
    fire_order, coords = load_fire_order_and_coords(args.runs_file)
    fire_dates = load_fire_dates(Path().glob(args.fires_glob))

    missing_dates = [fire_id for fire_id in fire_order if fire_id not in fire_dates]
    if missing_dates:
        print(f"Warning: {len(missing_dates)} fires missing start/end dates in YAMLs.")

    kept_rows: List[Dict[str, str]] = []
    removed: List[str] = []

    for fire_id in fire_order:
        if fire_id not in fire_dates:
            continue
        has_outputs, count = fire_has_outputs(
            args.workflow_root, fire_id, args.wrfout_threshold
        )
        if has_outputs:
            removed.append(fire_id)
            continue
        start, end = fire_dates[fire_id]
        lat, lon = coords.get(fire_id, ("", ""))
        kept_rows.extend(build_rows(fire_id, start, end, lat, lon))

    args.output_csv.parent.mkdir(parents=True, exist_ok=True)
    with args.output_csv.open("w", newline="") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=["date", "latitude", "longitude", "fire_id"]
        )
        writer.writeheader()
        writer.writerows(kept_rows)

    print(f"Wrote {len(kept_rows)} rows to {args.output_csv}")
    print(f"Removed {len(removed)} fires with > {args.wrfout_threshold} wrfout files.")


if __name__ == "__main__":
    main()
