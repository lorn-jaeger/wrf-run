#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path
from typing import Iterable

import f90nml


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Increment or decrement EPSSM in a fire-specific WRF namelist."
    )
    parser.add_argument("--fire-id", required=True, help="Fire identifier (e.g., fire_21458798).")
    parser.add_argument(
        "--date",
        required=True,
        help="Start date expected in the namelist (YYYYMMDD_HH or YYYY-MM-DD_HH[:MM[:SS]]).",
    )
    parser.add_argument(
        "--direction",
        choices=("increase", "decrease"),
        default="increase",
        help="Whether to increase or decrease EPSSM by 0.1 (default: increase).",
    )
    parser.add_argument(
        "--wrf-root",
        type=Path,
        default=Path("configs/built/wrf"),
        help="Root directory containing fire-specific WRF namelists.",
    )
    parser.add_argument(
        "--namelist-name",
        default="namelist.input.hrrr.hybr",
        help="File name of the namelist to modify (default: namelist.input.hrrr.hybr).",
    )
    return parser.parse_args()


def normalize_date(date_str: str) -> datetime:
    formats = ["%Y%m%d_%H", "%Y-%m-%d_%H:%M:%S", "%Y-%m-%d_%H", "%Y%m%d"]
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    raise SystemExit(f"Unable to parse --date '{date_str}'.")


def extract_start_datetime(time_control: dict) -> datetime:
    if "start_date" in time_control:
        date_field = time_control["start_date"]
        first_entry = date_field[0] if isinstance(date_field, Iterable) and not isinstance(date_field, str) else date_field
        return datetime.strptime(str(first_entry), "%Y-%m-%d_%H:%M:%S")

    def first_value(key: str, default: int = 0) -> int:
        value = time_control.get(key, default)
        if isinstance(value, Iterable) and not isinstance(value, str):
            value = value[0]
        return int(value)

    year = first_value("start_year")
    month = first_value("start_month")
    day = first_value("start_day")
    hour = first_value("start_hour")
    minute = first_value("start_minute", 0)
    second = first_value("start_second", 0)
    return datetime(year, month, day, hour, minute, second)


def clamp(value: float) -> float:
    return max(0.1, min(0.7, round(value, 10)))


def ensure_list(value) -> list[float]:
    if isinstance(value, (list, tuple)):
        return [float(v) for v in value]
    return [float(value)]


def main() -> None:
    args = parse_args()
    expected_date = normalize_date(args.date)
    namelist_path = args.wrf_root / args.fire_id / args.namelist_name
    if not namelist_path.exists():
        raise SystemExit(f"Namelist not found: {namelist_path}")

    namelist = f90nml.read(namelist_path)
    time_control = namelist.get("time_control", {})
    actual_start = extract_start_datetime(time_control)
    if actual_start != expected_date:
        raise SystemExit(
            f"Start date mismatch. Expected {expected_date:%Y-%m-%d_%H:%M:%S}, "
            f"found {actual_start:%Y-%m-%d_%H:%M:%S}."
        )

    dynamics = namelist.setdefault("dynamics", {})
    current_values = dynamics.get("epssm", [0.1, 0.1, 0.1])
    current_list = ensure_list(current_values)
    step = 0.1 if args.direction == "increase" else -0.1
    new_value = clamp(current_list[0] + step)
    updated = [new_value for _ in current_list] or [new_value]
    dynamics["epssm"] = updated
    namelist["dynamics"] = dynamics

    with namelist_path.open("w") as handle:
        namelist.write(handle)

    print(
        f"EPSSM for {args.fire_id} updated to {new_value:.1f} "
        f"in {namelist_path} (direction: {args.direction})."
    )


if __name__ == "__main__":
    main()
