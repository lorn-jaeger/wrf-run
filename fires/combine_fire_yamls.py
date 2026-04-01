#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Iterable, Iterator, Tuple

import yaml


def _iter_fire_files(fires_dir: Path) -> Iterator[Path]:
    """Yield fire YAML files sorted by name."""
    for path in sorted(fires_dir.glob("us_fire_*.yml")):
        if path.is_file():
            yield path


def _iter_fire_entries(
    fire_files: Iterable[Path], padding_days: int
) -> Iterator[Tuple[str, date, float, float]]:
    """Yield (fire_id, day, lat, lon) with requested padding."""
    padding = timedelta(days=padding_days)
    for path in fire_files:
        with path.open("r") as fh:
            content = yaml.safe_load(fh) or {}

        for fire_id, attrs in sorted(content.items()):
            if not fire_id.startswith("fire_"):
                continue

            try:
                lat = float(attrs["latitude"])
                lon = float(attrs["longitude"])
                start = datetime.strptime(str(attrs["start"]), "%Y-%m-%d").date()
                end = datetime.strptime(str(attrs["end"]), "%Y-%m-%d").date()
            except KeyError as exc:
                raise ValueError(f"Missing field {exc.args[0]} for {fire_id} in {path}") from exc

            current = start - padding
            last_day = end + padding
            while current <= last_day:
                yield fire_id, current, lat, lon
                current += timedelta(days=1)


def build_fire_csv(fires_dir: Path, output_path: Path, padding_days: int) -> None:
    fire_files = list(_iter_fire_files(fires_dir))
    if not fire_files:
        raise SystemExit(f"No us_fire_*.yml files found in {fires_dir}")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", newline="") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["date", "latitude", "longitude", "fire_id"])
        for fire_id, day, lat, lon in _iter_fire_entries(fire_files, padding_days):
            writer.writerow([day.isoformat(), lat, lon, fire_id])


def parse_args() -> argparse.Namespace:
    default_dir = Path(__file__).resolve().parent
    default_output = default_dir / "output.csv"

    parser = argparse.ArgumentParser(
        description="Combine us_fire YAML files into a per-day CSV with padding."
    )
    parser.add_argument(
        "--fires-dir",
        type=Path,
        default=default_dir,
        help="Directory containing us_fire_*.yml files (default: script directory)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=default_output,
        help="CSV file to write (default: fires/output.csv)",
    )
    parser.add_argument(
        "--padding-days",
        type=int,
        default=4,
        help="Number of days to pad on either side of each fire (default: 4)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    build_fire_csv(args.fires_dir, args.output, args.padding_days)


if __name__ == "__main__":
    main()
