#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import logging
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional
from urllib.error import HTTPError

import wget


log = logging.getLogger("download_hrrr_day")


AWS_BASE = "https://noaa-hrrr-bdp-pds.s3.amazonaws.com"
GC_BASE = "https://storage.googleapis.com/high-resolution-rapid-refresh"
AWS_VARIANTS = {"AWS", "aws"}
GC_VARIANTS = {
    "GoogleCloud",
    "googlecloud",
    "Google_Cloud",
    "google_cloud",
    "GC",
    "gc",
    "GCloud",
    "gcloud",
}


@dataclass
class FileRecord:
    tag: str
    valid_time: dt.datetime
    destination: Path
    label: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download HRRR analysis files for a full UTC day and fill any gaps via interpolation."
    )
    parser.add_argument(
        "--date",
        required=True,
        help="UTC date to download (format: YYYYMMDD).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("hrrr_downloads"),
        help="Destination root directory (default: ./hrrr_downloads).",
    )
    parser.add_argument(
        "--native-grid",
        action="store_true",
        help="Also download the HRRR native-grid files (wrfnatf00).",
    )
    parser.add_argument(
        "--source",
        default="AWS",
        help="Data source repository (AWS or GoogleCloud variants).",
    )
    parser.add_argument(
        "--start-hour",
        type=int,
        default=0,
        help="First UTC hour to download (0-23, default: 0).",
    )
    parser.add_argument(
        "--end-hour",
        type=int,
        default=23,
        help="Last UTC hour to download (0-23, inclusive, default: 23).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Python logging level (default: INFO).",
    )
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> dt.datetime:
    try:
        base_date = dt.datetime.strptime(args.date, "%Y%m%d")
    except ValueError as exc:
        raise SystemExit(f"Invalid --date '{args.date}': {exc}")

    if not (0 <= args.start_hour <= 23 and 0 <= args.end_hour <= 23):
        raise SystemExit("--start-hour and --end-hour must be within 0-23.")
    if args.start_hour > args.end_hour:
        raise SystemExit("--start-hour must be less than or equal to --end-hour.")

    source_upper = args.source
    if source_upper not in AWS_VARIANTS and source_upper not in GC_VARIANTS:
        raise SystemExit("Unknown --source. Expected AWS or GoogleCloud variants.")

    args.source = source_upper
    return base_date


def resolve_base_url(date_str: str, source: str) -> str:
    if source in AWS_VARIANTS:
        return f"{AWS_BASE}/hrrr.{date_str}/conus"
    return f"{GC_BASE}/hrrr.{date_str}/conus"


def download_day(base_date: dt.datetime, args: argparse.Namespace) -> None:
    available: Dict[str, Dict[dt.datetime, Path]] = {"wrfprsf": {}, "wrfnatf": {}}
    missing: List[FileRecord] = []

    for hour in range(args.start_hour, args.end_hour + 1):
        valid_time = base_date + dt.timedelta(hours=hour)
        valid_date = valid_time.strftime("%Y%m%d")
        valid_hour = valid_time.strftime("%H")
        host_dir = resolve_base_url(valid_date, args.source)
        out_dir = args.output_dir / f"hrrr.{valid_date}" / "conus"
        out_dir.mkdir(parents=True, exist_ok=True)

        if args.native_grid:
            fname_nat = f"hrrr.t{valid_hour}z.wrfnatf00.grib2"
            url_nat = f"{host_dir}/{fname_nat}"
            handle_download(
                url=url_nat,
                dest=out_dir / fname_nat,
                tag="wrfnatf",
                valid_time=valid_time,
                label=f"{valid_date}_{valid_hour} nat",
                available=available,
                missing=missing,
            )

        fname_prs = f"hrrr.t{valid_hour}z.wrfprsf00.grib2"
        url_prs = f"{host_dir}/{fname_prs}"
        handle_download(
            url=url_prs,
            dest=out_dir / fname_prs,
            tag="wrfprsf",
            valid_time=valid_time,
            label=f"{valid_date}_{valid_hour} prs",
            available=available,
            missing=missing,
        )

    interpolate_missing(available, missing)


def handle_download(
    url: str,
    dest: Path,
    tag: str,
    valid_time: dt.datetime,
    label: str,
    available: Dict[str, Dict[dt.datetime, Path]],
    missing: List[FileRecord],
) -> None:
    if dest.exists():
        log.info("File %s already exists, skipping download.", dest.name)
        available.setdefault(tag, {})[valid_time] = dest
        return

    log.info("Downloading %s", url)
    try:
        wget.download(url, out=str(dest))
        log.info("")
        available.setdefault(tag, {})[valid_time] = dest
    except HTTPError as exc:
        log.warning("Missing %s (%s). Will attempt interpolation. (%s)", dest.name, label, exc)
        missing.append(FileRecord(tag=tag, valid_time=valid_time, destination=dest, label=label))


def interpolate_missing(
    available: Dict[str, Dict[dt.datetime, Path]], missing: List[FileRecord]
) -> None:
    if not missing:
        return

    log.info("Attempting to interpolate %d missing files.", len(missing))
    failures: List[FileRecord] = []

    for record in missing:
        if interpolate_single(available, record):
            available.setdefault(record.tag, {})[record.valid_time] = record.destination
        else:
            failures.append(record)

    if failures:
        for record in failures:
            log.error("Unable to synthesize %s (%s).", record.destination, record.label)
        raise SystemExit("Interpolation failed for some files. See log for details.")


def interpolate_single(
    available: Dict[str, Dict[dt.datetime, Path]], record: FileRecord
) -> bool:
    tag_map = available.get(record.tag, {})
    if not tag_map:
        log.error("No existing %s files available for interpolation.", record.tag)
        return False

    sorted_times = sorted(tag_map.keys())
    prev_time = max((t for t in sorted_times if t < record.valid_time), default=None)
    next_time = min((t for t in sorted_times if t > record.valid_time), default=None)
    record.destination.parent.mkdir(parents=True, exist_ok=True)

    if prev_time is None and next_time is None:
        return False
    if prev_time is None:
        shutil.copy2(tag_map[next_time], record.destination)
        log.warning(
            "Copied %s to fill missing %s (no earlier neighbor).",
            tag_map[next_time].name,
            record.destination.name,
        )
        return True
    if next_time is None:
        shutil.copy2(tag_map[prev_time], record.destination)
        log.warning(
            "Copied %s to fill missing %s (no later neighbor).",
            tag_map[prev_time].name,
            record.destination.name,
        )
        return True

    prev_file = tag_map[prev_time]
    next_file = tag_map[next_time]
    span = (next_time - prev_time).total_seconds()
    if span <= 0:
        shutil.copy2(prev_file, record.destination)
        log.warning("Non-increasing timestamps detected. Copied %s.", prev_file.name)
        return True

    weight_prev = (next_time - record.valid_time).total_seconds() / span
    weight_next = 1.0 - weight_prev
    log.info(
        "Interpolating %s between %s and %s (weights %.2f / %.2f).",
        record.destination.name,
        prev_file.name,
        next_file.name,
        weight_prev,
        weight_next,
    )

    if run_wgrib2(prev_file, next_file, record.destination, weight_prev, weight_next):
        return True

    fallback = prev_file if weight_prev >= weight_next else next_file
    shutil.copy2(fallback, record.destination)
    log.warning(
        "wgrib2 interpolation not available. Copied %s to approximate %s.",
        fallback.name,
        record.destination.name,
    )
    return True


def run_wgrib2(
    prev_file: Path, next_file: Path, target_file: Path, weight_prev: float, weight_next: float
) -> bool:
    wgrib2_exe = shutil.which("wgrib2")
    if not wgrib2_exe:
        log.debug("wgrib2 binary not found on PATH.")
        return False

    cmd = [
        wgrib2_exe,
        str(prev_file),
        "-rpn",
        f"{weight_prev:.6f} *",
        "-import_grib",
        str(next_file),
        "-rpn",
        f"{weight_next:.6f} * +",
        "-grib",
        str(target_file),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        log.warning("wgrib2 interpolation failed: %s", result.stderr.strip())
        return False
    return True


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="download_hrrr_day: %(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    base_date = validate_args(args)
    download_day(base_date, args)
    log.info("Download complete. Files written under %s", args.output_dir.resolve())


if __name__ == "__main__":
    main()
