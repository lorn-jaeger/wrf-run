#!/usr/bin/env python3
from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Iterable, Set, Tuple


def parse_args() -> argparse.Namespace:
    user = Path("~").expanduser().name
    parser = argparse.ArgumentParser(
        description=(
            "Watch scratch usage and delete ungrib/metgrid/wrfrst/HRRR outputs "
            "once a threshold is reached."
        )
    )
    parser.add_argument(
        "--scratch-path",
        type=Path,
        default=Path(f"/glade/derecho/scratch/{user}"),
        help="Scratch path to monitor (default: /glade/derecho/scratch/$USER).",
    )
    parser.add_argument(
        "--threshold-tb",
        type=float,
        default=27.0,
        help="Trigger cleanup when used TB >= this value (default: 27).",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=300,
        help="Seconds between checks (default: 300).",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Exit after the first cleanup trigger (default).",
    )
    parser.add_argument(
        "--loop",
        action="store_true",
        help="Keep watching after cleanup.",
    )
    parser.add_argument(
        "--workflow-root",
        type=Path,
        default=Path(f"/glade/derecho/scratch/{user}/workflow"),
        help="Workflow root containing fire_* runs.",
    )
    parser.add_argument(
        "--grib-dir",
        type=Path,
        default=Path(f"/glade/derecho/scratch/{user}/data/hrrr"),
        help="HRRR grib directory.",
    )
    parser.add_argument(
        "--skip-ungrib",
        action="store_true",
        help="Do not delete ungrib outputs.",
    )
    parser.add_argument(
        "--skip-metgrid",
        action="store_true",
        help="Do not delete metgrid outputs.",
    )
    parser.add_argument(
        "--skip-wrfrst",
        action="store_true",
        help="Do not delete wrfrst_d0* files.",
    )
    parser.add_argument(
        "--skip-hrrr",
        action="store_true",
        help="Do not delete HRRR grib day directories.",
    )
    parser.add_argument(
        "--force-immutable",
        action="store_true",
        help="Attempt to remove immutable flag (chattr -i) before deletion.",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Perform deletions (default is dry-run).",
    )
    return parser.parse_args()


def parse_size_to_bytes(value: str, unit: str) -> int:
    unit_map = {
        "B": 1,
        "KiB": 1024**1,
        "MiB": 1024**2,
        "GiB": 1024**3,
        "TiB": 1024**4,
        "PiB": 1024**5,
    }
    if unit not in unit_map:
        raise ValueError(f"Unknown size unit: {unit}")
    return int(float(value) * unit_map[unit])


def get_usage_bytes(path: Path) -> Tuple[int, int]:
    # Prefer gladequota when available (matches GLADE policy reporting).
    if shutil.which("gladequota"):
        result = subprocess.run(
            ["gladequota"], capture_output=True, text=True, check=True
        )
        target = str(path)
        for line in result.stdout.splitlines():
            if not line.startswith(target):
                continue
            parts = line.split()
            if len(parts) < 5:
                break
            used_val, used_unit = parts[1], parts[2]
            quota_val, quota_unit = parts[3], parts[4]
            used = parse_size_to_bytes(used_val, used_unit)
            total = parse_size_to_bytes(quota_val, quota_unit)
            return used, total
        raise RuntimeError(f"Could not find {target} in gladequota output")

    # Fallback to df when gladequota is not available.
    result = subprocess.run(
        ["df", "-PB1", str(path)], capture_output=True, text=True, check=True
    )
    lines = result.stdout.strip().splitlines()
    if len(lines) < 2:
        raise RuntimeError(f"Unexpected df output for {path}")
    parts = lines[1].split()
    if len(parts) < 3:
        raise RuntimeError(f"Unexpected df output for {path}: {lines[1]}")
    total = int(parts[1])
    used = int(parts[2])
    return used, total


def remove_path(path: Path, execute: bool, force_immutable: bool) -> None:
    if not path.exists() and not path.is_symlink():
        return
    if not execute:
        print(f"[DRY-RUN] remove {path}")
        return
    if force_immutable and shutil.which("chattr"):
        try:
            if path.is_dir():
                subprocess.run(["chattr", "-R", "-i", str(path)], check=False)
            else:
                subprocess.run(["chattr", "-i", str(path)], check=False)
        except Exception:
            pass
    if path.is_symlink() or path.is_file():
        path.unlink(missing_ok=True)
    else:
        for child in path.glob("*"):
            remove_path(child, execute=True, force_immutable=force_immutable)
        path.rmdir()


def iter_ungrib(workflow_root: Path) -> Iterable[Path]:
    if not workflow_root.exists():
        return []
    for path in workflow_root.rglob("ungrib*"):
        if path.name.startswith("ungrib"):
            yield path


def iter_metgrid(workflow_root: Path) -> Iterable[Path]:
    if not workflow_root.exists():
        return []
    for path in workflow_root.rglob("metgrid"):
        if path.is_dir():
            yield path
    for path in workflow_root.rglob("met_em.d0*"):
        yield path


def iter_wrfrst(workflow_root: Path) -> Iterable[Path]:
    if not workflow_root.exists():
        return []
    for path in workflow_root.rglob("wrfrst_d0*"):
        yield path


def iter_hrrr(grib_dir: Path) -> Iterable[Path]:
    if not grib_dir.exists():
        return []
    for path in grib_dir.iterdir():
        if path.name.startswith("hrrr."):
            yield path


def build_targets(args: argparse.Namespace) -> Set[Path]:
    targets: Set[Path] = set()
    if not args.skip_ungrib:
        targets.update(iter_ungrib(args.workflow_root))
    if not args.skip_metgrid:
        targets.update(iter_metgrid(args.workflow_root))
    if not args.skip_wrfrst:
        targets.update(iter_wrfrst(args.workflow_root))
    if not args.skip_hrrr:
        targets.update(iter_hrrr(args.grib_dir))
    return targets


def cleanup(args: argparse.Namespace) -> None:
    targets = build_targets(args)
    if not targets:
        print("[INFO] No cleanup targets found.")
        return
    print("[INFO] Cleanup targets:")
    print(f"  workflow_root={args.workflow_root}")
    print(f"  grib_dir={args.grib_dir}")
    print(
        "  flags="
        f"ungrib={not args.skip_ungrib}, metgrid={not args.skip_metgrid}, "
        f"wrfrst={not args.skip_wrfrst}, hrrr={not args.skip_hrrr}"
    )
    print(f"  total_paths={len(targets)}")
    for path in sorted(targets):
        remove_path(path, execute=args.execute, force_immutable=args.force_immutable)
    if not args.execute:
        print("Dry-run only. Re-run with --execute to delete.")


def main() -> None:
    args = parse_args()
    if args.once and args.loop:
        raise SystemExit("Use only one of --once or --loop.")
    if not args.once and not args.loop:
        args.once = True

    threshold_bytes = int(args.threshold_tb * 1024**4)
    print(
        f"[INFO] Watching {args.scratch_path} (threshold {args.threshold_tb} TB). "
        f"Interval={args.interval}s"
    )

    while True:
        used, total = get_usage_bytes(args.scratch_path)
        used_tb = used / 1024**4
        total_tb = total / 1024**4
        print(f"[INFO] Scratch usage: {used_tb:.2f} TB / {total_tb:.2f} TB")
        if used >= threshold_bytes:
            print("[WARN] Threshold reached. Running cleanup.")
            cleanup(args)
            if args.once:
                return
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
