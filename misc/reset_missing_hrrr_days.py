#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import yaml


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Remove WPS/WRF outputs for day indices with missing HRRR files so stages rerun."
        )
    )
    parser.add_argument(
        "--indices-file",
        type=Path,
        default=Path("runs_hrrr_missing_indices.txt"),
        help="File with day indices to reset (default: runs_hrrr_missing_indices.txt).",
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
        "--execute",
        action="store_true",
        help="Perform deletions (default is dry-run).",
    )
    parser.add_argument(
        "--remove-ungrib",
        action="store_true",
        help="Remove ungrib outputs (default: True).",
    )
    parser.add_argument(
        "--remove-metgrid",
        action="store_true",
        help="Remove metgrid outputs (default: True).",
    )
    parser.add_argument(
        "--remove-real",
        action="store_true",
        help="Remove real outputs (wrfinput/wrfbdy) (default: True).",
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


def resolve_run_dirs(cfg: Dict[str, object], sim_start: str) -> Tuple[Path, Path]:
    wps_run_dir_parent = Path(cfg["wps_run_dir"])
    wrf_run_dir_parent = Path(cfg["wrf_run_dir"])
    exp_name = cfg.get("exp_name")
    exp_wrf_only = bool(cfg.get("exp_wrf_only", False))

    if exp_name:
        if exp_wrf_only:
            wps_run_dir = wps_run_dir_parent / sim_start
        else:
            wps_run_dir = wps_run_dir_parent / sim_start / str(exp_name)
        wrf_run_dir = wrf_run_dir_parent / sim_start / str(exp_name)
    else:
        wps_run_dir = wps_run_dir_parent / sim_start
        wrf_run_dir = wrf_run_dir_parent / sim_start

    return wps_run_dir, wrf_run_dir


def remove_path(path: Path, execute: bool) -> None:
    if not path.exists() and not path.is_symlink():
        return
    if execute:
        if path.is_symlink() or path.is_file():
            path.unlink()
        else:
            shutil.rmtree(path)
    else:
        print(f"[DRY-RUN] remove {path}")


def remove_glob(parent: Path, pattern: str, execute: bool) -> None:
    if not parent.exists():
        return
    for path in parent.glob(pattern):
        remove_path(path, execute)


def main() -> None:
    args = parse_args()

    # Defaults: remove all three unless user explicitly toggles any flags.
    if not (args.remove_ungrib or args.remove_metgrid or args.remove_real):
        args.remove_ungrib = True
        args.remove_metgrid = True
        args.remove_real = True

    rows = load_rows(args.runs_file)
    grouped = group_rows_by_date(rows)
    if not grouped:
        raise SystemExit("No runs found in CSV.")

    if not args.indices_file.exists():
        raise SystemExit(f"Indices file not found: {args.indices_file}")
    indices = [int(x) for x in args.indices_file.read_text().split() if x.strip()]

    config_cache: Dict[str, Dict[str, object]] = {}
    for day_index in indices:
        if day_index < 1 or day_index > len(grouped):
            print(f"[WARN] day-index {day_index} out of range; skipping")
            continue
        date_str, day_rows = grouped[day_index - 1]
        sim_start = compute_sim_start(date_str)
        print(f"[DAY] {date_str} (day-index {day_index}) sim_start={sim_start}")

        for row in day_rows:
            fire_id = row["fire_id"]
            if fire_id not in config_cache:
                config_cache[fire_id] = load_config(args.config_root, fire_id)
            cfg = config_cache[fire_id]
            wps_run_dir, wrf_run_dir = resolve_run_dirs(cfg, sim_start)

            if args.remove_ungrib:
                remove_path(wps_run_dir / "ungrib", execute=args.execute)
                remove_glob(wps_run_dir, "ungrib_*", execute=args.execute)

            if args.remove_metgrid:
                remove_path(wps_run_dir / "metgrid", execute=args.execute)

            if args.remove_real:
                remove_glob(wrf_run_dir, "wrfinput_d0*", execute=args.execute)
                remove_glob(wrf_run_dir, "wrfbdy_d0*", execute=args.execute)

    if not args.execute:
        print("Dry-run only. Re-run with --execute to delete.")


if __name__ == "__main__":
    main()
