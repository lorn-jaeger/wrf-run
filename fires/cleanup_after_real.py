#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import yaml


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Clean ungrib/metgrid (and optionally HRRR) for runs where real is complete."
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
        "--delete-hrrr",
        action="store_true",
        help="Delete HRRR grib directories for days where all HRRR runs have real completed.",
    )
    parser.add_argument(
        "--keep-ungrib",
        action="store_true",
        help="Do not delete ungrib outputs (default deletes ungrib).",
    )
    parser.add_argument(
        "--keep-metgrid",
        action="store_true",
        help="Do not delete metgrid outputs (default deletes metgrid).",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Perform deletions (default is dry-run).",
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


def real_complete(run_dir: Path) -> bool:
    if not run_dir.exists():
        return False
    return any(run_dir.glob("wrfinput_d0*")) and any(run_dir.glob("wrfbdy_d0*"))


def remove_path(path: Path, execute: bool) -> None:
    if not path.exists() and not path.is_symlink():
        return
    if execute:
        try:
            if path.is_symlink() or path.is_file():
                path.unlink()
            else:
                for child in path.glob("*"):
                    remove_path(child, execute=True)
                path.rmdir()
        except OSError:
            print(f"[WARN] Failed to remove {path}")
    else:
        print(f"[DRY-RUN] remove {path}")


def main() -> None:
    args = parse_args()
    rows = load_rows(args.runs_file)
    grouped = group_rows_by_date(rows)
    if not grouped:
        raise SystemExit("No runs found in CSV.")

    config_cache: Dict[str, Dict[str, object]] = {}
    ungrib_paths: Dict[Path, None] = {}
    metgrid_paths: Dict[Path, None] = {}

    date_summary: Dict[str, Dict[str, object]] = {}
    for date_str, day_rows in grouped:
        sim_start = compute_sim_start(date_str)
        summary = {
            "max_sim_hrs": 0,
            "grib_dir": None,
            "any_hrrr": False,
            "all_hrrr_real_done": True,
        }

        for row in day_rows:
            fire_id = row["fire_id"]
            if fire_id not in config_cache:
                config_cache[fire_id] = load_config(args.config_root, fire_id)
            cfg = config_cache[fire_id]
            wps_run_dir, wrf_run_dir = resolve_run_dirs(cfg, sim_start)

            is_hrrr = str(cfg.get("icbc_model", "")).lower() == "hrrr"
            if is_hrrr:
                summary["any_hrrr"] = True
                if summary["grib_dir"] is None:
                    summary["grib_dir"] = Path(cfg.get("grib_dir", ""))

            sim_hrs = int(cfg.get("sim_hrs", 24))
            if sim_hrs > summary["max_sim_hrs"]:
                summary["max_sim_hrs"] = sim_hrs

            done = real_complete(wrf_run_dir)
            if is_hrrr and not done:
                summary["all_hrrr_real_done"] = False

            if done:
                if not args.keep_ungrib and wps_run_dir.exists():
                    for path in wps_run_dir.glob("ungrib*"):
                        ungrib_paths[path] = None
                if not args.keep_metgrid:
                    metgrid_dir = wps_run_dir / "metgrid"
                    if metgrid_dir.exists():
                        metgrid_paths[metgrid_dir] = None

        date_summary[date_str] = summary

    if not args.keep_ungrib and ungrib_paths:
        print(f"Cleaning {len(ungrib_paths)} ungrib path(s).")
        for path in ungrib_paths:
            remove_path(path, execute=args.execute)

    if not args.keep_metgrid and metgrid_paths:
        print(f"Cleaning {len(metgrid_paths)} metgrid path(s).")
        for path in metgrid_paths:
            remove_path(path, execute=args.execute)

    if args.delete_hrrr:
        for date_str, summary in date_summary.items():
            if not summary["any_hrrr"]:
                continue
            if not summary["all_hrrr_real_done"]:
                continue
            grib_dir = summary["grib_dir"]
            if not grib_dir:
                continue
            sim_start = compute_sim_start(date_str)
            start_dt = datetime.strptime(sim_start, "%Y%m%d_%H")
            end_dt = start_dt + timedelta(hours=int(summary["max_sim_hrs"]))
            day_cursor = datetime(start_dt.year, start_dt.month, start_dt.day)
            print(f"[HRRR] {date_str} -> {start_dt} to {end_dt}")
            while day_cursor <= end_dt:
                day_str = day_cursor.strftime("%Y%m%d")
                remove_path(Path(grib_dir) / f"hrrr.{day_str}", execute=args.execute)
                remove_path(Path(grib_dir) / f"hrrr.{day_str}.subset", execute=args.execute)
                day_cursor += timedelta(days=1)

    if not args.execute:
        print("Dry-run only. Re-run with --execute to delete.")


if __name__ == "__main__":
    main()
