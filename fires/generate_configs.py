#!/usr/bin/env python3
from __future__ import annotations

import argparse
import copy
import csv
import re
import shutil
from pathlib import Path
from typing import Dict, Tuple

import yaml


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate per-fire workflow configs and WRF templates."
    )
    parser.add_argument(
        "--runs-csv",
        type=Path,
        default=Path("fires/runs.csv"),
        help="CSV containing fire metadata (default: fires/runs.csv).",
    )
    parser.add_argument(
        "--workflow-template",
        type=Path,
        default=Path("configs/templates/workflow/base.yaml"),
        help="Base workflow YAML template.",
    )
    parser.add_argument(
        "--wrf-template-dir",
        type=Path,
        default=Path("configs/templates/wrf"),
        help="Directory containing WRF namelist templates.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("configs/built"),
        help="Root directory for generated configs (default: configs/built).",
    )
    parser.add_argument(
        "--workflow-root",
        default="/glade/derecho/scratch/ljaeger/workflow",
        help="Remote workflow root used inside configs.",
    )
    parser.add_argument(
        "--grib-root",
        default="/glade/derecho/scratch/ljaeger/data/hrrr",
        help="Remote GRIB root used inside configs.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace existing generated configs if they already exist.",
    )
    return parser.parse_args()


def load_fire_metadata(csv_path: Path) -> Dict[str, Dict[str, str]]:
    if not csv_path.exists():
        raise SystemExit(f"Input CSV {csv_path} does not exist.")

    with csv_path.open("r", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise SystemExit(f"{csv_path} is empty.")

        id_field = next((f for f in ("key", "fire_id") if f in reader.fieldnames), None)
        lat_field = next((f for f in ("latitude", "lat") if f in reader.fieldnames), None)
        lon_field = next((f for f in ("longitude", "lon") if f in reader.fieldnames), None)

        if not id_field or not lat_field or not lon_field:
            raise SystemExit("CSV must include fire id, latitude, and longitude columns.")

        meta: Dict[str, Dict[str, str]] = {}
        for row in reader:
            fire_id = row[id_field]
            if not fire_id or fire_id in meta:
                continue
            meta[fire_id] = {
                "latitude": row[lat_field],
                "longitude": row[lon_field],
                "start": row.get("start") or row.get("current") or "",
                "end": row.get("end") or "",
                "sim_start": row.get("sim_start") or "",
            }
    if not meta:
        raise SystemExit(f"No fire metadata found in {csv_path}.")
    return meta


def update_wps_namelist(namelist_path: Path, lat: float, lon: float, fire_id: str) -> None:
    text = namelist_path.read_text()
    lat_str = f"{lat:.4f}"
    lon_str = f"{lon:.4f}"

    text = re.sub(r"ref_lat\s*=\s*[-\d\.]+", f"ref_lat   =  {lat_str}", text)
    text = re.sub(r"ref_lon\s*=\s*[-\d\.]+", f"ref_lon   =  {lon_str}", text)
    text = re.sub(r"truelat1\s*=\s*[-\d\.]+", f"truelat1  =  {lat_str}", text)
    text = re.sub(r"truelat2\s*=\s*[-\d\.]+", f"truelat2  =  {lat_str}", text)
    text = re.sub(r"stand_lon\s*=\s*[-\d\.]+", f"stand_lon =  {lon_str}", text)
    text = text.replace("UM_WRF_1Dom1km", fire_id)

    namelist_path.write_text(text)


def copy_wrf_template(
    template_dir: Path, dest_dir: Path, lat: float, lon: float, fire_id: str, overwrite: bool
) -> None:
    if dest_dir.exists():
        if overwrite:
            shutil.rmtree(dest_dir)
        else:
            raise SystemExit(f"Destination {dest_dir} already exists (use --overwrite to replace).")

    shutil.copytree(template_dir, dest_dir)
    namelist = dest_dir / "namelist.wps.hrrr"
    if namelist.exists():
        update_wps_namelist(namelist, lat, lon, fire_id)


def render_workflow_config(
    template_data: Dict[str, object],
    fire_id: str,
    template_dir: Path,
    workflow_root: Path,
    grib_root: Path,
) -> Dict[str, object]:
    cfg = copy.deepcopy(template_data)
    cfg["template_dir"] = str(template_dir.resolve())
    cfg["wps_run_dir"] = str((workflow_root / fire_id / "wps").as_posix())
    cfg["wrf_run_dir"] = str((workflow_root / fire_id / "wrf").as_posix())
    cfg["grib_dir"] = str((grib_root / fire_id).as_posix())
    return cfg


def write_workflow_config(dest_path: Path, config: Dict[str, object]) -> None:
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    with dest_path.open("w") as handle:
        yaml.safe_dump(config, handle, sort_keys=False)


def generate_configs(args: argparse.Namespace) -> Tuple[int, Path]:
    fire_meta = load_fire_metadata(args.runs_csv)
    with args.workflow_template.open("r") as handle:
        template_yaml = yaml.safe_load(handle) or {}

    workflow_root = Path(args.workflow_root)
    grib_root = Path(args.grib_root)
    workflow_out_dir = args.output_dir / "workflow"
    wrf_out_dir = args.output_dir / "wrf"
    wrf_out_dir.mkdir(parents=True, exist_ok=True)

    for fire_id, fields in sorted(fire_meta.items()):
        lat = float(fields["latitude"])
        lon = float(fields["longitude"])
        fire_template_dir = wrf_out_dir / fire_id
        copy_wrf_template(args.wrf_template_dir, fire_template_dir, lat, lon, fire_id, args.overwrite)

        cfg = render_workflow_config(template_yaml, fire_id, fire_template_dir, workflow_root, grib_root)
        dest_config = workflow_out_dir / f"{fire_id}.yaml"
        if dest_config.exists() and not args.overwrite:
            raise SystemExit(f"{dest_config} exists (use --overwrite to replace).")
        write_workflow_config(dest_config, cfg)

    return len(fire_meta), args.output_dir


def main() -> None:
    args = parse_args()
    count, out_dir = generate_configs(args)
    print(f"Generated configs for {count} fires in {out_dir.resolve()}")


if __name__ == "__main__":
    main()
