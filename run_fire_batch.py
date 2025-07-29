#!/usr/bin/env python3
"""Run setup_wps_wrf.py for multiple fires listed in a CSV file.

The CSV must contain columns:
    fire_id,start,end,lat,lon
where ``start`` and ``end`` are in YYYYMMDD_HH format.

For each fire a copy of a template directory is created with the
latitude/longitude inserted into ``namelist.wps.hrrr``. A per-fire YAML
configuration is written and ``setup_wps_wrf.py`` is invoked.
"""

import argparse
import shutil
import subprocess
import pandas as pd
from pathlib import Path
import yaml


def update_wps_namelist(nml_path: Path, lat: float, lon: float) -> None:
    """Update domain center coordinates in a namelist.wps file."""
    lines = []
    with nml_path.open() as f:
        for line in f:
            s = line.lstrip()
            if s.startswith('ref_lat'):
                lines.append(f" ref_lat   = {lat:.7f},\n")
            elif s.startswith('ref_lon'):
                lines.append(f" ref_lon   = {lon:.7f},\n")
            elif s.startswith('truelat1'):
                lines.append(f" truelat1  = {lat:.7f},\n")
            elif s.startswith('truelat2'):
                lines.append(f" truelat2  = {lat:.7f},\n")
            elif s.startswith('stand_lon'):
                lines.append(f" stand_lon = {lon:.7f},\n")
            else:
                lines.append(line)
    nml_path.write_text(''.join(lines))


def main() -> None:
    p = argparse.ArgumentParser(description='Batch-run setup_wps_wrf.py from a CSV list of fires.')
    p.add_argument('csv', help='CSV file with columns fire_id,start,end,lat,lon')
    p.add_argument('-b', '--base-config', default='config/wps_wrf_config.yaml',
                   help='Base configuration YAML file')
    p.add_argument('-t', '--template-dir', default='templates/fasteddy_nm',
                   help='Directory containing template namelists and submit scripts')
    p.add_argument('--wps-parent', default='wps_runs',
                   help='Parent directory for WPS runs')
    p.add_argument('--wrf-parent', default='wrf_runs',
                   help='Parent directory for WRF runs')
    p.add_argument('--cfg-out', default='config/generated',
                   help='Directory to write per-fire YAML configs')
    p.add_argument('--dry-run', action='store_true', help='Print commands without executing')
    args = p.parse_args()

    df = pd.read_csv(Path(args.csv))
    base_cfg = yaml.safe_load(Path(args.base_config).read_text())
    cfg_out_dir = Path(args.cfg_out)
    cfg_out_dir.mkdir(parents=True, exist_ok=True)

    for _, row in df.iterrows():
        fire_id = str(row['fire_id'])
        start = str(row['start'])
        end = str(row['end'])
        lat = float(row['lat'])
        lon = float(row['lon'])

        fire_template = Path(args.template_dir).parent / f"{Path(args.template_dir).name}_{fire_id}"
        if fire_template.exists():
            shutil.rmtree(fire_template)
        shutil.copytree(args.template_dir, fire_template)
        update_wps_namelist(fire_template / 'namelist.wps.hrrr', lat, lon)

        wps_dir = Path(args.wps_parent) / fire_id
        wrf_dir = Path(args.wrf_parent) / fire_id

        cfg = dict(base_cfg)
        cfg['template_dir'] = str(fire_template.resolve())
        cfg['wps_run_dir'] = str(wps_dir.resolve())
        cfg['wrf_run_dir'] = str(wrf_dir.resolve())

        cfg_file = cfg_out_dir / f"{fire_id}.yaml"
        with cfg_file.open('w') as f:
            yaml.safe_dump(cfg, f)

        cmd = ['python', 'setup_wps_wrf.py', '-b', start, '-e', end, '-c', str(cfg_file)]
        if args.dry_run:
            print(' '.join(cmd))
        else:
            subprocess.run(cmd, check=True)


if __name__ == '__main__':
    main()
