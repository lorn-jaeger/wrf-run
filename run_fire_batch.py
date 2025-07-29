#!/usr/bin/env python3
"""Run setup_wps_wrf.py for multiple fires listed in a CSV file.

The CSV must contain columns:
    fire_id,start,end,lat,lon
where ``start`` and ``end`` are in YYYYMMDD_HH format.

For each fire a copy of a template directory is created with the
latitude/longitude inserted into ``namelist.wps.hrrr``. A per-fire YAML
configuration is written and ``setup_wps_wrf.py`` is invoked.  The script
supports concurrent execution and configurable logging.
"""

import argparse
import logging
import shutil
import subprocess
import pandas as pd
from pathlib import Path
import yaml
from concurrent.futures import ThreadPoolExecutor, as_completed


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
    p.add_argument('--max-workers', type=int, default=1,
                   help='Number of concurrent workers to launch')
    p.add_argument('--log-level', default='INFO',
                   help='Logging level (DEBUG, INFO, WARNING, ERROR)')
    args = p.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format='%(asctime)s %(levelname)s:%(message)s'
    )
    logger = logging.getLogger(__name__)

    df = pd.read_csv(Path(args.csv))
    duplicate_ids = df.loc[df['fire_id'].duplicated(), 'fire_id'].unique()
    if len(duplicate_ids) > 0:
        p.error(f"Duplicate fire_id(s) found in CSV: {', '.join(map(str, duplicate_ids))}")

    base_cfg = yaml.safe_load(Path(args.base_config).read_text())
    cfg_out_dir = Path(args.cfg_out)
    cfg_out_dir.mkdir(parents=True, exist_ok=True)

    def process_fire(row):
        fire_id = str(row['fire_id'])
        start = str(row['start'])
        end = str(row['end'])
        lat = float(row['lat'])
        lon = float(row['lon'])

        logger.info('Processing fire %s', fire_id)

        try:
            fire_template = Path(args.template_dir).parent / f"{Path(args.template_dir).name}_{fire_id}"
            wps_dir = Path(args.wps_parent) / fire_id
            wrf_dir = Path(args.wrf_parent) / fire_id
            cfg_file = cfg_out_dir / f"{fire_id}.yaml"

            for path in (fire_template, wps_dir, wrf_dir, cfg_file):
                if path.exists():
                    raise FileExistsError(f"Path {path} already exists for fire_id {fire_id}")

            shutil.copytree(args.template_dir, fire_template)
            update_wps_namelist(fire_template / 'namelist.wps.hrrr', lat, lon)

            cfg = dict(base_cfg)
            cfg['template_dir'] = str(fire_template.resolve())
            cfg['wps_run_dir'] = str(wps_dir.resolve())
            cfg['wrf_run_dir'] = str(wrf_dir.resolve())

            with cfg_file.open('w') as f:
                yaml.safe_dump(cfg, f)

            cmd = ['python', 'setup_wps_wrf.py', '-b', start, '-e', end, '-c', str(cfg_file)]
            if args.dry_run:
                logger.info('DRY RUN: %s', ' '.join(cmd))
            else:
                logger.info('Running %s', ' '.join(cmd))
                subprocess.run(cmd, check=True)
            return fire_id, None
        except Exception as exc:
            logger.exception('Failed processing %s', fire_id)
            return fire_id, exc

    rows = [row for _, row in df.iterrows()]

    if args.max_workers > 1:
        logger.info('Processing %d fires with %d workers', len(rows), args.max_workers)
        with ThreadPoolExecutor(max_workers=args.max_workers) as ex:
            futures = {ex.submit(process_fire, row): row for row in rows}
            for fut in as_completed(futures):
                fire_id, exc = fut.result()
                if exc:
                    logger.error('Error processing %s: %s', fire_id, exc)
    else:
        for row in rows:
            fire_id, exc = process_fire(row)
            if exc:
                logger.error('Error processing %s: %s', fire_id, exc)


if __name__ == '__main__':
    main()
