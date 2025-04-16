#!/usr/bin/env python3

"""
link_gfs_from_glade.py

Created by: Jared A. Lee (jaredlee@ucar.edu)
Created on: 13 Dec 2024

This script links to GFS 0.25-deg forecast output files stored on GLADE at NSF NCAR for the requested
cycle(s) and lead times. Note that these files are stored every 3 h on GLADE, unlike AWS (1-hourly files).
"""

import os
import sys
import argparse
import pathlib
import datetime as dt
import numpy as np
import pandas as pd
import logging
from proc_util import exec_command

this_file = os.path.basename(__file__)
logging.basicConfig(format=f'{this_file}: %(asctime)s - %(message)s', level=logging.DEBUG, datefmt='%Y-%m-%dT%H:%M:%S')
log = logging.getLogger(__name__)

long_time = 5
long_long_time = 15
short_time = 3
curr_dir=os.path.dirname(os.path.abspath(__file__))

def parse_args():
    ## Parse the command-line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-b', '--cycle_dt', default='20220801_00',
                        help='GFS cycle date/time to download [YYYYMMDD_HH] (default: 20220801_00)')
    parser.add_argument('-s', '--sim_hrs', default=48, type=int,
                        help='integer number of forecast hours to download (default: 48)')
    parser.add_argument('-o', '--out_dir', default=None,
                        help='string or pathlib.Path object of the local directory where all downloaded GFS data should be stored')
    parser.add_argument('-f', '--icbc_fc_dt', default=0, type=int,
                        help='integer number of hours prior to WRF cycle time for IC/LBC model cycle (default: 0)')
    parser.add_argument('-r', '--resolution', default=0.25, type=float,
                        help='resolution of GFS to download (0.25 [default] or 0.5')
    parser.add_argument('-i', '--int_h', default=3, type=int,
                        help='integer number of hours between GFS files to download (default: 3)')

    args = parser.parse_args()
    cycle_dt = args.cycle_dt
    sim_hrs = args.sim_hrs
    out_dir = args.out_dir
    icbc_fc_dt = args.icbc_fc_dt
    resolution = args.resolution
    int_h = args.int_h

    if len(cycle_dt) != 11:
        log.error('ERROR! Incorrect length for positional argument cycle_dt. Exiting!')
        parser.print_help()
        sys.exit(1)
    elif cycle_dt[8] != '_':
        log.error('ERROR! Incorrect format for positional argument cycle_dt. Exiting!')
        parser.print_help()
        sys.exit(1)

    if out_dir is None:
        ## Make a default assumption about where to put the files
        out_dir = pathlib.Path('/', 'glade', 'derecho', 'scratch', 'jaredlee', 'data', 'gfs', cycle_dt)
        log.info('Using the default assumption for out_dir: ' + str(out_dir))
    else:
        out_dir = pathlib.Path(out_dir)

    return cycle_dt, sim_hrs, out_dir, icbc_fc_dt, resolution, int_h

def main(cycle_dt_str, sim_hrs, out_dir, icbc_fc_dt, resolution, now_time_beg, interval):
    log.info(f'Running link_gfs_from_glade.py from directory: {curr_dir}')

    # Calculate the desired lead hours for this cycle, accounting for the possible icbc_fc_dt offset.
    # Build array of forecast lead times to download. GFS output on GLADE is 3-hourly.
    leads = np.arange(icbc_fc_dt, sim_hrs + icbc_fc_dt + 1, interval)
    n_leads = len(leads)

    fmt_yyyy = '%Y'
    fmt_hh = '%H'
    fmt_yyyymmdd = '%Y%m%d'
    fmt_yyyymmdd_hh = '%Y%m%d_%H'

    cycle_dt = pd.to_datetime(cycle_dt_str, format=fmt_yyyymmdd_hh)
    cycle_date = cycle_dt.strftime(fmt_yyyymmdd)
    cycle_year = cycle_dt.strftime(fmt_yyyy)
    cycle_hour = cycle_dt.strftime(fmt_hh)
    cycle_datehh = cycle_date + cycle_hour

    glade_dir_parent = pathlib.Path('/', 'glade', 'campaign', 'collections', 'rda', 'data', 'd084001')
    glade_dir = glade_dir_parent.joinpath(cycle_year, cycle_date)

    out_dir.mkdir(parents=True, exist_ok=True)
    os.chdir(out_dir)

    # Loop over lead times
    for ll in range(n_leads):
        this_lead = str(leads[ll]).zfill(3)

        # Note that the filenames for 0.25-deg GFS files on GLADE and AWS differ...
        fname_glade = 'gfs.0p25.' + cycle_datehh + '.f' + this_lead + '.grib2'
        fname_aws = 'gfs.t' + cycle_hour + 'z.pgrb2.0p25.f' + this_lead
        glade_file = glade_dir.joinpath(fname_glade)

        # First, check for the file's existence on GLADE
        if not glade_file.is_file():
            log.info('WARNING: File ' + str(glade_file) + ' does not exist. Looping to the next expected IC/LBC time.')
            continue

        # Second, check for the existence of the link/file where ungrib will expect to find it
        if not out_dir.joinpath(fname_glade).is_file():
            log.info('Linking to ' + str(glade_file) + ' in ' + str(out_dir))
            ret,output = exec_command(['ln', '-sf', str(glade_dir.joinpath(fname_glade)), '.'], log)
        else:
            log.info('   File ' + fname_glade + ' already exists locally. No need to re-link to it on GLADE.')



if __name__ == '__main__':
    now_time_beg = dt.datetime.now(dt.UTC)
    cycle_dt, sim_hrs, out_dir, icbc_fc_dt, resolution, int_h = parse_args()
    main(cycle_dt, sim_hrs, out_dir, icbc_fc_dt, resolution, now_time_beg, int_h)
    now_time_end = dt.datetime.now(dt.UTC)
    run_time_tot = now_time_end - now_time_beg
    now_time_beg_str = now_time_beg.strftime('%Y-%m-%d %H:%M:%S')
    now_time_end_str = now_time_end.strftime('%Y-%m-%d %H:%M:%S')
    log.info('')
    log.info(this_file + ' completed successfully.')
    log.info('   Beg time: ' + now_time_beg_str)
    log.info('   End time: ' + now_time_end_str)
    log.info('   Run time: ' + str(run_time_tot) + '\n')

