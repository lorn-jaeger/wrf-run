#!/usr/bin/env python3

'''
run_avg_tsfc.py

Created by: Jared A. Lee (jaredlee@ucar.edu)
Created on: 21 Feb 2025

This script is designed to run avg_tsfc.exe and wait for its completion.
'''

import os
import sys
import shutil
import argparse
import pathlib
import glob
import time
import datetime as dt
import pandas as pd
import logging

from proc_util import exec_command
from wps_wrf_util import search_file

this_file = os.path.basename(__file__)
logging.basicConfig(format=f'{this_file}: %(asctime)s - %(message)s',
                    level=logging.DEBUG, datefmt='%Y-%m-%dT%H:%M:%S')
log = logging.getLogger(__name__)

long_time = 5
short_time = 3
curr_dir=os.path.dirname(os.path.abspath(__file__))

def parse_args():
    ## Parse the command-line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-b', '--cycle_dt_beg', default='20220801_00',
                        help='beginning date/time of the WRF model cycle [YYYYMMDD_HH] (default: 20220801_00)')
    parser.add_argument('-s', '--sim_hrs', default=192, type=int,
                        help='integer number of hours for the WRF simulation (default: 192)')
    parser.add_argument('-w', '--wps_dir', default=None,
                        help='string or pathlib.Path object of the WPS install directory')
    parser.add_argument('-r', '--run_dir', default=None,
                        help='string or pathlib.Path object of the run directory where files should be linked and run')
    parser.add_argument('-u', '--ungrib_dir', default=None,
                        help='string or pathlib.Path object of the ungrib output directory (default: run_dir/ungrib)')
    parser.add_argument('-t', '--tmp_dir', default=None,
                        help='string or pathlib.Path object that hosts namelist & queue submission script templates')
    parser.add_argument('-m', '--icbc_model', default='GEFS',
                        help='string specifying the IC/LBC model (default: GEFS)')
    parser.add_argument('-n', '--nml_tmp', default=None,
                        help='string for filename of namelist template (default: namelist.wps.[icbc_model])')
    parser.add_argument('-v', '--hrrr_native', action='store_true',
                        help='If flag present, then use HRRR native-grid data for atmospheric variables')

    args = parser.parse_args()
    cycle_dt_beg = args.cycle_dt_beg
    sim_hrs = args.sim_hrs
    wps_dir = args.wps_dir
    run_dir = args.run_dir
    ungrib_dir = args.ungrib_dir
    tmp_dir = args.tmp_dir
    icbc_model = args.icbc_model
    nml_tmp = args.nml_tmp
    hrrr_native = args.hrrr_native

    if len(cycle_dt_beg) != 11 or cycle_dt_beg[8] != '_':
        log.error('ERROR! Incorrect format for argument cycle_dt_beg in call to run_metgrid.py. Exiting!')
        parser.print_help()
        sys.exit(1)

    if sim_hrs < 24:
        log.error('ERROR! sim_hrs = ' + str(sim_hrs) + ', but must be at least 24 to run avg_tsfc.exe.')
        log.error('Exiting!')
        sys.exit(1)

    if sim_hrs % 24 != 0:
        log.info('WARNING: sim_hrs = ' + str(sim_hrs) + ', but avg_tsfc.exe will only process full 24-h periods.')
        log.info('         The last ' + str(sim_hrs % 24) + ' of this simulation will be ignored by avg_tsfc.exe.')

    if wps_dir is not None:
        wps_dir = pathlib.Path(wps_dir)
    else:
        log.error('ERROR! wps_dir not specified as an argument in call to run_avg_tsfc.py. Exiting!')
        sys.exit(1)

    if run_dir is not None:
        run_dir = pathlib.Path(run_dir)
    else:
        log.error('ERROR! run_dir not specified as an argument in call to run_avg_tsfc.py. Exiting!')
        sys.exit(1)

    if ungrib_dir is not None:
        ungrib_dir = pathlib.Path(ungrib_dir)
    else:
        ## Make a default assumption to find the ungribbed data in run_dir/ungrib
        ungrib_dir = run_dir.joinpath('ungrib')

    if tmp_dir is not None:
        tmp_dir = pathlib.Path(tmp_dir)
    else:
        log.error('ERROR! tmp_dir is not specified as an argument in call to run_avg_tsfc.py. Exiting!')
        sys.exit(1)

    if nml_tmp is None:
        ## Make a default assumption about what namelist template we want to use
        nml_tmp = 'namelist.wps.'+icbc_model.lower()

    return (cycle_dt_beg, sim_hrs, wps_dir, run_dir, ungrib_dir, tmp_dir, icbc_model, nml_tmp, hrrr_native)

def main(cycle_dt_beg, sim_hrs, wps_dir, run_dir, ungrib_dir, tmp_dir, icbc_model, nml_tmp, hrrr_native):

    log.info(f'Running run_avg_tsfc.py from directory: {curr_dir}')

    fmt_yyyymmdd_hh = '%Y%m%d_%H'
    fmt_yyyymmdd_hhmm = '%Y%m%d_%H%M'
    fmt_wrf_dt = '%Y-%m-%d_%H:%M:%S'
    fmt_wrf_date_hh = '%Y-%m-%d_%H'

    variants_gfs = ['GFS', 'gfs']
    variants_gfs_fnl = ['GFS_FNL', 'gfs_fnl']
    variants_gefs = ['GEFS', 'gefs']
    variants_hrrr = ['HRRR', 'hrrr']

    cycle_dt = pd.to_datetime(cycle_dt_beg, format=fmt_yyyymmdd_hh)
    beg_dt = cycle_dt
    end_dt = beg_dt + dt.timedelta(hours=sim_hrs)

    beg_dt_wrf = beg_dt.strftime(fmt_wrf_dt)
    end_dt_wrf = end_dt.strftime(fmt_wrf_dt)

    # Go to the run directory
    os.chdir(run_dir)

    # Link to avg_tsfc.exe
    if pathlib.Path('avg_tsfc.exe').is_symlink():
        pathlib.Path('avg_tsfc.exe').unlink()
    pathlib.Path('avg_tsfc.exe').symlink_to(wps_dir.joinpath('util', 'avg_tsfc.exe'))

    # Copy over the default namelist
    shutil.copy(tmp_dir.joinpath(nml_tmp), 'namelist.wps.template')

    # First, open the namelist template and find if a line starts with constants_name
    # If it's not found, then we know we need to add it when we modify the namelist
    constants_name = False
    with open('namelist.wps.template', 'r') as in_file:
        for num, line in enumerate(in_file):
            if 'constants_name' in line:
                constants_name = True

    # Modify the namelist for this date and simulation length
    # Track the line number for the start of the &metgrid namelist, to add constants_name = TAVGSFC if needed
    with open('namelist.wps.template', 'r') as in_file, open('namelist.wps', 'w') as out_file:
        for line in in_file:
            if line.strip()[0:10] == 'start_date':
                out_file.write(" start_date = '" + beg_dt_wrf + "', '" + beg_dt_wrf + "', '" + beg_dt_wrf + "',\n")
            elif line.strip()[0:8] == 'end_date':
                out_file.write(" end_date   = '" + end_dt_wrf + "', '" + end_dt_wrf + "', '" + end_dt_wrf + "',\n")
            elif line.strip()[0:7] == 'fg_name':
                if icbc_model in variants_gfs:
                    out_file.write(" fg_name = '" + str(ungrib_dir) + "/GFS',\n")
                elif icbc_model in variants_gfs_fnl:
                    out_file.write(" fg_name = '" + str(ungrib_dir) + "/GFS_FNL',\n")
                elif icbc_model in variants_gefs:
                    out_file.write(" fg_name = '" + str(ungrib_dir) + "/GEFS_B','" + str(ungrib_dir) + "/GEFS_A',\n")
                elif icbc_model in variants_hrrr:
                    if hrrr_native:
                        # If using native-grid HRRR output for atmos vars, then also need to have soil vars from pres
                        out_file.write(" fg_name = '" + str(ungrib_dir) + "/HRRR_hybr','" +
                                       str(ungrib_dir) + "/HRRR_soil',\n")
                    else:
                        # Otherwise, just use pressure-level HRRR output for both atmospheric & soil variables
                        out_file.write(" fg_name = '" + str(ungrib_dir) + "/HRRR_pres',\n")
                else:
                    out_file.write(" fg_name = '" + str(ungrib_dir) + "/FILE',\n")
            elif line.strip()[0:8] == '&metgrid' and not constants_name:
                # Add a new line in the &metgrid section since we know from before that constants_name is not present
                newline = line + " constants_name = '" + str(run_dir) + "/TAVGSFC',\n"
                out_file.write(newline)
            elif line.strip()[0:14] == 'constants_name':
                # Add TAVGSFC to the constants_name line if it isn't already included
                index = line.find('TAVGSFC')
                if index == -1:
                    # Find the newline character, add TAVGSFC before it
                    newline = line.split(sep='\n')[0]
                    newline += "'TAVGSFC',\n"
                out_file.write(newline)
            else:
                out_file.write(line)

    # Run avg_tsfc.exe utility
    ret, output = exec_command('./avg_tsfc.exe', log, wait=True)
    # avg_tsfc.exe won't return an error code of 1 or anything to stderr even if it fails, so manually search stdout
    if 'ERROR' in output:
        log.error('ERROR: avg_tsfc.exe failed. Exiting!')
        sys.exit(1)


if __name__ == '__main__':
    now_time_beg = dt.datetime.now(dt.UTC)
    cycle_dt_beg, sim_hrs, wps_dir, run_dir, grib_dir, tmp_dir, icbc_model, nml_tmp, hrrr_native = parse_args()
    main(cycle_dt_beg, sim_hrs, wps_dir, run_dir, grib_dir, tmp_dir, icbc_model, nml_tmp, hrrr_native)
    now_time_end = dt.datetime.now(dt.UTC)
    run_time_tot = now_time_end - now_time_beg
    now_time_beg_str = now_time_beg.strftime('%Y-%m-%d %H:%M:%S')
    now_time_end_str = now_time_end.strftime('%Y-%m-%d %H:%M:%S')
    log.info('run_avg_tsfc.py completed successfully.')
    log.info('Beg time: '+now_time_beg_str)
    log.info('End time: '+now_time_end_str)
    log.info('Run time: '+str(run_time_tot)+'\n')