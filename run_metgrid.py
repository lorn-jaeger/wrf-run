#!/usr/bin/env python3

'''
run_metgrid.py

Created by: Jared A. Lee (jaredlee@ucar.edu)
Created on: 5 Apr 2023

This script is designed to run metgrid.exe as a batch job and wait for its completion.
'''

import os
import sys
import subprocess
import shutil
import argparse
import pathlib
import glob
import time
import datetime as dt
import pandas as pd
import logging

from proc_util import exec_command

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
    parser.add_argument('-b', '--cycle_dt_beg', default='20220801_00', help='beginning date/time of the WRF model cycle [YYYYMMDD_HH] (default: 20220801_00)')
    parser.add_argument('-s', '--sim_hrs', default=192, type=int, help='integer number of hours for the WRF simulation (default: 192)')
    parser.add_argument('-w', '--wps_dir', default=None, help='string or pathlib.Path object of the WPS install directory')
    parser.add_argument('-r', '--run_dir', default=None, help='string or pathlib.Path object of the run directory where files should be linked and run')
    parser.add_argument('-o', '--out_dir', default=None, help='string or pathlib.Path object of the metgrid output directory (default: run_dir/metgrid)')
    parser.add_argument('-u', '--ungrib_dir', default=None, help='string or pathlib.Path object of the ungrib output directory (default: run_dir/ungrib)')
    parser.add_argument('-t', '--tmp_dir', default=None, help='string or pathlib.Path object that hosts namelist & queue submission script templates')
    parser.add_argument('-m', '--icbc_model', default='GEFS', help='string specifying the IC/LBC model (default: GEFS)')
    parser.add_argument('-n', '--nml_tmp', default=None, help='string for filename of namelist template (default: namelist.wps.icbc_model, with icbc_model in lower-case)')
    parser.add_argument('-q', '--scheduler', default='pbs', help='string specifying the cluster job scheduler (default: pbs)')

    args = parser.parse_args()
    cycle_dt_beg = args.cycle_dt_beg
    sim_hrs = args.sim_hrs
    wps_dir = args.wps_dir
    run_dir = args.run_dir
    out_dir = args.out_dir
    ungrib_dir = args.ungrib_dir
    tmp_dir = args.tmp_dir
    icbc_model = args.icbc_model
    nml_tmp = args.nml_tmp
    scheduler = args.scheduler

    if len(cycle_dt_beg) != 11 or cycle_dt_beg[8] != '_':
        print('ERROR! Incorrect format for argument cycle_dt_beg in call to run_metgrid.py. Exiting!')
        parser.print_help()
        sys.exit(1)

    if wps_dir is not None:
        wps_dir = pathlib.Path(wps_dir)
    else:
        print('ERROR! wps_dir not specified as an argument in call to run_metgrid.py. Exiting!')
        sys.exit(1)

    if run_dir is not None:
        run_dir = pathlib.Path(run_dir)
    else:
        print('ERROR! run_dir not specified as an argument in call to run_metgrid.py. Exiting!')
        sys.exit(1)

    if out_dir is not None:
        out_dir = pathlib.Path(out_dir)
    else:
        ## Make a default assumption to stick the output in run_dir/metgrid
        out_dir = run_dir.joinpath('metgrid')

    if ungrib_dir is not None:
        ungrib_dir = pathlib.Path(ungrib_dir)
    else:
        ## Make a default assumption to find the ungribbed data in run_dir/ungrib
        ungrib_dir = run_dir.joinpath('ungrib')

    if tmp_dir is not None:
        tmp_dir = pathlib.Path(tmp_dir)
    else:
        print('ERROR! tmp_dir is not specified as an argument in call to run_metgrid.py. Exiting!')
        sys.exit(1)

    if nml_tmp is None:
        ## Make a default assumption about what namelist template we want to use
        nml_tmp = 'namelist.wps.'+icbc_model.lower()

    return cycle_dt_beg, sim_hrs, wps_dir, run_dir, out_dir, ungrib_dir, tmp_dir, icbc_model, nml_tmp, scheduler

def main(cycle_dt_beg, sim_hrs, wps_dir, run_dir, out_dir, ungrib_dir, tmp_dir, icbc_model, nml_tmp, scheduler):

    log.info(f'Running run_metgrid.py from directory: {curr_dir}')

    fmt_yyyymmdd_hh = '%Y%m%d_%H'
    fmt_yyyymmdd_hhmm = '%Y%m%d_%H%M'
    fmt_wrf_dt = '%Y-%m-%d_%H:%M:%S'
    fmt_wrf_date_hh = '%Y-%m-%d_%H'

    cycle_dt = pd.to_datetime(cycle_dt_beg, format=fmt_yyyymmdd_hh)
    beg_dt = cycle_dt
    end_dt = beg_dt + dt.timedelta(hours=sim_hrs)

    beg_dt_wrf = beg_dt.strftime(fmt_wrf_dt)
    end_dt_wrf = end_dt.strftime(fmt_wrf_dt)

    ## Create the run directory and metgrid output directory if they don't already exist
    run_dir.mkdir(parents=True, exist_ok=True)
    out_dir.mkdir(parents=True, exist_ok=True)

    ## Go to the run directory
    os.chdir(run_dir)

    ## Link to metgrid.exe
    if pathlib.Path('metgrid.exe').is_symlink():
        pathlib.Path('metgrid.exe').unlink()
    pathlib.Path('metgrid.exe').symlink_to(wps_dir.joinpath('metgrid.exe'))

    ## Copy over the metgrid batch script
    shutil.copy(tmp_dir.joinpath('submit_metgrid.bash'), 'submit_metgrid.bash')

    ## Copy over the default namelist
    shutil.copy(tmp_dir.joinpath(nml_tmp), 'namelist.wps.template')

    ## Modify the namelist for this date and simulation length (only d01 needs to go the full length)
    with open('namelist.wps.template', 'r') as in_file, open('namelist.wps', 'w') as out_file:
        for line in in_file:
            if line.strip()[0:10] == 'start_date':
                out_file.write(" start_date = '"+beg_dt_wrf+"', '"+beg_dt_wrf+"', '"+beg_dt_wrf+"',\n")
            elif line.strip()[0:8] == 'end_date':
                out_file.write(" end_date   = '"+end_dt_wrf+"', '"+beg_dt_wrf+"', '"+beg_dt_wrf+"',\n")
            elif line.strip()[0:7] == 'fg_name':
                if icbc_model == 'GFS' or icbc_model == 'gfs':
                    out_file.write(" fg_name = '"+str(ungrib_dir)+"/GFS',\n")
                elif icbc_model == 'GEFS' or icbc_model == 'gefs':
                    out_file.write(" fg_name = '"+str(ungrib_dir)+"/GEFS_B','"+str(ungrib_dir)+"/GEFS_A',\n")
            elif line.strip()[0:28] == 'opt_output_from_metgrid_path':
                out_file.write(" opt_output_from_metgrid_path = '"+str(out_dir)+"',\n")
            else:
                out_file.write(line)

    ## Clean up old metgrid log files
    files = glob.glob('metgrid.log*')
    for file in files:
        ret,output = exec_command(['rm',file], log, False, False)
    files = glob.glob('log_metgrid.o*')
    for file in files:
        ret,output = exec_command(['rm',file], log, False, False)
    files = glob.glob('metgrid.o*')
    for file in files:
        ret,output = exec_command(['rm',file], log, False, False)

    ## Submit metgrid and get the job ID as a string
    if scheduler == 'slurm':
        ret,output = exec_command(['sbatch','submit_metgrid.bash'], log, False)
        jobid = output.split('job ')[1].split('\\n')[0].strip()
        log.info('Submitted batch job '+jobid)
        job_log_filename = 'log_metgrid.o' + jobid
    elif scheduler == 'pbs':
        ret,output = exec_command(['qsub','submit_metgrid.bash'], log, False)
        jobid = output.split('.')[0]
        queue = output.split('.')[1]
        log.info('Submitted batch job '+jobid+' to queue '+queue)
        job_log_filename = 'metgrid.o'+jobid
    time.sleep(long_time)   # give the file system a moment

    if scheduler == 'slurm':
        ret,output = exec_command([f'{curr_dir}/check_job_status.sh',jobid], log)
    elif scheduler == 'pbs':
        log.info('WARNING: check_jobs_status.sh needs to be modified to handle PBS calls')

    ## Monitor the progress of metgrid
    status = False
    while not status:
        if not pathlib.Path('metgrid.log.0000').is_file() or not pathlib.Path(job_log_filename).is_file():
            time.sleep(long_time)
        else:
            log.info('metgrid is now running on the cluster . . .')
            status = True
    status = False
    while not status:
        if '*** Successful completion of program metgrid.exe ***' in open('metgrid.log.0000').read():
            log.info('SUCCESS! metgrid completed successfully.')
            time.sleep(short_time)  # brief pause to let the file system gather itself
            status = True
        else:
            ## May need to add other error keywords to search for...
            if 'FATAL' in open('metgrid.log.0000').read() or 'Fatal' in open('metgrid.log.0000').read() or 'ERROR' in open('metgrid.log.0000').read():
                log.error('ERROR: metgrid.exe failed.')
                log.error('Consult '+str(run_dir)+'/metgrid.log.0000 for potential error messages.')
                log.error('Exiting!')
                sys.exit(1)
            elif os.path.exists(job_log_filename) and ('BAD TERMINATION' in open(job_log_filename).read() or 'ERROR' in open(job_log_filename).read()):
                log.error('ERROR: metgrid.exe failed.')
                log.error('Consult '+str(run_dir)+'/' + job_log_filename + ' for potential error messages.')
                log.error('Exiting!')
                sys.exit(1)
            time.sleep(long_time)


if __name__ == '__main__':
    now_time_beg = dt.datetime.utcnow()
    cycle_dt, sim_hrs, wps_dir, run_dir, out_dir, grib_dir, tmp_dir, icbc_model, nml_tmp, scheduler = parse_args()
    main(cycle_dt, sim_hrs, wps_dir, run_dir, out_dir, grib_dir, tmp_dir, icbc_model, nml_tmp, scheduler)
    now_time_end = dt.datetime.utcnow()
    run_time_tot = now_time_end - now_time_beg
    now_time_beg_str = now_time_beg.strftime('%Y-%m-%d %H:%M:%S')
    now_time_end_str = now_time_end.strftime('%Y-%m-%d %H:%M:%S')
    log.info('run_metgrid.py completed successfully.')
    log.info('Beg time: '+now_time_beg_str)
    log.info('End time: '+now_time_end_str)
    log.info('Run time: '+str(run_time_tot)+'\n')
