#!/usr/bin/env python3

'''
run_wrf.py

Created by: Jared A. Lee (jaredlee@ucar.edu)
Created on: 24 Apr 2023

This script is designed to run wrf.exe as a batch job.
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
long_long_time = 15
short_time = 3
curr_dir=os.path.dirname(os.path.abspath(__file__))

def parse_args():
    ## Parse the command-line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-b', '--cycle_dt_beg', default='20220801_00', help='beginning date/time of the WRF model cycle [YYYYMMDD_HH] (default: 20220801_00)')
    parser.add_argument('-s', '--sim_hrs', default=192, type=int, help='integer number of hours for the WRF simulation (default: 192)')
    parser.add_argument('-w', '--wrf_dir', default=None, help='string or pathlib.Path object of the WRF install directory')
    parser.add_argument('-r', '--run_dir', default=None, help='string or pathlib.Path object of the run directory where files should be linked and run')
    parser.add_argument('-t', '--tmp_dir', default=None, help='string or pathlib.Path object that hosts namelist & queue submission script templates')
    parser.add_argument('-i', '--icbc_model', default='GEFS', help='string specifying the IC/LBC model (default: GEFS)')
    parser.add_argument('-x', '--exp_name', default=None, help='string specifying the name of the experiment/member name (e.g., exp01, mem01, etc.)')
    parser.add_argument('-n', '--nml_tmp', default=None, help='string for filename of namelist template (default: namelist.input.icbc_model.exp_name, with icbc_model in lower-case)')
    parser.add_argument('-m', '--monitor_wrf', help='flag to keep the script active as long as wrf.exe is submitted/running on the cluster (True if flag present, False if not present)', action='store_true')
    parser.add_argument('-q', '--scheduler', default='pbs', help='string specifying the cluster job scheduler (default: pbs)')

    args = parser.parse_args()
    cycle_dt_beg = args.cycle_dt_beg
    sim_hrs = args.sim_hrs
    wrf_dir = args.wrf_dir
    run_dir = args.run_dir
    tmp_dir = args.tmp_dir
    icbc_model = args.icbc_model
    exp_name = args.exp_name
    nml_tmp = args.nml_tmp
    scheduler = args.scheduler

    if len(cycle_dt_beg) != 11 or cycle_dt_beg[8] != '_':
        print('ERROR! Incorrect format for argument cycle_dt_beg in call to run_real.py. Exiting!')
        parser.print_help()
        sys.exit(1)

    if wrf_dir is not None:
        wrf_dir = pathlib.Path(wrf_dir)
    else:
        print('ERROR! wrf_dir not specified as an argument in call to run_real.py. Exiting!')
        sys.exit(1)

    if run_dir is not None:
        run_dir = pathlib.Path(run_dir)
    else:
        print('ERROR! run_dir not specified as an argument in call to run_real.py. Exiting!')
        sys.exit(1)

    if tmp_dir is not None:
        tmp_dir = pathlib.Path(tmp_dir)
    else:
        print('ERROR! tmp_dir is not specified as an argument in call to run_real.py. Exiting!')
        sys.exit(1)

    if nml_tmp is None:
        ## Make a default assumption about what namelist template we want to use
        nml_tmp = 'namelist.input.'+icbc_model.lower()+'.'+exp_name

    monitor_wrf = False
    if args.monitor_wrf:
        monitor_wrf = True

    return cycle_dt_beg, sim_hrs, wrf_dir, run_dir, tmp_dir, icbc_model, exp_name, nml_tmp, monitor_wrf, scheduler

def main(cycle_dt_beg, sim_hrs, wrf_dir, run_dir, tmp_dir, icbc_model, exp_name, nml_tmp, monitor_wrf, scheduler):

    log.info(f'Running run_wrf.py from directory: {curr_dir}')

    fmt_yyyymmdd_hh = '%Y%m%d_%H'
    fmt_yyyymmdd_hhmm = '%Y%m%d_%H%M'
    fmt_wrf_dt = '%Y-%m-%d_%H:%M:%S'
    fmt_wrf_date_hh = '%Y-%m-%d_%H'

    cycle_dt = pd.to_datetime(cycle_dt_beg, format=fmt_yyyymmdd_hh)
    beg_dt = cycle_dt
    end_dt = beg_dt + dt.timedelta(hours=sim_hrs)

    beg_dt_wrf = beg_dt.strftime(fmt_wrf_dt)
    end_dt_wrf = end_dt.strftime(fmt_wrf_dt)

    beg_yr = beg_dt.strftime('%Y')
    end_yr = end_dt.strftime('%Y')
    beg_mo = beg_dt.strftime('%m')
    end_mo = end_dt.strftime('%m')
    beg_dy = beg_dt.strftime('%d')
    end_dy = end_dt.strftime('%d')
    beg_hr = beg_dt.strftime('%H')
    end_hr = end_dt.strftime('%H')
    beg_mn = beg_dt.strftime('%M')
    end_mn = end_dt.strftime('%M')

    ## Create the run directory if it doesn't already exist
    run_dir.mkdir(parents=True, exist_ok=True)

    ## Go to the run directory
    os.chdir(run_dir)

    ## Link to the files in the WRF/run directory
    files = glob.glob(str(wrf_dir)+'/run/*')
    for file in files:
        ret,output = exec_command(['ln','-sf',file,'.'], log)

    ## Delete the namelist.input link to the WRF default namelist
    pathlib.Path('namelist.input').unlink()

    ## Copy over the wrf batch script
    shutil.copy(tmp_dir.joinpath('submit_wrf.bash'), 'submit_wrf.bash')

    ## Copy over the default namelist
    shutil.copy(tmp_dir.joinpath(nml_tmp), 'namelist.input.template')

    ## Modify the namelist for this date and simulation length
    with open('namelist.input.template', 'r') as in_file, open('namelist.input', 'w') as out_file:
        for line in in_file:
            if line.strip()[0:9] == 'run_hours':
                out_file.write(' run_hours                = '+str(sim_hrs)+',\n')
            elif line.strip()[0:10] == 'start_year':
                out_file.write(' start_year               = '+str(beg_yr)+', '+str(beg_yr)+', '+str(beg_yr)+',\n')
            elif line.strip()[0:11] == 'start_month':
                out_file.write(' start_month              = '+str(beg_mo)+',   '+str(beg_mo)+',   '+str(beg_mo)+',\n')
            elif line.strip()[0:9]  == 'start_day':
                out_file.write(' start_day                = '+str(beg_dy)+',   '+str(beg_dy)+',   '+str(beg_dy)+',\n')
            elif line.strip()[0:10] == 'start_hour':
                out_file.write(' start_hour               = '+str(beg_hr)+',   '+str(beg_hr)+',   '+str(beg_hr)+',\n')
            elif line.strip()[0:12] == 'start_minute':
                out_file.write(' start_minute             = '+str(beg_mn)+',   '+str(beg_mn)+',   '+str(beg_mn)+',\n')
            elif line.strip()[0:8]  == 'end_year':
                out_file.write(' end_year                 = '+str(end_yr)+', '+str(end_yr)+', '+str(end_yr)+',\n')
            elif line.strip()[0:9]  == 'end_month':
                out_file.write(' end_month                = '+str(end_mo)+',   '+str(end_mo)+',   '+str(end_mo)+',\n')
            elif line.strip()[0:7]  == 'end_day':
                out_file.write(' end_day                  = '+str(end_dy)+',   '+str(end_dy)+',   '+str(end_dy)+',\n')
            elif line.strip()[0:8]  == 'end_hour':
                out_file.write(' end_hour                 = '+str(end_hr)+',   '+str(end_hr)+',   '+str(end_hr)+',\n')
            elif line.strip()[0:10] == 'end_minute':
                out_file.write(' end_minute               = '+str(end_mn)+',   '+str(end_mn)+',   '+str(end_mn)+',\n')
            else:
                out_file.write(line)

    ## Ensure that wrfinput and wrfbdy files exist. If not, exit the script with an error.
    ## First, read in max_dom from namelist.input to look for all the wrfinput files.
    with open('namelist.input') as nml:
        for line in nml:
            if line.strip()[0:7] == 'max_dom':
                max_dom = int(line.split('=')[1].strip().split(',')[0])
                break

    if not pathlib.Path('wrfbdy_d01').is_file():
        log.error('ERROR! '+str(run_dir)+'/wrfbdy_d01 not found, meaning WRF cannot run. Exiting!')
        success = False
        return success
        sys.exit(1)

    for dd in range(1,max_dom+1):
        if not pathlib.Path('wrfinput_d0'+str(dd)).is_file():
            log.error('ERROR! '+str(run_dir)+'/wrfinput_d0'+str(dd)+' not found, meaning WRF cannot run. Exiting!')
            success = False
            return success
            sys.exit(1)

    ## Clean up any rsl.out, rsl.error, and wrf log files
    files = glob.glob('rsl.*')
    for file in files:
        ret,output = exec_command(['rm',file], log, False, False)
    files = glob.glob('log_wrf.*')
    for file in files:
        ret,output = exec_command(['rm',file], log, False, False)
    files = glob.glob('wrf.o*')
    for file in files:
        ret,output = exec_command(['rm',file], log, False, False)

    ## Submit wrf and get the job ID as a string
    jobname = 'M' + exp_name[-1] + '_' + str(beg_dy) + '_' + str(beg_hr)
    if scheduler == 'slurm':
        ret,output = exec_command(['sbatch','-J',jobname,'submit_wrf.bash'], log)
        jobid = output.split('job ')[1].split('\\n')[0].strip()
        log.info('Submitted batch job '+jobid)
    elif scheduler == 'pbs':
        ret,output = exec_command(['qsub','-N',jobname,'submit_wrf.bash'], log)
        jobid = output.split('.')[0]
        queue = output.split('.')[1]
        log.info('Submitted batch job '+jobid+' to queue '+queue)

    # TODO: Remove after testing run_wrf process monitoring...
    log.info(f'Sleeping for {long_time} s...')

    time.sleep(long_time)   # give the file system a moment

    # TODO: Remove after testing run_wrf process monitoring...
    log.info(f'Checking job status...')
    if scheduler == 'slurm':
        ret,output = exec_command([f'{curr_dir}/check_job_status.sh',jobid], log)
        # TODO: Remove after testing run_wrf process monitoring...
        log.info(f'    Got: {ret}')
    elif scheduler == 'pbs':
        log.info('WARNING: check_job_status.sh needs to be modified to handle PBS calls')

    ## Monitor the progress of wrf
    if monitor_wrf:
        # TODO: Remove after testing run_wrf process monitoring...
        log.info(f'Monitoring WRF...')
        status = False
        while not status:
            if not pathlib.Path('rsl.out.0000').is_file():
                # TODO: Remove after testing run_wrf process monitoring...
                log.info(f'    No "rsl.out.0000 file present. Sleeping for {long_time} s...')
                time.sleep(long_time)
            else:
                log.info('wrf is now running on the cluster . . .')
                status = True
        status = False
        while not status:
            # TODO: Remove after testing run_wrf process monitoring...
            log.info(f'    Checking for "SUCCESS COMPLETE WRF" in rsl.out.0000...')
            if 'SUCCESS COMPLETE WRF' in open('rsl.out.0000').read():
                log.info('SUCCESS! wrf completed successfully.')
                time.sleep(short_time)  # brief pause to let the file system gather itself
                status = True
            else:
                ## The rsl.error files might be empty for a time, which may cause an error if attempting to read it
                if os.stat('rsl.error.0000') == 0:
                    # TODO: Remove after testing run_wrf process monitoring...
                    log.info(f'    No rsl.out.0000 file is present. Sleeping for {long_time} s...')
                    time.sleep(long_time)
                else:
                    # TODO: Remove after testing run_wrf process monitoring...
                    log.info(f'    Checking rsl.error.**** files...')

                    ## Loop through the rsl.error.* files to look for fatal errors
                    rslerr = 'rsl.error.*'
                    for fname in glob.glob(rslerr):
                        ## May need to add other error keywords to search for...
                        if 'Fatal' in open(fname).read() or 'FATAL' in open(fname).read() or 'ERROR' in open(fname).read():
                            log.error('ERROR: wrf.exe failed.')
                            log.error('Consult '+str(run_dir)+'/'+str(fname)+' for potential error messages.')
                            log.error('Exiting!')
                            sys.exit(1)

                        # TODO: Remove after testing run_wrf process monitoring...
                        log.info(f'        File {fname} contains no ERROR logs...')
                    if os.path.exists('log_wrf.o'+jobid) and ('BAD TERMINATION' in open('log_wrf.o'+jobid).read() or 'ERROR' in open('log_wrf.o'+jobid).read()):
                        log.error('ERROR: wrf.exe failed.')
                        log.error('Consult '+str(run_dir)+'/log_wrf.o'+jobid+' for potential error messages.')
                        log.error('Exiting!')
                        sys.exit(1)

                    # TODO: Remove after testing run_wrf process monitoring...
                    log.info(f'    File "log_wrf.o{jobid}" is free of termination logs. Sleeping for {long_time} s...')

                    time.sleep(long_time)
    else:
        log.info('wrf submitted to the queue. Check back later here to see if the model run was successful:')
        log.info('   '+str(run_dir))

if __name__ == '__main__':
    now_time_beg = dt.datetime.utcnow()
    cycle_dt, sim_hrs, wrf_dir, run_dir, tmp_dir, icbc_model, exp_name, nml_tmp, monitor_wrf, scheduler = parse_args()
    main(cycle_dt, sim_hrs, wrf_dir, run_dir, tmp_dir, icbc_model, exp_name, nml_tmp, monitor_wrf, scheduler)
    now_time_end = dt.datetime.utcnow()
    run_time_tot = now_time_end - now_time_beg
    now_time_beg_str = now_time_beg.strftime('%Y-%m-%d %H:%M:%S')
    now_time_end_str = now_time_end.strftime('%Y-%m-%d %H:%M:%S')
    log.info('run_wrf.py completed successfully.')
    log.info('Beg time: '+now_time_beg_str)
    log.info('End time: '+now_time_end_str)
    log.info('Run time: '+str(run_time_tot)+'\n')
