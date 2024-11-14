#!/usr/bin/env python3

'''
run_ungrib.py

Created by: Jared A. Lee (jaredlee@ucar.edu)
Created on: 31 Mar 2023

This script is designed to run ungrib.exe in embarrassingly parallel fashion.
Each file to be ungribbed will be submitted as a separate 1-core batch job to the queue.
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
long_long_time = 15
short_time = 3
curr_dir=os.path.dirname(os.path.abspath(__file__))

def parse_args():
    ## Parse the command-line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-b', '--cycle_dt_beg', default='20220801_00', help='beginning date/time of the WRF model cycle [YYYYMMDD_HH] (default: 20220801_00)')
    parser.add_argument('-s', '--sim_hrs', default=192, type=int, help='integer number of hours for the WRF simulation (default: 192)')
    parser.add_argument('-w', '--wps_dir', default=None, help='string or pathlib.Path object of the WPS install directory')
    parser.add_argument('-r', '--run_dir', default=None, help='string or pathlib.Path object of the run directory where files should be linked and run')
    parser.add_argument('-o', '--out_dir', default=None, help='string or pathlib.Path object of the ungrib output directory (default: run_dir/ungrib)')
    parser.add_argument('-g', '--grib_dir', default=None, help='string or pathlib.Path object that hosts the grib/grib2 data to be ungribbed')
    parser.add_argument('-t', '--temp_dir', default=None, help='string or pathlib.Path object that hosts namelist & queue submission script templates')
    parser.add_argument('-m', '--icbc_model', default='GEFS', help='string specifying the IC/LBC model (default: GEFS)')
    parser.add_argument('-f', '--icbc_fc_dt', default=0, type=int, help='integer number of hours prior to WRF cycle time for IC/LBC model cycle (default: 0)')
    parser.add_argument('-i', '--int_hrs', default=3, type=int, help='integer number of hours between IC/LBC files (default: 3)')
    parser.add_argument('-q', '--scheduler', default='pbs', help='string specifying the cluster job scheduler (default: pbs)')
    parser.add_argument('-n', '--mem_id', default=None, help='string specifying the numeric id (with any necessary leading zeros) of the GEFS or other ensemble member so that ./link_grib.csh can link to the correct file (default: None)')

    args = parser.parse_args()
    cycle_dt_beg = args.cycle_dt_beg
    sim_hrs = args.sim_hrs
    wps_dir = args.wps_dir
    run_dir = args.run_dir
    out_dir = args.out_dir
    grib_dir = args.grib_dir
    temp_dir = args.temp_dir
    icbc_model = args.icbc_model
    icbc_fc_dt = args.icbc_fc_dt
    int_hrs = args.int_hrs
    scheduler = args.scheduler
    mem_id = args.mem_id

    if len(cycle_dt_beg) != 11 or cycle_dt_beg[8] != '_':
        print('ERROR! Incorrect format for argument cycle_dt_beg in call to run_metgrid.py. Exiting!')
        parser.print_help()
        sys.exit(1)

    if wps_dir is not None:
        wps_dir = pathlib.Path(wps_dir)
    else:
        print('ERROR! wps_dir not specified as an argument in call to run_ungrib.py. Exiting!')
        sys.exit(1)

    if run_dir is not None:
        run_dir = pathlib.Path(run_dir)
    else:
        print('ERROR! run_dir not specified as an argument in call to run_ungrib.py. Exiting!')
        sys.exit(1)

    if out_dir is not None:
        out_dir = pathlib.Path(out_dir)
    else:
        ## Make a default assumption to stick the output in run_dir/ungrib
        out_dir = run_dir.joinpath('ungrib')

    if grib_dir is not None:
        grib_dir = pathlib.Path(grib_dir)
    else:
        print('ERROR! grib_dir not specified as an argument in call to run_ungrib.py. Exiting!')
        sys.exit(1)

    if temp_dir is not None:
        temp_dir = pathlib.Path(temp_dir)
    else:
        print('ERROR! temp_dir is not specified as an argument in call to run_ungrib.py. Exiting!')
        sys.exit(1)

    return cycle_dt_beg, sim_hrs, wps_dir, run_dir, out_dir, grib_dir, temp_dir, icbc_model, int_hrs, icbc_fc_dt, scheduler, mem_id

def main(cycle_dt_str, sim_hrs, wps_dir, run_dir, out_dir, grib_dir, temp_dir, icbc_model, int_hrs, icbc_fc_dt, scheduler, mem_id):

    log.info(f'Running run_ungrib.py from directory: {curr_dir}')

    fmt_yyyymmdd_hh = '%Y%m%d_%H'
    fmt_yyyymmdd_hhmm = '%Y%m%d_%H%M'
    fmt_wrf_dt = '%Y-%m-%d_%H:%M:%S'
    fmt_wrf_date_hh = '%Y-%m-%d_%H'

    cycle_dt = pd.to_datetime(cycle_dt_str, format=fmt_yyyymmdd_hh)
    beg_dt = cycle_dt
    end_dt = beg_dt + dt.timedelta(hours=sim_hrs)
    all_dt = pd.date_range(start=beg_dt, end=end_dt, freq=str(int_hrs)+'h')
    n_times = len(all_dt)

    ## Get the icbc model cycle (in real-time applications there may need to be an offset to stay ahead of the clock)
    icbc_cycle_dt = cycle_dt - dt.timedelta(hours=icbc_fc_dt)
    icbc_cycle_hr = icbc_cycle_dt.strftime('%H')

    ## Create the run directory if it doesn't already exist
    run_dir.mkdir(parents=True, exist_ok=True)

    ## Create the ungrib output directory if it doesn't already exist
    out_dir.mkdir(parents=True, exist_ok=True)
    if icbc_model == 'GEFS':
        out_dir.joinpath('gefs_a').mkdir(parents=True, exist_ok=True)
        out_dir.joinpath('gefs_b').mkdir(parents=True, exist_ok=True)

    ## Create the ungrib output directory(ies) if they don't already exist (and if they do, delete them first)
    if icbc_model == 'GEFS':
        if out_dir.joinpath('gefs_a').is_dir():
            shutil.rmtree(out_dir.joinpath('gefs_a'))
            out_dir.joinpath('gefs_a').mkdir(parents=True, exist_ok=True)
        if out_dir.joinpath('gefs_b').is_dir():
            shutil.rmtree(out_dir.joinpath('gefs_b'))
            out_dir.joinpath('gefs_b').mkdir(parents=True, exist_ok=True)
    else:
        if out_dir.is_dir():
            shutil.rmtree(out_dir)
            out_dir.mkdir(parents=True, exist_ok=True)

    ## Loop over times
    for tt in range(n_times):
        this_dt = all_dt[tt]
        this_dt_wrf_str = this_dt.strftime(fmt_wrf_dt)
        this_dt_wrf_date_hh = this_dt.strftime(fmt_wrf_date_hh)
        this_dt_yyyymmdd_hh = this_dt.strftime(fmt_yyyymmdd_hh)
        this_dt_yyyymmdd_hhmm = this_dt.strftime(fmt_yyyymmdd_hhmm)
        log.info('Processing date '+this_dt_yyyymmdd_hh)

        ## Calculate the lead hour for this cycle, accounting for the possible icbc_fc_dt offset
        lead_h = int((this_dt - cycle_dt).total_seconds() // 3600) + icbc_fc_dt
        lead_h_str = str(lead_h).zfill(3)

        os.chdir(run_dir)
        ## We want to ungrib everything as quickly as possible, one file per parallel job
        ## GEFS requires ungribbing two sets of files (a and b), so requires two directories
        ## HRRR would also require ungribbing two sets of files if native sigma level input is desired above-surface
        ## GFS only requires ungribbing one set of files
        if icbc_model == 'GEFS' or icbc_model == 'gefs':
            ungrib_dir = run_dir.joinpath('ungrib_'+this_dt_yyyymmdd_hh+'_b')
        else:
            ungrib_dir = run_dir.joinpath('ungrib_'+this_dt_yyyymmdd_hh)
        ungrib_dir.mkdir(parents=True, exist_ok=True)
        os.chdir(ungrib_dir)

        ## Link to ungrib-related executables & scripts
        if pathlib.Path('ungrib.exe').is_symlink():
            pathlib.Path('ungrib.exe').unlink()
        if pathlib.Path('link_grib.csh').is_symlink():
            pathlib.Path('link_grib.csh').unlink()
        pathlib.Path('ungrib.exe').symlink_to(wps_dir.joinpath('ungrib.exe'))
        pathlib.Path('link_grib.csh').symlink_to(wps_dir.joinpath('link_grib.csh'))

        ## Copy over the ungrib batch script
        shutil.copy(temp_dir.joinpath('submit_ungrib.bash'), 'submit_ungrib.bash')

        ## Link to the correct Vtable, grib/grib2 files, and copy in a namelist template
        ## (Add other elif options here as needed)
        if pathlib.Path('Vtable').is_symlink():
            pathlib.Path('Vtable').unlink()
        if pathlib.Path('namelist.wps').is_file():
            pathlib.Path('namelist.wps').unlink()
        if icbc_model == 'GFS' or icbc_model == 'gfs':
            pathlib.Path('Vtable').symlink_to(wps_dir.joinpath('ungrib','Variable_Tables','Vtable.GFS'))
            ret,output = exec_command(['./link_grib.csh',str(grib_dir)+'/*t'+icbc_cycle_hr+'z.pgrb2.0p25.f*'],log)
            shutil.copy(temp_dir.joinpath('namelist.wps.gfs'), 'namelist.wps.template')
        elif icbc_model == 'GEFS' or icbc_model == 'gefs':
            pathlib.Path('Vtable').symlink_to(wps_dir.joinpath('ungrib','Variable_Tables','Vtable.GFSENS'))
            ret,output = exec_command(['./link_grib.csh',str(grib_dir)+'/pgrb2bp5/gep'+mem_id+'.t'+icbc_cycle_hr+'z.pgrb2b.0p50.f'+lead_h_str],log)
            shutil.copy(temp_dir.joinpath('namelist.wps.gefs_b'), 'namelist.wps.template')

        ## Modify the namelist for this date, running ungrib separately on each grib file
        with open('namelist.wps.template', 'r') as in_file, open('namelist.wps', 'w') as out_file:
            for line in in_file:
                if line.strip()[0:10] == 'start_date':
                    out_file.write(" start_date = '"+this_dt_wrf_str+"',\n")
                elif line.strip()[0:8] == 'end_date':
                    out_file.write(" end_date   = '"+this_dt_wrf_str+"',\n")
                elif line.strip()[0:6] == 'prefix':
                    if icbc_model == 'GFS' or icbc_model == 'gfs':
                        out_file.write(" prefix = '"+str(out_dir)+"/GFS',\n")
                    elif icbc_model == 'GEFS' or icbc_model == 'gefs':
                        out_file.write(" prefix = '"+str(out_dir)+"/gefs_b/GEFS_B',\n")
                    else:
                        out_file.write(" prefix = '"+str(out_dir)+"/FILE',\n")
                else:
                    out_file.write(line)

        ## If the expected output file exists, delete it first
        ## This enables checking for its existence later as proof of successful completion
        if icbc_model == 'GFS' or icbc_model == 'gfs':
            ungribbed_file = out_dir.joinpath('GFS:'+this_dt_wrf_date_hh)
        elif icbc_model == 'GEFS' or icbc_model == 'gefs':
            ungribbed_file = out_dir.joinpath('gefs_b','GEFS_B:'+this_dt_wrf_date_hh)
        else:
            ungribbed_file = out_dir.joinpath('FILE:'+this_dt_wrf_date_hh)
        if ungribbed_file.is_file():
            ungribbed_file.unlink()

        ## Delete old log files
        ret,output = exec_command(['rm','ungrib.log'], log, False, False)
        files = glob.glob('log_ungrib.*')
        for file in files:
            ret,output = exec_command(['rm',file], log, False, False)
        files = glob.glob('ungrib.o*')
        for file in files:
            ret,output = exec_command(['rm',file], log, False, False)

        ## Submit ungrib and get the job ID as a string in case it's useful
        if scheduler == 'slurm':
            ret,output = exec_command(['sbatch','submit_ungrib.bash'], log)
            jobid = output.split('job ')[1].split('\\n')[0]
            log.info('Submitted batch job '+jobid)
        elif scheduler == 'pbs':
            ret,output = exec_command(['qsub','submit_ungrib.bash'], log)
            jobid = output.split('.')[0]
            queue = output.split('.')[1]
            log.info('Submitted batch job '+jobid+' to queue '+queue)
        time.sleep(short_time)

    '''
    ## Has the last ungrib job finished?
    if icbc_model == 'GFS' or icbc_model == 'gfs':
        last_file = out_dir.joinpath('GFS:'+this_dt_wrf_date_hh)
    elif icbc_model == 'GEFS' or icbc_model == 'gefs':
        last_file = out_dir.joinpath('gefs_b','GEFS_B:'+this_dt_wrf_date_hh)
    else:
        last_file = out_dir.joinpath('FILE:'+this_dt_wrf_date_hh)

    while not last_file.is_file():
        time.sleep(long_time)
    '''
    ## Loop back through the run directories, verifying that each ungrib job finished successfully
    for tt in range(n_times):
        this_dt = all_dt[tt]
        this_dt_yyyymmdd_hh = this_dt.strftime(fmt_yyyymmdd_hh)

        if icbc_model == 'GEFS' or icbc_model == 'gefs':
            ungrib_dir = run_dir.joinpath('ungrib_'+this_dt_yyyymmdd_hh+'_b')
        else:
            ungrib_dir = run_dir.joinpath('ungrib_'+this_dt_yyyymmdd_hh)
        os.chdir(ungrib_dir)

        time.sleep(short_time)

        if scheduler == 'slurm':
            ret,output = exec_command([f'{curr_dir}/check_job_status.sh',jobid], log)
        elif scheduler == 'pbs':
            log.info('WARNING: check_job_status.sh needs to be modified to handle PBS calls')

        ## First, ensure the job is running/did run and created a log file
        status = False
        while not status:
            if not pathlib.Path('ungrib.log').is_file():
                time.sleep(long_time)
            else:
                status = True
        ## Second, look for success/error messages in the log file
        status = False
        while not status:
            if search_file('ungrib.log', 'Successful completion of program ungrib.exe'):
                status = True
            else:
                ## May need to add other error keywords to search for...
                if search_file('ungrib.log','FATAL') or search_file('ungrib.log','Fatal') or search_file('ungrib.log','ERROR'):
                    log.error('ERROR: ungrib.exe failed.')
                    log.error('Consult '+str(ungrib_dir)+'/ungrib.log for potential error messages.')
                    log.error('Exiting!')
                    sys.exit(1)
                else:
                    for fname in glob.glob('log_ungrib.o*'):
                        if search_file(fname,'BAD TERMINATION')  or search_file(fname,'ERROR'):
                            log.error('ERROR: ungrib.exe failed.')
                            log.error('Consult '+str(ungrib_dir)+'/'+str(fname)+' for potential error messages.')
                            log.error('Exiting!')
                            sys.exit(1)
                time.sleep(long_time)

    ## If GEFS, run ungrib for the a files, too
    ## Could potentially merge this back in with loop above to get through all ungrib processes a bit faster,
    ## but keeping them as two separate code blocks is a little bit cleaner/easier to read. Maybe could do
    ## the same thing by making much of this code into a function.
    if icbc_model == 'GEFS' or icbc_model == 'gefs':
        
        ## Loop over times
        for tt in range(n_times):
            this_dt = all_dt[tt]
            this_dt_wrf_str = this_dt.strftime(fmt_wrf_dt)
            this_dt_wrf_date_hh = this_dt.strftime(fmt_wrf_date_hh)
            this_dt_yyyymmdd_hh = this_dt.strftime(fmt_yyyymmdd_hh)
            this_dt_yyyymmdd_hhmm = this_dt.strftime(fmt_yyyymmdd_hhmm)
            log.info('Processing date '+this_dt_yyyymmdd_hh)

            ## Calculate the lead hour for this cycle, accounting for the possible icbc_fc_dt offset
            lead_h = int((this_dt - cycle_dt).total_seconds() // 3600) + icbc_fc_dt
            lead_h_str = str(lead_h).zfill(3)

            ungrib_dir = run_dir.joinpath('ungrib_'+this_dt_yyyymmdd_hh+'_a')
            ungrib_dir.mkdir(parents=True, exist_ok=True)
            os.chdir(ungrib_dir)

            ## Link to ungrib-related executables & scripts
            if pathlib.Path('ungrib.exe').is_symlink():
                pathlib.Path('ungrib.exe').unlink()
            if pathlib.Path('link_grib.csh').is_symlink():
                pathlib.Path('link_grib.csh').unlink()
            pathlib.Path('ungrib.exe').symlink_to(wps_dir.joinpath('ungrib.exe'))
            pathlib.Path('link_grib.csh').symlink_to(wps_dir.joinpath('link_grib.csh'))

            ## Copy over the ungrib submission script
            shutil.copy(temp_dir.joinpath('submit_ungrib.bash'), 'submit_ungrib.bash')

            ## Link to the correct Vtable, grib/grib2 files, and copy in a namelist template
            if pathlib.Path('Vtable').is_symlink():
                pathlib.Path('Vtable').unlink()
            if pathlib.Path('namelist.wps').is_file():
                pathlib.Path('namelist.wps').unlink()
            pathlib.Path('Vtable').symlink_to(wps_dir.joinpath('ungrib','Variable_Tables','Vtable.GFSENS'))
            ret,output = exec_command(['./link_grib.csh',str(grib_dir)+'/pgrb2ap5/gep'+mem_id+'.t'+icbc_cycle_hr+'z.pgrb2a.0p50.f'+lead_h_str],log)
            shutil.copy(temp_dir.joinpath('namelist.wps.gefs_a'), 'namelist.wps.template')

            ## Modify the namelist for this date, running ungrib separately on each grib file
            with open('namelist.wps.template', 'r') as in_file, open('namelist.wps', 'w') as out_file:
                for line in in_file:
                    if line.strip()[0:10] == 'start_date':
                        out_file.write(" start_date = '"+this_dt_wrf_str+"',\n")
                    elif line.strip()[0:8] == 'end_date':
                        out_file.write(" end_date   = '"+this_dt_wrf_str+"',\n")
                    elif line.strip()[0:6] == 'prefix':
                        out_file.write(" prefix = '"+str(out_dir)+"/gefs_a/GEFS_A',\n")
                    else:
                        out_file.write(line)

            ## If the expected output file already exists, delete it first
            ungribbed_file = out_dir.joinpath('gefs_a','GEFS_A:'+this_dt_wrf_date_hh)
            if ungribbed_file.is_file():
                ungribbed_file.unlink()

            ## Delete old log files
            files = glob.glob('log_ungrib.*')
            for file in files:
                ret,output = exec_command(['rm',file], log, False, False)
            files = glob.glob('ungrib.o*')
            for file in files:
                ret,output = exec_command(['rm',file], log, False, False)

            ## Submit ungrib and get the job ID as a string in case it's useful
            if scheduler == 'slurm':
                ret,output = exec_command(['sbatch','submit_ungrib.bash'], log)
                jobid = output.split('job ')[1].split('\\n')[0].strip()
                log.info('Submitted batch job '+jobid)
            elif scheduler == 'pbs':
                ret,output = exec_command(['qsub','submit_ungrib.bash'], log)
                jobid = output.split('.')[0]
                queue = output.split('.')[1]
                log.info('Submitted batch job '+jobid+' to queue '+queue)
            time.sleep(short_time)

        '''
        ## Hold until the last ungrib job is finished
        last_file = out_dir.joinpath('gefs_a','GEFS_A:'+this_dt_wrf_date_hh)
        while not last_file.is_file():
            time.sleep(long_time)
        '''
        ## Loop back through the run directories, verifying that each ungrib job finished successfully
        for tt in range(n_times):
            this_dt = all_dt[tt]
            this_dt_yyyymmdd_hh = this_dt.strftime(fmt_yyyymmdd_hh)
         
            ungrib_dir = run_dir.joinpath('ungrib_'+this_dt_yyyymmdd_hh+'_a')
            os.chdir(ungrib_dir)

            ## First, ensure the job is running/did run and created a log file
            time.sleep(short_time)

            if scheduler == 'slurm':
                ret,output = exec_command([f'{curr_dir}/check_job_status.sh',jobid], log)
            elif scheduler == 'pbs':
                log.info('WARNING: check_job_status.sh needs to be modified to handle PBS calls')

            status = False
            while not status:
                if not pathlib.Path('ungrib.log').is_file():
                    time.sleep(long_time)
                else:
                    status = True
            ## Second, look for success/error messages in the log file
            status = False
            while not status:
                if search_file('ungrib.log', '*** Successful completion of program ungrib.exe ***'):
                    status = True
                else:
                    ## May need to add other error keywords to search for...
                    if search_file('ungrib.log', 'FATAL') or search_file('ungrib.log', 'Fatal'):
                        log.error('ERROR: ungrib.exe failed.')
                        log.error('Consult '+str(ungrib_dir)+'/ungrib.log for potential error messages.')
                        log.error('Exiting!')
                        sys.exit(1)
                    else:
                        for fname in glob.glob('log_ungrib.o*'):
                            if search_file('log_ungrib.o'+jobid, 'BAD TERMINATION'):
                                log.error('ERROR: ungrib.exe failed.')
                                log.error('Consult '+str(ungrib_dir)+'/log_ungrib.o'+jobid+' for potential error messages.')
                                log.error('Exiting!')
                                sys.exit(1)
                    time.sleep(long_time)

        ## Go to the main output directory, and link all the GEFS_A and GEFS_B files there for simplicity downstream
        ## First wait a bit to give straggler processes a chance to finish
        time.sleep(long_long_time)
        os.chdir(out_dir)
        files = glob.glob('GEFS_A*')
        for file in files:
            ret,output = exec_command(['rm',file], log, False, False)
        files = glob.glob('GEFS_B*')
        for file in files:
            ret,output = exec_command(['rm',file], log, False, False)

        files = glob.glob('gefs_a/GEFS_A*')
        for file in files:
            ret,output = exec_command(['ln','-sf',file,'.'], log)
        files = glob.glob('gefs_b/GEFS_B*')
        for file in files:
            ret,output = exec_command(['ln','-sf',file,'.'], log)

    log.info('SUCCESS! All ungrib jobs completed successfully.')


if __name__ == '__main__':
    now_time_beg = dt.datetime.utcnow()
    cycle_dt, sim_hrs, wps_dir, run_dir, out_dir, grib_dir, temp_dir, icbc_model, int_hrs, icbc_fc_dt, scheduler, mem_id = parse_args()
    main(cycle_dt, sim_hrs, wps_dir, run_dir, out_dir, grib_dir, temp_dir, icbc_model, int_hrs, icbc_fc_dt, scheduler, mem_id)
    now_time_end = dt.datetime.utcnow()
    run_time_tot = now_time_end - now_time_beg
    now_time_beg_str = now_time_beg.strftime('%Y-%m-%d %H:%M:%S')
    now_time_end_str = now_time_end.strftime('%Y-%m-%d %H:%M:%S')
    log.info('run_ungrib.py completed successfully.')
    log.info('Beg time: '+now_time_beg_str)
    log.info('End time: '+now_time_end_str)
    log.info('Run time: '+str(run_time_tot)+'\n')
