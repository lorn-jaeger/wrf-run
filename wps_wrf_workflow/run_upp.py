#!/usr/bin/env python3

'''
run_upp.py

Created by: Padhrig McCarthy (paddy@ucar.edu)
Created on: 26 June 2023

Process wrfout files for a WRF run, using sbatch.
Prepare sbatch file(s) that call upp_batch.py, then send the job(s) to the cluster.
Can be called on the entire WRF output directory, or individual domains in the output. Each domain is processed on
  its own node in the cluster.

Call summary:
  ==> run_upp.py 20230814_12 on d02
        - creates 'submit_upp_20230812_12_d02.bash'
        - calls 'sbatch -N 1 submit_upp_20230812_12_d02.bash' (runs on a single node)
          ==> upp_batch.py 20230814_12 on d02
                - breaks the job into tasks that each process one file
                - runs the tasks in parallel using joblib
'''

import os
import sys
import re
import joblib
from pathlib import Path

import math
import time as pytime
import shutil
import argparse
import pathlib
import glob
import datetime as dt
import logging
import yaml

from proc_util import exec_command

this_file = os.path.basename(__file__)
logging.basicConfig(format=f'{this_file}: %(asctime)s - %(message)s',
                    level=logging.DEBUG, datefmt='%Y-%m-%dT%H:%M:%S')
log = logging.getLogger(__name__)

long_time = 5
long_long_time = 15
short_time = 3
curr_dir=os.path.dirname(os.path.abspath(__file__))

def list_of_ints(arg):
    return list(map(int, arg.split(',')))

def parse_args():
    yaml_config_help = {
     'working_dir': 'string or Path object that hosts subdirectories where each of the individual UPP processes is run (default: /tmp)',
     'output_dir': 'string or Path object to place the UPP output (output will be placed in a subdir named with YYYYMMDD_HH of the WRF run init) (default: ./output/)',
     'upp_dir': 'string or Path object of the UPP install directory (default: ./)',
     'itag_template': 'string or Path object referring to itag template file',
     'sbatch_template': 'string or Path object referring to upp sbatch template file',
     #Add new parameters here
    }

    ## Parse the command-line arguments
    usage = ("Usage: run_upp [options]\n\n"
             "Sets up sbatch job for UPP post-processing of wrfout files from a single WRF run.")
    parser = argparse.ArgumentParser(usage=usage)
    parser.add_argument('-b', '--cycle_dt', default=None, help='beginning date/time of the WRF model cycle [YYYYMMDD_HH]')
    parser.add_argument('-r', '--run_dir', default=None, help='string or Path object of the WRF run directory to find wrfout files for processing')
    parser.add_argument('-x', '--exp_name', default=None, help='string indicating the experiment name -- used for naming files and directories')
    parser.add_argument('-c', '--config', required=True, help=f"yaml configuration file\n{yaml.dump(yaml_config_help, default_flow_style=False)}")
    parser.add_argument('-d', '--domains', default=[], type=list_of_ints, help='(optional) comma-separated list of integers indicating domains to process from the wrfout files. Otherwise all domains are processed')
    parser.add_argument('-N', '--no_cleanup', action="store_true", help='(optional) for debugging purposes, do not remove files in the temporary directory')

    args = parser.parse_args()

    if not args.cycle_dt:
        print('ERROR! cycle_dt not specified as an argument in call to run_upp.py. Exiting!')
        parser.print_help()
        sys.exit(1)
    if not args.run_dir:
        print('ERROR! run_dir not specified as an argument in call to run_upp.py. Exiting!')
        parser.print_help()
        sys.exit(1)
    if not args.exp_name:
        print('ERROR! exp_name not specified as an argument in call to run_upp.py. Exiting!')
        parser.print_help()
        sys.exit(1)
    if not args.config:
        print('ERROR! config not specified as an argument in call to run_upp.py. Exiting!')
        parser.print_help()
        sys.exit(1)

    with open(args.config) as yaml_f:
        params = yaml.safe_load(yaml_f)
    log.info(f"yaml params: {params}")
    params.setdefault('cycle_dt', None)
    params.setdefault('run_dir', './')
    params.setdefault('exp_name', 'NONAME') # Use Colorado I-70 Exit 119 name...
    params.setdefault('working_dir', '/tmp/upp')
    params.setdefault('output_dir', './output')
    params.setdefault('upp_dir', None)
    params.setdefault('itag_template', None)
    params.setdefault('sbatch_template', None)
    params.setdefault('domains', []) # empty list == process all domains
    params.setdefault('do_grib2_rsync', False)
    params.setdefault('grib2_rsync_target', '')
    params.setdefault('no_cleanup', False)

    # TODO: Check that the cycle_dt is realistic...

    params['run_dir'] = pathlib.Path(params['run_dir'])
    params['working_dir'] = pathlib.Path(params['working_dir'])
    params['output_dir'] = pathlib.Path(params['output_dir'])
    params['upp_dir'] = pathlib.Path(params['upp_dir'])
    params['itag_template'] = pathlib.Path(params['itag_template'])
    params['sbatch_template'] = pathlib.Path(params['sbatch_template'])

    # Overwrite params with anything provided on the command line
    params['cycle_dt'] = args.cycle_dt
    params['run_dir'] = pathlib.Path(args.run_dir)
    params['exp_name'] = args.exp_name
    params['domains'] = args.domains
    params['no_cleanup'] = args.no_cleanup

    return params

def setup_logging():
    """Set up logging (for child workers)."""
    file_handler = logging.FileHandler(filename="UPP_run.log")
    handlers = [file_handler]
    logging.basicConfig(
        level="INFO",
        format="[%(process)5s] [%(asctime)s] [%(levelname)5s] [%(name)10s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=handlers,
    )
    logging.captureWarnings(True)

    logger = logging.getLogger(__name__)
    logger.setLevel("DEBUG")
    return logger

def parseWrfoutFilename(rpath: str):
    """Parse domain and date components out of a wrfout file name -- full paths are allowed here.
    Return parsed components as strings, so they maintain their zero-filled status."""

    pattern = "wrfout_d0(?P<domain>[0-9])_(?P<yr>[0-9]{4})-(?P<mo>[0-9]{2})-(?P<da>[0-9]{2})_(?P<hr>[0-9]{2}):(?P<mn>[0-9]{2}):(?P<sc>[0-9]{2})"
    m = re.search(pattern, rpath)
    assert m, "File name didn't match wrfout with date pattern:\n    %s\n    wrfout_d0(?P<domain>[0-9])_(?P<yr>[0-9]{4})-(?P<mo>[0-9]{2})-(?P<da>[0-9]{2})_(?P<hr>[0-9]{2}):(?P<mn>[0-9]{2}):(?P<sc>[0-9]{2})" % rpath
    d = m.groupdict()
    domain = d['domain']
    year = d["yr"]
    month = d["mo"]
    day = d["da"]
    hour = d["hr"]
    minute = d["mn"]
    second = d["sc"]

    return domain, year, month, day, hour, minute, second

def main(cycle_dt: str, exp_name: str, run_dir: Path, working_dir: Path, output_dir: Path, upp_dir: Path, itag_template: Path, sbatch_template: Path, domains: list[int], do_grib2_rsync: bool, grib2_rsync_target: str, no_cleanup: bool):

    # Get a full path to this script, so it can be put in the sbatch file.
    this_path = pathlib.Path(__file__).parent.resolve()
    upp_batch_path = os.path.join(this_path, "upp_batch.py")

    # Create submit_upp.bash script(s)
    submitfile_paths = create_sbatch_files_from_tmpl(sbatch_template, cycle_dt, upp_batch_path, run_dir, exp_name, working_dir, output_dir, upp_dir, itag_template, domains, do_grib2_rsync, grib2_rsync_target, no_cleanup)

    # Submit the jobs to sbatch
    submitted_jobids = []
    for upp_submitfile in submitfile_paths:
        ret, output = exec_command(['sbatch', upp_submitfile], log)
        jobid = output.split('job ')[1].split('\\n')[0].strip()
        submitted_jobids.append(jobid)
        log.info(f'Submitted UPP batch job via "sbatch {upp_submitfile}": ' + jobid)

    # Monitor for completion of all jobs

    # TODO: Remove after testing run_upp process monitoring...
    log.info(f'Sleeping for {long_time} s...')

    pytime.sleep(long_time)  # give the file system a moment

    # TODO: Remove after testing run_wrf process monitoring...
    log.info('Checking status of all jobs:')

    for jobid in submitted_jobids:
        # TODO: Remove after testing run_wrf process monitoring...
        log.info(f'    Checking job {jobid} status...')

        ret, output = exec_command([f'{curr_dir}/check_job_status.sh', jobid], log)

        # TODO: Remove after testing run_wrf process monitoring...
        log.info(f'        Got: {ret}')

    # TODO: Remove after testing run_wrf process monitoring...
    log.info(f'Started {len(submitted_jobids)} jobs.')
    log.info('')

    status = False
    while not status:
        for jobid in submitted_jobids[:]:
            job_log_filename = 'log_upp.o' + jobid
            if not pathlib.Path(job_log_filename).is_file():
                log.info(f'    No "{job_log_filename}" file present. Sleeping for {long_time} s...')
                pytime.sleep(long_time)
            else:
                log.info(f'upp job {jobid} is now running on the cluster . . .')
                status = True

    # TODO: Remove after testing run_wrf process monitoring...
    log.info(f'Found job logfiles for {len(submitted_jobids)} jobs.')
    log.info('')

    # TODO: Remove after testing run_wrf process monitoring...
    log.info(f'Waiting for completion of all jobs:')

    timeout = 3600
    t0 = pytime.perf_counter()
    while len(submitted_jobids) > 0:


        t1 = pytime.perf_counter()
        log.info(f'Total time for UPP so far: {round(t1 - t0, 3)}.')

        if (t1 - t0) >= timeout:
            print('ERROR! Timeout reached for run_upp. Exiting!')
            print('ERROR! The following job ids are still running, or exited without detection: ')
            
            for remaining_jobid in submitted_jobids:
                print(f'          Jobid: {remaining_jobid}')
            sys.exit(1)

        for jobid in submitted_jobids[:]:
            job_log_filename = 'log_upp.o' + jobid
            
            # TODO: Remove after testing run_wrf process monitoring...
            log.info(f'    Checking for "upp_batch.py completed successfully" in {job_log_filename}...')
            
            if 'upp_batch.py completed successfully' in open(job_log_filename).read():
                submitted_jobids.remove(jobid)
                log.info(f'        SUCCESS! UPP job {jobid} completed successfully. {len(submitted_jobids)} UPP jobs still running...')
                pytime.sleep(short_time)  # brief pause
            else:
                # The log files might be empty for a time, which may cause an error if attempting to read it
                if os.stat(job_log_filename) == 0:
                    # TODO: Remove after testing run_wrf process monitoring...
                    log.info(f'    No {job_log_filename} file is present. Sleeping for {long_time} s...')
                    pytime.sleep(long_time)
                else:
                    if os.path.exists(job_log_filename) and 'ERROR' in open(job_log_filename).read():
                        log.error('    ERROR: UPP failed.')
                        log.error('    Consult ' + str(this_path) + '/' + job_log_filename + ' for potential error messages.')
                        log.error('    Exiting!')
                        sys.exit(1)
    
                    # TODO: Remove after testing run_wrf process monitoring...
                    log.info(f'    File "{job_log_filename}" is free of ERROR messages. Sleeping for {long_time} s...')
    
                    pytime.sleep(long_time)

    success = True
    return success


def create_sbatch_files_from_tmpl(submit_upp_tmpl: pathlib.Path, cycle_str: str, run_upp_script: pathlib.Path, wrf_run_dir: pathlib.Path, exp_name: str, working_dir: pathlib.Path, output_dir: pathlib.Path, upp_dir: pathlib.Path, itag_tmpl: pathlib.Path, domains: list[str], do_grib2_rsync: bool, grib2_rsync_target: str, no_cleanup: bool):
    submitfile_paths = []

    if domains and len(domains) > 0 and domains[0] > 0:
        for domain in domains:
            submit_file_path = f'submit_upp_{cycle_str}_{exp_name}_d0{domain}.bash'
            fill_tmpl_wildcards(submit_upp_tmpl, submit_file_path, run_upp_script, wrf_run_dir, exp_name, working_dir, output_dir, upp_dir, itag_tmpl, str(domain), do_grib2_rsync, grib2_rsync_target, no_cleanup)
            submitfile_paths.append(submit_file_path)
    else:
        submit_file_path = f'submit_upp_{cycle_str}_{exp_name}.bash'
        fill_tmpl_wildcards(submit_upp_tmpl, submit_file_path, run_upp_script, wrf_run_dir, exp_name, working_dir, output_dir, upp_dir, itag_tmpl, '', do_grib2_rsync, grib2_rsync_target, no_cleanup)
        submitfile_paths.append(submit_file_path)

    return submitfile_paths

def fill_tmpl_wildcards(tmpl_path: str, submit_file_path: str, run_upp_script: pathlib.Path, wrf_run_dir: pathlib.Path, exp_name: str, working_dir: pathlib.Path, output_dir: pathlib.Path, upp_dir: pathlib.Path, itag_tmpl: pathlib.Path, domain: str, do_grib2_rsync: bool, grib2_rsync_target: str, no_cleanup: bool):
    tmpl = open(tmpl_path, 'r')
    submit_file = open(submit_file_path, 'w')
    for line in tmpl:
        line = line.replace("THIS_FILE_NAME", str(submit_file_path))
        line = line.replace("RUN_UPP_SCRIPT", str(run_upp_script))
        line = line.replace("EXP_NAME", exp_name)
        line = line.replace("WRF_RUN_DIR", str(wrf_run_dir))
        line = line.replace("WORKING_DIR", str(working_dir))
        line = line.replace("OUTPUT_DIR", str(output_dir))
        line = line.replace("UPP_DIR", str(upp_dir))
        line = line.replace("ITAG_TEMPLATE", str(itag_tmpl))
        if do_grib2_rsync and len(grib2_rsync_target) > 2:
            line = line.replace("GRIB2_RSYNC_ARGS", f'-g {grib2_rsync_target}')
        else:
            line = line.replace("GRIB2_RSYNC_ARGS", '')

        if len(domain) < 1:
            if no_cleanup:
                line = line.replace("-d DOMAIN_IDX", '-N')
            else:
                line = line.replace("-d DOMAIN_IDX", '')
        else:
            if no_cleanup:
                line = line.replace("-d DOMAIN_IDX", f'-d {domain} -N')
            else:
                line = line.replace("-d DOMAIN_IDX", f'-d {domain}')

        submit_file.write(line)
    submit_file.close()

    return submit_file_path

if __name__ == '__main__':

    now_time_beg = dt.datetime.now(dt.UTC)

    params = parse_args()
    main(**params)

    now_time_end = dt.datetime.now(dt.UTC)
    run_time_tot = now_time_end - now_time_beg
    now_time_beg_str = now_time_beg.strftime('%Y-%m-%d %H:%M:%S')
    now_time_end_str = now_time_end.strftime('%Y-%m-%d %H:%M:%S')
    log.info('run_upp.py completed successfully.')
    log.info('Beg time: '+now_time_beg_str)
    log.info('End time: '+now_time_end_str)
    log.info('Run time: '+str(run_time_tot)+'\n')

