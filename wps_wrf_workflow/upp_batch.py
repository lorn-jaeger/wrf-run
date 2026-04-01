#!/usr/bin/env python3

'''
upp_batch.py

Created by: Padhrig McCarthy (paddy@ucar.edu)
Created on: 14 August 2023

Process many wrfout files as a single parallel job. Suitable for running on a single node of the cluster.
Can be called on the entire WRF output directory, or individual domains in the output.
Breaks the job into single-file tasks and executes them with joblib.
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

# Set this to True to have each parallel process log to 'UPP_debug.log'.
#   Otherwise, only the boss process writes logs.
debug = False

this_file = os.path.basename(__file__)
logging.basicConfig(format=f'{this_file}: %(asctime)s - %(message)s',
                    level=logging.DEBUG, datefmt='%Y-%m-%dT%H:%M:%S')
log = logging.getLogger(__name__)
curr_dir=os.path.dirname(os.path.abspath(__file__))

def parse_args():
    yaml_config_help = {
     # 'run_dir': 'string or Path object of the WRF run directory holding the wrfout files to be processed (default: ./)',
     'working_dir': 'string or Path object that hosts subdirectories where each of the individual UPP processes is run (default: /tmp)',
     'output_dir': 'string or Path object to place the UPP output (output will be placed in a subdir named with YYYYMMDD_HH of the WRF run init) (default: ./output/)',
     'upp_dir': 'string or Path object of the UPP install directory (default: ./)',
     'itag_template': 'string or Path object referring to itag template file',
     #Add new parameters here
    }

    ## Parse the command-line arguments
    usage = ("Usage: upp_batch [options]\n\n"
             "Converts wrfout_* files in a directory to grib2.")
    parser = argparse.ArgumentParser(usage=usage)
    parser.add_argument('-r', '--run_dir', default=None, help='string or Path object of the WRF run directory to find wrfout files for processing')
    parser.add_argument('-x', '--exp_name', default=None, help='string indicating the experiment name -- used for naming files and directories')
    # parser.add_argument('-c', '--config', required=True, help=f"yaml configuration file\n{yaml.dump(yaml_config_help, default_flow_style=False)}")
    parser.add_argument('-w', '--working_dir', default=None, help='string or Path to the working directory')
    parser.add_argument('-o', '--output_dir', default=None, help='string or Path indicating the output directory')
    parser.add_argument('-u', '--upp_dir', default=None, help='string or Path indicating location of the upp build directory (has parm/ and exec/ with upp.x)')
    parser.add_argument('-i', '--itag_template', default=None, help='string or Path to the UPP itag template file')
    parser.add_argument('-d', '--domain_idx', default=0, help='(optional) integer indicating a single domain to process from the wrfout files. Otherwise all domains are processed')
    parser.add_argument('-g', '--grib2_rsync_target', default='', help='(optional) string indicating directory for rsync of grib2 data')
    parser.add_argument('-N', '--no_cleanup', action="store_true", default=False, help='(optional) for debugging purposes, do not remove files in the temporary directory')

    args = parser.parse_args()

    if not args.run_dir:
        print('ERROR! run_dir not specified as an argument in call to run_upp.py. Exiting!')
        parser.print_help()
        sys.exit(1)
    if not args.exp_name:
        print('ERROR! exp_name not specified as an argument in call to run_upp.py. Exiting!')
        parser.print_help()
        sys.exit(1)

    # Move command line to params dictionary
    params = {}
    params['run_dir'] = pathlib.Path(args.run_dir)
    params['exp_name'] = args.exp_name
    params['working_dir'] = pathlib.Path(args.working_dir)
    params['output_dir'] = pathlib.Path(args.output_dir)
    params['upp_dir'] = pathlib.Path(args.upp_dir)
    params['itag_template'] = pathlib.Path(args.itag_template)
    params['domain_idx'] = args.domain_idx
    params['grib2_rsync_target'] = args.grib2_rsync_target
    params['no_cleanup'] = args.no_cleanup

    return params

def setup_logging():
    """Set up logging (for child workers)."""
    if debug:
        file_handler = logging.FileHandler(filename="UPP_debug.log")
    else:
        file_handler = logging.FileHandler(filename="/dev/null")

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

def main(exp_name: str, run_dir: Path, working_dir: Path, output_dir: Path, upp_dir: Path, itag_template: Path, domain_idx: int, grib2_rsync_target: str, no_cleanup: bool):

    log.info(f'Running upp_batch.py from directory: {curr_dir}')

    # run_dir = '/ipchome/masiheghdami/scratch/wrf_8nodes/20220801_00/mem01_nwpdiag/'
    # working_dir = '/ipcscratch/pmccarthy/upp_test_processing/'
    # output_dir = '/ipcscratch/pmccarthy/upp_test_output/'
    # upp_dir = '/ipchome/pmccarthy/SRW_build/UPP'
    # itag_template = '/ipchome/pmccarthy/scratch/UPP_Testing/config/itag.tmpl'
    upp_parm_dir = pathlib.Path(f'{upp_dir}/parm')
    upp_exec_dir = pathlib.Path(f'{upp_dir}/exec')
    upp_exec = pathlib.Path(f'{upp_exec_dir}/upp.x')

    domain_str = '' if domain_idx == 0 else f'{domain_idx}'

    # Include 30-minute files
    # rpaths = glob.glob(os.path.join(run_dir, f"wrfout_d0{domain_str}*"))

    # Process only hourly files
    rpaths = glob.glob(os.path.join(run_dir, f"wrfout_d0{domain_str}*:00:00"))

    rpaths.sort()

    log.info(f'Found {len(rpaths)} wrfout files in wrf run dir: {run_dir}')
    match = os.path.join(run_dir, f"wrfout_d0{domain_str}*")
    log.info(f'    glob match: {match}')

    # Generate a model run datetime based on the first rpath (assume this contains the model init time).
    #   This is used to generate output filenames, because a bug in UPP creates filenames
    #     like "WRFPRS.GrbF**.30" for output timesteps on the half hour that have a three-digit forecast hour.
    if len(rpaths) < 1:
        success = False
        return success
        sys.exit(1)
    init_file = rpaths[0]
    domain, year, month, day, hour, minute, second = parseWrfoutFilename(init_file)
    run_datetime = dt.datetime(year=int(year), month=int(month), day=int(day), hour=int(hour), minute=int(minute))

    # Log the rsync command that will be used for the output data, if a target was specified.
    if len(grib2_rsync_target) > 2:
        tmp_output_dir = construct_output_path_for_run(output_dir, run_datetime, exp_name)
        tmp_day_dir = run_datetime.strftime("%Y%m%d")
        log.info(f'This process will attempt to rsync grib2 output using the command:')
        log.info(f'    rsync -avz {tmp_output_dir} {grib2_rsync_target}/{tmp_day_dir}/')

    # TODO: Truncate list of files to process (for testing)...
    # rpaths = rpaths[0: 1]
    # rpaths = rpaths[0: 5]

    log.info(f'Processing {len(rpaths)} wrfout files...')
    for wrfout in rpaths:
        log.info(f'    {wrfout}')

    jobs = (joblib.delayed(prep_and_run_upp)(run_datetime, exp_name, rpath, working_dir, output_dir, itag_template, upp_parm_dir, upp_exec, no_cleanup) for rpath in rpaths)
    joblib.Parallel(n_jobs=48)(jobs)

    # Cleanup (unless this is suppressed for debugging purposes)
    parent_processing_dir = construct_output_path_for_run(working_dir, run_datetime, exp_name, is_working_dir=True)
    if not no_cleanup:
        run_identifier = run_datetime.strftime("%Y%m%d_%H")
        # No!
        # parent_processing_dir = f'{working_dir}/{run_identifier}'

        # /ipcshare/ncar-ensemble/upp-tmp/upp_20230901/06z-WRF-mem01
        parent_processing_dir = construct_output_path_for_run(working_dir, run_datetime, exp_name, is_working_dir=True)
        log.info(f'Done processing {len(rpaths)} wrfout files. Removing parent tmp dir for this run: {parent_processing_dir}')
        shutil.rmtree(parent_processing_dir)

    if len(grib2_rsync_target) > 2:
        # Copy output directory to borah-ldm001:/data/GRIBMET/BORAH/<day_dir>/
        # Use this method to copy the whole directory after all the files are created.
        t0 = pytime.perf_counter()
        final_output_dir = construct_output_path_for_run(output_dir, run_datetime, exp_name)
        day_dir = run_datetime.strftime("%Y%m%d")
        log.info(f'Using rsync to copy files from {final_output_dir} to {grib2_rsync_target}/{day_dir}/')
        ret, output = exec_command(['rsync', '-avz', final_output_dir, f'{grib2_rsync_target}/{day_dir}/'], log)

        t1 = pytime.perf_counter()
        log.info("  Time to copy grib2 output to /data/GRIBMET/BORAH/: %s", round(t1 - t0, 3))

    success = True
    return success

def prep_and_run_upp(run_datetime: dt.datetime, exp_name: str, rpath: str, working_dir: Path, output_dir: Path, itag_template: Path, upp_parm_dir: Path, upp_exec: Path, suppress_cleanup: bool):
    """Process netCDF at ``rpath`` by running UPP."""

    logger = setup_logging()

    t0 = pytime.perf_counter()

    if debug: 
        logger.info(f"Processing file: {rpath}")

    # Get date components of this output timestep
    domain, year, month, day, hour, minute, second = parseWrfoutFilename(rpath)
    this_datetime = dt.datetime(year=int(year), month=int(month), day=int(day), hour=int(hour), minute=int(minute))

    # Get just the wrfout filename...
    wrfout_filename = os.path.basename(rpath)

    # Create processing dir for this job
    parent_processing_dir = construct_output_path_for_run(working_dir, run_datetime, exp_name, is_working_dir=True)
    processing_dir = pathlib.Path(f"{parent_processing_dir}/{wrfout_filename}")
    if processing_dir.exists():
        shutil.rmtree(processing_dir)
    processing_dir.mkdir(parents=True, exist_ok=True)

    # Create an itag file from the itag_template_file
    f_itag_in = open(itag_template, 'r')
    f_itag = open(f'{processing_dir}/itag', 'w')
    for line in f_itag_in:
        # Substitute fileName='FILE_NAME' with fileName='/path/to/wrfout_blah'
        line = line.replace("FILE_NAME", rpath)
        # Substitute DateStr='DATE_STR' with something like DateStr='2022-08-03_08:30:00'
        line = line.replace("DATE_STR", f'{year}-{month}-{day}_{hour}:{minute}:{second}')

        # Alternate method, now that we have a datetime for this file...
        # this_datetime_str = this_datetime.strftime("%Y-%m-%d_%H:%M:%S")
        # line = line.replace("DATE_STR", f'{year}-{month}-{day}_{hour}:{minute}:{second}')

        f_itag.write(line)
    f_itag.close()

    # Link in params from the UPP build area

    # TODO: Note the name change for this file -- we probably want to parameterize this so user can specify different output configs.
    # Note the Path.symlink_to() is reversed from unix, where mv and ln both follow the same direction.
    Path(f'{processing_dir}/postxconfig-NT.txt').symlink_to(Path(f'{upp_parm_dir}/postxconfig-NT-ipc.txt'))
    # os.symlink(f'{upp_parm_dir}/postxconfig-NT-ipc.txt', f'{processing_dir}/postxconfig-NT.txt')

    Path(f'{processing_dir}/post_avblflds.xml').symlink_to(Path(f'{upp_parm_dir}/post_avblflds.xml'))
    Path(f'{processing_dir}/params_grib2_tbl_new').symlink_to(Path(f'{upp_parm_dir}/params_grib2_tbl_new'))
    Path(f'{processing_dir}/nam_micro_lookup.dat').symlink_to(Path(f'{upp_parm_dir}/nam_micro_lookup.dat'))
    Path(f'{processing_dir}/hires_micro_lookup.dat').symlink_to(Path(f'{upp_parm_dir}/hires_micro_lookup.dat'))

    t1 = pytime.perf_counter()

    # Run UPP
    # ret, output = exec_command(['sbatch', 'submit_upp.bash'], log)
    # >> / ipchome / pmccarthy / SRW_build / UPP / exec / upp.x & > upp.log
    os.chdir(processing_dir)
    ret, output = exec_command([upp_exec], logger)

    # Move output from this job to output_dir
    # final_output_dir = pathlib.Path(f"{output_dir}/{run_identifier}")
    final_output_dir = construct_output_path_for_run(output_dir, run_datetime, exp_name)

    # if final_output_dir.exists():
    #     shutil.rmtree(final_output_dir)
    final_output_dir.mkdir(parents=True, exist_ok=True)
    gribfiles = glob.glob(f"{processing_dir}/WRFPRS.*")
    if len(gribfiles) == 1:
        src_file = Path(gribfiles.pop())
        src_file_name = os.path.basename(src_file)

        # Bug in UPP produces filenames like "WRFPRS.GrbF**.30" for output timesteps on the
        #   half hour that have a three-digit forecast hour.
        #
        # Calculate the forecast hour and minutes and manually create a valid filename.
        diff = this_datetime - run_datetime
        total_hours = (diff.days * 24 + math.floor(diff.seconds / 3600))
        extra_mins = (diff.days*1440 + diff.seconds/60) % 60
        upp_pattern = '(?P<prefix>.*)\\.GrbF.*'
        upp_m = re.search(upp_pattern, src_file_name)
        assert upp_m, "File name didn't match wrfout with date pattern:\n    %s\n    (?P<prefix>.*)\\.GrbF.*" % src_file_name
        upp_d = upp_m.groupdict()
        prefix = upp_d['prefix']

        # IPC uses filenames like: WRFPRS_d02.083.30.grib2
        # Leave the .minutes off if forecast length has hours only.
        extra_mins_str = ''
        if extra_mins > 0:
            extra_mins_str = f'.{int(extra_mins):02}'
        corrected_file_name = f'{prefix}_d0{domain}.{total_hours:03}{extra_mins_str}.grib2'

        dst_file = Path(f'{final_output_dir}/{corrected_file_name}')
        if suppress_cleanup:
            shutil.copy(src_file, dst_file)
        else:
          src_file.rename(dst_file)
    else:
        logger.error(f'UPP produced {len(gribfiles)} files. Not moving file to output directory.')

    t2 = pytime.perf_counter()

    # # Copy output file to borah-ldm001:/data/GRIBMET/BORAH/<day_dir>/
    # # Use this method to copy each file after it is created.
    # final_dir = Path(f'{final_output_dir}')   
    # day_dir = run_datetime.strftime("%Y%m%d")
    #  
    # logger.info(f'Using rsync to copy files from {final_dir} to borah-ldm001:/data/GRIBMET/BORAH/{day_dir}')
    # ret, output = exec_command(['rsync', '-avz', final_dir, f'borah-ldm001:/data/GRIBMET/BORAH/{day_dir}'], logger)

    t3 = pytime.perf_counter()
    logger.info("Create grib2 output for %s in %s secs.", rpath, round(t3 - t0, 3))
    logger.info("  Time to create output dir: %s", round(t1 - t0, 3))
    logger.info("  Time to run UPP: %s", round(t2 - t1, 3))
    # logger.info("  Time to copy grib2 output to /data/GRIBMET/BORAH/: %s", round(t3 - t2, 3))


def construct_parent_output_path_for_run(root_dir, run_datetime, exp_name, is_working_dir=False):
    '''
        Construct a parent output or tmp path for this run, based on root directory and run datetime. Used to construct
        both tmp and output directory paths.
            Example:
                parent_dir/{DAY_DIR}
              -or-
                parent_dir/upp_{DAY_DIR}
    '''
    run_day = run_datetime.strftime("%Y%m%d")
    if is_working_dir:
        full_output_path = pathlib.Path(f"{root_dir}/upp_{run_day}")
    else:
        full_output_path = pathlib.Path(f"{root_dir}/{run_day}")

    return full_output_path

def construct_output_path_for_run(root_dir, run_datetime, exp_name, is_working_dir=False):
    '''
        Construct an output or tmp path for this run, based on root directory, run datetime, and experiment name.
        Used to construct both tmp and output directory paths.
            Example:
                root_dir/{DAY_DIR}/{INIT_HOUR}z-WRF-{EXP_NAME}
              -or-
                root_dir/upp_{DAY_DIR}/{INIT_HOUR}z-WRF-{EXP_NAME}
    '''
    run_hour = run_datetime.strftime("%H")
    parent_dir = construct_parent_output_path_for_run(root_dir, run_datetime, exp_name, is_working_dir)
    full_output_path = pathlib.Path(f"{parent_dir}/{run_hour}z-WRF-{exp_name}")

    return full_output_path

if __name__ == '__main__':

    now_time_beg = dt.datetime.now(dt.UTC)

    params = parse_args()
    main(**params)

    now_time_end = dt.datetime.now(dt.UTC)
    run_time_tot = now_time_end - now_time_beg
    now_time_beg_str = now_time_beg.strftime('%Y-%m-%d %H:%M:%S')
    now_time_end_str = now_time_end.strftime('%Y-%m-%d %H:%M:%S')
    log.info(f'upp_batch.py completed successfully.')
    log.info('Beg time: '+now_time_beg_str)
    log.info('End time: '+now_time_end_str)
    log.info('Run time: '+str(run_time_tot)+'\n')
