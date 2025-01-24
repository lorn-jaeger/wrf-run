#!/usr/bin/env python3

'''
download_gefs_from_aws.py

Created by: Jared A. Lee (jaredlee@ucar.edu)
Created on: 3 Mar 2023

This script downloads GEFS output files for the requested cycle(s), member(s), and lead times.
'''

import os
import sys
import argparse
import pathlib
import datetime as dt
import numpy as np
import pandas as pd
import wget
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
    parser.add_argument('-b','--cycle_dt', default='20220801_00', help='GEFS cycle date/time to download [YYYYMMDD_HH] (default: 20220801_00)')
    parser.add_argument('-s', '--sim_hrs', default=192, type=int, help='integer number of forecast hours to download (default: 192)')
    parser.add_argument('-m', '--members', default='01', help='GEFS ensemble member(s) to download. If requesting multiple members, separate them by commas only (e.g., 01,02). (default: 01)')
    parser.add_argument('-o', '--out_dir_parent', default=None, help='string or pathlib.Path object of the parent local directory where all downloaded GEFS data should be stored')
    parser.add_argument('-f', '--icbc_fc_dt', default=0, type=int, help='integer number of hours prior to WRF cycle time for IC/LBC model cycle (default: 0)')
    parser.add_argument('-i', '--int_h', default=3, type=int, help='integer number of hours between GEFS files to download (default: 3)')

    args = parser.parse_args()
    cycle_dt = args.cycle_dt
    sim_hrs = args.sim_hrs
    members_inp = args.members
    out_dir_parent = args.out_dir_parent
    icbc_fc_dt = args.icbc_fc_dt
    int_h = args.int_h

    members = members_inp.split(',')

    if len(cycle_dt) != 11:
        log.error('ERROR! Incorrect length for optional argument cycle_dt. Exiting!')
        parser.print_help()
        sys.exit(1)
    elif cycle_dt[8] != '_':
        log.error('ERROR! Incorrect format for opitional argument cycle_dt. Exiting!')
        parser.print_help()
        sys.exit(1)

    if out_dir_parent is not None:
        out_dir_parent = pathlib.Path(out_dir_parent)
    else:
        ## Make a default assumption about where to put the files
        out_dir_parent = pathlib.Path('/','glade','derecho','scratch','jaredlee','data','gefs',cycle_dt)
        log.info('Using the default assumption for out_dir_parent: '+str(out_dir_parent))

    return cycle_dt, sim_hrs, members, out_dir_parent, icbc_fc_dt, int_h

def wget_error(error_msg, now_time_beg):
    log.error('ERROR: '+error_msg)
    log.error('Check if an earlier cycle has the required files and adjust icbc_fc_dt if necessary. Exiting!')
    now_time_end = dt.datetime.utcnow()
    run_time_tot = now_time_end - now_time_beg
    now_time_beg_str = now_time_beg.strftime('%Y-%m-%d %H:%M:%S')
    now_time_end_str = now_time_end.strftime('%Y-%m-%d %H:%M:%S')
    log.error('\nScript completed with an error.')
    log.error('   Beg time: '+now_time_beg_str)
    log.error('   End time: '+now_time_end_str)
    log.error('   Run time: '+str(run_time_tot)+'\n')
    sys.exit(1)

def main(cycle_dt_str, sim_hrs, members, out_dir_parent, icbc_fc_dt, now_time_beg, int_h):

    fmt_yyyy = '%Y'
    fmt_hh = '%H'
    fmt_yyyymmdd = '%Y%m%d'
    fmt_yyyymmdd_hh = '%Y%m%d_%H'

    cycle_dt = pd.to_datetime(cycle_dt_str, format=fmt_yyyymmdd_hh)
    cycle_date = cycle_dt.strftime(fmt_yyyymmdd)
    cycle_hour = cycle_dt.strftime(fmt_hh)

    # First cycle dates for GEFS v12 and v11
    # Actual v11 start date was sometime in Dec 2015, but exact date likely irrelevant. Use 1 Jan 2016 for ease.
    gefsv12_dt = dt.datetime(2020, 9, 23, 12, 0, 0)
    gefsv11_dt = dt.datetime(2016, 1, 1, 0, 0, 0)
    is_gefsv12 = False
    is_gefsv11 = False
    
    if cycle_dt >= gefsv12_dt:
        is_gefsv12 = True
        aws_dir = 'https://noaa-gefs-pds.s3.amazonaws.com/gefs.'+cycle_date+'/'+cycle_hour+'/atmos'
        if int_h % 3 != 0:
            print('ERROR: int_h (option -i) must be divisible by 3 for GEFS v12 data. Exiting!')
            sys.exit()
    elif cycle_dt < gefsv12_dt and cycle_dt >= gefsv11_dt:
        is_gefsv11 = True
        aws_dir = 'https://noaa-gefs-pds.s3.amazonaws.com/gefs.'+cycle_date+'/'+cycle_hour
        if int_h % 6 != 0:
            print('ERROR: int_h (option -i) must be divisible by 6 for GEFS v11 data. Exiting!')
            sys.exit()
    else:
        print('ERROR: Cannot retrieve gefs data this old.')
        print('       Choose a cycle start date '+str(gefsv11_dt)+' or newer.')
        sys.exit()

    ## Calculate the desired lead hours for this cycle, accounting for the possible icbc_fc_dt offset.
    ## Build array of forecast lead times to download. GEFSv12 output on AWS is 3-hourly. v11 is 6-hourly.
    leads = np.arange(icbc_fc_dt, sim_hrs+icbc_fc_dt+1, int_h)
    n_leads = len(leads)

    out_dir_parent.mkdir(parents=True, exist_ok=True)

    n_members = len(members)
    ## Loop over GEFS members
    for mm in range(n_members):
#       out_dir = out_dir_parent.joinpath('mem'+members[mm])
#       out_dir.mkdir(parents=True, exist_ok=True)
        ## Create folders for each type of file, as there is on AWS
#       out_dir.joinpath('pgrb2ap5').mkdir(parents=True, exist_ok=True)
#       out_dir.joinpath('pgrb2bp5').mkdir(parents=True, exist_ok=True)
#       os.chdir(out_dir)
        if is_gefsv12:
            out_dir_parent.joinpath('pgrb2ap5').mkdir(parents=True, exist_ok=True)
            out_dir_parent.joinpath('pgrb2bp5').mkdir(parents=True, exist_ok=True)
        elif is_gefsv11:
            out_dir_parent.joinpath('pgrb2a').mkdir(parents=True, exist_ok=True)
            out_dir_parent.joinpath('pgrb2b').mkdir(parents=True, exist_ok=True)
        os.chdir(out_dir_parent)
        if members[mm] == '00':
            gefs_prefix = 'gec'
        else:
            gefs_prefix = 'gep'

        ## Loop over lead times
        for ll in range(n_leads):
            if is_gefsv12:
                this_lead = str(leads[ll]).zfill(3)
            elif is_gefsv11:
                if leads[ll] < 100:
                    this_lead = str(leads[ll]).zfill(2)
                elif leads[ll] >= 100 and leads[ll] < 1000:
                    this_lead = str(leads[ll]).zfill(3)

            ## Download 0.5-deg "a" file
#           os.chdir(out_dir.joinpath('pgrb2ap5'))
            if is_gefsv12:
                os.chdir(out_dir_parent.joinpath('pgrb2ap5'))
                fname = gefs_prefix+members[mm]+'.t'+cycle_hour+'z.pgrb2a.0p50.f'+this_lead
                url = aws_dir+'/pgrb2ap5/'+fname
                local_fname = out_dir_parent.joinpath('pgrb2ap5', fname)
            elif is_gefsv11:
                os.chdir(out_dir_parent.joinpath('pgrb2a'))
                fname = gefs_prefix+members[mm]+'.t'+cycle_hour+'z.pgrb2af'+this_lead
                url = aws_dir+'/pgrb2a/'+fname
                local_fname = out_dir_parent.joinpath('pgrb2a', fname)
#            if not out_dir.joinpath('pgrb2ap5',fname).is_file():
            if not local_fname.is_file():
                log.info('Downloading '+url)
                try:
                    wget.download(url)
                    log.info('')
                except:
                    wget_error(str(e), now_time_beg)
            else:
                log.info('   File '+fname+' already exists locally. Not downloading again from server.')

            ## Download 0.5-deg "b" file
#            os.chdir(out_dir.joinpath('pgrb2bp5'))
            if is_gefsv12:
                os.chdir(out_dir_parent.joinpath('pgrb2bp5'))
                fname = gefs_prefix+members[mm]+'.t'+cycle_hour+'z.pgrb2b.0p50.f'+this_lead
                url = aws_dir+'/pgrb2bp5/'+fname
                local_fname = out_dir_parent.joinpath('pgrb2bp5', fname)
            elif is_gefsv11:
                os.chdir(out_dir_parent.joinpath('pgrb2b'))
                fname = gefs_prefix+members[mm]+'.t'+cycle_hour+'z.pgrb2bf'+this_lead
                url = aws_dir+'/pgrb2b/'+fname
                local_fname = out_dir_parent.joinpath('pgrb2b', fname)
#            if not out_dir.joinpath('pgrb2bp5',fname).is_file():
            if not local_fname.is_file():
                log.info('Downloading '+url)
                try:
                    wget.download(url)
                    log.info('')
                except:
                    wget_error(str(e), now_time_beg)
            else:
                log.info('   File '+fname+' already exists locally. Not downloading again from server.')
            


if __name__ == '__main__':
    now_time_beg = dt.datetime.now(dt.UTC)
    cycle_dt, sim_hrs, members, out_dir_parent, icbc_fc_dt, int_h = parse_args()
    main(cycle_dt, sim_hrs, members, out_dir_parent, icbc_fc_dt, now_time_beg, int_h)
    now_time_end = dt.datetime.now(dt.UTC)
    run_time_tot = now_time_end - now_time_beg
    now_time_beg_str = now_time_beg.strftime('%Y-%m-%d %H:%M:%S')
    now_time_end_str = now_time_end.strftime('%Y-%m-%d %H:%M:%S')
    log.info('\ndownload_gefs_from_aws.py completed successfully.')
    log.info('   Beg time: '+now_time_beg_str)
    log.info('   End time: '+now_time_end_str)
    log.info('   Run time: '+str(run_time_tot)+'\n')
