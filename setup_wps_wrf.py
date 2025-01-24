#!/usr/bin/env python3

"""
setup_wps_wrf.py

Written by: Jared A. Lee (jaredlee@ucar.edu)
Written on: 31 Mar 2023
Other contributors: Bill Petzke, Paddy McCarthy

This script links to (and downloads, if necessary) all the files needed to run WPS/WRF on Casper/Derecho.
Each program in the WPS/WRF workflow can be optionally executed.
WRF output files can also be optionally moved to an archival directory (arc_dir).
"""

import os
import sys
import argparse
import pathlib
import datetime as dt
import pandas as pd
import logging
import yaml
import subprocess
import glob
import socket
from argparse import RawTextHelpFormatter

from proc_util import exec_command

this_file = os.path.basename(__file__)
logging.basicConfig(format=f'{this_file}: %(asctime)s - %(message)s',
                    level=logging.DEBUG, datefmt='%Y-%m-%dT%H:%M:%S')
log = logging.getLogger(__name__)

def parse_args():
    yaml_config_help = {
     'cycle_int_h': 'integer number of hours between forecast cycles, if cycle_beg_dt and cycle_end_dt are different (default: 24)',
     'sim_hrs': 'integer number of hours for WRF simulation (default: 24)',
     'icbc_fc_dt': 'integer number of hours prior to WRF cycle time for IC/LBC model cycle (default: 0)',
     'exp_name': 'experiment name (e.g., exp01, mem01, etc.) (default: None)',
     'realtime': 'flag when running in real-time to keep this script running until WRF is done',
     'archive': 'flag to archive wrfout, wrfinput, wrfbdy, and namelist files to another location',
     'icbc_model': 'string specifying the model to be used for ICs/LBCs (default: GEFS)',
     'icbc_source': 'string specifying the repository from which to obtain ICs/LBCs (GLADE, AWS, GoogleCloud, NOMADS) (default: GLADE)',
     'grib_dir': 'string or Path object specifying the parent directory for where grib/grib2 input data (e.g., GEFS, GFS, etc.) is downloaded for use by ungrib (default: /glade/derecho/scratch/jaredlee/data',
     'ungrib_domain': 'string (either "full" or "subset") indicating whether to run ungrib on full-domain or geographically-subsetted grib/grib2 files (default: full)',
     'wps_ins_dir': 'string or Path object specifying the WPS installation directory (default: /glade/u/home/jaredlee/programs/WPS-4.6-dmpar)',
     'wrf_ins_dir': 'string or Path object specifying the WRF installation directory (default: /glade/u/home/jaredlee/programs/WRF-4.6)',
     'wps_run_dir': 'string or Path object specifying the parent WPS run directory (default: /glade/derecho/scratch/jaredlee/workflow/wps)',
     'wrf_run_dir': 'string or Path object specifying the parent WRF run directory (default: /glade/derecho/scratch/jaredlee/workflow/wrf)',
     'template_dir': 'string or Path object specifying the directory containing templates for sbatch submission scripts and WPS/WRF namelists (default: /glade/work/jaredlee/workflow/templates)',
     'arc_dir': 'string or Path object specifying the parent directory where WRF output should be archived (default: /glade/work/jaredlee/workflow)',
     'upp_yaml': 'string or Path object specifying the config file for the "run_upp" task.',
     'upp_domains': 'list of wrfout domain indices to process with UPP (default: [empty list | 0] to process all wrfout files)',
     'upp_working_dir': 'string or Path object that hosts subdirectories where each of the individual UPP processes is run (default: /tmp/upp)',
     'get_icbc':    'flag to download/link to IC/BC grib data',
     'do_geogrid':  'flag to run geogrid for this case',
     'do_ungrib':   'flag to run ungrib for this case',
     'do_metgrid':  'flag to run metgrid for this case',
     'do_real':     'flag to run real for this case',
     'do_wrf':      'flag to submit wrf for this case',
     'do_upp':      'flag to perform UPP post-processing to grib2 for this case',
     #Add new parameters here
    }

    ## Parse the command-line arguments
    parser = argparse.ArgumentParser(formatter_class=RawTextHelpFormatter)
    parser.add_argument('-b', '--cycle_dt_beg', required=True, help='beginning date/time of first WRF simulation [YYYYMMDD_HH] (default: 20220801_00)')
    parser.add_argument('-e', '--cycle_dt_end', required=False, default=None, help='beginning date/time of last WRF simulation [YYYYMMDD_HH]')
    parser.add_argument('-c', '--config', required=True, help=f"yaml configuration file\n{yaml.dump(yaml_config_help, default_flow_style=False)}")
    
    args = parser.parse_args()
    cycle_dt_beg = args.cycle_dt_beg
    cycle_dt_end = args.cycle_dt_end

    if len(cycle_dt_beg) != 11 or cycle_dt_beg[8] != '_':
        print('ERROR! Incorrect format for argument cycle_dt_beg. Exiting!')
        parser.print_help()
        sys.exit(1)

    if cycle_dt_end != None:
        if len(cycle_dt_end) != 11 or cycle_dt_end[8] != '_':
            print('ERROR! Incorrect length for argument cycle_dt_end. Exiting!')
            parser.print_help()
            sys.exit(1)
    else:
        cycle_dt_end = cycle_dt_beg

    # Get the hostname. If an NCAR HPC machine (derecho or casper), then we can link to IC/LBC data on GLADE.
    # As derecho and casper allow cross-submitting jobs, then the appropriate PBS job script can be selected,
    # which would allow users to seamlessly run this workflow on either derecho or casper.
    hostname = socket.gethostname()
    # Simplify the hostnames for derecho & casper, which have a single numeral as the final character.
    # If other hostnames in the future need to be simplified similarly, add another elif branch here.
    if hostname[0:-1] == 'derecho':
        hostname = 'derecho'
    elif hostname[0:-1] == 'casper-login':
        hostname = 'casper'

    with open(args.config) as yaml_f:
        params = yaml.safe_load(yaml_f)
    log.info(f"yaml params: {params}")
    params.setdefault('cycle_int_h',24)
    params.setdefault('sim_hrs', 24)
    params.setdefault('icbc_fc_dt',0)
    params.setdefault('exp_name', None)
    params.setdefault('realtime', False)
    params.setdefault('archive', False)
    params.setdefault('ungrib_domain', 'full')
    params.setdefault('icbc_model', 'GFS')
    params.setdefault('icbc_source', 'GLADE')
    params.setdefault('grib_dir', '/glade/derecho/scratch/jaredlee/data')
    params.setdefault('wps_ins_dir', '/glade/u/home/jaredlee/programs/WPS-4.6-dmpar')
    params.setdefault('wrf_ins_dir', '/glade/u/home/jaredlee/programs/WRF-4.6')
    params.setdefault('wps_run_dir', '/glade/derecho/scratch/jaredlee/workflow/wps')
    params.setdefault('wrf_run_dir', '/glade/derecho/scratch/jaredlee/workflow/wrf')
    params.setdefault('template_dir', '/glade/work/jaredlee/workflow/templates')
    params.setdefault('arc_dir', '/glade/work/jaredlee/workflow')
    params.setdefault('upp_working_dir', '/tmp/upp')
    params.setdefault('upp_yaml', './config/run_upp.yaml')
    params.setdefault('upp_domains', ['0'])

    params.setdefault('get_icbc', False)
    params.setdefault('do_geogrid', False)
    params.setdefault('do_ungrib', False)
    params.setdefault('do_metgrid', False)
    params.setdefault('do_real', False)
    params.setdefault('do_wrf', False)
    params.setdefault('do_upp', False)

    params['hostname'] = hostname
    params['grib_dir_parent'] = pathlib.Path(params['grib_dir'])
    del params['grib_dir']
    params['icbc_source'] = params['icbc_source']
    params['icbc_model'] = params['icbc_model']
    params['icbc_fc_dt'] = params['icbc_fc_dt']
    params['ungrib_domain'] = params['ungrib_domain']
    params['wps_ins_dir'] = pathlib.Path(params['wps_ins_dir'])
    params['wrf_ins_dir'] = pathlib.Path(params['wrf_ins_dir'])
    params['wps_run_dir_parent'] = pathlib.Path(params['wps_run_dir'])
    del params['wps_run_dir']
    params['wrf_run_dir_parent'] = pathlib.Path(params['wrf_run_dir'])
    del params['wrf_run_dir']
    params['template_dir'] = pathlib.Path(params['template_dir'])
    params['arc_dir_parent'] = pathlib.Path(params['arc_dir'])
    del params['arc_dir']
    params['upp_working_dir'] = pathlib.Path(params['upp_working_dir'])
    params['upp_yaml'] = pathlib.Path(params['upp_yaml'])
    params['archive'] = params['archive']

    # Check upp_domains and convert to int if possible.
    for idx, domain in enumerate(params['upp_domains']):
        try:
            params['upp_domains'][idx] = int(domain)
        except ValueError:
            print(f'ERROR! Specify integers for upp_domains, or leave this param empty to process all domains. Got: {domain}')
            parser.print_help()
            sys.exit(1)

    params['cycle_dt_str_beg'] = cycle_dt_beg
    params['cycle_dt_str_end'] = cycle_dt_end

    return params

def main(cycle_dt_str_beg, cycle_dt_str_end, cycle_int_h, sim_hrs, icbc_fc_dt, exp_name, realtime, archive, hostname,
         now_time_beg, icbc_model, icbc_source, ungrib_domain, grib_dir_parent, wps_ins_dir, wrf_ins_dir,
         wps_run_dir_parent, wrf_run_dir_parent, template_dir, arc_dir_parent,
         upp_working_dir, upp_yaml, upp_domains,
         get_icbc, do_geogrid, do_ungrib, do_metgrid, do_real, do_wrf, do_upp):

    ## String format statements
    fmt_exp_dir        = '%Y-%m-%d_%H'
    fmt_yyyymmdd       = '%Y%m%d'
    fmt_yyyymmddhh     = '%Y%m%d%H'
    fmt_yyyymmdd_hh    = '%Y%m%d_%H'
    fmt_yyyymmdd_hhmm  = '%Y%m%d_%H%M'

    ## Date/time manipulation
    cycle_dt_beg = pd.to_datetime(cycle_dt_str_beg, format=fmt_yyyymmdd_hh)
    cycle_dt_end = pd.to_datetime(cycle_dt_str_end, format=fmt_yyyymmdd_hh)
    cycle_dt_all = pd.date_range(start=cycle_dt_beg, end=cycle_dt_end, freq=str(cycle_int_h)+'h')
    n_cycles = len(cycle_dt_all)

    ## Check if this cluster uses slurm or pbs
    test = subprocess.run(['which','sbatch'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    code = test.returncode
    if code == 0:
        scheduler = 'slurm'
    else:
        test = subprocess.run(['which','qsub'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        code = test.returncode
        if code == 0:
            scheduler = 'pbs'
        else:
            ## No jobs can get submitted if we get here
            log.error('ERROR: Neither sbatch nor qsub executables found.')
            log.error('       Modify scripts to handle a different batch job scheduler. Exiting!')
            sys.exit(1)
    log.info('Using the '+scheduler+' scheduler for batch job submission')

    ## Loop over forecast cycles
    for cc in range(n_cycles):
        cycle_dt = cycle_dt_all[cc]
        cycle_yr = cycle_dt.strftime('%Y')
        cycle_mo = cycle_dt.strftime('%m')
        cycle_dy = cycle_dt.strftime('%d')
        cycle_hr = cycle_dt.strftime('%H')
        cycle_yyyymmdd    = cycle_dt.strftime(fmt_yyyymmdd)
        cycle_yyyymmdd_hh = cycle_dt.strftime(fmt_yyyymmdd_hh)
        cycle_str = cycle_yyyymmdd_hh

        ## Directories for WPS & WRF where everything should be linked & run
        geo_run_dir = wps_run_dir_parent.joinpath('geogrid')
        if exp_name is None:
            wps_run_dir = wps_run_dir_parent.joinpath(cycle_yyyymmdd_hh)
            wrf_run_dir = wrf_run_dir_parent.joinpath(cycle_yyyymmdd_hh)
            ## Directory for archival
            arc_dir = arc_dir_parent.joinpath(cycle_yyyymmdd_hh)
        else:
            wps_run_dir = wps_run_dir_parent.joinpath(cycle_yyyymmdd_hh,exp_name)
            wrf_run_dir = wrf_run_dir_parent.joinpath(cycle_yyyymmdd_hh,exp_name)
            ## Directory for archival
            arc_dir = arc_dir_parent.joinpath(cycle_yyyymmdd_hh, exp_name)
        ## Directories for containing ungrib & metgrid output
        ungrib_dir  = wps_run_dir.joinpath('ungrib')
        metgrid_dir = wps_run_dir.joinpath('metgrid')
        ## WPS & WRF namelist templates
        wps_nml_tmp = 'namelist.wps.'+icbc_model.lower()
        if exp_name is None:
            wrf_nml_tmp = 'namelist.input.' + icbc_model.lower()
        else:
            wrf_nml_tmp = 'namelist.input.' + icbc_model.lower() + '.'+exp_name

        ## Get the icbc model cycle
        ## In real-time applications there may need to be an offset to stay ahead of the clock
        icbc_cycle_dt = cycle_dt - dt.timedelta(hours=icbc_fc_dt)
        icbc_cycle_yr = icbc_cycle_dt.strftime('%Y')
        icbc_cycle_mo = icbc_cycle_dt.strftime('%m')
        icbc_cycle_dy = icbc_cycle_dt.strftime('%d')
        icbc_cycle_hr = icbc_cycle_dt.strftime('%H')
        icbc_cycle_yyyymmdd    = icbc_cycle_dt.strftime(fmt_yyyymmdd)
        icbc_cycle_yyyymmdd_hh = icbc_cycle_dt.strftime(fmt_yyyymmdd_hh)
        icbc_cycle_str = icbc_cycle_yyyymmdd_hh

        ## Local directory where ICs/LBC grib2 files should be downloaded
        # Model directory structure & naming conventions to mimic AWS, rather than GLADE or GoogleCloud
        if icbc_model == 'GEFS':
            # Full-domain grib directory
#            grib_dir_full = grib_dir_parent.joinpath('gefs',icbc_cycle_yyyymmdd_hh,exp_name)
            grib_dir_full = grib_dir_parent.joinpath(f'gefs.{icbc_cycle_yyyymmdd}',icbc_cycle_hr,'atmos')

            # Use subset GEFS
            grib_dir_subset = grib_dir_parent.joinpath(f'gefs.{icbc_cycle_yyyymmdd}.subset',icbc_cycle_hr,'atmos')
        elif icbc_model == 'GFS':
            # Full-domain grib directory
#            grib_dir_full = grib_dir_parent.joinpath('gfs',icbc_cycle_yyyymmdd_hh)
            grib_dir_full = grib_dir_parent.joinpath(f'gfs.{icbc_cycle_yyyymmdd}',icbc_cycle_hr,'atmos')

            # Subsetted-domain grib directory
            grib_dir_subset = grib_dir_parent.joinpath(f'gfs.{icbc_cycle_yyyymmdd}.subset',icbc_cycle_hr,'atmos')
        else:
            log.error('ERROR: Unknown option chosen for icbc_model in the yaml file.')
            log.error('       Current options are GEFS|GFS. Add code to handle other IC/LBC model data. Exiting!')
            sys.exit(1)

        if ungrib_domain == 'full':
            grib_dir = grib_dir_full
        elif ungrib_domain == 'subset':
            grib_dir = grib_dir_subset
        else:
            log.error('ERROR: Set ungrib_domain to either full or subset in the yaml file. Exiting!')
            sys.exit(1)

        ## Get the start and end times of this simulation
        beg_dt = cycle_dt
        end_dt = beg_dt + dt.timedelta(hours=sim_hrs)

        beg_yr = beg_dt.strftime('%Y')
        beg_mo = beg_dt.strftime('%m')
        beg_dy = beg_dt.strftime('%d')
        beg_hr = beg_dt.strftime('%H')
        end_yr = end_dt.strftime('%Y')
        end_mo = end_dt.strftime('%m')
        end_dy = end_dt.strftime('%d')
        end_hr = end_dt.strftime('%H')

        ## ***********
        ## WPS Section
        ## ***********

        ## Read the template namelist.wps to get interval_seconds, and convert to int_hrs
        nml_tmp = template_dir.joinpath('namelist.wps.'+icbc_model.lower())
        log.info('Opening '+str(nml_tmp))
        with open(nml_tmp) as nml:
            for line in nml:
                if line.strip()[0:16] == 'interval_seconds':
                    int_sec = int(line.split('=')[1].strip().split(',')[0])
                    int_hrs = int_sec // 3600
                    break

        if icbc_model == 'GEFS':
            if exp_name is None:
                log.error('ERROR! exp_name is None, so a GEFS member number cannot be extracted. Exiting!')
            ## Make an assumption about which GEFS member to download or linked to based on exp_name
            ## Assume it starts with memNN or expNN, and set NN to the GEFS member to get or link to
            if exp_name[0:3] == 'mem' or exp_name[0:3] == 'exp':
                mem_id = exp_name[3:5]
                ## If this number exceeds the GEFS members, then base it only on the last number to get member 01-10
                if int(mem_id) > 20:
                    mem_id = exp_name[4]
                    if mem_id == '0':
                        mem_id = '10'
                    else:
                        mem_id = '0'+mem_id
            else:
                log.error('ERROR! Unable to obtain a GEFS member number from exp_name. Exiting!')
                sys.exit(1)
        else:
            mem_id = None

        if get_icbc:
            # If an ICBC dataset is locally available on GLADE, use that instead of downloading from an external repo
            if icbc_model == 'GFS' or icbc_model == 'gfs':
                if icbc_source == 'GLADE' or icbc_source == 'glade':
                    ret,output = exec_command(
                        ['python', 'link_gfs_from_glade.py', '-b', icbc_cycle_str, '-s', str(sim_hrs),
                         '-i', str(int_hrs), '-o', grib_dir_full], log)
                elif icbc_source == 'AWS' or icbc_source == 'aws':
                    ret,output = exec_command(
                        ['python', 'download_gfs_from_aws.py', '-b', icbc_cycle_str, '-s', str(sim_hrs),
                         '-i', str(int_hrs), '-o', grib_dir_full], log)
                else:
                    log.error('ERROR: No option yet to download or link to GFS data from icbc_source=' + icbc_source + ' in setup_wps_wrf.py. Exiting!')
                    sys.exit(1)
            elif icbc_model == 'GEFS' or icbc_model == 'gefs':
                if icbc_source == 'GLADE' or icbc_source == 'glade':
                    log.error('There is no known dataset containing GEFS files on GLADE. Change icbc_source. Exiting!')
                    sys.exit(1)
                elif icbc_source == 'AWS' or icbc_source == 'aws':
                    ret,output = exec_command(
                        ['python', 'download_gefs_from_aws.py', '-b', icbc_cycle_str, '-s', str(sim_hrs),
                         '-i', str(int_hrs), '-m', mem_id, '-o', grib_dir_full, '-f', str(icbc_fc_dt)], log)
                else:
                    log.error('ERROR: No option yet to download or link to GEFS data from icbc_source=' + icbc_source + ' in setup_wps_wrf.py. Exiting!')
                    sys.exit(1)

        if do_geogrid:
            ret,output = exec_command(
                ['python', 'run_geogrid.py', '-w', wps_ins_dir, '-r', geo_run_dir, '-t', template_dir,
                 '-n', wps_nml_tmp, '-q', scheduler, '-a', hostname], log)

        if do_ungrib:
            if mem_id is None:
                ret, output = exec_command(
                    ['python', 'run_ungrib.py', '-b', cycle_str, '-s', str(sim_hrs), '-w', wps_ins_dir,
                     '-r', wps_run_dir, '-o', ungrib_dir, '-g', grib_dir, '-t', template_dir, '-m', icbc_model,
                     '-i', str(int_hrs), '-q', scheduler, '-f', str(icbc_fc_dt), '-a', hostname, '-c', icbc_source],
                    log)
            else:
                ret,output = exec_command(
                    ['python', 'run_ungrib.py', '-b', cycle_str, '-s', str(sim_hrs), '-w', wps_ins_dir,
                     '-r', wps_run_dir, '-o', ungrib_dir, '-g', grib_dir, '-t', template_dir, '-m', icbc_model,
                     '-i', str(int_hrs), '-q', scheduler, '-f', str(icbc_fc_dt), '-a', hostname, '-c', icbc_source,
                     '-n', mem_id], log)

        if do_metgrid:
            ret,output = exec_command(
                ['python', 'run_metgrid.py', '-b', cycle_str, '-s', str(sim_hrs), '-w', wps_ins_dir,
                 '-r', wps_run_dir, '-o', metgrid_dir, '-u', ungrib_dir, '-t', template_dir, '-m', icbc_model,
                 '-q', scheduler, '-a', hostname], log)

        if do_real:
            if exp_name is None:
                ret, output = exec_command(
                    ['python', 'run_real.py', '-b', cycle_str, '-s', str(sim_hrs), '-w', wrf_ins_dir,
                     '-r', wrf_run_dir, '-m', metgrid_dir, '-t', template_dir, '-i', icbc_model, '-n', wrf_nml_tmp,
                     '-q', scheduler, '-a', hostname], log)
            else:
                ret, output = exec_command(
                    ['python', 'run_real.py', '-b', cycle_str, '-s', str(sim_hrs), '-w', wrf_ins_dir,
                     '-r', wrf_run_dir, '-m', metgrid_dir, '-t', template_dir, '-i', icbc_model, '-x', exp_name,
                     '-n', wrf_nml_tmp, '-q', scheduler, '-a', hostname], log)

        if do_wrf:
           if do_upp or archive:
               if exp_name is None:
                   ret, output = exec_command(
                       ['python', 'run_wrf.py', '-b', cycle_str, '-s', str(sim_hrs), '-w', wrf_ins_dir,
                        '-r', wrf_run_dir, '-t', template_dir, '-i', icbc_model, '-n', wrf_nml_tmp, '-m',
                        '-q', scheduler, '-a', hostname], log)
               else:
                   ret, output = exec_command(
                       ['python', 'run_wrf.py', '-b', cycle_str, '-s', str(sim_hrs), '-w', wrf_ins_dir,
                        '-r', wrf_run_dir, '-t', template_dir, '-i', icbc_model, '-x', exp_name, '-n', wrf_nml_tmp,
                        '-m', '-q', scheduler, '-a', hostname], log)
           else:
               if exp_name is None:
                   ret, output = exec_command(
                       ['python', 'run_wrf.py', '-b', cycle_str, '-s', str(sim_hrs), '-w', wrf_ins_dir,
                        '-r', wrf_run_dir, '-t', template_dir, '-i', icbc_model, '-n', wrf_nml_tmp, '-q', scheduler,
                        '-a', hostname], log)
               else:
                   ret, output = exec_command(
                       ['python', 'run_wrf.py', '-b', cycle_str, '-s', str(sim_hrs), '-w', wrf_ins_dir,
                        '-r', wrf_run_dir, '-t', template_dir, '-i', icbc_model, '-x', exp_name, '-n', wrf_nml_tmp,
                        '-q', scheduler, '-a', hostname], log)

        if do_upp:
            if upp_domains and len(upp_domains) > 0 and upp_domains[0] > 0:
                domains_str = str(upp_domains).strip().replace('[', '').replace(']', '').replace(' ', '')
                log.info(f'Sending domains_str to run_upp: {domains_str}')
                if exp_name is None:
                    ret, output = exec_command(
                        ['python', 'run_upp.py', '-b', cycle_str, '-r', wrf_run_dir, '-d', str(domains_str),
                         '-c', upp_yaml, '-N'], log)
                else:
                    ret, output = exec_command(
                        ['python', 'run_upp.py', '-b', cycle_str, '-r', wrf_run_dir, '-x', exp_name, '-d',
                         str(domains_str), '-c', upp_yaml, '-N'], log)
            else:
                if exp_name is None:
                    ret, output = exec_command(
                        ['python', 'run_upp.py', '-b', cycle_str, '-r', wrf_run_dir, '-c', upp_yaml, '-N'], log)
                else:
                    ret, output = exec_command(
                        ['python', 'run_upp.py', '-b', cycle_str, '-r', wrf_run_dir, '-x', exp_name, '-c', upp_yaml,
                         '-N'], log)

            # # TODO: Take this out after testing
            # if not upp_yaml.exists():
            #     log.warning(f'WARNING: Yaml config {upp_yaml} does not exist. Using ./test/run_upp.yaml instead.')
            #     upp_yaml = './test/run_upp.yaml'
            #
            # # Get a full path to ./run_upp.py
            # run_upp = pathlib.Path(os.path.abspath('./run_upp.py'))
            # submit_upp_tmpl = pathlib.Path(os.path.abspath('./config/submit_upp.tmpl'))
            # if not run_upp.is_file():
            #     log.error('ERROR! No "./run_upp.py" present to drive UPP post-processing. Exiting!')
            #     sys.exit(1)
            #
            # # Create submit_upp.bash script(s)
            # submitfile_paths = create_submit_upp_files_from_tmpl(submit_upp_tmpl, cycle_str, run_upp, wrf_run_dir, exp_name, upp_yaml, upp_domains)
            #
            # for upp_submitfile in submitfile_paths:
            #     ret, output = exec_command(['sbatch', upp_submitfile], log)
            #     jobid = output.split('job ')[1].split('\\n')[0]
            #     log.info(f'Submitted UPP batch job for "sbatch {upp_submitfile}": ' + jobid)

        if archive:
            arc_dir.joinpath('config').mkdir(exist_ok=True, parents=True)
            arc_dir.joinpath('wrfout').mkdir(exist_ok=True, parents=True)
            os.chdir(wps_run_dir)
            # subprocess.run cannot handle wildcards, so we need to iterate over matching files in calls to exec_command
            log.info('Copying namelist.wps to ' + str(arc_dir.joinpath('config')))
            ret,output = exec_command(['cp', 'namelist.wps', str(arc_dir.joinpath('config'))], log)
            os.chdir(wrf_run_dir)
            log.info('Copying namelist.input to '+str(arc_dir.joinpath('config')))
            ret,output = exec_command(['cp','namelist.input',str(arc_dir.joinpath('config'))],log)
            log.info('Copying wrfinput* and wrfbdy* files to ' + str(arc_dir.joinpath('config')))
            files = glob.glob('wrfinput_d0*')
            for file in files:
                ret,output = exec_command(['cp',file,str(arc_dir.joinpath('config'))],log)
            files = glob.glob('wrfbdy_d*')
            for file in files:
                ret,output = exec_command(['mv',file,str(arc_dir.joinpath('config'))],log)
            log.info('Moving wrfout* and wrfxtrm* files to '+str(arc_dir.joinpath('wrfout')))
            files = glob.glob('wrfout*')
            for file in files:
                ret,output = exec_command(['mv',file,str(arc_dir.joinpath('wrfout'))],log)
            files = glob.glob('wrfxtrm*')
            for file in files:
                ret,output = exec_command(['mv', file, str(arc_dir.joinpath('wrfout'))], log)


if __name__ == '__main__':
    now_time_beg = dt.datetime.now(dt.UTC)
    params = parse_args()
    params['now_time_beg'] = now_time_beg
    main(**params)
    now_time_end = dt.datetime.now(dt.UTC)
    run_time_tot = now_time_end - now_time_beg
    now_time_beg_str = now_time_beg.strftime('%Y-%m-%d %H:%M:%S')
    now_time_end_str = now_time_end.strftime('%Y-%m-%d %H:%M:%S')
    log.info('\nsetup_wps_wrf.py completed.')
    log.info('   Beg time: '+now_time_beg_str)
    log.info('   End time: '+now_time_end_str)
    log.info('   Run time: '+str(run_time_tot)+'\n')
