#!/usr/bin/env python3

'''
download_gefs_from_nomads.py

Created by: Jared A. Lee (jaredlee@ucar.edu)
Created on: 24 Feb 2023

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

def parse_args():
	## Parse the command-line arguments
	parser = argparse.ArgumentParser()
	parser.add_argument('cycle_dt', help='GEFS cycle date/time to download [YYYYMMDD_HH]')
	parser.add_argument('-s', '--sim_hrs', default=48, type=int, help='integer number of forecast hours to download (default: 48)')
	parser.add_argument('-m', '--members', default='01', help='GEFS ensemble member(s) to download. If requesting multiple members, separate them by commas only (e.g., 01,02). (default: 01)')

	args = parser.parse_args()
	cycle_dt = args.cycle_dt
	sim_hrs = args.sim_hrs
	members_inp = args.members
	members = members_inp.split(',')

	if len(cycle_dt) != 11:
		print('ERROR! Incorrect length for positional argument cycle_dt. Exiting!')
		parser.print_help()
		sys.exit()
	elif cycle_dt[8] != '_':
		print('ERROR! Incorrect format for positional argument cycle_dt. Exiting!')
		parser.print_help()
		sys.exit()

	return cycle_dt, sim_hrs, members

def wget_error(error_msg, now_time_beg):
	print('ERROR: '+error_msg)
	print('Check if an earlier cycle has the required files and adjust icbc_fc_dt if necessary. Exiting!')
	now_time_end = dt.datetime.utcnow()
	run_time_tot = now_time_end - now_time_beg
	now_time_beg_str = now_time_beg.strftime('%Y-%m-%d %H:%M:%S')
	now_time_end_str = now_time_end.strftime('%Y-%m-%d %H:%M:%S')
	print('\nScript completed.')
	print('   Beg time: '+now_time_beg_str)
	print('   End time: '+now_time_end_str)
	print('   Run time: '+str(run_time_tot)+'\n')
	sys.exit()

def main(cycle_dt_str, sim_hrs, members, now_time_beg):

	## Build array of forecast lead times to download. GEFS output on NOMADS is 3-hourly.
	leads = np.arange(0, sim_hrs+1, 3)
	n_leads = len(leads)

	fmt_yyyy = '%Y'
	fmt_hh = '%H'
	fmt_yyyymmdd = '%Y%m%d'
	fmt_yyyymmdd_hh = '%Y%m%d_%H'

	cycle_dt = pd.to_datetime(cycle_dt_str, format=fmt_yyyymmdd_hh)
	cycle_date = cycle_dt.strftime(fmt_yyyymmdd)
	cycle_hour = cycle_dt.strftime(fmt_hh)

	nomads_dir = 'https://nomads.ncep.noaa.gov/pub/data/nccf/com/gens/prod/gefs.'+cycle_date+'/'+cycle_hour+'/atmos'

	out_dir_parent = pathlib.Path('/','ipcscratch','jaredlee154','data','gefs',cycle_dt_str)
	out_dir_parent.mkdir(parents=True, exist_ok=True)

	n_members = len(members)
	## Loop over GEFS members
	for mm in range(n_members):
		out_dir = out_dir_parent.joinpath('mem'+members[mm])
		out_dir.mkdir(parents=True, exist_ok=True)
		if members[mm] == '00':
			gefs_prefix = 'gec'
		else:
			gefs_prefix = 'gep'

		## Loop over lead times
		for ll in range(n_leads):
			this_lead = str(leads[ll]).zfill(3)

			## Download 0.5-deg "a" file
			fname = gefs_prefix+members[mm]+'.t'+cycle_hour+'z.pgrb2a.0p50.f'+this_lead
			url = nomads_dir+'/pgrb2ap5/'+fname
			if not out_dir.joinpath(fname).is_file():
				print('Downloading '+url)
				try:
					wget.download(url)
					print('')
				except:
					wget_error(str(e), now_time_beg)
			else:
				print('   File '+fname+' already exists locally. Not downloading again from server.')

			## Download 0.5-deg "b" file
			fname = gefs_prefix+members[mm]+'.t'+cycle_hour+'z.pgrb2b.0p50.f'+this_lead
			url = nomads_dir+'/pgrb2bp5/'+fname
			if not out_dir.joinpath(fname).is_file():
				print('Downloading '+url)
				try:
					wget.download(url)
					print('')
				except:
					wget_error(str(e), now_time_beg)
			else:
				print('   File '+fname+' already exists locally. Not downloading again from server.')
			


if __name__ == '__main__':
	now_time_beg = dt.datetime.utcnow()
	cycle_dt, sim_hrs, members = parse_args()
	main(cycle_dt, sim_hrs, members, now_time_beg)
	now_time_end = dt.datetime.utcnow()
	run_time_tot = now_time_end - now_time_beg
	now_time_beg_str = now_time_beg.strftime('%Y-%m-%d %H:%M:%S')
	now_time_end_str = now_time_end.strftime('%Y-%m-%d %H:%M:%S')
	print('\nScript completed successfully.')
	print('   Beg time: '+now_time_beg_str)
	print('   End time: '+now_time_end_str)
	print('   Run time: '+str(run_time_tot)+'\n')
