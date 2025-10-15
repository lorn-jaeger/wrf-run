> [!NOTE]
> Documentation of the steps I followed to set up wps_wrf_workflow both for me and for anyone helping me debug this.


### Obtaining Code

Clone an up to date fork of wps_wrf_workflow into my home directory.

`git clone https://github.com/lorn-jaeger/wrf-run.git`

### Jeremy Files

I am using the config files and compiled WRF and WPS version available in Jeremy's For_Um directory. 

I put the the templates and configs in the template and config directories and am keeping WRF and WPS in a /bin directory I created.

I changed each of the marked paths in the configs and templates to the actual locations. You can see the diff for more information.

### Jared Changes

Previous runs crashed MPI, Jared told us that we were using too many CPU cores for this domain size and that we should reduce it to 128.


### Test Run 1

Command run

```
python setup_wps_wrf.py -b 20200818_18 -c config/config_wrfonly_UM.yaml
setup_wps_wrf.py: 2025-10-12T18:47:49 - yaml params: {'sim_hrs': 30, 'template_dir': '/glade/u//home/ljaeger/wrf-run/templates/WRF_1Dom1km', 'wps_ins_dir': '/glade/u/home/ljaeger/wrf-run/bin/WPS-4.6-dmpar-casper', 'wrf_ins_dir': '/glade/u/home/ljaeger/wrf-
run/bin/WRF-4.6', 'wps_run_dir': '/glade/derecho/scratch/ljaeger/workflow/UM_WRF_1Dom1km/wps', 'wrf_run_dir': '/glade/derecho/scratch/ljaeger/workflow/UM_WRF_1Dom1km/wrf', 'grib_dir': '/glade/derecho/scratch/ljaeger/data/hrrr', 'ungrib_domain': 'full', 'ic
bc_model': 'HRRR', 'icbc_source': 'AWS', 'icbc_fc_dt': 0, 'icbc_analysis': True, 'hrrr_native': True, 'get_icbc': True, 'do_geogrid': True, 'do_ungrib': True, 'do_avg_tsfc': True, 'use_tavgsfc': True, 'do_metgrid': True, 'do_real': True, 'do_wrf': True, 'a
rchive': False}
```

Error

```
setup_wps_wrf.py: 2025-10-12T18:56:24 - Executing Command: python run_geogrid.py -w /glade/u/home/ljaeger/wrf-run/bin/WPS-4.6-dmpar-casper -r /glade/derecho/scratch/ljaeger/workflow/UM_WRF_1Dom1km/wps/geogrid -t /glade/u/home/ljaeger/wrf-run/templates/WRF_1Dom1km -n namelist.wps.hrrr -q pbs -a derecho
run_geogrid.py: 2025-10-12T18:56:24 - Executing Command: qsub submit_geogrid.bash
run_geogrid.py: 2025-10-12T18:56:24 - Command stderr:
 mkstemp: No such file or directory
qsub: could not create/open tmp file /glade/derecho/scratch/ljaeger/tmp/pbsscrptpDJGZD for script

run_geogrid.py: 2025-10-12T18:56:24 - Error Executing Command: qsub submit_geogrid.bash
run_geogrid.py: 2025-10-12T18:56:24 - Return Code: 1
Traceback (most recent call last):
  File "/glade/u/home/ljaeger/wrf-run/run_geogrid.py", line 180, in <module>
    main(wps_dir, run_dir, tmp_dir, nml_tmp, scheduler, hostname)
  File "/glade/u/home/ljaeger/wrf-run/run_geogrid.py", line 138, in main
    queue = output.split('.')[1]
            ~~~~~~~~~~~~~~~~~^^^
IndexError: list index out of range
setup_wps_wrf.py: 2025-10-12T18:56:24 - Error Executing Command: python run_geogrid.py -w /glade/u/home/ljaeger/wrf-run/bin/WPS-4.6-dmpar-casper -r /glade/derecho/scratch/ljaeger/workflow/UM_WRF_1Dom1km/wps/geogrid -t /glade/u/home/ljaeger/wrf-run/templates/WRF_1Dom1km -n namelist.wps.hrrr -q pbs -a derecho
setup_wps_wrf.py: 2025-10-12T18:56:24 - Return Code: 1
setup_wps_wrf.py: 2025-10-12T18:56:24 - Exiting
```

Fixed by making tmp directory world writable 

### Test Run 2

```
python setup_wps_wrf.py -b 20200818_18 -c config/config_wrfonly_UM.yaml
```

Successful completion of the full script. All hours of the simulation run until the final step where they crashed. 

Error at end of rsl.out and rsl.error files

```
Timing for main: time 2020-08-19_23:59:12 on domain   1:    0.23203 elapsed seconds
Timing for main: time 2020-08-19_23:59:18 on domain   1:    0.23441 elapsed seconds
Timing for main: time 2020-08-19_23:59:24 on domain   1:    0.23361 elapsed seconds
Timing for main: time 2020-08-19_23:59:30 on domain   1:    0.23314 elapsed seconds
Timing for main: time 2020-08-19_23:59:36 on domain   1:    0.23241 elapsed seconds
Timing for main: time 2020-08-19_23:59:42 on domain   1:    0.23211 elapsed seconds
Timing for main: time 2020-08-19_23:59:48 on domain   1:    0.23334 elapsed seconds
Timing for main: time 2020-08-19_23:59:54 on domain   1:    0.23326 elapsed seconds
Timing for main: time 2020-08-20_00:00:00 on domain   1:    0.23268 elapsed seconds
Timing for Writing wrfout_d01_2020-08-20_00:00:00 for domain        1:    7.99026 elapsed seconds
Timing for Writing restart for domain        1:   33.94203 elapsed seconds
d01 2020-08-20_00:00:00  Input data is acceptable to use: wrfbdy_d01
           1  input_wrf: wrf_get_next_time current_date: 2020-08-20_00:00:00 Status =           -4
-------------- FATAL CALLED ---------------
FATAL CALLED FROM FILE:  <stdin>  LINE:    1159
 ---- ERROR: Ran out of valid boundary conditions in file wrfbdy_d01
-------------------------------------------
taskid: 0 hostname: dec2440
```

Looked on the forums and there is a fix here. https://forum.mmm.ucar.edu/threads/error-ran-out-of-valid-boundary-conditions-in-file-wrfbdy_d01.10340/

```
ncdump -v Times wrfbdy_d01

data:

 Times =
  "2020-08-18_18:00:00",
  "2020-08-18_19:00:00",
  "2020-08-18_20:00:00",
  "2020-08-18_21:00:00",
  "2020-08-18_22:00:00",
  "2020-08-18_23:00:00",
  "2020-08-19_00:00:00",
  "2020-08-19_01:00:00",
  "2020-08-19_02:00:00",
  "2020-08-19_03:00:00",
  "2020-08-19_04:00:00",
  "2020-08-19_05:00:00",
  "2020-08-19_06:00:00",
  "2020-08-19_07:00:00",
  "2020-08-19_08:00:00",
  "2020-08-19_09:00:00",
  "2020-08-19_10:00:00",
  "2020-08-19_11:00:00",
  "2020-08-19_12:00:00",
  "2020-08-19_13:00:00",
  "2020-08-19_14:00:00",
  "2020-08-19_15:00:00",
  "2020-08-19_16:00:00",
  "2020-08-19_17:00:00",
  "2020-08-19_18:00:00",
  "2020-08-19_19:00:00",
  "2020-08-19_20:00:00",
  "2020-08-19_21:00:00",
  "2020-08-19_22:00:00",
  "2020-08-19_23:00:00" ;
}
```

```
ls wrfout_d01_2020-08-*

wrfout_d01_2020-08-18_18:00:00  wrfout_d01_2020-08-18_22:00:00  wrfout_d01_2020-08-19_02:00:00  wrfout_d01_2020-08-19_06:00:00  wrfout_d01_2020-08-19_10:00:00  wrfout_d01_2020-08-19_14:00:00  wrfout_d01_2020-08-19_18:00:00  wrfout_d01_2020-08-19_22:00:00
wrfout_d01_2020-08-18_19:00:00  wrfout_d01_2020-08-18_23:00:00  wrfout_d01_2020-08-19_03:00:00  wrfout_d01_2020-08-19_07:00:00  wrfout_d01_2020-08-19_11:00:00  wrfout_d01_2020-08-19_15:00:00  wrfout_d01_2020-08-19_19:00:00  wrfout_d01_2020-08-19_23:00:00
wrfout_d01_2020-08-18_20:00:00  wrfout_d01_2020-08-19_00:00:00  wrfout_d01_2020-08-19_04:00:00  wrfout_d01_2020-08-19_08:00:00  wrfout_d01_2020-08-19_12:00:00  wrfout_d01_2020-08-19_16:00:00  wrfout_d01_2020-08-19_20:00:00  wrfout_d01_2020-08-20_00:00:00
wrfout_d01_2020-08-18_21:00:00  wrfout_d01_2020-08-19_01:00:00  wrfout_d01_2020-08-19_05:00:00  wrfout_d01_2020-08-19_09:00:00  wrfout_d01_2020-08-19_13:00:00  wrfout_d01_2020-08-19_17:00:00  wrfout_d01_2020-08-19_21:00:00
```

```
ls wrfout_d01_2020-08-* | wc -l

31
```

Should be 30?

Setting 30 hours of simulation runs 31 hours, but the script only builds 30 hours of input, hence the crash. Fortunatley we still get all the data we need. I have to look through the configs and code to see where the mismatch is. Hopefully I can either patch it or call the script in a different way so that it is no longer an issue. 

Might be thinking of the output files wrong. Are they snapshots of the simulation in time or the average value over the interval of an hour? From here https://yidongwonyi.wordpress.com/models/wrf-weather-research-and-forecasting/is-wrfout-written-in-snapshot-or-average-value/ it looks like they are snapshots. It also mentions setting a diagnostic


