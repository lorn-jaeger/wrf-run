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


### Test Run

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
