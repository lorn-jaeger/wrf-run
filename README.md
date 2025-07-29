# wps_wrf_workflow
Python-based modular workflow for configuring and running WPS and WRF, and optionally also post-processing programs like UPP. Because of its modular nature, scripts to do additional steps can easily be added if desired.

Before running this Python-based workflow, you need to do the following steps:
 - Have compiled versions of WRF and WPS available (for now, WPS should be compiled with dmpar; later updates may make this workflow compatible with serial-compiled WPS)
 - Create a template namelist.wps for WPS for your simulation domain(s)
 - Create a template namelist.input for WRF for your simulation
 - Create template job submission scripts, one for each WPS/WRF executable
 - Modify config/wps_wrf_config.yaml (or create your own) with configuration options. If you desire an experiment name, then add a line "exp_name: exp01" or similar. Some of the options include whether to run the various WPS/WRF executables, where your WPS/WRF installation and run directories are, and more.

The typical way to run the workflow is to execute this command:
```
./setup_wps_wrf.py -b [cycle start date in YYYYMMDD_HH format] -c [/relative/path/to/wps_wrf_config.yaml]
```
Based on the cycle start date you provide with the `-b` flag, combined with simulation duration settings you provide in the config yaml file with the `-c` flag, the workflow will automatically update the simulation start/end dates in the WPS/WRF namelists. 

A full usage statement with additional options can be seen by executing:
```
./setup_wps_wrf.py -h
```
Keep in mind that any default settings for many of these parameters are overridden if provided in the config yaml file that is passed to setup_wps_wrf.py.

## Running multiple fires

Use `run_fire_batch.py` to automate running `setup_wps_wrf.py` for many fire
locations. The input CSV must contain one row per fire with columns
`fire_id,start,end,lat,lon`:

```
fire_id,start,end,lat,lon
test1,20240601_00,20240602_00,35.6,-105.3
test2,20240605_00,20240606_00,34.1,-104.2
```

For every `fire_id` the script copies the template directory and writes a
per-fire configuration so outputs end up under unique directories such as
`wps_runs/test1` and `wrf_runs/test1`. **All `fire_id` values must be unique.**
If any per-fire directory already exists the script stops to avoid clobbering
previous results.

Run the batch job with four concurrent workers:

```
python run_fire_batch.py fires.csv --max-workers 4
```

Other useful options include `--dry-run` to print commands without executing
them and `--log-level DEBUG` for verbose logging. Custom parent directories for
WPS or WRF runs can also be supplied with `--wps-parent` and `--wrf-parent`.

