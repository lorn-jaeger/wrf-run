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

The `run_fire_batch.py` script processes a CSV file describing multiple fire
events. Each fire is identified by a unique `fire_id` column. The script creates
per-fire working directories named after each ID so that files remain
distinguishable. **Every `fire_id` in the CSV must be unique;** otherwise the
script will exit with an error to avoid two workers writing to the same
locations.