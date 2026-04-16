# WRF Run Workflow Overview

This document summarizes the current fire-oriented workflow in this repository, with emphasis on the files that actually drive runs on Derecho/Casper.

It is written from the code paths that are active today, not from older helper scripts.

## Short version

The core workflow is:

1. Build per-fire configs and template directories.
2. Choose a simulation day from `fires/output_budget.csv`.
3. For each fire on that day, call `wps_wrf_workflow/setup_wps_wrf.py`.
4. Let `setup_wps_wrf.py` decide which stages still need to run.
5. Submit and monitor the individual WPS/WRF stages:
   - geogrid
   - ungrib
   - metgrid
   - real
   - wrf
6. Write outputs to scratch under `/glade/derecho/scratch/ljaeger/workflow/<fire_id>/`.

The two most important entry points are:

- `fires/run_budget_day.py`
- `wps_wrf_workflow/setup_wps_wrf.py`

## Core execution path

### 1. Input selection

The day-level driver is `fires/run_budget_day.py`.

It reads:

- `fires/output_budget.csv`

That CSV is treated as the list of approved fire/day runs. The script groups rows by `date`, lets you choose a day by order of appearance, and computes the WRF cycle start as:

- `sim_start = date at 00 UTC minus 6 hours`
- Example: `2018-07-17` becomes `20180716_18`

Important behaviors in `fires/run_budget_day.py`:

- Loads one generated workflow YAML per fire from `configs/built/workflow/<fire_id>.yaml`
- Skips a run if the WRF output directory already has at least `--wrfout-threshold` files
- Shares the first fire's `ungrib` output with later fires on the same day by symlinking their cycle `ungrib` directories to the first one
- Can retry a narrow class of failed WRF cases with `--retry-vcfl`
- Writes per-fire run logs to `logs/budget_runs/` by default

### 2. Per-fire config generation

Generated configs come from `fires/generate_configs.py`.

This script:

- Reads fire metadata from `fires/runs.csv`
- Copies the base WRF template directory from `configs/templates/wrf/` into `configs/built/wrf/<fire_id>/`
- Writes a per-fire workflow YAML to `configs/built/workflow/<fire_id>.yaml`
- Injects fire-specific latitude/longitude into `namelist.wps.hrrr`
- Creates `iofields_fire.txt` and patches `namelist.input*` so WRF history output is trimmed to the desired field list

The workflow YAML sets the main storage roots:

- `wps_run_dir`: `/glade/derecho/scratch/ljaeger/workflow/<fire_id>/wps`
- `wrf_run_dir`: `/glade/derecho/scratch/ljaeger/workflow/<fire_id>/wrf`
- `grib_dir`: `/glade/derecho/scratch/ljaeger/data/hrrr`

### 3. Cycle orchestration

The actual WPS/WRF engine is `wps_wrf_workflow/setup_wps_wrf.py`.

For each cycle, it:

- Loads the YAML config
- Resolves the cycle-specific WPS and WRF run directories
- Chooses namelist templates from the fire template directory
- Resolves the GRIB source directory based on IC/LBC model and source
- Decides which workflow stages to run
- Calls the stage-specific runner scripts

The main run directories are:

- Shared geogrid: `<wps_run_dir_parent>/geogrid`
- Cycle WPS: `<wps_run_dir_parent>/<YYYYMMDD_HH>`
- Cycle WRF: `<wrf_run_dir_parent>/<YYYYMMDD_HH>`
- Cycle ungrib output: `<wps_run_dir>/<cycle>/ungrib`
- Cycle metgrid output: `<wps_run_dir>/<cycle>/metgrid`

## Auto-stage logic

The most important repo behavior is in `setup_wps_wrf.py --auto-stages`.

Current rules:

- If `wrfout_d0*` count is at least the threshold, treat the run as complete and skip everything.
- Else if `wrfinput_d0*` and `wrfbdy_d0*` exist, run WRF only.
- Else if `met_em.d0*` exists, run `real` and `wrf`.
- Else run any missing `geogrid` and `ungrib`, then run `metgrid`, `real`, and `wrf`.

Completion checks are based on:

- geogrid: `geo_em.d0*`
- ungrib: any of `FILE:*`, `PFILE:*`, `GFS:*`, `GFS_FNL:*`, `GEFS_*:*`, `HRRR_*:*`, `HRRR_soil*`
- metgrid: `met_em.d0*`
- real: `wrfinput_d0*` and `wrfbdy_d0*`
- wrf: `wrfout_d0* >= threshold`

This makes the workflow restart-friendly. The day driver depends on this behavior.

## Stage runner responsibilities

### `wps_wrf_workflow/run_geogrid.py`

Purpose:

- Creates or reuses the shared geogrid run directory
- Links `geogrid.exe`
- Copies `submit_geogrid.bash.*`
- Copies `namelist.wps`
- Submits the batch job and waits for successful completion

Outputs:

- Shared `geo_em.d0*` files under the geogrid output path referenced in `namelist.wps`

### `wps_wrf_workflow/run_ungrib.py`

Purpose:

- Creates a fresh cycle `ungrib` output directory
- Locates GRIB inputs for HRRR/GFS/GFS_FNL/GEFS
- Builds the right `namelist.wps` and Vtable selection
- Runs ungrib in parallel across times
- Monitors job completion and output

Outputs:

- WPS intermediate files in the cycle `ungrib` directory
- Prefixes depend on the IC/LBC source, e.g. `HRRR_*`, `GFS*`, `GEFS_*`

### `wps_wrf_workflow/run_avg_tsfc.py`

Purpose:

- Optional preprocessing step for lake surface temperatures

Used when:

- `do_avg_tsfc` is enabled

### `wps_wrf_workflow/run_metgrid.py`

Purpose:

- Uses `ungrib` output, optional `TAVGSFC`, and optional GEOS-5 aerosol intermediates
- Rewrites `namelist.wps` for start/end time and `fg_name`
- Submits and monitors `metgrid.exe`

Outputs:

- `met_em.d0*` files in the cycle `metgrid` directory

### `wps_wrf_workflow/run_real.py`

Purpose:

- Creates the WRF run directory
- Links WRF runtime files from the WRF installation
- Copies and rewrites `namelist.input`
- Links `met_em*`
- Submits and monitors `real.exe`

Outputs:

- `wrfinput_d0*`
- `wrfbdy_d01`

### `wps_wrf_workflow/run_wrf.py`

Purpose:

- Rebuilds `namelist.input` for the cycle
- Verifies `wrfinput`/`wrfbdy` are present
- Copies any configured `iofields` files from the template directory
- Submits `wrf.exe`
- Optionally keeps the script alive to monitor the batch job

Outputs:

- `wrfout_d0*`
- optionally `wrfxtrm*`

### Optional post-processing

`setup_wps_wrf.py` can also call:

- `wps_wrf_workflow/run_upp.py`

and can archive selected outputs if `archive: true` is enabled in the YAML.

## File map by role

### Main entry points

- `fires/run_budget_day.py`
  - Main day-level driver for the fire workflow
- `wps_wrf_workflow/setup_wps_wrf.py`
  - Main cycle-level WPS/WRF orchestrator

### Config generation and templates

- `fires/generate_configs.py`
  - Generates fire-specific YAMLs and template directories
- `configs/templates/workflow/base.yaml`
  - Base workflow YAML template
- `configs/templates/wrf/namelist.wps.hrrr`
  - Base WPS namelist template
- `configs/templates/wrf/namelist.input.hrrr.hybr`
  - Base WRF namelist template
- `configs/templates/wrf/submit_*.bash.{derecho,casper}`
  - Batch submission templates for each stage

### Supporting workflow modules

- `wps_wrf_workflow/proc_util.py`
  - Shared command execution helper used by stage runners
- `wps_wrf_workflow/wps_wrf_util.py`
  - Small utility helpers, mainly file-content search
- `wps_wrf_workflow/download_hrrr_from_aws_or_gc.py`
  - HRRR download/link helper
- `wps_wrf_workflow/link_gfs_from_glade.py`
  - GFS link helper
- `wps_wrf_workflow/link_gfs_fnl_from_glade.py`
  - GFS FNL link helper
- `wps_wrf_workflow/download_gfs_from_aws.py`
  - GFS download helper
- `wps_wrf_workflow/download_gefs_from_aws.py`
  - GEFS download helper

### Day-level control and reporting

- `scripts/run_budget_parallel.sh`
  - Launches multiple `run_budget_day.py` day indices in parallel with queue throttling
- `fires/report_stage_counts.py`
  - Scans the budget CSV plus scratch directories and reports which stage each run has reached
- `fires/cleanup_day.py`
  - Cleanup helper used after successful day runs
- `fires/report_day_status.py`
  - Status reporting around daily runs

### Inputs and generated artifacts

- `fires/output_budget.csv`
  - Current day-level run list
- `fires/runs.csv`
  - Fire metadata used to build configs
- `configs/built/workflow/<fire_id>.yaml`
  - Generated per-fire workflow YAML
- `configs/built/wrf/<fire_id>/`
  - Generated per-fire namelist and batch-script template directory
- `logs/budget_runs/*.log`
  - Day-run logs

## Expected scratch layout

The workflow writes to scratch, not into the repo.

Main tree:

```text
/glade/derecho/scratch/ljaeger/workflow/
  fire_<id>/
    wps/
      geogrid/
      <cycle>/
        ungrib/
        metgrid/
    wrf/
      <cycle>/
        wrfinput_d0*
        wrfbdy_d01
        wrfout_d0*
```

Where `<cycle>` is usually a string like `YYYYMMDD_18`.

## Typical commands

List available days:

```bash
python fires/run_budget_day.py --list-days
```

Run one day with auto-stage selection:

```bash
python fires/run_budget_day.py --day-index N
```

Resume a day starting at a later fire:

```bash
python fires/run_budget_day.py --day-index N --start-fire M
```

Run several days in parallel with queue throttling:

```bash
scripts/run_budget_parallel.sh --max-days 10 --max-jobs 400 668 669 670
```

Generate configs:

```bash
python fires/generate_configs.py
```

Inspect stage completion across the budget CSV:

```bash
python fires/report_stage_counts.py
```

## What is current vs. older

These files appear to be older or less central than the current fire workflow:

- `fires/run.py`
  - Older CSV command runner based on a `command` column
- `fires/main.py`
  - Older config/bootstrap script for a different directory layout
- `fires/runs.py`
  - Small ad hoc launcher script

The current repo center of gravity is:

- `fires/run_budget_day.py`
- `fires/generate_configs.py`
- `wps_wrf_workflow/setup_wps_wrf.py`

## Where to start reading

If you only want four files to understand the repo, read these in order:

1. `README.md`
2. `fires/run_budget_day.py`
3. `fires/generate_configs.py`
4. `wps_wrf_workflow/setup_wps_wrf.py`

Then read the stage runners in this order:

1. `wps_wrf_workflow/run_geogrid.py`
2. `wps_wrf_workflow/run_ungrib.py`
3. `wps_wrf_workflow/run_metgrid.py`
4. `wps_wrf_workflow/run_real.py`
5. `wps_wrf_workflow/run_wrf.py`
