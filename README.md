# wrf-run

Local automation for running WPS/WRF fire workflows on NCAR systems.

The repository has two main entry points:

- `fires/run_budget_day.py` runs one day of fire workflows from `fires/output_budget.csv`.
- `wps_wrf_workflow/setup_wps_wrf.py` drives the WPS and WRF stages for each cycle.

## Layout

- `fires/`: daily runner, reporting utilities, CSV-driven filtering, and cleanup helpers.
- `wps_wrf_workflow/`: workflow engine and stage runners for geogrid, ungrib, metgrid, real, and wrf.
- `configs/`: templates and saved workflow configuration inputs.
- `data/`: small reference CSV inputs.
- `scripts/`: helper scripts for setup, monitoring, and parallel execution.

Large local installations and generated outputs are intentionally kept out of version control:

- `bin/`: local WPS/WRF installs and build artifacts.
- `compression/`: local experiments and large NetCDF outputs.
- `logs/`: runtime logs and archived run output.
- `configs/save/`: local saved per-fire config snapshots.
- `configs/built/`: generated per-fire workflow YAMLs.

## Common Commands

List available budget days:

```bash
python fires/run_budget_day.py --list-days
```

Run a single day with automatic stage selection:

```bash
python fires/run_budget_day.py --day-index N
```

Preview a day without running it:

```bash
python fires/run_budget_day.py --day-index N --dry-run
```

Report workflow stage completion across the budget CSV:

```bash
python fires/report_stage_counts.py
```

Build the no-buffer unrun CSV:

```bash
python fires/build_unrun_no_buffer_csv.py
```

## Workflow Notes

- WRF completion is treated as `wrfout_d0* >= 12`.
- Auto-stage selection lives in `wps_wrf_workflow/setup_wps_wrf.py`.
- Shared geogrid output is kept under each fire's `wps` parent directory.
- Workflow outputs are written to scratch storage, not this repository.
