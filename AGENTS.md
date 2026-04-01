# AGENTS.md

Local guidance for Codex agents working in `/glade/u/home/ljaeger/wrf-run`.

## Repo context (quick)
- Primary workflow runner: `fires/run_budget_day.py` (reads `fires/output_budget.csv`).
- Workflow engine: `wps_wrf_workflow/setup_wps_wrf.py` (controls WPS/WRF stages).
- Auto-stage logic (geogrid/ungrib/metgrid/real/wrf) is **inside** `setup_wps_wrf.py`.
- WRF completion threshold: `wrfout_d0*` count **>= 12**.
- Main data roots:
  - Workflow output: `/glade/derecho/scratch/ljaeger/workflow/<fire_id>/{wps,wrf}/<cycle>`
  - Configs: `configs/built/workflow/<fire_id>.yaml`
  - Budget CSV: `fires/output_budget.csv`

## Common tasks & scripts
- Run a day (auto-stage default):
  - `python fires/run_budget_day.py --day-index N`
  - `--dry-run` prints commands without running.
- Report stage counts across all budget runs:
  - `python fires/report_stage_counts.py`
  - Filters: `--fire-id`, `--date YYYY-MM-DD`, `--sim-start YYYYMMDD_HH`, `--config-file <yaml>`.
- Build a no‑buffer “unrun” CSV:
  - `python fires/build_unrun_no_buffer_csv.py`

## Stage detection rules (current behavior)
Auto-stages in `setup_wps_wrf.py` decide per-cycle:
- If `wrfout_d0*` count >= 12 → skip all stages.
- Else if `wrfinput_d0*` + `wrfbdy_d0*` exist → run WRF only.
- Else if `met_em.d0*` exists → run real + WRF.
- Else → run missing `geogrid` / `ungrib`, then `metgrid + real + wrf`.
- Ungrib “done” is detected by any of:
  - `FILE:*`, `PFILE:*`, `GFS:*`, `GFS_FNL:*`, `GEFS_*:*`, `HRRR_*:*`, `HRRR_soil*`

## Practical notes
- Geogrid output is one‑time per fire (shared) and lives under `wps_run_dir/geogrid`.
- Deleting intermediate files is OK; WRF output is the terminal goal.
- `fires/run_budget_day.py` skips running a fire/day if WRF already meets threshold.

## Editing conventions
- Prefer `rg` for searching.
- Use `apply_patch` for small edits.
- Keep changes ASCII‑only unless the file already uses unicode.

## Safety
- Do **not** delete or move scratch outputs unless explicitly requested.
- Avoid destructive git commands (`reset --hard`, `checkout --`) unless asked.
