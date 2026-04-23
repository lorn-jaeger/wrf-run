# Minimal Commands

```bash
cd /glade/u/home/ljaeger/wrf-run

module load conda
conda activate /glade/work/ljaeger/conda-envs/workflow

# Skip this block if test.csv already exists.
python - <<'PY'
import csv
from pathlib import Path

src = Path("fires/data/output_budget.csv")
dst = Path("test.csv")

with src.open() as f:
    rows = list(csv.DictReader(f))

keep_dates = []
seen = set()
for row in rows:
    d = row["date"]
    if d not in seen:
        seen.add(d)
        keep_dates.append(d)
    if len(keep_dates) == 3:
        break

keep = set(keep_dates)

with dst.open("w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=["date", "latitude", "longitude", "fire_id"])
    w.writeheader()
    for row in rows:
        if row["date"] in keep:
            w.writerow({k: row[k] for k in ["date", "latitude", "longitude", "fire_id"]})
PY

python fires/generate_configs.py \
  --runs-csv test.csv \
  --output-dir configs/test \
  --workflow-root /glade/derecho/scratch/ljaeger/wrf-run-test/workflow \
  --grib-root /glade/derecho/scratch/ljaeger/data/hrrr \
  --overwrite

python fires/run_budget_day.py --list-days --runs-file test.csv

./run.sh \
  --runs-file test.csv \
  --max-days 1 \
  --max-jobs 50 \
  1 2 3 \
  -- \
  --config-root configs/test/workflow \
  --logs-dir logs/test

qstat -u ljaeger
find logs/test -type f | sort
```
