SCR="/glade/derecho/scratch/ljaeger/workflow"
WRK="/glade/work/ljaeger/wrfout/fire"

find "$SCR"/fire_* -path "*/wrf/*/wrfout_*" | shuf -n 20 | while read -r src; do
  fire=$(basename "$(dirname "$(dirname "$(dirname "$src")")")")
  date=$(basename "$(dirname "$src")")
  fname=$(basename "$src")
  dst="$WRK/$fire/$date/$fname"

  if [[ ! -f "$dst" ]]; then
    echo "MISSING DST: $dst"
    continue
  fi

  python - "$src" "$dst" <<'PY'
import sys
import numpy as np
from netCDF4 import Dataset

src = sys.argv[1]
dst = sys.argv[2]

with Dataset(src) as a, Dataset(dst) as b:
    t2a = a.variables["T2"][0, :, :]
    t2b = b.variables["T2"][0, :, :]
    d = np.nanmax(np.abs(t2a - t2b))
print(f"{d:.6g}  {src} -> {dst}")
PY

done
