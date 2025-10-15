from pathlib import Path
import yaml
from datetime import datetime, timedelta
import csv

directory = Path("./configs/")

# Open CSV once and write the header
with open("output_wcommand.csv", "w", newline="") as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(["key", "latitude", "longitude", "current", "sim_start", "start", "end", "command"])

    # Loop through all YAML files
    for file_path in directory.glob("*.yml"):
        with open(file_path, 'r') as f:
            data = yaml.safe_load(f)
            keys = list(data.keys())

            for key in keys[3:]:
                row = data[key]
                lat = row["latitude"]
                lon = row["longitude"]
                start = row["start"]
                end = row["end"]
                current = start

                while current <= end:
                    sim_start = (datetime.combine(current, datetime.min.time()) - timedelta(hours=6)).strftime("%Y%m%d_%H")
                    command = f"./setup_wps_wrf.py -b {sim_start} -c configs/{key}.yaml"
                    writer.writerow([key, lat, lon, current, sim_start, start, end, command])
                    current += timedelta(days=1)

import pandas as pd
from pathlib import Path
import shutil
import yaml
import re

# Paths
df = pd.read_csv("./fires/output.csv")
wps_template = Path("./namelist.wps.hrrr")
input_template = Path("./namelist.input.hrrr")
config_template = Path("./wps_wrf_config.yaml")

fires = df.groupby("key")

for fire_id, group in list(fires)[:1]:
    print(group.iloc[0])
    lat = round(group.iloc[0]["latitude"], 4)
    lon = round(group.iloc[0]["longitude"], 4)

    print(f"Generating templates for {fire_id} ({lat:.4f}, {lon:.4f})")

    # --- 1. namelist.wps.hrrr.fire_id ---
    wps_text = wps_template.read_text()

    # Replace all float-looking lat/lon values with the new ones
    # Assuming consistent formatting like ref_lat, ref_lon, truelat1, etc.
    wps_text = re.sub(r"ref_lat\s*=\s*[\d\.\-]+", f"ref_lat   =  {lat}", wps_text)
    wps_text = re.sub(r"ref_lon\s*=\s*[\d\.\-]+", f"ref_lon   =  {lon}", wps_text)
    wps_text = re.sub(r"truelat1\s*=\s*[\d\.\-]+", f"truelat1  =  {lat}", wps_text)
    wps_text = re.sub(r"truelat2\s*=\s*[\d\.\-]+", f"truelat2  =  {lat}", wps_text)
    wps_text = re.sub(r"stand_lon\s*=\s*[\d\.\-]+", f"stand_lon =  {lon}", wps_text)

    wps_out = Path(f"./namelist.wps.hrrr.{fire_id}")
    wps_out.write_text(wps_text)

    # --- 2. namelist.input.hrrr.fire_id ---
    input_out = Path(f"./namelist.input.hrrr.{fire_id}")
    shutil.copy(input_template, input_out)

    # --- 3. fire_id.yml ---
    with open(config_template, "r") as f:
        config = yaml.safe_load(f)

    config["exp_name"] = fire_id  # replace experiment name

    config_out = Path(f"./{fire_id}.yml")
    with open(config_out, "w") as f:
        yaml.dump(config, f, sort_keys=False)

print("✅ All template files generated.")


