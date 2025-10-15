from pathlib import Path
import yaml
from datetime import datetime, timedelta
import csv
import shutil
import re
import pandas as pd


directory = Path("./wsts/")

# Open CSV once and write the header
with open("./wsts/fires.csv", "w", newline="") as csvfile:
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



df = pd.read_csv("./wsts/fires.csv")

config = Path("./configs/base.yaml")
template = Path("/templates/base/")

fires = df.groupby("key")

for fire_id, group in list(fires)[:1]:
    # move all the files around to the correct spots

    fire_config = Path(f"./configs/{fire_id}.yaml")

    fire_template = Path(f"./templates/{fire_id}/")
    fire_wps_template = fire_template / "namelist.wps.hrrr"
    fire_input_template = fire_template / "namelist.input.hrrr"

    shutil.copy(config, fire_config)
    shutil.copytree(template, fire_template)

    # edit the config 

    with open(fire_config, "r") as f:
        fire_config_yaml = yaml.safe_load(f)

    fire_config_yaml["exp_name"] = fire_id

    with open(fire_config, "w") as f:
        yaml.dump(fire_config_yaml, f, sort_keys=False)

    # edit the wps namelist 

    text = fire_wps_template.read_text()

    lat = round(group.iloc[0]["latitude"], 4)
    lon = round(group.iloc[0]["longitude"], 4)

    text = re.sub(r"ref_lat\s*=\s*[\d\.\-]+", f"ref_lat   =  {lat}", text)
    text = re.sub(r"ref_lon\s*=\s*[\d\.\-]+", f"ref_lon   =  {lon}", text)
    text = re.sub(r"truelat1\s*=\s*[\d\.\-]+", f"truelat1  =  {lat}", text)
    text = re.sub(r"truelat2\s*=\s*[\d\.\-]+", f"truelat2  =  {lat}", text)
    text = re.sub(r"stand_lon\s*=\s*[\d\.\-]+", f"stand_lon =  {lon}", text)

    fire_wps_template.write_text(text)

    # rename the input namelist
    
    name = fire_input_template.with_suffix(f".{fire_id}")
    fire_input_template.rename(name)
