from pathlib import Path
import yaml
from datetime import datetime, timedelta
import csv
import shutil
import re
import pandas as pd
import argparse

def generate_csv():
    directory = Path("./wsts/")
    output_csv = Path("./wsts/fires.csv")

    with open(output_csv, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["key", "latitude", "longitude", "current", "sim_start", "start", "end", "command"])

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

    print(f"Generated CSV: {output_csv}")

def setup_fire():
    df = pd.read_csv("./wsts/fires.csv")

    config = Path("./configs/base.yaml")
    template = Path("./templates/base/")
    fires = df.groupby("key")

    for fire_id, group in list(fires):
        # --------------------------------------------------
        # copy base configs and templates to over to run
        # specific configs and templates
        # --------------------------------------------------

        fire_config = Path(f"./configs/{fire_id}.yaml")
        fire_template = Path(f"./templates/{fire_id}/")
        fire_wps_template = fire_template / "namelist.wps.hrrr"
        fire_input_template = fire_template / "namelist.input.hrrr.hybr"

        shutil.copy(config, fire_config)
        shutil.copytree(template, fire_template)

        # --------------------------------------------------
        # modify config so that each run gets it's own
        # directories for wrf and wps and hrrr
        # --------------------------------------------------

        with open(fire_config, "r") as f:
            fire_config_yaml = yaml.safe_load(f)

        fire_config_yaml["template_dir"] = f"/glade/u//home/ljaeger/wrf-run/templates/{fire_id}"
        fire_config_yaml["wps_run_dir"] = f"/glade/derecho/scratch/ljaeger/workflow/{fire_id}/wps"
        fire_config_yaml["wrf_run_dir"] = f"/glade/derecho/scratch/ljaeger/workflow/{fire_id}/wrf"
        fire_config_yaml["grib_dir"] = "/glade/derecho/scratch/ljaeger/data/hrrr"

        # -------------------------------------------------
        # write fire latitude and longitude to wps namelist
        # -------------------------------------------------

        with open(fire_config, "w") as f:
            yaml.dump(fire_config_yaml, f, sort_keys=False)

        text = fire_wps_template.read_text()
        lat = round(group.iloc[0]["latitude"], 4)
        lon = round(group.iloc[0]["longitude"], 4)

        text = re.sub(r"ref_lat\s*=\s*[\d\.\-]+", f"ref_lat   =  {lat}", text)
        text = re.sub(r"ref_lon\s*=\s*[\d\.\-]+", f"ref_lon   =  {lon}", text)
        text = re.sub(r"truelat1\s*=\s*[\d\.\-]+", f"truelat1  =  {lat}", text)
        text = re.sub(r"truelat2\s*=\s*[\d\.\-]+", f"truelat2  =  {lat}", text)
        text = re.sub(r"stand_lon\s*=\s*[\d\.\-]+", f"stand_lon =  {lon}", text)

        for keyword in ("opt_output_from_geogrid_path", "opt_output_from_metgrid_path"):
            text = re.sub(
                rf"(?m)^(.*{keyword}.*)$",
                lambda match: match.group(0).replace("UM_WRF_1Dom1km", fire_id),
                text,
                count=1,
            )

        fire_wps_template.write_text(text)

        print(f"Setup complete for fire: {fire_id}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process fire YAMLs and generate configs.")
    parser.add_argument("--generate-csv", action="store_true", help="Generate fires.csv from YAML files.")
    parser.add_argument("--setup-fire", action="store_true", help="Setup configs and templates for fires.")
    args = parser.parse_args()

    if args.generate_csv:
        generate_csv()
    elif args.setup_fire:
        setup_fire()
    else:
        parser.print_help()
