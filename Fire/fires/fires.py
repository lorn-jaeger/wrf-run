from pathlib import Path
import yaml
from datetime import datetime, timedelta
import csv

directory = Path("./configs/")

# Open CSV once and write the header
with open("output.csv", "w", newline="") as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(["key", "latitude", "longitude", "current", "sim_start", "start", "end"])

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
                    writer.writerow([key, lat, lon, current, sim_start, start, end])
                    current += timedelta(days=1)

