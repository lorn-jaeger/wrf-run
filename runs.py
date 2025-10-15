# df = pd.read_csv("./addons/fires/wsts/fires.csv")
# sims = df.groupby("key").first().sample(n=100, random_state=1)
#sims.to_csv("runs.csv")

import pandas as pd
import subprocess
from pathlib import Path

df = pd.read_csv("runs.csv").head(10)
Path("logs").mkdir(exist_ok=True)

procs = []

# launch all processes
for _, row in df.iterrows():
    key = row["key"]
    cmd = row["command"]
    log_path = Path("logs") / f"{key}.log"

    print(f"[START] {key}: {cmd}")
    log = open(log_path, "a")
    p = subprocess.Popen(cmd, shell=True, stdout=log, stderr=log)
    procs.append((key, cmd, p, log))

# wait for completion and print status
for key, cmd, p, log in procs:
    ret = p.wait()
    log.close()
    if ret == 0:
        print(f"[SUCCESS] {key} finished successfully.")
    else:
        print(f"[FAILURE] {key} exited with status {ret}. See logs/{key}.log")


