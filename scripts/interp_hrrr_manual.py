#!/usr/bin/env python3
import argparse
import os
import pathlib
import re
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple


@dataclass
class GribRecord:
    recnum: int
    var: str
    level: str
    time_range: str
    key: str
    inv_line: str


def find_wgrib2() -> Optional[str]:
    wgrib2_exe = shutil.which("wgrib2")
    if wgrib2_exe:
        return wgrib2_exe
    env_exe = os.environ.get("WGRIB2")
    if env_exe and pathlib.Path(env_exe).exists():
        return env_exe
    root = pathlib.Path("/glade/u/apps")
    if root.exists():
        matches = sorted(root.glob("**/wgrib2*/bin/wgrib2"))
        if matches:
            return str(matches[0])
    return None


def run(cmd: List[str], capture: bool = False, quiet: bool = False) -> subprocess.CompletedProcess:
    if capture:
        return subprocess.run(cmd, capture_output=True, text=True)
    if quiet:
        return subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return subprocess.run(cmd)


def parse_inventory(output: str) -> List[GribRecord]:
    records: List[GribRecord] = []
    for line in output.splitlines():
        match = re.match(r"^\s*(\d+):\d+:d=\d{10}:(.*)$", line)
        if not match:
            continue
        recnum = int(match.group(1))
        rest = match.group(2)
        parts = rest.split(":")
        if len(parts) < 2:
            continue
        var = parts[0].strip()
        level = parts[1].strip()
        time_range = ":".join(parts[2:]).strip()
        key = f"{var}:{level}:{time_range}"
        records.append(GribRecord(recnum, var, level, time_range, key, line.strip()))
    return records


def list_records(wgrib2_exe: str, grib_path: pathlib.Path) -> List[GribRecord]:
    cmd = [wgrib2_exe, str(grib_path), "-s"]
    result = run(cmd, capture=True)
    output = (result.stdout or "") + (result.stderr or "")
    return parse_inventory(output)


def build_clean_file(
    wgrib2_exe: str,
    grib_path: pathlib.Path,
    drop_vars: List[str],
) -> pathlib.Path:
    records = list_records(wgrib2_exe, grib_path)
    drop_vars_set = {v.upper() for v in drop_vars}
    drop_recs = sorted({rec.recnum for rec in records if rec.var.upper() in drop_vars_set})
    if not drop_recs:
        return grib_path
    last_rec = records[-1].recnum if records else 0
    if last_rec == 0:
        return grib_path
    ranges: List[Tuple[int, int]] = []
    start = 1
    for recnum in drop_recs:
        if start <= recnum - 1:
            ranges.append((start, recnum - 1))
        start = recnum + 1
    if start <= last_rec:
        ranges.append((start, last_rec))

    clean_path = grib_path.with_suffix(grib_path.suffix + ".clean")
    if clean_path.exists():
        clean_path.unlink()

    with tempfile.TemporaryDirectory() as tmpdir:
        for idx, (r_start, r_end) in enumerate(ranges):
            part_path = pathlib.Path(tmpdir) / f"part_{idx}.grib2"
            cmd = [
                wgrib2_exe,
                str(grib_path),
                "-for", f"{r_start}:{r_end}:1",
                "-grib", str(part_path),
            ]
            run(cmd, capture=False, quiet=True)
            if part_path.exists():
                with open(clean_path, "ab") as out_f, open(part_path, "rb") as in_f:
                    shutil.copyfileobj(in_f, out_f)

    return clean_path if clean_path.exists() else grib_path


def is_stat_field(time_range: str) -> bool:
    tr = time_range.lower()
    if not tr or tr == "anl":
        return False
    stat_keywords = ["acc", "ave", "avg", "max", "min", "sum", "var", "prob"]
    for kw in stat_keywords:
        if re.search(rf"\b{kw}\b", tr):
            return True
    return False


def run_with_stdin(cmd: List[str], stdin_path: pathlib.Path) -> None:
    with open(stdin_path, "r", encoding="utf-8") as fh:
        subprocess.run(cmd, stdin=fh, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def set_date(wgrib2_exe: str, grib_path: pathlib.Path, date_code: str) -> None:
    tmp_path = grib_path.with_suffix(grib_path.suffix + ".tmp")
    cmd = [wgrib2_exe, str(grib_path), "-set_date", date_code, "-grib", str(tmp_path)]
    result = run(cmd, capture=False, quiet=True)
    if tmp_path.exists() and tmp_path.stat().st_size > 0:
        tmp_path.replace(grib_path)
    elif result.returncode != 0:
        print(f"[WARN] wgrib2 -set_date failed for {grib_path.name}", file=sys.stderr)


def main() -> int:
    parser = argparse.ArgumentParser(description="Interpolate missing HRRR analysis file between two hours.")
    parser.add_argument("--prev", required=True, help="Previous hour GRIB2 file (e.g. t01z)")
    parser.add_argument("--next", required=True, help="Next hour GRIB2 file (e.g. t03z)")
    parser.add_argument("--out", required=True, help="Output GRIB2 file (e.g. t02z)")
    parser.add_argument("--weight-prev", type=float, default=0.5, help="Weight for prev file (default 0.5)")
    parser.add_argument("--weight-next", type=float, default=0.5, help="Weight for next file (default 0.5)")
    parser.add_argument("--drop-var", action="append", default=["ASNOW"], help="Drop bad variables (default ASNOW)")
    parser.add_argument("--valid-time", help="Set date code YYYYMMDDHH (optional)")
    parser.add_argument("--overwrite", action="store_true", help="Allow overwriting output file")
    args = parser.parse_args()

    wgrib2_exe = find_wgrib2()
    if not wgrib2_exe:
        print("ERROR: wgrib2 not found in PATH or /glade/u/apps", file=sys.stderr)
        return 2

    prev_path = pathlib.Path(args.prev)
    next_path = pathlib.Path(args.next)
    out_path = pathlib.Path(args.out)
    if out_path.exists() and not args.overwrite:
        print(f"ERROR: output file exists: {out_path} (use --overwrite)", file=sys.stderr)
        return 2

    prev_clean = build_clean_file(wgrib2_exe, prev_path, args.drop_var)
    next_clean = build_clean_file(wgrib2_exe, next_path, args.drop_var)

    prev_records = list_records(wgrib2_exe, prev_clean)
    next_records = list_records(wgrib2_exe, next_clean)
    next_by_key: Dict[str, GribRecord] = {rec.key: rec for rec in next_records}

    inst_prev_lines: List[str] = []
    inst_next_lines: List[str] = []
    stat_prev_lines: List[str] = []
    for rec in prev_records:
        rec_next = next_by_key.get(rec.key)
        if rec_next and not is_stat_field(rec.time_range):
            inst_prev_lines.append(rec.inv_line)
            inst_next_lines.append(rec_next.inv_line)
        else:
            stat_prev_lines.append(rec.inv_line)

    if out_path.exists():
        out_path.unlink()

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = pathlib.Path(tmpdir)
        inst_prev_txt = tmpdir_path / "inst_prev.txt"
        inst_next_txt = tmpdir_path / "inst_next.txt"
        stat_prev_txt = tmpdir_path / "stat_prev.txt"
        inst_prev_grib = tmpdir_path / "inst_prev.grib2"
        inst_next_grib = tmpdir_path / "inst_next.grib2"
        stat_prev_grib = tmpdir_path / "stat_prev.grib2"
        out_inst = tmpdir_path / "out_inst.grib2"

        if inst_prev_lines:
            inst_prev_txt.write_text("\n".join(inst_prev_lines) + "\n", encoding="utf-8")
            inst_next_txt.write_text("\n".join(inst_next_lines) + "\n", encoding="utf-8")
            run_with_stdin([wgrib2_exe, str(prev_clean), "-i", "-grib", str(inst_prev_grib)], inst_prev_txt)
            run_with_stdin([wgrib2_exe, str(next_clean), "-i", "-grib", str(inst_next_grib)], inst_next_txt)
            cmd_interp = [
                wgrib2_exe,
                str(inst_prev_grib),
                "-rpn", f"{args.weight_prev:.6f}:*:sto_1",
                "-import_grib", str(inst_next_grib),
                "-rpn", f"{args.weight_next:.6f}:*:rcl_1:+",
                "-grib", str(out_inst),
            ]
            run(cmd_interp, capture=False, quiet=True)

        if stat_prev_lines:
            stat_prev_txt.write_text("\n".join(stat_prev_lines) + "\n", encoding="utf-8")
            run_with_stdin([wgrib2_exe, str(prev_clean), "-i", "-grib", str(stat_prev_grib)], stat_prev_txt)

        with open(out_path, "ab") as out_f:
            if inst_prev_lines and out_inst.exists():
                with open(out_inst, "rb") as in_f:
                    shutil.copyfileobj(in_f, out_f)
            if stat_prev_lines and stat_prev_grib.exists():
                with open(stat_prev_grib, "rb") as in_f:
                    shutil.copyfileobj(in_f, out_f)

    if args.valid_time:
        date_code = args.valid_time + "0000"
        set_date(wgrib2_exe, out_path, date_code)

    print(f"Wrote {out_path}")
    return 0


if __name__ == "__main__":
    import os
    raise SystemExit(main())
