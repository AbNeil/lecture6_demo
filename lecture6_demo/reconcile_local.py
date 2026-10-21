"""Local reconciliation: do two different methods agree?

Method 1 ("ELT-style"): plain pandas on the raw CSV files, applying the same rules.
Method 2 ("ETL"): the output of the Beam pipeline in out/real/hourly-*.jsonl.
Run after ./run_local_real.sh:   python reconcile_local.py
"""
import glob
import json
import sys

import pandas as pd

RAW = sorted(glob.glob("data/raw/*.csv"))
OUT = sorted(glob.glob("out/real/hourly-0*.jsonl"))
if not RAW or not OUT:
    sys.exit("Run ./get_data_local.sh and ./run_local_real.sh first.")


def to_time(series):
    """Seconds since 1970 or ISO text -> UTC timestamps (bad values become NaT)."""
    numeric = pd.to_numeric(series, errors="coerce")
    if numeric.notna().all():
        return pd.to_datetime(numeric, unit="s", utc=True)
    try:
        return pd.to_datetime(series, errors="coerce", utc=True, format="ISO8601")
    except (TypeError, ValueError):  # older pandas
        return pd.to_datetime(series, errors="coerce", utc=True)


parts = []
for path in RAW:
    cols = pd.read_csv(path, nrows=0).columns
    need = ["station_id", "last_reported", "num_bikes_available"]
    if not set(need) <= set(cols):
        print("skipping (required column missing):", path)
        continue
    df = pd.read_csv(path, usecols=need, dtype=str)
    df["station_id"] = pd.to_numeric(df["station_id"], errors="coerce")
    df["bikes"] = pd.to_numeric(df["num_bikes_available"], errors="coerce")
    df["time"] = to_time(df["last_reported"])
    ok = df[df.station_id.notna() & df.bikes.notna() & (df.bikes >= 0) & df.time.notna()].copy()
    ok["station_id"] = ok["station_id"].astype(int)
    ok["bikes"] = ok["bikes"].astype(int)
    ok["hour"] = ok["time"].dt.floor("h")
    parts.append(ok[["station_id", "hour", "bikes"]])

elt = (pd.concat(parts).groupby(["station_id", "hour"])
         .agg(samples=("bikes", "size"), avg=("bikes", "mean")).reset_index())
elt["avg"] = elt["avg"].round(2)

etl = pd.DataFrame([json.loads(line) for path in OUT for line in open(path)])
etl["hour"] = pd.to_datetime(etl["hour_start"].str.replace(" UTC", "", regex=False), utc=True)

merged = elt.merge(etl, on=["station_id", "hour"])
match = int(((merged.samples_x == merged.samples_y) & ((merged.avg - merged.avg_bikes).abs() < 0.01)).sum())
print(f"elt_rows      {len(elt)}")
print(f"etl_rows      {len(etl)}")
print(f"matching_rows {match}")
if len(elt) == len(etl) == match:
    print("All three equal: two different methods agree.")
else:
    print("Not equal: investigate (rules applied in one method only, time zones, or timestamp format).")
