"""How many rows arrive on each day? (the "June 2024 mystery", locally, no cloud)

Usage:   python rows_per_day.py [file.csv]
Default: data/raw/dublin-bikes_station_status_062024.csv
Prints rows per day with a simple bar, and flags days well below the BUSIEST days
(the 90th-percentile day). Comparing with the median would hide a problem that
affects more than half of the days.
"""
import sys

import pandas as pd

path = sys.argv[1] if len(sys.argv) > 1 else "data/raw/dublin-bikes_station_status_062024.csv"
raw = pd.read_csv(path, usecols=["last_reported"], dtype=str)["last_reported"]

numeric = pd.to_numeric(raw, errors="coerce")
if numeric.notna().all():
    when = pd.to_datetime(numeric, unit="s", utc=True)
else:
    try:
        when = pd.to_datetime(raw, errors="coerce", utc=True, format="ISO8601")
    except (TypeError, ValueError):  # older pandas
        when = pd.to_datetime(raw, errors="coerce", utc=True)

bad = int(when.isna().sum())
per_day = when.dropna().dt.date.value_counts().sort_index()
reference = per_day.quantile(0.90)   # a busy, normal day (not the median: see above)
widest = per_day.max()

print(f"File: {path}")
print(f"Rows: {len(raw)}   (unreadable timestamps: {bad})")
print(f"Reference day (90th percentile): {reference:.0f} rows   (median day: {per_day.median():.0f})\n")
print(f"{'day':<12}{'rows':>8}   ")
for day, n in per_day.items():
    flag = "  <-- below 75% of the reference day" if n < 0.75 * reference else ""
    print(f"{str(day):<12}{n:>8}   {'#' * int(40 * n / widest)}{flag}")
low = per_day[per_day < 0.75 * reference]
print(f"\nDays below 75% of the reference day: {len(low)} of {len(per_day)}")
if len(low):
    print("First:", low.index[0], "  Last:", low.index[-1])
    first_ok = per_day[per_day >= 0.75 * reference]
    if len(first_ok):
        print("First day at a normal level:", first_ok.index[0])
