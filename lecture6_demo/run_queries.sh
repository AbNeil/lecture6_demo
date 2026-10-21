#!/usr/bin/env bash
# Step A (ELT): load the raw June file straight into BigQuery, untouched.
# Step B: run the queries in queries.sql (1 to 5) one by one.
set -euo pipefail
source env.sh

echo "== ELT: load the raw June 2024 file into BigQuery as it is"
bq load --source_format=CSV --skip_leading_rows=1 --autodetect --replace \
  "$DATASET.raw_2024_06" "gs://$BUCKET/raw/dublin-bikes_station_status_062024.csv"
bq show --schema --format=prettyjson "$DATASET.raw_2024_06" | head -n 12

python3 - "$PROJECT_ID" <<'PY'
import re, subprocess, sys
project = sys.argv[1]
sql = open("queries.sql").read().replace("YOUR_PROJECT", project)
for block in re.split(r"^-- ## ", sql, flags=re.M)[1:]:
    title, _, query = block.partition("\n")
    print("\n=== " + title.strip())
    subprocess.run(["bq", "query", "--use_legacy_sql=false", "--format=pretty", query], check=False)
PY
