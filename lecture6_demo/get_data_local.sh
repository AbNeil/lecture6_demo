#!/usr/bin/env bash
# LOCAL-ONLY version of get_data.sh: download two real Dublin Bikes files and the station list
# onto THIS machine, and show the first lines. No Google Cloud is used.
# Source: Dublin City Council, CC BY 4.0, https://data.smartdublin.ie/dataset/dublinbikes-api
# If a download fails, open that page, copy the link of the file, and set the variable, e.g.
#     JUN2024_URL="https://..." ./get_data_local.sh
set -euo pipefail

BASE="https://data.smartdublin.ie/dataset/33ec9fe2-4957-4e9a-ab55-c5e917c7a9ab/resource"
JUN2024_URL="${JUN2024_URL:-$BASE/022119a2-7679-4742-9b0a-c39dce0f38c1/download/dublin-bikes_station_status_062024.csv}"
FEB2026_URL="${FEB2026_URL:-$BASE/91098ee8-7ea7-4c70-b96d-face618a02b6/download/dublin-bikes_station_status_022026.csv}"
STATIONS_URL="${STATIONS_URL:-$BASE/2dec86ed-76ed-47a3-ae28-646db5c5b965/download/dublin.csv}"

mkdir -p data/raw data/ref
curl -fL --retry 3 -o data/raw/dublin-bikes_station_status_062024.csv "$JUN2024_URL"
curl -fL --retry 3 -o data/raw/dublin-bikes_station_status_022026.csv "$FEB2026_URL"
curl -fL --retry 3 -o data/ref/dublin.csv "$STATIONS_URL"

echo; echo "== File sizes"; ls -lh data/raw data/ref
echo; echo "== LOOK AT THE DATA FIRST: the header and two rows of each file"
for f in data/raw/*.csv data/ref/dublin.csv; do
  echo "--- $f"; head -n 3 "$f"
done
echo; echo "== Rows (including the header line)"; wc -l data/raw/*.csv data/ref/dublin.csv
echo; echo "Next: ./run_local_real.sh"
