#!/usr/bin/env bash
# Download two real Dublin Bikes files and the station list, look at them, upload them to Cloud Storage.
# Source: Dublin City Council, CC BY 4.0, https://data.smartdublin.ie/dataset/dublinbikes-api
# If a download fails, open that page, copy the link of the file you want, and replace the URL below.
set -euo pipefail
source env.sh

BASE="https://data.smartdublin.ie/dataset/33ec9fe2-4957-4e9a-ab55-c5e917c7a9ab/resource"
JUN2024="$BASE/022119a2-7679-4742-9b0a-c39dce0f38c1/download/dublin-bikes_station_status_062024.csv"
FEB2026="$BASE/91098ee8-7ea7-4c70-b96d-face618a02b6/download/dublin-bikes_station_status_022026.csv"
STATIONS="$BASE/2dec86ed-76ed-47a3-ae28-646db5c5b965/download/dublin.csv"

mkdir -p data
curl -fL --retry 3 -o data/dublin-bikes_station_status_062024.csv "$JUN2024"
curl -fL --retry 3 -o data/dublin-bikes_station_status_022026.csv "$FEB2026"
curl -fL --retry 3 -o data/dublin.csv "$STATIONS"

echo; echo "== File sizes"; ls -lh data
echo; echo "== LOOK AT THE DATA FIRST: the header and two rows of each file"
for f in data/dublin-bikes_station_status_*.csv data/dublin.csv; do
  echo "--- $f"; head -n 3 "$f"
done
echo; echo "== Rows (including the header line)"; wc -l data/*.csv

gcloud storage cp data/dublin-bikes_station_status_*.csv "gs://$BUCKET/raw/"
gcloud storage cp data/dublin.csv "gs://$BUCKET/ref/"
echo; gcloud storage ls -l "gs://$BUCKET/raw/" "gs://$BUCKET/ref/"
