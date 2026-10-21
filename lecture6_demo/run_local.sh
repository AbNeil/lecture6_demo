#!/usr/bin/env bash
# Run the pipeline on your own machine / Cloud Shell, on the tiny sample data. No cloud services used.
set -euo pipefail
python make_sample_data.py
rm -rf out
python bike_pipeline.py --runner DirectRunner \
  --input "sample/raw/*.csv" \
  --stations sample/ref/stations.csv \
  --output out/hourly
echo; echo "Output rows:"; wc -l out/*.jsonl
echo; echo "First result rows:"; head -n 3 out/hourly-*.jsonl
echo; echo "Quarantined rows (reason, source):"; python - <<'PY'
import glob, json
for path in glob.glob("out/hourly_bad-*.jsonl"):
    for line in open(path):
        row = json.loads(line); print("  ", row["reason"], "|", row["source"])
PY
