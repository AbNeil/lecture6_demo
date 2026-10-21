#!/usr/bin/env bash
# Run the pipeline LOCALLY on the two real files from get_data_local.sh (no Google Cloud).
# On a laptop this takes a couple of minutes: the full-size files are about a million rows.
set -euo pipefail
[ -d data/raw ] || { echo "No data yet. Run ./get_data_local.sh first."; exit 1; }
rm -rf out/real
SECONDS=0
python bike_pipeline.py --runner DirectRunner \
  --input "data/raw/*.csv" \
  --stations data/ref/dublin.csv \
  --output out/real/hourly
echo; echo "Finished in ${SECONDS} seconds."
echo; echo "Output files:"; wc -l out/real/*.jsonl
echo; echo "First result rows:"; cat out/real/hourly-0*.jsonl | head -n 3
echo; echo "Quarantined rows by reason and file:"
python - <<'PY'
import collections, glob, json
counts = collections.Counter()
for path in glob.glob("out/real/hourly_bad-*.jsonl"):
    for line in open(path):
        row = json.loads(line)
        counts[(row["reason"], row["source"])] += 1
if not counts:
    print("   none")
for (reason, source), n in counts.most_common():
    print(f"   {n:>8}  {reason}  |  {source}")
PY
echo; echo "Next: python reconcile_local.py   (does plain pandas agree with the pipeline?)"
