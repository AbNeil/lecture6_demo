#!/usr/bin/env bash
# Stop jobs and delete everything this demo created, so nothing keeps costing credits.
# The simplest cleanup of all is to delete the whole project (it can be restored for 30 days):
#     gcloud projects delete "$PROJECT_ID"
set -uo pipefail
source env.sh

echo "== Cancel any Dataflow job that is still running"
for JOB in $(gcloud dataflow jobs list --region="$REGION" --status=active --format="value(JOB_ID)"); do
  gcloud dataflow jobs cancel "$JOB" --region="$REGION"
done
echo "== Delete the bucket and the BigQuery dataset"
gcloud storage rm --recursive "gs://$BUCKET"
bq rm -r -f -d "$PROJECT_ID:$DATASET"
echo "== Check: these should show nothing"
gcloud dataflow jobs list --region="$REGION" --status=active
gcloud storage ls
bq ls
