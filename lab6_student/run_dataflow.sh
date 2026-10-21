#!/usr/bin/env bash
# Run the SAME pipeline on Google Cloud Dataflow: files in Cloud Storage in, BigQuery tables out.
# Cost guard: at most 2 small workers (--max_num_workers 2 --machine_type e2-standard-2).
set -euo pipefail
source env.sh

python bike_pipeline.py \
  --runner DataflowRunner \
  --project "$PROJECT_ID" \
  --region "$REGION" \
  --temp_location "gs://$BUCKET/tmp" \
  --staging_location "gs://$BUCKET/staging" \
  --job_name "bikes-$(date +%Y%m%d-%H%M%S)" \
  --max_num_workers 2 \
  --machine_type e2-standard-2 \
  --input "gs://$BUCKET/raw/*.csv" \
  --stations "gs://$BUCKET/ref/dublin.csv" \
  --bq_dataset "$DATASET"
