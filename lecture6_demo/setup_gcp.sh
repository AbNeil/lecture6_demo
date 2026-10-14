#!/usr/bin/env bash
# One-time setup for the Dublin Bikes pipeline demo. Run it in Google Cloud Shell.
#
#   ./setup_gcp.sh YOUR_PROJECT_ID
#
# What it does: checks billing, turns on the services we need, lets the Dataflow worker
# account do its job, and creates one bucket and one BigQuery dataset in ONE region.
set -euo pipefail

PROJECT_ID="${1:?Usage: ./setup_gcp.sh YOUR_PROJECT_ID}"
REGION="europe-west1"                 # Belgium. Bucket, dataset and Dataflow all use this region.
BUCKET="${PROJECT_ID}-bikes"
DATASET="bikes"

gcloud config set project "$PROJECT_ID"

echo "== 1. Is billing (your course credits) linked to this project?"
BILLING=$(gcloud billing projects describe "$PROJECT_ID" --format="value(billingEnabled)")
if [ "$BILLING" != "True" ]; then
  echo "Billing is NOT enabled. In the console: Billing > Link a billing account, choose your credits account, then re-run."
  exit 1
fi

echo "== 2. Turn on the services (takes about a minute)"
gcloud services enable dataflow.googleapis.com compute.googleapis.com storage.googleapis.com \
  bigquery.googleapis.com logging.googleapis.com cloudresourcemanager.googleapis.com

echo "== 3. Give the Dataflow worker account the roles it needs"
PROJECT_NUMBER=$(gcloud projects describe "$PROJECT_ID" --format="value(projectNumber)")
WORKER_SA="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"
until gcloud iam service-accounts describe "$WORKER_SA" >/dev/null 2>&1; do
  echo "   waiting for the default worker account to appear..."; sleep 5
done
for ROLE in roles/dataflow.worker roles/storage.objectAdmin roles/bigquery.dataEditor roles/bigquery.jobUser; do
  gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:${WORKER_SA}" --role="$ROLE" --condition=None --quiet >/dev/null
  echo "   granted $ROLE"
done

echo "== 4. Create the bucket and the BigQuery dataset (same region)"
gcloud storage buckets describe "gs://$BUCKET" >/dev/null 2>&1 || \
  gcloud storage buckets create "gs://$BUCKET" --location="$REGION" --uniform-bucket-level-access
bq --location="$REGION" show "$PROJECT_ID:$DATASET" >/dev/null 2>&1 || \
  bq --location="$REGION" mk --dataset "$PROJECT_ID:$DATASET"

cat > env.sh <<ENV
export PROJECT_ID="$PROJECT_ID"
export REGION="$REGION"
export BUCKET="$BUCKET"
export DATASET="$DATASET"
ENV
echo "== Done. Settings saved to env.sh. Next: ./get_data.sh"
