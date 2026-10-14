# Lecture 6 demo kit: a Dublin Bikes data pipeline (Apache Beam, Google Cloud Dataflow, BigQuery)

This kit is the demo from Lecture 6 and the basis of the lab. You will build and run **one pipeline in two places**:

1. **On your own machine / Cloud Shell**, on a tiny sample, with no cloud services (free, takes minutes).
2. **On Google Cloud Dataflow**, on two real Dublin Bikes files, writing to BigQuery (costs a few cents of your credits).

Data: [Dublin City Council, Dublinbikes API dataset](https://data.smartdublin.ie/dataset/dublinbikes-api), licence CC BY 4.0.

## What the pipeline does

```
CSV files ──► read each file with ITS OWN header ──► check every row (data contract)
                                                        ├── good rows ──► hourly windows ──► average bikes per station ──► add station name ──► BigQuery: station_hourly
                                                        └── bad rows  ──► quarantine with a reason ───────────────────────────────────────► BigQuery: bad_rows
```

## Files

| File | What it is |
|---|---|
| `bike_pipeline.py` | The pipeline (Apache Beam, Python). Read this first. |
| `test_pipeline.py` | Unit tests for the pipeline's building blocks |
| `make_sample_data.py` | Makes a small **synthetic** sample with two schema versions, an empty file and bad rows |
| `run_local.sh` | Runs the pipeline locally on the sample |
| `setup_gcp.sh` | One-time Google Cloud setup (services, permissions, bucket, dataset) |
| `get_data.sh` | Downloads two real files + the station list, shows you the first lines, uploads them |
| `run_dataflow.sh` | Runs the same pipeline on Dataflow (max 2 small workers) |
| `run_queries.sh`, `queries.sql` | ELT load into BigQuery and the SQL queries, including a reconciliation |
| `cleanup.sh` | Stops jobs and deletes everything you created |

## Part A: run it locally

Open Google Cloud Shell (the `>_` button at the top right of the console) or any terminal with Python 3.

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt          # about 1 to 2 minutes
python -m pytest -q test_pipeline.py     # 5 tests should pass
./run_local.sh
```

A couple of `INFO:PrismRunner` lines at the start are normal. You should see these counters at the end of the local run (the sample data is fixed, so the numbers are always the same):

```
Counters:
  bad_rows        4
  empty_files     1
  files_read      3
  good_rows       182
  over_capacity   1
  rows_read       186
  unknown_station 1
```

and `out/hourly-*.jsonl` (31 hourly rows) and `out/hourly_bad-*.jsonl` (4 quarantined rows). The order of rows in the output files can vary from run to run.

## Part B: run it on Google Cloud

Prerequisites: a Google Cloud project that you created, with your **course credits linked as its billing account**.

```bash
./setup_gcp.sh YOUR_PROJECT_ID      # services, permissions, bucket, dataset (about 3 minutes)
./get_data.sh                       # download and upload the data; LOOK at the header lines it prints
./run_dataflow.sh                   # starts the Dataflow job and waits for it to finish
./run_queries.sh                    # ELT load + the 5 SQL queries
./cleanup.sh                        # ALWAYS finish with this
```

**Cost guard:** the job uses at most 2 workers of type `e2-standard-2`, runs as a batch job (it ends by itself) and reads about 120 MB. Expect cents, but check the real figure for your own job in the console (Billing > Reports) and note it down. Never start a streaming job and walk away.

**Always run `./cleanup.sh`** (or delete the whole project: `gcloud projects delete YOUR_PROJECT_ID`) when you finish.

## If something goes wrong

| Symptom | Likely cause and fix |
|---|---|
| `setup_gcp.sh` says billing is not enabled | Link your credits: console > Billing > Link a billing account. Re-run. |
| `Permission denied` / `403` when the job starts or writes | A role is missing on the worker account. Re-run `setup_gcp.sh` (it is safe to re-run). |
| Job fails to start: quota exceeded | Try again in a few minutes, or in the same region with `--machine_type n1-standard-2`. |
| `get_data.sh`: download failed | Open the dataset page, copy the file's link, replace the URL in the script. |
| Job finishes but `station_hourly` is empty | Check `bad_rows`: the reasons tell you what went wrong (for example a different timestamp format). |
| `bad_rows` is huge | Your `--input` glob matched a file that is not a status file. Check `gcloud storage ls gs://$BUCKET/raw/`. |
| `Invalid GCS bucket` / not found | Run `source env.sh` first; check the bucket exists with `gcloud storage ls`. |
| BigQuery: dataset not found in location | Bucket, dataset and Dataflow region must all be the same (`europe-west1`). |

## What was tested, and what was not

Tested on the authors' machine with Apache Beam 2.77.0 on Python 3.12: the 5 unit tests, the full local run on the sample data (counters above), and that the BigQuery-writing version of the pipeline builds. **Not testable without a cloud project and so to be confirmed in your own dry run:** the Dataflow run itself (start-up time, cost, permissions), the real column names and timestamp format of the large files (see the header lines `get_data.sh` prints), and the BigQuery queries. The column lists in the lecture come from the dataset's published data dictionaries.
