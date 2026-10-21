# Lab 6: Build, test and run a data pipeline on Google Cloud

**Data Handling and Infrastructure for AI | MSc | 2 hours | no homework**

Last week you learned why pipelines matter and watched one being built. Today **you** will extend that pipeline, prove your changes work with automatic tests, run it on Google Cloud, and use the result to investigate real Dublin Bikes data, including finding days when something looks wrong.

> [!NOTE]
> **You will not be left alone with this.** Every step has the exact command to copy, what you should see, a checkpoint so you know you are on track, and hints when you are stuck. Hints come in three levels: try Hint 1, then 2, then 3. If you are stuck for more than two minutes, ask.

## To do 

| Part | What you do | 
|---|---|
| [0](#part-0-prepare_10) | Create your project, open Cloud Shell, install the kit, see the tests fail
| [1](#part-1-run-it-locally-and-read-the-counters) | Run the pipeline locally and read its counters
| [2](#part-2-make-two-changes-25-min) | Make two changes to the pipeline: **TODO 1** and **TODO 2**
| [3](#part-3-the-third-change-and-the-green-light) | **TODO 3** and a fully green test run 
| [4](#part-4-run-it-on-google-cloud) | Set up Google Cloud, look at the real data, run on Dataflow
| [5](#part-5-ask-questions-of-the-real-data) | Query the result in BigQuery and look for thin days in June 2024 
| [6](#part-6-cost-and-clean-up) | Check what it cost and clean up

## Your credits: the cost promise

You each have **$50 of Google Cloud credit, shared across the whole course** (not just this lab). This lab is designed to use **only a few cents**, because:

- the Dataflow job is a **batch** job, so it ends by itself;
- the script allows **at most 2 small workers** (`--max_num_workers 2`, `--machine_type e2-standard-2`);
- the input is just two monthly files;
- everything is in **one region** (`europe-west1`);
- you will **clean up at the end**.

Do not remove the worker limit, do not start any streaming job, and do not skip Part 6. The tests in Parts 2 and 3 are also a cost guard: you will only run on Dataflow once your code passes them, so you never pay to run a bug.

## How to read this sheet

| You see | It means |
|---|---|
| A grey code box | Copy it into Cloud Shell and press Enter |
| **You should see** | Roughly what the output looks like. Small differences are fine |
| **Checkpoint** | Stop. If yours matches, carry on. If not, use "If it fails" |
| **Hint 1, 2, 3** | Click to open. Try the earlier hints before the later ones |
| **Think about it** | A short question. Jot your answer in a notes file or on paper. Nothing is handed in |

---

## Part 0: Prepare 

### 0.1 Create a Google Cloud project

1. Open the [Google Cloud console](https://console.cloud.google.com) and sign in.
2. Click the **project picker** at the top of the page, then **New project**.
3. Give it a name, for example `bikes-lab`. Google will also suggest a **project ID** (usually the name plus some digits). **Copy the project ID.** You will need it several times. In this sheet it is written `YOUR_PROJECT_ID`.
4. Click **Create** and make sure the new project is selected in the picker.

> The **name** and the **ID** are different things. Commands use the **ID**.

### 0.2 Link your credits

A new project cannot run Dataflow until it is linked to a **billing account**, and for you that account is the one that holds your course credits.

1. Open the navigation menu (the three lines at the top left) and find **Billing**.
2. Choose the option to **link a billing account** to your project and select the account with your course credits.

If you cannot find it, ask now. Nothing later works without this step, and the setup script will tell you if billing is missing.

### 0.3 Open Cloud Shell

Cloud Shell is a free terminal in your browser with the `gcloud` command already installed.

1. Click the **`>_` button** at the top right of the console. A terminal opens at the bottom of the page. (If it asks you to authorise, click **Authorize**.)
2. Tell it which project to use:

```bash
gcloud config set project YOUR_PROJECT_ID
```

3. Click **Open Editor** (top right of the Cloud Shell panel). This gives you a file editor in the same window, where you will edit `bike_pipeline.py` later. You can switch back with **Open Terminal**.

### 0.4 Get the lab kit

Your lecturer will tell you which option to use.

**Option A: from GitHub**

```bash
git clone https://github.com/AbNeil/lecture6_demo
cd lecture6_demo/lab6_student
chmod +x *.sh      # files downloaded from GitHub are not executable by default
```

**Option B: from a zip file.** In the Cloud Shell menu (three dots) choose **Upload** and select `lab6_student.zip`. Then:

```bash
unzip lab6_student.zip
cd lab6_student
chmod +x *.sh
```

**You should see** a list of files when you type `ls`, including `bike_pipeline.py`, `test_pipeline.py`, `run_local.sh` and `setup_gcp.sh`.

### 0.5 Create a Python environment and install the libraries

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

This takes one to two minutes. When it works, your prompt starts with `(.venv)`.

> If you ever open a **new** terminal tab or Cloud Shell restarts, repeat `source .venv/bin/activate` from inside the `lab6_student` folder.

### 0.6 See the tests fail (on purpose)

The kit contains tests that check your three changes. Run them now, before you change anything:

```bash
python -m pytest -q test_pipeline.py
```

**You should see**, at the end of a longer message:

```
5 failed, 4 passed
```

**This is expected.** Four tests check parts that already work. The five failed tests are your to-do list: they turn **green** as you complete TODO 1, 2 and 3. Their names tell you which TODO they belong to, for example `test_todo1_...`.

**Checkpoint:** you see `5 failed, 4 passed`.

<details><summary>If it fails</summary>

| What you see | What to do |
|---|---|
| `No module named apache_beam` | Your environment is not active. Run `source .venv/bin/activate`, then the `pip install` line again |
| `No such file` | You are in the wrong folder. `cd` into `lab6_student` |
| `command not found: python` | Use `python3` instead of `python` for the first commands, or activate the environment |
| It takes more than a minute to start | The first run is slower. Wait |
</details>

---

## Part 1: Run it locally and read the counters

Before changing anything, run the pipeline as it is, **on your own machine, with no cloud services** (this is free). It uses a small, made-up sample with a few deliberately bad rows.

```bash
python make_sample_data.py
wc -l sample/raw/*.csv
```

`wc -l` counts the lines in each file. **You should see**:

```
   95 sample/raw/dublin-bikes_station_status_sample_2024.csv
   93 sample/raw/dublin-bikes_station_status_sample_2026.csv
    1 sample/raw/dublin-bikes_station_status_sample_empty.csv
  189 total
```

Now run the pipeline:

```bash
./run_local.sh
```

**You should see** (a couple of `INFO` lines at the start are normal):

```
Counters:
  bad_rows        4
  empty_files     1
  files_read      3
  good_rows       182
  over_capacity   1
  rows_read       186
  unknown_station 1

Output rows:
  31 out/hourly-00000-of-00001.jsonl
   4 out/hourly_bad-00000-of-00001.jsonl
```

and, further down, the **reasons** for the four quarantined rows.

> **Think about it (1):** the three files have **189 lines** in total, but `rows_read` is **186**. Where did the missing 3 go? *(Hint: what does every CSV file start with?)*
>
> **Think about it (2):** `rows_read` is 186, `good_rows` is 182, `bad_rows` is 4. What does that tell you about whether any row was lost? Why is that reassuring?
>
> **Think about it (3):** look at the first lines of `out/hourly-*.jsonl` with `head -n 3 out/hourly-0*.jsonl`. Each line is one station for one hour. Which numbers does it report?

<details><summary>Hint for (1)</summary>

Each file begins with a header line holding the column names. That line is not a data row. Three files means three header lines.
</details>

**Checkpoint:** your counters match the ones above.

<details><summary>If it fails</summary>

| What you see | What to do |
|---|---|
| Counters differ, for example `bad_rows 9` | Did you edit `bike_pipeline.py` already? Undo your changes, or re-download the kit |
| `Permission denied: ./run_local.sh` | Run `chmod +x *.sh` once |
| No `Counters:` section | Scroll up: there may be an error message from Python. Read its **last line** |
</details>

---

## Part 2: Make two changes 

Now open **`bike_pipeline.py`** in the editor. Search for `TODO`: you will find comments marking exactly where to work. **Nothing else needs changing.**

### How the tests help you

After each change, run the tests again:

```bash
python -m pytest -q test_pipeline.py
```

and watch the number of failed tests go down. Each failed test prints a message that tells you what is missing.

### TODO 1: count how often a station is empty (a, b, c, d)

**Why?** The hourly table tells you the average, minimum and maximum bikes. But a city planner wants to know something more direct: **how often is a station completely empty?** So we add a fifth number to the summary: `empty_readings`, the number of readings with **0 bikes**.

**Where?** The class `BikeStats`. Remember from the lecture: a `CombineFn` builds a summary in four steps, so that many workers can share the job:

| Step | What it does | Your change |
|---|---|---|
| `create_accumulator` | Starts an empty summary | **TODO 1a:** add a fifth number, starting at 0 |
| `add_input` | Adds one reading to the summary | **TODO 1b:** add 1 to the new number when `bikes == 0` |
| `merge_accumulators` | Joins partial summaries from different workers | **TODO 1c:** add the new numbers together |
| `extract_output` | Produces the final answer | **TODO 1d:** report it as `"empty_readings"` |

The summary is a **tuple**, currently `(n, total, low, high)`. It becomes `(n, total, low, high, empty)`.

<details><summary>Hint 1: the idea</summary>

Everywhere the code unpacks or builds the summary tuple, it must now have **five** items instead of four. The new fifth item counts the readings where `bikes` is `0`.
</details>

<details><summary>Hint 2: what to change in each step</summary>

- **1a:** the starting tuple gets one more `0` at the end.
- **1b:** unpack five names (`n, total, low, high, empty = acc`). Return five items. The fifth is `empty` plus 1 if `bikes == 0`, otherwise plus 0.
- **1c:** in `merge_accumulators`, start `empty` at 0. In the loop, unpack five items (`a_empty` is the new one) and add `a_empty` to `empty`. Return five items.
- **1d:** unpack five names, and add `"empty_readings": empty` to the dictionary that is returned.
</details>

<details><summary>Hint 3: the code (try Hints 1 and 2 first!)</summary>

```python
    def create_accumulator(self):
        return (0, 0, None, None, 0)  # n, total, lowest, highest, empty

    def add_input(self, acc, bikes):
        n, total, low, high, empty = acc
        return (n + 1, total + bikes,
                bikes if low is None else min(low, bikes),
                bikes if high is None else max(high, bikes),
                empty + (1 if bikes == 0 else 0))

    def merge_accumulators(self, accs):
        n, total, low, high, empty = 0, 0, None, None, 0
        for a_n, a_total, a_low, a_high, a_empty in accs:
            n, total, empty = n + a_n, total + a_total, empty + a_empty
            if a_low is not None:
                low = a_low if low is None else min(low, a_low)
                high = a_high if high is None else max(high, a_high)
        return (n, total, low, high, empty)

    def extract_output(self, acc):
        n, total, low, high, empty = acc
        return {"samples": n, "min_bikes": low, "max_bikes": high,
                "avg_bikes": round(total / n, 2) if n else None,
                "empty_readings": empty}
```
</details>

Run the tests. **You should see** `test_todo1_...` turn green.

<details><summary>If it fails</summary>

| What you see | What it means |
|---|---|
| `ValueError: not enough values to unpack (expected 5, got 4)` | You changed one step but not another. Every place that builds or unpacks the tuple must use **five** items: check 1a, 1b, 1c and 1d |
| `Expected empty_readings == 2, got: {...}` | The tuple is right but the number is not reported or not counted: check 1b and 1d |
| `IndentationError` or `SyntaxError` | A typo. Look at the line number in the message, and the line just above |
| `empty_readings == 1` instead of 2 | In `merge_accumulators`, you did not add the partial counts together (1c) |
</details>

**Checkpoint:** both `test_todo1_...` tests pass.

> **Think about it (4):** why does the merge step add the five numbers together, instead of, say, averaging them? What would go wrong if two workers each counted 3 empty readings and we reported 3?

### TODO 2: choose the window length (a and b)

**Why?** Right now the window is fixed at one hour (`3600` seconds) **inside the code**. Changing it means editing code. A good pipeline takes this as an **option**, so you can ask for 30-minute or 15-minute windows without touching the code.

**Where?** Two places in `bike_pipeline.py`:

- **TODO 2a**, in `parse_args`: add an option called `--window_minutes` (an integer, default 60).
- **TODO 2b**, in `build`: use it in place of `3600`. A window of N minutes is N times 60 seconds.

<details><summary>Hint 1: the idea</summary>

2a is the same as the lines above it, which add other options with `parser.add_argument(...)`. 2b: the number inside `window.FixedWindows( )` is a number of **seconds**.
</details>

<details><summary>Hint 2: the shape</summary>

For 2a you need `"--window_minutes"`, `type=int`, `default=60` and a short help text. For 2b you need `args.window_minutes * 60` in place of `3600`.
</details>

<details><summary>Hint 3: the code</summary>

2a:

```python
    parser.add_argument("--window_minutes", type=int, default=60,
                        help="length of each time window in minutes (default 60)")
```

2b:

```python
        | "Hourly" >> beam.WindowInto(
            window.FixedWindows(args.window_minutes * 60))
```
</details>

Run the tests again. **You should see** `test_todo2_...` turn green.

<details><summary>If it fails</summary>

| What you see | What it means |
|---|---|
| `Add --window_minutes (type=int, default=60) in parse_args: TODO 2a` | The option is missing or has a different name. Check the spelling, including the two dashes and the underscore |
| `Expected 61 rows with 30-minute windows, got 31` | The option exists but `build()` still uses `3600`: do TODO 2b |
| `'Namespace' object has no attribute 'window_minutes'` | `build()` uses the option but `parse_args` does not define it: do 2a |
</details>

**Checkpoint:** both `test_todo2_...` tests pass.

> **Think about it (5):** with 30-minute windows the output has about **twice** as many rows (61 instead of 31). Why? And what would you expect with 15-minute windows?

---

## Part 3: The third change and the green light 

### TODO 3: tell BigQuery about the new column

**Why?** When the pipeline writes to BigQuery it uses a **schema**: the list of columns and their types. The pipeline now produces a new field, `empty_readings`. If the schema does not list it, BigQuery will not accept the data (the load would fail because the table has no such column). **Code and schema must always agree.**

**Where?** Search for `HOURLY_SCHEMA`. Add the new column at the end of the string, as an `INTEGER`.

<details><summary>Hint 1</summary>

Look at how the other columns are written: `name:TYPE`, separated by commas. Mind the quotes: the schema is split over three lines but is one string.
</details>

<details><summary>Hint 2: the code</summary>

```python
HOURLY_SCHEMA = ("station_id:INTEGER,station_name:STRING,hour_start:TIMESTAMP,"
                 "samples:INTEGER,avg_bikes:FLOAT,min_bikes:INTEGER,"
                 "max_bikes:INTEGER,empty_readings:INTEGER")
```
</details>

Now the full test run:

```bash
python -m pytest -q test_pipeline.py
```

**You should see:**

```
9 passed
```

### Try your new option locally

```bash
rm -rf out
python bike_pipeline.py --runner DirectRunner \
  --input "sample/raw/*.csv" --stations sample/ref/stations.csv \
  --output out/half --window_minutes 30
wc -l out/half-0*.jsonl
head -n 2 out/half-0*.jsonl
```

**You should see** `61` lines, and each line now ends with `"empty_readings": ...`. Also note that the column is still called `hour_start`, even for 30-minute windows. Here it means "start of the window". (Renaming a column is a bigger decision than it looks, because other people's queries may depend on it.)

> **Do not go on to Part 4 until you see `9 passed`.** That is your gate: the cloud costs credits, so we only run code that has passed its tests.

**Checkpoint:** `9 passed`, and 61 rows with `--window_minutes 30`.

---

## Part 4: Run it on Google Cloud 

Now the same pipeline you just tested runs on **Dataflow**, reading real files from Cloud Storage and writing to BigQuery. You run four scripts, in order.

### 4.1 Set up your project 

```bash
./setup_gcp.sh YOUR_PROJECT_ID
```

This script does five things: checks your credits are linked, switches on the services you need, gives the Dataflow worker account the permissions it needs, and creates **one bucket and one BigQuery dataset in the same region**. It saves your settings in a file called `env.sh`.

**You should see** headings like `== 1. Is billing ...`, `== 2. Turn on the services`, a line `granted roles/...` for each of four roles, and finally `== Done. Settings saved to env.sh`.

<details><summary>If it fails</summary>

| What you see | What to do |
|---|---|
| `Billing is NOT enabled` | Go back to step 0.2 and link your credits, then run the script again |
| A permission or API error | Wait a minute and run it again. The script is **safe to repeat** |
| `Usage: ./setup_gcp.sh YOUR_PROJECT_ID` | You forgot to type your project ID after the script name |
</details>

### 4.2 Download the real data, and look at it first

```bash
./get_data.sh
```

This downloads two real monthly files (June 2024 and February 2026) and the list of stations, **prints the first lines of each**, and uploads them to your bucket.

**Look carefully at the output.** The rule of data engineering is: *look at the data before you run anything on it.*

> **Think about it (6):** the dataset's published data dictionary lists **15** columns for the June 2024 file but only **9** for February 2026. Compare the header line (the first line) of each file. What do you actually see?
>
> **Think about it (7):** what does the timestamp in the `last_reported` column look like? Does it include a time zone?
>
> **Think about it (8):** what does this tell you about trusting a description of a dataset? And why is it still good design that our pipeline reads each file's own header and needs only three columns?

<details><summary>If it fails</summary>

| What you see | What to do |
|---|---|
| `curl: (22)` or a download error | The link may have changed. Open the dataset page (`data.smartdublin.ie/dataset/dublinbikes-api`), copy the link of the file, and put it in the URL variable at the top of `get_data.sh` |
| `No such file env.sh` | Run `./setup_gcp.sh YOUR_PROJECT_ID` first |
</details>

### 4.3 Run the pipeline on Dataflow

First, the safety check. Run the tests once more, to be sure you are about to run **your tested code**:

```bash
python -m pytest -q test_pipeline.py
```

You should see `9 passed`. Then start the job:

```bash
./run_dataflow.sh
```

This starts a Dataflow job from your code and **waits until it finishes**. Starting up takes a few minutes before any work happens; this is normal. **Do not press Ctrl+C and start again**: a second job means paying twice.

Notice what the script contains: `--runner DataflowRunner` (run in the cloud), `--max_num_workers 2` and `--machine_type e2-standard-2` (your cost guards), `--input gs://.../raw/*.csv` and `--bq_dataset bikes` (read from the bucket, write to BigQuery). It is the **same pipeline file** as your local run. Only the options changed.

### While you wait: watch the job

While the script waits, open the Cloud console in your browser, find **Dataflow** and then **Jobs**, and click your job. Its name starts with `bikes-` followed by the date and time.

> **Think about it (9):** the **job graph** shows boxes with names. Can you find `ReadRows`, `Validate`, `Stats` and `WriteHourly`? Where have you seen these names before? *(Hint: in the code.)*

Start your cost log. Fill in as much as you can **right now** (you will complete it in Part 6):

| | Your value |
|---|---|
| Job started at | |
| Job finished at | |
| Final state (succeeded or failed) | |
| Most workers you saw | |
| Resource use shown on the job page (for example, vCPU time) | |
| Cost shown in Billing (later) | |

<details><summary>If the job fails</summary>

Click the job, open its **Logs**, and read the **first** error message (not the last). Usual causes:

| What you see | What to do |
|---|---|
| `403` or `permission denied` | Run `./setup_gcp.sh YOUR_PROJECT_ID` again, then `./run_dataflow.sh` |
| `quota` or `resource` errors | Wait a few minutes and try again, or ask |
| `Invalid GCS bucket` or file not found | Did `get_data.sh` finish? Check with `gcloud storage ls gs://YOUR_PROJECT_ID-bikes/raw/` |
| A Python error in your code | It should not happen if you had `9 passed`: ask for help |
</details>

**Checkpoint:** your job finishes in the state **succeeded**.

---

## Part 5: Ask questions of the real data 

### 5.1 The ELT path and the reconciliation

```bash
./run_queries.sh
```

This does two things. First, it loads the **raw June file straight into BigQuery**, untouched (the **ELT** way). Then it runs five SQL queries. Read the results as they appear. The last query (number 5) is a **reconciliation**: it computes the same hourly numbers **with plain SQL on the raw table** and compares them with **your Beam pipeline's output**.

> **Think about it (10):** write down the three numbers from query 5: `elt_rows`, `etl_rows` and `matching_rows`. If all three are equal, what have you shown? If `matching_rows` were smaller, what might explain it?

<details><summary>Hint</summary>

If two completely different methods (SQL and a Beam pipeline) give identical numbers, that is strong evidence both are right. If they differ, the cause is often a time-zone difference, or a rule applied in one method but not the other.
</details>

<details><summary>If it fails</summary>

| What you see | What to do |
|---|---|
| `Not found: Table ... station_hourly` | The Dataflow job did not finish or failed. Check the job state in the console |
| Query 5 returns `0` for `etl_rows` | The pipeline output has no June hours: query `bad_rows` and look at the reasons |
| `No such file env.sh` | Run `./setup_gcp.sh YOUR_PROJECT_ID` first |
</details>

### 5.2 Three investigations

Open `lab_queries.sql` in the editor. It has three queries. Two have **blanks** (`___`). Run each one with `./run_lab_sql.sh N`.

#### Investigation 1: count the rows, then look for thin days

The web page for the dataset publishes a "total record count" for each file. For June 2024 it says **315,747**. Do not just believe it: **count the rows yourself**. In Cloud Shell, `wc -l` counts the lines in a file, and one of them is the header:

```bash
wc -l data/dublin-bikes_station_status_062024.csv
```

Now run query 1, which counts the rows **per day** in the raw June file. It needs no changes:

```bash
./run_lab_sql.sh 1
```

> **Think about it (11):** (a) Does your own count agree with the page? (b) Look at the number of rows per day. What is a normal day, and do any days stand out? Write **one sentence** with your hypothesis about what you see, and the question you would ask Dublin City Council to find out whether you are right.

<details><summary>Hint</summary>

For (a): remember to subtract the header line from the `wc -l` figure. For (b): scan down the numbers for a typical level, then look for days that are clearly lower. Also notice whether the weekend days look different from weekdays. The data tells you **when** something changes, but it cannot tell you **why**: that is a question for the data owner.
</details>

#### Investigation 2: which stations are most often empty?

This uses **the column you added**. Fill in blanks 1 and 2 in `lab_queries.sql` (they are in query 2), save, and run:

```bash
./run_lab_sql.sh 2
```

<details><summary>Hint</summary>

Both blanks are the name of the column you added in TODO 1. The query adds it up per station and divides by the total number of readings.

If a row is labelled `station 35` (or 1, 14, 20, 60 or 70), that station's ID is not in the station list the pipeline joined, so it has no name. The pipeline kept those rows on purpose and counted them, rather than dropping them.
</details>

> **Think about it (12):** which station is empty most often? Does that surprise you? What might explain why one station is empty more often than another?

#### Investigation 3: when are stations empty?

Fill in blanks 3 and 4 in query 3, then run it:

```bash
./run_lab_sql.sh 3
```

<details><summary>Hint for blank 3</summary>

BigQuery wants a time zone name written like `Continent/City`. What would you write for Dublin?
</details>

> **Think about it (13):** at what hours of the day are stations empty most often? Does it match what you would expect about how people use city bikes (for example, the commute)?

**Checkpoint:** you have run all three lab queries and have a sentence for each question.

---

## Part 6: Cost and clean-up 

### 6.1 Complete your cost log

Back in the Cloud console:

1. Open your job page and note the **resource use** it shows (for example total vCPU time), and the start and end times.
2. Open **Billing**, then **Reports**, and look at your project's costs.

> Billing figures can take some hours to appear. If the cost for your job is not visible yet, write "not visible yet". That is normal, and you do not need to wait.

> **Think about it (14):** was your job's cost closer to cents or to dollars? Why is a small, capped batch job so cheap compared with leaving a streaming job running for a day?

### 6.2 Clean up: do this before you leave

```bash
./cleanup.sh
```

It cancels any Dataflow job that is still running, then deletes the bucket and the BigQuery dataset. At the end it lists active jobs, buckets and datasets. **They should show nothing.**

**Checkpoint:** the final lists are empty.

> **The simplest cleanup of all** is to delete the whole project. It removes everything at once and can be undone for 30 days:
>
> ```bash
> gcloud projects delete YOUR_PROJECT_ID
> ```

---

## When done 

check:

- [ ] `9 passed` in your last test run
- [ ] Your Dataflow job showed **succeeded**
- [ ] You ran `./cleanup.sh` and the lists were empty
- [ ] You noted what your job cost (or "not visible yet")

**There is nothing to hand in and no homework.** If you want to keep your changes, copy `bike_pipeline.py` somewhere safe before Cloud Shell is reset.

**One thing to remember:** a pipeline is not finished when it runs. It is finished when it is **tested** (the traffic-light tests), **checked** (the reconciliation), **watched** (the counters and logs) and **cleaned up** (nothing left running).

---

## Quick reference

| File | What it is |
|---|---|
| `bike_pipeline.py` | The pipeline. You edit this (TODO 1, 2, 3) |
| `test_pipeline.py` | The tests (your traffic lights). Do not edit |
| `make_sample_data.py` | Makes the small made-up sample for local runs |
| `run_local.sh` | Runs the pipeline locally on the sample |
| `setup_gcp.sh` | One-time Google Cloud setup |
| `get_data.sh` | Downloads real data, shows it, uploads it |
| `run_dataflow.sh` | Runs your pipeline on Dataflow (capped at 2 small workers) |
| `run_queries.sh`, `queries.sql` | ELT load and five SQL queries |
| `lab_queries.sql`, `run_lab_sql.sh` | The three investigations |
| `cleanup.sh` | Stops jobs and deletes everything you created |

| Word | Meaning |
|---|---|
| Pipeline | The steps that take raw data to a trustworthy result, automatically |
| Beam / Dataflow | Beam is the programming model; Dataflow is Google's service that runs Beam pipelines |
| DirectRunner / DataflowRunner | Run on your machine / run on Google Cloud |
| Window | A slice of time (here, one hour) used to group readings |
| Quarantine (`bad_rows`) | Where rows that break the contract are kept, with the reason |
| Idempotent | Safe to run again: running twice gives the same result |
| ETL / ELT | Transform before loading / load first, then transform in the warehouse |
| Reconciliation | Checking that two different methods give the same answer |
| Schema | The list of columns and their types |
