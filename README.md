# Lecture 6: Data pipelines (ETL, ELT, Apache Beam, Google Cloud Dataflow)

Data Handling and Infrastructure for AI | MSc | SETU

This repository holds the code for Lecture 6 and its lab. The example throughout is **Dublin Bikes** open data (Dublin City Council, licence CC BY 4.0, <https://data.smartdublin.ie/dataset/dublinbikes-api>).

| Folder | What it is | Start here |
|---|---|---|
| [`lecture6_demo/`](lecture6_demo/) | The demo shown in the lecture: an Apache Beam pipeline, with tests and scripts. Runs **locally** (free, no cloud account) or on **Google Cloud Dataflow** | [`lecture6_demo/README.md`](lecture6_demo/README.md) |
| [`lab6_student/`](lab6_student/) | **Lab 6**: you extend the pipeline, prove it with tests, run it, and investigate the data | [`lab6_student/README.md`](lab6_student/README.md) (the lab sheet) |

## Get the code (once)

```bash
git clone https://github.com/AbNeil/lecture6_demo
```

**For the lecture demo** (the same commands as the slide "Step 0"):

```bash
cd lecture6_demo/lecture6_demo
chmod +x *.sh
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

**For Lab 6** (the lab sheet in `lab6_student/README.md` has every step in full):

```bash
cd lecture6_demo/lab6_student
chmod +x *.sh
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

The repository and its demo folder share a name, which is why the demo path has `lecture6_demo` twice. `chmod +x *.sh` is needed once because scripts downloaded from GitHub are not always executable.

## Your Google Cloud credits

You have **$50 of credit, shared across the whole course**. The demo and lab are designed to cost cents (batch jobs only, at most 2 small workers, one region, clean-up at the end). Never start a streaming job, never remove the worker limit, and always run `cleanup.sh` when you finish a cloud session.

## The data

The data files are **not stored in this repository**: the scripts download them from Dublin City Council when you run them. Please keep the attribution to Dublin City Council when you use or share results.
