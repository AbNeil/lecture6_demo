"""Unit and integration tests for the Dublin Bikes pipeline (Lab 6).

Run with:  python -m pytest -q test_pipeline.py
Your three changes (TODO 1, 2, 3) turn the failing tests green.
"""
import glob
import json

import apache_beam as beam
import pytest
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

import bike_pipeline as bp
import make_sample_data


# ---------------------------------------------------------------- already green
def test_parse_timestamp_accepts_epoch_and_iso():
    assert bp.parse_timestamp("1717228807") == 1717228807.0
    aware = bp.parse_timestamp("2024-06-01 08:00:07+00:00")
    assert aware == bp.parse_timestamp("2024-06-01T08:00:07Z")
    assert aware == bp.parse_timestamp("2024-06-01 08:00:07")  # naive = UTC


def test_parse_timestamp_rejects_garbage():
    with pytest.raises(bp.RowError):
        bp.parse_timestamp("not a time")


GOOD = {"station_id": "42", "num_bikes_available": "5", "capacity": "20",
        "last_reported": "2024-06-01 08:00:07+00:00", "_source": "a.csv"}


def test_validate_sends_good_and_bad_rows_to_the_right_place():
    rows = [GOOD,
            dict(GOOD, num_bikes_available="-1"),
            dict(GOOD, station_id=""),
            dict(GOOD, last_reported="nope"),
            dict(GOOD, num_bikes_available="many")]
    reasons = ["negative_bikes", "missing:station_id", "bad_timestamp",
               "bad_number:num_bikes_available"]
    with TestPipeline() as p:
        out = (p | beam.Create(rows)
                 | beam.ParDo(bp.ValidateRow()).with_outputs(
                     bp.BAD, main="good"))
        count = out.good | beam.combiners.Count.Globally()
        assert_that(count, equal_to([1]), label="one good row")
        why = out[bp.BAD] | beam.Map(lambda row: row["reason"])
        assert_that(why, equal_to(reasons), label="four reasons")


def test_parse_station_skips_bad_lines():
    line = "42,SMITHFIELD NORTH,Smithfield North,53.34,-6.27"
    assert bp.parse_station(line) == [(42, "SMITHFIELD NORTH")]
    assert bp.parse_station("Number,Name") == []
    assert bp.parse_station("") == []


# ---------------------------------------------------------------- TODO 1
def test_todo1_bike_stats_counts_empty_readings():
    """BikeStats must report how many readings had 0 bikes, even after
    the partial results of two workers are merged."""
    fn = bp.BikeStats()
    first = fn.create_accumulator()          # worker 1 sees 4, 8 and 0
    for bikes in (4, 8, 0):
        first = fn.add_input(first, bikes)
    second = fn.create_accumulator()         # worker 2 sees 0 and 5
    for bikes in (0, 5):
        second = fn.add_input(second, bikes)
    out = fn.extract_output(fn.merge_accumulators([first, second]))
    assert out.get("empty_readings") == 2, (
        f"Expected empty_readings == 2, got: {out}. Finish TODO 1 (a to d).")
    assert (out["samples"], out["min_bikes"], out["max_bikes"]) == (5, 0, 8)
    assert out["avg_bikes"] == 3.4


def run_local(tmp_path, *extra):
    """Run the whole pipeline locally on the sample data; return output rows."""
    make_sample_data.main()
    prefix = str(tmp_path / "hourly")
    bp.run(["--runner", "DirectRunner", "--input", "sample/raw/*.csv",
            "--stations", "sample/ref/stations.csv", "--output", prefix,
            *extra])
    files = sorted(glob.glob(prefix + "-0*.jsonl"))
    return [json.loads(line) for f in files for line in open(f)]


def test_todo1_local_run_has_empty_readings(tmp_path):
    """The hourly output must contain an empty_readings column."""
    rows = run_local(tmp_path)
    assert rows and all("empty_readings" in r for r in rows), \
        "Hourly rows have no 'empty_readings' yet: finish TODO 1 (a to d)."
    assert sum(r["empty_readings"] for r in rows) == 8, \
        "The sample data has 8 readings with 0 bikes in total."


# ---------------------------------------------------------------- TODO 2
def test_todo2_window_option_parses():
    """--window_minutes must exist, be an int, and default to 60."""
    base = ["--input", "x", "--stations", "y", "--output", "z"]
    args, _ = bp.parse_args(base)
    assert getattr(args, "window_minutes", None) == 60, \
        "Add --window_minutes (type=int, default=60) in parse_args: TODO 2a."
    args, _ = bp.parse_args(base + ["--window_minutes", "30"])
    assert args.window_minutes == 30


def test_todo2_local_run_uses_window(tmp_path):
    """30-minute windows give twice as many rows as 60-minute windows."""
    rows = run_local(tmp_path, "--window_minutes", "30")
    assert len(rows) == 61, (
        f"Expected 61 rows with 30-minute windows, got {len(rows)}. "
        "Did you replace 3600 with args.window_minutes * 60 (TODO 2b)?")


# ---------------------------------------------------------------- TODO 3
def test_todo3_schema_has_new_column():
    """BigQuery needs the new column in its schema, as an INTEGER."""
    assert "empty_readings:INTEGER" in bp.HOURLY_SCHEMA, \
        "Add ',empty_readings:INTEGER' to HOURLY_SCHEMA: TODO 3."
