"""Unit tests for the Dublin Bikes pipeline.
Run with:  python -m pytest -q test_pipeline.py"""
import apache_beam as beam
import pytest
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

import bike_pipeline as bp


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


def test_bike_stats_combines_correctly():
    readings = [(1, 4), (1, 8), (1, 0), (2, 5)]
    with TestPipeline() as p:
        out = (p | beam.Create(readings)
                 | beam.CombinePerKey(bp.BikeStats()))
        assert_that(out, equal_to([
            (1, {"samples": 3, "avg_bikes": 4.0,
                 "min_bikes": 0, "max_bikes": 8}),
            (2, {"samples": 1, "avg_bikes": 5.0,
                 "min_bikes": 5, "max_bikes": 5})]))


def test_parse_station_skips_bad_lines():
    line = "42,SMITHFIELD NORTH,Smithfield North,53.34,-6.27"
    assert bp.parse_station(line) == [(42, "SMITHFIELD NORTH")]
    assert bp.parse_station("Number,Name") == []
    assert bp.parse_station("") == []
