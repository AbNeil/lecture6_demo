"""Dublin Bikes hourly pipeline (Apache Beam).

Reads Dublin Bikes station-status CSV files (any schema version), checks every
row, sends bad rows to a quarantine output, averages the bikes available per
station per hour, adds station names, and writes the results locally or to
BigQuery.

Run locally (no cloud account needed):
    python bike_pipeline.py --runner DirectRunner --input "sample/raw/*.csv" \
        --stations sample/ref/stations.csv --output out/hourly
Run on Google Cloud Dataflow: see run_dataflow.sh
"""
import argparse
import csv
import io
import json
import logging
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.metrics import Metrics
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms import window

REQUIRED = ("station_id", "last_reported", "num_bikes_available")
BAD = "bad"  # name of the quarantine (dead-letter) output


class ReadCsvFile(beam.DoFn):
    """One element in = one file. We read the header of THAT file, so files
    with different columns (schema drift) are all handled correctly."""

    def __init__(self):
        self.files = Metrics.counter("bikes", "files_read")
        self.empty = Metrics.counter("bikes", "empty_files")
        self.rows = Metrics.counter("bikes", "rows_read")

    def process(self, readable_file):
        name = readable_file.metadata.path.rsplit("/", 1)[-1]
        self.files.inc()
        count = 0
        with readable_file.open() as raw:
            text = io.TextIOWrapper(raw, encoding="utf-8-sig", newline="")
            for row in csv.DictReader(text):
                count += 1
                row["_source"] = name
                yield row
        self.rows.inc(count)
        if count == 0:
            self.empty.inc()


class RowError(Exception):
    """Raised when a row breaks the data contract; the message is the reason."""


def parse_timestamp(value):
    """Return seconds since 1970 (UTC). Accepts epoch seconds or ISO text."""
    text = (value or "").strip()
    try:
        return float(text)
    except ValueError:
        pass
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        raise RowError("bad_timestamp")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


def to_int(row, column):
    try:
        return int(float(row[column]))
    except (ValueError, TypeError):
        raise RowError("bad_number:" + column)


class ValidateRow(beam.DoFn):
    """The data contract from Lecture 5, as code. Good rows go to the main
    output, bad rows go to the BAD output together with the reason."""

    def __init__(self):
        self.good = Metrics.counter("bikes", "good_rows")
        self.bad = Metrics.counter("bikes", "bad_rows")
        self.over = Metrics.counter("bikes", "over_capacity")

    def process(self, row):
        try:
            missing = [c for c in REQUIRED if row.get(c) in (None, "")]
            if missing:
                raise RowError("missing:" + ",".join(missing))
            station_id = to_int(row, "station_id")
            bikes = to_int(row, "num_bikes_available")
            seconds = parse_timestamp(row["last_reported"])
            if bikes < 0:
                raise RowError("negative_bikes")
        except RowError as err:
            self.bad.inc()
            yield beam.pvalue.TaggedOutput(BAD, {
                "reason": str(err),
                "source": row.get("_source"),
                "raw": json.dumps(row)[:500]})
            return
        capacity = row.get("capacity")
        if capacity not in (None, "") and bikes > float(capacity):
            self.over.inc()  # odd but possible: keep the row, count it
        self.good.inc()
        yield {"station_id": station_id, "bikes": bikes, "seconds": seconds}


def stamp(rec):
    """Give each element its real event time: windows follow the data."""
    return window.TimestampedValue(
        (rec["station_id"], rec["bikes"]), rec["seconds"])


class BikeStats(beam.CombineFn):
    """Count, average, min and max of bikes available, computed in parallel."""

    def create_accumulator(self):
        # TODO 1a: the summary needs a fifth number: how many readings had 0 bikes
        return (0, 0, None, None)  # n, total, lowest, highest

    def add_input(self, acc, bikes):
        # TODO 1b: unpack the fifth number, and add 1 to it when bikes == 0
        n, total, low, high = acc
        return (n + 1, total + bikes,
                bikes if low is None else min(low, bikes),
                bikes if high is None else max(high, bikes))

    def merge_accumulators(self, accs):
        # TODO 1c: merge the fifth number too (partial results are added together)
        n, total, low, high = 0, 0, None, None
        for a_n, a_total, a_low, a_high in accs:
            n, total = n + a_n, total + a_total
            if a_low is not None:
                low = a_low if low is None else min(low, a_low)
                high = a_high if high is None else max(high, a_high)
        return (n, total, low, high)

    def extract_output(self, acc):
        # TODO 1d: report the fifth number as "empty_readings"
        n, total, low, high = acc
        return {"samples": n, "min_bikes": low, "max_bikes": high,
                "avg_bikes": round(total / n, 2) if n else None}


def parse_station(line):
    """Reference line -> (station number, name). Bad lines are skipped."""
    try:
        cells = next(csv.reader([line]))
        return [(int(cells[0]), cells[1])]
    except (ValueError, IndexError, StopIteration):
        return []


class ToRow(beam.DoFn):
    """Make an output row; station names arrive as a side input."""

    def __init__(self):
        self.unknown = Metrics.counter("bikes", "unknown_station")

    def process(self, element, win=beam.DoFn.WindowParam, names=None):
        station_id, stats = element
        name = names.get(station_id)
        if name is None:
            self.unknown.inc()
        start = datetime.fromtimestamp(win.start.micros / 1e6, tz=timezone.utc)
        yield {"station_id": station_id, "station_name": name,
               "hour_start": start.strftime("%Y-%m-%d %H:%M:%S UTC"), **stats}


# TODO 3: the BigQuery table needs the new column too (type INTEGER)
HOURLY_SCHEMA = ("station_id:INTEGER,station_name:STRING,hour_start:TIMESTAMP,"
                 "samples:INTEGER,avg_bikes:FLOAT,min_bikes:INTEGER,"
                 "max_bikes:INTEGER")
BAD_SCHEMA = "reason:STRING,source:STRING,raw:STRING"


def build(p, args):
    """Wire the pipeline: read, validate, window, aggregate, enrich."""
    stations = (
        p | "ReadStations" >> beam.io.ReadFromText(
            args.stations, skip_header_lines=1)
          | "ParseStations" >> beam.FlatMap(parse_station))

    checked = (
        p | "Match" >> fileio.MatchFiles(args.input)
          | "OpenFiles" >> fileio.ReadMatches()
          | "ReadRows" >> beam.ParDo(ReadCsvFile())
          | "Validate" >> beam.ParDo(ValidateRow()).with_outputs(
              BAD, main="good"))

    hourly = (
        checked.good
        | "Stamp" >> beam.Map(stamp)
        # TODO 2b: use your --window_minutes option here instead of 3600
        | "Hourly" >> beam.WindowInto(window.FixedWindows(3600))
        | "Stats" >> beam.CombinePerKey(BikeStats())
        | "Enrich" >> beam.ParDo(
            ToRow(), names=beam.pvalue.AsDict(stations)))
    return hourly, checked[BAD]


def write_results(hourly, bad, args, project):
    """Send the two results to BigQuery or to local files."""
    if args.bq_dataset:
        def sink(table, schema):
            return beam.io.WriteToBigQuery(
                f"{project}:{args.bq_dataset}.{table}", schema=schema,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                # replace, never append: re-running gives the same table
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                method=beam.io.WriteToBigQuery.Method.FILE_LOADS)
        hourly | "WriteHourly" >> sink("station_hourly", HOURLY_SCHEMA)
        bad | "WriteBad" >> sink("bad_rows", BAD_SCHEMA)
    else:
        text = beam.io.WriteToText
        (hourly | "HourlyJson" >> beam.Map(json.dumps)
                | "WriteHourly" >> text(args.output, file_name_suffix=".jsonl"))
        (bad | "BadJson" >> beam.Map(json.dumps)
             | "WriteBad" >> text(args.output + "_bad",
                                  file_name_suffix=".jsonl"))


def parse_args(argv):
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--input", required=True,
                        help="CSV file or glob, local or gs://")
    parser.add_argument("--stations", required=True,
                        help="station reference CSV (Number,Name,...)")
    parser.add_argument("--output",
                        help="local output prefix (use this OR --bq_dataset)")
    parser.add_argument("--bq_dataset", help="BigQuery dataset for the results")
    # TODO 2a: add a --window_minutes option here (an int, default 60)
    args, beam_args = parser.parse_known_args(argv)
    if bool(args.output) == bool(args.bq_dataset):
        parser.error("give exactly one of --output or --bq_dataset")
    return args, beam_args


def print_counters(result):
    query = beam.metrics.MetricsFilter().with_namespace("bikes")
    counters = result.metrics().query(query)["counters"]
    print("\nCounters:")
    for c in sorted(counters, key=lambda c: c.key.metric.name):
        value = c.committed if c.committed is not None else c.attempted
        print(f"  {c.key.metric.name:<16}{value}")


def run(argv=None):
    args, beam_args = parse_args(argv)
    options = PipelineOptions(beam_args, save_main_session=True)
    project = options.view_as(GoogleCloudOptions).project
    local = options.get_all_options().get("runner") in (None, "DirectRunner")
    if local:
        logging.getLogger().setLevel(logging.WARNING)  # short local output
        logging.getLogger("apache_beam.utils.subprocess_server").setLevel(
            logging.ERROR)

    pipeline = beam.Pipeline(options=options)
    hourly, bad = build(pipeline, args)
    write_results(hourly, bad, args, project)
    result = pipeline.run()
    result.wait_until_finish()
    if local:
        print_counters(result)


if __name__ == "__main__":
    run()
