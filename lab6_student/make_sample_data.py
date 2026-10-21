"""Create a tiny SYNTHETIC Dublin Bikes dataset for trying the pipeline without any cloud account.

It mimics two real schema versions (15 columns and 10 columns), includes a header-only file,
and contains deliberately bad rows so you can watch the quarantine output work.
The numbers are made up. The station numbers and names are real (from the station reference file).
"""
import csv
import os
import random

OUT = "sample"
RAW, REF = OUT + "/raw", OUT + "/ref"
STATIONS = [(42, "SMITHFIELD NORTH", "Smithfield North", 53.349562, -6.278198),
            (30, "PARNELL SQUARE NORTH", "Parnell Square North", 53.353462, -6.265305),
            (54, "CLONMEL STREET", "Clonmel Street", 53.336021, -6.26298),
            (108, "AVONDALE ROAD", "Avondale Road", 53.359405, -6.276142),
            (56, "MOUNT STREET LOWER", "Mount Street Lower", 53.33796, -6.24153)]
OLD = ["system_id", "last_reported", "station_id", "num_bikes_available", "num_docks_available", "is_installed",
       "is_renting", "is_returning", "name", "short_name", "address", "lat", "lon", "region_id", "capacity"]
NEW = ["last_reported", "station_id", "num_bikes_available", "is_renting", "name", "address", "lat", "lon", "capacity"]


def rows_for(day, rng, style):
    out = []
    for number, name, address, lat, lon in STATIONS:
        for hour in range(8, 11):                      # three hours: 08:00-10:59
            for minute in range(0, 60, 10):            # a reading every 10 minutes
                bikes = rng.randint(0, 20)
                stamp = f"{day} {hour:02d}:{minute:02d}:07+00:00" if style == "old" else f"{day}T{hour:02d}:{minute:02d}:07Z"
                out.append({"system_id": "dublin", "last_reported": stamp, "station_id": number,
                            "num_bikes_available": bikes, "num_docks_available": 20 - bikes, "is_installed": "TRUE",
                            "is_renting": "TRUE", "is_returning": "TRUE", "name": name, "short_name": str(number),
                            "address": address, "lat": lat, "lon": lon, "region_id": "", "capacity": 20})
    return out


def write(path, columns, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def main():
    os.makedirs(RAW, exist_ok=True)
    os.makedirs(REF, exist_ok=True)
    rng = random.Random(42)
    old, new = rows_for("2024-06-01", rng, "old"), rows_for("2026-02-01", rng, "new")
    # deliberately bad rows (one of each kind) in the old file
    old += [dict(old[0], num_bikes_available=-3),            # negative bikes
            dict(old[1], station_id=""),                      # missing station id
            dict(old[2], last_reported="not a time"),         # bad timestamp
            dict(old[3], num_bikes_available="many")]         # not a number
    # plausible-but-odd rows in the new file: unknown station and over capacity
    new += [dict(new[0], station_id=999, name="MYSTERY STATION"),
            dict(new[1], num_bikes_available=25)]
    write(f"{RAW}/dublin-bikes_station_status_sample_2024.csv", OLD, old)
    write(f"{RAW}/dublin-bikes_station_status_sample_2026.csv", NEW, new)
    write(f"{RAW}/dublin-bikes_station_status_sample_empty.csv", NEW, [])   # header only
    with open(f"{REF}/stations.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["Number", "Name", "Address", "Latitude", "Longitude"])
        w.writerows(STATIONS)
    print("Wrote", len(old), "rows (15 columns),", len(new), "rows (9 columns), 1 header-only file and stations.csv under", OUT + "/")


if __name__ == "__main__":
    main()
