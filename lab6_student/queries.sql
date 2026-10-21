-- Queries for the Dublin Bikes demo. Replace YOUR_PROJECT with your project id (run_queries.sh does it for you).
-- Each query is separated by a line starting with "-- ##".

-- ## 1. Did the pipeline load anything? (hourly rows, stations, first and last hour)
SELECT COUNT(*) AS hourly_rows,
       COUNT(DISTINCT station_id) AS stations,
       MIN(hour_start) AS first_hour,
       MAX(hour_start) AS last_hour
FROM `YOUR_PROJECT.bikes.station_hourly`;

-- ## 2. What did the quarantine catch? (bad rows by file and reason)
SELECT source, reason, COUNT(*) AS bad_rows
FROM `YOUR_PROJECT.bikes.bad_rows`
GROUP BY source, reason
ORDER BY bad_rows DESC;

-- ## 3. Which stations are most often empty? (June 2024)
SELECT COALESCE(station_name, CONCAT('station ', CAST(station_id AS STRING))) AS station,
       ROUND(AVG(avg_bikes), 1) AS typical_bikes,
       ROUND(100 * SAFE_DIVIDE(COUNTIF(min_bikes = 0), COUNT(*)), 1) AS pct_hours_that_hit_zero
FROM `YOUR_PROJECT.bikes.station_hourly`
WHERE hour_start >= TIMESTAMP '2024-06-01' AND hour_start < TIMESTAMP '2024-07-01'
GROUP BY station_id, station
ORDER BY pct_hours_that_hit_zero DESC
LIMIT 10;

-- ## 4. Average bikes available by hour of day, Dublin local time (June 2024)
SELECT EXTRACT(HOUR FROM hour_start AT TIME ZONE 'Europe/Dublin') AS hour_of_day,
       ROUND(AVG(avg_bikes), 1) AS avg_bikes_per_station
FROM `YOUR_PROJECT.bikes.station_hourly`
WHERE hour_start >= TIMESTAMP '2024-06-01' AND hour_start < TIMESTAMP '2024-07-01'
GROUP BY hour_of_day
ORDER BY hour_of_day;

-- ## 5. RECONCILIATION: does the Beam pipeline (ETL) agree with plain SQL on the raw table (ELT)? June 2024 only.
WITH elt AS (
  SELECT station_id,
         TIMESTAMP_TRUNC(SAFE_CAST(last_reported AS TIMESTAMP), HOUR) AS hour_start,
         COUNT(*) AS samples,
         ROUND(AVG(num_bikes_available), 2) AS avg_bikes
  FROM `YOUR_PROJECT.bikes.raw_2024_06`
  WHERE station_id IS NOT NULL
    AND num_bikes_available >= 0
    AND SAFE_CAST(last_reported AS TIMESTAMP) >= TIMESTAMP '2024-06-01'
    AND SAFE_CAST(last_reported AS TIMESTAMP) <  TIMESTAMP '2024-07-01'
  GROUP BY station_id, hour_start
),
etl AS (
  SELECT station_id, hour_start, samples, avg_bikes
  FROM `YOUR_PROJECT.bikes.station_hourly`
  WHERE hour_start >= TIMESTAMP '2024-06-01' AND hour_start < TIMESTAMP '2024-07-01'
)
SELECT (SELECT COUNT(*) FROM elt) AS elt_rows,
       (SELECT COUNT(*) FROM etl) AS etl_rows,
       (SELECT COUNT(*) FROM elt JOIN etl USING (station_id, hour_start)
         WHERE elt.samples = etl.samples AND ABS(elt.avg_bikes - etl.avg_bikes) < 0.01) AS matching_rows;
