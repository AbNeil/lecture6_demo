-- Lab 6 queries. Replace YOUR_PROJECT with your project id (run_lab_sql.sh does it for you).
-- Run one with:  ./run_lab_sql.sh 1      (or 2, or 3)

-- ## 1. THIN DAYS: how many rows arrive on each day of June 2024?
SELECT DATE(SAFE_CAST(last_reported AS TIMESTAMP)) AS day,
       COUNT(*) AS rows_that_day
FROM `YOUR_PROJECT.bikes.raw_2024_06`
GROUP BY day
ORDER BY day;

-- ## 2. Which stations are most often EMPTY? (uses the empty_readings column YOU added)
SELECT COALESCE(station_name, CONCAT('station ', CAST(station_id AS STRING))) AS station,
       SUM(___) AS empty_readings,                 -- blank 1: the column you added
       SUM(samples) AS readings,
       ROUND(100 * SAFE_DIVIDE(SUM(___), SUM(samples)), 1) AS pct_of_readings_empty   -- blank 2: same column
FROM `YOUR_PROJECT.bikes.station_hourly`
WHERE hour_start >= TIMESTAMP '2024-06-01' AND hour_start < TIMESTAMP '2024-07-01'
GROUP BY station_id, station
ORDER BY pct_of_readings_empty DESC
LIMIT 10;

-- ## 3. At what time of day are stations most often empty? (Dublin local time, June 2024)
SELECT EXTRACT(HOUR FROM hour_start AT TIME ZONE '___') AS hour_of_day,   -- blank 3: Dublin's time zone name
       ROUND(100 * SAFE_DIVIDE(SUM(___), SUM(samples)), 1) AS pct_of_readings_empty   -- blank 4: same column
FROM `YOUR_PROJECT.bikes.station_hourly`
WHERE hour_start >= TIMESTAMP '2024-06-01' AND hour_start < TIMESTAMP '2024-07-01'
GROUP BY hour_of_day
ORDER BY hour_of_day;
