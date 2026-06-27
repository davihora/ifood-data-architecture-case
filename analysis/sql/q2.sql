-- Q2 — Average passenger_count by pickup hour-of-day (May 2023, passengers > 0).
SELECT pickup_hour, trips, avg_passenger_count
FROM delta.gold.agg_may_passengers_by_hour
ORDER BY pickup_hour;

-- Equivalent computed straight from the cleaned Silver table:
-- SELECT hour(tpep_pickup_datetime)    AS pickup_hour,
--        count(*)                      AS trips,
--        round(avg(passenger_count),3) AS avg_passenger_count
-- FROM delta.silver.yellow_trips
-- WHERE month(tpep_pickup_datetime) = 5
--   AND passenger_count > 0
-- GROUP BY 1
-- ORDER BY 1;
