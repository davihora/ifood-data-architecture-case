-- Q1 — Average total_amount per month (yellow, Jan–May 2023).
-- The Gold mart is pre-aggregated; query it directly via Trino:
SELECT pickup_year_month, trips, avg_total_amount
FROM delta.gold.agg_monthly_total_amount
ORDER BY pickup_year_month;

-- Equivalent computed straight from the cleaned Silver table (Trino or Spark SQL):
-- SELECT date_format(tpep_pickup_datetime, 'yyyy-MM') AS pickup_year_month,
--        count(*)                                     AS trips,
--        round(avg(total_amount), 2)                  AS avg_total_amount
-- FROM delta.silver.yellow_trips
-- GROUP BY 1
-- ORDER BY 1;
