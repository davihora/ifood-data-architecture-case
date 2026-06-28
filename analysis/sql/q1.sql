-- Q1 — Average total_amount per month (yellow, Jan–May 2023).
-- The Gold mart is pre-aggregated; query it via DuckDB (the consumption engine; `make analyze`):
SELECT pickup_year_month, trips, avg_total_amount
FROM delta_scan('s3://datalake/gold/agg_monthly_total_amount')
ORDER BY pickup_year_month;

-- Equivalent computed straight from the cleaned Silver table:
-- SELECT strftime(tpep_pickup_datetime, '%Y-%m')  AS pickup_year_month,
--        count(*)                                 AS trips,
--        round(avg(total_amount), 2)              AS avg_total_amount
-- FROM delta_scan('s3://datalake/silver/yellow_trips')
-- GROUP BY 1
-- ORDER BY 1;
