"""Independent DuckDB oracle: recomputes the Silver cleaning + Q1/Q2 on the fixture and
asserts the SAME hand-computed expectations the PySpark pipeline must produce. Runs
anywhere (no JVM), so it validates the analytics logic even where Spark can't start.
"""

from __future__ import annotations

import pathlib

import duckdb

from tests.expected import (
    FIXTURE_TOTAL_ROWS,
    Q1_EXPECTED,
    Q2_EXPECTED,
    SILVER_QUARANTINE_ROWS,
    SILVER_VALID_ROWS,
)

FIX = str(pathlib.Path(__file__).parent / "fixtures" / "yellow_sample.parquet")

VALID_CTE = f"""
WITH deduped AS (
  SELECT DISTINCT VendorID, passenger_count, total_amount,
         tpep_pickup_datetime, tpep_dropoff_datetime, trip_distance
  FROM read_parquet('{FIX}')
),
valid AS (
  SELECT * FROM deduped
  WHERE VendorID IS NOT NULL AND tpep_pickup_datetime IS NOT NULL
    AND tpep_dropoff_datetime IS NOT NULL AND total_amount IS NOT NULL
    AND tpep_pickup_datetime >= TIMESTAMP '2023-01-01 00:00:00'
    AND tpep_pickup_datetime <  TIMESTAMP '2023-06-01 00:00:00'
    AND total_amount > 0 AND tpep_dropoff_datetime >= tpep_pickup_datetime
)
"""


def test_total_rows():
    assert (
        duckdb.sql(f"SELECT count(*) FROM read_parquet('{FIX}')").fetchone()[0]
        == FIXTURE_TOTAL_ROWS
    )


def test_silver_counts():
    deduped = duckdb.sql(
        f"SELECT count(*) FROM (SELECT DISTINCT * FROM read_parquet('{FIX}'))"
    ).fetchone()[0]
    valid = duckdb.sql(VALID_CTE + "SELECT count(*) FROM valid").fetchone()[0]
    assert deduped == FIXTURE_TOTAL_ROWS - 1  # one exact duplicate removed
    assert valid == SILVER_VALID_ROWS
    assert deduped - valid == SILVER_QUARANTINE_ROWS


def test_q1_monthly():
    rows = duckdb.sql(
        VALID_CTE + "SELECT strftime(tpep_pickup_datetime,'%Y-%m') m, count(*) trips, "
        "round(avg(total_amount),2) a FROM valid GROUP BY 1 ORDER BY 1"
    ).fetchall()
    assert {m: (trips, a) for m, trips, a in rows} == Q1_EXPECTED


def test_q2_may_by_hour():
    rows = duckdb.sql(
        VALID_CTE
        + "SELECT hour(tpep_pickup_datetime) h, count(*) trips, round(avg(passenger_count),3) a "
        "FROM valid WHERE month(tpep_pickup_datetime)=5 AND passenger_count>0 GROUP BY 1 ORDER BY 1"
    ).fetchall()
    assert {int(h): (trips, a) for h, trips, a in rows} == Q2_EXPECTED
