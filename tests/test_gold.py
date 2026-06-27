"""PySpark tests for the Gold marts — Q1/Q2 must match the hand-computed expectations
(the same constants the DuckDB oracle asserts). Run in CI.
"""

from __future__ import annotations

import pytest

from tests.expected import Q1_EXPECTED, Q2_EXPECTED

pytestmark = pytest.mark.spark


def _clean(spark, fixture_path):
    from src.jobs.silver import clean

    valid, _, _ = clean(spark.read.parquet(fixture_path))
    return valid


def test_q1_monthly_avg_amount(spark, fixture_path):
    from src.jobs.gold import q1_monthly_avg_amount

    rows = q1_monthly_avg_amount(_clean(spark, fixture_path)).collect()
    got = {r["pickup_year_month"]: (r["trips"], r["avg_total_amount"]) for r in rows}
    assert got == Q1_EXPECTED


def test_q2_may_passengers_by_hour(spark, fixture_path):
    from src.jobs.gold import q2_may_passengers_by_hour

    rows = q2_may_passengers_by_hour(_clean(spark, fixture_path)).collect()
    got = {r["pickup_hour"]: (r["trips"], r["avg_passenger_count"]) for r in rows}
    assert got == Q2_EXPECTED


def test_fact_has_required_columns(spark, fixture_path):
    from src.jobs.gold import build_fact

    fact = build_fact(_clean(spark, fixture_path))
    required = {
        "VendorID",
        "passenger_count",
        "total_amount",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
    }
    assert required <= set(fact.columns)
