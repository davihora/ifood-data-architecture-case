"""PySpark tests for the Silver cleaning + DQ (run in CI)."""

from __future__ import annotations

import pytest

from tests.expected import SILVER_QUARANTINE_ROWS, SILVER_VALID_ROWS

pytestmark = pytest.mark.spark


def test_clean_splits_valid_and_quarantine(spark, fixture_path):
    from src.jobs.silver import clean

    valid, quarantine, _ = clean(spark.read.parquet(fixture_path))
    assert valid.count() == SILVER_VALID_ROWS
    assert quarantine.count() == SILVER_QUARANTINE_ROWS
    assert {"pickup_hour", "pickup_year_month", "pickup_date"} <= set(valid.columns)


def test_quarantine_reasons_cover_each_rule(spark, fixture_path):
    from src.jobs.silver import clean

    _, quarantine, _ = clean(spark.read.parquet(fixture_path))
    reasons: set[str] = set()
    for r in quarantine.select("_reject_reason").collect():
        reasons.update(r["_reject_reason"])
    assert {
        "pickup_in_window",
        "amount_positive",
        "pickup_not_null",
        "dropoff_after_pickup",
    } <= reasons


def test_passenger_check_is_warn_only(spark, fixture_path):
    from src.jobs.silver import clean

    valid, _, metrics = clean(spark.read.parquet(fixture_path))
    # the zero-passenger May trip stays in Silver (passenger filter is per-question)
    assert valid.filter("passenger_count = 0").count() == 1
    passenger_metric = next(m for m in metrics if m["check"] == "passenger_positive")
    assert passenger_metric["severity"] == "WARN"
