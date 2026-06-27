"""PySpark tests for the Bronze transforms (run in CI)."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.spark


def test_conform_schema_casts_and_projects(spark):
    from src.jobs.bronze import KEEP_TYPES, conform_schema

    df = spark.createDataFrame(
        [(1, "2", 3, "drop-me")],
        "VendorID int, passenger_count string, total_amount int, extra string",
    )
    out = conform_schema(df)
    assert set(out.columns) <= set(KEEP_TYPES)
    assert "extra" not in out.columns
    types = dict(out.dtypes)
    assert types["passenger_count"] == "double"
    assert types["total_amount"] == "double"


def test_add_audit_parses_source_month(spark):
    from src.jobs.bronze import add_audit

    key = "landing/yellow/year=2023/month=04/yellow_tripdata_2023-04.parquet"
    row = add_audit(spark.range(1), key, "BATCH1").first()
    assert row["_source_month"] == "2023-04"
    assert row["source_year"] == "2023"
    assert row["source_month"] == "04"
    assert row["_batch_id"] == "BATCH1"


def test_union_by_name_handles_type_drift(spark):
    from src.jobs.bronze import conform_schema

    a = conform_schema(
        spark.createDataFrame(
            [(1, 2, 10.0)], "VendorID int, passenger_count int, total_amount double"
        )
    )
    b = conform_schema(
        spark.createDataFrame(
            [(2, 3.0, 20.0)], "VendorID int, passenger_count double, total_amount double"
        )
    )
    union = a.unionByName(b, allowMissingColumns=True)
    assert union.count() == 2
    assert dict(union.dtypes)["passenger_count"] == "double"
