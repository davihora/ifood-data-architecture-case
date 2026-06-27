"""PySpark tests for the data-quality engine (run in CI)."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.spark


def test_gate_splits_and_warn_does_not_quarantine(spark):
    from pyspark.sql import functions as F

    from src.common.dq import QUARANTINE, WARN, Check, run_row_checks

    df = spark.createDataFrame([(1, 5.0), (2, -1.0), (3, 0.0)], "id int, amt double")
    checks = [
        Check("amt_positive", F.col("amt") > 0, QUARANTINE),
        Check("id_negative", F.col("id") < 0, WARN),
    ]
    res = run_row_checks(df, checks)

    assert res.valid.count() == 1  # only amt=5.0 passes the gate
    assert res.quarantine.count() == 2  # amt<=0 rows
    metrics = {m["check"]: m for m in res.metrics}
    assert metrics["amt_positive"]["rows_failed"] == 2
    # WARN check fails for all 3 rows but must NOT move them to quarantine
    assert metrics["id_negative"]["rows_failed"] == 3
    assert metrics["id_negative"]["severity"] == WARN


def test_reject_reason_lists_failed_checks(spark):
    from pyspark.sql import functions as F

    from src.common.dq import QUARANTINE, Check, run_row_checks

    df = spark.createDataFrame([(-1, -1.0)], "id int, amt double")
    checks = [
        Check("amt_positive", F.col("amt") > 0, QUARANTINE),
        Check("id_positive", F.col("id") > 0, QUARANTINE),
    ]
    res = run_row_checks(df, checks)
    reasons = set(res.quarantine.first()["_reject_reason"])
    assert reasons == {"amt_positive", "id_positive"}
