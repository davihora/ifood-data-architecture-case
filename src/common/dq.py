"""Lightweight data-quality engine for the Silver gate.

A ``Check`` is a boolean Spark ``Column`` that is TRUE for VALID rows. ``run_row_checks``
splits a DataFrame into (valid, quarantine) and emits one metric per check in a SINGLE
aggregation pass. Severities:

* ``BLOCK``      — caller should fail the pipeline if any row fails.
* ``QUARANTINE`` — failing rows are routed to ``*_rejected`` with a ``_reject_reason``.
* ``WARN``       — only recorded as a metric.

Kept dependency-free (plain PySpark) so it runs identically in the cluster and in
unit tests; Great Expectations / Soda are noted as heavier alternatives in the ADRs.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

BLOCK = "BLOCK"
QUARANTINE = "QUARANTINE"
WARN = "WARN"


@dataclass(frozen=True)
class Check:
    name: str
    valid_when: Column  # TRUE for rows that PASS the check
    severity: str = QUARANTINE


@dataclass(frozen=True)
class DQResult:
    valid: DataFrame
    quarantine: DataFrame
    metrics: list[dict]


def _failed(valid_when: Column) -> Column:
    """Null-safe failure: a NULL predicate (e.g. ``amount > 0`` on a NULL amount) counts as a
    failure, so per-check metrics and reasons don't undercount NULL-input rows."""
    return ~F.coalesce(valid_when, F.lit(False))


def run_row_checks(df: DataFrame, checks: list[Check]) -> DQResult:
    """Tag, split into valid/quarantine, and compute per-check metrics in one pass.

    Only BLOCK/QUARANTINE checks gate rows; WARN checks contribute metrics only.
    """
    gates = [c for c in checks if c.severity != WARN]
    reason = F.array_compact(
        F.array(*[F.when(_failed(c.valid_when), F.lit(c.name)) for c in gates])
    )
    tagged = df.withColumn("_reject_reason", reason)
    valid = tagged.filter(F.size("_reject_reason") == 0).drop("_reject_reason")
    quarantine = tagged.filter(F.size("_reject_reason") > 0)

    agg = [F.count(F.lit(1)).alias("rows_in")]
    agg += [
        F.sum(F.when(_failed(c.valid_when), 1).otherwise(0)).alias(f"failed__{c.name}")
        for c in checks
    ]
    row = df.agg(*agg).collect()[0]
    total = int(row["rows_in"])
    metrics = [
        {
            "check": c.name,
            "severity": c.severity,
            "rows_in": total,
            "rows_failed": int(row[f"failed__{c.name}"] or 0),
            "passed": int(row[f"failed__{c.name}"] or 0) == 0,
        }
        for c in checks
    ]
    return DQResult(valid=valid, quarantine=quarantine, metrics=metrics)


def assert_no_block_failures(metrics: list[dict]) -> None:
    """Raise if any BLOCK-severity check failed (use to stop the pipeline)."""
    failed = [m for m in metrics if m["severity"] == BLOCK and not m["passed"]]
    if failed:
        raise ValueError(f"BLOCK-severity DQ checks failed: {failed}")
