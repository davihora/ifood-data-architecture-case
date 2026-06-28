"""Silver job: clean & conform Bronze into analytics-ready trips + a quarantine table.

Cleaning policy: keep rows with non-null keys, pickup inside the
ingestion window, total_amount > 0, and dropoff >= pickup. Exact duplicates are removed.
passenger_count quality is recorded as a WARN metric only — the passengers>0 filter is
applied per-question in Gold, so Q1 (revenue) is not distorted by dropping those trips.
Per-check metrics are persisted to a dq.check_results Delta table for observability.
"""

from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.dq import QUARANTINE, WARN, Check, assert_no_block_failures, run_row_checks
from src.common.io import BRONZE, SILVER, table_uri
from src.common.spark import get_spark
from src.config import settings

# Ingestion window for this case (Jan–May 2023): pickup must fall inside [start, end).
WINDOW_START = "2023-01-01 00:00:00"
WINDOW_END = "2023-06-01 00:00:00"

BUSINESS_COLS = [
    "VendorID",
    "passenger_count",
    "total_amount",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_distance",
]


def build_checks() -> list[Check]:
    pickup = F.col("tpep_pickup_datetime")
    dropoff = F.col("tpep_dropoff_datetime")
    amount = F.col("total_amount")
    passengers = F.col("passenger_count")
    return [
        Check("vendor_not_null", F.col("VendorID").isNotNull(), QUARANTINE),
        Check("pickup_not_null", pickup.isNotNull(), QUARANTINE),
        Check("dropoff_not_null", dropoff.isNotNull(), QUARANTINE),
        Check("amount_not_null", amount.isNotNull(), QUARANTINE),
        Check(
            "pickup_in_window",
            (pickup >= F.to_timestamp(F.lit(WINDOW_START)))
            & (pickup < F.to_timestamp(F.lit(WINDOW_END))),
            QUARANTINE,
        ),
        Check("amount_positive", amount > 0, QUARANTINE),
        Check("dropoff_after_pickup", dropoff >= pickup, QUARANTINE),
        Check("passenger_positive", passengers.isNotNull() & (passengers > 0), WARN),
    ]


def enrich(df: DataFrame) -> DataFrame:
    """Add pickup-derived time dimensions (computed on already-valid rows)."""
    pickup = F.col("tpep_pickup_datetime")
    return (
        df.withColumn("pickup_year", F.year(pickup))
        .withColumn("pickup_month", F.month(pickup))
        .withColumn("pickup_year_month", F.date_format(pickup, "yyyy-MM"))
        .withColumn("pickup_hour", F.hour(pickup))
        .withColumn("pickup_date", F.to_date(pickup))
    )


def clean(df: DataFrame) -> tuple[DataFrame, DataFrame, list[dict]]:
    """Return (valid_enriched, quarantine, dq_metrics)."""
    deduped = df.dropDuplicates(BUSINESS_COLS)
    res = run_row_checks(deduped, build_checks())
    return enrich(res.valid), res.quarantine, res.metrics


def write_metrics(spark: SparkSession, metrics: list[dict]) -> None:
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    rows = [{**m, "run_id": run_id, "layer": "silver"} for m in metrics]
    spark.createDataFrame(rows).write.format("delta").mode("append").save(
        table_uri("dq", "check_results")
    )
    for m in metrics:
        flag = "WARN" if m["severity"] == WARN else ("FAIL" if not m["passed"] else "ok")
        print(f"  DQ {m['check']:<22} {flag:<5} failed={m['rows_failed']:,}/{m['rows_in']:,}")


def run(spark: SparkSession | None = None) -> str:
    spark = spark or get_spark("silver")
    bronze = spark.read.format("delta").load(table_uri(BRONZE, f"{settings.tlc_dataset}_trips"))
    valid, quarantine, metrics = clean(bronze)

    silver_t = table_uri(SILVER, f"{settings.tlc_dataset}_trips")
    (
        valid.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("pickup_year", "pickup_month")
        .save(silver_t)
    )
    (
        quarantine.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(table_uri(SILVER, f"{settings.tlc_dataset}_trips_rejected"))
    )
    write_metrics(spark, metrics)
    assert_no_block_failures(metrics)  # fail fast if any BLOCK-severity check failed
    print(f"[silver] valid={valid.count():,} quarantined={quarantine.count():,} -> {silver_t}")
    return silver_t


def main() -> int:
    run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
