"""Gold job: curated ML-ready fact + the two business-question marts.

Q1 — average total_amount per month (cleaning already applied in Silver; no passenger
     filter, because revenue should not be distorted by passenger-count quality).
Q2 — average passenger_count by pickup hour-of-day for May, restricted to passengers>0.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.io import GOLD, SILVER, table_uri
from src.common.spark import get_spark
from src.config import settings

MAY = 5

FACT_COLUMNS = [
    "VendorID",
    "passenger_count",
    "total_amount",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_distance",
    "pickup_year",
    "pickup_month",
    "pickup_year_month",
    "pickup_hour",
    "pickup_date",
]


def build_fact(silver: DataFrame) -> DataFrame:
    """Analytics-/ML-ready trip fact: required columns + time dimensions."""
    return silver.select(*FACT_COLUMNS)


def q1_monthly_avg_amount(silver: DataFrame) -> DataFrame:
    """Q1: average total_amount per month."""
    return (
        silver.groupBy("pickup_year_month")
        .agg(
            F.count(F.lit(1)).alias("trips"),
            F.round(F.avg("total_amount"), 2).alias("avg_total_amount"),
        )
        .orderBy("pickup_year_month")
    )


def q2_may_passengers_by_hour(silver: DataFrame) -> DataFrame:
    """Q2: average passenger_count by pickup hour-of-day for May, passengers>0."""
    return (
        silver.filter((F.col("pickup_month") == MAY) & (F.col("passenger_count") > 0))
        .groupBy("pickup_hour")
        .agg(
            F.count(F.lit(1)).alias("trips"),
            F.round(F.avg("passenger_count"), 3).alias("avg_passenger_count"),
        )
        .orderBy("pickup_hour")
    )


def _save(df: DataFrame, name: str, *partition: str) -> None:
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition:
        writer = writer.partitionBy(*partition)
    writer.save(table_uri(GOLD, name))


def run(spark: SparkSession | None = None) -> str:
    spark = spark or get_spark("gold")
    silver = spark.read.format("delta").load(table_uri(SILVER, f"{settings.tlc_dataset}_trips"))
    _save(build_fact(silver), "fact_yellow_trips", "pickup_year", "pickup_month")
    _save(q1_monthly_avg_amount(silver), "agg_monthly_total_amount")
    _save(q2_may_passengers_by_hour(silver), "agg_may_passengers_by_hour")
    print("[gold] wrote fact_yellow_trips, agg_monthly_total_amount, agg_may_passengers_by_hour")
    return table_uri(GOLD, "fact_yellow_trips")


def main() -> int:
    run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
