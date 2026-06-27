"""Exploratory data analysis over the Silver layer + the data-quality results.

Run with `make eda` (added) or `spark-submit /opt/app/analysis/eda.py`. Prints the
volume per month, the share of dirty rows caught, and basic distributions — the EDA
narrative behind the cleaning decisions.
"""

from __future__ import annotations

from pyspark.sql import functions as F

from src.common.io import SILVER, table_uri
from src.common.spark import get_spark
from src.config import settings


def main() -> int:
    spark = get_spark("eda")
    ds = settings.tlc_dataset

    silver = spark.read.format("delta").load(table_uri(SILVER, f"{ds}_trips"))
    print("\n=== Volume por mês (Silver, limpo) ===")
    silver.groupBy("pickup_year_month").count().orderBy("pickup_year_month").show(truncate=False)

    print("=== total_amount: estatísticas (Silver) ===")
    silver.select(
        F.round(F.min("total_amount"), 2).alias("min"),
        F.round(F.avg("total_amount"), 2).alias("avg"),
        F.round(F.expr("percentile_approx(total_amount, 0.5)"), 2).alias("p50"),
        F.round(F.max("total_amount"), 2).alias("max"),
    ).show(truncate=False)

    print("=== passenger_count: distribuição (Silver) ===")
    silver.groupBy("passenger_count").count().orderBy("passenger_count").show(truncate=False)

    rejected = spark.read.format("delta").load(table_uri(SILVER, f"{ds}_trips_rejected"))
    print("=== Linhas quarentenadas por motivo ===")
    (
        rejected.select(F.explode("_reject_reason").alias("reason"))
        .groupBy("reason")
        .count()
        .orderBy(F.desc("count"))
        .show(truncate=False)
    )

    try:
        dq = spark.read.format("delta").load(table_uri("dq", "check_results"))
        print("=== Últimas métricas de DQ ===")
        dq.orderBy(F.desc("run_id")).show(20, truncate=False)
    except Exception as exc:  # noqa: BLE001 - dq table is optional
        print(f"(sem tabela dq.check_results ainda: {exc})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
