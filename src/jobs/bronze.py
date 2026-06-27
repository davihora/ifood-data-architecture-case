"""Bronze job: landing parquet -> Delta, conformed types + audit columns.

Raw fidelity is preserved in the landing zone; Bronze keeps the columns of interest
(the 5 required by the case + trip_distance for richer EDA) with CANONICAL types. Each
monthly file is read separately and unioned by name, because TLC monthly files drift
INT64<->DOUBLE (passenger_count / VendorID) and a single multi-file read can fail.
Provenance columns are added for lineage/auditing.
"""

from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.io import BRONZE, LANDING, table_uri
from src.common.s3 import list_keys
from src.common.spark import get_spark
from src.config import settings

# Columns of interest with their canonical types (drift-proof projection).
KEEP_TYPES = {
    "VendorID": "int",
    "passenger_count": "double",
    "total_amount": "double",
    "tpep_pickup_datetime": "timestamp",
    "tpep_dropoff_datetime": "timestamp",
    "trip_distance": "double",
}
_MONTH_RE = r"(\d{4})-(\d{2})"


def conform_schema(df: DataFrame) -> DataFrame:
    """Project to the columns of interest with canonical types."""
    cols = [F.col(c).cast(t).alias(c) for c, t in KEEP_TYPES.items() if c in df.columns]
    return df.select(*cols)


def add_audit(df: DataFrame, source_file: str, batch_id: str) -> DataFrame:
    """Append provenance + partition columns derived from the source file name."""
    src = F.lit(source_file)
    return (
        df.withColumn("_source_file", src)
        .withColumn("_source_month", F.regexp_extract(src, _MONTH_RE, 0))
        .withColumn("source_year", F.regexp_extract(src, _MONTH_RE, 1))
        .withColumn("source_month", F.regexp_extract(src, _MONTH_RE, 2))
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_ingested_at", F.current_timestamp())
    )


def read_conformed(spark: SparkSession, dataset: str, batch_id: str) -> DataFrame:
    prefix = f"{LANDING}/{dataset}/"
    keys = sorted(k for k in list_keys(prefix) if k.endswith(".parquet"))
    if not keys:
        raise FileNotFoundError(f"no landing parquet under {prefix} — run ingestion first")
    frames = []
    for key in keys:
        raw = spark.read.parquet(f"s3a://{settings.bucket}/{key}")
        frames.append(add_audit(conform_schema(raw), key, batch_id))
    out = frames[0]
    for frame in frames[1:]:
        out = out.unionByName(frame, allowMissingColumns=True)
    return out


def run(spark: SparkSession | None = None) -> str:
    spark = spark or get_spark("bronze")
    batch_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    df = read_conformed(spark, settings.tlc_dataset, batch_id)
    target = table_uri(BRONZE, f"{settings.tlc_dataset}_trips")
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("source_year", "source_month")
        .save(target)
    )
    print(f"[bronze] wrote {df.count():,} rows -> {target}")
    return target


def main() -> int:
    run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
