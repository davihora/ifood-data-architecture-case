"""Dagster assets wiring the medallion pipeline: ingest -> bronze -> silver -> gold.

Each layer is a software-defined asset; ordering is declared with deps=[...] and data is
passed through Delta table paths (NOT Dagster I/O managers — Spark DataFrames are lazy and
not picklable). The underlying job functions (src.jobs.*.run) stay runnable WITHOUT Dagster,
so `make pipeline` / spark-submit exercise the exact same code. Lineage renders in the UI.

Note on SparkSession lifecycle (see docs/adr/0008): we call get_spark() inside each asset
rather than dagster-pyspark's PySparkResource, which never stops the session.
"""

from __future__ import annotations

import os

import dagster as dg

from src.config import settings


@dg.asset(group_name="medallion")
def landing(context: dg.AssetExecutionContext) -> None:
    """Download TLC parquet for the configured window into the MinIO landing zone."""
    from src.ingestion.downloader import main as ingest

    start = os.environ.get("INGEST_START", "2023-01")
    end = os.environ.get("INGEST_END", "2023-05")
    ingest(["--start", start, "--end", end, "--dataset", settings.tlc_dataset])
    context.add_output_metadata({"dataset": settings.tlc_dataset, "window": f"{start}..{end}"})


@dg.asset(deps=[landing], group_name="medallion")
def bronze_trips(context: dg.AssetExecutionContext) -> None:
    from src.jobs import bronze

    context.add_output_metadata({"table": bronze.run()})


@dg.asset(deps=[bronze_trips], group_name="medallion")
def silver_trips(context: dg.AssetExecutionContext) -> None:
    from src.jobs import silver

    context.add_output_metadata({"table": silver.run()})


@dg.asset(deps=[silver_trips], group_name="medallion")
def gold_marts(context: dg.AssetExecutionContext) -> None:
    from src.jobs import gold

    context.add_output_metadata({"table": gold.run()})
