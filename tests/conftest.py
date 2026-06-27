"""Pytest fixtures: a local SparkSession (Delta-enabled) and the fixture-parquet path.

The `spark` fixture builds a local[1] session via configure_spark_with_delta_pip; tests
that use it are marked `spark` and run in CI (Java + network). Pure-Python tests and the
DuckDB oracle need neither and run anywhere.
"""

from __future__ import annotations

import os
import pathlib

import pytest

FIXTURE = pathlib.Path(__file__).parent / "fixtures" / "yellow_sample.parquet"


@pytest.fixture
def fixture_path() -> str:
    return str(FIXTURE)


@pytest.fixture(scope="session")
def spark():
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master("local[1]")
        .appName("tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    # CI (pip pyspark, no jars): pull Delta via --packages. Container (baked jars):
    # set DELTA_VIA_PACKAGES=0 to use the image's jars offline.
    if os.environ.get("DELTA_VIA_PACKAGES", "1") == "1":
        from delta import configure_spark_with_delta_pip

        session = configure_spark_with_delta_pip(builder).getOrCreate()
    else:
        session = builder.getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
