"""Pure-Python tests for path/config helpers (no Spark)."""

from __future__ import annotations

from src.common import io
from src.config import settings


def test_landing_key():
    assert (
        io.landing_key("yellow", "2023-01")
        == "landing/yellow/year=2023/month=01/yellow_tripdata_2023-01.parquet"
    )


def test_landing_uri_matches_key():
    assert (
        io.landing_uri("yellow", "2023-03")
        == f"s3a://{settings.bucket}/{io.landing_key('yellow', '2023-03')}"
    )


def test_table_uri():
    assert io.table_uri("silver", "yellow_trips") == f"s3a://{settings.bucket}/silver/yellow_trips"


def test_layer_uri_joins_parts():
    assert settings.layer_uri("gold", "a", "b") == f"s3a://{settings.bucket}/gold/a/b"


def test_landing_glob():
    assert io.landing_glob("yellow").endswith("/landing/yellow/year=*/month=*/*.parquet")


def test_use_ssl_flag():
    assert settings.use_ssl == settings.s3_endpoint.lower().startswith("https")
