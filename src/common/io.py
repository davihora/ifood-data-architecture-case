"""Lake path helpers — single source of truth for where each layer lives.

Object keys (no scheme) are used by boto3 in the ingestion step; the matching
``s3a://`` URIs are used by Spark. Both point at the exact same location.
"""

from __future__ import annotations

from src.config import settings

LANDING = "landing"
BRONZE = "bronze"
SILVER = "silver"
GOLD = "gold"


def _yymm(month: str) -> tuple[str, str]:
    year, mm = month.split("-")
    return year, mm


def landing_key(dataset: str, month: str) -> str:
    """Object key (relative to the bucket) for a raw monthly parquet file."""
    year, mm = _yymm(month)
    return f"{LANDING}/{dataset}/year={year}/month={mm}/{dataset}_tripdata_{month}.parquet"


def landing_uri(dataset: str, month: str) -> str:
    return f"s3a://{settings.bucket}/{landing_key(dataset, month)}"


def landing_glob(dataset: str) -> str:
    """All landed parquet files for a dataset (Spark read path)."""
    return f"s3a://{settings.bucket}/{LANDING}/{dataset}/year=*/month=*/*.parquet"


def table_uri(layer: str, name: str) -> str:
    """s3a:// URI of a Delta table in a given layer (e.g. table_uri(SILVER, 'yellow_trips'))."""
    return settings.layer_uri(layer, name)
