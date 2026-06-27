"""Answer Q1 & Q2 over the Gold Delta marts via DuckDB SQL — the lightweight consumption
path (`make analyze`).

DuckDB reads the Spark-written Delta tables directly from MinIO (S3-compatible), so serving
the answers via SQL needs no Spark or Trino. The exact same query runs on AWS by pointing
DuckDB/Athena at S3 — see docs/aws-reference-architecture.md.
"""

from __future__ import annotations

import os

import duckdb

from src.common.io import GOLD
from src.config import settings

_EXTENSIONS = ("httpfs", "delta")


def connect() -> duckdb.DuckDBPyConnection:
    """A DuckDB connection wired to read Delta tables from the (MinIO) object store.

    Extensions are baked into the image (DUCKDB_EXTENSION_DIRECTORY) so this works offline;
    if absent (e.g. local dev), it falls back to fetching them from the DuckDB CDN.
    """
    con = duckdb.connect()
    ext_dir = os.environ.get("DUCKDB_EXTENSION_DIRECTORY")
    if ext_dir:
        con.execute(f"SET extension_directory='{ext_dir}'")
    for ext in _EXTENSIONS:
        try:
            con.execute(f"LOAD {ext}")
        except Exception:  # offline-baked dir missing -> fetch from CDN (needs network)
            con.execute(f"INSTALL {ext}; LOAD {ext}")

    # delta_scan() goes through the delta-kernel/object_store layer, which does NOT read the
    # legacy `SET s3_*` settings — without an explicit S3 SECRET it falls back to the AWS
    # default credential chain and hangs on the EC2 metadata endpoint (169.254.169.254).
    # A `PROVIDER config` secret pins static MinIO credentials + endpoint so no IMDS lookup.
    endpoint = settings.s3_endpoint.split("://", 1)[-1]
    con.execute(
        f"""
        CREATE OR REPLACE SECRET minio (
            TYPE S3,
            PROVIDER config,
            KEY_ID '{settings.s3_access_key}',
            SECRET '{settings.s3_secret_key}',
            ENDPOINT '{endpoint}',
            REGION '{settings.s3_region}',
            URL_STYLE 'path',
            USE_SSL {'true' if settings.use_ssl else 'false'}
        )
        """
    )
    return con


def _table(name: str) -> str:
    return f"s3://{settings.bucket}/{GOLD}/{name}"


def main() -> int:
    con = connect()
    print("\n=== Q1: média de total_amount por mês (yellow, Jan–Mai 2023) ===")
    con.sql(
        f"SELECT pickup_year_month, trips, avg_total_amount "
        f"FROM delta_scan('{_table('agg_monthly_total_amount')}') ORDER BY pickup_year_month"
    ).show()
    print("=== Q2: média de passenger_count por hora do pickup (maio 2023) ===")
    con.sql(
        f"SELECT pickup_hour, trips, avg_passenger_count "
        f"FROM delta_scan('{_table('agg_may_passengers_by_hour')}') ORDER BY pickup_hour"
    ).show(max_rows=25)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
