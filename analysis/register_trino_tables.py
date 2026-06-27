"""Register the Spark-written Gold Delta tables into Trino and print Q1/Q2 via SQL.

Run after `make consumption`. Trino reads the SAME MinIO objects through its native S3
connector (s3://), independently of Spark's s3a:// writes — demonstrating a real, decoupled
SQL consumption layer over the lakehouse.
"""

from __future__ import annotations

import os

import trino

HOST = os.environ.get("TRINO_HOST", "trino")
PORT = int(os.environ.get("TRINO_PORT", "8080"))
BUCKET = os.environ.get("S3_BUCKET", "datalake")
CATALOG = "delta"
SCHEMA = "gold"
TABLES = ["fact_yellow_trips", "agg_monthly_total_amount", "agg_may_passengers_by_hour"]


def _exec(cur, sql: str) -> list:
    cur.execute(sql)
    return cur.fetchall()


def main() -> int:
    conn = trino.dbapi.connect(host=HOST, port=PORT, user="analyst", catalog=CATALOG)
    cur = conn.cursor()

    # Explicit location avoids any dependency on the metastore warehouse dir.
    _exec(
        cur,
        f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA} "
        f"WITH (location = 's3://{BUCKET}/{SCHEMA}')",
    )
    for table in TABLES:
        location = f"s3://{BUCKET}/{SCHEMA}/{table}"
        _exec(cur, f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.{table}")
        _exec(
            cur,
            f"CALL {CATALOG}.system.register_table("
            f"schema_name => '{SCHEMA}', table_name => '{table}', "
            f"table_location => '{location}')",
        )
        print(f"registered {CATALOG}.{SCHEMA}.{table} -> {location}")

    print("\n=== Q1 via Trino ===")
    for row in _exec(
        cur,
        f"SELECT pickup_year_month, trips, avg_total_amount "
        f"FROM {CATALOG}.{SCHEMA}.agg_monthly_total_amount ORDER BY 1",
    ):
        print(row)

    print("\n=== Q2 via Trino ===")
    for row in _exec(
        cur,
        f"SELECT pickup_hour, trips, avg_passenger_count "
        f"FROM {CATALOG}.{SCHEMA}.agg_may_passengers_by_hour ORDER BY 1",
    ):
        print(row)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
