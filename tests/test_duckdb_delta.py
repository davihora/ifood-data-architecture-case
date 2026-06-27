"""Validate the consumption protocol seam: DuckDB's `delta` extension must read tables written
by Spark/Delta 4.0. Writes a tiny Delta table with delta-spark, reads it back via `delta_scan`
(local path — no MinIO needed). Run in CI / `make test-docker` (needs Spark + the delta ext).
"""

from __future__ import annotations

import os

import pytest

pytestmark = pytest.mark.spark


def _duckdb_with_delta():
    import duckdb

    con = duckdb.connect()
    ext_dir = os.environ.get("DUCKDB_EXTENSION_DIRECTORY")
    if ext_dir:
        con.execute(f"SET extension_directory='{ext_dir}'")
    try:
        con.execute("LOAD delta")
    except Exception:  # not baked (local dev) -> fetch from CDN
        con.execute("INSTALL delta; LOAD delta")
    return con


def test_duckdb_reads_spark_written_delta(spark, tmp_path):
    path = str(tmp_path / "t")
    (
        spark.createDataFrame([(1, 10.0), (2, 20.0)], "id int, amt double")
        .write.format("delta")
        .mode("overwrite")
        .save(path)
    )

    con = _duckdb_with_delta()
    rows = con.execute(
        f"SELECT count(*) AS c, round(avg(amt), 1) AS a FROM delta_scan('{path}')"
    ).fetchone()
    assert rows == (2, 15.0)
