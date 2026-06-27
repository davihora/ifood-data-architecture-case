"""Generate the deterministic yellow-taxi test fixture (committed as yellow_sample.parquet).

12 rows covering every cleaning path: valid trips across Jan/Feb/May, an exact duplicate
(dedup), an out-of-window pickup, a negative amount, a null pickup, a dropoff-before-pickup,
and a zero-passenger May trip (kept for Q1, excluded from Q2). Hand-computed expectations
live in tests/expected.py and are asserted by both the DuckDB oracle and the PySpark tests.

Run:  python tests/fixtures/make_fixture.py
"""

from __future__ import annotations

import pathlib

import duckdb

OUT = pathlib.Path(__file__).with_name("yellow_sample.parquet")

ROWS = """
    (1, 1.0,  10.0, TIMESTAMP '2023-01-10 12:00:00', TIMESTAMP '2023-01-10 12:15:00', 1.0),
    (2, 3.0,  20.0, TIMESTAMP '2023-01-20 09:00:00', TIMESTAMP '2023-01-20 09:30:00', 2.0),
    (1, 2.0,  30.0, TIMESTAMP '2023-02-05 08:00:00', TIMESTAMP '2023-02-05 08:20:00', 3.0),
    (1, 1.0,  40.0, TIMESTAMP '2023-05-03 08:10:00', TIMESTAMP '2023-05-03 08:30:00', 1.5),
    (2, 3.0,  50.0, TIMESTAMP '2023-05-03 08:50:00', TIMESTAMP '2023-05-03 09:10:00', 4.0),
    (2, 2.0,  60.0, TIMESTAMP '2023-05-05 09:15:00', TIMESTAMP '2023-05-05 09:40:00', 5.0),
    (1, 0.0,  70.0, TIMESTAMP '2023-05-06 09:20:00', TIMESTAMP '2023-05-06 09:45:00', 2.0),
    (1, 1.0,  10.0, TIMESTAMP '2023-01-10 12:00:00', TIMESTAMP '2023-01-10 12:15:00', 1.0),
    (1, 5.0, 999.0, TIMESTAMP '2022-12-15 10:00:00', TIMESTAMP '2022-12-15 10:20:00', 3.0),
    (1, 1.0,  -5.0, TIMESTAMP '2023-05-04 08:00:00', TIMESTAMP '2023-05-04 08:20:00', 1.0),
    (1, 1.0,  12.0, NULL,                            TIMESTAMP '2023-01-01 00:10:00', 1.0),
    (1, 1.0,  15.0, TIMESTAMP '2023-03-01 10:00:00', TIMESTAMP '2023-03-01 09:00:00', 2.0)
"""


def build() -> pathlib.Path:
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE t (VendorID INTEGER, passenger_count DOUBLE, total_amount DOUBLE, "
        "tpep_pickup_datetime TIMESTAMP, tpep_dropoff_datetime TIMESTAMP, trip_distance DOUBLE)"
    )
    con.execute(f"INSERT INTO t VALUES {ROWS}")
    con.execute(f"COPY t TO '{OUT}' (FORMAT PARQUET)")
    n = con.execute("SELECT count(*) FROM t").fetchone()[0]
    print(f"wrote {n} rows -> {OUT}")
    return OUT


if __name__ == "__main__":
    build()
