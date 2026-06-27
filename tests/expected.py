"""Hand-computed expectations for tests/fixtures/yellow_sample.parquet.

Asserted by BOTH the DuckDB oracle (tests/test_oracle.py, runs anywhere) and the
PySpark transform tests (tests/test_gold.py etc., run in CI) — so the two engines must
agree on the exact numbers, cross-validating the pipeline logic.
"""

from __future__ import annotations

FIXTURE_TOTAL_ROWS = 12  # includes 1 exact duplicate + 4 dirty rows
SILVER_VALID_ROWS = 7  # after dedup (-1) and quarantine (-4)
SILVER_QUARANTINE_ROWS = 4

# Q1 — average total_amount per month: pickup_year_month -> (trips, avg_total_amount)
Q1_EXPECTED = {
    "2023-01": (2, 15.0),
    "2023-02": (1, 30.0),
    "2023-05": (4, 55.0),
}

# Q2 — avg passenger_count by pickup hour (May, passengers>0): hour -> (trips, avg)
Q2_EXPECTED = {
    8: (2, 2.0),
    9: (1, 2.0),
}
