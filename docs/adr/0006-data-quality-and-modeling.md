# ADR 0006 — Medallion modeling & data-quality policy

**Status:** accepted

## Context
TLC monthly files are dirty: timestamps outside the file's month (back to 2008/2009 and into
the next month), negative/zero `total_amount` (refunds), `passenger_count` null/0, exact
duplicates, and schema drift (INT64↔DOUBLE for `passenger_count`/`VendorID`). Cleaning can
drop ~24% of rows, so cleaned averages differ materially from raw.

## Decision
- **Landing**: original parquet, immutable (raw fidelity preserved).
- **Bronze**: read each monthly file, **cast to a canonical schema, then `unionByName`** (a
  single multi-file read can fail on the type drift — do NOT use `mergeSchema`); add audit
  columns; partition by source year/month.
- **Silver**: dedup + a DQ gate. Quarantine (not silently drop) rows that fail; persist
  per-check metrics to `dq.check_results`.
  - Gate (QUARANTINE): non-null keys, pickup in `[2023-01-01, 2023-06-01)`, `total_amount > 0`,
    `dropoff >= pickup`.
  - `passenger_count > 0` is **WARN only** at Silver — it is applied **per-question in Gold**,
    so Q1 (revenue) is not distorted by dropping otherwise-valid trips.
- **Gold**: ML-ready `fact_yellow_trips` + the two question marts.

## Q1/Q2 interpretation (documented assumptions)
- **Q1** — `avg(total_amount)` grouped by pickup month; no passenger filter.
- **Q2** — `avg(passenger_count)` by pickup hour for **May**, `passenger_count > 0`.
- "All taxis" is scoped to **yellow** (the required columns `tpep_*` are yellow-specific).

## Consequences
- A small, deliberate cleaning policy that is auditable (quarantine + metrics) and explained,
  rather than a silent filter. The DuckDB oracle and PySpark tests both pin the resulting
  numbers on a fixture.
