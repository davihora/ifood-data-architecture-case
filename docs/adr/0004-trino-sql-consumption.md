# ADR 0004 — Trino as the SQL consumption layer (optional profile)

**Status:** accepted

## Context
The case asks to expose the data "for users to consume (via SQL, for example)". We want a
production-like SQL experience without making the core demo fragile.

## Decision
Provide **Trino** (with a Hive Metastore) over the Gold Delta tables, behind an optional
Compose **`consumption` profile**. The default `make demo` answers Q1/Q2 via **DuckDB SQL** over
the Gold Delta tables (lightweight, reads MinIO directly), so the case answers never depend on
the heaviest containers.

## Rationale
- Trino is the de-facto SQL engine for lakehouses; it shows how a consumer really queries the
  lake, decoupled from the writer (Trino reads via native S3 `s3://`, Spark writes via `s3a://`).
- Putting it behind a profile keeps the critical path (the answers) resilient: HMS schema init
  is the most failure-prone piece, so it must not gate the deliverable.

## Consequences
- Trino 481 removed legacy `hive.s3.*` config — native S3 (`fs.s3.enabled`) is used instead.
- A standalone Hive Metastore + Postgres is required (the `delta_lake` connector has no
  file/REST metastore option). Pre-existing Spark tables are surfaced with `register_table`.
