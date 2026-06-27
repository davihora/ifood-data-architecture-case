# ADR 0008 — SparkSession lifecycle in Dagster & cross-engine Delta protocol

**Status:** accepted

## Context
Two subtle, well-documented pitfalls in this exact stack can cause leaks or silent data loss.

## Decision & guidance
1. **SparkSession lifecycle in Dagster.** `dagster-pyspark`'s built-in `PySparkResource`
   calls `getOrCreate()` but never `.stop()` the session, leaking JVM drivers across runs.
   We therefore call `get_spark()` inside each asset (one session per run/process). If a
   `PySparkResource` is ever used, subclass it to add `teardown_after_execution` that stops
   the session. Never move Spark DataFrames through Dagster I/O managers (not serializable);
   pass data via Delta paths and order with `deps=[...]`.

2. **Cross-engine Delta protocol (highest residual risk).** Trino 481 reads Spark-4.0/Delta-4.0
   tables only at **default table-protocol features**. Enabling deletion vectors, column
   mapping (id mode), or v2 checkpoints on the Spark side can make Trino reads fail or
   silently skip data. We keep table features at defaults and do not enable those options.

## Consequences
- Predictable resource usage and correct cross-engine reads.
- If advanced Delta features are needed later, verify each against Trino's supported-features
  list before enabling.
