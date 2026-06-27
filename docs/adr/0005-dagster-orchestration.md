# ADR 0005 — Dagster for orchestration

**Status:** accepted

## Context
Orchestration was not required, but bronze → silver → gold is a natural DAG and showing it
demonstrates engineering maturity. It must not become a single point of failure.

## Decision
Use **Dagster** with **software-defined assets** (one per medallion layer), as a thin layer
over the job functions. Dagster lives behind the Compose `orchestration` profile.

## Rationale
- Asset-based model maps 1:1 to bronze/silver/gold and renders lineage in the UI for free.
- Lighter dev loop (`dagster dev`) and more modern than a full Airflow deployment.
- **Decoupled from compute:** assets call `src.jobs.*.run()` — the exact same code runs via
  `make pipeline` / `spark-submit` with no Dagster installed. If the UI fails to start, the
  pipeline still runs.

## Alternatives
- **Airflow** — more universally recognized but heavier (webserver + scheduler + metadata DB).
- **Plain Makefile** — already provided as the lightest path.

## Consequences
- Data is passed between assets via Delta paths + `deps=[...]`, never Dagster I/O managers
  (Spark DataFrames are lazy/non-serializable). See ADR 0008 for the SparkSession lifecycle.
