<img width="600" height="600" alt="image" src="https://github.com/user-attachments/assets/0414dcbd-ac2a-4553-8308-98c18e34bc26" />


# iFood — Data Architecture Case (NYC Taxi Lakehouse)

End-to-end, **fully local and reproducible** lakehouse that ingests NYC TLC yellow-taxi trip
data (Jan–May 2023), models it through a **medallion** (bronze → silver → gold) architecture
on **Delta Lake**, exposes it via **SQL (DuckDB by default; Trino optional)**, and answers the
two business questions.

> **Why not Databricks Community Edition?** The case allows "any technology of your choice" and
> grades *technical justification* and *creativity*. This solution runs the whole platform with
> one command, no cloud account, with an adversarially-validated pinned version matrix — so the
> evaluator reproduces it exactly. Every decision is recorded in [`docs/adr/`](docs/adr).

## TL;DR — run it

```bash
make demo      # build → ingest Jan–May 2023 → run pipeline → print Q1 & Q2
make all       # same, with an overridable window (START=YYYY-MM END=YYYY-MM)
make test      # unit tests on a local SparkSession (no Docker)
```

**Prerequisites (host):** everything else (Java, Spark, Python, jars) lives in the images, so
the host only needs:
- **`make`** — macOS: `xcode-select --install` (Command Line Tools); Debian/Ubuntu: `apt install make`.
- **A Docker engine** — Docker Desktop **or** Colima (`brew install colima`). You don't start it
  yourself: `make` runs a `check-docker` step that **auto-starts** Colima (`--cpu 4 --memory 8`)
  or Docker Desktop if the daemon is down (disable with `SKIP_DOCKER_AUTOSTART=1`).
- **~8 GB free RAM** for the VM and **outbound network** (TLC data + first-build deps). Works on
  Apple Silicon **and** Intel Macs (the images are multi-arch, no emulation).
- Already running Colima? Make sure it has ≥8 GB — `make` won't resize a live VM:
  `colima stop && colima start --cpu 4 --memory 8`.

**Timing:** the **first** `make demo` builds the Spark image (one-time pull of the ~2 GB Spark
base image + jars + deps) — typically **~10–15 min, dominated by that base-image pull**.
Everything is then cached, so **subsequent runs take ~6 min** — the demo ingests the full
Jan–May 2023 window (~16 M trips) and runs the whole bronze→silver→gold pipeline in `local[*]`.
A first Colima start also downloads a small VM image once.

After `make up`: MinIO console <http://localhost:9001> (`minio` / `minio123`); the Spark UI is
on <http://localhost:4040> while a job runs.

## Architecture

```
TLC CDN ──(downloader: date range, idempotent, manifest)──▶ MinIO  s3a://datalake/
  landing/ (raw parquet, immutable)
     └▶ bronze/  (Delta: conformed types + audit, part. source year/month)
          └▶ silver/ (Delta: cleaned, dedup, DQ + quarantine, time dims)
               ├▶ silver_rejected/ (+ _reject_reason)   └▶ dq.check_results (metrics)
               └▶ gold/ (fact_yellow_trips [ML-ready] + Q1 mart + Q2 mart)
                    └▶ DuckDB SQL → make analyze    (default consumption — lightweight)

Compute: Spark local[*] (single container).  Optional profiles: `cluster` (real Spark
standalone), `consumption` (Trino + Hive), `orchestration` (Dagster).
Production target: serverless AWS (EMR Serverless + Athena) — docs/aws-reference-architecture.md.
Quality: pytest + ruff/black/mypy + CI.
```

Full diagram + data model: [`docs/architecture.md`](docs/architecture.md).

## The two questions — answers

Computed on the official TLC files (Jan–May 2023); full tables in [`docs/RESULTS.md`](docs/RESULTS.md).

**Q1 — avg `total_amount` per month** (yellow) → `gold.agg_monthly_total_amount`

| 2023-01 | 2023-02 | 2023-03 | 2023-04 | 2023-05 |
|---:|---:|---:|---:|---:|
| 27.45 | 27.34 | 28.27 | 28.76 | 29.46 |

**Q2 — avg `passenger_count` by pickup hour** (May, `passenger_count > 0`) → `gold.agg_may_passengers_by_hour`
— ranges **1.26** (06h, morning commute) to **1.46** (02h, late-night); 24-hour table in [`docs/RESULTS.md`](docs/RESULTS.md).

`make analyze` prints both; SQL in [`analysis/sql/`](analysis/sql). The 5 required consumption
columns (`VendorID`, `passenger_count`, `total_amount`, `tpep_pickup_datetime`,
`tpep_dropoff_datetime`) are guaranteed in Silver/Gold. Cleaning policy, the Q1 "monthly-revenue"
alternative, and the Q2 **"all taxis" → yellow** scoping are in
[ADR 0006](docs/adr/0006-data-quality-and-modeling.md). Full point-by-point case mapping:
[`docs/CASE_COVERAGE.md`](docs/CASE_COVERAGE.md).

## Validated version matrix (see [ADR 0007](docs/adr/0007-pinned-version-matrix.md))

Spark **4.0.1** (Scala 2.13, Java 17) · Delta **4.0.0** · hadoop-aws **3.4.1** + AWS SDK v2
**2.24.6** · MinIO · Trino **481** (native S3) · Hive Metastore 4.0.0 · Dagster 1.13.11 ·
Python 3.11. Spark is pinned to **4.0.x, not 4.1**, because only 4.0.x has a verified
`hadoop-aws` match — the #1 s3a stack-breaker. Jars **and DuckDB extensions** are **baked into
the image** (offline-safe).

The default path is just **MinIO + one Spark `local[*]` container + DuckDB**; Trino, Dagster
and the standalone Spark cluster are **optional profiles** (so the default stays ~4 GB).

## Repository layout

```
src/ingestion/   downloader: TLC parquet -> MinIO landing (idempotent + manifest)
src/jobs/        PySpark transforms: bronze.py, silver.py, gold.py (pure fns + thin run())
src/common/      SparkSession (s3a+Delta), lake paths, S3 client, data-quality engine
src/config.py    env-driven settings (single source of truth)
orchestration/   Dagster assets wiring the medallion
analysis/        run_questions.py (DuckDB SQL), register_trino_tables.py, eda.py, sql/
infra/           docker-compose, Spark image (baked jars), Trino catalog, Dagster/test images
tests/           pytest (pure-Python + DuckDB oracle + PySpark) + deterministic fixture
docs/            adr/ (9 ADRs), architecture.md, aws-reference-architecture.md, RESULTS.md, CASE_COVERAGE.md
```

## Testing

`make test` runs unit tests on a local SparkSession. The suite has three layers:
- **Pure-Python** (`test_io_config`, `test_downloader`) — paths, config, idempotent ingestion.
- **DuckDB oracle** (`test_oracle`) — recomputes the Silver cleaning + Q1/Q2 on a deterministic
  fixture and asserts the same hand-computed numbers the Spark pipeline must produce (a JVM-free
  cross-check of the analytics logic).
- **PySpark** (`test_dq`, `test_bronze`, `test_silver`, `test_gold`) — the actual transforms;
  Q1/Q2 are asserted against the shared expectations in `tests/expected.py`.

**Zero local setup:** `make test-docker` runs the **full** suite (incl. PySpark, against the
baked Delta jars — no network) inside a container; the host needs only Docker. `make test`
is the same suite using a local Python+Java toolchain.

CI ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) runs ruff + black + mypy and the
full suite (incl. PySpark) on Python 3.11 + Java 17.

## Optional: production-like SQL via Trino

```bash
make consumption   # starts Hive Metastore + Trino and registers the Gold Delta tables
make trino-cli     # then: SELECT * FROM delta.gold.agg_monthly_total_amount ORDER BY 1;
```

## Optional: orchestration UI

```bash
make dagster       # Dagster UI at http://localhost:3000 — asset graph + lineage + run
```

## Optional: real Spark standalone cluster (proof of scale)

```bash
make cluster-up    # spark-master + spark-worker (real distributed Spark)
make cluster-run   # run the full pipeline on the cluster
```

## Production: AWS reference architecture

The same code maps to a **serverless AWS lakehouse** (S3 + EMR Serverless + Glue Data Catalog +
Athena + MWAA) by **config only** — see [`docs/aws-reference-architecture.md`](docs/aws-reference-architecture.md)
and [ADR 0009](docs/adr/0009-local-first-aws-target.md).

## Make targets

`make help` lists everything: `demo`, `all`, `ingest`, `pipeline` (`bronze`/`silver`/`gold`),
`analyze`, `eda`, `cluster-up`/`cluster-run`, `consumption`, `dagster`, `test`/`test-docker`,
`lint`, `clean`.

## Notes & limitations
- Local Spark is single-node `local[*]` — right-sized for ~16M rows. Scale = serverless AWS
  (EMR Serverless + Athena), a config-only swap ([docs/aws-reference-architecture.md](docs/aws-reference-architecture.md)).
- The Hive Metastore (consumption profile) is the most environment-sensitive container; the
  default answer path uses **DuckDB over Gold** and does not depend on it.
- Demo credentials (`minio`/`minio123`) are for local use only.
