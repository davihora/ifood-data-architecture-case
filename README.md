<img width="600" height="600" alt="image" src="https://github.com/user-attachments/assets/0414dcbd-ac2a-4553-8308-98c18e34bc26" />


# iFood — Data Architecture Case (NYC Taxi Lakehouse)

End-to-end, **fully local and reproducible** lakehouse that ingests NYC TLC yellow-taxi trip
data (Jan–May 2023), models it through a **medallion** (bronze → silver → gold) architecture
on **Delta Lake**, exposes it via **SQL (DuckDB)**, and answers the two business questions.

It delivers the case's three asks end-to-end — **(1) ingestion** into the lake, **(2) SQL
consumption**, and **(3) the two analyses** — with the bronze/silver/gold transforms written in
**PySpark** (the required compute step).

> **Why not Databricks Community Edition?** The case allows "any technology of your choice" and
> grades *technical justification* and *creativity*. This solution runs the whole platform with
> one command, no cloud account, with an adversarially-validated pinned version matrix — so the
> evaluator reproduces it exactly.

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
  `colima stop && colima start --cpu 4 --memory 8`. (`make` runs a `check-mem` preflight that
  **fails fast** with this instruction if the engine exposes < 7 GB — no more mid-pipeline
  OOM-kills; bypass with `MIN_MEM_GB=0`.)

**Timing:** the **first** `make demo` builds the Spark image (one-time pull of the ~2 GB Spark
base image + jars + deps) — typically **~10–15 min, dominated by that base-image pull**.
Everything is then cached, so **subsequent runs take ~6 min** — the demo ingests the full
Jan–May 2023 window (~16 M trips) and runs the whole bronze→silver→gold pipeline in `local[*]`.
A first Colima start also downloads a small VM image once.

After `make up`: MinIO console <http://localhost:9001> (`minio` / `minio123`); the Spark UI is
on <http://localhost:4040> while a job runs.

## Architecture

![Local architecture: Makefile orchestrates a trigger → Python ingestion (Capture Raw Data) → Spark jobs (Clean → Transformation) with a Data-Quality gate over a MinIO/Delta medallion (landing → bronze → silver → gold), answered via DuckDB SQL; EDA over Silver.](docs/img/local-architecture.png)

```
TLC CDN ──(downloader: date range, idempotent, manifest)──▶ MinIO  s3a://datalake/
  landing/ (raw parquet, immutable)
     └▶ bronze/  (Delta: conformed types + audit, part. source year/month)
          └▶ silver/ (Delta: cleaned, dedup, DQ + quarantine, time dims)
               ├▶ silver_rejected/ (+ _reject_reason)   └▶ dq.check_results (metrics)
               └▶ gold/ (fact_yellow_trips [ML-ready] + Q1 mart + Q2 mart)
                    └▶ DuckDB SQL → make analyze    (consumption — lightweight)

Compute: Spark local[*] (single container).  Optional profile: `cluster` (real Spark standalone).
Production target: serverless AWS (EMR Serverless + Athena) — docs/aws-reference-architecture.md.
Quality: pytest + ruff/black/mypy + CI.
```

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
`tpep_dropoff_datetime`) are guaranteed in Silver/Gold. The cleaning policy is detailed below in
[Exploratory analysis → cleaning decisions](#exploratory-analysis--cleaning-decisions); the Q2
**"all taxis" → yellow** scoping (the `tpep_*` columns are yellow-specific) is a deliberate,
documented assumption.

## Exploratory analysis → cleaning decisions

`make eda` profiles the Silver layer and the quarantine — the loop behind every cleaning
rule is **explore → find → decide**, not "I cleaned the data". All numbers below are the
actual `make eda` output on the Jan–May 2023 load (16,186,383 raw Bronze rows).

**What the data looks like (profiling):**
- **Volume/month** (clean Silver): 3.04M · 2.89M · 3.37M · 3.26M · 3.48M = **16,041,339** trips.
- **`total_amount`**: min **0.01**, median **20.70**, mean **28.31**, max **6,304.90** — right-skewed;
  outliers are **kept** (we only drop non-positive amounts, see below).
- **`passenger_count`**: **73.5%** carry 1 passenger; **701,006 (4.4%)** are 0/NULL.

**Findings → rules (the loop):**

| EDA probe | What it found (real) | Rule → severity |
|---|---|---|
| pickup timestamp range | **104** pickups outside Jan–May 2023 (stray 2008/09, month-bleed) | `pickup_in_window` → QUARANTINE |
| `total_amount` distribution | **144,146** trips ≤ \$0 (refunds/voids/zero-fare) | `amount_positive` → QUARANTINE |
| pickup vs dropoff order | **795** trips with dropoff < pickup (impossible) | `dropoff_after_pickup` → QUARANTINE |
| required keys | **0** nulls in VendorID/pickup/dropoff/amount | `*_not_null` → QUARANTINE (guards) |
| `passenger_count` distribution | **702,146** trips with 0/NULL passengers (4.3%) | `passenger_positive` → **WARN** |

**How failing rows are treated** (`src/common/dq.py`):
- **QUARANTINE** → row is routed to `silver_yellow_trips_rejected` with a `_reject_reason`
  (audited, *not* silently dropped) and excluded from Gold.
- **WARN** → row is **kept**; only recorded as a metric. `passenger_count` is WARN because
  0-passenger trips are real revenue (valid for Q1) and only wrong for Q2 — so we keep them
  and filter `passenger_count > 0` **in the Q2 query**, not globally.
- **BLOCK** → would halt the pipeline (none configured today).
- Every check is persisted per run to `dq.check_results` (Delta, keyed by `run_id`).

**The numbers reconcile (auditable):** quarantine total = 144,146 + 795 + 104 − **1** overlap
= **145,044**; Silver = 16,186,383 − 145,044 = **16,041,339**, which equals the per-month sum
above. ~0.9% quarantined, 4.3% flagged-but-kept — far from a blind filter.

Reproduce: `make eda`.

## Validated version matrix

Spark **4.0.1** (Scala 2.13, Java 17) · Delta **4.0.0** · hadoop-aws **3.4.1** + AWS SDK v2
**2.24.6** · MinIO · DuckDB · Python 3.10 (Spark base image). Spark is pinned to **4.0.x, not
4.1**, because only 4.0.x has a verified `hadoop-aws` match — the #1 s3a stack-breaker. Jars
**and DuckDB extensions** are **baked into the image** (offline-safe).

The default path is just **MinIO + one Spark `local[*]` container + DuckDB**; the standalone
Spark cluster is an **optional profile** (so the default stays ~4 GB).

## Repository layout

```
src/ingestion/   downloader: TLC parquet -> MinIO landing (idempotent + manifest)
src/jobs/        PySpark transforms: bronze.py, silver.py, gold.py (pure fns + thin run())
src/common/      SparkSession (s3a+Delta), lake paths, S3 client, data-quality engine
src/config.py    env-driven settings (single source of truth)
analysis/        run_questions.py (DuckDB SQL), build_report.py (charts), eda.py, sql/
infra/           docker-compose, Spark image (baked jars), containerized test image
tests/           pytest (pure-Python + DuckDB oracle + PySpark) + deterministic fixture
docs/            aws-reference-architecture.md (+ diagram), RESULTS.md
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

CI ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) runs two jobs:
- **`test`** (every push/PR) — ruff + black + mypy and the full unit suite (incl. PySpark) on
  Python 3.10 + Java 17, matching the Spark container runtime.
- **`smoke-docker`** (PRs + pushes to `main`) — builds the **real** Spark image and runs the suite
  **inside it**, catching runtime issues the lint/unit job can't (e.g. `spark-submit` on PATH,
  baked Delta jars, the container's Python version).

## Optional: real Spark standalone cluster (proof of scale)

```bash
make cluster-up    # spark-master + spark-worker (real distributed Spark)
make cluster-run   # run the full pipeline on the cluster
```

## Production: AWS reference architecture

The same code maps to a **serverless AWS lakehouse** (S3 + EMR Serverless + Glue Data Catalog +
Athena + MWAA) by **config only** — see [`docs/aws-reference-architecture.md`](docs/aws-reference-architecture.md).

![AWS reference architecture: Airflow/MWAA orchestrates an EventBridge/cron trigger → Lambda ingestion (period as a parameter) → EMR Serverless Spark jobs over an S3 medallion (landing → bronze → silver → gold) → Glue Data Catalog → Athena, with a data-quality gate that alerts (Teams/Slack) on failure; Terraform provisions everything.](docs/img/aws-architecture.png)

## Make targets

`make help` lists everything: `demo`, `all`, `ingest`, `pipeline` (`bronze`/`silver`/`gold`),
`analyze`, `eda`, `report`, `cluster-up`/`cluster-run`, `test`/`test-docker`, `lint`, `clean`.

## Mapping to the case's evaluation criteria

| Criterion | Where it's addressed |
|---|---|
| **Code quality & organization** | `src/` as pure functions + thin `run()`; 3-layer test suite; ruff + black + mypy and a 2-job [CI](.github/workflows/ci.yml) (lint/unit + in-container smoke test). |
| **Exploratory analysis** | [Exploratory analysis → cleaning decisions](#exploratory-analysis--cleaning-decisions) — real `make eda` numbers driving each cleaning rule. |
| **Technical justification** | The "Why not Databricks CE" rationale, the validated version matrix, and the design decisions walked through in the presentation. |
| **Creativity** | A one-command, fully-local, offline-reproducible lakehouse (baked jars/extensions), DuckDB-over-Delta as zero-infra SQL consumption, and a config-only swap to serverless AWS. |
| **Clarity of results** | Q1/Q2 answer tables + [`docs/RESULTS.md`](docs/RESULTS.md), the AWS architecture diagram, and slide-ready charts via `make report`. |

## Notes & limitations
- Local Spark is single-node `local[*]` — right-sized for ~16M rows. Scale = serverless AWS
  (EMR Serverless + Athena), a config-only swap ([docs/aws-reference-architecture.md](docs/aws-reference-architecture.md)).
- **Memory:** the ~16M-row jobs want a Docker VM with ≥ 8 GB. `make` preflights this
  (`check-mem`); and if a job is still OOM-killed under host memory pressure, the run prints a
  clear *"out-of-memory — environment limit, not a pipeline bug"* message (exit 137) with the
  fix, instead of a cryptic error.
- Demo credentials (`minio`/`minio123`) are for local use only.
