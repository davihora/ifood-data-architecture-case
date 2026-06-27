# Case coverage — requirement-by-requirement

Maps every point of the iFood "Data Architect" case PDF to where it is satisfied in this repo.
Legend: ✅ met · ➕ exceeded/extra · ⚠️ deliberate deviation (justified).

## Objective

| # | Asked | Status | Where |
|---|---|---|---|
| O1 | Ingest NYC taxi trip data into a Data Lake | ✅ | [`src/ingestion/downloader.py`](../src/ingestion/downloader.py) → MinIO `landing/`; [`src/jobs/bronze.py`](../src/jobs/bronze.py) |
| O2 | Make data available to consumers (e.g. via SQL) | ✅ | **DuckDB SQL** over Gold by default ([`analysis/run_questions.py`](../analysis/run_questions.py), [`analysis/sql/`](../analysis/sql)); **Trino** optional ([`analysis/register_trino_tables.py`](../analysis/register_trino_tables.py)) |
| O3 | Run analyses and show results | ✅➕ | [`docs/RESULTS.md`](RESULTS.md) (real Jan–May numbers) + [`analysis/eda.py`](../analysis/eda.py) |

## Data

| # | Asked | Status | Where |
|---|---|---|---|
| D1 | Source = TLC site | ✅ | `TLC_BASE_URL` (official CDN) in [`src/config.py`](../src/config.py) |
| D2 | Store & expose **Jan–May 2023** | ✅ | default window `2023-01..2023-05` (`make all`) |

## Considerations

| # | Asked | Status | Where |
|---|---|---|---|
| C1 | Landing zone with **original files** | ✅ | `landing/yellow/year=/month=/*.parquet` (immutable) + `*.manifest.json` provenance |
| C2 | Consumption layer with structured/transformed data | ✅ | Delta `silver` + `gold` |
| C3 | Clean/manipulate data as needed | ✅➕ | Silver DQ gate + quarantine + `dq.check_results` metrics ([ADR 0006](adr/0006-data-quality-and-modeling.md)) |
| C4 | **Guarantee columns** VendorID, passenger_count, total_amount, tpep_pickup_datetime, tpep_dropoff_datetime in consumption | ✅ | `KEEP_TYPES`/`FACT_COLUMNS` ([bronze](../src/jobs/bronze.py), [gold](../src/jobs/gold.py)); asserted by `tests/test_gold.py::test_fact_has_required_columns` (others ignored, `trip_distance` kept for EDA) |
| C5 | Tables must be modeled & created (lake starts empty) | ✅ | jobs create Delta tables bronze/silver/gold |

## The Challenge — Part 1 (solution)

| # | Asked | Status | Where |
|---|---|---|---|
| P1.1 | Read raw → ingest into lake → expose to end users | ✅ | downloader → bronze → silver → gold → Trino/Spark |
| P1.2 | **Must use PySpark in some step** | ✅ | all transforms are PySpark ([`src/jobs/*`](../src/jobs)) |
| P1.3 | Databricks Community Edition *recommended* | ⚠️ | **Self-hosted Spark 4.0.1** instead — reproducible, version-pinned, no vendor lock; justified in [ADR 0002](adr/0002-self-hosted-spark-vs-databricks-ce.md). (Recommendation, not a requirement; PySpark requirement still met.) |
| P1.4 | Metadata technology = your choice | ✅ | **Hive Metastore** + Delta transaction log ([ADR 0004](adr/0004-trino-sql-consumption.md)) |
| P1.5 | Query language = your choice | ✅ | SQL (Trino) + PySpark; both provided |

## The Challenge — Part 2 (the two questions)

**Q1 — "média de valor total (total_amount) recebido em um mês, todos os yellow táxis"**

- Implementation: `gold.agg_monthly_total_amount` ([gold.py](../src/jobs/gold.py) `q1_monthly_avg_amount`); SQL in [`analysis/sql/q1.sql`](../analysis/sql/q1.sql).
- ✅ Result (per-trip avg by month): Jan 27.45 · Feb 27.34 · Mar 28.27 · Apr 28.76 · May 29.46 — full table + the alternative "monthly revenue" reading in [`docs/RESULTS.md`](RESULTS.md).

**Q2 — "média de passageiros por hora do dia, maio, todos os táxis da frota"**

- Implementation: `gold.agg_may_passengers_by_hour` ([gold.py](../src/jobs/gold.py) `q2_may_passengers_by_hour`); SQL in [`analysis/sql/q2.sql`](../analysis/sql/q2.sql). Pickup hour, `passenger_count > 0`.
- ✅ Result: 24-hour table in [`docs/RESULTS.md`](RESULTS.md) (peak 02h ≈ 1.46, trough 06h ≈ 1.26).
- ⚠️ **Scope:** "todos os táxis" is scoped to **yellow** (required columns `tpep_*` are yellow-specific; green = `lpep_*`, FHV has no `passenger_count`). The dataset is parameterized, so green can be added via config; documented in [ADR 0006](adr/0006-data-quality-and-modeling.md).

## Repository structure

| Asked | Status | Notes |
|---|---|---|
| `src/` | ✅ | `ingestion/`, `jobs/`, `common/`, `config.py` |
| `analysis/` | ✅ | `run_questions.py`, `register_trino_tables.py`, `eda.py`, `sql/` |
| `README.md` | ✅ | run instructions + architecture + results |
| `requirements.txt` | ✅ | + `requirements-dev.txt`, `orchestration/requirements.txt` |
| (extra) | ➕ | `infra/`, `orchestration/`, `tests/`, `docs/adr/`, `.github/workflows/` |

## Evaluation criteria

| Criterion | How addressed |
|---|---|
| Code quality & organization | ruff + black + mypy clean; vulture (no dead code); CI; pure functions + thin runners |
| Exploratory analysis process | [`analysis/eda.py`](../analysis/eda.py) + cleaning rationale in [ADR 0006](adr/0006-data-quality-and-modeling.md) + [`docs/RESULTS.md`](RESULTS.md) |
| Justification of technical choices | **9 ADRs** in [`docs/adr/`](adr) |
| Creativity | MinIO + self-hosted Spark + Delta + Trino + Dagster, fully local; DuckDB oracle cross-check; quarantine + DQ metrics |
| Clarity of communication | README + [architecture diagram](architecture.md) + results tables |

## Delivery

| Asked | Status |
|---|---|
| Create GitHub repo | ⏳ pending (awaiting personal git credentials; no commit made yet) |
| Develop solution | ✅ |
| Update README with run instructions | ✅ |
| Send repo link | ⏳ pending |
