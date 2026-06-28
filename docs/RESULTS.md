# Results — Q1 & Q2 (yellow taxi, Jan–May 2023)

Produced by the pipeline (`make all` → `make analyze`, reading the Gold Delta marts) and
cross-checked with the project's DuckDB oracle using the **identical cleaning policy**
(`tests/test_oracle.py` proves the Spark transforms and the SQL agree on a fixture).

**Data volume:** 16,186,386 raw rows (Jan–May) → **16,041,339** after the Silver gate
(in-window pickup, `total_amount > 0`, `dropoff >= pickup`, exact-duplicate removal) =
**0.9% dropped**. The gate is deliberately conservative; `passenger_count > 0` is applied
only where it is the metric (Q2), so Q1 (revenue) is not distorted.

## Q1 — average `total_amount` per month (all yellow taxis)

| Month | Trips | **avg_total_amount (USD)** |
|---|---:|---:|
| 2023-01 | 3,040,951 | **27.45** |
| 2023-02 | 2,888,258 | **27.34** |
| 2023-03 | 3,372,941 | **28.27** |
| 2023-04 | 3,257,885 | **28.76** |
| 2023-05 | 3,481,304 | **29.46** |

Interpretation: average fare **per trip**, grouped by pickup month. A steady climb from
$27.45 (Jan) to $29.46 (May). The wording "média de valor total recebido em um mês" is
ambiguous; the alternative reading (total monthly revenue) is also available from the same
clean data: Jan ≈ $83.5M, Feb ≈ $79.0M, Mar ≈ $95.3M, Apr ≈ $93.7M, May ≈ $102.6M.

## Q2 — average `passenger_count` by pickup hour-of-day (May 2023)

| Hour | avg | Hour | avg | Hour | avg | Hour | avg |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 00 | 1.427 | 06 | 1.261 | 12 | 1.376 | 18 | 1.384 |
| 01 | 1.438 | 07 | 1.282 | 13 | 1.385 | 19 | 1.392 |
| 02 | **1.455** | 08 | 1.296 | 14 | 1.390 | 20 | 1.401 |
| 03 | 1.452 | 09 | 1.312 | 15 | 1.402 | 21 | 1.420 |
| 04 | 1.405 | 10 | 1.348 | 16 | 1.399 | 22 | 1.428 |
| 05 | 1.284 | 11 | 1.362 | 17 | 1.390 | 23 | 1.423 |

Interpretation (`passenger_count > 0`): occupancy peaks in the **late night / early morning**
(02h ≈ 1.46 — social/group trips) and bottoms out in the **morning commute** (06h ≈ 1.26 —
mostly solo riders), rising again through the evening. Classic NYC ride-occupancy curve.

> **Scope note:** Q2 says "todos os táxis da frota". We scope to **yellow** because the
> case's required columns (`tpep_*`) are yellow-specific (green taxis use `lpep_*`; FHV has no
> `passenger_count`). The pipeline parameterizes the dataset, so extending to green (which also
> has `passenger_count`) is a config change.

*Numbers above were computed from the official TLC files for Jan–May 2023; reproduce with
`make all`.*
