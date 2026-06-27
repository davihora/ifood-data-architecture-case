# ADR 0003 — Delta Lake as the table format

**Status:** accepted

## Context
The Data Lake has no tables; they must be modeled and created. We need ACID, schema
control, and idempotent (re-runnable) jobs over object storage.

## Decision
Use **Delta Lake (OSS)** for the Bronze/Silver/Gold tables.

## Rationale
- ACID transactions and schema enforcement/evolution on top of Parquet in MinIO.
- Idempotent writes (`overwrite`/`MERGE`), time travel, and `OPTIMIZE` available.
- First-class pairing with Spark; readable by Trino's `delta_lake` connector for the SQL
  consumption layer.

## Alternatives
- **Apache Iceberg** — equally strong, more engine-agnostic; chosen against only for tighter
  Spark cohesion and simplicity here. A reasonable future swap.

## Consequences
- Cross-engine reads (Trino) require keeping table protocol at **default features** (no
  deletion vectors / column-mapping-id / v2 checkpoints) — see ADR 0007.
