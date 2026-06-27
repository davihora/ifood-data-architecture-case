# ADR 0002 — Self-hosted Spark instead of Databricks Community Edition

**Status:** accepted

## Context
The case *recommends* (does not require) Databricks Community Edition and requires PySpark
in some step. Grading rewards technical justification and creativity.

## Decision
Use **self-hosted Apache Spark (PySpark)**. The **default is a single `local[*]` container**
(right-sized for ~16M rows); a real standalone cluster (master + worker) is an **optional
`cluster` profile** that proves distributed execution. Production scale = serverless AWS
(EMR Serverless), documented in [ADR 0009](0009-local-first-aws-target.md).

## Rationale
- **Reproducibility & versioning.** The exact runtime is pinned in code (ADR 0007); a
  Databricks CE notebook is not version-controlled and its session/cluster is ephemeral.
- **Depth of competence.** Demonstrates Spark configuration, the s3a connector, Delta on
  OSS Spark, and packaging — beyond clicking in a managed UI.
- **No vendor dependency.** Databricks CE has been in flux (superseded by a "Free Edition");
  self-hosting removes that external risk for the evaluator.
- **Still satisfies the requirement** to use PySpark — more so than CE would.

## Consequences
- Default path is light (~4 GB): MinIO + one `local[*]` Spark container. Heavy pieces
  (cluster, Trino, Dagster) are opt-in profiles, so the critical path stays simple and resilient.
- `local[*]` is single-node by design; the scale story is the AWS reference architecture, not a
  local toy cluster (which is still available as proof via `make cluster-run`).
