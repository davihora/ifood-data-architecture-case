# ADR 0007 — Pinned, coherence-checked version matrix

**Status:** accepted

## Context
A self-hosted Spark + Delta + s3a + Trino + Dagster stack is extremely sensitive to version
drift. The single biggest failure mode is the s3a connector: `hadoop-aws` must EXACTLY match
the Hadoop version bundled in Spark, with the matching AWS SDK major.

## Decision
Pin the following coherent matrix (validated against current docs):

| Component | Version | Note |
|---|---|---|
| Apache Spark | 4.0.1 | Scala 2.13, Java 17 |
| Delta Lake (delta-spark) | 4.0.0 | jar `io.delta:delta-spark_2.13:4.0.0` |
| pyspark (pip, tests/CI) | 4.0.1 | matches the JVM build |
| Hadoop (bundled in Spark) | 3.4.1 | the version s3a must match |
| hadoop-aws | 3.4.1 | == bundled Hadoop |
| AWS SDK v2 bundle | 2.24.6 | Hadoop 3.4.x uses SDK v2 (not v1) |
| Trino | 481 | native S3 (`fs.s3.enabled`); legacy `hive.s3.*` removed |
| Hive Metastore / Postgres | 4.0.0 / 15 | required by the delta_lake connector |
| Dagster | 1.13.11 | webserver version-locked to core |
| Python | 3.11 | satisfies delta-spark and Dagster |

**Spark is pinned to 4.0.x, NOT 4.1** — only 4.0.x has an independently verified `hadoop-aws`
match (3.4.1 + SDK v2 2.24.6). Pinning 4.1 would mean guessing its bundled Hadoop and risking
`NoSuchMethodError`/`NoClassDefFoundError` on the first s3a call.

## Footguns encoded in the build
- Jars are **baked into the Spark image** (no `--packages` at runtime → works offline).
- MinIO needs `path.style.access=true` + plain HTTP, set consistently in Spark, HMS and Trino.
- Cross-engine Delta: keep table protocol at **default features** so Trino 481 can read
  Spark-written tables (no deletion vectors / column-mapping-id / v2 checkpoints).
- Scala is uniformly **2.13** (no 2.12 build exists for Spark 4.x / Delta 4.x).
