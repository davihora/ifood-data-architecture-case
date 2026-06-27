# ADR 0009 — Local-first execution, AWS as the production target

**Status:** accepted

## Context
The case recommends Databricks Community Edition but allows "any technology of your choice"
and requires only PySpark in some step. A take-home must be **reproducible by the evaluator**
and **right-sized** to the problem (2 questions over ~16M rows / 250 MB), while still showing
production/scale awareness.

## Decision
- **Default = local single-node** Spark (`local[*]`) + Delta + MinIO, with DuckDB/Spark SQL for
  consumption. Heavy pieces (real Spark cluster, Trino+Hive, Dagster) are **opt-in profiles**.
- **Production target = a documented serverless AWS lakehouse**: S3 + **EMR Serverless** (Spark)
  + Glue Data Catalog + **Athena** (SQL) + Lambda/EventBridge (ingestion) + **MWAA** (Airflow).
  Documented as a reference architecture, **not deployed** — see
  [aws-reference-architecture.md](../aws-reference-architecture.md).

## Rationale
- **Reproducibility & cost:** local runs by `git clone` + Docker, free; Databricks/S3 needs the
  evaluator's account/credentials and money → not reproducible. (See ADR 0001/0002.)
- **Right-sizing:** the dataset is small; a single node is the appropriate engine. Permanent
  clusters would be over-engineering. Scale is demonstrated by the AWS design, not a local toy.
- **Portability by design:** `config.py`/`spark.py` centralize endpoint, credentials provider
  and master, so local→AWS is a **config swap, not a rewrite** (same PySpark jobs + Delta).

## Why EMR Serverless (compute)
Runs the same PySpark entrypoints via a Spark-submit job driver (`StartJobRun`: one `entryPoint`
per job + `sparkSubmitParameters` for `--conf`/`--jars`), with full Spark-conf control and a chosen
release — preserving code/version parity with local. Pay-per-use, no cluster to manage, fits an
intermittent monthly pull. (A managed-ETL framework would require rewriting the transforms into
its API and pin the runtime — rejected for this design.)

## Consequences
- The runnable artifact is the local stack; AWS is a credible, code-accurate design on paper.
- A Terraform/CDK skeleton (S3 + EMR Serverless + Glue Catalog + Athena + EventBridge + MWAA)
  is a planned follow-up, not included yet.
