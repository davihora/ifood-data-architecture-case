# ADR 0001 — MinIO as the object store (instead of real AWS S3)

**Status:** accepted

## Context
The case allows the landing/consumption zones to be "an S3 bucket or any other technology
of your choice". The solution must be reproducible by the evaluator with minimal friction.

## Decision
Use **MinIO**, an S3-API-compatible object store, running locally in Docker.

## Rationale
- **Same code, any backend.** Spark talks to it via the `s3a://` connector and the app via
  `boto3` — identical to real S3. Switching to AWS S3 in production is just an endpoint +
  credentials change, with zero code changes. This proves we understand the S3 abstraction
  rather than avoiding the cloud.
- **Reproducibility.** No AWS account, no credentials, no cost. `make demo` works offline.
- **Self-contained grading.** The evaluator runs one command instead of provisioning cloud.

## Consequences
- Local MinIO is single-node (not HA) — fine for the exercise; production would use real S3
  or a MinIO cluster.
- MinIO requires S3 **path-style access** and plain-HTTP endpoints; this is configured
  consistently across Spark, Hive Metastore and Trino (see ADR 0007 footguns).
