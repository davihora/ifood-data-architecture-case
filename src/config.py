"""Central configuration. Every value is overridable via environment variables so the
same code runs in the Docker stack (against MinIO) and locally (local[*], tests)."""

from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env(key: str, default: str) -> str:
    return os.environ.get(key, default)


@dataclass(frozen=True)
class Settings:
    # Object store (MinIO / S3-compatible)
    s3_endpoint: str = field(default_factory=lambda: _env("MINIO_ENDPOINT", "http://minio:9000"))
    s3_access_key: str = field(default_factory=lambda: _env("AWS_ACCESS_KEY_ID", "minio"))
    s3_secret_key: str = field(default_factory=lambda: _env("AWS_SECRET_ACCESS_KEY", "minio123"))
    s3_region: str = field(default_factory=lambda: _env("AWS_REGION", "us-east-1"))
    bucket: str = field(default_factory=lambda: _env("S3_BUCKET", "datalake"))

    # Spark — defaults to local[*] so plain `python job.py` and tests work everywhere.
    spark_master: str = field(default_factory=lambda: _env("SPARK_MASTER", "local[*]"))

    # Ingestion source
    tlc_dataset: str = field(default_factory=lambda: _env("TLC_DATASET", "yellow"))
    tlc_base_url: str = field(
        default_factory=lambda: _env(
            "TLC_BASE_URL", "https://d37ci6vzurychx.cloudfront.net/trip-data"
        )
    )

    @property
    def use_ssl(self) -> bool:
        return self.s3_endpoint.lower().startswith("https")

    def layer_uri(self, layer: str, *parts: str) -> str:
        """s3a:// URI for a path inside a medallion layer."""
        suffix = "/".join(p.strip("/") for p in parts if p)
        base = f"s3a://{self.bucket}/{layer}"
        return f"{base}/{suffix}" if suffix else base


settings = Settings()
