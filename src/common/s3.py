"""boto3 S3 client + helpers, pointed at the configured (MinIO) endpoint."""

from __future__ import annotations

import boto3
from botocore.client import Config

from src.config import settings


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
        region_name=settings.s3_region,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def list_keys(prefix: str) -> list[str]:
    """All object keys under a prefix (handles pagination)."""
    s3 = s3_client()
    keys: list[str] = []
    token: str | None = None
    while True:
        kwargs = {"Bucket": settings.bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kwargs)
        keys.extend(obj["Key"] for obj in resp.get("Contents", []))
        if resp.get("IsTruncated"):
            token = resp["NextContinuationToken"]
        else:
            return keys
