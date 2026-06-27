"""Ingest NYC TLC trip parquet files into the MinIO landing zone.

The TLC publishes deterministic, date-templated URLs, so this is a parametrized,
idempotent downloader (not an HTML crawler): it skips months already present and
writes a ``.manifest.json`` (provenance) next to each file.

Usage:
    python -m src.ingestion.downloader --start 2023-01 --end 2023-05 [--dataset yellow] [--force]
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time

import requests
from botocore.exceptions import ClientError

from src.common.io import landing_key
from src.common.s3 import s3_client
from src.config import settings


def month_range(start: str, end: str) -> list[str]:
    """Inclusive list of 'YYYY-MM' from start to end."""
    sy, sm = (int(x) for x in start.split("-"))
    ey, em = (int(x) for x in end.split("-"))
    out: list[str] = []
    y, m = sy, sm
    while (y, m) <= (ey, em):
        out.append(f"{y:04d}-{m:02d}")
        m += 1
        if m > 12:
            y, m = y + 1, 1
    return out


def object_exists(s3, bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def download_with_retry(url: str, attempts: int = 4, timeout: int = 120) -> bytes:
    last: Exception | None = None
    for i in range(attempts):
        try:
            resp = requests.get(url, timeout=timeout)
            resp.raise_for_status()
            return resp.content
        except requests.RequestException as e:  # noqa: PERF203
            last = e
            wait = 2**i
            print(f"  retry {i + 1}/{attempts} after error: {e} (sleep {wait}s)", file=sys.stderr)
            time.sleep(wait)
    raise RuntimeError(f"failed to download {url}: {last}")


def ingest_month(s3, dataset: str, month: str, *, force: bool = False) -> dict:
    key = landing_key(dataset, month)
    url = f"{settings.tlc_base_url}/{dataset}_tripdata_{month}.parquet"
    if not force and object_exists(s3, settings.bucket, key):
        print(f"• {month}: already present, skipping")
        return {"month": month, "status": "skipped", "key": key}

    print(f"↓ {month}: {url}")
    data = download_with_retry(url)
    sha = hashlib.sha256(data).hexdigest()
    s3.put_object(Bucket=settings.bucket, Key=key, Body=data)

    manifest = {
        "dataset": dataset,
        "month": month,
        "url": url,
        "key": key,
        "bytes": len(data),
        "sha256": sha,
    }
    s3.put_object(
        Bucket=settings.bucket,
        Key=f"{key}.manifest.json",
        Body=json.dumps(manifest, indent=2).encode(),
    )
    print(f"  stored {len(data):,} bytes  sha256={sha[:12]}…")
    return {**manifest, "status": "ingested"}


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Ingest NYC TLC parquet into the MinIO landing zone.")
    p.add_argument("--start", default="2023-01", help="first month, YYYY-MM")
    p.add_argument("--end", default="2023-05", help="last month, YYYY-MM (inclusive)")
    p.add_argument("--dataset", default=settings.tlc_dataset, help="yellow|green|fhv|fhvhv")
    p.add_argument("--force", action="store_true", help="re-download even if present")
    args = p.parse_args(argv)

    s3 = s3_client()
    months = month_range(args.start, args.end)
    print(f"Ingesting {args.dataset} {months[0]}..{months[-1]} -> s3://{settings.bucket}/landing/")
    results = [ingest_month(s3, args.dataset, m, force=args.force) for m in months]
    ingested = sum(r["status"] == "ingested" for r in results)
    print(f"Done: {ingested} ingested, {len(results) - ingested} skipped.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
