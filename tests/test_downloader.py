"""Pure-Python tests for the ingestion downloader (fake S3, no network/Spark)."""

from __future__ import annotations

import json

from botocore.exceptions import ClientError

from src.common.io import landing_key
from src.ingestion import downloader


def test_month_range_simple():
    assert downloader.month_range("2023-01", "2023-05") == [
        "2023-01",
        "2023-02",
        "2023-03",
        "2023-04",
        "2023-05",
    ]


def test_month_range_crosses_year():
    assert downloader.month_range("2022-11", "2023-02") == [
        "2022-11",
        "2022-12",
        "2023-01",
        "2023-02",
    ]


def test_month_range_single():
    assert downloader.month_range("2023-03", "2023-03") == ["2023-03"]


class FakeS3:
    def __init__(self, existing=()):
        self.existing = set(existing)
        self.puts: dict[str, bytes] = {}

    def head_object(self, **kwargs):
        if kwargs["Key"] in self.existing:
            return {}
        raise ClientError({"Error": {"Code": "404"}}, "HeadObject")

    def put_object(self, **kwargs):
        self.puts[kwargs["Key"]] = kwargs["Body"]


def test_ingest_month_skips_when_present():
    key = landing_key("yellow", "2023-01")
    s3 = FakeS3(existing=[key])
    res = downloader.ingest_month(s3, "yellow", "2023-01")
    assert res["status"] == "skipped"
    assert s3.puts == {}


def test_ingest_month_downloads_and_writes_manifest(monkeypatch):
    s3 = FakeS3()
    monkeypatch.setattr(downloader, "download_with_retry", lambda url: b"PARQUETDATA")
    res = downloader.ingest_month(s3, "yellow", "2023-02")
    key = landing_key("yellow", "2023-02")

    assert res["status"] == "ingested"
    assert res["bytes"] == len(b"PARQUETDATA")
    assert key in s3.puts
    manifest = json.loads(s3.puts[f"{key}.manifest.json"].decode())
    assert manifest["month"] == "2023-02"
    assert manifest["dataset"] == "yellow"
    assert len(manifest["sha256"]) == 64
