"""Lists object keys under an S3/MinIO bucket+prefix.

Pure boto3 I/O: no Airflow imports, no dataset-specific business rules.
Built for missing-file detection ahead of Bronze ingestion — a wildcard
read of per-store CSVs silently skips whatever isn't there, so callers
that need to know exactly which store keys are absent must first get the
keys that *are* present, by name, and diff against the expected set
themselves.

The boto3 client here is built the same way `generators/s3_io.py` builds
one (same `MINIO_ENDPOINT`/`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` env
vars, same reliance on boto3's default credential chain), but
reimplemented rather than imported from `generators/` to keep
`infrastructure/` independent of the source-emulation layer — that
layer is conceptually "the outside world" this platform ingests from,
not a library this platform's own code should depend on. The small
amount of duplication this causes is accepted deliberately.
"""

from __future__ import annotations

import os

import boto3


def _get_s3_client():
    """Builds a boto3 S3 client pointed at MinIO.

    Reads `MINIO_ENDPOINT` directly rather than defaulting it, matching
    `generators/s3_io.py`'s convention: every environment that actually
    runs this code (the Airflow containers) sets it explicitly.
    `region_name` is a required-but-functionally-unused placeholder for
    MinIO's S3-compatible API, which has no AWS regions.

    Returns:
        A boto3 S3 client configured for the MinIO endpoint.
    """
    endpoint_url = os.environ["MINIO_ENDPOINT"]
    return boto3.client("s3", endpoint_url=endpoint_url, region_name="us-east-1")


def list_keys(bucket: str, prefix: str) -> list[str]:
    """Lists every object key under a bucket+prefix.

    Uses boto3's `list_objects_v2` paginator instead of a single call, so
    a prefix with more than one page of objects (>1000 keys, S3's
    per-response cap) is still listed in full rather than silently
    truncated.

    Args:
        bucket: Bucket to list, e.g. `Buckets.RAW_INCOMING_DATA`.
        prefix: Key prefix to filter on, e.g. `"fulfillment/2026-08-30/"`.

    Returns:
        Every matching object key as a plain string (not a full `s3://`
        URI), sorted for deterministic output. Empty if no keys match
        the prefix — this is the expected outcome for missing-file
        detection, not an error.
    """
    client = _get_s3_client()
    paginator = client.get_paginator("list_objects_v2")

    keys: list[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])

    return sorted(keys)
