"""Writes small JSON documents to S3/MinIO as control-plane metadata.

Pure boto3 I/O: no Airflow imports, no dataset-specific business rules.
Built for low-volume, human-readable control-plane metadata, not
high-throughput payloads — things like "which keys has this trigger
already seen" or "what did the last poll diff to", not Bronze/Silver
data itself.

The boto3 client here is built the same way `infrastructure/
s3_object_lister.py` builds one (same `MINIO_ENDPOINT` env var, same
reliance on boto3's default credential chain), reimplemented rather
than shared so each module stays a minimal, independently readable
adapter.
"""

from __future__ import annotations

import json
import os

import boto3


def _get_s3_client():
    """Builds a boto3 S3 client pointed at MinIO.

    Reads `MINIO_ENDPOINT` directly rather than defaulting it, matching
    `infrastructure/s3_object_lister.py`'s convention: every environment
    that actually runs this code (the Airflow containers) sets it
    explicitly. `region_name` is a required-but-functionally-unused
    placeholder for MinIO's S3-compatible API, which has no AWS regions.

    Returns:
        A boto3 S3 client configured for the MinIO endpoint.
    """
    endpoint_url = os.environ["MINIO_ENDPOINT"]
    return boto3.client("s3", endpoint_url=endpoint_url, region_name="us-east-1")


def write_json(bucket: str, key: str, payload: dict) -> None:
    """Writes a dict to a bucket+key as pretty-printed JSON.

    Args:
        bucket: Bucket to write to, e.g. `Buckets.RAW_INCOMING_DATA`.
        key: Full object key to write, e.g.
            `"_meta/s3_key_persister/raw-incoming-data/orders/manifest.json"`.
        payload: JSON-serializable dict to write. Serialized with
            `indent=2` for human readability, since this is
            control-plane metadata meant to be eyeballed in a bucket
            browser, not high-throughput payloads.
    """
    client = _get_s3_client()
    body = json.dumps(payload, indent=2).encode("utf-8")
    client.put_object(Bucket=bucket, Key=key, Body=body)
