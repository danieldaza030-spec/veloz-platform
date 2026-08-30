"""Shared S3/MinIO I/O helpers for the four data-source generators.

All four generators land their output in MinIO's `raw-incoming-data` bucket
(the raw landing zone for the four upstream sources — see docker-compose.yml)
instead of the local `data/raw/` folder, using the same `veloz-ingest`
(read/write/list, no delete) role every other ingest-side process in this
project already runs as. boto3 picks up AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY
from the environment automatically via its default credential chain (already
wired to that role for every Airflow container — see docker-compose.yml's
x-airflow-common); this module only has to point boto3's endpoint at MinIO
instead of real AWS, the same way dags/spark_minio_smoke_test.py points Spark
at it via spark.hadoop.fs.s3a.endpoint.

Shared here (like generators/reference_data.py) rather than duplicated across
the four generator files: each one already needs the same "build a client,
read/write an object, raise FileNotFoundError on a missing key" logic.
"""

from __future__ import annotations

import io
import os

import boto3
import pandas as pd

# Fixed by the engineer's instruction — this is the raw landing zone bucket,
# distinct from bronze-veloz/silver-veloz/gold-veloz (see docker-compose.yml).
DEFAULT_BUCKET = "raw-incoming-data"


def get_s3_client():
    """Builds a boto3 S3 client pointed at MinIO.

    MINIO_ENDPOINT is required, not defaulted — every environment that
    actually runs the generators post-migration (the Airflow containers) sets
    it explicitly, same convention as dags/spark_minio_smoke_test.py's
    os.environ["MINIO_ENDPOINT"]. region_name is a required-but-functionally-
    unused placeholder for MinIO's S3-compatible API, which doesn't have AWS
    regions.
    """
    endpoint_url = os.environ["MINIO_ENDPOINT"]
    return boto3.client("s3", endpoint_url=endpoint_url, region_name="us-east-1")


def read_csv(bucket: str, key: str, **read_csv_kwargs) -> pd.DataFrame:
    """Reads a CSV object from S3/MinIO into a DataFrame.

    Raises FileNotFoundError (not boto3/botocore's own exception types) on a
    missing key, so callers can produce the same "generate the upstream file
    first" style message this project used for local paths, without every
    caller needing to import botocore's exception classes itself.
    """
    client = get_s3_client()
    try:
        obj = client.get_object(Bucket=bucket, Key=key)
    except client.exceptions.NoSuchKey as exc:
        raise FileNotFoundError(f"No object at s3://{bucket}/{key}") from exc
    except client.exceptions.ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code")
        if error_code in ("404", "NoSuchKey", "NoSuchBucket"):
            raise FileNotFoundError(f"No object at s3://{bucket}/{key}") from exc
        raise
    return pd.read_csv(io.BytesIO(obj["Body"].read()), **read_csv_kwargs)


def write_csv(df: pd.DataFrame, bucket: str, key: str) -> None:
    """Writes a DataFrame as CSV to S3/MinIO (no index column, matching the previous local behavior)."""
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    get_s3_client().put_object(Bucket=bucket, Key=key, Body=buffer.getvalue().encode("utf-8"))


def write_text(bucket: str, key: str, text: str) -> None:
    """Writes raw text (e.g. JSON Lines, hand-built CSV) to S3/MinIO."""
    get_s3_client().put_object(Bucket=bucket, Key=key, Body=text.encode("utf-8"))
