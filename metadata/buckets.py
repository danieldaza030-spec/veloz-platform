"""MinIO/S3 bucket names for the Veloz lakehouse."""

from __future__ import annotations


class Buckets:
    """Bucket name constants for the Veloz lakehouse."""

    RAW_INCOMING_DATA = "raw-incoming-data"
    """Landing bucket for the 4 generators, created by `minio-init` in
    `docker-compose.yml`. Hyphenated, not the originally requested
    `raw_incoming_data` (underscore) — MinIO/S3 reject underscored bucket
    names outright. Mirrors `DEFAULT_BUCKET` in `generators/s3_io.py` and
    `RAW_BUCKET` in each `dags/generate_*.py`, which are not imported from
    here (each generator/DAG is self-contained, no shared module)."""

    BRONZE = "bronze-veloz"
    """Bronze layer bucket, created by `minio-init` in `docker-compose.yml`."""

    SILVER = "silver-veloz"
    """Silver layer bucket, created by `minio-init` in `docker-compose.yml`."""

    GOLD = "gold-veloz"
    """Gold layer bucket, created by `minio-init` in `docker-compose.yml`."""
