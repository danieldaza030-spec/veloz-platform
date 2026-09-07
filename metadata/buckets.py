"""MinIO/S3 bucket names for the Veloz lakehouse."""

from __future__ import annotations

ORDERS_SILVER_PREFIX = "orders"
"""Bucket-relative path segment for the Silver orders table
(`s3a://{Buckets.SILVER}/{ORDERS_SILVER_PREFIX}/`). Shared by `dags.
ingest_orders_silver` (writes/merges into it) and `dags.
maintain_orders_silver` (OPTIMIZE/VACUUMs it) so the two DAGs, which target
the exact same physical table, can never disagree on its path. Kept as a
plain module-level constant here rather than in `metadata.
orders_silver_schema` -- that module imports `pyspark.sql.types` at its own
top level, which would drag a real pyspark import into both DAGs' module
scope (both otherwise defer every pyspark-touching import to inside their
`@task` function bodies -- see each DAG's own docstring)."""


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
