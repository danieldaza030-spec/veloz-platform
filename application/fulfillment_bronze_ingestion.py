"""Fulfillment-specific Bronze ingestion helpers.

Fulfillment lands as one CSV per store per day and is documented as
unreliable (`docs/data-sources.md` #3): a store's whole file can be
missing for the day, and individual rows within a present file can be
mechanically corrupt. Neither failure mode exists for orders, so these
functions build fulfillment's two quarantine-record shapes on top of the
generic pieces in `application/bronze_ingestion.py` rather than forking
them.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, lit


def build_expected_key(raw_prefix: str, target_date: str, store_id: str) -> str:
    """Builds the key a store's fulfillment file is expected to land at.

    Args:
        raw_prefix: Key prefix the fulfillment feed lands under, e.g.
            `"fulfillment"`.
        target_date: Partition date, `YYYY-MM-DD`.
        store_id: Store identifier, e.g. `"STORE-004"`.

    Returns:
        The expected key, relative to the raw-incoming-data bucket, e.g.
        `"fulfillment/date=2026-08-30/STORE-004.csv"`.
    """
    return f"{raw_prefix}/date={target_date}/{store_id}.csv"


def find_missing_store_keys(
    store_ids: Sequence[str],
    present_keys: Sequence[str],
    raw_prefix: str,
    target_date: str,
) -> list[str]:
    """Diffs the expected per-store keys against what actually landed.

    Args:
        store_ids: Canonical store identifiers expected to report every
            day, e.g. `metadata.fulfillment_schema.FulfillmentSchema.STORE_IDS`.
        present_keys: Keys actually found under the day's fulfillment
            prefix, from `infrastructure.s3_object_lister.list_keys`.
        raw_prefix: Key prefix the fulfillment feed lands under.
        target_date: Partition date, `YYYY-MM-DD`.

    Returns:
        Expected keys for stores whose file did not land, sorted.
    """
    present = set(present_keys)
    return sorted(
        expected_key
        for store_id in store_ids
        if (expected_key := build_expected_key(raw_prefix, target_date, store_id)) not in present
    )


def build_missing_file_records(
    missing_keys: Sequence[str],
    source: str,
    partition_date: str,
    detected_at: datetime,
) -> list[dict]:
    """Builds one `missing_file` quarantine record per absent store file.

    Args:
        missing_keys: Expected-but-absent object keys, from
            `find_missing_store_keys`.
        source: Raw source name, e.g. `"fulfillment"`.
        partition_date: Partition date the missing files belong to.
        detected_at: When the absence was detected. Must be a plain
            `datetime.datetime` instance, not a subclass (e.g.
            `pendulum.DateTime`) — `spark.createDataFrame` verifies
            `TimestampType` values by exact type, not `isinstance`, and
            rejects subclasses.

    Returns:
        Records matching
        `metadata.quarantine_schema.QuarantineSchema.RECORD`, one per
        missing key, with `reason="missing_file"` and no raw snippet
        (nothing was read).
    """
    return [
        {
            "source": source,
            "partition_date": partition_date,
            "key": key,
            "reason": "missing_file",
            "detected_at": detected_at,
            "raw_snippet": None,
        }
        for key in missing_keys
    ]


def build_malformed_row_quarantine_df(
    corrupt_df: DataFrame,
    corrupt_record_column: str,
    source: str,
    partition_date: str,
) -> DataFrame:
    """Builds `malformed_row` quarantine records from PERMISSIVE-mode rejects.

    `key` is taken from `corrupt_df`'s `_source_file` lineage column (the
    full path Spark actually read the row from), unlike a `missing_file`
    record's `key`, which is a bucket-relative key we constructed
    ourselves since no file was ever located to reference.

    Args:
        corrupt_df: Rows PERMISSIVE mode couldn't parse into the declared
            schema, tagged with a `_source_file` column (see
            `application.bronze_ingestion.ingest_clean_rows_to_bronze`).
        corrupt_record_column: Name of the column holding each row's raw,
            unparsed content.
        source: Raw source name, e.g. `"fulfillment"`.
        partition_date: Partition date the rows belong to.

    Returns:
        A DataFrame matching
        `metadata.quarantine_schema.QuarantineSchema.RECORD`, one row per
        corrupt input row.
    """
    return corrupt_df.select(
        lit(source).alias("source"),
        lit(partition_date).alias("partition_date"),
        col("_source_file").alias("key"),
        lit("malformed_row").alias("reason"),
        current_timestamp().alias("detected_at"),
        col(corrupt_record_column).alias("raw_snippet"),
    )
