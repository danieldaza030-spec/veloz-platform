"""Delta Lake writer for Bronze-layer quarantine records.

Pure Spark/Delta I/O: no Airflow imports, no dataset-specific business
rules. Quarantine records accumulate forever across runs and dates, so
writes are plain Delta appends rather than the `replaceWhere`-per-partition
pattern `DeltaBronzeWriter` uses for Bronze tables — a bad record detected
on a past run must not disappear when a later, unrelated date is reprocessed.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from metadata.buckets import Buckets
from metadata.quarantine_schema import QuarantineSchema


def build_quarantine_path(source: str) -> str:
    """Builds the Delta path for a source's quarantine records.

    Args:
        source: Name of the raw source being quarantined, e.g.
            `"fulfillment"`.

    Returns:
        The `s3a://` path under the shared Bronze bucket's
        `_quarantine/<source>/` prefix.
    """
    return f"s3a://{Buckets.BRONZE}/_quarantine/{source}/"


def _validate_quarantine_schema(records: DataFrame) -> DataFrame:
    """Validates a DataFrame's schema against `QuarantineSchema.RECORD`.

    Guards against a caller-supplied DataFrame silently defining the
    on-disk schema for a quarantine table that doesn't exist yet: Delta
    creates the table from whatever schema it's given on first write, so
    a name or type mismatch here must fail loudly instead of getting
    baked in permanently.

    Args:
        records: DataFrame to validate.

    Returns:
        The same DataFrame, unchanged, once validated.

    Raises:
        ValueError: If `records`'s columns don't match
            `QuarantineSchema.RECORD` — missing columns, extra columns,
            or a type mismatch on a shared column.
    """
    expected = {field.name: field.dataType for field in QuarantineSchema.RECORD.fields}
    actual = {field.name: field.dataType for field in records.schema.fields}

    missing = expected.keys() - actual.keys()
    extra = actual.keys() - expected.keys()
    mismatched = {
        name: (expected[name], actual[name])
        for name in expected.keys() & actual.keys()
        if expected[name] != actual[name]
    }

    if missing or extra or mismatched:
        problems = []
        if missing:
            problems.append(f"missing columns: {sorted(missing)}")
        if extra:
            problems.append(f"extra columns: {sorted(extra)}")
        if mismatched:
            type_diffs = ", ".join(
                f"{name} expected {expected_type}, got {actual_type}"
                for name, (expected_type, actual_type) in mismatched.items()
            )
            problems.append(f"type mismatches: {type_diffs}")
        raise ValueError(
            "records DataFrame does not match QuarantineSchema.RECORD: "
            + "; ".join(problems)
        )

    return records


def write_quarantine_records(
    spark: SparkSession,
    records: list[dict] | DataFrame,
    quarantine_path: str,
) -> int:
    """Appends quarantine records to a source's quarantine Delta table.

    Args:
        spark: Active SparkSession to build a DataFrame with, if `records`
            is a `list[dict]`.
        records: Quarantine records to write, either as a `list[dict]`
            matching `QuarantineSchema.RECORD` or an already-built
            DataFrame with that schema.
        quarantine_path: Delta table location to append into, e.g.
            `s3a://bronze-veloz/_quarantine/fulfillment/`. Build this with
            `build_quarantine_path`.

    Returns:
        Number of records written.

    Raises:
        ValueError: If `records` is a DataFrame whose schema doesn't
            match `QuarantineSchema.RECORD` (missing, extra, or
            mismatched-type columns).
    """
    if isinstance(records, DataFrame):
        quarantine_df = _validate_quarantine_schema(records)
    else:
        quarantine_df = spark.createDataFrame(records, schema=QuarantineSchema.RECORD)

    record_count = quarantine_df.count()
    quarantine_df.write.format("delta").mode("append").save(quarantine_path)

    return record_count
