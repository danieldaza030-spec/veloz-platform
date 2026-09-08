"""Idempotent Delta Lake MERGE writer for the Silver orders table.

Pure Spark/Delta I/O: no Airflow imports, no dataset-specific ingestion
orchestration -- `application.orders_silver_ingestion` wires this together
with the Bronze read and `application.orders_silver_dedup`'s collapse.
This module owns exactly one thing: writing an already-collapsed batch
(one row per `order_id`) into Silver via a single Delta MERGE, so a
cross-batch update follows the same per-column sticky/latest-wins rules
`metadata.orders_silver_schema` documents.

The MERGE condition is `target.order_id = source.order_id AND
target.created_date BETWEEN <batch min> AND <batch max>`, with the bounds
computed from the incoming batch at runtime. `created_date` never changes
for a given `order_id` (it's derived from `created_at`), so this predicate
can never produce a false-negative match or a duplicate insert -- every
order in the batch has a `created_date` inside its own batch's
[min, max] by construction. It exists purely for performance: Delta's
data-skipping can prune target files by partition before the
`order_id` equality check runs, instead of scanning every `created_date`
partition on every merge regardless of Z-ORDER.

That "cannot produce a false-negative match" guarantee is coupled to an
assumption this module does not itself enforce: `created_at` is a
`STICKY_COLUMNS` entry (see `metadata.orders_silver_schema`), which is
technically overwritable by a later, differently-valued batch, and
`created_date` is deliberately excluded from `build_update_set()`'s
mapping (a matched row's stored `created_date` is never touched by any
MERGE, regardless of what the incoming batch's own `created_date` says).
If `created_at`/`created_date` ever did drift for an `order_id` across
batches, a later batch's `[batch_min, batch_max]` -- computed from that
batch's own rows, not the target's already-stored value -- could exclude
the target's actual `created_date` and miss the match entirely, silently
falling through to `whenNotMatchedInsert` and creating a duplicate
`order_id` row instead of updating the existing one. This module's
correctness therefore depends on the (unenforced, but true for this
platform's order lifecycle) invariant that `created_at` never changes
once set -- see `tests/test_delta_silver_merge_writer.py::
TestCreatedDateStability` for the regression coverage.

The table is created partitioned by `created_date` ONLY -- not
`store_id`, `_extract_date`, or `_ingestion_window`, all of which can
change or repeat across a single order's lifecycle. Partitioning by any
of those would make a row physically migrate between partitions as it
updates, which Delta's MERGE does not do safely.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from metadata.orders_silver_schema import (
    BRONZE_LINEAGE_COLUMNS,
    LATEST_WINS_COLUMNS,
    PARTITION_COLUMN,
    SILVER_FIRST_SEEN_AT_COLUMN,
    SILVER_INGESTED_AT_COLUMN,
    STICKY_COLUMNS,
)

UPDATED_AT_COLUMN = "updated_at"

# Table properties for a newly created Silver table. `enableDeletionVectors`
# lets MERGE mark deleted/updated rows without rewriting whole Parquet
# files -- valuable at this table's 5M-orders/day update volume.
# `autoOptimize.optimizeWrite`/`autoOptimize.autoCompact` are both real
# OSS delta-spark 3.2.1 table properties (confirmed against the
# `DeltaConfigsBase` class shipped in this project's pinned
# `io.delta:delta-spark_2.12:3.2.1` jar, which defines both
# `autoOptimize.optimizeWrite` and `autoOptimize.autoCompact` -- these are
# not Databricks-only): they keep MERGE's small per-batch writes from
# accumulating into many small files between compaction runs, without
# needing a human to schedule and babysit `OPTIMIZE` by hand.
TABLE_PROPERTIES: dict[str, str] = {
    "delta.enableDeletionVectors": "true",
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true",
}


def _newer_condition() -> Column:
    """The freshness test every per-column MERGE expression below keys off of."""
    return F.col("source." + UPDATED_AT_COLUMN) > F.col("target." + UPDATED_AT_COLUMN)


def _sticky_expr(newer: Column, column: str) -> Column:
    """Builds a sticky column's `whenMatchedUpdate` expression.

    A newer batch's value wins, but falls back to the existing target
    value when the newer batch's own value is null -- a late-arriving,
    older-`updated_at` batch can still fill a gap the current target row
    has, but a newer batch can never regress an already-set sticky value
    back to null.
    """
    source_column = F.col("source." + column)
    target_column = F.col("target." + column)
    return F.when(newer, F.coalesce(source_column, target_column)).otherwise(
        F.coalesce(target_column, source_column)
    )


def _latest_wins_expr(newer: Column, column: str) -> Column:
    """Builds a latest-wins column's `whenMatchedUpdate` expression: newer batch's value, no fallback."""
    return F.when(newer, F.col("source." + column)).otherwise(F.col("target." + column))


def build_update_set() -> dict[str, Column]:
    """Builds the single `whenMatchedUpdate` per-column `set` mapping.

    One `CASE`-equivalent expression per column, so the MERGE has exactly
    one `whenMatchedUpdate` clause rather than two guarded clauses that
    could each match ambiguously.

    Returns:
        A `{column_name: Column}` mapping covering every `STICKY_COLUMNS`
        and `LATEST_WINS_COLUMNS` entry (`updated_at` handled separately,
        via `greatest`, since it's the freshness signal itself and must
        never move backwards), `BRONZE_LINEAGE_COLUMNS` (treated like
        latest-wins -- they should describe the most recent update), and
        the two `_silver_*` audit columns.
    """
    newer = _newer_condition()
    update_set: dict[str, Column] = {}

    for column in STICKY_COLUMNS:
        update_set[column] = _sticky_expr(newer, column)

    for column in LATEST_WINS_COLUMNS:
        if column == UPDATED_AT_COLUMN:
            continue
        update_set[column] = _latest_wins_expr(newer, column)

    for column in BRONZE_LINEAGE_COLUMNS:
        update_set[column] = _latest_wins_expr(newer, column)

    update_set[UPDATED_AT_COLUMN] = F.greatest(
        F.col("source." + UPDATED_AT_COLUMN), F.col("target." + UPDATED_AT_COLUMN)
    )
    update_set[SILVER_INGESTED_AT_COLUMN] = F.current_timestamp()
    update_set[SILVER_FIRST_SEEN_AT_COLUMN] = F.col("target." + SILVER_FIRST_SEEN_AT_COLUMN)

    return update_set


def build_insert_values(source_columns: list[str]) -> dict[str, Column]:
    """Builds the `whenNotMatchedInsert` `values` mapping.

    Args:
        source_columns: Column names present on the collapsed batch
            (`application.orders_silver_dedup.collapse_bronze_batch`'s
            output) -- every one of them is a plain pass-through from
            `source` on first insert.

    Returns:
        A `{column_name: Column}` mapping: every `source_columns` entry
        taken as-is, plus both `_silver_*` audit columns set to the same
        `current_timestamp()` call -- a brand-new row was first seen
        exactly when it was ingested.
    """
    insert_values = {column: F.col("source." + column) for column in source_columns}
    now = F.current_timestamp()
    insert_values[SILVER_INGESTED_AT_COLUMN] = now
    insert_values[SILVER_FIRST_SEEN_AT_COLUMN] = now
    return insert_values


def _column_ddl(field) -> str:
    """Renders one `StructField` as a backtick-quoted `CREATE TABLE` column clause."""
    not_null = "" if field.nullable else " NOT NULL"
    return f"`{field.name}` {field.dataType.simpleString()}{not_null}"


def _ensure_table_exists(spark: SparkSession, path: str, schema: StructType) -> None:
    """Creates the Silver Delta table at `path` if it doesn't already exist.

    Uses a raw `CREATE TABLE IF NOT EXISTS delta.\\`<path>\\`` SQL statement
    instead of `DeltaTable.createIfNotExists(spark).location(path)`. The
    fluent builder's `.location(path)` call goes through `DeltaCatalog`
    (registered as `spark_catalog` in this platform's Spark config) and,
    against the `s3a://` filesystem specifically, ends up comparing two
    independently-derived representations of `path` -- the literal string
    passed to `.location()`, and a second one `DeltaCatalog`
    re-qualifies via `S3AFileSystem`, which appends a trailing slash when
    it qualifies what it treats as a directory-like key. Those two
    representations differing by exactly a trailing slash is what raises
    `DELTA_AMBIGUOUS_PATHS_IN_CREATE_TABLE`, regardless of whether the
    caller's own `path` already had a trailing slash stripped (see
    `DeltaSilverMergeWriter.__post_init__`) -- local-filesystem unit tests
    never exercise this because plain `Path` qualification on a local FS
    doesn't add that trailing slash, so the fluent-builder version passed
    in CI while failing every time against the real MinIO/s3a stack. A
    path-based identifier (`delta.`<path>`` in the raw SQL) with no separate
    `.location()`/`LOCATION` clause gives Delta only one representation of
    `path` to work with, so there's nothing left to disagree with itself.

    Args:
        spark: Active SparkSession.
        path: Delta table location, e.g. `s3a://silver-veloz/orders`.
        schema: Full target schema, `OrdersSilverSchema.TARGET`.
    """
    if DeltaTable.isDeltaTable(spark, path):
        return

    columns_ddl = ",\n            ".join(_column_ddl(field) for field in schema.fields)
    properties_ddl = ", ".join(f"'{key}' = '{value}'" for key, value in TABLE_PROPERTIES.items())
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS delta.`{path}` (
            {columns_ddl}
        )
        USING delta
        PARTITIONED BY ({PARTITION_COLUMN})
        TBLPROPERTIES ({properties_ddl})
        """
    )


@dataclass(frozen=True)
class DeltaSilverMergeWriter:
    """Writes a collapsed orders batch into the Silver Delta table via MERGE.

    Attributes:
        path: Delta table location, e.g. `s3a://silver-veloz/orders/`.
        schema: Full target schema used to create the table the first
            time `write` is called against `path`.
    """

    path: str
    schema: StructType

    def __post_init__(self) -> None:
        """Strips trailing slash(es) from `path`.

        Delta resolves `s3a://bucket/orders/` and `s3a://bucket/orders`
        as two different table locations, so a caller passing either
        convention must not be able to trigger
        `DELTA_AMBIGUOUS_PATHS_IN_CREATE_TABLE`. Normalizing once here
        keeps `_ensure_table_exists` and `DeltaTable.forPath` below
        consistent, regardless of how the caller built `path`.
        """
        object.__setattr__(self, "path", self.path.rstrip("/"))

    def write(self, collapsed_df: DataFrame) -> None:
        """MERGEs one already-collapsed batch (one row per `order_id`) into Silver.

        Args:
            collapsed_df: Output of `application.orders_silver_dedup.
                collapse_bronze_batch` -- exactly one row per `order_id`,
                with `created_date` populated on every row.

        No-ops on an empty batch: there is no `created_date` range to
        scope a MERGE condition to, and nothing to write.
        """
        spark = collapsed_df.sparkSession
        bounds = collapsed_df.agg(
            F.min(PARTITION_COLUMN).alias("min_date"),
            F.max(PARTITION_COLUMN).alias("max_date"),
        ).collect()[0]
        batch_min: date | None = bounds["min_date"]
        batch_max: date | None = bounds["max_date"]
        if batch_min is None or batch_max is None:
            return

        _ensure_table_exists(spark, self.path, self.schema)
        target = DeltaTable.forPath(spark, self.path)

        condition = (F.col("target.order_id") == F.col("source.order_id")) & (
            F.col("target." + PARTITION_COLUMN).between(F.lit(batch_min), F.lit(batch_max))
        )

        (
            target.alias("target")
            .merge(collapsed_df.alias("source"), condition)
            .whenMatchedUpdate(set=build_update_set())
            .whenNotMatchedInsert(values=build_insert_values(collapsed_df.columns))
            .execute()
        )
