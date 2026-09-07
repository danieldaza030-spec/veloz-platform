"""Intra-batch collapse of a Bronze orders read down to one row per order_id.

A bounded Bronze read (a set of `_ingestion_window`s, or a set of
`_extract_date`s) can carry more than one row for the same `order_id`: an
order legitimately appears in multiple extract windows as it progresses
through its lifecycle, and Bronze never deduplicates that (see
`dags/ingest_orders_bronze.py`'s module docstring). This module collapses
that batch down to Silver's grain -- one row per `order_id` -- *before*
`infrastructure.delta_silver_merge_writer` ever sees it, so the MERGE
itself only has to reconcile one incoming row against one existing row.

Deliberately not `row_number() == 1`: a plain "keep the newest row" would
drop a non-null value an older row in the same batch holds where the
newest row happens to be null there (e.g. `picked_up_at` set on an
`assigned` extract, then a later `delivered` extract's row -- correct
about `delivered_at`, but silent on `picked_up_at` because Postgres
periodic extracts aren't changelogs). Column-wise `last(..., ignorenulls=
True)` over the whole batch, ordered oldest-to-newest, preserves that
value instead of losing it.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from metadata.orders_silver_schema import (
    BRONZE_LINEAGE_COLUMNS,
    LATEST_WINS_COLUMNS,
    STATUS_RANK,
    STICKY_COLUMNS,
    TERMINAL_STATUS,
)

_RAW_STICKY_COLUMNS: tuple[str, ...] = tuple(
    column for column in STICKY_COLUMNS if column != "cancelled_at"
)
"""`STICKY_COLUMNS` minus `cancelled_at`: `cancelled_at` has no bronze
source column of its own to collapse -- it's computed separately below --
so it's excluded from the set of columns collapsed by `last(...,
ignorenulls=True)`."""

COLLAPSE_COLUMNS: tuple[str, ...] = _RAW_STICKY_COLUMNS + LATEST_WINS_COLUMNS + BRONZE_LINEAGE_COLUMNS
"""Every bronze column, other than `order_id` itself, that the intra-batch
collapse reduces via `last(..., ignorenulls=True)` -- the raw sticky
timestamp columns, the latest-wins business columns, and the bronze
lineage columns kept for audit."""


def _status_rank_column(status_column: Column) -> Column:
    """Maps `status_column`'s value to its `STATUS_RANK` lifecycle rank.

    Args:
        status_column: The `status` column to rank.

    Returns:
        An integer `Column`: `STATUS_RANK[status]` for a known status.
        Unknown values map to `None`, sorting them ahead of every known
        rank in the ascending order the collapse window uses (`NULL`
        sorts first by default), so an unexpected status value fails
        loudly downstream rather than silently winning or losing a tie.
    """
    mapping = F.create_map(*(F.lit(value) for pair in STATUS_RANK.items() for value in pair))
    return F.element_at(mapping, status_column)


def _collapse_window() -> Window:
    """Builds the whole-batch, oldest-to-newest window every collapsed column shares.

    Ordered by `updated_at` ascending, then `STATUS_RANK` ascending (a
    lifecycle tiebreak for same-`updated_at` rows), then
    `_ingestion_window` ascending (a final, deterministic tiebreak).
    `rowsBetween(unboundedPreceding, unboundedFollowing)` makes every row
    in an `order_id` partition see the whole batch, which is what lets
    `last(..., ignorenulls=True)` reach back past a newer, null-carrying
    row to an older row's non-null value.
    """
    return (
        Window.partitionBy("order_id")
        .orderBy(
            F.col("updated_at").asc(),
            _status_rank_column(F.col("status")).asc(),
            F.col("_ingestion_window").asc(),
        )
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )


def collapse_bronze_batch(bronze_df: DataFrame) -> DataFrame:
    """Collapses a Bronze orders read to exactly one row per `order_id`.

    Args:
        bronze_df: Rows read from `s3a://bronze-veloz/orders/` for a
            bounded, already-resolved set of `_ingestion_window`s or
            `_extract_date`s. Must carry `order_id` plus every column in
            `COLLAPSE_COLUMNS`.

    Returns:
        One row per distinct `order_id`, with every `COLLAPSE_COLUMNS`
        entry set to the last non-null value seen across the batch (in
        the ordering `_collapse_window` defines), plus two derived
        columns: `cancelled_at` (the `updated_at` of the first row, in
        that same ordering, where `status == "cancelled"`; unset
        otherwise) and `created_date` (`to_date(created_at)`, a `DATE`).
    """
    window = _collapse_window()

    collapsed_columns = [
        F.last(F.col(column), ignorenulls=True).over(window).alias(column)
        for column in COLLAPSE_COLUMNS
    ]
    cancelled_at_column = F.first(
        F.when(F.col("status") == TERMINAL_STATUS, F.col("updated_at")),
        ignorenulls=True,
    ).over(window).alias("cancelled_at")

    collapsed = bronze_df.select(
        F.col("order_id"),
        *collapsed_columns,
        cancelled_at_column,
    ).withColumn("created_date", F.to_date(F.col("created_at")))

    # Every row within an `order_id` partition now holds identical values
    # for every selected column (they're all whole-partition window
    # aggregates) -- dropDuplicates on the grouping key alone is enough to
    # reduce the batch to Silver's one-row-per-order_id grain.
    return collapsed.dropDuplicates(["order_id"])
