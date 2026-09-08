"""Tests for infrastructure/delta_silver_merge_writer.py.

Builds already-collapsed batches directly (one row per order_id, matching
`application.orders_silver_dedup.collapse_bronze_batch`'s output shape)
rather than routing every scenario through the dedup step -- this module
is what's under test here, and hand-crafting the source batch makes each
MERGE scenario (sticky gap-fill, sticky non-regression, latest-wins,
audit columns, the partition predicate) exact and independent of dedup
behavior covered in `test_orders_silver_dedup.py`.
"""

from __future__ import annotations

import os
import tempfile
import time
from datetime import date, datetime
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from infrastructure.delta_silver_merge_writer import DeltaSilverMergeWriter, build_update_set
from metadata.orders_silver_schema import (
    PARTITION_COLUMN,
    SILVER_FIRST_SEEN_AT_COLUMN,
    SILVER_INGESTED_AT_COLUMN,
    OrdersSilverSchema,
)

# The collapsed batch schema: OrdersSilverSchema.TARGET minus the two
# audit columns the writer itself populates -- a collapsed batch never
# carries those, the same as application.orders_silver_dedup's real output.
_COLLAPSED_SCHEMA = StructType(
    [
        field
        for field in OrdersSilverSchema.TARGET.fields
        if field.name not in (SILVER_INGESTED_AT_COLUMN, SILVER_FIRST_SEEN_AT_COLUMN)
    ]
)

_DEFAULTS = {
    "store_id": "STORE-001",
    "rider_id": None,
    "status": "created",
    "assigned_at": None,
    "picked_up_at": None,
    "delivered_at": None,
    "cancelled_at": None,
    "order_total": 100.0,
    "created_date": date(2026, 8, 30),
    "_extract_date": date(2026, 8, 30),
    "_ingestion_window": "20260830T000000Z",
    "_bronze_ingested_at": datetime(2026, 8, 30, 0, 0, 5),
}


def _collapsed_row(**overrides: object) -> dict:
    """Builds one already-collapsed batch row, filling in sane defaults."""
    row = dict(_DEFAULTS)
    row.update(overrides)
    return row


def _format_csv_value(value: object) -> str:
    """Formats one row value for the CSV round-trip below."""
    if value is None:
        return ""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return str(value)


def _build_batch(spark: SparkSession, tmp_path: Path, rows: list[dict]) -> DataFrame:
    """Builds an already-collapsed batch DataFrame from row dicts via a CSV round-trip.

    `spark.createDataFrame(list_of_dicts_or_rows, schema=...)` hits a
    cloudpickle stack overflow under Python 3.14 + PySpark 3.5.3 in this
    environment (`tests/test_quarantine_writer.py` documents the same
    issue). Writing an actual CSV file and reading it back with an
    explicit schema sidesteps cloudpickle entirely.
    """
    field_names = [field.name for field in _COLLAPSED_SCHEMA.fields]
    lines = [",".join(field_names)]
    lines.extend(",".join(_format_csv_value(row[name]) for name in field_names) for row in rows)
    fd, csv_path = tempfile.mkstemp(suffix=".csv", dir=tmp_path)
    os.close(fd)
    Path(csv_path).write_text("\n".join(lines) + "\n")
    return (
        spark.read.format("csv")
        .schema(_COLLAPSED_SCHEMA)
        .option("header", "true")
        .load(csv_path)
    )


def _read_silver(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.format("delta").load(path)


def _write(spark: SparkSession, tmp_path: Path, path: str, rows: list[dict]) -> None:
    writer = DeltaSilverMergeWriter(path=path, schema=OrdersSilverSchema.TARGET)
    writer.write(_build_batch(spark, tmp_path, rows))


class TestTableCreation:
    """Test the Silver table is created with the locked partitioning/properties."""

    def test_partitioned_by_created_date_only(
        self, spark_session: SparkSession, temp_delta_path: str, tmp_path: Path
    ) -> None:
        """Verify the table's only partition column is created_date."""
        _write(
            spark_session,
            tmp_path,
            temp_delta_path,
            [
                _collapsed_row(
                    order_id="order-1",
                    created_at=datetime(2026, 8, 30, 10, 0, 0),
                    updated_at=datetime(2026, 8, 30, 10, 0, 0),
                )
            ],
        )
        detail = DeltaTable.forPath(spark_session, temp_delta_path).detail().collect()[0]
        assert detail["partitionColumns"] == ["created_date"]

    def test_table_properties_are_set(
        self, spark_session: SparkSession, temp_delta_path: str, tmp_path: Path
    ) -> None:
        """Verify deletion vectors and autoOptimize properties are set at creation."""
        _write(
            spark_session,
            tmp_path,
            temp_delta_path,
            [
                _collapsed_row(
                    order_id="order-1",
                    created_at=datetime(2026, 8, 30, 10, 0, 0),
                    updated_at=datetime(2026, 8, 30, 10, 0, 0),
                )
            ],
        )
        detail = DeltaTable.forPath(spark_session, temp_delta_path).detail().collect()[0]
        properties = detail["properties"]
        assert properties.get("delta.enableDeletionVectors") == "true"
        assert properties.get("delta.autoOptimize.optimizeWrite") == "true"
        assert properties.get("delta.autoOptimize.autoCompact") == "true"


class TestStickyColumns:
    """Test the per-column sticky CASE: newer-wins-with-fallback, never regresses to null."""

    def test_late_older_batch_fills_a_sticky_null_gap(
        self, spark_session: SparkSession, temp_delta_path: str, tmp_path: Path
    ) -> None:
        """A late-arriving, OLDER batch's non-null sticky value must land into an existing null gap."""
        _write(
            spark_session,
            tmp_path,
            temp_delta_path,
            [
                _collapsed_row(
                    order_id="order-1",
                    status="delivered",
                    created_at=datetime(2026, 8, 30, 10, 0, 0),
                    delivered_at=datetime(2026, 8, 30, 10, 30, 0),
                    picked_up_at=None,
                    updated_at=datetime(2026, 8, 30, 10, 30, 0),
                )
            ],
        )
        # Late arrival: an older extract (updated_at earlier than what's
        # already in Silver) that carries the picked_up_at value the
        # first write never had.
        _write(
            spark_session,
            tmp_path,
            temp_delta_path,
            [
                _collapsed_row(
                    order_id="order-1",
                    status="picked_up",
                    created_at=datetime(2026, 8, 30, 10, 0, 0),
                    picked_up_at=datetime(2026, 8, 30, 10, 10, 0),
                    updated_at=datetime(2026, 8, 30, 10, 10, 0),
                )
            ],
        )

        result = _read_silver(spark_session, temp_delta_path).collect()
        assert len(result) == 1
        assert result[0]["picked_up_at"] == datetime(2026, 8, 30, 10, 10, 0)
        # The late/older batch must not regress the latest-wins status
        # back to "picked_up" -- covered fully in TestLatestWinsColumns,
        # asserted here too since it's the same write.
        assert result[0]["status"] == "delivered"

    def test_sticky_value_is_not_regressed_to_null_by_a_newer_batch(
        self, spark_session: SparkSession, temp_delta_path: str, tmp_path: Path
    ) -> None:
        """A NEWER batch carrying null for a sticky column must not erase the existing value."""
        _write(
            spark_session,
            tmp_path,
            temp_delta_path,
            [
                _collapsed_row(
                    order_id="order-1",
                    status="picked_up",
                    created_at=datetime(2026, 8, 30, 10, 0, 0),
                    picked_up_at=datetime(2026, 8, 30, 10, 10, 0),
                    updated_at=datetime(2026, 8, 30, 10, 10, 0),
                )
            ],
        )
        # Newer batch: later updated_at, but this particular extract row
        # carries no picked_up_at value.
        _write(
            spark_session,
            tmp_path,
            temp_delta_path,
            [
                _collapsed_row(
                    order_id="order-1",
                    status="delivered",
                    created_at=datetime(2026, 8, 30, 10, 0, 0),
                    picked_up_at=None,
                    delivered_at=datetime(2026, 8, 30, 10, 30, 0),
                    updated_at=datetime(2026, 8, 30, 10, 30, 0),
                )
            ],
        )

        result = _read_silver(spark_session, temp_delta_path).collect()
        assert len(result) == 1
        assert result[0]["picked_up_at"] == datetime(2026, 8, 30, 10, 10, 0)

    def test_cancelled_at_set_on_cancellation_and_not_erased_by_a_later_null(
        self, spark_session: SparkSession, temp_delta_path: str, tmp_path: Path
    ) -> None:
        """cancelled_at, a STICKY column, follows the same set-once/non-regressing rule."""
        cancelled_at = datetime(2026, 8, 30, 10, 15, 0)
        _write(
            spark_session,
            tmp_path,
            temp_delta_path,
            [
                _collapsed_row(
                    order_id="order-1",
                    status="cancelled",
                    created_at=datetime(2026, 8, 30, 10, 0, 0),
                    cancelled_at=cancelled_at,
                    updated_at=cancelled_at,
                )
            ],
        )
        # A late, older batch for the same order carrying no cancelled_at
        # (e.g. an out-of-order "assigned" extract window) must not erase it.
        _write(
            spark_session,
            tmp_path,
            temp_delta_path,
            [
                _collapsed_row(
                    order_id="order-1",
                    status="assigned",
                    created_at=datetime(2026, 8, 30, 10, 0, 0),
                    cancelled_at=None,
                    updated_at=datetime(2026, 8, 30, 10, 5, 0),
                )
            ],
        )

        result = _read_silver(spark_session, temp_delta_path).collect()
        assert result[0]["cancelled_at"] == cancelled_at


class TestLatestWinsColumns:
    """Test the per-column latest-wins CASE: newer batch's value, no fallback."""

    def test_newer_batch_updates_a_latest_wins_column(
        self, spark_session: SparkSession, temp_delta_path: str, tmp_path: Path
    ) -> None:
        """Verify store_id changes when a strictly newer batch reports a different value."""
        _write(
            spark_session,
            tmp_path,
            temp_delta_path,
            [
                _collapsed_row(
                    order_id="order-1",
                    store_id="STORE-001",
                    created_at=datetime(2026, 8, 30, 10, 0, 0),
                    updated_at=datetime(2026, 8, 30, 10, 0, 0),
                )
            ],
        )
        _write(
            spark_session,
            tmp_path,
            temp_delta_path,
            [
                _collapsed_row(
                    order_id="order-1",
                    store_id="STORE-002",
                    created_at=datetime(2026, 8, 30, 10, 0, 0),
                    updated_at=datetime(2026, 8, 30, 10, 5, 0),
                )
            ],
        )

        result = _read_silver(spark_session, temp_delta_path).collect()
        assert result[0]["store_id"] == "STORE-002"

    def test_older_late_batch_does_not_update_a_latest_wins_column(
        self, spark_session: SparkSession, temp_delta_path: str, tmp_path: Path
    ) -> None:
        """Verify a late, OLDER batch's differing value is discarded, not applied."""
        _write(
            spark_session,
            tmp_path,
            temp_delta_path,
            [
                _collapsed_row(
                    order_id="order-1",
                    store_id="STORE-002",
                    created_at=datetime(2026, 8, 30, 10, 0, 0),
                    updated_at=datetime(2026, 8, 30, 10, 5, 0),
                )
            ],
        )
        _write(
            spark_session,
            tmp_path,
            temp_delta_path,
            [
                _collapsed_row(
                    order_id="order-1",
                    store_id="STORE-999",
                    created_at=datetime(2026, 8, 30, 10, 0, 0),
                    updated_at=datetime(2026, 8, 30, 10, 0, 0),
                )
            ],
        )

        result = _read_silver(spark_session, temp_delta_path).collect()
        assert result[0]["store_id"] == "STORE-002"

    def test_updated_at_never_moves_backwards(
        self, spark_session: SparkSession, temp_delta_path: str, tmp_path: Path
    ) -> None:
        """Verify updated_at is the greatest of source/target, even on a late/older write."""
        _write(
            spark_session,
            tmp_path,
            temp_delta_path,
            [
                _collapsed_row(
                    order_id="order-1",
                    created_at=datetime(2026, 8, 30, 10, 0, 0),
                    updated_at=datetime(2026, 8, 30, 10, 30, 0),
                )
            ],
        )
        _write(
            spark_session,
            tmp_path,
            temp_delta_path,
            [
                _collapsed_row(
                    order_id="order-1",
                    created_at=datetime(2026, 8, 30, 10, 0, 0),
                    updated_at=datetime(2026, 8, 30, 10, 5, 0),
                )
            ],
        )

        result = _read_silver(spark_session, temp_delta_path).collect()
        assert result[0]["updated_at"] == datetime(2026, 8, 30, 10, 30, 0)


class TestAuditColumns:
    """Test _silver_first_seen_at/_silver_ingested_at behavior."""

    def test_first_seen_at_unchanged_ingested_at_refreshed(
        self, spark_session: SparkSession, temp_delta_path: str, tmp_path: Path
    ) -> None:
        """Verify _silver_first_seen_at is stable and _silver_ingested_at advances on update."""
        _write(
            spark_session,
            tmp_path,
            temp_delta_path,
            [
                _collapsed_row(
                    order_id="order-1",
                    created_at=datetime(2026, 8, 30, 10, 0, 0),
                    updated_at=datetime(2026, 8, 30, 10, 0, 0),
                )
            ],
        )
        first_write = _read_silver(spark_session, temp_delta_path).collect()[0]
        first_seen_at = first_write[SILVER_FIRST_SEEN_AT_COLUMN]
        first_ingested_at = first_write[SILVER_INGESTED_AT_COLUMN]

        time.sleep(0.05)

        _write(
            spark_session,
            tmp_path,
            temp_delta_path,
            [
                _collapsed_row(
                    order_id="order-1",
                    status="delivered",
                    created_at=datetime(2026, 8, 30, 10, 0, 0),
                    delivered_at=datetime(2026, 8, 30, 10, 30, 0),
                    updated_at=datetime(2026, 8, 30, 10, 30, 0),
                )
            ],
        )
        second_write = _read_silver(spark_session, temp_delta_path).collect()[0]

        assert second_write[SILVER_FIRST_SEEN_AT_COLUMN] == first_seen_at
        assert second_write[SILVER_INGESTED_AT_COLUMN] > first_ingested_at

    def test_first_seen_at_and_ingested_at_set_equal_on_insert(
        self, spark_session: SparkSession, temp_delta_path: str, tmp_path: Path
    ) -> None:
        """Verify a brand-new row's two audit columns are set to the same insert-time value."""
        _write(
            spark_session,
            tmp_path,
            temp_delta_path,
            [
                _collapsed_row(
                    order_id="order-1",
                    created_at=datetime(2026, 8, 30, 10, 0, 0),
                    updated_at=datetime(2026, 8, 30, 10, 0, 0),
                )
            ],
        )
        result = _read_silver(spark_session, temp_delta_path).collect()[0]
        assert result[SILVER_FIRST_SEEN_AT_COLUMN] == result[SILVER_INGESTED_AT_COLUMN]


class TestPartitionPredicateCorrectness:
    """Test the created_date BETWEEN batch_min AND batch_max predicate.

    Correctness invariant: every order in the batch has a created_date
    inside [batch_min, batch_max] by construction (batch_min/batch_max
    are computed from that same batch), so the predicate can never
    produce a false-negative match or a duplicate insert.
    """

    def test_batch_spanning_multiple_created_dates_merges_without_duplicates(
        self, spark_session: SparkSession, temp_delta_path: str, tmp_path: Path
    ) -> None:
        """A batch spanning several created_date values still matches/updates each order exactly once."""
        _write(
            spark_session,
            tmp_path,
            temp_delta_path,
            [
                _collapsed_row(
                    order_id="order-1",
                    created_date=date(2026, 8, 28),
                    created_at=datetime(2026, 8, 28, 9, 0, 0),
                    updated_at=datetime(2026, 8, 28, 9, 0, 0),
                ),
                _collapsed_row(
                    order_id="order-2",
                    created_date=date(2026, 8, 30),
                    created_at=datetime(2026, 8, 30, 9, 0, 0),
                    updated_at=datetime(2026, 8, 30, 9, 0, 0),
                ),
            ],
        )
        # Re-run the same multi-date batch, plus an update to one order.
        _write(
            spark_session,
            tmp_path,
            temp_delta_path,
            [
                _collapsed_row(
                    order_id="order-1",
                    status="delivered",
                    created_date=date(2026, 8, 28),
                    created_at=datetime(2026, 8, 28, 9, 0, 0),
                    delivered_at=datetime(2026, 8, 28, 9, 30, 0),
                    updated_at=datetime(2026, 8, 28, 9, 30, 0),
                ),
                _collapsed_row(
                    order_id="order-2",
                    created_date=date(2026, 8, 30),
                    created_at=datetime(2026, 8, 30, 9, 0, 0),
                    updated_at=datetime(2026, 8, 30, 9, 0, 0),
                ),
            ],
        )

        result = _read_silver(spark_session, temp_delta_path).collect()
        assert len(result) == 2
        by_id = {row["order_id"]: row for row in result}
        assert by_id["order-1"]["status"] == "delivered"
        assert by_id["order-1"]["created_date"] == date(2026, 8, 28)
        assert by_id["order-2"]["created_date"] == date(2026, 8, 30)


class TestCreatedDateStability:
    """Regression coverage for the module docstring's `created_date`-never-changes coupling.

    The MERGE condition's "cannot produce a false-negative match"
    guarantee (see this module's docstring) depends on `created_date`
    staying put for a given `order_id` once it's first inserted --
    otherwise a later batch's `[batch_min, batch_max]` bound, computed
    from *that batch's own* rows, could exclude the target's actual
    stored `created_date` and miss the match, falling through to
    `whenNotMatchedInsert` and creating a duplicate `order_id` row.
    """

    def test_build_update_set_excludes_the_partition_column(
        self, spark_session: SparkSession
    ) -> None:
        """`created_date` must never appear in the whenMatchedUpdate set -- a matched row's value is never touched."""
        update_set = build_update_set()
        assert PARTITION_COLUMN not in update_set

    def test_later_batch_for_existing_order_id_does_not_move_created_date(
        self, spark_session: SparkSession, temp_delta_path: str, tmp_path: Path
    ) -> None:
        """A later batch for an already-Silver order_id leaves created_date exactly as first inserted."""
        original_created_date = date(2026, 8, 28)
        _write(
            spark_session,
            tmp_path,
            temp_delta_path,
            [
                _collapsed_row(
                    order_id="order-1",
                    status="created",
                    created_date=original_created_date,
                    created_at=datetime(2026, 8, 28, 9, 0, 0),
                    updated_at=datetime(2026, 8, 28, 9, 0, 0),
                )
            ],
        )
        # A later batch for the same order_id -- created_at (STICKY) never
        # changes upstream, so this row carries the same created_date the
        # first batch did, matching real generator behavior.
        _write(
            spark_session,
            tmp_path,
            temp_delta_path,
            [
                _collapsed_row(
                    order_id="order-1",
                    status="delivered",
                    created_date=original_created_date,
                    created_at=datetime(2026, 8, 28, 9, 0, 0),
                    delivered_at=datetime(2026, 8, 28, 9, 30, 0),
                    updated_at=datetime(2026, 8, 28, 9, 30, 0),
                )
            ],
        )

        result = _read_silver(spark_session, temp_delta_path).collect()
        # Exactly one row: the later batch's write matched and updated the
        # existing row in place, rather than missing the match and
        # inserting a second "order-1" row.
        assert len(result) == 1
        assert result[0]["created_date"] == original_created_date
        assert result[0]["status"] == "delivered"


class TestEmptyBatch:
    """Test the empty-batch no-op path."""

    def test_empty_batch_does_not_create_a_table(
        self, spark_session: SparkSession, temp_delta_path: str, tmp_path: Path
    ) -> None:
        """Verify writing an empty collapsed batch is a safe no-op."""
        writer = DeltaSilverMergeWriter(path=temp_delta_path, schema=OrdersSilverSchema.TARGET)
        writer.write(_build_batch(spark_session, tmp_path, []))

        assert DeltaTable.isDeltaTable(spark_session, temp_delta_path) is False


class TestPathNormalization:
    """Test path normalization in __post_init__ to avoid DELTA_AMBIGUOUS_PATHS_IN_CREATE_TABLE."""

    def test_trailing_slash_is_stripped_from_path(self) -> None:
        """Verify a path with trailing slash(es) is normalized by __post_init__."""
        writer = DeltaSilverMergeWriter(
            path="s3a://silver-veloz/orders/", schema=OrdersSilverSchema.TARGET
        )
        assert writer.path == "s3a://silver-veloz/orders"

    def test_multiple_trailing_slashes_are_stripped(self) -> None:
        """Verify multiple trailing slashes are all removed."""
        writer = DeltaSilverMergeWriter(
            path="s3a://silver-veloz/orders///", schema=OrdersSilverSchema.TARGET
        )
        assert writer.path == "s3a://silver-veloz/orders"

    def test_path_without_trailing_slash_is_unchanged(self) -> None:
        """Verify a path without trailing slash is left as-is."""
        writer = DeltaSilverMergeWriter(
            path="s3a://silver-veloz/orders", schema=OrdersSilverSchema.TARGET
        )
        assert writer.path == "s3a://silver-veloz/orders"
