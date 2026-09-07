"""Tests for application/orders_silver_dedup.py."""

from __future__ import annotations

import os
import tempfile
from datetime import date, datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DateType, StringType, StructField, StructType, TimestampType

from application.orders_silver_dedup import collapse_bronze_batch
from metadata.orders_schema import OrdersSchema

# Bronze's physical schema: the raw source columns plus the three lineage
# columns `application.orders_silver_dedup` collapses alongside them.
_BRONZE_SCHEMA = StructType(
    list(OrdersSchema.RAW.fields)
    + [
        StructField("_extract_date", DateType(), True),
        StructField("_ingestion_window", StringType(), True),
        StructField("_bronze_ingested_at", TimestampType(), True),
    ]
)

_DEFAULTS = {
    "store_id": "STORE-001",
    "rider_id": None,
    "assigned_at": None,
    "picked_up_at": None,
    "delivered_at": None,
    "order_total": 100.0,
    "_extract_date": date(2026, 8, 30),
    "_ingestion_window": "20260830T000000Z",
    "_bronze_ingested_at": datetime(2026, 8, 30, 0, 0, 5),
}


def _bronze_row(**overrides: object) -> dict:
    """Builds one bronze row dict, filling unspecified fields with sane defaults."""
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


def _build_bronze_df(spark: SparkSession, tmp_path: Path, rows: list[dict]) -> DataFrame:
    """Builds a bronze-shaped DataFrame from row dicts via a CSV round-trip.

    `spark.createDataFrame(list_of_dicts_or_rows, schema=...)` hits a
    cloudpickle stack overflow under Python 3.14 + PySpark 3.5.3 in this
    environment (`tests/test_quarantine_writer.py` documents the same
    issue). Writing an actual CSV file and reading it back with an
    explicit schema sidesteps cloudpickle entirely, and mirrors how
    `application.bronze_ingestion` reads real data in production.
    """
    field_names = [field.name for field in _BRONZE_SCHEMA.fields]
    lines = [",".join(field_names)]
    lines.extend(",".join(_format_csv_value(row[name]) for name in field_names) for row in rows)
    fd, csv_path = tempfile.mkstemp(suffix=".csv", dir=tmp_path)
    os.close(fd)
    Path(csv_path).write_text("\n".join(lines) + "\n")
    return (
        spark.read.format("csv")
        .schema(_BRONZE_SCHEMA)
        .option("header", "true")
        .load(csv_path)
    )


class TestCollapseGrain:
    """Test that the collapse always reduces to one row per order_id."""

    def test_collapses_multiple_rows_to_one_per_order_id(
        self, spark_session: SparkSession, tmp_path: Path
    ) -> None:
        """Verify a 3-row batch for the same order_id collapses to exactly 1 row."""
        rows = [
            _bronze_row(
                order_id="order-1",
                status="created",
                created_at=datetime(2026, 8, 30, 10, 0, 0),
                updated_at=datetime(2026, 8, 30, 10, 0, 0),
                _ingestion_window="20260830T100000Z",
            ),
            _bronze_row(
                order_id="order-1",
                status="assigned",
                created_at=datetime(2026, 8, 30, 10, 0, 0),
                rider_id="rider-1",
                updated_at=datetime(2026, 8, 30, 10, 5, 0),
                _ingestion_window="20260830T100500Z",
            ),
            _bronze_row(
                order_id="order-1",
                status="delivered",
                created_at=datetime(2026, 8, 30, 10, 0, 0),
                rider_id="rider-1",
                delivered_at=datetime(2026, 8, 30, 10, 30, 0),
                updated_at=datetime(2026, 8, 30, 10, 30, 0),
                _ingestion_window="20260830T103000Z",
            ),
        ]
        result = collapse_bronze_batch(_build_bronze_df(spark_session, tmp_path, rows)).collect()

        assert len(result) == 1
        assert result[0]["order_id"] == "order-1"

    def test_multiple_order_ids_each_get_their_own_row(
        self, spark_session: SparkSession, tmp_path: Path
    ) -> None:
        """Verify two distinct order_ids in the same batch each survive."""
        rows = [
            _bronze_row(
                order_id="order-1",
                status="created",
                created_at=datetime(2026, 8, 30, 10, 0, 0),
                updated_at=datetime(2026, 8, 30, 10, 0, 0),
            ),
            _bronze_row(
                order_id="order-2",
                status="created",
                created_at=datetime(2026, 8, 30, 11, 0, 0),
                updated_at=datetime(2026, 8, 30, 11, 0, 0),
            ),
        ]
        result = collapse_bronze_batch(_build_bronze_df(spark_session, tmp_path, rows)).collect()

        assert {row["order_id"] for row in result} == {"order-1", "order-2"}


class TestStickyGapFillWithinBatch:
    """Test the column-wise last(ignorenulls=True) collapse (not row_number()==1)."""

    def test_older_rows_only_non_null_picked_up_at_survives(
        self, spark_session: SparkSession, tmp_path: Path
    ) -> None:
        """An older row's only non-null picked_up_at must land even if the newest row is null there.

        This is the exact scenario row_number()==1 would get wrong: the
        newest row (by updated_at) reflects a `delivered` extract that
        never carried `picked_up_at` forward, but an earlier `assigned`
        extract in the same batch did set it.
        """
        rows = [
            _bronze_row(
                order_id="order-1",
                status="assigned",
                created_at=datetime(2026, 8, 30, 10, 0, 0),
                picked_up_at=datetime(2026, 8, 30, 10, 10, 0),
                updated_at=datetime(2026, 8, 30, 10, 10, 0),
                _ingestion_window="20260830T101000Z",
            ),
            _bronze_row(
                order_id="order-1",
                status="delivered",
                created_at=datetime(2026, 8, 30, 10, 0, 0),
                picked_up_at=None,
                delivered_at=datetime(2026, 8, 30, 10, 30, 0),
                updated_at=datetime(2026, 8, 30, 10, 30, 0),
                _ingestion_window="20260830T103000Z",
            ),
        ]
        result = collapse_bronze_batch(_build_bronze_df(spark_session, tmp_path, rows)).collect()

        assert result[0]["picked_up_at"] == datetime(2026, 8, 30, 10, 10, 0)
        assert result[0]["status"] == "delivered"


class TestTieBreakOrdering:
    """Test the orderBy(updated_at, status_rank, _ingestion_window) tiebreak chain."""

    def test_status_rank_breaks_an_updated_at_tie(
        self, spark_session: SparkSession, tmp_path: Path
    ) -> None:
        """On an updated_at tie, the higher STATUS_RANK row's values win (sorts last, ascending)."""
        tied_updated_at = datetime(2026, 8, 30, 10, 30, 0)
        rows = [
            _bronze_row(
                order_id="order-1",
                status="assigned",
                created_at=datetime(2026, 8, 30, 10, 0, 0),
                rider_id="rider-assigned",
                updated_at=tied_updated_at,
                _ingestion_window="20260830T103000Z",
            ),
            _bronze_row(
                order_id="order-1",
                status="picked_up",
                created_at=datetime(2026, 8, 30, 10, 0, 0),
                rider_id="rider-picked-up",
                updated_at=tied_updated_at,
                _ingestion_window="20260830T103000Z",
            ),
        ]
        result = collapse_bronze_batch(_build_bronze_df(spark_session, tmp_path, rows)).collect()

        assert result[0]["rider_id"] == "rider-picked-up"

    def test_ingestion_window_breaks_a_full_tie(
        self, spark_session: SparkSession, tmp_path: Path
    ) -> None:
        """On an updated_at AND status_rank tie, the lexicographically-later _ingestion_window wins."""
        tied_updated_at = datetime(2026, 8, 30, 10, 30, 0)
        rows = [
            _bronze_row(
                order_id="order-1",
                status="assigned",
                created_at=datetime(2026, 8, 30, 10, 0, 0),
                rider_id="rider-early-window",
                updated_at=tied_updated_at,
                _ingestion_window="20260830T102500Z",
            ),
            _bronze_row(
                order_id="order-1",
                status="assigned",
                created_at=datetime(2026, 8, 30, 10, 0, 0),
                rider_id="rider-late-window",
                updated_at=tied_updated_at,
                _ingestion_window="20260830T103000Z",
            ),
        ]
        result = collapse_bronze_batch(_build_bronze_df(spark_session, tmp_path, rows)).collect()

        assert result[0]["rider_id"] == "rider-late-window"


class TestCancelledAtDerivation:
    """Test cancelled_at's derivation: first cancelled row's updated_at, sticky thereafter."""

    def test_cancelled_at_set_from_the_cancelling_row(
        self, spark_session: SparkSession, tmp_path: Path
    ) -> None:
        """Verify cancelled_at takes the updated_at of the row where status became cancelled."""
        cancelled_at = datetime(2026, 8, 30, 10, 15, 0)
        rows = [
            _bronze_row(
                order_id="order-1",
                status="created",
                created_at=datetime(2026, 8, 30, 10, 0, 0),
                updated_at=datetime(2026, 8, 30, 10, 0, 0),
                _ingestion_window="20260830T100000Z",
            ),
            _bronze_row(
                order_id="order-1",
                status="cancelled",
                created_at=datetime(2026, 8, 30, 10, 0, 0),
                updated_at=cancelled_at,
                _ingestion_window="20260830T101500Z",
            ),
        ]
        result = collapse_bronze_batch(_build_bronze_df(spark_session, tmp_path, rows)).collect()

        assert result[0]["cancelled_at"] == cancelled_at
        assert result[0]["status"] == "cancelled"

    def test_cancelled_at_stays_at_the_first_cancellation_not_a_later_duplicate(
        self, spark_session: SparkSession, tmp_path: Path
    ) -> None:
        """A second, later cancelled extract in the same batch must not move cancelled_at forward."""
        first_cancelled_at = datetime(2026, 8, 30, 10, 15, 0)
        rows = [
            _bronze_row(
                order_id="order-1",
                status="cancelled",
                created_at=datetime(2026, 8, 30, 10, 0, 0),
                updated_at=first_cancelled_at,
                _ingestion_window="20260830T101500Z",
            ),
            _bronze_row(
                order_id="order-1",
                status="cancelled",
                created_at=datetime(2026, 8, 30, 10, 0, 0),
                updated_at=datetime(2026, 8, 30, 10, 45, 0),
                _ingestion_window="20260830T104500Z",
            ),
        ]
        result = collapse_bronze_batch(_build_bronze_df(spark_session, tmp_path, rows)).collect()

        assert result[0]["cancelled_at"] == first_cancelled_at

    def test_cancelled_at_is_null_when_never_cancelled(
        self, spark_session: SparkSession, tmp_path: Path
    ) -> None:
        """Verify cancelled_at stays null for an order that never reached cancelled."""
        rows = [
            _bronze_row(
                order_id="order-1",
                status="created",
                created_at=datetime(2026, 8, 30, 10, 0, 0),
                updated_at=datetime(2026, 8, 30, 10, 0, 0),
            ),
        ]
        result = collapse_bronze_batch(_build_bronze_df(spark_session, tmp_path, rows)).collect()

        assert result[0]["cancelled_at"] is None


class TestCreatedDateDerivation:
    """Test created_date's derivation: to_date(created_at), a DATE."""

    def test_created_date_is_the_date_part_of_created_at(
        self, spark_session: SparkSession, tmp_path: Path
    ) -> None:
        """Verify created_date equals created_at's date, as a python date, not a datetime."""
        rows = [
            _bronze_row(
                order_id="order-1",
                status="created",
                created_at=datetime(2026, 8, 30, 23, 45, 0),
                updated_at=datetime(2026, 8, 30, 23, 45, 0),
            ),
        ]
        result_df = collapse_bronze_batch(_build_bronze_df(spark_session, tmp_path, rows))
        result = result_df.collect()

        created_date_type = dict(result_df.dtypes)["created_date"]
        assert created_date_type == "date"
        assert result[0]["created_date"] == date(2026, 8, 30)
