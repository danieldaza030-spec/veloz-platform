"""Tests for application/orders_silver_ingestion.py."""

from __future__ import annotations

import os
import tempfile
from datetime import date, datetime
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, StringType, StructField, StructType, TimestampType

from application.orders_silver_ingestion import (
    OrdersSilverIngestionRequest,
    ingest_orders_to_silver,
)
from metadata.orders_schema import OrdersSchema

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


def _write_bronze(spark: SparkSession, tmp_path: Path, path: str, rows: list[dict]) -> None:
    """Writes bronze row dicts into a Bronze Delta table via a CSV round-trip.

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
    df = (
        spark.read.format("csv")
        .schema(_BRONZE_SCHEMA)
        .option("header", "true")
        .load(csv_path)
    )
    df.write.format("delta").mode("append").partitionBy("_extract_date", "_ingestion_window").save(path)


class TestOrdersSilverIngestionRequestValidation:
    """Test the exactly-one-of-two-filter-modes rule."""

    def test_raises_when_both_filters_set(self) -> None:
        """Verify setting both ingestion_windows and extract_dates raises."""
        with pytest.raises(ValueError, match="exactly one"):
            OrdersSilverIngestionRequest(
                bronze_path="s3a://bronze-veloz/orders/",
                silver_path="s3a://silver-veloz/orders/",
                ingestion_windows=["20260830T100000Z"],
                extract_dates=["2026-08-30"],
            )

    def test_raises_when_neither_filter_set(self) -> None:
        """Verify leaving both filters unset raises (no third, unbounded mode)."""
        with pytest.raises(ValueError, match="exactly one"):
            OrdersSilverIngestionRequest(
                bronze_path="s3a://bronze-veloz/orders/",
                silver_path="s3a://silver-veloz/orders/",
            )

    def test_accepts_ingestion_windows_only(self) -> None:
        """Verify the ingestion-window-only mode constructs cleanly."""
        request = OrdersSilverIngestionRequest(
            bronze_path="s3a://bronze-veloz/orders/",
            silver_path="s3a://silver-veloz/orders/",
            ingestion_windows=["20260830T100000Z"],
        )
        assert request.ingestion_windows == ["20260830T100000Z"]
        assert request.extract_dates is None

    def test_accepts_extract_dates_only(self) -> None:
        """Verify the extract-date-only mode constructs cleanly."""
        request = OrdersSilverIngestionRequest(
            bronze_path="s3a://bronze-veloz/orders/",
            silver_path="s3a://silver-veloz/orders/",
            extract_dates=["2026-08-30"],
        )
        assert request.extract_dates == ["2026-08-30"]
        assert request.ingestion_windows is None


class TestIngestOrdersToSilverEndToEnd:
    """Test the read -> collapse -> MERGE wiring."""

    def test_filters_by_ingestion_window_set(self, spark_session: SparkSession, tmp_path: Path) -> None:
        """Verify only bronze rows in the requested _ingestion_window set are ingested."""
        bronze_path = str(tmp_path / "bronze")
        silver_path = str(tmp_path / "silver")

        _write_bronze(
            spark_session,
            tmp_path,
            bronze_path,
            [
                _bronze_row(
                    order_id="order-1",
                    status="created",
                    created_at=datetime(2026, 8, 30, 10, 0, 0),
                    updated_at=datetime(2026, 8, 30, 10, 0, 0),
                    _extract_date=date(2026, 8, 30),
                    _ingestion_window="20260830T100000Z",
                ),
                _bronze_row(
                    order_id="order-2",
                    status="created",
                    created_at=datetime(2026, 8, 30, 11, 0, 0),
                    updated_at=datetime(2026, 8, 30, 11, 0, 0),
                    _extract_date=date(2026, 8, 30),
                    # Outside the requested window set below.
                    _ingestion_window="20260830T110000Z",
                ),
            ],
        )

        request = OrdersSilverIngestionRequest(
            bronze_path=bronze_path,
            silver_path=silver_path,
            ingestion_windows=["20260830T100000Z"],
        )
        row_count = ingest_orders_to_silver(spark_session, request)

        assert row_count == 1
        result = spark_session.read.format("delta").load(silver_path).collect()
        assert len(result) == 1
        assert result[0]["order_id"] == "order-1"

    def test_filters_by_extract_date_set(self, spark_session: SparkSession, tmp_path: Path) -> None:
        """Verify only bronze rows in the requested _extract_date set are ingested."""
        bronze_path = str(tmp_path / "bronze")
        silver_path = str(tmp_path / "silver")

        _write_bronze(
            spark_session,
            tmp_path,
            bronze_path,
            [
                _bronze_row(
                    order_id="order-1",
                    status="created",
                    created_at=datetime(2026, 8, 30, 10, 0, 0),
                    updated_at=datetime(2026, 8, 30, 10, 0, 0),
                    _extract_date=date(2026, 8, 30),
                    _ingestion_window="20260830T100000Z",
                ),
                _bronze_row(
                    order_id="order-2",
                    status="created",
                    created_at=datetime(2026, 8, 31, 9, 0, 0),
                    updated_at=datetime(2026, 8, 31, 9, 0, 0),
                    _extract_date=date(2026, 8, 31),
                    _ingestion_window="20260831T090000Z",
                ),
            ],
        )

        request = OrdersSilverIngestionRequest(
            bronze_path=bronze_path,
            silver_path=silver_path,
            extract_dates=["2026-08-30"],
        )
        row_count = ingest_orders_to_silver(spark_session, request)

        assert row_count == 1
        result = spark_session.read.format("delta").load(silver_path).collect()
        assert result[0]["order_id"] == "order-1"

    def test_full_rerun_of_same_window_set_is_idempotent(
        self, spark_session: SparkSession, tmp_path: Path
    ) -> None:
        """Verify re-running the exact same request twice leaves Silver unchanged."""
        bronze_path = str(tmp_path / "bronze")
        silver_path = str(tmp_path / "silver")

        _write_bronze(
            spark_session,
            tmp_path,
            bronze_path,
            [
                _bronze_row(
                    order_id="order-1",
                    status="assigned",
                    created_at=datetime(2026, 8, 30, 10, 0, 0),
                    rider_id="rider-1",
                    updated_at=datetime(2026, 8, 30, 10, 5, 0),
                    _extract_date=date(2026, 8, 30),
                    _ingestion_window="20260830T100500Z",
                ),
            ],
        )

        request = OrdersSilverIngestionRequest(
            bronze_path=bronze_path,
            silver_path=silver_path,
            ingestion_windows=["20260830T100500Z"],
        )

        first_row_count = ingest_orders_to_silver(spark_session, request)
        first_result = spark_session.read.format("delta").load(silver_path).collect()

        second_row_count = ingest_orders_to_silver(spark_session, request)
        second_result = spark_session.read.format("delta").load(silver_path).collect()

        assert first_row_count == 1
        assert second_row_count == 1
        assert len(second_result) == 1
        assert first_result[0]["status"] == second_result[0]["status"]
        assert first_result[0]["rider_id"] == second_result[0]["rider_id"]
        assert first_result[0]["updated_at"] == second_result[0]["updated_at"]
        assert first_result[0]["_silver_first_seen_at"] == second_result[0]["_silver_first_seen_at"]
