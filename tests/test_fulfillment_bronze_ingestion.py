"""Tests for application/fulfillment_bronze_ingestion.py."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from application.fulfillment_bronze_ingestion import (
    build_expected_key,
    build_malformed_row_quarantine_df,
    build_missing_file_records,
    find_missing_store_keys,
)


class TestBuildExpectedKey:
    """Test the expected-key builder matches the generator's key layout."""

    def test_matches_generator_key_layout(self) -> None:
        """Verify the built key matches `fulfillment/date=<date>/<store>.csv`."""
        result = build_expected_key("fulfillment", "2026-08-30", "STORE-004")
        assert result == "fulfillment/date=2026-08-30/STORE-004.csv"


class TestFindMissingStoreKeys:
    """Test the expected-vs-present store key diff."""

    def test_no_missing_when_all_present(self) -> None:
        """Verify an empty list is returned when every store's file landed."""
        store_ids = ["STORE-001", "STORE-002"]
        present_keys = [
            "fulfillment/date=2026-08-30/STORE-001.csv",
            "fulfillment/date=2026-08-30/STORE-002.csv",
        ]

        result = find_missing_store_keys(store_ids, present_keys, "fulfillment", "2026-08-30")

        assert result == []

    def test_returns_sorted_missing_keys(self) -> None:
        """Verify absent stores' expected keys are returned, sorted."""
        store_ids = ["STORE-001", "STORE-002", "STORE-003"]
        present_keys = ["fulfillment/date=2026-08-30/STORE-002.csv"]

        result = find_missing_store_keys(store_ids, present_keys, "fulfillment", "2026-08-30")

        assert result == [
            "fulfillment/date=2026-08-30/STORE-001.csv",
            "fulfillment/date=2026-08-30/STORE-003.csv",
        ]

    def test_all_missing_when_nothing_present(self) -> None:
        """Verify every store is reported missing when the prefix is empty."""
        store_ids = ["STORE-001", "STORE-002"]

        result = find_missing_store_keys(store_ids, [], "fulfillment", "2026-08-30")

        assert result == [
            "fulfillment/date=2026-08-30/STORE-001.csv",
            "fulfillment/date=2026-08-30/STORE-002.csv",
        ]


class TestBuildMissingFileRecords:
    """Test the `missing_file` quarantine record builder."""

    def test_builds_one_record_per_missing_key(self) -> None:
        """Verify one record is built per missing key, with the right shape."""
        missing_keys = [
            "fulfillment/date=2026-08-30/STORE-003.csv",
            "fulfillment/date=2026-08-30/STORE-007.csv",
        ]
        detected_at = datetime(2026, 8, 30, 6, 0, 0, tzinfo=UTC)

        records = build_missing_file_records(missing_keys, "fulfillment", "2026-08-30", detected_at)

        assert len(records) == 2
        assert records[0] == {
            "source": "fulfillment",
            "partition_date": "2026-08-30",
            "key": "fulfillment/date=2026-08-30/STORE-003.csv",
            "reason": "missing_file",
            "detected_at": detected_at,
            "raw_snippet": None,
        }

    def test_empty_missing_keys_returns_empty_list(self) -> None:
        """Verify no records are built when nothing is missing."""
        records = build_missing_file_records([], "fulfillment", "2026-08-30", datetime.now(UTC))
        assert records == []


class TestBuildMalformedRowQuarantineDf:
    """Test the `malformed_row` quarantine DataFrame builder."""

    def test_builds_expected_columns_and_values(
        self, spark_session: SparkSession, temp_delta_path: str
    ) -> None:
        """Verify the output matches QuarantineSchema.RECORD's shape."""
        csv_dir = Path(temp_delta_path) / "corrupt_source"
        csv_dir.mkdir()
        csv_file = csv_dir / "corrupt.csv"
        csv_file.write_text(
            "store_id,_corrupt_record,_source_file\n"
            "STORE-001,\"STORE-001,broken\",fulfillment/date=2026-08-30/STORE-001.csv\n"
        )

        corrupt_df = spark_session.read.option("header", "true").csv(str(csv_file))

        result = build_malformed_row_quarantine_df(
            corrupt_df, "_corrupt_record", "fulfillment", "2026-08-30"
        )

        assert set(result.columns) == {
            "source",
            "partition_date",
            "key",
            "reason",
            "detected_at",
            "raw_snippet",
        }
        row = result.collect()[0]
        assert row["source"] == "fulfillment"
        assert row["partition_date"] == "2026-08-30"
        assert row["key"] == "fulfillment/date=2026-08-30/STORE-001.csv"
        assert row["reason"] == "malformed_row"
        assert row["raw_snippet"] == "STORE-001,broken"
        assert row["detected_at"] is not None
