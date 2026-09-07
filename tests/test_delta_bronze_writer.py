"""Tests for infrastructure/delta_bronze_writer.py."""

from __future__ import annotations

from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from infrastructure.delta_bronze_writer import DeltaBronzeWriter


class TestDeltaBronzeWriterValidation:
    """Test input validation."""

    def test_partition_value_with_single_quote_raises_error(self) -> None:
        """Verify ValueError is raised when partition_value contains single quote."""
        writer = DeltaBronzeWriter(
            path="/tmp/test",
            partition_columns=["_extract_date"],
        )

        # Mock DataFrame - we don't need a real one to test validation
        class MockDF:
            pass

        with pytest.raises(
            ValueError,
            match=r"partition_value must not contain a single quote",
        ):
            writer.write(
                MockDF(),  # type: ignore
                partition_column="_extract_date",
                partition_value="2026-08-30'; DROP TABLE --",
            )

    def test_window_values_with_single_quote_raises_error(self) -> None:
        """Verify ValueError is raised when a window_values element contains a single quote."""
        writer = DeltaBronzeWriter(
            path="/tmp/test",
            partition_columns=["_extract_date", "_ingestion_window"],
        )

        class MockDF:
            pass

        with pytest.raises(
            ValueError,
            match=r"window_values must not contain a single quote",
        ):
            writer.write(
                MockDF(),  # type: ignore
                partition_column="_extract_date",
                partition_value="2026-08-30",
                window_column="_ingestion_window",
                window_values=["20260830T000000Z'; DROP TABLE --"],
            )

    def test_window_values_without_window_column_raises_error(self) -> None:
        """Verify ValueError is raised when window_values is set without window_column."""
        writer = DeltaBronzeWriter(
            path="/tmp/test",
            partition_columns=["_extract_date"],
        )

        class MockDF:
            pass

        with pytest.raises(
            ValueError,
            match=r"window_values requires window_column to also be set",
        ):
            writer.write(
                MockDF(),  # type: ignore
                partition_column="_extract_date",
                partition_value="2026-08-30",
                window_column=None,
                window_values=["20260830T000000Z"],
            )


class TestDeltaBronzeWriterWindowOverwrite:
    """Test the window-scoped `replaceWhere` overwrite path against a real Delta table."""

    def test_overwriting_one_window_leaves_other_window_intact(
        self, spark_session: SparkSession, temp_delta_path: str
    ) -> None:
        """Verify replaceWhere on partition + window only touches the targeted window."""
        raw_dir = Path(temp_delta_path) / "raw"
        raw_dir.mkdir()

        first_csv = raw_dir / "first.csv"
        first_csv.write_text(
            "_extract_date,_ingestion_window,value\n"
            "2026-09-05,20260905T000000Z,100\n"
            "2026-09-05,20260905T000500Z,200\n"
        )

        bronze_path = str(Path(temp_delta_path) / "bronze")
        writer = DeltaBronzeWriter(
            path=bronze_path,
            partition_columns=["_extract_date", "_ingestion_window"],
        )

        first_df = spark_session.read.format("csv").option("header", "true").load(str(first_csv))
        writer.write(
            first_df,
            partition_column="_extract_date",
            partition_value="2026-09-05",
            window_column="_ingestion_window",
            window_values=["20260905T000000Z", "20260905T000500Z"],
        )

        result = spark_session.read.format("delta").load(bronze_path)
        assert result.count() == 2

        second_csv = raw_dir / "second.csv"
        second_csv.write_text("_extract_date,_ingestion_window,value\n2026-09-05,20260905T000000Z,999\n")
        second_df = spark_session.read.format("csv").option("header", "true").load(str(second_csv))
        writer.write(
            second_df,
            partition_column="_extract_date",
            partition_value="2026-09-05",
            window_column="_ingestion_window",
            window_values=["20260905T000000Z"],
        )

        result = spark_session.read.format("delta").load(bronze_path)
        rows = {row["_ingestion_window"]: row["value"] for row in result.collect()}

        assert result.count() == 2
        assert rows["20260905T000000Z"] == "999"
        assert rows["20260905T000500Z"] == "200"


class TestDeltaBronzeWriterWindowColumnWithoutValues:
    """Test the `window_column` set alone (no `window_values`) full-day overwrite path."""

    def test_replaces_whole_day_partition_across_all_windows(
        self, spark_session: SparkSession, temp_delta_path: str
    ) -> None:
        """window_column alone still partitions by window, but replaceWhere scopes the whole day."""
        raw_dir = Path(temp_delta_path) / "raw"
        raw_dir.mkdir()

        first_csv = raw_dir / "first.csv"
        first_csv.write_text(
            "_extract_date,_ingestion_window,value\n"
            "2026-09-05,20260905T000000Z,100\n"
            "2026-09-05,20260905T000500Z,200\n"
            "2026-09-06,20260906T000000Z,300\n"
        )

        bronze_path = str(Path(temp_delta_path) / "bronze")
        writer = DeltaBronzeWriter(
            path=bronze_path,
            partition_columns=["_extract_date", "_ingestion_window"],
        )

        first_df = spark_session.read.format("csv").option("header", "true").load(str(first_csv))
        # No single-quote validation error and no partition-mismatch error:
        # window_column set, window_values left None.
        writer.write(
            first_df,
            partition_column="_extract_date",
            partition_value="2026-09-05",
            window_column="_ingestion_window",
            window_values=None,
        )

        result = spark_session.read.format("delta").load(bronze_path)
        assert result.count() == 3

        # A second, full-day-glob re-run for the same date carries only one
        # of that day's windows -- since window_values is None, the
        # replaceWhere scope is the whole `_extract_date` partition, so the
        # window not present in this run's data must be dropped, not kept.
        second_csv = raw_dir / "second.csv"
        second_csv.write_text("_extract_date,_ingestion_window,value\n2026-09-05,20260905T999900Z,999\n")
        second_df = spark_session.read.format("csv").option("header", "true").load(str(second_csv))
        writer.write(
            second_df,
            partition_column="_extract_date",
            partition_value="2026-09-05",
            window_column="_ingestion_window",
            window_values=None,
        )

        result = spark_session.read.format("delta").load(bronze_path)
        rows_by_date = {}
        for row in result.collect():
            rows_by_date.setdefault(row["_extract_date"], {})[row["_ingestion_window"]] = row["value"]

        # 2026-09-06 partition is untouched by a replaceWhere scoped to 2026-09-05.
        assert rows_by_date["2026-09-06"] == {"20260906T000000Z": "300"}
        # 2026-09-05's whole-day scope replaced both prior windows with the
        # single window this run's data actually carried.
        assert rows_by_date["2026-09-05"] == {"20260905T999900Z": "999"}
