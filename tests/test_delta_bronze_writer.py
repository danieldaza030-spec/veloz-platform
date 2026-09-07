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

    @pytest.mark.parametrize(
        ("window_column", "window_values"),
        [
            pytest.param("_ingestion_window", None, id="column_without_values"),
            pytest.param(None, ["20260830T000000Z"], id="values_without_column"),
        ],
    )
    def test_mismatched_window_column_and_values_raises_error(
        self, window_column: str | None, window_values: list[str] | None
    ) -> None:
        """Verify ValueError is raised when only one of window_column/window_values is set."""
        writer = DeltaBronzeWriter(
            path="/tmp/test",
            partition_columns=["_extract_date"],
        )

        class MockDF:
            pass

        with pytest.raises(
            ValueError,
            match=r"window_column and window_values must both be set or both be None",
        ):
            writer.write(
                MockDF(),  # type: ignore
                partition_column="_extract_date",
                partition_value="2026-08-30",
                window_column=window_column,
                window_values=window_values,
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
