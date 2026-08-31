"""Tests for application/bronze_ingestion.py."""

from __future__ import annotations

from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType

from application.bronze_ingestion import (
    BronzeIngestionRequest,
    SplitBronzeIngestionRequest,
    add_lineage_columns,
    ingest_clean_rows_to_bronze,
    ingest_to_bronze,
    split_clean_and_corrupt_rows,
    with_corrupt_record_column,
)




class TestIngestToBronze:
    """Test end-to-end ingestion flow."""

    def test_ingest_to_bronze_basic(
        self, spark_session: SparkSession, temp_delta_path: str
    ) -> None:
        """Verify ingest_to_bronze reads, tags, and writes data."""
        # Create a simple CSV file
        csv_dir = Path(temp_delta_path) / "raw"
        csv_dir.mkdir()
        csv_path = csv_dir / "test.csv"

        csv_content = (
            "order_id,store_id,value\n"
            "order_1,store_1,100\n"
            "order_2,store_1,200\n"
        )
        csv_path.write_text(csv_content)

        schema = StructType([
            StructField("order_id", StringType(), False),
            StructField("store_id", StringType(), False),
            StructField("value", StringType(), False),
        ])

        bronze_path = str(Path(temp_delta_path) / "bronze")

        request = BronzeIngestionRequest(
            raw_path=str(csv_path),
            raw_format="csv",
            schema=schema,
            read_options={"header": "true", "mode": "FAILFAST"},
            bronze_path=bronze_path,
            partition_column="_extract_date",
            extract_date="2026-08-30",
        )

        row_count = ingest_to_bronze(spark_session, request)

        assert row_count == 2
        result = spark_session.read.format("delta").load(bronze_path)
        assert result.count() == 2

        columns = result.columns
        assert "_extract_date" in columns
        assert "_ingested_at" in columns
        assert "_source_file" in columns

    def test_ingest_to_bronze_idempotent(
        self, spark_session: SparkSession, temp_delta_path: str
    ) -> None:
        """Verify re-running for same partition is idempotent."""
        csv_dir = Path(temp_delta_path) / "raw"
        csv_dir.mkdir()
        csv_path = csv_dir / "test.csv"

        csv_content = (
            "order_id,store_id,value\n"
            "order_1,store_1,100\n"
            "order_2,store_1,200\n"
        )
        csv_path.write_text(csv_content)

        schema = StructType([
            StructField("order_id", StringType(), False),
            StructField("store_id", StringType(), False),
            StructField("value", StringType(), False),
        ])

        bronze_path = str(Path(temp_delta_path) / "bronze")

        request = BronzeIngestionRequest(
            raw_path=str(csv_path),
            raw_format="csv",
            schema=schema,
            read_options={"header": "true", "mode": "FAILFAST"},
            bronze_path=bronze_path,
            partition_column="_extract_date",
            extract_date="2026-08-30",
        )

        # First run
        row_count_1 = ingest_to_bronze(spark_session, request)
        assert row_count_1 == 2

        result_1 = spark_session.read.format("delta").load(bronze_path)
        count_1 = result_1.count()

        # Second run (same partition)
        row_count_2 = ingest_to_bronze(spark_session, request)
        assert row_count_2 == 2

        result_2 = spark_session.read.format("delta").load(bronze_path)
        count_2 = result_2.count()

        assert count_2 == count_1, "Should be idempotent"
        assert count_2 == 2

    def test_ingest_to_bronze_multiple_partitions(
        self, spark_session: SparkSession, temp_delta_path: str
    ) -> None:
        """Verify different partitions are both written."""
        csv_dir = Path(temp_delta_path) / "raw"
        csv_dir.mkdir()

        csv_path_1 = csv_dir / "test_1.csv"
        csv_content_1 = (
            "order_id,store_id,value\n"
            "order_1,store_1,100\n"
        )
        csv_path_1.write_text(csv_content_1)

        csv_path_2 = csv_dir / "test_2.csv"
        csv_content_2 = (
            "order_id,store_id,value\n"
            "order_2,store_1,200\n"
        )
        csv_path_2.write_text(csv_content_2)

        schema = StructType([
            StructField("order_id", StringType(), False),
            StructField("store_id", StringType(), False),
            StructField("value", StringType(), False),
        ])

        bronze_path = str(Path(temp_delta_path) / "bronze")

        # Ingest first partition
        request_1 = BronzeIngestionRequest(
            raw_path=str(csv_path_1),
            raw_format="csv",
            schema=schema,
            read_options={"header": "true", "mode": "FAILFAST"},
            bronze_path=bronze_path,
            partition_column="_extract_date",
            extract_date="2026-08-29",
        )
        ingest_to_bronze(spark_session, request_1)

        # Ingest second partition
        request_2 = BronzeIngestionRequest(
            raw_path=str(csv_path_2),
            raw_format="csv",
            schema=schema,
            read_options={"header": "true", "mode": "FAILFAST"},
            bronze_path=bronze_path,
            partition_column="_extract_date",
            extract_date="2026-08-30",
        )
        ingest_to_bronze(spark_session, request_2)

        result = spark_session.read.format("delta").load(bronze_path)
        assert result.count() == 2


class TestWithCorruptRecordColumn:
    """Test the PERMISSIVE-mode schema-extension helper."""

    def test_appends_nullable_string_column(self) -> None:
        """Verify the corrupt-record column is appended, nullable, string."""
        base_schema = StructType(
            [
                StructField("store_id", StringType(), False),
                StructField("value", IntegerType(), True),
            ]
        )

        extended = with_corrupt_record_column(base_schema, "_corrupt_record")

        assert [f.name for f in extended.fields] == ["store_id", "value", "_corrupt_record"]
        corrupt_field = extended["_corrupt_record"]
        assert isinstance(corrupt_field.dataType, StringType)
        assert corrupt_field.nullable is True

    def test_does_not_mutate_base_schema(self) -> None:
        """Verify the base schema is left untouched (no shared-schema mutation)."""
        base_schema = StructType([StructField("store_id", StringType(), False)])
        original_field_count = len(base_schema.fields)

        with_corrupt_record_column(base_schema, "_corrupt_record")

        assert len(base_schema.fields) == original_field_count


class TestSplitCleanAndCorruptRows:
    """Test the clean/corrupt row split used by PERMISSIVE-mode sources."""

    def test_splits_by_corrupt_record_nullity(
        self, spark_session: SparkSession, temp_delta_path: str
    ) -> None:
        """Verify rows are partitioned by whether the corrupt column is set."""
        # Built from a CSV file, not spark.createDataFrame(list[...]): the
        # latter hits a Python 3.14 + PySpark 3.5.3 cloudpickle recursion
        # issue in this local test environment (see test_quarantine_writer.py
        # for the same workaround).
        csv_dir = Path(temp_delta_path) / "raw"
        csv_dir.mkdir()
        csv_path = csv_dir / "test.csv"
        csv_path.write_text(
            "store_id,quantity\n"
            "STORE-001,10\n"
            "STORE-002,10,extra_field\n"
        )

        schema = with_corrupt_record_column(
            StructType(
                [
                    StructField("store_id", StringType(), True),
                    StructField("quantity", IntegerType(), True),
                ]
            ),
            "_corrupt_record",
        )
        df = (
            spark_session.read.format("csv")
            .schema(schema)
            .option("header", "true")
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .load(str(csv_path))
        )

        clean_df, corrupt_df = split_clean_and_corrupt_rows(df, "_corrupt_record")

        assert clean_df.count() == 1
        assert clean_df.columns == ["store_id"]
        assert corrupt_df.count() == 1
        assert corrupt_df.collect()[0]["store_id"] == "STORE-002"


class TestIngestCleanRowsToBronze:
    """Test the split-then-write Bronze ingestion path."""

    def test_writes_only_clean_rows_and_returns_corrupt_rows(
        self, spark_session: SparkSession, temp_delta_path: str
    ) -> None:
        """Verify clean rows land in Bronze and corrupt rows come back to the caller."""
        csv_dir = Path(temp_delta_path) / "raw"
        csv_dir.mkdir()
        csv_path = csv_dir / "STORE-001.csv"
        csv_path.write_text(
            "store_id,quantity\n"
            "STORE-001,10\n"
            "STORE-001,5,extra_field\n"  # malformed: more tokens than the schema
        )

        schema = with_corrupt_record_column(
            StructType(
                [
                    StructField("store_id", StringType(), False),
                    StructField("quantity", IntegerType(), True),
                ]
            ),
            "_corrupt_record",
        )
        bronze_path = str(Path(temp_delta_path) / "bronze")

        request = SplitBronzeIngestionRequest(
            raw_path=str(csv_path),
            raw_format="csv",
            schema=schema,
            read_options={
                "header": "true",
                "mode": "PERMISSIVE",
                "columnNameOfCorruptRecord": "_corrupt_record",
            },
            bronze_path=bronze_path,
            partition_column="_extract_date",
            extract_date="2026-08-30",
            corrupt_record_column="_corrupt_record",
            derive_partition_column=True,
        )

        row_count, corrupt_df = ingest_clean_rows_to_bronze(spark_session, request)

        assert row_count == 1
        assert corrupt_df.count() == 1
        corrupt_row = corrupt_df.collect()[0]
        assert corrupt_row["store_id"] == "STORE-001"
        assert "extra_field" in corrupt_row["_corrupt_record"]
        assert corrupt_row["_source_file"]  # tagged before caching, must be non-empty

        bronze = spark_session.read.format("delta").load(bronze_path)
        assert bronze.count() == 1
        assert "_corrupt_record" not in bronze.columns
        assert "_source_file" in bronze.columns
        assert "_ingested_at" in bronze.columns
