"""Shared test fixtures for unit/integration tests."""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path

import pytest
from pyspark.sql import SparkSession


# Workaround for Python 3.14 + PySpark 3.5.3 cloudpickle recursion issue
# Increase the recursion limit to avoid stack overflow during serialization
sys.setrecursionlimit(10000)


@pytest.fixture
def spark_session() -> SparkSession:
    """Build a local Spark session with Delta enabled for testing.

    Uses `local[1]` mode for isolated test execution. Manually configures
    Delta extensions since configure_spark_with_delta_pip has issues in
    test environments.
    """
    try:
        # Try to use configure_spark_with_delta_pip with manual extension config
        from delta import configure_spark_with_delta_pip

        builder = (
            SparkSession.builder
            .appName("veloz-test")
            .master("local[1]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
    except Exception as e:
        # Fallback: manual configuration
        builder = (
            SparkSession.builder
            .appName("veloz-test")
            .master("local[1]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )
        try:
            spark = builder.getOrCreate()
        except Exception as fallback_e:
            print(f"Warning: Delta configuration failed: {e}, {fallback_e}")
            # Last resort: plain Spark
            spark = (
                SparkSession.builder
                .appName("veloz-test")
                .master("local[1]")
                .getOrCreate()
            )

    yield spark
    spark.stop()


@pytest.fixture
def temp_delta_path() -> str:
    """Provide a temporary directory for Delta table writes.

    Each test gets a fresh temp path to avoid cross-test pollution.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir
