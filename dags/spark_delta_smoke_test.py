"""Smoke test for the custom Airflow image.

Confirms the image can build a PySpark session with Delta Lake enabled inside
a task, using the same JDK/pyspark/delta-spark versions as the local dev venv.
Not part of the ingestion pipeline — run manually after `docker compose up`
to prove the image works before building real DAGs on top of it.
"""

from __future__ import annotations

from importlib.metadata import version

import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="spark_delta_smoke_test",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["infra", "smoke-test"],
)
def spark_delta_smoke_test():
    @task
    def check_spark_delta() -> None:
        """Build a local Spark session with Delta enabled and log versions."""
        import pyspark
        from delta import configure_spark_with_delta_pip
        from pyspark.sql import SparkSession

        builder = (
            SparkSession.builder.appName("veloz-smoke-test")
            .master("local[*]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )
        spark = configure_spark_with_delta_pip(builder).getOrCreate()

        print(f"pyspark version: {pyspark.__version__}")
        print(f"delta-spark version: {version('delta-spark')}")
        print(f"spark version: {spark.version}")

        df = spark.range(3)
        df.show()

        spark.stop()

    check_spark_delta()


spark_delta_smoke_test()
