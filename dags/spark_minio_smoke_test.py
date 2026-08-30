"""Smoke test for the MinIO / s3a connection.

Confirms a task can build a Spark session that talks Delta over s3a:// to the
MinIO service in the compose stack, using the ingest (no-delete) credentials the
containers get by default. Infrastructure verification only — not Bronze
ingestion; run manually after `docker compose up` to prove s3a works before
building real ingestion DAGs on top of it.

The throwaway table is left behind on purpose: the ingest role has no
s3:DeleteObject, so a task running with the default credentials *cannot* clean
it up, which is the policy doing its job. `_smoke_test/` is namespaced out of
the way and re-runs overwrite it (Delta overwrite rewrites the log, it does not
delete data files).
"""

from __future__ import annotations

import os

import pendulum
from airflow.sdk import dag, task

# hadoop-aws must match the Hadoop that pyspark 3.5.3 bundles
# (hadoop-client-api-3.3.4.jar), and aws-java-sdk-bundle must be the version
# hadoop-aws 3.3.4 was built against (hadoop-project-3.3.4.pom pins 1.12.262).
# Mixing versions here is the usual source of NoSuchMethodError at first write.
# Resolved at runtime via Ivy, same as Delta's own jars — nothing baked into
# the image; the first run downloads ~200MB.
HADOOP_AWS_PACKAGES = [
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
]

SMOKE_TEST_PATH = "s3a://bronze-veloz/_smoke_test/"


@dag(
    dag_id="spark_minio_smoke_test",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["infra", "smoke-test"],
)
def spark_minio_smoke_test():
    @task
    def check_spark_minio() -> None:
        """Write a tiny Delta table to MinIO over s3a and read it back."""
        from delta import configure_spark_with_delta_pip
        from pyspark.sql import SparkSession

        endpoint = os.environ["MINIO_ENDPOINT"]

        builder = (
            SparkSession.builder.appName("veloz-minio-smoke-test")
            .master("local[*]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.endpoint", endpoint)
            .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
            .config(
                "spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"]
            )
            # MinIO serves one host for every bucket, so bucket-as-subdomain
            # addressing (the S3 default) doesn't resolve here.
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        )
        spark = configure_spark_with_delta_pip(
            builder, extra_packages=HADOOP_AWS_PACKAGES
        ).getOrCreate()

        print(f"minio endpoint: {endpoint}")
        print(f"smoke test path: {SMOKE_TEST_PATH}")

        spark.range(3).write.format("delta").mode("overwrite").save(SMOKE_TEST_PATH)

        df = spark.read.format("delta").load(SMOKE_TEST_PATH)
        df.show()
        print(f"rows read back from MinIO: {df.count()}")

        spark.stop()

    check_spark_minio()


spark_minio_smoke_test()
