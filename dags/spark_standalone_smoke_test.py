"""Smoke test for the Spark Standalone cluster.

Sibling to `dags/spark_minio_smoke_test.py` (that DAG's local-mode behavior
is untouched by this one) -- proves a task can submit a real Spark
application to the shared Standalone cluster (`spark-master`/`spark-worker`
in `docker-compose.yml`, via `plugins/spark_session.py`'s
`StandaloneSparkSessionFactory`) and still complete the same kind of
Delta-over-s3a round trip the local-mode smoke test does. Infrastructure
verification only, not Bronze ingestion.

Replaces the retired `spark_yarn_smoke_test` DAG (see docs/ADR.md's dated
appendix entry for why this repo moved off Hadoop YARN).

The throwaway table is left behind on purpose, same reasoning as the
local-mode smoke test: the ingest role has no s3:DeleteObject, so a task
running with the default credentials *cannot* clean it up.
"""

from __future__ import annotations

import pendulum
from airflow.sdk import dag, task

SMOKE_TEST_PATH = "s3a://bronze-veloz/_smoke_test_standalone/"


@dag(
    dag_id="spark_standalone_smoke_test",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["infra", "smoke-test", "spark-standalone"],
)
def spark_standalone_smoke_test():
    @task
    def check_spark_standalone() -> None:
        """Submit a tiny Delta write/read to the cluster, prove it ran there."""
        # plugins/ is mounted at /opt/airflow/plugins, which Airflow puts on
        # sys.path for every DAG/task -- same import path any DAG under
        # dags/ would use.
        from spark_session import StandaloneClusterConfig, StandaloneSparkSessionFactory

        spark = StandaloneSparkSessionFactory(
            app_name="veloz-standalone-smoke-test",
            cluster_config=StandaloneClusterConfig.from_env(),
        ).get_session()

        print(f"spark master: {spark.sparkContext.master}")
        print(f"spark application id: {spark.sparkContext.applicationId}")
        print(f"smoke test path: {SMOKE_TEST_PATH}")

        spark.range(3).write.format("delta").mode("overwrite").save(SMOKE_TEST_PATH)

        df = spark.read.format("delta").load(SMOKE_TEST_PATH)
        df.show()
        print(f"rows read back from MinIO: {df.count()}")

        spark.stop()

    check_spark_standalone()


spark_standalone_smoke_test()
