"""SparkSession factory for the shared Spark Standalone cluster.

Infra glue only: this module builds and returns a `SparkSession` configured
to submit to the Standalone cluster in `docker-compose.yml` (`spark-master`
/ `spark-worker`), the same way `dags/spark_minio_smoke_test.py` builds one
for `local[*]`. It contains no Bronze/Silver/Gold transformation,
reconciliation or dashboard logic — DAG tasks call `get_session()` and do
their own work with the result, same division as everywhere else in this
repo.

Replaces the earlier `YarnSparkSessionFactory` (see docs/ADR.md's dated
appendix entry for why this repo moved off Hadoop YARN). Standalone mode
needs none of that factory's YARN-specific plumbing (no shared staging
filesystem, no `spark.yarn.archive` of Spark's own jars for a bare-Hadoop
NodeManager to run an executor against): every `spark-worker` container is
built from the same image as this driver, so it already has a matching
Spark install, and Standalone ships the driver's resolved
jars/Ivy-packages to executors itself, over the network, as ordinary task
submission.

Lives under `plugins/` because that's already the Airflow shared-code mount
point (`./plugins:/opt/airflow/plugins` in `x-airflow-common`), so any DAG
under `dags/` can `from spark_session import StandaloneSparkSessionFactory`
without a package install step.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

# hadoop-aws must match the Hadoop that pyspark 3.5.3 bundles
# (hadoop-client-api-3.3.4.jar), and aws-java-sdk-bundle must be the version
# hadoop-aws 3.3.4 was built against (hadoop-project-3.3.4.pom pins 1.12.262).
# Identical pinning to dags/spark_minio_smoke_test.py's HADOOP_AWS_PACKAGES —
# duplicated here rather than imported from the DAG module, since DAG files
# aren't meant to be imported as a library by other DAGs/plugins in this repo.
HADOOP_AWS_PACKAGES = [
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
]


@dataclass(frozen=True)
class StandaloneClusterConfig:
    """Explicit connection + sizing config for the shared Standalone cluster.

    Everything a `StandaloneSparkSessionFactory` needs to reach and size
    itself against the cluster lives on this one object — no scattered
    `os.environ` reads inside the factory itself. `from_env()` is the only
    place environment variables get read, mirroring how `docker-compose.yml`
    already passes this cluster's address/sizing into every Airflow
    container (`SPARK_MASTER_URL`, `SPARK_WORKER_CPU_LIMIT`,
    `SPARK_WORKER_MEM_LIMIT`) rather than a mounted config file, since
    Standalone (unlike the YARN cluster this replaces) has no shared
    rendered-conf file to read those numbers from instead.

    No credentials/auth field: Spark Standalone has no built-in
    username/password auth, and `spark.authenticate` (its one real
    auth mechanism, a shared-secret HMAC) is off in this cluster the same
    way it was for the YARN cluster this replaces — this project's posture
    everywhere else in `docker-compose.yml` is trusting the Docker network
    the containers already share, not authenticating each hop within it.
    If that posture changes, `spark.authenticate.secret` would need to be
    threaded through here, into `spark-master`/`spark-worker`'s own env, and
    into `.env` — flagged in this session's report rather than silently
    assumed; add it here if that's confirmed.
    """

    master_url: str
    executor_cores: int
    executor_memory_mb: int
    # Caps how many total cores a single application may hold across the
    # whole cluster at once (`spark.cores.max`). Defaulted to exactly one
    # worker's own core count below, not the whole pool's: a smoke test or
    # any other single DAG task has no business reserving the entire shared
    # cluster by default, only what one worker can give it.
    cores_max: int

    @classmethod
    def from_env(cls) -> "StandaloneClusterConfig":
        """Build config from the env vars `docker-compose.yml` sets.

        `SPARK_WORKER_CPU_LIMIT`/`SPARK_WORKER_MEM_LIMIT` are read here
        rather than redeclared: they're the exact same values driving
        `spark-worker`'s own `deploy.resources.limits` in
        `docker-compose.yml`, so a default executor request built from them
        can never ask a worker for more than its own container actually has.
        """
        worker_cores = int(os.environ.get("SPARK_WORKER_CPU_LIMIT", "2"))
        worker_memory_mb = int(os.environ.get("SPARK_WORKER_MEM_LIMIT", "4096"))
        return cls(
            master_url=os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077"),
            executor_cores=worker_cores,
            executor_memory_mb=worker_memory_mb,
            cores_max=worker_cores,
        )


class StandaloneSparkSessionFactory:
    """Builds `SparkSession`s that submit to the shared Standalone cluster.

    Mirrors `dags/spark_minio_smoke_test.py`'s local-mode Delta/s3a config
    exactly (same Delta extensions, same s3a/MinIO settings pulled from the
    container's own env vars -- nothing hardcoded here either), swapping
    only the master URL and executor sizing a shared cluster needs.
    """

    def __init__(
        self,
        app_name: str,
        cluster_config: StandaloneClusterConfig,
        extra_packages: list[str] | None = None,
    ) -> None:
        """Args:
        app_name: Spark application name, shown in the Master UI/
            `spark-submit --status` -- pick something that identifies which
            DAG task submitted it.
        cluster_config: explicit connection + sizing config, e.g.
            `StandaloneClusterConfig.from_env()`. Not defaulted implicitly
            inside this constructor -- a caller has to build one and hand it
            over, so it's always visible what a given session actually asked
            the cluster for.
        extra_packages: additional Ivy coordinates beyond
            `HADOOP_AWS_PACKAGES`, for callers that need more than Delta +
            s3a (e.g. a Kafka connector for the streaming consumer).
        """
        self.app_name = app_name
        self.cluster_config = cluster_config
        self.extra_packages = extra_packages or []

    def get_session(self):
        """Return a fully configured `SparkSession` targeting the cluster."""
        from delta import configure_spark_with_delta_pip
        from pyspark.sql import SparkSession

        endpoint = os.environ["MINIO_ENDPOINT"]
        cfg = self.cluster_config

        builder = (
            SparkSession.builder.appName(self.app_name)
            .master(cfg.master_url)
            .config("spark.executor.cores", cfg.executor_cores)
            .config("spark.executor.memory", f"{cfg.executor_memory_mb}m")
            .config("spark.cores.max", cfg.cores_max)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.endpoint", endpoint)
            .config(
                "spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"]
            )
            .config(
                "spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"]
            )
            # MinIO serves one host for every bucket, so bucket-as-subdomain
            # addressing (the S3 default) doesn't resolve here -- identical
            # to dags/spark_minio_smoke_test.py's reasoning.
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        )
        return configure_spark_with_delta_pip(
            builder, extra_packages=HADOOP_AWS_PACKAGES + self.extra_packages
        ).getOrCreate()
