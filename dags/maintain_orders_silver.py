"""Compacts and Z-ORDERs the Silver orders Delta table, then reclaims stale files.

A separate, time-scheduled DAG rather than a task appended to `dags.
ingest_orders_silver`'s write path: that DAG's MERGE runs dozens of times a
day, straight off `ORDERS_BRONZE_ASSET` events, and needs to stay fast per
batch -- bolting a full-table OPTIMIZE/VACUUM onto every one of those runs
would make each Bronze-triggered MERGE pay a full-table-scan cost it has no
need for, just to keep the table compacted for reads that happen on their
own, independent cadence.

Schedule: `"30 6 * * *"` (06:30 UTC daily). Off-peak across all three
markets (Medellín/Bogotá at UTC-5, São Paulo at UTC-3, so 06:30 UTC lands
overnight/very early morning local in every one of them) and comfortably
ahead of Finance's 8am-local reconciliation deadline (G2), so the ZORDERed
layout is already in place for that morning's heaviest read pattern
instead of running concurrently with it.

VACUUM retention: `VACUUM_RETENTION_HOURS = 168` (7 days) -- Delta's own
built-in safe default, kept as-is rather than shortened. Silver's
"one-row-per-order_id, updated in place by MERGE" grain means VACUUM is
the only thing that reclaims a merged row's superseded file versions;
168 hours is long enough to cover any in-flight long-running read or
time-travel query against recent history without disabling Delta's
`retentionDurationCheck` safety guard (which refuses a shorter retention
unless a caller explicitly turns that guard off), and short auditability
needs beyond that window are already served by Bronze's own untouched,
append-only retention (see `dags.ingest_orders_bronze`'s docstring) --
this job does not need a longer retention to satisfy G2's "what did
yesterday's numbers look like" on its own.

ZORDER BY (order_id, store_id): `order_id` matches `infrastructure.
delta_silver_merge_writer`'s own MERGE join key, and `store_id` matches
the Ops-facing, store-by-store queries G1 asks for -- both point-lookup
and store-scoped range reads benefit from the same Z-order without
needing two separate physical layouts.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pendulum
from airflow.sdk import dag, task
from dag_defaults import SILVER_DEFAULT_ARGS
from metadata.buckets import ORDERS_SILVER_PREFIX, Buckets

if TYPE_CHECKING:
    # Type-checking only: `_run_maintenance`'s `spark: SparkSession` hint is
    # never evaluated at import time (`from __future__ import annotations`
    # postpones it, see that function's own docstring) -- guarding this
    # import keeps pyspark out of DAG-parse time while still giving static
    # analysis the real symbol instead of an undefined name.
    from pyspark.sql import SparkSession

ZORDER_COLUMNS = ("order_id", "store_id")

VACUUM_RETENTION_HOURS = 168
"""Delta's own default safe retention (7 days) -- see this module's
docstring for why it's kept, not shortened."""


def _run_maintenance(
    spark: SparkSession,
    path: str,
    zorder_columns: tuple[str, ...],
    vacuum_retention_hours: int,
) -> None:
    """Runs `OPTIMIZE ... ZORDER BY <zorder_columns>` then `VACUUM` on the Delta table at `path`.

    The `SparkSession` type hint above is never evaluated at import time
    (`from __future__ import annotations` postpones it), and `delta.
    tables` is imported inside this function body rather than at module
    top level -- both deferring the pyspark-dependent surface out of
    DAG-parse time, same pattern `dags.ingest_orders_bronze.run` uses for
    its own `application`/`spark_session` imports (see that module's
    docstring).

    Args:
        spark: Active SparkSession.
        path: Delta table location to maintain, e.g.
            `s3a://silver-veloz/orders/`.
        zorder_columns: Columns to `ZORDER BY`, in order.
        vacuum_retention_hours: Hours of stale file history to retain;
            `VACUUM_RETENTION_HOURS` is the only value this DAG passes,
            kept as a parameter so a test can exercise this function
            without depending on that module-level constant directly.
    """
    from delta.tables import DeltaTable

    table = DeltaTable.forPath(spark, path)
    table.optimize().executeZOrderBy(*zorder_columns)
    table.vacuum(vacuum_retention_hours)


@dag(
    dag_id="maintain_orders_silver",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["silver", "orders", "maintenance"],
    default_args={
        **SILVER_DEFAULT_ARGS,
        # A full-table OPTIMIZE+ZORDER+VACUUM rewrites/ZORDERs and vacuums the
        # *entire* accumulating Silver orders table, not one bounded ~17k-order
        # batch (SILVER_DEFAULT_ARGS's 30-minute budget) -- at the
        # 5,000,000-orders/day design target that full-table pass would blow
        # through 30 minutes and die on timeout. 3 hours is materially longer
        # while still leaving a safe buffer before this 06:30 UTC start's
        # tightest downstream deadline: Sao Paulo's 8am-local Finance standup
        # (11:00 UTC, 4.5 hours after this DAG starts).
        "execution_timeout": pendulum.duration(hours=3),
    },
)
def maintain_orders_silver():
    @task
    def run() -> None:
        # Deferred out of DAG-parse time, same as dags.ingest_orders_bronze.
        from spark_session import StandaloneClusterConfig, StandaloneSparkSessionFactory

        silver_path = f"s3a://{Buckets.SILVER}/{ORDERS_SILVER_PREFIX}/"

        spark = StandaloneSparkSessionFactory(
            app_name="veloz-maintain-orders-silver",
            cluster_config=StandaloneClusterConfig.from_env(worker_count=2, cores_max=4),
        ).get_session()

        try:
            _run_maintenance(spark, silver_path, ZORDER_COLUMNS, VACUUM_RETENTION_HOURS)
            print(
                f"optimized+vacuumed {silver_path} "
                f"(ZORDER BY {ZORDER_COLUMNS}, retained {VACUUM_RETENTION_HOURS}h)"
            )
        finally:
            spark.stop()

    run()


maintain_orders_silver()
