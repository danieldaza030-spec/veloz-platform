"""Ingests a day's raw orders extract into the Bronze layer.

Reads `s3a://raw-incoming-data/orders/orders_<date>.csv` (written by
`generate_orders`, see `dags/generate_orders.py`), applies
`metadata.orders_schema.OrdersSchema.RAW` explicitly (no `inferSchema` — a
column silently changing type on the raw side should fail this task loudly,
not get silently coerced), tags each row with ingestion lineage, and writes
it into the Delta table at `s3a://bronze-veloz/orders/`.

This is platform-layer logic (Bronze schema normalization), not infra glue —
built here only because the engineer explicitly opted to change the usual
CLAUDE.md boundary for this one piece of work. It does not deduplicate
orders across days (an order's state can legitimately appear in multiple
daily extracts as it progresses through its lifecycle) — that collapse into
"one row per order" is documented as Silver's job in `PROGRESS.md`'s
platform-build table, not Bronze's. Bronze is intentionally an append-only,
per-extract-date audit trail: exactly what `docs/data-sources.md` #1 already
says the raw source is ("each row is one order's state as of extract time,
not a changelog entry") preserved as-is, one partition per extract date, so
G2's "what did yesterday's numbers look like" is answerable directly off
Bronze without needing Delta time travel for routine lookups.

Idempotency: the write uses `replaceWhere` to atomically overwrite only the
`_extract_date` partition being ingested, rather than a plain `append`. A
plain append would double-count rows on any Airflow retry that ran after a
partially-or-fully successful write — `replaceWhere` makes re-running this
task for the same date (retry, backfill, or manual re-trigger) safe by
construction, which is what G3 ("resilient... without someone manually
intervening") actually requires from a scheduled loader, not just a green
task the first time.

Schedule: 01:10 UTC, a 10-minute buffer after `generate_orders`' 01:00 UTC
run — same fixed-offset pattern `generate_payments`/`generate_rider_events`
already use to wait on that same file, chosen there over an Asset trigger
because these DAGs simulate independent real systems. This DAG is different
in kind (it's platform ingestion pulling from a landing zone, not another
simulated upstream) but a real ingestion job polling/scheduled off a landing
bucket is exactly as fixed-cadence in practice, so the same pattern is kept
for consistency rather than introducing Asset-based coupling for one DAG.
Missing file at that time (orders run late/failed) surfaces as a clear
Spark `AnalysisException: Path does not exist`, which Airflow retries
(3 attempts, 5 min apart) and then alerts on via a failed task -- exactly
the "retry automatically, alert when it can't" split G3 asks for.
"""

from __future__ import annotations

import pendulum
from airflow.sdk import Param, dag, get_current_context, task
from dag_defaults import BRONZE_DEFAULT_ARGS

ORDERS_RAW_PREFIX = "orders"
ORDERS_BRONZE_PREFIX = "orders"
EXTRACT_DATE_COLUMN = "_extract_date"


@dag(
    dag_id="ingest_orders_bronze",
    schedule="10 1 * * *",  # 01:10 UTC daily -- 10 min after generate_orders' 01:00 UTC run
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["bronze", "orders"],
    default_args=BRONZE_DEFAULT_ARGS,
    params={
        "date": Param(
            default=None,
            type=["string", "null"],
            format="date",
            description=(
                "Extract date to ingest, YYYY-MM-DD. Defaults to the run's "
                "logical date (ds), same fallback generate_orders uses."
            ),
        ),
    },
)
def ingest_orders_bronze():
    @task
    def run() -> None:
        # plugins/ is on sys.path for every DAG/task (see spark_session's own
        # bare-name import above); metadata/ is mounted as a subdirectory of
        # it in docker-compose.yml specifically so this import works without
        # any extra path wiring. application/ and infrastructure/ sit next
        # to metadata/ at the repo root and follow the same lazy-import
        # pattern, deferring pyspark-dependent imports out of DAG parse time.
        from application.bronze_ingestion import BronzeIngestionRequest, ingest_to_bronze
        from metadata.buckets import Buckets
        from metadata.orders_schema import OrdersSchema
        from spark_session import StandaloneClusterConfig, StandaloneSparkSessionFactory

        context = get_current_context()
        params = context["params"]
        target_date = params["date"] or context.get("ds") or pendulum.now("UTC").to_date_string()

        raw_path = f"s3a://{Buckets.RAW_INCOMING_DATA}/{ORDERS_RAW_PREFIX}/orders_{target_date}.csv"
        bronze_path = f"s3a://{Buckets.BRONZE}/{ORDERS_BRONZE_PREFIX}/"

        spark = StandaloneSparkSessionFactory(
            app_name="veloz-ingest-orders-bronze",
            cluster_config=StandaloneClusterConfig.from_env(),
        ).get_session()

        try:
            request = BronzeIngestionRequest(
                raw_path=raw_path,
                raw_format="csv",
                schema=OrdersSchema.RAW,
                read_options={
                    "header": "true",
                    # FAILFAST: fail loudly on any row that doesn't match
                    # OrdersSchema.RAW instead of coercing it.
                    "mode": "FAILFAST",
                },
                bronze_path=bronze_path,
                partition_column=EXTRACT_DATE_COLUMN,
                extract_date=target_date,
            )

            print(f"reading raw orders extract: {raw_path}")
            row_count = ingest_to_bronze(spark, request)
            print(
                f"wrote {row_count} rows to {bronze_path} "
                f"(partition {EXTRACT_DATE_COLUMN}={target_date})"
            )
        finally:
            spark.stop()

    run()


ingest_orders_bronze()
