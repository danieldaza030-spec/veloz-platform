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

RAW_BUCKET = "raw-incoming-data"
BRONZE_BUCKET = "bronze-veloz"
ORDERS_RAW_PREFIX = "orders"
ORDERS_BRONZE_PREFIX = "orders"


@dag(
    dag_id="ingest_orders_bronze",
    schedule="10 1 * * *",  # 01:10 UTC daily -- 10 min after generate_orders' 01:00 UTC run
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["bronze", "orders"],
    default_args={
        "retries": 3,
        "retry_delay": pendulum.duration(minutes=5),
    },
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
        # any extra path wiring.
        from metadata.orders_schema import OrdersSchema
        from spark_session import StandaloneClusterConfig, StandaloneSparkSessionFactory

        context = get_current_context()
        params = context["params"]
        target_date = params["date"] or context.get("ds") or pendulum.now("UTC").to_date_string()

        raw_path = f"s3a://{RAW_BUCKET}/{ORDERS_RAW_PREFIX}/orders_{target_date}.csv"
        bronze_path = f"s3a://{BRONZE_BUCKET}/{ORDERS_BRONZE_PREFIX}/"

        spark = StandaloneSparkSessionFactory(
            app_name="veloz-ingest-orders-bronze",
            cluster_config=StandaloneClusterConfig.from_env(),
        ).get_session()

        try:
            from pyspark.sql.functions import current_timestamp, input_file_name, lit

            print(f"reading raw orders extract: {raw_path}")
            raw_df = (
                spark.read.format("csv")
                .schema(OrdersSchema.RAW)
                .option("header", "true")
                # FAILFAST, not the default PERMISSIVE: a row that doesn't
                # match OrdersSchema.RAW should fail this task loudly (G3's
                # "alert when it can't recover"), not land in Bronze as a
                # silently null-padded row. orders.py doesn't inject
                # malformed rows the way fulfillment.py's --bad-night does,
                # so a parse failure here means real, unexpected schema
                # drift worth stopping on, not routine messiness to absorb.
                .option("mode", "FAILFAST")
                .load(raw_path)
            )

            bronze_df = raw_df.withColumn("_extract_date", lit(target_date).cast("date")).withColumn(
                "_ingested_at", current_timestamp()
            ).withColumn("_source_file", input_file_name())

            row_count = bronze_df.count()
            print(f"read {row_count} rows for extract_date={target_date}")

            (
                bronze_df.write.format("delta")
                .mode("overwrite")
                .option("replaceWhere", f"_extract_date = '{target_date}'")
                .partitionBy("_extract_date")
                .save(bronze_path)
            )

            print(f"wrote {row_count} rows to {bronze_path} (partition _extract_date={target_date})")
        finally:
            spark.stop()

    run()


ingest_orders_bronze()
