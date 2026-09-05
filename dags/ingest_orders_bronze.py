"""Ingests newly landed raw orders window-files into the Bronze layer.

`generate_orders` (the `generate_orders` task in
`dags/generate_orders_and_rider_events.py`) now runs every 5 minutes,
landing one object per run at
`s3a://raw-incoming-data/orders/date=<date>/orders_<run_timestamp>.csv`
instead of one file per day. This DAG is scheduled off `ORDERS_RAW_ASSET`, an
Airflow Asset watched by `infrastructure.s3_new_object_trigger.S3NewObjectTrigger`
(see that module for why a plain `S3KeyTrigger` isn't safe for event-driven
scheduling here): every time a new window-file lands, the trigger fires and
this DAG runs, reading *every* window-file present so far for that file's
`date=` partition (`orders/date=<date>/*.csv`) and applying
`metadata.orders_schema.OrdersSchema.RAW` explicitly (no `inferSchema` — a
column silently changing type on the raw side should fail this task loudly,
not get silently coerced). Re-globbing the whole day's partition on every
trigger, rather than reading only the one new file, keeps the write idempotent
(see below) without needing to track which window-files have already been
ingested.

This is platform-layer logic (Bronze schema normalization), not infra glue —
built here only because the engineer explicitly opted to change the usual
CLAUDE.md boundary for this one piece of work. It does not deduplicate
orders across extract windows (an order's state can legitimately appear in
multiple window extracts as it progresses through its lifecycle) — that
collapse into "one row per order" is documented as Silver's job in
`PROGRESS.md`'s platform-build table, not Bronze's. Bronze is intentionally
an append-only, per-extract-date audit trail: exactly what
`docs/data-sources.md` #1 already says the raw source is ("each row is one
order's state as of extract time, not a changelog entry") preserved as-is,
one partition per extract date, so G2's "what did yesterday's numbers look
like" is answerable directly off Bronze without needing Delta time travel
for routine lookups.

Idempotency: the write uses `replaceWhere` to atomically overwrite only the
`_extract_date` partition being ingested, rather than a plain `append`. A
plain append would double-count rows on any Airflow retry, or on any two
Asset-triggered runs for the same date racing each other — `replaceWhere`
makes re-running this task for the same date (retry, backfill, manual
re-trigger, or the next window's own trigger) safe by construction, which is
what G3 ("resilient... without someone manually intervening") actually
requires from an event-driven loader, not just a green task the first time.

The extract date(s) ingested are read off the triggering Asset event(s)'
keys (`date=<date>/` in the new object's path) rather than the run's
logical date, since an Asset-scheduled run has no `data_interval`/`ds` of
its own. With `max_active_runs=1`, a run still in progress makes Airflow
coalesce every Asset event that arrives meanwhile onto the next run, so a
single run can carry keys spanning more than one `date=` partition (e.g.
across midnight); every distinct date found is ingested, not just the
first. Manual triggers fall back to the `date` param, then today's UTC
date.
"""

from __future__ import annotations

import re

import pendulum
from airflow.sdk import Asset, AssetWatcher, Param, dag, get_current_context, task
from dag_defaults import BRONZE_DEFAULT_ARGS
from infrastructure.s3_new_object_trigger import S3NewObjectTrigger
from metadata.buckets import Buckets

ORDERS_RAW_PREFIX = "orders"
ORDERS_BRONZE_PREFIX = "orders"
EXTRACT_DATE_COLUMN = "_extract_date"
DATE_PARTITION_PATTERN = re.compile(r"date=(\d{4}-\d{2}-\d{2})")

# How many workers/cores this DAG's Spark submission requests from the
# shared Standalone cluster. Defined per-DAG (rather than left to
# plugins/spark_session.py's env-var defaults) so this job's cluster
# footprint is visible and tunable at the call site.
SPARK_WORKER_COUNT = 2
SPARK_CORES_MAX = 4  # None = derive from SPARK_WORKER_COUNT * per-worker cores (see plugins/spark_session.py)

ORDERS_RAW_ASSET = Asset(
    f"s3://{Buckets.RAW_INCOMING_DATA}/{ORDERS_RAW_PREFIX}/",
    watchers=[
        AssetWatcher(
            name="orders_raw_watcher",
            trigger=S3NewObjectTrigger(bucket=Buckets.RAW_INCOMING_DATA, prefix=f"{ORDERS_RAW_PREFIX}/"),
        )
    ],
)


def _extract_dates_from_triggering_event(context: dict) -> list[str]:
    """Pulls every distinct `date=<date>` segment out of the Asset events that triggered this run.

    `context["triggering_asset_events"]` maps each `Asset` to the list of
    `AssetEvent`s that caused this run; `S3NewObjectTrigger` sets each
    event's `extra` to `{"bucket": ..., "key": ...}` (see that module's
    `TriggerEvent` payload), so each new object's key is read back off of
    it here. Plural because `max_active_runs=1` means a run still in
    progress makes Airflow coalesce every Asset event that arrives
    meanwhile onto the next run: if those coalesced events span more than
    one `date=` partition (e.g. some land just before midnight, some just
    after), a single run can be responsible for ingesting more than one
    date, and dropping all but the first would silently leave that other
    partition un-ingested. Returns an empty list for manually triggered
    runs, which have no triggering asset event.
    """
    triggering_events = context.get("triggering_asset_events") or {}
    dates: set[str] = set()
    for events in triggering_events.values():
        for event in events:
            match = DATE_PARTITION_PATTERN.search((event.extra or {}).get("key", ""))
            if match:
                dates.add(match.group(1))
    return sorted(dates)


@dag(
    dag_id="ingest_orders_bronze",
    schedule=[ORDERS_RAW_ASSET],
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "orders"],
    default_args=BRONZE_DEFAULT_ARGS,
    params={
        "date": Param(
            default=None,
            type=["string", "null"],
            format="date",
            description=(
                "Extract date to ingest, YYYY-MM-DD. Only used for a manual "
                "trigger (no triggering Asset event to read the date off "
                "of); defaults to today's UTC date."
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
        from metadata.orders_schema import OrdersSchema
        from spark_session import StandaloneClusterConfig, StandaloneSparkSessionFactory

        context = get_current_context()
        params = context["params"]
        target_dates = _extract_dates_from_triggering_event(context) or [
            params["date"] or pendulum.now("UTC").to_date_string()
        ]

        bronze_path = f"s3a://{Buckets.BRONZE}/{ORDERS_BRONZE_PREFIX}/"

        spark = StandaloneSparkSessionFactory(
            app_name="veloz-ingest-orders-bronze",
            cluster_config=StandaloneClusterConfig.from_env(
                worker_count=SPARK_WORKER_COUNT, cores_max=SPARK_CORES_MAX
            ),
        ).get_session()

        try:
            for target_date in target_dates:
                raw_path = f"s3a://{Buckets.RAW_INCOMING_DATA}/{ORDERS_RAW_PREFIX}/date={target_date}/*.csv"
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
