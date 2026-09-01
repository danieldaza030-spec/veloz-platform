"""Ingests newly landed raw rider-event window-files into the Bronze layer.

`generate_rider_events` (see `dags/generate_rider_events.py`) runs every 5
minutes, landing one JSON Lines object per run at
`s3a://raw-incoming-data/rider_events/date=<date>/rider_events_<run_timestamp>.jsonl`.
This DAG is scheduled off `RIDER_EVENTS_RAW_ASSET`, an Airflow Asset watched
by `infrastructure.s3_new_object_trigger.S3NewObjectTrigger` (see that module
for why a plain `S3KeyTrigger` isn't safe for event-driven scheduling here):
every time a new window-file lands, the trigger fires and this DAG runs,
reading *every* window-file present so far for that file's `date=` partition
(`rider_events/date=<date>/*.jsonl`) and applying
`metadata.rider_events_schema.RiderEventsSchema.RAW` explicitly (no
`inferSchema` — a field silently changing type on the raw side should fail
this task loudly, not get silently coerced). Re-globbing the whole day's
partition on every trigger, rather than reading only the one new file, keeps
the write idempotent (see below) without needing to track which
window-files have already been ingested.

This DAG mirrors `dags/ingest_orders_bronze.py` structurally, batch-loading
the rider-events source the same way orders is batch-loaded, even though
`docs/data-sources.md`'s reference architecture calls for this source to
eventually be consumed as a Kafka + Spark Structured Streaming stream (for
G1's near-live Ops visibility). This DAG is explicitly a temporary,
throwaway stand-in reusing the existing Bronze/Asset plumbing rather than
standing up streaming infra for the two-week demo; it does not replace the
streaming design in the architecture table.

Bronze is intentionally an append-only, per-extract-date audit trail here
too: it does not deduplicate or collapse events, matching the same
Bronze-vs-Silver boundary documented for orders in `PROGRESS.md`'s
platform-build table.

Idempotency: the write uses `replaceWhere` to atomically overwrite only the
`_extract_date` partition being ingested, rather than a plain `append`. A
plain append would double-count rows on any Airflow retry, or on any two
Asset-triggered runs for the same date racing each other — `replaceWhere`
makes re-running this task for the same date (retry, backfill, manual
re-trigger, or the next window's own trigger) safe by construction.

The extract date ingested is read off the triggering Asset event's key
(`date=<date>/` in the new object's path) rather than the run's logical
date, since an Asset-scheduled run has no `data_interval`/`ds` of its own.
Manual triggers fall back to the `date` param, then today's UTC date.
"""

from __future__ import annotations

import re

import pendulum
from airflow.sdk import Asset, AssetWatcher, Param, dag, get_current_context, task
from dag_defaults import BRONZE_DEFAULT_ARGS
from infrastructure.s3_new_object_trigger import S3NewObjectTrigger
from metadata.buckets import Buckets

RIDER_EVENTS_RAW_PREFIX = "rider_events"
RIDER_EVENTS_BRONZE_PREFIX = "rider_events"
EXTRACT_DATE_COLUMN = "_extract_date"
DATE_PARTITION_PATTERN = re.compile(r"date=(\d{4}-\d{2}-\d{2})")

# How many workers/cores this DAG's Spark submission requests from the
# shared Standalone cluster. Defined per-DAG (rather than left to
# plugins/spark_session.py's env-var defaults) so this job's cluster
# footprint is visible and tunable at the call site.
SPARK_WORKER_COUNT = 5
SPARK_CORES_MAX = 2  # None = derive from SPARK_WORKER_COUNT * per-worker cores (see plugins/spark_session.py)

RIDER_EVENTS_RAW_ASSET = Asset(
    f"s3://{Buckets.RAW_INCOMING_DATA}/{RIDER_EVENTS_RAW_PREFIX}/",
    watchers=[
        AssetWatcher(
            name="rider_events_raw_watcher",
            trigger=S3NewObjectTrigger(bucket=Buckets.RAW_INCOMING_DATA, prefix=f"{RIDER_EVENTS_RAW_PREFIX}/"),
        )
    ],
)


def _extract_date_from_triggering_event(context: dict) -> str | None:
    """Pulls the `date=<date>` segment out of the Asset event that triggered this run.

    `context["triggering_asset_events"]` maps each `Asset` to the list of
    `AssetEvent`s that caused this run; `S3NewObjectTrigger` sets each
    event's `extra` to `{"bucket": ..., "key": ...}` (see that module's
    `TriggerEvent` payload), so the new object's key is read back off of it
    here. Returns None for manually triggered runs, which have no
    triggering asset event.
    """
    triggering_events = context.get("triggering_asset_events") or {}
    for events in triggering_events.values():
        for event in events:
            match = DATE_PARTITION_PATTERN.search((event.extra or {}).get("key", ""))
            if match:
                return match.group(1)
    return None


@dag(
    dag_id="ingest_rider_events_bronze",
    schedule=[RIDER_EVENTS_RAW_ASSET],
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["bronze", "rider_events"],
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
def ingest_rider_events_bronze():
    @task
    def run() -> None:
        # plugins/ is on sys.path for every DAG/task (see spark_session's own
        # bare-name import above); metadata/ is mounted as a subdirectory of
        # it in docker-compose.yml specifically so this import works without
        # any extra path wiring. application/ and infrastructure/ sit next
        # to metadata/ at the repo root and follow the same lazy-import
        # pattern, deferring pyspark-dependent imports out of DAG parse time.
        from application.bronze_ingestion import BronzeIngestionRequest, ingest_to_bronze
        from metadata.rider_events_schema import RiderEventsSchema
        from spark_session import StandaloneClusterConfig, StandaloneSparkSessionFactory

        context = get_current_context()
        params = context["params"]
        target_date = (
            _extract_date_from_triggering_event(context)
            or params["date"]
            or pendulum.now("UTC").to_date_string()
        )

        raw_path = f"s3a://{Buckets.RAW_INCOMING_DATA}/{RIDER_EVENTS_RAW_PREFIX}/date={target_date}/*.jsonl"
        bronze_path = f"s3a://{Buckets.BRONZE}/{RIDER_EVENTS_BRONZE_PREFIX}/"

        spark = StandaloneSparkSessionFactory(
            app_name="veloz-ingest-rider-events-bronze",
            cluster_config=StandaloneClusterConfig.from_env(
                worker_count=SPARK_WORKER_COUNT, cores_max=SPARK_CORES_MAX
            ),
        ).get_session()

        try:
            request = BronzeIngestionRequest(
                raw_path=raw_path,
                raw_format="json",
                schema=RiderEventsSchema.RAW,
                read_options={
                    # FAILFAST: fail loudly on any row that doesn't match
                    # RiderEventsSchema.RAW instead of coercing it.
                    "mode": "FAILFAST",
                },
                bronze_path=bronze_path,
                partition_column=EXTRACT_DATE_COLUMN,
                extract_date=target_date,
            )

            print(f"reading raw rider events extract: {raw_path}")
            row_count = ingest_to_bronze(spark, request)
            print(
                f"wrote {row_count} rows to {bronze_path} "
                f"(partition {EXTRACT_DATE_COLUMN}={target_date})"
            )
        finally:
            spark.stop()

    run()


ingest_rider_events_bronze()
