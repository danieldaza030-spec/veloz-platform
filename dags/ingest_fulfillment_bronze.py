"""Ingests a day's store fulfillment feed into the Bronze layer.

Reads whatever per-store CSVs actually landed at
`s3a://raw-incoming-data/fulfillment/date=<date>/*.csv` (written by
`generate_fulfillment`, see `dags/generate_fulfillment.py`) and applies
`metadata.fulfillment_schema.FulfillmentSchema.RAW`. Unlike orders, this
feed is documented as unreliable (`docs/data-sources.md` #3): a store's
file can be missing entirely for the day, and rows within a present file
can be mechanically corrupt. Both failure modes must be visible, not
swallowed:

- Missing files: `infrastructure.s3_object_lister.list_keys` lists what's
  actually present under the day's prefix; that's diffed against
  `FulfillmentSchema.STORE_IDS` (every store expected to report) via
  `application.fulfillment_bronze_ingestion.find_missing_store_keys`.
  Every absent store's expected key becomes a `missing_file` quarantine
  record.
- Malformed rows: the read uses `mode="PERMISSIVE"` with
  `columnNameOfCorruptRecord` (schema extended locally via
  `application.bronze_ingestion.with_corrupt_record_column`, which never
  mutates the shared `FulfillmentSchema.RAW` constant other readers rely
  on). `application.bronze_ingestion.ingest_clean_rows_to_bronze` writes
  only the well-formed rows to Bronze and hands back the rejects, which
  become `malformed_row` quarantine records via
  `application.fulfillment_bronze_ingestion.build_malformed_row_quarantine_df`.

Clean rows land in the Delta table at `s3a://bronze-veloz/fulfillment/`,
partitioned on the raw `date` column the schema already carries (one
export date per row) rather than a synthetic `_extract_date` — fulfillment
doesn't need a derived lineage column duplicating data it already has, so
`derive_partition_column=False` leaves that column as read. `_bronze_ingested_at`
and `_source_file` lineage columns are added the same way orders does.
Quarantine records accumulate at `s3a://bronze-veloz/_quarantine/fulfillment/`
(see `docs/bronze-conventions.md`) and are never overwritten by later runs.

If every store's file is missing for the date, there's nothing to read:
the task quarantines the full set of missing keys and skips the Bronze
read/write for that date rather than letting Spark fail on a glob with no
matches.

Schedule: 01:20 UTC, 15 minutes after `generate_fulfillment`'s 01:05 UTC
run — same fixed-offset landing-zone-polling pattern `ingest_orders_bronze`
uses, widened slightly from orders' 10-minute buffer since fulfillment
writes up to 30 objects per run instead of one.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pendulum
from airflow.sdk import Param, dag, get_current_context, task
from dag_defaults import BRONZE_DEFAULT_ARGS

FULFILLMENT_RAW_PREFIX = "fulfillment"
FULFILLMENT_BRONZE_PREFIX = "fulfillment"
SOURCE_NAME = "fulfillment"
PARTITION_COLUMN = "date"
CORRUPT_RECORD_COLUMN = "_corrupt_record"


@dag(
    dag_id="ingest_fulfillment_bronze",
    schedule="20 1 * * *",  # 01:20 UTC daily -- 15 min after generate_fulfillment's 01:05 UTC run
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["bronze", "fulfillment"],
    default_args=BRONZE_DEFAULT_ARGS,
    params={
        "date": Param(
            default=None,
            type=["string", "null"],
            format="date",
            description=(
                "Export date to ingest, YYYY-MM-DD. Defaults to the run's "
                "logical date (ds), same fallback generate_fulfillment uses."
            ),
        ),
    },
)
def ingest_fulfillment_bronze():
    @task
    def run() -> None:
        # See ingest_orders_bronze.py's `run()` for why these are bare-name/
        # package imports deferred inside the task rather than at module
        # scope: plugins/, metadata/, application/, and infrastructure/ are
        # all mounted for DAG tasks to import without extra PYTHONPATH
        # wiring, and pyspark-dependent imports stay out of DAG parse time.
        from application.bronze_ingestion import (
            SplitBronzeIngestionRequest,
            ingest_clean_rows_to_bronze,
            with_corrupt_record_column,
        )
        from application.fulfillment_bronze_ingestion import (
            build_malformed_row_quarantine_df,
            build_missing_file_records,
            find_missing_store_keys,
        )
        from infrastructure.quarantine_writer import build_quarantine_path, write_quarantine_records
        from infrastructure.s3_object_lister import list_keys
        from metadata.buckets import Buckets
        from metadata.fulfillment_schema import FulfillmentSchema
        from spark_session import StandaloneClusterConfig, StandaloneSparkSessionFactory

        context = get_current_context()
        params = context["params"]
        target_date = params["date"] or context.get("ds") or pendulum.now("UTC").to_date_string()

        raw_prefix = f"{FULFILLMENT_RAW_PREFIX}/date={target_date}/"
        present_keys = list_keys(Buckets.RAW_INCOMING_DATA, raw_prefix)
        missing_keys = find_missing_store_keys(
            FulfillmentSchema.STORE_IDS, present_keys, FULFILLMENT_RAW_PREFIX, target_date
        )

        spark = StandaloneSparkSessionFactory(
            app_name="veloz-ingest-fulfillment-bronze",
            cluster_config=StandaloneClusterConfig.from_env(),
        ).get_session()

        try:
            quarantine_path = build_quarantine_path(SOURCE_NAME)

            if missing_keys:
                # Plain datetime, not a pendulum.DateTime: PySpark's
                # createDataFrame(list[dict], schema) verifies TimestampType
                # values by exact type, not isinstance, and rejects
                # datetime.datetime subclasses outright.
                detected_at = datetime.now(UTC)
                missing_records = build_missing_file_records(
                    missing_keys, SOURCE_NAME, target_date, detected_at
                )
                write_quarantine_records(spark, missing_records, quarantine_path)
                print(f"quarantined {len(missing_keys)} missing store file(s): {missing_keys}")

            if not present_keys:
                print(f"no fulfillment files present for {target_date}; skipping Bronze read")
                return

            raw_path = f"s3a://{Buckets.RAW_INCOMING_DATA}/{FULFILLMENT_RAW_PREFIX}/date={target_date}/*.csv"
            bronze_path = f"s3a://{Buckets.BRONZE}/{FULFILLMENT_BRONZE_PREFIX}/"

            request = SplitBronzeIngestionRequest(
                raw_path=raw_path,
                raw_format="csv",
                schema=with_corrupt_record_column(FulfillmentSchema.RAW, CORRUPT_RECORD_COLUMN),
                read_options={
                    "header": "true",
                    # PERMISSIVE + columnNameOfCorruptRecord: capture
                    # unparseable rows instead of failing the whole load,
                    # per this feed's documented unreliability.
                    "mode": "PERMISSIVE",
                    "columnNameOfCorruptRecord": CORRUPT_RECORD_COLUMN,
                },
                bronze_path=bronze_path,
                partition_column=PARTITION_COLUMN,
                extract_date=target_date,
                corrupt_record_column=CORRUPT_RECORD_COLUMN,
            )

            print(f"reading present fulfillment files: {raw_path}")
            row_count, corrupt_df = ingest_clean_rows_to_bronze(spark, request)
            print(
                f"wrote {row_count} rows to {bronze_path} "
                f"(partition {PARTITION_COLUMN}={target_date})"
            )

            corrupt_count = corrupt_df.count()
            if corrupt_count:
                malformed_df = build_malformed_row_quarantine_df(
                    corrupt_df, CORRUPT_RECORD_COLUMN, SOURCE_NAME, target_date
                )
                write_quarantine_records(spark, malformed_df, quarantine_path)
                print(f"quarantined {corrupt_count} malformed row(s)")
        finally:
            spark.stop()

    run()


ingest_fulfillment_bronze()
