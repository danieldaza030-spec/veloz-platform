"""Ingests newly landed raw rider-event window-files into the Bronze layer.

`generate_rider_events` (the `generate_rider_events` task in
`dags/generate_orders_and_rider_events.py`) runs every 5
minutes, landing one JSON Lines object per run at
`s3a://raw-incoming-data/rider_events/date=<date>/rider_events_<run_timestamp>.jsonl`.
This DAG is scheduled off `RIDER_EVENTS_RAW_ASSET`, an Airflow Asset watched
by `infrastructure.s3_new_object_trigger.S3NewObjectTrigger` (see that module
for why a plain `S3KeyTrigger` isn't safe for event-driven scheduling here):
every time a new window-file lands, the trigger fires and this DAG runs,
applying `metadata.rider_events_schema.RiderEventsSchema.RAW` explicitly (no
`inferSchema` — a field silently changing type on the raw side should fail
this task loudly, not get silently coerced).

When the run was Asset-triggered, the raw read is a *bounded* reglob: only
the triggering window-file(s) plus the previous `LOOKBACK_WINDOW_COUNT - 1`
windows already present under that `date=` partition are read (see
`_resolve_windows_to_ingest`), not the whole day's files. Bronze is
sub-partitioned by `_extract_date` and `_ingestion_window` (the run
timestamp embedded in each window-file's name), so the idempotent
`replaceWhere` overwrite below only needs to cover that bounded window set
— re-reading the whole day on every 5-minute trigger would make this job's
cost grow with the day's file count instead of staying constant. The bounded
lookback (30 minutes of windows) is enough to safely re-cover any window the
triggerer's own coalescing might have skipped a trigger for, without paying
day-long reglob cost. A manually triggered run (no triggering Asset event,
so no window information) falls back to the previous full-day-glob
behavior, `window_column=None`.

This DAG mirrors `dags/ingest_orders_bronze.py` structurally, batch-loading
the rider-events source the same way orders is batch-loaded, even though
`docs/data-sources.md`'s reference architecture calls for this source to
eventually be consumed as a Kafka + Spark Structured Streaming stream (for
G1's near-live Ops visibility). This DAG is explicitly a temporary,
throwaway stand-in reusing the existing Bronze/Asset plumbing rather than
standing up streaming infra for the two-week demo; it does not replace the
streaming design in the architecture table.

Bronze is intentionally an append-only, per-extract-date (and, now,
per-ingestion-window) audit trail here too: it does not deduplicate or
collapse events, matching the same Bronze-vs-Silver boundary documented for
orders in `PROGRESS.md`'s platform-build table.

Idempotency: the write uses `replaceWhere` to atomically overwrite only the
`_extract_date` partition (and, for an Asset-triggered run, only the
`_ingestion_window` values actually read) being ingested, rather than a
plain `append`. A plain append would double-count rows on any Airflow
retry, or on any two Asset-triggered runs for the same date/windows racing
each other — `replaceWhere` makes re-running this task safe by
construction.

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
from infrastructure.s3_object_lister import list_keys
from metadata.buckets import Buckets

RIDER_EVENTS_RAW_PREFIX = "rider_events"
RIDER_EVENTS_BRONZE_PREFIX = "rider_events"
EXTRACT_DATE_COLUMN = "_extract_date"
INGESTION_WINDOW_COLUMN = "_ingestion_window"
DATE_PARTITION_PATTERN = re.compile(r"date=(\d{4}-\d{2}-\d{2})")
WINDOW_PATTERN = re.compile(r"rider_events_(\d{8}T\d{6}Z)\.jsonl$")

# File cadence is 5 minutes (see generators/s3_io.py's WINDOW_MINUTES); 6
# windows = 30 minutes of lookback on every Asset-triggered run, enough to
# safely re-cover a window a triggerer restart might have skipped a trigger
# for, without approaching the cost of a full-day reglob.
LOOKBACK_WINDOW_COUNT = 6

# How many workers/cores this DAG's Spark submission requests from the
# shared Standalone cluster. Defined per-DAG (rather than left to
# plugins/spark_session.py's env-var defaults) so this job's cluster
# footprint is visible and tunable at the call site.
SPARK_WORKER_COUNT = 2
SPARK_CORES_MAX = 4  # None = derive from SPARK_WORKER_COUNT * per-worker cores (see plugins/spark_session.py)

RIDER_EVENTS_RAW_ASSET = Asset(
    f"s3://{Buckets.RAW_INCOMING_DATA}/{RIDER_EVENTS_RAW_PREFIX}/",
    watchers=[
        AssetWatcher(
            name="rider_events_raw_watcher",
            trigger=S3NewObjectTrigger(bucket=Buckets.RAW_INCOMING_DATA, prefix=f"{RIDER_EVENTS_RAW_PREFIX}/"),
        )
    ],
)


def _extract_dates_from_triggering_event(context: dict) -> list[str]:
    """Pulls every distinct `date=<date>` segment out of the Asset events that triggered this run.

    `context["triggering_asset_events"]` maps each `Asset` to the list of
    `AssetEvent`s that caused this run. `S3NewObjectTrigger` yields a
    `TriggerEvent({"bucket": ..., "key": ...})`, but Airflow's own
    `Trigger.submit_event` (see `airflow/models/trigger.py`) wraps that
    payload before storing it on the `AssetEvent`, so each event's `extra`
    actually looks like `{"from_trigger": True, "payload": {"bucket": ...,
    "key": ...}}` — the new object's key is read back off the nested
    `payload` dict here, not off `extra` directly. Plural because
    `max_active_runs=1` means a run still in
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
            key = ((event.extra or {}).get("payload") or {}).get("key", "")
            match = DATE_PARTITION_PATTERN.search(key)
            if match:
                dates.add(match.group(1))
    return sorted(dates)


def _windows_from_triggering_event(context: dict) -> dict[str, list[str]]:
    """Groups the triggering Asset events' window tokens by their `date=<date>` segment.

    Same loop over `context["triggering_asset_events"]` as
    `_extract_dates_from_triggering_event`, but additionally extracts each
    event key's window token (`WINDOW_PATTERN`) and groups it under the
    `date=<date>` segment (`DATE_PARTITION_PATTERN`) found in that same
    key, so `run()` can resolve, per date, exactly which windows
    triggered this run and bound its reglob to those.

    Args:
        context: Airflow task context, as passed to `run()`.

    Returns:
        A `{date: [window, ...]}` mapping, values sorted. Empty for a
        manually triggered run, which has no triggering Asset event.
    """
    triggering_events = context.get("triggering_asset_events") or {}
    windows_by_date: dict[str, set[str]] = {}
    for events in triggering_events.values():
        for event in events:
            key = ((event.extra or {}).get("payload") or {}).get("key", "")
            date_match = DATE_PARTITION_PATTERN.search(key)
            window_match = WINDOW_PATTERN.search(key)
            if date_match and window_match:
                windows_by_date.setdefault(date_match.group(1), set()).add(window_match.group(1))
    return {date: sorted(windows) for date, windows in windows_by_date.items()}


def _resolve_windows_to_ingest(
    all_windows_for_date: list[str], triggering_windows: list[str], lookback_count: int
) -> list[str]:
    """Expands each triggering window into a bounded lookback of already-landed windows.

    For every window that triggered this run, finds its position in
    `all_windows_for_date` (every window-file present so far for that
    date) and takes it plus the `lookback_count - 1` windows immediately
    before it, clamped at the start of the day. The union of these ranges
    across all triggering windows is returned, sorted — this is what
    bounds the reglob to a fixed lookback instead of the whole day's
    files, while still re-covering any window a coalesced
    (`max_active_runs=1`) trigger run might otherwise have skipped.

    Args:
        all_windows_for_date: Every window token present so far under
            this date's raw partition, sorted ascending.
        triggering_windows: Window tokens that triggered this run, for
            this date.
        lookback_count: Number of windows (including the triggering one)
            to include per triggering window.

    Returns:
        The sorted union of window tokens to ingest.

    Raises:
        ValueError: If a triggering window is not present in
            `all_windows_for_date` (via `list.index`) — the triggering
            file was just listed as part of that same set, so this
            should never happen; left as a loud failure rather than
            silently skipped.
    """
    selected: set[str] = set()
    for window in triggering_windows:
        idx = all_windows_for_date.index(window)
        selected.update(all_windows_for_date[max(0, idx - (lookback_count - 1)) : idx + 1])
    return sorted(selected)


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
        target_dates = _extract_dates_from_triggering_event(context) or [
            params["date"] or pendulum.now("UTC").to_date_string()
        ]
        windows_by_date = _windows_from_triggering_event(context)

        bronze_path = f"s3a://{Buckets.BRONZE}/{RIDER_EVENTS_BRONZE_PREFIX}/"

        spark = StandaloneSparkSessionFactory(
            app_name="veloz-ingest-rider-events-bronze",
            cluster_config=StandaloneClusterConfig.from_env(
                worker_count=SPARK_WORKER_COUNT, cores_max=SPARK_CORES_MAX
            ),
        ).get_session()

        try:
            for target_date in target_dates:
                triggering_windows = windows_by_date.get(target_date, [])
                if triggering_windows:
                    all_keys = list_keys(
                        Buckets.RAW_INCOMING_DATA, f"{RIDER_EVENTS_RAW_PREFIX}/date={target_date}/"
                    )
                    window_by_key = {
                        key: match.group(1)
                        for key in all_keys
                        if (match := WINDOW_PATTERN.search(key))
                    }
                    selected = _resolve_windows_to_ingest(
                        sorted(window_by_key.values()),
                        sorted(triggering_windows),
                        LOOKBACK_WINDOW_COUNT,
                    )
                    selected_set = set(selected)
                    raw_path = [
                        f"s3a://{Buckets.RAW_INCOMING_DATA}/{key}"
                        for key, window in window_by_key.items()
                        if window in selected_set
                    ]
                    window_column = INGESTION_WINDOW_COLUMN
                    window_values = selected
                else:
                    raw_path = (
                        f"s3a://{Buckets.RAW_INCOMING_DATA}/{RIDER_EVENTS_RAW_PREFIX}/date={target_date}/*.jsonl"
                    )
                    window_column = None
                    window_values = None

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
                    window_column=window_column,
                    window_values=window_values,
                )

                print(f"reading raw rider events extract: {raw_path}")
                row_count = ingest_to_bronze(spark, request)
                print(
                    f"wrote {row_count} rows to {bronze_path} "
                    f"(partition {EXTRACT_DATE_COLUMN}={target_date}"
                    + (f", {INGESTION_WINDOW_COLUMN} in {window_values}" if window_values else "")
                    + ")"
                )
        finally:
            spark.stop()

    run()


ingest_rider_events_bronze()
