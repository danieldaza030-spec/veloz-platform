"""Ingests newly landed raw orders window-files into the Bronze layer.

`generate_orders` (the `generate_orders` task in
`dags/generate_orders_and_rider_events.py`) now runs every 5 minutes,
landing one object per run at
`s3a://raw-incoming-data/orders/date=<date>/orders_<run_timestamp>.csv`
instead of one file per day. This DAG is scheduled off `ORDERS_RAW_ASSET`, an
Airflow Asset watched by `infrastructure.s3_new_object_trigger.S3NewObjectTrigger`
(see that module for why a plain `S3KeyTrigger` isn't safe for event-driven
scheduling here): every time a new window-file lands, the trigger fires and
this DAG runs, applying `metadata.orders_schema.OrdersSchema.RAW` explicitly
(no `inferSchema` — a column silently changing type on the raw side should
fail this task loudly, not get silently coerced).

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
so no window information) falls back to a full-day glob, but still declares
`window_column=INGESTION_WINDOW_COLUMN` with `window_values=None` (see
`_resolve_window_spec`): a Delta table has one fixed partition spec, so
every run mode has to agree on the same `[_extract_date, _ingestion_window]`
layout regardless of which one creates the table first, or whichever run
mode runs second fails outright. `window_values=None` is what tells
`DeltaBronzeWriter.write` to scope the `replaceWhere` to the whole
`_extract_date` partition instead of a bounded window set — correct for a
manual run's full-day read.

Beyond consuming `ORDERS_RAW_ASSET`, this DAG also produces
`ORDERS_BRONZE_ASSET`, declared as `run()`'s only outlet: a plain Asset (no
watcher of its own) a downstream Silver DAG can schedule off via
`schedule=[ORDERS_BRONZE_ASSET]`, distinct from `ORDERS_RAW_ASSET`'s
watcher-driven consumption above -- one is a custom-triggered *consumer*
mechanism, the other a plain outlet-event *producer* mechanism, not two
names for the same thing. Each run publishes
`outlet_events[ORDERS_BRONZE_ASSET].extra = {"extract_dates": [...],
"windows": [...]}` naming exactly the `_extract_date` values and
`_ingestion_window` tokens this run actually wrote (not what it intended
to write): window tokens are the same `%Y%m%dT%H%M%SZ`-formatted string
`WINDOW_PATTERN`/`add_lineage_columns` already derive from each raw
window-file's name, e.g. `"20260905T191000Z"`, so a Silver consumer can
filter its own read directly on that token instead of rereading the whole
table.

This is platform-layer logic (Bronze schema normalization), not infra glue —
built here only because the engineer explicitly opted to change the usual
CLAUDE.md boundary for this one piece of work. It does not deduplicate
orders across extract windows (an order's state can legitimately appear in
multiple window extracts as it progresses through its lifecycle) — that
collapse into "one row per order" is documented as Silver's job in
`PROGRESS.md`'s platform-build table, not Bronze's. Bronze is intentionally
an append-only, per-extract-date (and, now, per-ingestion-window) audit
trail: exactly what `docs/data-sources.md` #1 already says the raw source is
("each row is one order's state as of extract time, not a changelog entry")
preserved as-is, so G2's "what did yesterday's numbers look like" is
answerable directly off Bronze without needing Delta time travel for
routine lookups.

Idempotency: the write uses `replaceWhere` to atomically overwrite only the
`_extract_date` partition (and, for an Asset-triggered run, only the
`_ingestion_window` values actually read) being ingested, rather than a
plain `append`. A plain append would double-count rows on any Airflow
retry, or on any two Asset-triggered runs for the same date/windows racing
each other — `replaceWhere` makes re-running this task safe by
construction, which is what G3 ("resilient... without someone manually
intervening") actually requires from an event-driven loader, not just a
green task the first time.

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
from metadata.ingestion_windows import WINDOW_MINUTES

ORDERS_RAW_PREFIX = "orders"
ORDERS_BRONZE_PREFIX = "orders"
EXTRACT_DATE_COLUMN = "_extract_date"
INGESTION_WINDOW_COLUMN = "_ingestion_window"
DATE_PARTITION_PATTERN = re.compile(r"date=(\d{4}-\d{2}-\d{2})")
WINDOW_PATTERN = re.compile(r"orders_(\d{8}T\d{6}Z)\.csv$")

# `WINDOW_MINUTES` (metadata.ingestion_windows, the one shared cadence
# constant `dags.ingest_orders_silver` also imports) is the file cadence;
# 6 windows * WINDOW_MINUTES = 30 minutes of lookback on every
# Asset-triggered run, enough to safely re-cover a window a triggerer
# restart might have skipped a trigger for, without approaching the cost
# of a full-day reglob.
LOOKBACK_WINDOW_COUNT = 6
if LOOKBACK_WINDOW_COUNT * WINDOW_MINUTES != 30:
    raise ValueError(
        "LOOKBACK_WINDOW_COUNT's 30-minute lookback assumption no longer holds "
        "for the current WINDOW_MINUTES cadence -- update the count (or this "
        "assumption) deliberately instead of letting the two silently drift."
    )

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

# Bronze->Silver producer/consumer asset: no watcher of its own (unlike
# ORDERS_RAW_ASSET above), just an outlet a downstream Silver DAG schedules
# off of. Kept as a separate Asset object rather than reusing ORDERS_RAW_ASSET
# -- the two represent different stages of the pipeline and different
# triggering mechanisms (custom S3NewObjectTrigger watcher vs. plain outlet
# event), not the same dependency.
ORDERS_BRONZE_ASSET = Asset(f"s3://{Buckets.BRONZE}/{ORDERS_BRONZE_PREFIX}/")


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


def _resolve_window_spec(
    triggering_windows: list[str], windows_read: list[str]
) -> tuple[str, list[str] | None]:
    """Determines one target date's uniform `(window_column, window_values)` request pair.

    Every run mode -- Asset-triggered or manual -- partitions Bronze by
    the same `[EXTRACT_DATE_COLUMN, INGESTION_WINDOW_COLUMN]` spec: a
    Delta table has one fixed partition layout, so whichever run mode
    happens to create the table first locks that layout in, and a later
    run in the *other* mode would fail outright if it disagreed. Only the
    `replaceWhere` overwrite *scope* differs by mode: an Asset-triggered
    run bounds it to `windows_read` (the reglob it actually read); a
    manual run leaves it `None`, which `DeltaBronzeWriter.write` treats as
    a full-`_extract_date`-partition overwrite, matching its full-day
    glob read.

    Args:
        triggering_windows: Window tokens that triggered this run for the
            target date; empty for a manual run.
        windows_read: Every window token this run actually read for the
            target date, both modes -- the bounded lookback set for an
            Asset-triggered run, or every window token present under the
            date's raw prefix for a manual run's full-day glob.

    Returns:
        `(INGESTION_WINDOW_COLUMN, windows_read)` for an Asset-triggered
        run (`triggering_windows` non-empty), or
        `(INGESTION_WINDOW_COLUMN, None)` for a manual run.
    """
    return INGESTION_WINDOW_COLUMN, (windows_read if triggering_windows else None)


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
    @task(outlets=[ORDERS_BRONZE_ASSET])
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
        windows_by_date = _windows_from_triggering_event(context)

        bronze_path = f"s3a://{Buckets.BRONZE}/{ORDERS_BRONZE_PREFIX}/"

        spark = StandaloneSparkSessionFactory(
            app_name="veloz-ingest-orders-bronze",
            cluster_config=StandaloneClusterConfig.from_env(
                worker_count=SPARK_WORKER_COUNT, cores_max=SPARK_CORES_MAX
            ),
        ).get_session()

        # Accumulated across every target_date this run actually wrote, for
        # the ORDERS_BRONZE_ASSET outlet event published below -- exactly
        # what landed in Bronze this run, not what a given date's request
        # merely intended to write.
        written_extract_dates: list[str] = []
        written_windows: set[str] = set()

        try:
            for target_date in target_dates:
                triggering_windows = windows_by_date.get(target_date, [])
                # Listed unconditionally (not just for an Asset-triggered
                # date): the triggered branch below needs this set to build
                # raw_path; the manual full-day-glob branch needs it too,
                # not merely to log, but to report the windows it ACTUALLY
                # covered in the ORDERS_BRONZE_ASSET outlet extra published
                # at the end of this run -- that reporting has to stay
                # accurate, so this LIST can't be skipped on that branch
                # the way dags.ingest_rider_events_bronze's identical call
                # can (that DAG has no outlet consuming windows_read; see
                # its own matching comment).
                all_keys = list_keys(
                    Buckets.RAW_INCOMING_DATA, f"{ORDERS_RAW_PREFIX}/date={target_date}/"
                )
                window_by_key = {
                    key: match.group(1)
                    for key in all_keys
                    if (match := WINDOW_PATTERN.search(key))
                }

                if triggering_windows:
                    windows_read = _resolve_windows_to_ingest(
                        sorted(window_by_key.values()),
                        sorted(triggering_windows),
                        LOOKBACK_WINDOW_COUNT,
                    )
                    windows_read_set = set(windows_read)
                    raw_path = [
                        f"s3a://{Buckets.RAW_INCOMING_DATA}/{key}"
                        for key, window in window_by_key.items()
                        if window in windows_read_set
                    ]
                else:
                    raw_path = f"s3a://{Buckets.RAW_INCOMING_DATA}/{ORDERS_RAW_PREFIX}/date={target_date}/*.csv"
                    windows_read = sorted(window_by_key.values())

                window_column, window_values = _resolve_window_spec(triggering_windows, windows_read)

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
                    window_column=window_column,
                    window_values=window_values,
                )

                print(f"reading raw orders extract: {raw_path}")
                row_count = ingest_to_bronze(spark, request)
                # Full-day branch logs a window count, not the enumerated
                # list -- windows_read can run to hundreds of tokens for a
                # full day and isn't worth spelling out in a log line; the
                # ORDERS_BRONZE_ASSET outlet extra below is where the exact
                # list actually gets reported.
                print(
                    f"wrote {row_count} rows to {bronze_path} "
                    f"(partition {EXTRACT_DATE_COLUMN}={target_date}, "
                    f"{INGESTION_WINDOW_COLUMN}"
                    + (
                        f" in {window_values})"
                        if window_values is not None
                        else f" full day, {len(windows_read)} windows)"
                    )
                )

                written_extract_dates.append(target_date)
                written_windows.update(windows_read)
        finally:
            spark.stop()

        context["outlet_events"][ORDERS_BRONZE_ASSET].extra = {
            "extract_dates": written_extract_dates,
            "windows": sorted(written_windows),
        }

    run()


ingest_orders_bronze()
