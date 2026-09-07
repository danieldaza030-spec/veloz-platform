"""Collapses newly landed Bronze orders windows into the Silver orders table.

Scheduled off `ORDERS_BRONZE_ASSET` (`dags.ingest_orders_bronze`'s outlet,
a plain Asset with no watcher of its own -- see that module's docstring
for why it's a distinct concept from `ORDERS_RAW_ASSET`'s
`S3NewObjectTrigger`-watched consumption). `ORDERS_BRONZE_ASSET` is
redeclared here with the exact same URI (`s3://bronze-veloz/orders/`)
rather than imported from `dags.ingest_orders_bronze`: Airflow 3 keys an
Asset by its URI, not Python object identity, and DAG modules in this
repo aren't meant to be imported as a library by other DAGs (each file is
self-contained -- see `plugins/spark_session.py`'s own reasoning for a
similar duplication). If `ORDERS_BRONZE_ASSET`'s URI ever changes in
`dags.ingest_orders_bronze`, this constant has to change with it or this
DAG silently stops triggering; flagged here as a dedup candidate the
reviewer may want a shared `metadata/` constant for.

With `max_active_runs=1`, a run still in progress makes Airflow coalesce
every `ORDERS_BRONZE_ASSET` outlet event that arrives meanwhile onto the
next run, so `context["triggering_asset_events"]` can carry more than one
event -- `_union_bronze_asset_event_extras` reads every one of them and
unions their `extra["extract_dates"]`/`extra["windows"]`, not just the
latest, for the same reason `dags.ingest_orders_bronze.
_extract_dates_from_triggering_event` does (see that function's
docstring): dropping all but the latest event would silently leave
whatever the dropped events described un-ingested.

Filter resolution precedence, resolved by `_resolve_ingestion_filter`:

1. An explicit manual-trigger `mode` param, if set, always wins over
   whatever triggered the run -- exactly two modes, no third:
   - `mode="ingestion_window"`: `window_start`/`window_end` name an
     inclusive range of `_ingestion_window` tokens (`WINDOW_TOKEN_FORMAT`,
     the same 5-minute-cadence tokens `dags.ingest_orders_bronze` writes),
     expanded by `_enumerate_windows` into the explicit list
     `OrdersSilverIngestionRequest.ingestion_windows` needs -- the
     5-minute cadence (`WINDOW_MINUTES`, imported from `metadata.
     ingestion_windows`, the one shared source of truth `dags.
     ingest_orders_bronze` also references) mirrors, but is not imported
     from, `generators/s3_io.py`'s own `WINDOW_MINUTES`: `generators/` is
     invoked by DAGs as a subprocess, not imported as a package -- see
     `dags.generate_orders_and_rider_events._run_generator` -- so
     `metadata.ingestion_windows`'s own docstring documents that mirrored
     assumption, and `tests/test_ingestion_windows_cadence.py`
     cross-checks it against `generators.s3_io.WINDOW_MINUTES` directly.
   - `mode="extract_date"`: `extract_dates` names an explicit list of
     `_extract_date` values to reprocess.
2. No explicit `mode`: falls back to the union of triggering
   `ORDERS_BRONZE_ASSET` event extras, preferring `windows` (the narrow,
   common case) over `extract_dates` when both are non-empty.
3. Neither an explicit `mode` nor any triggering event extra: raises
   `ValueError` loudly. There is no third "read everything"/"assume
   today" fallback -- a Silver MERGE silently processing the wrong slice
   is worse than a task that fails clearly.

Kept thin per this repo's layering: this module only resolves the
filter and calls `application.orders_silver_ingestion.
ingest_orders_to_silver`; the read/collapse/MERGE logic itself lives in
`application`/`infrastructure`, not here.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pendulum
from airflow.sdk import Asset, Param, dag, get_current_context, task
from dag_defaults import SILVER_DEFAULT_ARGS
from metadata.buckets import ORDERS_SILVER_PREFIX, Buckets
from metadata.ingestion_windows import WINDOW_MINUTES

ORDERS_BRONZE_PREFIX = "orders"

WINDOW_TOKEN_FORMAT = "%Y%m%dT%H%M%SZ"

VALID_MODES = ("ingestion_window", "extract_date")
"""The only two supported manual-rerun `mode` param values. No third mode,
and no default that silently reads "everything"."""

# Same Asset URI dags.ingest_orders_bronze.ORDERS_BRONZE_ASSET declares --
# see this module's docstring for why it's redeclared here instead of
# imported.
ORDERS_BRONZE_ASSET = Asset(f"s3://{Buckets.BRONZE}/{ORDERS_BRONZE_PREFIX}/")

# How many workers/cores this DAG's Spark submission requests from the
# shared Standalone cluster, mirroring dags.ingest_orders_bronze's own
# per-DAG sizing constants.
SPARK_WORKER_COUNT = 2
SPARK_CORES_MAX = 4


def _enumerate_windows(window_start: str, window_end: str) -> list[str]:
    """Expands an inclusive `[window_start, window_end]` bound into every window token between them.

    Assumes the fixed `WINDOW_MINUTES`-wide cadence every window-token
    producer in this repo (`generators/s3_io.py`, `dags.
    ingest_orders_bronze`) already shares -- see this module's docstring
    and `metadata.ingestion_windows` for why that shared cadence is
    imported from one place instead of each consumer keeping its own copy.

    Args:
        window_start: First `_ingestion_window` token to include,
            `WINDOW_TOKEN_FORMAT`-formatted.
        window_end: Last `_ingestion_window` token to include, inclusive.

    Returns:
        Every `WINDOW_MINUTES`-spaced window token from `window_start` to
        `window_end`, inclusive, sorted ascending.

    Raises:
        ValueError: If `window_end` is earlier than `window_start`.
    """
    start = datetime.strptime(window_start, WINDOW_TOKEN_FORMAT)
    end = datetime.strptime(window_end, WINDOW_TOKEN_FORMAT)
    if end < start:
        raise ValueError(f"window_end ({window_end}) precedes window_start ({window_start})")

    step = timedelta(minutes=WINDOW_MINUTES)
    windows: list[str] = []
    current = start
    while current <= end:
        windows.append(current.strftime(WINDOW_TOKEN_FORMAT))
        current += step
    return windows


def _union_bronze_asset_event_extras(context: dict) -> tuple[list[str], list[str]]:
    """Unions `extract_dates`/`windows` extras across every triggering `ORDERS_BRONZE_ASSET` event.

    Mirrors the iterate-and-union *pattern* `dags.ingest_orders_bronze.
    _extract_dates_from_triggering_event` already established for
    `max_active_runs=1` coalescing (several Asset events arriving while a
    run is in progress are all delivered to the next run, not just the
    latest one) -- copied here rather than imported, since DAG modules in
    this repo aren't meant to be imported as a library by other DAGs. A
    dedup candidate for the reviewer: a shared "iterate
    `context['triggering_asset_events']`, union some per-event
    extraction" helper could live in `plugins/`, factoring out this loop
    from both this function and bronze's own -- deliberately not done
    here, out of this task's scope.

    The actual per-event extraction differs from bronze's, though:
    `ORDERS_BRONZE_ASSET` has no watcher of its own (see this module's
    docstring), so its outlet events are plain -- each event's `.extra`
    is exactly what `dags.ingest_orders_bronze.run` set directly via
    `context["outlet_events"][ORDERS_BRONZE_ASSET].extra = {"extract_dates":
    [...], "windows": [...]}`, not the `{"from_trigger": ..., "payload":
    {...}}` wrapping `Trigger.submit_event` applies to a *watcher*-triggered
    event like `ORDERS_RAW_ASSET`'s.

    Args:
        context: Airflow task context, as passed to `run()`.

    Returns:
        `(extract_dates, windows)`, each the sorted union across every
        triggering event's extra -- both empty for a manually triggered
        run, which has no triggering Asset event.
    """
    triggering_events = context.get("triggering_asset_events") or {}
    extract_dates: set[str] = set()
    windows: set[str] = set()
    for events in triggering_events.values():
        for event in events:
            extra = event.extra or {}
            extract_dates.update(extra.get("extract_dates") or [])
            windows.update(extra.get("windows") or [])
    return sorted(extract_dates), sorted(windows)


def _resolve_ingestion_filter(params: dict, context: dict) -> tuple[list[str] | None, list[str] | None]:
    """Resolves this run's bounded Bronze filter: explicit params, else asset-event extras, else raise.

    See this module's docstring for the full precedence rule this
    implements.

    Args:
        params: The run's resolved Airflow params (`context["params"]`).
        context: Airflow task context, as passed to `run()`.

    Returns:
        `(ingestion_windows, extract_dates)`, matching
        `application.orders_silver_ingestion.OrdersSilverIngestionRequest`'s
        own fields -- exactly one of the pair is non-`None`.

    Raises:
        ValueError: If `params["mode"]` is set to something other than
            `VALID_MODES`; if a set mode's required param(s) are missing
            or empty; or if no explicit mode is set and no triggering
            `ORDERS_BRONZE_ASSET` event carried a non-empty `extra` --
            there is no silent "today"/"everything" default.
    """
    mode = params.get("mode")
    if mode is not None:
        if mode not in VALID_MODES:
            raise ValueError(f"params['mode'] must be one of {VALID_MODES}, got {mode!r}")

        if mode == "ingestion_window":
            window_start = params.get("window_start")
            window_end = params.get("window_end")
            if not window_start or not window_end:
                raise ValueError(
                    "mode='ingestion_window' requires both window_start and window_end params"
                )
            return _enumerate_windows(window_start, window_end), None

        extract_dates_param = params.get("extract_dates")
        if not extract_dates_param:
            raise ValueError("mode='extract_date' requires a non-empty extract_dates param")
        return None, list(extract_dates_param)

    extract_dates_extra, windows_extra = _union_bronze_asset_event_extras(context)
    if windows_extra:
        return windows_extra, None
    if extract_dates_extra:
        return None, extract_dates_extra

    raise ValueError(
        "no explicit params['mode'] and no triggering ORDERS_BRONZE_ASSET event "
        "extras present -- refusing to silently default to 'today' or 'all'"
    )


@dag(
    dag_id="ingest_orders_silver",
    schedule=[ORDERS_BRONZE_ASSET],
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["silver", "orders"],
    default_args=SILVER_DEFAULT_ARGS,
    params={
        "mode": Param(
            default=None,
            type=["string", "null"],
            enum=[None, *VALID_MODES],
            description=(
                "Manual-rerun filter mode. 'ingestion_window' uses "
                "window_start/window_end; 'extract_date' uses "
                "extract_dates. Leave unset for an Asset-triggered run "
                "(the filter is read off the triggering ORDERS_BRONZE_ASSET "
                "event(s) instead)."
            ),
        ),
        "window_start": Param(
            default=None,
            type=["string", "null"],
            description=(
                f"First _ingestion_window token to reprocess, "
                f"{WINDOW_TOKEN_FORMAT}-formatted. Only used with "
                f"mode='ingestion_window'."
            ),
        ),
        "window_end": Param(
            default=None,
            type=["string", "null"],
            description=(
                "Last _ingestion_window token to reprocess, inclusive. "
                "Only used with mode='ingestion_window'."
            ),
        ),
        "extract_dates": Param(
            default=None,
            type=["array", "null"],
            description=(
                "_extract_date values (YYYY-MM-DD) to reprocess. Only "
                "used with mode='extract_date'."
            ),
        ),
    },
)
def ingest_orders_silver():
    @task
    def run() -> None:
        # Deferred out of DAG-parse time, same as dags.ingest_orders_bronze --
        # see that module's docstring for why.
        from application.orders_silver_ingestion import (
            OrdersSilverIngestionRequest,
            ingest_orders_to_silver,
        )
        from spark_session import StandaloneClusterConfig, StandaloneSparkSessionFactory

        context = get_current_context()
        ingestion_windows, extract_dates = _resolve_ingestion_filter(context["params"], context)

        bronze_path = f"s3a://{Buckets.BRONZE}/{ORDERS_BRONZE_PREFIX}/"
        silver_path = f"s3a://{Buckets.SILVER}/{ORDERS_SILVER_PREFIX}/"

        spark = StandaloneSparkSessionFactory(
            app_name="veloz-ingest-orders-silver",
            cluster_config=StandaloneClusterConfig.from_env(
                worker_count=SPARK_WORKER_COUNT, cores_max=SPARK_CORES_MAX
            ),
        ).get_session()

        try:
            request = OrdersSilverIngestionRequest(
                bronze_path=bronze_path,
                silver_path=silver_path,
                ingestion_windows=ingestion_windows,
                extract_dates=extract_dates,
            )
            row_count = ingest_orders_to_silver(spark, request)
            print(
                f"merged {row_count} rows into {silver_path} "
                f"(ingestion_windows={ingestion_windows}, extract_dates={extract_dates})"
            )
        finally:
            spark.stop()

    run()


ingest_orders_silver()
