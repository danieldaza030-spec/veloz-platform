"""Triggers `generators/orders.py` then `generators/rider_events.py` for the
current 5-minute window.

Tradeoff: the original design (`generate_orders.py` + `generate_rider_events.py`,
now deleted) intentionally ran these as two decoupled DAGs to simulate two
independent upstream systems - the orders extract from Postgres vs. the
rider-app event stream off a queue - each on its own cron, with the rider
events generator's own `FileNotFoundError` acting as the (informal, racy)
backstop for the case where that window's `orders_*.csv` hadn't landed yet.
That's realistic: real upstreams don't coordinate with each other.

This merge trades that realism for demo determinism and local-dev
convenience: `generate_orders` and `generate_rider_events` are now two tasks
in one DAG, wired `orders_task >> rider_events_task`, sharing one
`window_start`/`run_timestamp` computed once at the top of the DAG body. That
removes the race entirely (rider events can never run before its paired
orders window exists, and never drifts onto a different window under a
manual params override) at the cost of no longer simulating two independent
upstreams for *this* pair. The real ingestion path stays decoupled and
matches production: `ingest_orders_bronze` and `ingest_rider_events_bronze`
are triggered independently off S3 object writes via
`S3NewObjectTrigger`-watched Assets, not off this DAG's id or task
boundaries, so this merge is invisible to them as long as the output S3 key
layout is unchanged (it is).

Params expose both generators' tunable knobs (the union of what
`generate_orders.py` and `generate_rider_events.py` exposed), including
STATUS_WEIGHTS and ping count/location jitter, which are module-level
constants/CLI flags in `orders.py`/`rider_events.py`, not reimplemented here -
this DAG is a thin pass-through onto those flags; see `generators/orders.py`
and `generators/rider_events.py`. Manual "Trigger DAG w/ config" overrides
any of these per run; the schedule uses the generators' own documented
defaults.

Usage: unpause, then either wait for the next 5-minute run or
`Trigger DAG w/ config` in the UI to override date/num_orders/seed/
status_weights/min_pings/max_pings/location_jitter_degrees for a one-off run.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pendulum
from airflow.sdk import Param, dag, get_current_context, task

GENERATORS_DIR = Path("/opt/airflow/generators")
# S3 key prefixes under the raw-incoming-data bucket, not local paths - the
# generator reads/writes MinIO (see generators/s3_io.py), not the bind-mounted
# ./data/raw the container also has, which is now historical-only.
RAW_BUCKET = "raw-incoming-data"
ORDERS_PREFIX = "orders"
RIDER_EVENTS_PREFIX = "rider_events"


def _run_generator(script: str, args: list[str]) -> None:
    """Invokes a generators/*.py CLI script as its own OS process.

    Not an in-process import: the generator's own argparse stays the single
    source of truth for its CLI surface and defaults, and a generator crash
    can't take the Airflow task's interpreter down with it. Output streams to
    the task log; a non-zero exit fails the task with the generator's stderr.
    """
    cmd = [sys.executable, str(GENERATORS_DIR / script), *args]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.stdout:
        print(result.stdout)
    if result.returncode != 0:
        if result.stderr:
            print(result.stderr, file=sys.stderr)
        raise RuntimeError(f"{script} failed (exit {result.returncode}); see stderr above")


def _resolve_window_start(params: dict, interval_start: pendulum.DateTime | None) -> pendulum.DateTime:
    """Resolves the single window this run's tasks share.

    Uses the run's `data_interval_start`, or now, as the base, then applies
    the `date` param override (if any) onto that same time-of-day - computed
    once so `generate_orders` and `generate_rider_events` never independently
    derive diverging windows, including under a manual params override.

    Args:
        params: The run's resolved Airflow params.
        interval_start: The run's `data_interval_start`, or None outside a
            scheduled/backfill context.

    Returns:
        The UTC window start both tasks operate on.
    """
    if interval_start is not None:
        window_start = interval_start.in_timezone("UTC")
    else:
        window_start = pendulum.now("UTC").start_of("minute")
    if params["date"]:
        target_date = pendulum.parse(params["date"]).date()
        window_start = window_start.set(
            year=target_date.year,
            month=target_date.month,
            day=target_date.day,
        )
    return window_start


@dag(
    dag_id="generate_orders_and_rider_events",
    schedule="*/5 * * * *",  # Every 5 minutes
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["infra", "generator"],
    params={
        "date": Param(
            default=None,
            type=["string", "null"],
            format="date",
            description="Extract/event date, YYYY-MM-DD. Defaults to the run's logical date (ds).",
        ),
        "num_orders": Param(
            default=50_000_000,
            type="integer",
            minimum=1,
            description=(
                "Daily order target distributed across 5-minute windows "
                "(generator default: 50000000)."
            ),
        ),
        "seed": Param(default=42, type="integer", description="Random seed (generator default: 42)."),
        "status_weights": Param(
            default=None,
            type=["object", "null"],
            description=(
                "Override the delivered/cancelled/picked_up/assigned/created status "
                'mix, e.g. {"delivered": 0.5, "cancelled": 0.5, "picked_up": 0, '
                '"assigned": 0, "created": 0}. Must name exactly those five statuses '
                "(values need not sum to 1). Null uses the generator's documented "
                "default mix."
            ),
        ),
        "min_pings": Param(
            default=2,
            type="integer",
            minimum=0,
            description="Minimum location_ping events per active order (generator default: 2).",
        ),
        "max_pings": Param(
            default=5,
            type="integer",
            minimum=0,
            description="Maximum location_ping events per active order (generator default: 5).",
        ),
        "location_jitter_degrees": Param(
            default=0.05,
            type="number",
            minimum=0,
            description="+/- jitter radius, in degrees, around the order's store-city "
            "center (generator default: 0.05).",
        ),
    },
)
def generate_orders_and_rider_events():
    @task
    def resolve_window() -> dict[str, str]:
        """Computes the shared window start and formats for both generators.

        Returns:
            A dict with keys: orders_run_timestamp, rider_events_run_timestamp,
            target_date. Both generators derive from the same window_start,
            ensuring no independent drift.
        """
        context = get_current_context()
        window_start = _resolve_window_start(
            context["params"], context.get("data_interval_start")
        )
        return {
            "orders_run_timestamp": window_start.strftime("%Y-%m-%dT%H:%M:%S"),
            "rider_events_run_timestamp": window_start.strftime("%Y%m%dT%H%M%SZ"),
            "target_date": window_start.to_date_string(),
        }

    @task
    def generate_orders(run_timestamp: str) -> None:
        context = get_current_context()
        params = context["params"]

        args = [
            "--run-timestamp", run_timestamp,
            "--daily-target-orders", str(params["num_orders"]),
            "--seed", str(params["seed"]),
            "--bucket", RAW_BUCKET,
            "--output-dir", ORDERS_PREFIX,
        ]
        status_weights = params["status_weights"]
        if status_weights:
            pairs = ",".join(f"{k}={v}" for k, v in status_weights.items())
            args += ["--status-weights", pairs]

        _run_generator("orders.py", args)

    @task
    def generate_rider_events(target_date: str, run_timestamp: str) -> None:
        context = get_current_context()
        params = context["params"]
        orders_key = f"{ORDERS_PREFIX}/date={target_date}/orders_{run_timestamp}.csv"

        args = [
            "--date", target_date,
            "--run-timestamp", run_timestamp,
            "--orders-key", orders_key,
            "--seed", str(params["seed"]),
            "--bucket", RAW_BUCKET,
            "--orders-dir", ORDERS_PREFIX,
            "--output-dir", RIDER_EVENTS_PREFIX,
            "--min-pings", str(params["min_pings"]),
            "--max-pings", str(params["max_pings"]),
            "--location-jitter-degrees", str(params["location_jitter_degrees"]),
        ]

        _run_generator("rider_events.py", args)

    window = resolve_window()
    orders_task = generate_orders(window["orders_run_timestamp"])
    rider_events_task = generate_rider_events(window["target_date"], window["rider_events_run_timestamp"])
    orders_task >> rider_events_task


generate_orders_and_rider_events()
