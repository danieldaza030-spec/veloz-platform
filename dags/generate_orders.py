"""Triggers `generators/orders.py` to produce a day's orders extract.

Schedules daily, early — `orders.py` is upstream of `payments.py` and
`rider_events.py`, which both read that same date's `orders_<date>.csv` and
fail clearly if it isn't there yet (see those generators' `--orders-dir`
loaders). Deliberately no Asset outlet here: this DAG simulates an upstream
source system, and a real upstream doesn't push a completion event into your
pipeline — `generate_payments`/`generate_rider_events` instead run on their
own offset crons and rely on the generators' own FileNotFoundError as the
backstop if orders hasn't landed yet by then.

Params expose the generator's tunable knobs, including STATUS_WEIGHTS, which
is a module-level constant in orders.py, not a CLI flag before this task —
`--status-weights` was added to the generator itself for that (see
generators/orders.py); this DAG is a thin pass-through onto it, not a
reimplementation. Manual "Trigger DAG w/ config" overrides any of these
per run; the schedule uses the generator's own documented defaults.

Usage: unpause, then either wait for the 01:00 UTC daily run or
`Trigger DAG w/ config` in the UI to override date/num_orders/seed/
status_weights for a one-off run (e.g. a skewed status mix for testing).
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pendulum
from airflow.sdk import Param, dag, get_current_context, task

GENERATORS_DIR = Path("/opt/airflow/generators")
# S3 key prefixes under the raw-incoming-data bucket, not local paths — the
# generator writes to MinIO (see generators/s3_io.py), not the bind-mounted
# ./data/raw the container also has, which is now historical-only.
RAW_BUCKET = "raw-incoming-data"
ORDERS_PREFIX = "orders"


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


@dag(
    dag_id="generate_orders",
    schedule="0 1 * * *",  # 01:00 UTC daily — first of the four, nothing upstream of it
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["infra", "generator"],
    params={
        "date": Param(
            default=None,
            type=["string", "null"],
            format="date",
            description="Extract date, YYYY-MM-DD. Defaults to the run's logical date (ds).",
        ),
        "num_orders": Param(
            default=6000,
            type="integer",
            minimum=1,
            description="Number of order rows to generate (generator default: 6000).",
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
    },
)
def generate_orders():
    @task
    def run() -> None:
        context = get_current_context()
        params = context["params"]
        # context["ds"] is only populated for runs with a logical_date/data
        # interval (cron-scheduled runs). Manual triggers have neither, so
        # fall back to today's UTC date — the same default orders.py itself
        # uses when --date is omitted.
        target_date = params["date"] or context.get("ds") or pendulum.now("UTC").to_date_string()

        args = [
            "--date", target_date,
            "--num-orders", str(params["num_orders"]),
            "--seed", str(params["seed"]),
            "--bucket", RAW_BUCKET,
            "--output-dir", ORDERS_PREFIX,
        ]
        status_weights = params["status_weights"]
        if status_weights:
            pairs = ",".join(f"{k}={v}" for k, v in status_weights.items())
            args += ["--status-weights", pairs]

        _run_generator("orders.py", args)

    run()


generate_orders()
