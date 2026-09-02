"""Triggers `generators/fulfillment.py` to produce a day's store fulfillment feed.

Independent of orders.py — the fulfillment feed only needs the shared store/SKU
reference data, not that date's orders extract — so this DAG runs on its own
daily schedule with no Asset dependency on `generate_orders` (now the
`generate_orders` task in `dags/generate_orders_and_rider_events.py`).

Params expose the generator's tunable knobs, including the --bad-night
severity, which was previously only togglable on/off, not tunable in degree
(BASELINE_MISSING_QUANTITY_RATE, MISSING_FILE_STORE_SHARE,
MALFORMED_FILE_STORE_SHARE, MALFORMED_ROW_SHARE were module-level constants
in fulfillment.py before this task — new CLI flags were added to the
generator itself for that; see generators/fulfillment.py). This DAG is a thin
pass-through onto those flags, not a reimplementation. Manual "Trigger DAG
w/ config" overrides any of these per run; the schedule uses the generator's
own documented defaults (bad_night off).

Usage: unpause, then either wait for the daily run or `Trigger DAG w/ config`
in the UI to simulate a worse-than-usual bad night for testing quarantine
logic.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pendulum
from airflow.sdk import Param, dag, get_current_context, task

GENERATORS_DIR = Path("/opt/airflow/generators")
# S3 key prefix under the raw-incoming-data bucket, not a local path — the
# generator writes to MinIO (see generators/s3_io.py), not the bind-mounted
# ./data/raw the container also has, which is now historical-only.
RAW_BUCKET = "raw-incoming-data"
FULFILLMENT_PREFIX = "fulfillment"


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
    dag_id="generate_fulfillment",
    schedule="5 1 * * *",  # 01:05 UTC daily — independent of orders, offset just to stagger load
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["infra", "generator"],
    params={
        "date": Param(
            default=None,
            type=["string", "null"],
            format="date",
            description="Export date, YYYY-MM-DD. Defaults to the run's logical date (ds).",
        ),
        "seed": Param(default=42, type="integer", description="Random seed (generator default: 42)."),
        "bad_night": Param(
            default=False,
            type="boolean",
            description="Simulate the feed's baseline unreliability (missing/malformed store files).",
        ),
        "baseline_missing_quantity_rate": Param(
            default=0.02,
            type="number",
            minimum=0,
            maximum=1,
            description="Share of (store, sku) rows with no quantity counted that day "
            "(applies every night regardless of bad_night; generator default: 0.02).",
        ),
        "missing_file_store_share": Param(
            default=0.10,
            type="number",
            minimum=0,
            maximum=1,
            description="bad_night only: share of stores whose file never arrives (generator default: 0.10).",
        ),
        "malformed_file_store_share": Param(
            default=0.15,
            type="number",
            minimum=0,
            maximum=1,
            description="bad_night only: share of (non-missing) stores whose file arrives "
            "malformed (generator default: 0.15).",
        ),
        "malformed_row_share": Param(
            default=0.12,
            type="number",
            minimum=0,
            maximum=1,
            description="bad_night only: share of rows corrupted within a malformed store's "
            "file (generator default: 0.12).",
        ),
    },
)
def generate_fulfillment():
    @task
    def run() -> None:
        context = get_current_context()
        params = context["params"]
        # context["ds"] is only populated for runs with a logical_date/data
        # interval (cron-scheduled runs); manual triggers lack one in
        # Airflow 3, so fall back to today's UTC date — the same default
        # fulfillment.py itself uses when --date is omitted.
        target_date = params["date"] or context.get("ds") or pendulum.now("UTC").to_date_string()

        args = [
            "--date", target_date,
            "--seed", str(params["seed"]),
            "--bucket", RAW_BUCKET,
            "--output-dir", FULFILLMENT_PREFIX,
            "--baseline-missing-quantity-rate", str(params["baseline_missing_quantity_rate"]),
            "--missing-file-store-share", str(params["missing_file_store_share"]),
            "--malformed-file-store-share", str(params["malformed_file_store_share"]),
            "--malformed-row-share", str(params["malformed_row_share"]),
        ]
        if params["bad_night"]:
            args.append("--bad-night")

        _run_generator("fulfillment.py", args)

    run()


generate_fulfillment()
