"""Triggers `generators/payments.py` to produce a day's payments/commissions ledger.

`payments.py` reads that same date's `orders_<date>.csv` (produced by
`generate_orders`) and raises FileNotFoundError if it's missing. Deliberately
no Asset dependency on `generate_orders` here: this DAG simulates a separate
upstream system (Finance's payments ledger), and a real upstream doesn't get
notified when another upstream's extract has landed — it runs on its own
schedule. So this DAG runs on its own daily cron, offset late enough after
`generate_orders`' 01:00 UTC run to normally find that day's file already
there; the generator's own FileNotFoundError is the backstop for the case
where it isn't (e.g. an orders run that's late, failed, or a manual
out-of-band trigger naming a different date).

Params expose the generator's tunable knobs, including the wrong-amount/
missing-payment injection rates and settlement-lag distribution, which were
module-level constants in payments.py before this task — new CLI flags were
added to the generator itself for that; see generators/payments.py. This DAG
is a thin pass-through onto those flags, not a reimplementation. Manual
"Trigger DAG w/ config" overrides any of these per run (e.g. to stress-test a
reconciliation job against a much higher error rate); the asset-triggered
run uses the generator's own documented defaults.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pendulum
from airflow.sdk import Param, dag, get_current_context, task

GENERATORS_DIR = Path("/opt/airflow/generators")
# S3 key prefixes under the raw-incoming-data bucket, not local paths — the
# generator reads/writes MinIO (see generators/s3_io.py), not the bind-mounted
# ./data/raw the container also has, which is now historical-only.
RAW_BUCKET = "raw-incoming-data"
ORDERS_PREFIX = "orders"
PAYMENTS_PREFIX = "payments"


def _run_generator(script: str, args: list[str]) -> None:
    """Invokes a generators/*.py CLI script as its own OS process.

    Not an in-process import: the generator's own argparse stays the single
    source of truth for its CLI surface and defaults, and a generator crash
    can't take the Airflow task's interpreter down with it. Output streams to
    the task log; a non-zero exit fails the task with the generator's stderr
    (e.g. its FileNotFoundError when that date's orders extract is missing).
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
    dag_id="generate_payments",
    schedule="15 1 * * *",  # 01:15 UTC daily — offset after generate_orders' 01:00 run, own cron
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["infra", "generator"],
    params={
        "date": Param(
            default=None,
            type=["string", "null"],
            format="date",
            description="Date whose delivered orders to settle, YYYY-MM-DD. "
            "Defaults to the run's logical date (ds).",
        ),
        "seed": Param(default=42, type="integer", description="Random seed (generator default: 42)."),
        "wrong_amount_rate": Param(
            default=0.02,
            type="number",
            minimum=0,
            maximum=1,
            description="Share of payment rows given a policy-mismatched commission_amount "
            "(generator default: 0.02).",
        ),
        "missing_payment_rate": Param(
            default=0.01,
            type="number",
            minimum=0,
            maximum=1,
            description="Share of delivered orders with no payment row at all "
            "(generator default: 0.01).",
        ),
        "lag_mean_hours": Param(
            default=20.0,
            type="number",
            minimum=0,
            description="Mean of the settlement-lag normal distribution, in hours "
            "(generator default: 20.0).",
        ),
        "lag_stdev_hours": Param(
            default=6.0,
            type="number",
            minimum=0,
            description="Stdev of the settlement-lag normal distribution, in hours "
            "(generator default: 6.0).",
        ),
    },
)
def generate_payments():
    @task
    def run() -> None:
        context = get_current_context()
        params = context["params"]
        # context["ds"] is only populated for runs with a logical_date/data
        # interval (cron-scheduled runs). Manual triggers lack a
        # logical_date, so fall back to today's UTC date — the same default
        # payments.py itself uses when --date is omitted.
        target_date = params["date"] or context.get("ds") or pendulum.now("UTC").to_date_string()
        interval_start = context.get("data_interval_start")
        if interval_start is not None:
            run_timestamp = interval_start.in_timezone("UTC").strftime("%Y%m%dT%H%M%SZ")
        else:
            run_timestamp = pendulum.now("UTC").strftime("%Y%m%dT%H%M%SZ")

        args = [
            "--date", target_date,
            "--run-timestamp", run_timestamp,
            "--seed", str(params["seed"]),
            "--bucket", RAW_BUCKET,
            "--orders-dir", ORDERS_PREFIX,
            "--output-dir", PAYMENTS_PREFIX,
            "--wrong-amount-rate", str(params["wrong_amount_rate"]),
            "--missing-payment-rate", str(params["missing_payment_rate"]),
            "--lag-mean-hours", str(params["lag_mean_hours"]),
            "--lag-stdev-hours", str(params["lag_stdev_hours"]),
        ]

        _run_generator("payments.py", args)

    run()


generate_payments()
