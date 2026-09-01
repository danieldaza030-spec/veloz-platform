"""Generates a simulated Postgres periodic extract of Veloz's orders table.

Each row is a single order's current state as observed at extract time —
this mirrors a periodic poll of the app's transactional order table, not a
changelog. Lifecycle: created -> assigned -> picked_up -> delivered, with
cancellation possible from any stage before delivery.

Orders are generated one 5-minute window at a time, matching the producing
Airflow run's own `data_interval_start` — not a whole day in one shot — so
that each run lands its own object under a `date=<date>/` partition (see
docs/data-sources.md's Landing zone section). `--daily-target-orders`
still drives the *day's* total volume; HOURLY_VOLUME_MULTIPLIERS spreads
that total across the day's windows to match quick-commerce demand shape.

Usage:
    python generators/orders.py --run-timestamp 2026-08-27T13:05:00 --daily-target-orders 6000
"""

from __future__ import annotations

import argparse
import random
import uuid
from datetime import datetime, timedelta, timezone

import pandas as pd

from reference_data import STORE_IDS, riders_for_store
from s3_io import DEFAULT_BUCKET, RUN_TIMESTAMP_FORMAT, WINDOW_MINUTES, floor_to_window, write_csv

# Most orders complete normally; a smaller share cancel or are still
# in-flight at the moment the extract is taken. Order matches STATUS_STAGES.
STATUS_WEIGHTS = {
    "delivered": 0.78,
    "cancelled": 0.07,
    "picked_up": 0.05,
    "assigned": 0.06,
    "created": 0.04,
}
STATUSES = list(STATUS_WEIGHTS.keys())
WEIGHTS = list(STATUS_WEIGHTS.values())

# Cancellations happen more often early (customer changes their mind, item
# out of stock) than after a rider has already picked the order up.
CANCEL_STAGE_WEIGHTS = {"created": 0.55, "assigned": 0.35, "picked_up": 0.10}
CANCEL_STAGES = list(CANCEL_STAGE_WEIGHTS.keys())
CANCEL_WEIGHTS = list(CANCEL_STAGE_WEIGHTS.values())

ORDER_TOTAL_RANGE = (8.0, 65.0)  # plausible quick-commerce basket size, USD-equivalent

# Today's implicit daily total (the old --num-orders default), now the day's
# aggregate target that HOURLY_VOLUME_MULTIPLIERS spreads across windows.
DEFAULT_DAILY_TARGET_ORDERS = 5000000

# Relative demand by hour-of-day (index 0 = 00:00, 23 = 23:00), shaping a
# quick-commerce day: near-zero overnight (stores are effectively closed,
# the same 06:00-23:59 store-hours rule this module always enforced, now
# expressed here instead of in _random_time_on's bounds), a breakfast climb,
# a lunch peak, an afternoon lull, a dinner peak, then a late tapering-off.
# Values are relative weights, not probabilities — they don't need to sum
# to 1; window_order_count normalizes against their sum.
HOURLY_VOLUME_MULTIPLIERS = [
    0.05, 0.02, 0.01, 0.01, 0.01, 0.02,  # 00:00-05:59 — closed / near-zero
    0.30, 0.60, 0.90, 1.00, 0.80, 0.90,  # 06:00-11:59 — opening, breakfast
    1.30, 1.40, 1.00, 0.80, 0.90, 1.10,  # 12:00-17:59 — lunch peak, afternoon lull
    1.40, 1.50, 1.30, 1.00, 0.60, 0.20,  # 18:00-23:59 — dinner peak, tapering off
]


def expected_window_order_count(
    window_start: datetime,
    daily_target_orders: int,
    window_minutes: int = WINDOW_MINUTES,
) -> float:
    """Computes a window's deterministic expected share of the day's order volume.

    A window's expected count is daily_target_orders times that window's
    share of the day's total multiplier-weighted volume: every hour has the
    same number of windows, so a window's share is simply its own hour's
    multiplier divided by the sum of all 24 hours' multipliers, spread
    evenly across that hour's windows.

    Args:
        window_start: the window's start timestamp; only its hour matters.
        daily_target_orders: the day's total order count to distribute.
        window_minutes: width of each window in minutes.

    Returns:
        The window's expected (fractional, pre-rounding) order count.
    """
    windows_per_hour = 60 // window_minutes
    total_weight = sum(HOURLY_VOLUME_MULTIPLIERS) * windows_per_hour
    hour_multiplier = HOURLY_VOLUME_MULTIPLIERS[window_start.hour]
    return daily_target_orders * hour_multiplier / total_weight


def window_order_count(
    window_start: datetime,
    daily_target_orders: int,
    rng: random.Random,
    window_minutes: int = WINDOW_MINUTES,
) -> int:
    """Draws a single window's order count around its expected value.

    Stochastically rounds expected_window_order_count's fractional result
    (probability of rounding up equal to the fractional part) rather than
    always flooring or always rounding to nearest, so that summing this
    function's output across a full day's windows is unbiased — its
    expectation equals daily_target_orders exactly — while still varying
    window-to-window under a fixed seed instead of producing an identical
    count every time the same hour recurs.

    Args:
        window_start: the window's start timestamp.
        daily_target_orders: the day's total order count to distribute.
        rng: seeded RNG used for the stochastic rounding step.
        window_minutes: width of each window in minutes.

    Returns:
        The number of orders to generate for this window.
    """
    expected = expected_window_order_count(window_start, daily_target_orders, window_minutes)
    count = int(expected)
    if rng.random() < expected - count:
        count += 1
    return count


def _random_time_on(window_start: datetime, window_end: datetime, rng: random.Random) -> datetime:
    """Returns a random timestamp within [window_start, window_end).

    The overnight near-silence that used to be enforced here directly (via a
    06:00-24:00 bound) is now a consequence of HOURLY_VOLUME_MULTIPLIERS
    instead: overnight windows simply get very few (often zero) orders, so
    this function only needs to stay within its own window's bounds.
    """
    window_seconds = int((window_end - window_start).total_seconds())
    offset = rng.randint(0, window_seconds - 1)
    return window_start + timedelta(seconds=offset)


def _build_timeline(status: str, created_at: datetime, rng: random.Random) -> dict:
    """Builds the assigned/picked_up/delivered/cancelled timestamps for one order.

    Returns a dict with keys assigned_at, picked_up_at, delivered_at (each a
    datetime or None) and updated_at (the timestamp of the order's last
    recorded state change, i.e. what a periodic-extract job would show).
    """
    assigned_at = picked_up_at = delivered_at = None
    updated_at = created_at

    def advance_to_assigned():
        nonlocal assigned_at, updated_at
        assigned_at = created_at + timedelta(minutes=rng.randint(1, 5))
        updated_at = assigned_at

    def advance_to_picked_up():
        nonlocal picked_up_at, updated_at
        picked_up_at = assigned_at + timedelta(minutes=rng.randint(3, 10))
        updated_at = picked_up_at

    def advance_to_delivered():
        nonlocal delivered_at, updated_at
        delivered_at = picked_up_at + timedelta(minutes=rng.randint(8, 25))
        updated_at = delivered_at

    if status == "created":
        pass
    elif status == "assigned":
        advance_to_assigned()
    elif status == "picked_up":
        advance_to_assigned()
        advance_to_picked_up()
    elif status == "delivered":
        advance_to_assigned()
        advance_to_picked_up()
        advance_to_delivered()
    elif status == "cancelled":
        stage = rng.choices(CANCEL_STAGES, weights=CANCEL_WEIGHTS, k=1)[0]
        if stage in ("assigned", "picked_up"):
            advance_to_assigned()
        if stage == "picked_up":
            advance_to_picked_up()
        # The cancellation itself is recorded a short while after whatever
        # stage the order last reached.
        updated_at = updated_at + timedelta(minutes=rng.randint(1, 10))
    else:
        raise ValueError(f"unknown status: {status}")

    return {
        "assigned_at": assigned_at,
        "picked_up_at": picked_up_at,
        "delivered_at": delivered_at,
        "updated_at": updated_at,
    }


def generate_orders_window(
    window_start: datetime,
    num_orders: int,
    rng: random.Random,
    status_weights: dict[str, float] | None = None,
    window_minutes: int = WINDOW_MINUTES,
) -> pd.DataFrame:
    """Generates num_orders order rows representing extract state within one window.

    Args:
        window_start: start of the (window_minutes-wide) window; created_at
            is drawn from [window_start, window_start + window_minutes).
        num_orders: number of order rows to generate for this window, e.g.
            from window_order_count.
        rng: seeded RNG.
        status_weights: overrides the default STATUS_WEIGHTS mix. Must have
            exactly the same keys as STATUS_WEIGHTS — _build_timeline only
            knows how to advance those five statuses. Values need not sum to
            1; random.choices normalizes them.
        window_minutes: width of the window in minutes.

    Returns:
        One row per generated order, with columns order_id, store_id,
        rider_id, status, created_at, assigned_at, picked_up_at,
        delivered_at, order_total and updated_at.

    Raises:
        ValueError: If status_weights is given but its keys don't exactly
            match STATUS_WEIGHTS' keys.
    """
    if status_weights is None:
        status_weights = STATUS_WEIGHTS
    elif set(status_weights) != set(STATUS_WEIGHTS):
        raise ValueError(
            f"status_weights keys must be exactly {sorted(STATUS_WEIGHTS)}, "
            f"got {sorted(status_weights)}"
        )
    statuses = list(status_weights.keys())
    weights = list(status_weights.values())
    window_end = window_start + timedelta(minutes=window_minutes)

    rows = []
    for _ in range(num_orders):
        store_id = rng.choice(STORE_IDS)
        status = rng.choices(statuses, weights=weights, k=1)[0]
        created_at = _random_time_on(window_start, window_end, rng)
        timeline = _build_timeline(status, created_at, rng)

        rider_id = None
        if timeline["assigned_at"] is not None:
            rider_id = rng.choice(riders_for_store(store_id))

        rows.append(
            {
                "order_id": str(uuid.uuid4()),
                "store_id": store_id,
                "rider_id": rider_id,
                "status": status,
                "created_at": created_at,
                "assigned_at": timeline["assigned_at"],
                "picked_up_at": timeline["picked_up_at"],
                "delivered_at": timeline["delivered_at"],
                "order_total": round(rng.uniform(*ORDER_TOTAL_RANGE), 2),
                "updated_at": timeline["updated_at"],
            }
        )

    columns = [
        "order_id",
        "store_id",
        "rider_id",
        "status",
        "created_at",
        "assigned_at",
        "picked_up_at",
        "delivered_at",
        "order_total",
        "updated_at",
    ]
    return pd.DataFrame(rows, columns=columns)


def _parse_status_weights(raw: str) -> dict[str, float]:
    """Parses '--status-weights' CLI input: comma-separated key=value pairs."""
    weights: dict[str, float] = {}
    for pair in raw.split(","):
        key, _, value = pair.partition("=")
        if not value:
            raise argparse.ArgumentTypeError(
                f"malformed status_weights entry {pair!r}, expected 'status=weight'"
            )
        weights[key.strip()] = float(value)
    return weights


def _parse_run_timestamp(raw: str) -> datetime:
    """Parses '--run-timestamp' CLI input: a naive-UTC ISO 8601 timestamp."""
    return datetime.strptime(raw, "%Y-%m-%dT%H:%M:%S")


def _default_run_timestamp() -> datetime:
    """Current UTC time floored to its window — used only for standalone runs.

    The DAG that eventually schedules this generator always passes
    --run-timestamp explicitly (its own data_interval_start); this default
    only matters when the generator is invoked by hand.
    """
    return floor_to_window(datetime.now(timezone.utc).replace(tzinfo=None))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--run-timestamp",
        type=_parse_run_timestamp,
        default=_default_run_timestamp(),
        help=(
            "This run's window start, matching the producing Airflow run's "
            "data_interval_start (ISO 8601, naive UTC, e.g. 2026-08-27T13:05:00). "
            "Defaults to now, floored to the current 5-minute window."
        ),
    )
    parser.add_argument(
        "--daily-target-orders",
        type=int,
        default=DEFAULT_DAILY_TARGET_ORDERS,
        help=(
            f"Full day's total order count, spread across windows by "
            f"HOURLY_VOLUME_MULTIPLIERS (default: {DEFAULT_DAILY_TARGET_ORDERS}, "
            f"~180k/mo / 30 days)."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="orders",
        help="S3 key prefix (under --bucket) to write the extract CSV into (default: orders).",
    )
    parser.add_argument(
        "--bucket",
        type=str,
        default=DEFAULT_BUCKET,
        help=f"MinIO/S3 bucket to write into (default: {DEFAULT_BUCKET}).",
    )
    parser.add_argument("--seed", type=int, default=42, help="Random seed (default: 42).")
    parser.add_argument(
        "--status-weights",
        type=_parse_status_weights,
        default=None,
        help=(
            "Override the delivered/cancelled/picked_up/assigned/created status "
            "mix as comma-separated key=value pairs, e.g. "
            "'delivered=0.7,cancelled=0.1,picked_up=0.05,assigned=0.1,created=0.05'. "
            "Must name exactly those five statuses; values need not sum to 1 "
            "(default: the documented mix in STATUS_WEIGHTS)."
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    window_start = args.run_timestamp
    # Seeded from (seed, window_start) rather than just seed: keeps output
    # deterministic for a given (seed, run_timestamp) pair, but stops every
    # window of the day from replaying the same store/status/timing sequence
    # a bare random.Random(args.seed) would produce each run. A single
    # string, not a tuple: random.Random no longer accepts arbitrary
    # hashable objects as a seed (removed in Python 3.11).
    rng = random.Random(f"{args.seed}:{window_start.isoformat()}")
    num_orders = window_order_count(window_start, args.daily_target_orders, rng)
    df = generate_orders_window(window_start, num_orders, rng, status_weights=args.status_weights)

    run_timestamp_str = window_start.strftime(RUN_TIMESTAMP_FORMAT)
    key = f"{args.output_dir}/date={window_start.date().isoformat()}/orders_{run_timestamp_str}.csv"
    write_csv(df, args.bucket, key)

    print(f"Wrote {len(df)} orders to s3://{args.bucket}/{key}")
    print(df["status"].value_counts())


if __name__ == "__main__":
    main()
