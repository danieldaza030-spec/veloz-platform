"""Generates a simulated Postgres periodic extract of Veloz's orders table.

Each row is a single order's current state as observed at extract time —
this mirrors a periodic poll of the app's transactional order table, not a
changelog. Lifecycle: created -> assigned -> picked_up -> delivered, with
cancellation possible from any stage before delivery.

Usage:
    python generators/orders.py --date 2026-08-27 --num-orders 6000
"""

from __future__ import annotations

import argparse
import random
import uuid
from datetime import date, datetime, timedelta

import pandas as pd

from reference_data import STORE_IDS, riders_for_store
from s3_io import DEFAULT_BUCKET, write_csv

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


def _random_time_on(target_date: date, rng: random.Random) -> datetime:
    """Returns a random timestamp within target_date (stores operate ~06:00-23:59)."""
    seconds_into_day = rng.randint(6 * 3600, 24 * 3600 - 1)
    return datetime.combine(target_date, datetime.min.time()) + timedelta(seconds=seconds_into_day)


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


def generate_orders(
    target_date: date,
    num_orders: int,
    rng: random.Random,
    status_weights: dict[str, float] | None = None,
) -> pd.DataFrame:
    """Generates num_orders order rows representing extract state on target_date.

    Args:
        target_date: the extract date.
        num_orders: number of order rows to generate.
        rng: seeded RNG.
        status_weights: overrides the default STATUS_WEIGHTS mix. Must have
            exactly the same keys as STATUS_WEIGHTS — _build_timeline only
            knows how to advance those five statuses. Values need not sum to
            1; random.choices normalizes them.
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

    rows = []
    for _ in range(num_orders):
        store_id = rng.choice(STORE_IDS)
        status = rng.choices(statuses, weights=weights, k=1)[0]
        created_at = _random_time_on(target_date, rng)
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        default=date.today(),
        help="Extract date, YYYY-MM-DD (default: today).",
    )
    parser.add_argument(
        "--num-orders",
        type=int,
        default=6000,
        help="Number of order rows to generate (default: 6000, ~180k/mo / 30 days).",
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
    rng = random.Random(args.seed)

    df = generate_orders(args.date, args.num_orders, rng, status_weights=args.status_weights)

    key = f"{args.output_dir}/orders_{args.date.isoformat()}.csv"
    write_csv(df, args.bucket, key)

    print(f"Wrote {len(df)} orders to s3://{args.bucket}/{key}")
    print(df["status"].value_counts())


if __name__ == "__main__":
    main()
