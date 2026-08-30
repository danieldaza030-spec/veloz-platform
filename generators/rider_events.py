"""Generates Veloz's rider app event stream: location pings and status changes.

Simulates the queue a rider's phone publishes to while working an active
delivery — status_change events at each lifecycle stage plus a handful of
location_ping events in between, jittered around the order's store city.
Full schema and scope are documented in `docs/data-sources.md` (section 2):
events are only generated for riders actively working an order that day (an
order that reached `assigned` or further), not for every rider with the app
open.

Deliberately not globally sorted by event_time: events are written in the
order they're naturally produced (per order, chronological within that
order's own sequence, orders interleaved in whatever order they're processed)
— a real network-delivered stream doesn't arrive time-sorted, and this is
ordinary stream behavior, not an injected bug.

Usage:
    python generators/rider_events.py --date 2026-08-27
"""

from __future__ import annotations

import argparse
import json
import random
import uuid
from datetime import date, datetime, timedelta

import pandas as pd

from reference_data import STORE_CITY
from s3_io import DEFAULT_BUCKET
from s3_io import read_csv as s3_read_csv
from s3_io import write_text

CITY_CENTERS = {
    "Medellín": (6.2442, -75.5812),
    "Bogotá": (4.7110, -74.0721),
    "São Paulo": (-23.5505, -46.6333),
}

# Lifecycle stages in order; each maps a timeline column to its status value.
LIFECYCLE_STAGES = [
    ("assigned_at", "assigned"),
    ("picked_up_at", "picked_up"),
    ("delivered_at", "delivered"),
]

LOCATION_JITTER_DEGREES = 0.05
MIN_PINGS, MAX_PINGS = 2, 5
# Window used when an order is still in-flight with nothing after assigned_at
# to bound the ping window against.
IN_FLIGHT_PING_WINDOW = timedelta(minutes=15)


def _seeded_uuid(rng: random.Random) -> str:
    """Draws a UUID from the seeded RNG rather than uuid.uuid4()'s own entropy.

    uuid.uuid4() reads from the OS, not from `rng`, so using it here would
    make event_id vary run-to-run even under a fixed --seed. This keeps the
    whole file byte-identical for a given seed, matching fulfillment.py's
    --bad-night reproducibility.
    """
    return str(uuid.UUID(int=rng.getrandbits(128), version=4))


def _status_change_events(order, rng: random.Random) -> list[dict]:
    """One status_change event per lifecycle timestamp the order reached."""
    events = []
    for column, status in LIFECYCLE_STAGES:
        event_time = getattr(order, column)
        if pd.isna(event_time):
            continue
        events.append(
            {
                "event_id": _seeded_uuid(rng),
                "rider_id": order.rider_id,
                "order_id": order.order_id,
                "event_type": "status_change",
                "event_time": pd.Timestamp(event_time).to_pydatetime().isoformat(),
                "latitude": None,
                "longitude": None,
                "status": status,
            }
        )
    return events


def _location_ping_events(
    order,
    rng: random.Random,
    min_pings: int = MIN_PINGS,
    max_pings: int = MAX_PINGS,
    location_jitter_degrees: float = LOCATION_JITTER_DEGREES,
) -> list[dict]:
    """min_pings-max_pings location_ping events between assigned_at and the order's last reached timestamp."""
    start = pd.Timestamp(order.assigned_at).to_pydatetime()
    end = start
    for column, _ in LIFECYCLE_STAGES[1:]:
        value = getattr(order, column)
        if not pd.isna(value):
            end = pd.Timestamp(value).to_pydatetime()
    if end <= start:
        end = start + IN_FLIGHT_PING_WINDOW

    center_lat, center_lon = CITY_CENTERS[STORE_CITY[order.store_id]]
    span_seconds = max(int((end - start).total_seconds()), 1)

    events = []
    num_pings = rng.randint(min_pings, max_pings)
    for _ in range(num_pings):
        event_time = start + timedelta(seconds=rng.randint(0, span_seconds))
        events.append(
            {
                "event_id": _seeded_uuid(rng),
                "rider_id": order.rider_id,
                "order_id": order.order_id,
                "event_type": "location_ping",
                "event_time": event_time.isoformat(),
                "latitude": center_lat + rng.uniform(-location_jitter_degrees, location_jitter_degrees),
                "longitude": center_lon + rng.uniform(-location_jitter_degrees, location_jitter_degrees),
                "status": None,
            }
        )
    return events


def generate_rider_events(
    orders_df: pd.DataFrame,
    rng: random.Random,
    min_pings: int = MIN_PINGS,
    max_pings: int = MAX_PINGS,
    location_jitter_degrees: float = LOCATION_JITTER_DEGREES,
) -> list[dict]:
    """Builds the rider-event stream for one day's active orders.

    Args:
        orders_df: that date's orders extract, already filtered to orders
            with a non-null assigned_at (i.e. reached assigned or further).
        rng: seeded RNG for ping counts, timing and jitter.
        min_pings: minimum location_ping events per order (default: MIN_PINGS).
        max_pings: maximum location_ping events per order (default: MAX_PINGS).
        location_jitter_degrees: +/- jitter radius, in degrees, around the
            order's store-city center (default: LOCATION_JITTER_DEGREES).

    Returns:
        Events in natural generation order (per order, chronological within
        that order; orders interleaved as they appear in orders_df) — not
        globally sorted by event_time. See module docstring.
    """
    events = []
    for order in orders_df.itertuples(index=False):
        order_events = _status_change_events(order, rng) + _location_ping_events(
            order, rng, min_pings, max_pings, location_jitter_degrees
        )
        order_events.sort(key=lambda e: e["event_time"])
        events.extend(order_events)
    return events


def _load_active_orders(bucket: str, orders_prefix: str, target_date: date) -> pd.DataFrame:
    key = f"{orders_prefix}/orders_{target_date.isoformat()}.csv"
    try:
        orders_df = s3_read_csv(
            bucket, key, parse_dates=["assigned_at", "picked_up_at", "delivered_at"]
        )
    except FileNotFoundError as exc:
        raise FileNotFoundError(
            f"No orders extract at s3://{bucket}/{key} — generate it first with "
            f"generators/orders.py --date {target_date.isoformat()} before "
            f"running rider_events.py for the same date."
        ) from exc
    return orders_df[orders_df["assigned_at"].notna()].reset_index(drop=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        default=date.today(),
        help="Event date, YYYY-MM-DD (default: today).",
    )
    parser.add_argument(
        "--orders-dir",
        type=str,
        default="orders",
        help="S3 key prefix (under --bucket) containing that date's orders extract (default: orders).",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="rider_events",
        help="S3 key prefix (under --bucket) to write the events JSONL into (default: rider_events).",
    )
    parser.add_argument(
        "--bucket",
        type=str,
        default=DEFAULT_BUCKET,
        help=f"MinIO/S3 bucket to read/write (default: {DEFAULT_BUCKET}).",
    )
    parser.add_argument("--seed", type=int, default=42, help="Random seed (default: 42).")
    parser.add_argument(
        "--min-pings",
        type=int,
        default=MIN_PINGS,
        help=f"Minimum location_ping events per active order (default: {MIN_PINGS}).",
    )
    parser.add_argument(
        "--max-pings",
        type=int,
        default=MAX_PINGS,
        help=f"Maximum location_ping events per active order (default: {MAX_PINGS}).",
    )
    parser.add_argument(
        "--location-jitter-degrees",
        type=float,
        default=LOCATION_JITTER_DEGREES,
        help=f"+/- jitter radius, in degrees, around the order's store-city "
        f"center (default: {LOCATION_JITTER_DEGREES}).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rng = random.Random(args.seed)

    active_orders = _load_active_orders(args.bucket, args.orders_dir, args.date)
    events = generate_rider_events(
        active_orders, rng, args.min_pings, args.max_pings, args.location_jitter_degrees
    )

    key = f"{args.output_dir}/rider_events_{args.date.isoformat()}.jsonl"
    jsonl_text = "".join(json.dumps(event) + "\n" for event in events)
    write_text(args.bucket, key, jsonl_text)

    status_changes = sum(1 for e in events if e["event_type"] == "status_change")
    pings = len(events) - status_changes
    print(
        f"Wrote {len(events)} events to s3://{args.bucket}/{key} "
        f"({len(active_orders)} active orders, {status_changes} status_change, "
        f"{pings} location_ping)"
    )


if __name__ == "__main__":
    main()
