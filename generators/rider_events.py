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

Runs on a 5-minute cadence in production (see `docs/data-sources.md`'s
landing-zone note). Each run writes its own
`rider_events/date=<date>/rider_events_<run_timestamp>.jsonl` object and, in
production, reads the exact `orders_<run_timestamp>.csv` its paired orders
run produced via `--orders-key` (passed by the future combined orders +
rider_events DAG over XCom) rather than globbing the whole day's orders
partition. `--orders-dir`/`--date` remain a day-level glob fallback for
manual/standalone invocation only.

Usage:
    # Production: exact paired orders file, explicit run_timestamp.
    python generators/rider_events.py --date 2026-08-27 \\
        --orders-key orders/date=2026-08-27/orders_20260827T143000Z.csv \\
        --run-timestamp 20260827T143000Z

    # Manual/standalone: globs that day's whole orders partition.
    python generators/rider_events.py --date 2026-08-27
"""

from __future__ import annotations

import argparse
import json
import random
import uuid
from datetime import UTC, date, datetime, timedelta

import pandas as pd

from reference_data import STORE_CITY
from s3_io import DEFAULT_BUCKET, RUN_TIMESTAMP_FORMAT, floor_to_window
from s3_io import list_keys as s3_list_keys
from s3_io import read_csv as s3_read_csv
from s3_io import write_text

# argparse help strings run through `%`-style substitution internally
# (e.g. for %(default)s), so a literal strftime format needs its own `%`
# characters doubled wherever it's interpolated into a help= string.
_RUN_TIMESTAMP_FORMAT_HELP = RUN_TIMESTAMP_FORMAT.replace("%", "%%")

ORDERS_DATE_COLUMNS = ["assigned_at", "picked_up_at", "delivered_at"]

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


def _active_orders(orders_df: pd.DataFrame) -> pd.DataFrame:
    """Filters an orders extract down to orders that reached assigned or further."""
    return orders_df[orders_df["assigned_at"].notna()].reset_index(drop=True)


def _load_active_orders_from_key(bucket: str, orders_key: str) -> pd.DataFrame:
    """Loads active orders from one exact orders object — the paired run's own file.

    This is the production path: the combined orders + rider_events DAG
    passes the exact key its own orders run just wrote via XCom, so this
    run's rider_events reads exactly that window's orders and nothing else
    — never a stale file or a different run's window.

    Args:
        bucket: MinIO/S3 bucket to read from.
        orders_key: exact key of the orders extract to read, e.g.
            `orders/date=2026-08-27/orders_20260827T143000Z.csv`.

    Returns:
        Rows with a non-null `assigned_at` (i.e. reached assigned or
        further), reset to a fresh index.

    Raises:
        FileNotFoundError: If `orders_key` doesn't exist.
    """
    try:
        orders_df = s3_read_csv(bucket, orders_key, parse_dates=ORDERS_DATE_COLUMNS)
    except FileNotFoundError as exc:
        raise FileNotFoundError(
            f"No orders extract at s3://{bucket}/{orders_key} — this should have "
            f"been produced by the paired orders run for the same window before "
            f"rider_events.py ran against it."
        ) from exc
    return _active_orders(orders_df)


def _load_active_orders_for_date(bucket: str, orders_prefix: str, target_date: date) -> pd.DataFrame:
    """Loads active orders by globbing an entire day's orders partition.

    Fallback for manual/standalone invocation only — not used by the
    production DAG, which always passes `--orders-key` instead. Orders now
    lands as many 5-minute files per day under `orders/date=<date>/`, so
    this reads and concatenates every object in that partition rather than
    a single known key.

    Args:
        bucket: MinIO/S3 bucket to read from.
        orders_prefix: S3 key prefix (under `bucket`) that orders partitions
            live under, e.g. `orders`.
        target_date: the date partition to glob.

    Returns:
        Rows with a non-null `assigned_at` (i.e. reached assigned or
        further), reset to a fresh index.

    Raises:
        FileNotFoundError: If that date's partition has no objects yet.
    """
    partition_prefix = f"{orders_prefix}/date={target_date.isoformat()}/"
    keys = [key for key in s3_list_keys(bucket, partition_prefix) if key.endswith(".csv")]
    if not keys:
        raise FileNotFoundError(
            f"No orders extracts under s3://{bucket}/{partition_prefix} — generate "
            f"at least one first, e.g. generators/orders.py --run-timestamp "
            f"{target_date.isoformat()}T00:00:00 --daily-target-orders <n> "
            f"(see docs/data-sources.md), before running rider_events.py for "
            f"the same date."
        )
    orders_df = pd.concat(
        (s3_read_csv(bucket, key, parse_dates=ORDERS_DATE_COLUMNS) for key in keys),
        ignore_index=True,
    )
    return _active_orders(orders_df)


def _load_active_orders(
    bucket: str,
    orders_key: str | None,
    orders_prefix: str,
    target_date: date,
) -> pd.DataFrame:
    """Loads that run's active orders, preferring an exact key over a day-level glob.

    When `orders_key` is given, this reads only that single object — the
    day-level glob (`_load_active_orders_for_date`) is never invoked. That
    exact-key path is what the production DAG uses; the glob is a
    manual/standalone fallback only.

    Args:
        bucket: MinIO/S3 bucket to read from.
        orders_key: exact orders object key to read, or `None` to fall back
            to the day-level glob.
        orders_prefix: S3 key prefix orders partitions live under, used only
            when `orders_key` is `None`.
        target_date: date partition to glob, used only when `orders_key` is
            `None`.

    Returns:
        Rows with a non-null `assigned_at` (i.e. reached assigned or
        further), reset to a fresh index.
    """
    if orders_key is not None:
        return _load_active_orders_from_key(bucket, orders_key)
    return _load_active_orders_for_date(bucket, orders_prefix, target_date)


def _parse_run_timestamp(raw: str) -> str:
    """Validates a `--run-timestamp` value against the shared run_timestamp format.

    Args:
        raw: candidate timestamp string.

    Returns:
        `raw`, unchanged, once validated.

    Raises:
        argparse.ArgumentTypeError: If `raw` doesn't match RUN_TIMESTAMP_FORMAT.
    """
    try:
        datetime.strptime(raw, RUN_TIMESTAMP_FORMAT)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"--run-timestamp must match {RUN_TIMESTAMP_FORMAT!r} (UTC, second "
            f"precision), got {raw!r}"
        ) from exc
    return raw


def _default_run_timestamp() -> str:
    """Current UTC time floored to its 5-minute window, formatted for --run-timestamp.

    Mirrors orders.py's own _default_run_timestamp so a manual/standalone run
    of either generator floors to the same window boundary. Used only as a
    fallback: the production DAG always passes --run-timestamp explicitly,
    pairing this run with its own orders run's file.

    Returns:
        The floored current UTC time, formatted per RUN_TIMESTAMP_FORMAT.
    """
    floored = floor_to_window(datetime.now(UTC).replace(tzinfo=None))
    return floored.strftime(RUN_TIMESTAMP_FORMAT)


def _resolve_target_date(raw_date: date | None, raw_run_timestamp: str | None) -> tuple[date, str]:
    """Resolves --date/--run-timestamp into one agreeing (date, run_timestamp) pair.

    When --run-timestamp is given, its own date takes precedence — this is
    the production path, where the DAG passes both --date and
    --run-timestamp, and the two must never silently diverge (a real risk
    near UTC-midnight boundaries). When --run-timestamp isn't given, --date
    (or today) drives the partition on its own, and run_timestamp falls back
    to the current window's default — the manual/standalone path, where
    --date and run_timestamp aren't expected to relate to each other (e.g.
    backfilling a past date's events by hand today).

    Args:
        raw_date: --date as parsed, or None if the flag wasn't given.
        raw_run_timestamp: --run-timestamp as parsed, or None if the flag
            wasn't given.

    Returns:
        A (target_date, run_timestamp) pair to drive this run's output key
        and, for the day-level glob fallback, which partition to read.

    Raises:
        ValueError: If both flags are given and name different dates.
    """
    if raw_run_timestamp is not None:
        run_timestamp_date = datetime.strptime(raw_run_timestamp, RUN_TIMESTAMP_FORMAT).date()
        if raw_date is not None and raw_date != run_timestamp_date:
            raise ValueError(
                f"--date {raw_date.isoformat()} disagrees with --run-timestamp "
                f"{raw_run_timestamp!r} (implies date {run_timestamp_date.isoformat()}); "
                f"pass only one, or make them agree."
            )
        return run_timestamp_date, raw_run_timestamp
    return raw_date or date.today(), _default_run_timestamp()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        default=None,
        help="Event date, YYYY-MM-DD (default: --run-timestamp's own date when "
        "that's given, else today). Sets the output 'date=<date>/' partition. "
        "Must agree with --run-timestamp's date if both are given.",
    )
    parser.add_argument(
        "--run-timestamp",
        type=_parse_run_timestamp,
        default=None,
        help=f"This run's start time, formatted {_RUN_TIMESTAMP_FORMAT_HELP} (UTC, "
        "second precision) — same run_timestamp convention orders.py uses for "
        "its own output key, so a paired run's rider_events and orders objects "
        "share one run_timestamp. Also drives the output date partition (see "
        "--date). PRODUCTION: passed by the future combined orders + "
        "rider_events DAG. Defaults to the current UTC time floored to the "
        "current 5-minute window, for manual/standalone invocation only.",
    )
    parser.add_argument(
        "--orders-key",
        type=str,
        default=None,
        help="PRODUCTION: exact S3 key of the orders extract to read, e.g. "
        "'orders/date=2026-08-27/orders_20260827T143000Z.csv' — the future "
        "combined orders + rider_events DAG passes this via XCom so this run "
        "reads exactly its own paired orders run's file. Overrides "
        "--orders-dir/--date when given; only that one object is read.",
    )
    parser.add_argument(
        "--orders-dir",
        type=str,
        default="orders",
        help="FALLBACK for manual/standalone invocation only (not used by the "
        "production DAG): S3 key prefix (under --bucket) that orders "
        "partitions live under. Globs every orders_*.csv object under that "
        "date's partition when --orders-key is not given (default: orders).",
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
    target_date, run_timestamp = _resolve_target_date(args.date, args.run_timestamp)
    # Seeded from (seed, run_timestamp) rather than just seed: keeps output
    # deterministic for a given (seed, run_timestamp) pair, but stops every
    # window of the day from replaying the same ping timing/jitter/event_id
    # sequence a bare random.Random(args.seed) would produce each run. A
    # single string, not a tuple: random.Random no longer accepts arbitrary
    # hashable objects as a seed (removed in Python 3.11).
    rng = random.Random(f"{args.seed}:{run_timestamp}")

    active_orders = _load_active_orders(args.bucket, args.orders_key, args.orders_dir, target_date)
    events = generate_rider_events(
        active_orders, rng, args.min_pings, args.max_pings, args.location_jitter_degrees
    )

    key = f"{args.output_dir}/date={target_date.isoformat()}/rider_events_{run_timestamp}.jsonl"
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
