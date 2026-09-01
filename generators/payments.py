"""Generates Veloz's daily payments/commissions ledger export.

Simulates the daily file Finance's separate payments system produces for the
previous day's completed (delivered) orders: one commission payout per
delivered order, computed from Veloz's tiered commission policy with a
late-delivery deduction, settled some hours after delivery per Finance's
normal settlement-lag pattern. Full policy and lag distribution are
documented in `docs/data-sources.md` (section 4) — this generator implements
that spec, it doesn't define it.

Two failure modes are injected at fixed, seeded rates, the same way
`--bad-night` works for `fulfillment.py`: some delivered orders get a
commission amount that doesn't match the policy, and some get no payment row
at all. The point isn't that a reconciliation job can't find them — it's that
it has to recompute the expected commission itself rather than trust the
ledger's own number.

Usage:
    python generators/payments.py --date 2026-08-27
"""

from __future__ import annotations

import argparse
import random
import uuid
from datetime import UTC, date, datetime, timedelta

import pandas as pd

from s3_io import DEFAULT_BUCKET, RUN_TIMESTAMP_FORMAT
from s3_io import list_keys as s3_list_keys
from s3_io import read_csv as s3_read_csv
from s3_io import write_csv

COLUMNS = ["payment_id", "order_id", "rider_id", "commission_amount", "payment_recorded_at"]

# Commission policy: base rate by order_total tier, per docs/data-sources.md.
COMMISSION_TIERS = [
    (15.00, 0.10),  # order_total < $15.00
    (35.00, 0.13),  # $15.00 <= order_total < $35.00
    (float("inf"), 0.16),  # order_total >= $35.00
]
LATE_DELIVERY_THRESHOLD = timedelta(minutes=45)
LATE_DEDUCTION = 0.03
MIN_EFFECTIVE_RATE = 0.05

# Settlement lag: normal(mean=20h, stdev=6h), clipped to [2h, 48h].
LAG_MEAN_HOURS = 20.0
LAG_STDEV_HOURS = 6.0
LAG_MIN_HOURS = 2.0
LAG_MAX_HOURS = 48.0


def _seeded_uuid(rng: random.Random) -> str:
    """Draws a UUID from the seeded RNG rather than uuid.uuid4()'s own entropy.

    uuid.uuid4() reads from the OS, not from `rng`, so using it here would
    make payment_id vary run-to-run even under a fixed --seed. This keeps
    the whole file byte-identical for a given seed, matching fulfillment.py's
    --bad-night reproducibility.
    """
    return str(uuid.UUID(int=rng.getrandbits(128), version=4))

# Deliberate data-quality issues, documented rates (docs/data-sources.md #4).
WRONG_AMOUNT_RATE = 0.02
MISSING_PAYMENT_RATE = 0.01


def _base_rate(order_total: float) -> float:
    """Returns the base commission rate for order_total's tier."""
    for upper_bound, rate in COMMISSION_TIERS:
        if order_total < upper_bound:
            return rate
    raise AssertionError("unreachable: last tier bound is inf")


def _effective_rate(order_total: float, created_at: datetime, delivered_at: datetime) -> float:
    """Applies the late-delivery deduction, if any, to the base rate."""
    base_rate = _base_rate(order_total)
    is_late = (delivered_at - created_at) > LATE_DELIVERY_THRESHOLD
    if is_late:
        return max(base_rate - LATE_DEDUCTION, MIN_EFFECTIVE_RATE)
    return base_rate


def _correct_commission(order_total: float, created_at: datetime, delivered_at: datetime) -> float:
    """Computes the policy-correct commission_amount for a delivered order."""
    rate = _effective_rate(order_total, created_at, delivered_at)
    return round(order_total * rate, 2)


def _settlement_lag_hours(
    rng: random.Random,
    mean_hours: float = LAG_MEAN_HOURS,
    stdev_hours: float = LAG_STDEV_HOURS,
) -> float:
    lag = rng.gauss(mean_hours, stdev_hours)
    return min(max(lag, LAG_MIN_HOURS), LAG_MAX_HOURS)


def _wrong_commission(correct_amount: float, order_total: float, rng: random.Random) -> float:
    """Produces a commission_amount that deliberately does not match the policy.

    Two mechanisms, chosen at random: apply a different tier's rate, or apply
    an arbitrary error multiplier — either way the result is detectably wrong
    against a recomputation from order_total/timing, which is the point.
    """
    if rng.random() < 0.5:
        wrong_rate = rng.choice([r for _, r in COMMISSION_TIERS])
        candidate = round(order_total * wrong_rate, 2)
        if candidate != correct_amount:
            return candidate
    # Fall through (or chosen path): scale the correct amount by an error
    # factor clearly outside normal rounding/float noise.
    error_factor = rng.choice([0.5, 0.75, 1.25, 1.5, 2.0])
    return round(correct_amount * error_factor, 2)


def generate_payments(
    orders_df: pd.DataFrame,
    rng: random.Random,
    wrong_amount_rate: float = WRONG_AMOUNT_RATE,
    missing_payment_rate: float = MISSING_PAYMENT_RATE,
    lag_mean_hours: float = LAG_MEAN_HOURS,
    lag_stdev_hours: float = LAG_STDEV_HOURS,
) -> pd.DataFrame:
    """Builds the payments ledger for one day's delivered orders.

    Args:
        orders_df: that date's orders extract, already filtered to
            status == "delivered" rows.
        rng: seeded RNG shared across lag/error injection for reproducibility.
        wrong_amount_rate: share of remaining rows given a policy-mismatched
            commission_amount (default: the documented ~2%, WRONG_AMOUNT_RATE).
        missing_payment_rate: share of delivered orders with no payment row at
            all (default: the documented ~1%, MISSING_PAYMENT_RATE).
        lag_mean_hours: mean of the settlement-lag normal distribution.
        lag_stdev_hours: stdev of the settlement-lag normal distribution.

    Returns:
        One payment row per delivered order, minus the missing_payment_rate
        share dropped entirely, with wrong_amount_rate of the remaining rows
        carrying a policy-mismatched amount.
    """
    rows = []
    for order in orders_df.itertuples(index=False):
        if rng.random() < missing_payment_rate:
            continue  # a payment that never landed

        created_at = pd.Timestamp(order.created_at).to_pydatetime()
        delivered_at = pd.Timestamp(order.delivered_at).to_pydatetime()
        # order.order_total is a numpy.float64 from pandas; round() on it can
        # disagree with round() on the equal-valued native float (numpy's
        # rounding doesn't always match Python's for values like x.xx5), which
        # would inject spurious mismatches beyond the documented ~2%. Casting
        # to a native float keeps this generator's "correct" value the one a
        # downstream reconciliation doing plain-float arithmetic will recompute.
        order_total = float(order.order_total)
        correct_amount = _correct_commission(order_total, created_at, delivered_at)

        if rng.random() < wrong_amount_rate:
            commission_amount = _wrong_commission(correct_amount, order_total, rng)
        else:
            commission_amount = correct_amount

        lag = timedelta(hours=_settlement_lag_hours(rng, lag_mean_hours, lag_stdev_hours))
        payment_recorded_at = delivered_at + lag

        rows.append(
            {
                "payment_id": _seeded_uuid(rng),
                "order_id": order.order_id,
                "rider_id": order.rider_id,
                "commission_amount": commission_amount,
                "payment_recorded_at": payment_recorded_at,
            }
        )

    return pd.DataFrame(rows, columns=COLUMNS)


def _load_delivered_orders(bucket: str, orders_prefix: str, target_date: date) -> pd.DataFrame:
    partition_prefix = f"{orders_prefix}/date={target_date.isoformat()}/"
    keys = [
        key
        for key in s3_list_keys(bucket, partition_prefix)
        if key.rsplit("/", 1)[-1].startswith("orders_") and key.endswith(".csv")
    ]
    if not keys:
        raise FileNotFoundError(
            f"No orders extracts under s3://{bucket}/{partition_prefix} — generate "
            f"at least one first with generators/orders.py --run-timestamp "
            f"<window-start> before "
            f"running payments.py for the same date."
        )
    orders_df = pd.concat(
        (s3_read_csv(bucket, key, parse_dates=["created_at", "delivered_at"]) for key in keys),
        ignore_index=True,
    )
    return orders_df[orders_df["status"] == "delivered"].reset_index(drop=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        default=date.today(),
        help="Date whose delivered orders to settle, YYYY-MM-DD (default: today).",
    )
    parser.add_argument(
        "--run-timestamp",
        type=lambda s: datetime.strptime(s, RUN_TIMESTAMP_FORMAT),
        default=datetime.now(UTC).replace(tzinfo=None),
        help="Timestamp used in the output key, formatted "
        f"{RUN_TIMESTAMP_FORMAT!r} (default: current UTC time).",
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
        default="payments",
        help="S3 key prefix (under --bucket) to write the payments CSV into (default: payments).",
    )
    parser.add_argument(
        "--bucket",
        type=str,
        default=DEFAULT_BUCKET,
        help=f"MinIO/S3 bucket to read/write (default: {DEFAULT_BUCKET}).",
    )
    parser.add_argument("--seed", type=int, default=42, help="Random seed (default: 42).")
    parser.add_argument(
        "--wrong-amount-rate",
        type=float,
        default=WRONG_AMOUNT_RATE,
        help=f"Share of payment rows given a policy-mismatched commission_amount "
        f"(default: {WRONG_AMOUNT_RATE}).",
    )
    parser.add_argument(
        "--missing-payment-rate",
        type=float,
        default=MISSING_PAYMENT_RATE,
        help=f"Share of delivered orders with no payment row at all "
        f"(default: {MISSING_PAYMENT_RATE}).",
    )
    parser.add_argument(
        "--lag-mean-hours",
        type=float,
        default=LAG_MEAN_HOURS,
        help=f"Mean of the settlement-lag normal distribution, in hours "
        f"(default: {LAG_MEAN_HOURS}).",
    )
    parser.add_argument(
        "--lag-stdev-hours",
        type=float,
        default=LAG_STDEV_HOURS,
        help=f"Stdev of the settlement-lag normal distribution, in hours "
        f"(default: {LAG_STDEV_HOURS}).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rng = random.Random(args.seed)

    delivered = _load_delivered_orders(args.bucket, args.orders_dir, args.date)
    df = generate_payments(
        delivered,
        rng,
        wrong_amount_rate=args.wrong_amount_rate,
        missing_payment_rate=args.missing_payment_rate,
        lag_mean_hours=args.lag_mean_hours,
        lag_stdev_hours=args.lag_stdev_hours,
    )

    run_timestamp = args.run_timestamp.strftime(RUN_TIMESTAMP_FORMAT)
    key = f"{args.output_dir}/date={args.date.isoformat()}/payments_{run_timestamp}.csv"
    write_csv(df, args.bucket, key)

    missing = len(delivered) - len(df)
    print(
        f"Wrote {len(df)} payments to s3://{args.bucket}/{key} "
        f"({len(delivered)} delivered orders, {missing} with no payment row)"
    )


if __name__ == "__main__":
    main()
