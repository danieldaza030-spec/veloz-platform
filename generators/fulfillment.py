"""Generates Veloz's daily store fulfillment feed: one CSV per store per day.

Every store emits the same schema and the same unit of measure, per
docs/data-sources.md's spec — there is no per-store schema drift in this
source. What *is* in scope, per CLAUDE.md's G3, is the feed's baseline
unreliability: with --bad-night, some stores' files never arrive for the day
and some arrive with malformed rows a downstream pipeline must quarantine
rather than crash on.

Usage:
    python generators/fulfillment.py --date 2026-08-27
    python generators/fulfillment.py --date 2026-08-27 --bad-night
"""

from __future__ import annotations

import argparse
import csv
import io
import random
from datetime import date, datetime, timedelta

from reference_data import SKU_IDS, STORE_IDS
from s3_io import DEFAULT_BUCKET, write_text

COLUMNS = ["store_id", "sku", "date", "quantity_on_hand", "exported_at"]

# A store not counting a SKU on a given day is a normal, everyday gap in this
# feed — distinct from the --bad-night failure modes below, and applied the
# same way for every store so there's exactly one null semantics convention.
BASELINE_MISSING_QUANTITY_RATE = 0.02
QUANTITY_RANGE = (0, 500)

# --bad-night severity: fixed fractions of the 30 stores, sampled per run
# from --seed so a given seed reproduces the same bad night.
MISSING_FILE_STORE_SHARE = 0.10
MALFORMED_FILE_STORE_SHARE = 0.15
MALFORMED_ROW_SHARE = 0.12


def _export_timestamp(target_date: date, rng: random.Random) -> datetime:
    """Daily exports land some time in the evening, close to end-of-day."""
    seconds_into_evening = rng.randint(20 * 3600, 24 * 3600 - 1)
    return datetime.combine(target_date, datetime.min.time()) + timedelta(seconds=seconds_into_evening)


def _clean_rows(
    store_id: str,
    target_date: date,
    rng: random.Random,
    missing_quantity_rate: float = BASELINE_MISSING_QUANTITY_RATE,
) -> list[list[str]]:
    """Builds one well-formed row per SKU for a single store's daily file."""
    exported_at = _export_timestamp(target_date, rng)
    rows = []
    for sku in SKU_IDS:
        if rng.random() < missing_quantity_rate:
            quantity = ""  # not counted today — the one null convention this feed uses
        else:
            quantity = str(rng.randint(*QUANTITY_RANGE))
        rows.append([store_id, sku, target_date.isoformat(), quantity, exported_at.isoformat()])
    return rows


def _corrupt_row(row: list[str], rng: random.Random) -> list[str]:
    """Applies one mechanical corruption to a row, simulating a bad export.

    These are physical file-integrity problems (a write cut short, a stray
    delimiter) — not per-store schema variation, which stays out of scope.
    """
    corruption = rng.choice(["truncated", "extra_field"])
    if corruption == "truncated":
        # Row cut off mid-write: trailing field(s) never made it to disk.
        cut = rng.randint(1, 2)
        return row[:-cut]
    # An unescaped delimiter inside a field split it into an extra column.
    return row + ["CORRUPT"]


def _apply_bad_night(
    rows: list[list[str]], rng: random.Random, malformed_row_share: float = MALFORMED_ROW_SHARE
) -> list[list[str]]:
    corrupted = []
    for row in rows:
        if rng.random() < malformed_row_share:
            corrupted.append(_corrupt_row(row, rng))
        else:
            corrupted.append(row)
    return corrupted


def generate_fulfillment_day(
    target_date: date,
    bucket: str,
    key_prefix: str,
    rng: random.Random,
    bad_night: bool,
    missing_file_store_share: float = MISSING_FILE_STORE_SHARE,
    malformed_file_store_share: float = MALFORMED_FILE_STORE_SHARE,
    malformed_row_share: float = MALFORMED_ROW_SHARE,
    missing_quantity_rate: float = BASELINE_MISSING_QUANTITY_RATE,
) -> dict[str, str]:
    """Writes one CSV object per store for target_date. Returns a store_id -> outcome map.

    The four *_share/_rate args tune --bad-night severity in degree, not just
    on/off; they're no-ops when bad_night is False except missing_quantity_rate,
    which applies every night (it's the baseline-gap convention, not a
    --bad-night failure mode).
    """
    partition_key_prefix = f"{key_prefix}/date={target_date.isoformat()}"

    missing_stores: set[str] = set()
    malformed_stores: set[str] = set()
    if bad_night:
        missing_stores = set(rng.sample(STORE_IDS, k=round(len(STORE_IDS) * missing_file_store_share)))
        remaining = [s for s in STORE_IDS if s not in missing_stores]
        malformed_stores = set(
            rng.sample(remaining, k=round(len(STORE_IDS) * malformed_file_store_share))
        )

    outcomes: dict[str, str] = {}
    for store_id in STORE_IDS:
        if store_id in missing_stores:
            outcomes[store_id] = "missing"
            continue

        rows = _clean_rows(store_id, target_date, rng, missing_quantity_rate=missing_quantity_rate)
        if store_id in malformed_stores:
            rows = _apply_bad_night(rows, rng, malformed_row_share=malformed_row_share)
            outcomes[store_id] = "malformed"
        else:
            outcomes[store_id] = "clean"

        buffer = io.StringIO(newline="")
        writer = csv.writer(buffer)
        writer.writerow(COLUMNS)
        writer.writerows(rows)
        write_text(bucket, f"{partition_key_prefix}/{store_id}.csv", buffer.getvalue())

    return outcomes


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        default=date.today(),
        help="Export date, YYYY-MM-DD (default: today).",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="fulfillment",
        help="S3 key prefix (under --bucket) to partition output into (default: fulfillment).",
    )
    parser.add_argument(
        "--bucket",
        type=str,
        default=DEFAULT_BUCKET,
        help=f"MinIO/S3 bucket to write into (default: {DEFAULT_BUCKET}).",
    )
    parser.add_argument("--seed", type=int, default=42, help="Random seed (default: 42).")
    parser.add_argument(
        "--bad-night",
        action="store_true",
        help="Simulate the feed's baseline unreliability: some stores' files "
        "missing entirely, some arriving with malformed rows.",
    )
    parser.add_argument(
        "--baseline-missing-quantity-rate",
        type=float,
        default=BASELINE_MISSING_QUANTITY_RATE,
        help=f"Share of (store, sku) rows with no quantity counted that day, "
        f"applies every night regardless of --bad-night (default: {BASELINE_MISSING_QUANTITY_RATE}).",
    )
    parser.add_argument(
        "--missing-file-store-share",
        type=float,
        default=MISSING_FILE_STORE_SHARE,
        help=f"--bad-night only: share of stores whose file never arrives "
        f"(default: {MISSING_FILE_STORE_SHARE}).",
    )
    parser.add_argument(
        "--malformed-file-store-share",
        type=float,
        default=MALFORMED_FILE_STORE_SHARE,
        help=f"--bad-night only: share of (non-missing) stores whose file arrives "
        f"with malformed rows (default: {MALFORMED_FILE_STORE_SHARE}).",
    )
    parser.add_argument(
        "--malformed-row-share",
        type=float,
        default=MALFORMED_ROW_SHARE,
        help=f"--bad-night only: share of rows corrupted within a malformed "
        f"store's file (default: {MALFORMED_ROW_SHARE}).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rng = random.Random(args.seed)

    outcomes = generate_fulfillment_day(
        args.date,
        args.bucket,
        args.output_dir,
        rng,
        args.bad_night,
        missing_file_store_share=args.missing_file_store_share,
        malformed_file_store_share=args.malformed_file_store_share,
        malformed_row_share=args.malformed_row_share,
        missing_quantity_rate=args.baseline_missing_quantity_rate,
    )

    counts = {"clean": 0, "malformed": 0, "missing": 0}
    for outcome in outcomes.values():
        counts[outcome] += 1
    print(
        f"Fulfillment feed for {args.date.isoformat()}: "
        f"{counts['clean']} clean, {counts['malformed']} malformed, "
        f"{counts['missing']} missing (of {len(outcomes)} stores)"
    )


if __name__ == "__main__":
    main()
