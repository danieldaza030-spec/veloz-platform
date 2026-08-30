"""Raw schema for the payments/commissions ledger source.

See `docs/data-sources.md` #4 ("Payments / commissions ledger — daily
file"). One row per delivered order from that date's orders extract, minus
the deliberately-missing rows documented there.
"""

from __future__ import annotations

from pyspark.sql.types import (
    FloatType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


class PaymentsSchema:
    """Raw schema constants for `payments/payments_<date>.csv`."""

    RAW = StructType(
        [
            StructField("payment_id", StringType(), False),
            StructField("order_id", StringType(), False),
            StructField("rider_id", StringType(), False),
            StructField("commission_amount", FloatType(), False),
            StructField("payment_recorded_at", TimestampType(), False),
        ]
    )
    """Schema of one row in `payments/payments_<date>.csv` — one row per
    delivered order (minus the ~1% deliberately-missing rows)."""
