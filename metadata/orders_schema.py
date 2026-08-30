"""Raw schema for the orders source.

See `docs/data-sources.md` #1 ("Orders — Postgres periodic extract"). Each
row is one order's state as of extract time, not a changelog entry.
"""

from __future__ import annotations

from pyspark.sql.types import (
    FloatType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


class OrdersSchema:
    """Raw schema constants for `orders/orders_<date>.csv`."""

    RAW = StructType(
        [
            StructField("order_id", StringType(), False),
            StructField("store_id", StringType(), False),
            StructField("rider_id", StringType(), True),
            StructField("status", StringType(), False),
            StructField("created_at", TimestampType(), False),
            StructField("assigned_at", TimestampType(), True),
            StructField("picked_up_at", TimestampType(), True),
            StructField("delivered_at", TimestampType(), True),
            StructField("order_total", FloatType(), False),
            StructField("updated_at", TimestampType(), False),
        ]
    )
    """Schema of one row in `orders/orders_<date>.csv`."""

    STATUSES = ["created", "assigned", "picked_up", "delivered", "cancelled"]
    """Valid values of the `status` column, in lifecycle order (cancellable from any stage before delivered)."""
