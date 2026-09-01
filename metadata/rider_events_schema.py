"""Raw schema for the rider app events source.

See `docs/data-sources.md` #2 ("Rider app events"). Each row is one
event on the rider app's stream: either a lifecycle `status_change`
(mirroring an order's `orders_schema.OrdersSchema.STATUSES`) or a
`location_ping`. `latitude`/`longitude` are only populated for
`location_ping` events; `status` is only populated for `status_change`
events — see `generators/rider_events.py`. Coordinates use `DoubleType`
rather than `FloatType` (unlike `OrdersSchema.RAW.order_total`):
geo-coordinates need double precision to avoid rounding error
compounding across the location history of a single order/rider.
"""

from __future__ import annotations

from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


class RiderEventsSchema:
    """Raw schema constants for `rider_events/rider_events_<run_timestamp>.jsonl`."""

    RAW = StructType(
        [
            StructField("event_id", StringType(), False),
            StructField("rider_id", StringType(), False),
            StructField("order_id", StringType(), False),
            StructField("event_type", StringType(), False),
            StructField("event_time", TimestampType(), False),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("status", StringType(), True),
        ]
    )
    """Schema of one row in `rider_events/rider_events_<run_timestamp>.jsonl`."""

    EVENT_TYPES = ["status_change", "location_ping"]
    """Valid values of the `event_type` column."""
