"""Raw schema for the rider app events source.

See `docs/data-sources.md` #2 ("Rider app events — queue stream"). One JSON
object per line; a stand-in for what would be produced to a Kafka topic
until that infra is stood up.
"""

from __future__ import annotations

from pyspark.sql.types import (
    FloatType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


class RiderEventsSchema:
    """Raw schema constants for `rider_events/rider_events_<date>.jsonl`."""

    RAW = StructType(
        [
            StructField("event_id", StringType(), False),
            StructField("rider_id", StringType(), False),
            StructField("order_id", StringType(), True),
            StructField("event_type", StringType(), False),
            StructField("event_time", TimestampType(), False),
            StructField("latitude", FloatType(), True),
            StructField("longitude", FloatType(), True),
            StructField("status", StringType(), True),
        ]
    )
    """Schema of one JSON line in `rider_events/rider_events_<date>.jsonl`.

    `latitude`/`longitude` populate only for `location_ping` events;
    `status` populates only for `status_change` events.
    """

    EVENT_TYPES = ["location_ping", "status_change"]
    """Valid values of the `event_type` column."""

    STATUS_CHANGE_STATUSES = ["assigned", "picked_up", "delivered"]
    """Valid values of `status` when `event_type` is `status_change`."""
