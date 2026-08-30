"""Raw schema for the store fulfillment source.

See `docs/data-sources.md` #3 ("Store fulfillment exports — daily CSV per
store"). One object per store per date; every store uses the identical
schema, unit, and null convention.
"""

from __future__ import annotations

from pyspark.sql.types import (
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


class FulfillmentSchema:
    """Raw schema constants for `fulfillment/date=<date>/<store_id>.csv`."""

    RAW = StructType(
        [
            StructField("store_id", StringType(), False),
            StructField("sku", StringType(), False),
            StructField("date", DateType(), False),
            StructField("quantity_on_hand", IntegerType(), True),
            StructField("exported_at", TimestampType(), False),
        ]
    )
    """Schema of one row in `fulfillment/date=<date>/<store_id>.csv`.

    `quantity_on_hand` is nullable: blank means the SKU wasn't counted that
    day, not zero stock — a null convention used identically by every store.
    """
