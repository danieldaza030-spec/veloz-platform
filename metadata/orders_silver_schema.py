"""Target schema and column-classification constants for the Silver orders table.

Silver's grain is one row per `order_id` — a current-state accumulating
snapshot, unlike Bronze's append-only per-extract audit trail (see
`dags/ingest_orders_bronze.py`'s docstring). Every order lifecycle field
falls into exactly one of two behaviors when a later batch touches a row
already present in Silver:

- `STICKY_COLUMNS`: once set, a value should never be erased by a later
  batch that happens to carry a null there (e.g. a late-arriving,
  older-updated_at batch filling in `picked_up_at` after a newer batch
  already moved the order to `delivered`). `application.
  orders_silver_dedup` and `infrastructure.delta_silver_merge_writer`
  both key their coalescing behavior off this same tuple so the
  intra-batch collapse and the cross-batch MERGE agree on which columns
  behave this way.
- `LATEST_WINS_COLUMNS`: always reflects whichever batch has the highest
  `updated_at` — no fallback to an older value when the newer batch's
  value is non-null.

No pyspark I/O in this module — schema/constant definitions only.
"""

from __future__ import annotations

from pyspark.sql.types import (
    DateType,
    FloatType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

STATUS_RANK: dict[str, int] = {
    "created": 1,
    "assigned": 2,
    "picked_up": 3,
    "delivered": 4,
    "cancelled": 5,
}
"""Lifecycle order of `orders.status`, used to break ties when two rows in
the same intra-batch collapse share the same `updated_at`.

`cancelled` is TERMINAL: it ranks highest so that, on an `updated_at` tie,
a cancellation always sorts after every other status an order could be in
at that instant, matching `docs/data-sources.md` #1's "cancellable from
any stage before delivered" rule — a cancelled order never reverts.
"""

TERMINAL_STATUS = "cancelled"
"""The one status with no further transition. Drives `cancelled_at`'s
derivation in `application.orders_silver_dedup`: the timestamp of the
first row (by the collapse ordering) that reaches this status."""

STICKY_COLUMNS: tuple[str, ...] = (
    "created_at",
    "assigned_at",
    "picked_up_at",
    "delivered_at",
    "cancelled_at",
)
"""Silver columns that only ever fill in or stay put, never regress to
null. `cancelled_at` has no bronze-source counterpart — it is derived in
`application.orders_silver_dedup` — but is listed here because it needs
the exact same coalescing treatment on write."""

LATEST_WINS_COLUMNS: tuple[str, ...] = (
    "status",
    "rider_id",
    "store_id",
    "order_total",
    "updated_at",
)
"""Silver columns that always reflect the most recently updated batch.

`updated_at` is a member of this set conceptually (it *is* the freshness
signal), but `infrastructure.delta_silver_merge_writer` special-cases it
with a `greatest(source, target)` expression instead of the generic
latest-wins `CASE`, so a merge is never allowed to move it backwards."""

BRONZE_LINEAGE_COLUMNS: tuple[str, ...] = (
    "_extract_date",
    "_ingestion_window",
    "_bronze_ingested_at",
)
"""Bronze lineage columns worth keeping on the Silver row for audit --
"which Bronze extract/window last touched this order" -- without carrying
every Bronze column forward. `_source_file` is deliberately dropped: it's
one raw CSV path per window-file, already implied by `_ingestion_window`,
and not worth a column on a 5M-orders/day accumulating table.

Treated like `LATEST_WINS_COLUMNS` by `infrastructure.
delta_silver_merge_writer`'s MERGE: they should describe the most recent
update, not the first one. Kept as a separate tuple, passed alongside
`LATEST_WINS_COLUMNS` rather than folded into it, since that tuple is
the shared source of truth for business columns only -- lineage columns
aren't part of the order's business state.
"""

PARTITION_COLUMN = "created_date"
"""Silver's only physical partition column. Derived from `created_at`,
which never changes for a given `order_id`, so a row can never migrate
partitions -- unlike `store_id`/`_extract_date`/`_ingestion_window`,
which do change or repeat across an order's lifecycle and would make
rows migrate across partitions if used instead (see
`infrastructure.delta_silver_merge_writer`'s module docstring)."""

SILVER_INGESTED_AT_COLUMN = "_silver_ingested_at"
"""Refreshed on every write (insert or update) -- "when was this row's
data last touched by a Silver run"."""

SILVER_FIRST_SEEN_AT_COLUMN = "_silver_first_seen_at"
"""Set once, on first insert, and never updated again -- "when did this
order first land in Silver"."""


class OrdersSilverSchema:
    """Target schema constants for `s3a://silver-veloz/orders/`."""

    TARGET = StructType(
        [
            StructField("order_id", StringType(), False),
            StructField("store_id", StringType(), False),
            StructField("rider_id", StringType(), True),
            StructField("status", StringType(), False),
            StructField("created_at", TimestampType(), False),
            StructField("assigned_at", TimestampType(), True),
            StructField("picked_up_at", TimestampType(), True),
            StructField("delivered_at", TimestampType(), True),
            StructField("cancelled_at", TimestampType(), True),
            StructField("order_total", FloatType(), False),
            StructField("updated_at", TimestampType(), False),
            StructField("created_date", DateType(), False),
            StructField("_extract_date", DateType(), True),
            StructField("_ingestion_window", StringType(), True),
            StructField("_bronze_ingested_at", TimestampType(), True),
            StructField("_silver_ingested_at", TimestampType(), False),
            StructField("_silver_first_seen_at", TimestampType(), False),
        ]
    )
    """Schema of one row in `s3a://silver-veloz/orders/`: one row per
    `order_id` (current-state accumulating snapshot), source columns from
    `metadata.orders_schema.OrdersSchema.RAW` plus the derived
    `cancelled_at`, the `created_date` partition column, and audit
    columns. Bronze lineage columns are nullable here since they're
    always populated by the write path, never by a raw source read.
    """
