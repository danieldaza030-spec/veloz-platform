"""Bronze-to-Silver ingestion use case for orders.

Wires the three Silver steps together: read a bounded slice of Bronze,
collapse it to Silver's one-row-per-`order_id` grain (`application.
orders_silver_dedup`), and MERGE the result into the Silver Delta table
(`infrastructure.delta_silver_merge_writer`). Mirrors `application.
bronze_ingestion.BronzeIngestionRequest`'s shape: the caller supplies a
request object naming the source/destination and the bounded filter for
this run, and this module does the read/transform/write.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from application.orders_silver_dedup import collapse_bronze_batch
from infrastructure.delta_silver_merge_writer import DeltaSilverMergeWriter
from metadata.orders_silver_schema import OrdersSilverSchema

EXTRACT_DATE_COLUMN = "_extract_date"
INGESTION_WINDOW_COLUMN = "_ingestion_window"


@dataclass(frozen=True)
class OrdersSilverIngestionRequest:
    """Inputs for one Bronze-to-Silver ingestion run.

    Exactly two filter modes are supported, matching how `dags.
    ingest_orders_bronze` reports what it wrote (`ORDERS_BRONZE_ASSET`'s
    outlet event `extra`): a bounded set of `_ingestion_window` tokens
    (the narrow, common case -- a handful of 5-minute windows), or a
    bounded set of `_extract_date` values (a manually triggered/backfill
    run with no window information). Exactly one of the two must be set;
    there is no third mode and no "read everything" default.

    Attributes:
        bronze_path: Bronze Delta table location to read from, e.g.
            `s3a://bronze-veloz/orders/`.
        silver_path: Silver Delta table location to MERGE into, e.g.
            `s3a://silver-veloz/orders/`.
        ingestion_windows: `_ingestion_window` values to read, e.g.
            `["20260905T191000Z", "20260905T191500Z"]`. Mutually
            exclusive with `extract_dates`.
        extract_dates: `_extract_date` values to read, `YYYY-MM-DD`
            format, e.g. `["2026-08-30"]`. Mutually exclusive with
            `ingestion_windows`.

    Raises:
        ValueError: If both or neither of `ingestion_windows`/
            `extract_dates` are set.
    """

    bronze_path: str
    silver_path: str
    ingestion_windows: list[str] | None = None
    extract_dates: list[str] | None = None

    def __post_init__(self) -> None:
        if (self.ingestion_windows is None) == (self.extract_dates is None):
            raise ValueError(
                "exactly one of ingestion_windows or extract_dates must be set"
            )


def _read_bronze_batch(spark: SparkSession, request: OrdersSilverIngestionRequest) -> DataFrame:
    """Reads Bronze, filtered to `request`'s bounded window/date set.

    Args:
        spark: Active SparkSession to read with.
        request: Names the Bronze table and the bounded filter to apply.

    Returns:
        Bronze rows matching the requested `_ingestion_window`s or
        `_extract_date`s -- may still contain more than one row per
        `order_id`; collapsing that is `application.orders_silver_dedup`'s
        job, not this function's.
    """
    bronze_df = spark.read.format("delta").load(request.bronze_path)
    if request.ingestion_windows is not None:
        return bronze_df.filter(F.col(INGESTION_WINDOW_COLUMN).isin(request.ingestion_windows))
    return bronze_df.filter(F.col(EXTRACT_DATE_COLUMN).isin(request.extract_dates))


def ingest_orders_to_silver(spark: SparkSession, request: OrdersSilverIngestionRequest) -> int:
    """Reads a bounded Bronze slice, collapses it, and MERGEs it into Silver.

    Idempotent by construction: re-running the same `request` re-reads the
    same Bronze rows, re-collapses them to the same one-row-per-`order_id`
    batch, and re-runs the same MERGE. Every `whenMatchedUpdate` expression
    in `infrastructure.delta_silver_merge_writer` resolves to Silver's
    current value when source and target already agree (`newer` is
    `False` on an exact `updated_at` repeat, so latest-wins columns keep
    target's already-identical value and sticky columns coalesce that
    value with itself) -- a repeat run changes no already-current row.

    Args:
        spark: Active SparkSession to read and write with.
        request: Bronze source, Silver destination, and the bounded
            window/date filter for this run.

    Returns:
        Number of rows in the collapsed batch that were MERGEd -- one per
        distinct `order_id` in the filtered Bronze read. Not a
        rows-inserted/rows-updated split: Delta's MERGE doesn't surface
        that split without a separate metrics read this use case doesn't
        need.
    """
    bronze_batch = _read_bronze_batch(spark, request)
    collapsed = collapse_bronze_batch(bronze_batch)
    row_count = collapsed.count()

    writer = DeltaSilverMergeWriter(path=request.silver_path, schema=OrdersSilverSchema.TARGET)
    writer.write(collapsed)

    return row_count
