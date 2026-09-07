"""Raw-extract-to-Bronze ingestion use case.

Generic across raw sources (orders, fulfillment, payments): nothing here
assumes a particular dataset name, schema, or file format — callers supply
those via `BronzeIngestionRequest`. The use case is always the same three
steps: read the raw file(s) with an explicit schema, tag ingestion
lineage, and write the result into Bronze idempotently.

`SplitBronzeIngestionRequest`/`ingest_clean_rows_to_bronze` cover the one
variant orders never needed: a source read in PERMISSIVE mode where some
rows fail to parse and must be split off before the Bronze write instead
of failing the whole load. They reuse `add_lineage_columns` and
`DeltaBronzeWriter` rather than duplicating the read/tag/write steps.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, input_file_name, lit, regexp_extract
from pyspark.sql.types import StringType, StructField, StructType

from infrastructure.delta_bronze_writer import DeltaBronzeWriter


@dataclass(frozen=True)
class BronzeIngestionRequest:
    """Inputs for one raw-extract-to-Bronze ingestion run.

    Attributes:
        raw_path: Location of the raw file(s) to read, e.g.
            `s3a://raw-incoming-data/orders/orders_2026-08-30.csv`. Also
            accepts a list of file locations — Spark's `.load()` reads a
            list of paths natively — for a caller that has resolved an
            explicit, bounded set of window-files instead of a glob.
        raw_format: Spark data source format for the raw file, e.g.
            `"csv"`.
        schema: Explicit schema to apply while reading. Never inferred —
            unexpected upstream schema drift should fail the read instead
            of being silently coerced.
        read_options: Extra `DataFrameReader` options, e.g.
            `{"header": "true", "mode": "FAILFAST"}`.
        bronze_path: Delta table location to write into, e.g.
            `s3a://bronze-veloz/orders/`.
        partition_column: Name of the lineage column the Bronze table is
            partitioned and idempotently overwritten on, e.g.
            `_extract_date`.
        extract_date: Value of `partition_column` for this load, in
            `YYYY-MM-DD` format.
        window_column: Name of a finer-grained sub-partition column
            derived from the ingested window-file's own name, e.g.
            `_ingestion_window`. `None` (default) leaves the Bronze table
            partitioned by `partition_column` alone.
        window_values: Values of `window_column` covered by this load's
            `raw_path`, used to scope the Bronze `replaceWhere` predicate
            to only those windows. Must be set together with
            `window_column`, or left `None`.
    """

    raw_path: str | list[str]
    raw_format: str
    schema: StructType
    read_options: dict[str, str]
    bronze_path: str
    partition_column: str
    extract_date: str
    window_column: str | None = None
    window_values: list[str] | None = None


def add_lineage_columns(
    df: DataFrame,
    extract_date: str,
    partition_column: str,
    derive_partition_column: bool = True,
    derive_source_file: bool = True,
    window_column: str | None = None,
) -> DataFrame:
    """Tags a raw DataFrame with ingestion lineage columns.

    Args:
        df: Raw DataFrame read with an explicit schema.
        extract_date: Extract date for this load, `YYYY-MM-DD` format.
        partition_column: Name to give the extract-date lineage column.
        derive_partition_column: If `True` (default), `partition_column`
            is (re)computed from `extract_date`, overwriting any existing
            values — correct for a source with no date field of its own,
            like orders. If `False`, `partition_column` is left untouched:
            the source already carries an unambiguous per-row date (e.g.
            fulfillment's native `date` column) and `extract_date` is only
            needed by the caller to build the Bronze `replaceWhere`
            predicate, not to overwrite real data.
        derive_source_file: If `True` (default), `_source_file` is
            computed here via `input_file_name()`. If `False`, `df` must
            already carry a correct `_source_file` column computed before
            any `.cache()`/filter step — `input_file_name()` stops
            resolving correctly once a DataFrame has been materialized
            through a cache, so a caller that needs to cache `df` first
            (see `ingest_clean_rows_to_bronze`) must tag it beforehand.
        window_column: If given, an ingestion-window sub-partition column
            is derived from `_source_file`'s embedded run timestamp (the
            `%Y%m%dT%H%M%SZ`-formatted token `generators/s3_io.py`'s
            window-scoped generators put in each object's name, e.g.
            `orders_20260905T191000Z.csv`) and added under this name.
            `None` (default) skips this column entirely.

    Returns:
        `df` with `partition_column` (as a `date`, when derived),
        `_ingested_at`, `_source_file` (when derived), and
        `window_column` (when given) columns added.
    """
    tagged = df
    if derive_partition_column:
        tagged = tagged.withColumn(partition_column, lit(extract_date).cast("date"))
    tagged = tagged.withColumn("_ingested_at", current_timestamp())
    if derive_source_file:
        tagged = tagged.withColumn("_source_file", input_file_name())
    if window_column is not None:
        tagged = tagged.withColumn(
            window_column,
            regexp_extract(col("_source_file"), r"_(\d{8}T\d{6}Z)\.", 1),
        )
    return tagged


def _read_raw(
    spark: SparkSession,
    raw_format: str,
    schema: StructType,
    read_options: dict[str, str],
    raw_path: str | list[str],
) -> DataFrame:
    """Reads a raw file(s) with an explicit schema and reader options.

    Args:
        spark: Active SparkSession to read with.
        raw_format: Spark data source format, e.g. `"csv"`.
        schema: Explicit schema to apply while reading.
        read_options: Extra `DataFrameReader` options.
        raw_path: Location of the raw file(s) to read. A list of
            locations is read natively by Spark's `.load()`, no different
            handling needed here.

    Returns:
        The raw file(s) as a DataFrame, with no lineage tagging applied.
    """
    reader = spark.read.format(raw_format).schema(schema)
    for option_name, option_value in read_options.items():
        reader = reader.option(option_name, option_value)
    return reader.load(raw_path)


def ingest_to_bronze(spark: SparkSession, request: BronzeIngestionRequest) -> int:
    """Reads a raw extract, tags lineage, and writes it into Bronze.

    Args:
        spark: Active SparkSession to read and write with.
        request: Source location, schema, and Bronze destination for this
            load.

    Returns:
        Number of rows written to the Bronze table.
    """
    raw_df = _read_raw(
        spark, request.raw_format, request.schema, request.read_options, request.raw_path
    )

    bronze_df = add_lineage_columns(
        raw_df,
        request.extract_date,
        request.partition_column,
        window_column=request.window_column,
    )
    row_count = bronze_df.count()

    writer = DeltaBronzeWriter(
        path=request.bronze_path,
        partition_columns=[request.partition_column]
        + ([request.window_column] if request.window_column else []),
    )
    writer.write(
        bronze_df,
        partition_column=request.partition_column,
        partition_value=request.extract_date,
        window_column=request.window_column,
        window_values=request.window_values,
    )

    return row_count


def with_corrupt_record_column(schema: StructType, column_name: str) -> StructType:
    """Returns a copy of `schema` with a nullable corrupt-record column appended.

    Spark's `PERMISSIVE` read mode requires a nullable string column,
    named via `columnNameOfCorruptRecord`, to capture each unparseable
    row's raw content. Building the extended schema here rather than
    mutating a shared `metadata.*Schema.RAW` constant keeps that constant
    safe to reuse elsewhere for readers that never opt into PERMISSIVE
    mode and don't expect this extra column.

    Args:
        schema: Base schema to extend, e.g. `FulfillmentSchema.RAW`.
        column_name: Name for the corrupt-record column, e.g.
            `"_corrupt_record"`.

    Returns:
        A new `StructType`: `schema`'s fields followed by a nullable
        `StringType` field named `column_name`.
    """
    return StructType(list(schema.fields) + [StructField(column_name, StringType(), True)])


def split_clean_and_corrupt_rows(
    df: DataFrame, corrupt_record_column: str
) -> tuple[DataFrame, DataFrame]:
    """Splits a PERMISSIVE-mode read into well-formed and corrupt rows.

    Args:
        df: DataFrame read with a schema extended via
            `with_corrupt_record_column`.
        corrupt_record_column: Name of the corrupt-record column: null
            for rows that parsed cleanly, populated with the raw row
            content for rows that didn't.

    Returns:
        A `(clean_df, corrupt_df)` tuple. `clean_df` has
        `corrupt_record_column` dropped, since it's always null there;
        `corrupt_df` keeps it, holding each rejected row's raw content.
    """
    clean_df = df.filter(col(corrupt_record_column).isNull()).drop(corrupt_record_column)
    corrupt_df = df.filter(col(corrupt_record_column).isNotNull())
    return clean_df, corrupt_df


@dataclass(frozen=True)
class SplitBronzeIngestionRequest:
    """Inputs for a Bronze load that must separate corrupt rows before writing.

    Covers the same read/write essentials as `BronzeIngestionRequest`,
    plus what a PERMISSIVE-mode source needs on top: a corrupt-record
    column to split on, and the option to keep a raw source's own
    partition-column values instead of deriving them from `extract_date`.

    Attributes:
        raw_path: Location of the raw file(s) to read, e.g.
            `s3a://raw-incoming-data/fulfillment/date=2026-08-30/*.csv`.
        raw_format: Spark data source format for the raw file, e.g.
            `"csv"`.
        schema: Explicit schema to apply while reading, extended with
            `corrupt_record_column` via `with_corrupt_record_column`.
        read_options: Extra `DataFrameReader` options, e.g.
            `{"header": "true", "mode": "PERMISSIVE"}`.
        bronze_path: Delta table location to write clean rows into.
        partition_column: Name of the column the Bronze table is
            partitioned and idempotently overwritten on.
        extract_date: Value used to build the Bronze `replaceWhere`
            predicate for this load, `YYYY-MM-DD` format.
        corrupt_record_column: Name of the schema's corrupt-record
            column, matching the `columnNameOfCorruptRecord` read option.
        derive_partition_column: Passed through to `add_lineage_columns`.
            Defaults to `False`: sources needing PERMISSIVE-mode splitting
            typically already carry their own per-row partition date (see
            `add_lineage_columns`'s docstring), unlike orders.
    """

    raw_path: str
    raw_format: str
    schema: StructType
    read_options: dict[str, str]
    bronze_path: str
    partition_column: str
    extract_date: str
    corrupt_record_column: str
    derive_partition_column: bool = False


def ingest_clean_rows_to_bronze(
    spark: SparkSession, request: SplitBronzeIngestionRequest
) -> tuple[int, DataFrame]:
    """Reads a raw extract in PERMISSIVE mode and writes only its clean rows to Bronze.

    Args:
        spark: Active SparkSession to read and write with.
        request: Source location, schema, and Bronze destination for this
            load.

    Returns:
        A `(clean_row_count, corrupt_df)` tuple: rows written to Bronze,
        and the rejected rows (tagged with a `_source_file` lineage
        column) for the caller to quarantine.
    """
    raw_df = _read_raw(
        spark, request.raw_format, request.schema, request.read_options, request.raw_path
    )
    # input_file_name() must be captured before caching -- it stops resolving
    # correctly once rows are served from a cached/materialized plan instead
    # of the live file scan.
    raw_df = raw_df.withColumn("_source_file", input_file_name())
    # Spark's PERMISSIVE-mode corrupt-record column gives wrong counts if a
    # query touches only that column (its detection is tied to parsing every
    # other column, which the optimizer can otherwise prune away) -- caching
    # right after the read forces one correct materialization that both the
    # clean and corrupt branches below then reuse.
    raw_df = raw_df.cache()
    raw_df.count()
    clean_df, corrupt_df = split_clean_and_corrupt_rows(raw_df, request.corrupt_record_column)

    bronze_df = add_lineage_columns(
        clean_df,
        request.extract_date,
        request.partition_column,
        derive_partition_column=request.derive_partition_column,
        derive_source_file=False,
    )
    row_count = bronze_df.count()

    writer = DeltaBronzeWriter(
        path=request.bronze_path,
        partition_columns=[request.partition_column],
    )
    writer.write(
        bronze_df,
        partition_column=request.partition_column,
        partition_value=request.extract_date,
    )

    # corrupt_df already carries _source_file: it was tagged on raw_df above,
    # before caching, and survives the filter in split_clean_and_corrupt_rows.
    return row_count, corrupt_df
