"""Idempotent Delta Lake writer for the Bronze layer.

Pure Spark/Delta I/O: no Airflow imports, no dataset-specific business
rules. Wraps the `replaceWhere` overwrite pattern that lets a Bronze load
for a given partition be re-run safely (retry, backfill, manual
re-trigger) instead of double-counting rows the way a plain `append`
would.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame


def _validate_no_single_quote(value: str, field_name: str) -> None:
    """Raises if `value` contains a single-quote character.

    A `replaceWhere` predicate is built by string interpolation, so a
    single quote in a value would break out of the quoted literal and
    produce a malformed (or injectable) predicate.

    Args:
        value: Value about to be interpolated into a `replaceWhere`
            predicate.
        field_name: Name of the field `value` came from, used in the
            error message so a caller can tell which value was rejected.

    Raises:
        ValueError: If `value` contains a single-quote character.
    """
    if "'" in value:
        raise ValueError(f"{field_name} must not contain a single quote: {value!r}")


@dataclass(frozen=True)
class DeltaBronzeWriter:
    """Writes a DataFrame into a Bronze Delta table idempotently.

    Attributes:
        path: Delta table location, e.g. `s3a://bronze-veloz/orders/`.
        partition_columns: Columns to partition the table by, in order.
            Empty for an unpartitioned table.
    """

    path: str
    partition_columns: list[str]

    def write(
        self,
        df: DataFrame,
        partition_column: str,
        partition_value: str,
        window_column: str | None = None,
        window_values: list[str] | None = None,
    ) -> None:
        """Overwrites one partition (optionally, one set of windows within it) of the Bronze Delta table.

        Only rows matching `partition_column = partition_value` (and, when
        `window_column`/`window_values` are given, also
        `window_column IN (...)`) are replaced; every other existing
        partition/window is left untouched. This makes re-running the same
        load for the same partition value (and, for a sub-partitioned
        table, the same bounded set of windows) safe by construction,
        rather than relying on the caller to deduplicate.

        Args:
            df: Rows to write. Must include `partition_column` (and
                `window_column`, when given).
            partition_column: Column identifying the partition being
                replaced, e.g. `_extract_date`.
            partition_value: Value of `partition_column` for this load,
                e.g. `2026-08-30`. Quoted into the `replaceWhere` predicate.
            window_column: Name of a finer-grained sub-partition column
                to additionally scope the overwrite to, e.g.
                `_ingestion_window`. May be set alone, with
                `window_values` left `None`: the table is still
                physically partitioned by this column (via
                `partition_columns`), but the `replaceWhere` overwrite
                scope stays the full `partition_column` partition — a
                full-day overwrite, for a caller (e.g. a manual/full-day
                ingestion run) with no bounded window set of its own to
                scope to.
            window_values: Values of `window_column` to include in the
                overwrite scope. Requires `window_column` to also be
                set; raises if given without it. Leave both `None` for
                the plain single-partition overwrite behavior.

        Raises:
            ValueError: If `partition_value` or any `window_values`
                element contains a single-quote character, which would
                break out of the quoted literal and produce a malformed
                `replaceWhere` predicate. Also raised if `window_values`
                is set without `window_column`.
        """
        if window_values is not None and window_column is None:
            raise ValueError("window_values requires window_column to also be set")

        _validate_no_single_quote(partition_value, "partition_value")
        replace_where = f"{partition_column} = '{partition_value}'"

        if window_column is not None and window_values is not None:
            for window_value in window_values:
                _validate_no_single_quote(window_value, "window_values")
            quoted_values = ", ".join(f"'{value}'" for value in window_values)
            replace_where = f"{replace_where} AND {window_column} IN ({quoted_values})"

        writer = (
            df.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", replace_where)
        )
        if self.partition_columns:
            writer = writer.partitionBy(*self.partition_columns)
        writer.save(self.path)
