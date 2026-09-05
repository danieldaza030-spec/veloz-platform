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
    ) -> None:
        """Overwrites one partition of the Bronze Delta table.

        Only rows matching `partition_column = partition_value` are
        replaced; every other existing partition is left untouched. This
        makes re-running the same load for the same partition value safe
        by construction, rather than relying on the caller to deduplicate.

        Args:
            df: Rows to write. Must include `partition_column`.
            partition_column: Column identifying the partition being
                replaced, e.g. `_extract_date`.
            partition_value: Value of `partition_column` for this load,
                e.g. `2026-08-30`. Quoted into the `replaceWhere` predicate.

        Raises:
            ValueError: If `partition_value` contains a single-quote
                 character, which would break out of the quoted literal
                and produce a malformed `replaceWhere` predicate.
        """
        if "'" in partition_value:
            raise ValueError(
                f"partition_value must not contain a single quote: "
                f"{partition_value!r}"
            )
        replace_where = f"{partition_column} = '{partition_value}'"
        writer = (
            df.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", replace_where)
        )
        if self.partition_columns:
            writer = writer.partitionBy(*self.partition_columns)
        writer.save(self.path)
