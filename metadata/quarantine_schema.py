"""Schema for Bronze-layer quarantine records.

See `docs/bronze-conventions.md` for the storage location convention and
the meaning of each column.
"""

from __future__ import annotations

from pyspark.sql.types import StringType, StructField, StructType, TimestampType


class QuarantineSchema:
    """Schema constants for quarantine records.

    Written to the path returned by
    `infrastructure.quarantine_writer.build_quarantine_path`; see
    `docs/bronze-conventions.md` for the storage location convention.
    """

    RECORD = StructType(
        [
            StructField("source", StringType(), False),
            StructField("partition_date", StringType(), False),
            StructField("key", StringType(), False),
            StructField("reason", StringType(), False),
            StructField("detected_at", TimestampType(), False),
            StructField("raw_snippet", StringType(), True),
        ]
    )
    """Schema of one quarantine record.

    `raw_snippet` is nullable: null when there is no raw content to show,
    e.g. a `missing_file` record where nothing was read.
    """
