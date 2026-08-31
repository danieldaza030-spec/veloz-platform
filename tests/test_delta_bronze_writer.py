"""Tests for infrastructure/delta_bronze_writer.py."""

from __future__ import annotations

import pytest

from infrastructure.delta_bronze_writer import DeltaBronzeWriter


class TestDeltaBronzeWriterValidation:
    """Test input validation."""

    def test_partition_value_with_single_quote_raises_error(self) -> None:
        """Verify ValueError is raised when partition_value contains single quote."""
        writer = DeltaBronzeWriter(
            path="/tmp/test",
            partition_columns=["_extract_date"],
        )

        # Mock DataFrame - we don't need a real one to test validation
        class MockDF:
            pass

        with pytest.raises(
            ValueError,
            match=r"partition_value must not contain a single quote",
        ):
            writer.write(
                MockDF(),  # type: ignore
                partition_column="_extract_date",
                partition_value="2026-08-30'; DROP TABLE --",
            )
