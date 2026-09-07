"""Tests for metadata/orders_silver_schema.py."""

from __future__ import annotations

from metadata.orders_silver_schema import (
    BRONZE_LINEAGE_COLUMNS,
    LATEST_WINS_COLUMNS,
    PARTITION_COLUMN,
    SILVER_FIRST_SEEN_AT_COLUMN,
    SILVER_INGESTED_AT_COLUMN,
    STATUS_RANK,
    STICKY_COLUMNS,
    TERMINAL_STATUS,
    OrdersSilverSchema,
)


class TestStatusRank:
    """Test the STATUS_RANK lifecycle ordering."""

    def test_every_lifecycle_status_is_ranked(self) -> None:
        """Verify all five order statuses have a rank."""
        assert set(STATUS_RANK) == {"created", "assigned", "picked_up", "delivered", "cancelled"}

    def test_ranks_are_unique_and_ascending_by_lifecycle_order(self) -> None:
        """Verify ranks strictly increase in lifecycle order."""
        lifecycle_order = ["created", "assigned", "picked_up", "delivered", "cancelled"]
        ranks = [STATUS_RANK[status] for status in lifecycle_order]
        assert ranks == sorted(ranks)
        assert len(set(ranks)) == len(ranks)

    def test_cancelled_is_terminal_and_ranks_highest(self) -> None:
        """Verify cancelled is TERMINAL_STATUS and outranks every other status."""
        assert TERMINAL_STATUS == "cancelled"
        assert STATUS_RANK["cancelled"] == max(STATUS_RANK.values())


class TestColumnClassification:
    """Test the STICKY_COLUMNS/LATEST_WINS_COLUMNS shared source of truth."""

    def test_sticky_columns_match_spec(self) -> None:
        """Verify STICKY_COLUMNS is exactly the locked set."""
        assert STICKY_COLUMNS == (
            "created_at",
            "assigned_at",
            "picked_up_at",
            "delivered_at",
            "cancelled_at",
        )

    def test_latest_wins_columns_match_spec(self) -> None:
        """Verify LATEST_WINS_COLUMNS is exactly the locked set."""
        assert LATEST_WINS_COLUMNS == ("status", "rider_id", "store_id", "order_total", "updated_at")

    def test_sticky_and_latest_wins_are_disjoint(self) -> None:
        """Verify no column is classified as both sticky and latest-wins."""
        assert set(STICKY_COLUMNS).isdisjoint(set(LATEST_WINS_COLUMNS))

    def test_bronze_lineage_columns_do_not_overlap_business_columns(self) -> None:
        """Verify lineage columns are distinct from the business column classes."""
        assert set(BRONZE_LINEAGE_COLUMNS).isdisjoint(set(STICKY_COLUMNS) | set(LATEST_WINS_COLUMNS))


class TestOrdersSilverSchema:
    """Test the OrdersSilverSchema.TARGET StructType."""

    def test_target_contains_every_source_column(self) -> None:
        """Verify every raw orders source column is present in the target schema."""
        target_fields = {field.name for field in OrdersSilverSchema.TARGET.fields}
        source_columns = {
            "order_id",
            "store_id",
            "rider_id",
            "status",
            "created_at",
            "assigned_at",
            "picked_up_at",
            "delivered_at",
            "order_total",
            "updated_at",
        }
        assert source_columns <= target_fields

    def test_target_contains_derived_and_partition_columns(self) -> None:
        """Verify cancelled_at and the created_date partition column are present."""
        target_fields = {field.name for field in OrdersSilverSchema.TARGET.fields}
        assert "cancelled_at" in target_fields
        assert PARTITION_COLUMN in target_fields

    def test_target_contains_audit_columns(self) -> None:
        """Verify both silver audit columns are present."""
        target_fields = {field.name for field in OrdersSilverSchema.TARGET.fields}
        assert SILVER_INGESTED_AT_COLUMN in target_fields
        assert SILVER_FIRST_SEEN_AT_COLUMN in target_fields

    def test_target_keeps_bronze_lineage_columns(self) -> None:
        """Verify every BRONZE_LINEAGE_COLUMNS entry made it into the target schema."""
        target_fields = {field.name for field in OrdersSilverSchema.TARGET.fields}
        assert set(BRONZE_LINEAGE_COLUMNS) <= target_fields

    def test_partition_column_is_a_date_type(self) -> None:
        """Verify created_date is a DateType, never a timestamp."""
        from pyspark.sql.types import DateType

        field = next(f for f in OrdersSilverSchema.TARGET.fields if f.name == PARTITION_COLUMN)
        assert isinstance(field.dataType, DateType)

    def test_order_id_is_not_nullable(self) -> None:
        """Verify order_id, the merge/grouping key, is declared non-nullable."""
        field = next(f for f in OrdersSilverSchema.TARGET.fields if f.name == "order_id")
        assert field.nullable is False
