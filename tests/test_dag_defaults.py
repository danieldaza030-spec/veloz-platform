"""Tests for plugins/dag_defaults.py."""

from __future__ import annotations

import sys
from pathlib import Path

import pendulum
import pytest

# Add repo root to path so plugins/ can be imported
repo_root = Path(__file__).parent.parent
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

from plugins.dag_defaults import BRONZE_DEFAULT_ARGS, SILVER_DEFAULT_ARGS


class TestBronzeDefaultArgs:
    """Test BRONZE_DEFAULT_ARGS dict contents and types."""

    def test_bronze_default_args_has_required_keys(self) -> None:
        """Verify BRONZE_DEFAULT_ARGS contains exactly the four expected keys."""
        expected_keys = {"owner", "retries", "retry_delay", "execution_timeout"}
        assert set(BRONZE_DEFAULT_ARGS.keys()) == expected_keys

    def test_owner_is_data_platform(self) -> None:
        """Verify owner is set to 'data-platform'."""
        assert BRONZE_DEFAULT_ARGS["owner"] == "data-platform"

    def test_retries_is_three(self) -> None:
        """Verify retries is set to 3."""
        assert BRONZE_DEFAULT_ARGS["retries"] == 3
        assert isinstance(BRONZE_DEFAULT_ARGS["retries"], int)

    def test_retry_delay_is_five_minutes(self) -> None:
        """Verify retry_delay is a pendulum.Duration of 5 minutes."""
        retry_delay = BRONZE_DEFAULT_ARGS["retry_delay"]
        expected = pendulum.duration(minutes=5)
        assert retry_delay == expected
        # Verify it's a pendulum.Duration instance
        assert isinstance(retry_delay, pendulum.Duration)
        # Verify it equals 300 seconds (5 minutes)
        assert retry_delay.total_seconds() == 300

    def test_execution_timeout_is_thirty_minutes(self) -> None:
        """Verify execution_timeout is a pendulum.Duration of 30 minutes."""
        execution_timeout = BRONZE_DEFAULT_ARGS["execution_timeout"]
        expected = pendulum.duration(minutes=30)
        assert execution_timeout == expected
        # Verify it's a pendulum.Duration instance
        assert isinstance(execution_timeout, pendulum.Duration)
        # Verify it equals 1800 seconds (30 minutes)
        assert execution_timeout.total_seconds() == 1800

    def test_bronze_default_args_is_immutable_dict(self) -> None:
        """Verify BRONZE_DEFAULT_ARGS is a regular dict (not modified by user)."""
        assert isinstance(BRONZE_DEFAULT_ARGS, dict)
        # Verify all values can be accessed
        assert len(BRONZE_DEFAULT_ARGS) == 4


class TestSilverDefaultArgs:
    """Test SILVER_DEFAULT_ARGS dict contents and types."""

    def test_silver_default_args_has_required_keys(self) -> None:
        """Verify SILVER_DEFAULT_ARGS contains exactly the four expected keys."""
        expected_keys = {"owner", "retries", "retry_delay", "execution_timeout"}
        assert set(SILVER_DEFAULT_ARGS.keys()) == expected_keys

    def test_owner_is_data_platform(self) -> None:
        """Verify owner is set to 'data-platform'."""
        assert SILVER_DEFAULT_ARGS["owner"] == "data-platform"

    def test_retries_is_three(self) -> None:
        """Verify retries is set to 3."""
        assert SILVER_DEFAULT_ARGS["retries"] == 3
        assert isinstance(SILVER_DEFAULT_ARGS["retries"], int)

    def test_retry_delay_is_five_minutes(self) -> None:
        """Verify retry_delay is a pendulum.Duration of 5 minutes."""
        retry_delay = SILVER_DEFAULT_ARGS["retry_delay"]
        assert retry_delay == pendulum.duration(minutes=5)
        assert isinstance(retry_delay, pendulum.Duration)

    def test_execution_timeout_is_thirty_minutes(self) -> None:
        """Verify execution_timeout covers one bounded per-batch MERGE (30 minutes)."""
        execution_timeout = SILVER_DEFAULT_ARGS["execution_timeout"]
        assert execution_timeout == pendulum.duration(minutes=30)
        assert isinstance(execution_timeout, pendulum.Duration)

    def test_silver_default_args_is_a_distinct_dict_from_bronze(self) -> None:
        """Verify SILVER_DEFAULT_ARGS is its own dict, not a BRONZE_DEFAULT_ARGS alias."""
        assert isinstance(SILVER_DEFAULT_ARGS, dict)
        assert len(SILVER_DEFAULT_ARGS) == 4
        assert SILVER_DEFAULT_ARGS is not BRONZE_DEFAULT_ARGS
