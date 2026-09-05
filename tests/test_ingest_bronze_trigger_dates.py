"""Tests for `_extract_dates_from_triggering_event` in the Bronze ingestion DAGs.

`dags/ingest_orders_bronze.py` and `dags/ingest_rider_events_bronze.py`
define this function identically (see each module's docstring for why:
`max_active_runs=1` lets Airflow coalesce several pending `AssetEvent`s
onto the next run, and a coalesced batch can span more than one `date=`
partition). These tests exercise the function straight out of each DAG
module's source via the `load_dag_pure_symbols` fixture (see
`tests/conftest.py`), parametrized over both DAG files instead of
duplicating the test bodies per file, since the function under test is
byte-for-byte identical in both.
"""

from __future__ import annotations

import re

import pytest

DAG_FILENAMES = ["ingest_orders_bronze.py", "ingest_rider_events_bronze.py"]


class _FakeAssetEvent:
    """Stand-in for Airflow's `AssetEvent`, carrying only the `extra` dict read by the DAG."""

    def __init__(self, extra: dict | None) -> None:
        self.extra = extra


def _load_extract_dates_fn(load_dag_pure_symbols, dag_filename: str):
    namespace = load_dag_pure_symbols(
        dag_filename,
        {"DATE_PARTITION_PATTERN", "_extract_dates_from_triggering_event"},
        {"re": re},
    )
    return namespace["_extract_dates_from_triggering_event"]


@pytest.mark.parametrize("dag_filename", DAG_FILENAMES)
class TestExtractDatesFromTriggeringEvent:
    """Regression coverage for coalesced, cross-date Asset-triggered runs."""

    def test_returns_both_dates_when_coalesced_events_cross_midnight(
        self, load_dag_pure_symbols, dag_filename: str
    ) -> None:
        """A coalesced run whose events span two dates must return both, not just the first."""
        fn = _load_extract_dates_fn(load_dag_pure_symbols, dag_filename)
        context = {
            "triggering_asset_events": {
                "some-asset": [
                    _FakeAssetEvent({"bucket": "raw-incoming-data", "key": "orders/date=2026-09-04/orders_2350.csv"}),
                    _FakeAssetEvent({"bucket": "raw-incoming-data", "key": "orders/date=2026-09-05/orders_0005.csv"}),
                ]
            }
        }

        assert fn(context) == ["2026-09-04", "2026-09-05"]

    def test_deduplicates_repeated_dates(self, load_dag_pure_symbols, dag_filename: str) -> None:
        """Multiple window-files for the same date collapse to a single entry."""
        fn = _load_extract_dates_fn(load_dag_pure_symbols, dag_filename)
        context = {
            "triggering_asset_events": {
                "some-asset": [
                    _FakeAssetEvent({"bucket": "raw-incoming-data", "key": "orders/date=2026-09-05/orders_0000.csv"}),
                    _FakeAssetEvent({"bucket": "raw-incoming-data", "key": "orders/date=2026-09-05/orders_0005.csv"}),
                ]
            }
        }

        assert fn(context) == ["2026-09-05"]

    def test_returns_empty_list_for_manual_trigger(self, load_dag_pure_symbols, dag_filename: str) -> None:
        """A manually triggered run has no `triggering_asset_events` to read a date off of."""
        fn = _load_extract_dates_fn(load_dag_pure_symbols, dag_filename)

        assert fn({}) == []
        assert fn({"triggering_asset_events": {}}) == []

    def test_ignores_events_with_no_matching_date_segment(self, load_dag_pure_symbols, dag_filename: str) -> None:
        """An event whose key has no `date=<date>` segment contributes nothing."""
        fn = _load_extract_dates_fn(load_dag_pure_symbols, dag_filename)
        context = {
            "triggering_asset_events": {
                "some-asset": [_FakeAssetEvent({"bucket": "raw-incoming-data", "key": "orders/malformed-key.csv"})]
            }
        }

        assert fn(context) == []
