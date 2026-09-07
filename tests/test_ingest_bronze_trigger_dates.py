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

The same file also covers `_windows_from_triggering_event` and
`_resolve_windows_to_ingest`, the two pure functions behind each DAG's
bounded reglob (see each module's docstring): the former groups a
triggering run's window tokens by date, the latter expands a run's
triggering windows into the bounded lookback of already-landed windows
each run's Bronze write actually needs to cover.
"""

from __future__ import annotations

import re

import pytest

DAG_FILENAMES = ["ingest_orders_bronze.py", "ingest_rider_events_bronze.py"]

# Each DAG uses a dataset-specific WINDOW_PATTERN (orders_*.csv vs.
# rider_events_*.jsonl); this mapping lets a single parametrized test build
# a key matching the pattern actually loaded for a given DAG file.
WINDOW_KEY_TEMPLATES = {
    "ingest_orders_bronze.py": "orders/date={date}/orders_{window}.csv",
    "ingest_rider_events_bronze.py": "rider_events/date={date}/rider_events_{window}.jsonl",
}


class _FakeAssetEvent:
    """Stand-in for Airflow's `AssetEvent`, carrying only the `extra` dict read by the DAG."""

    def __init__(self, extra: dict | None) -> None:
        self.extra = extra


def _trigger_extra(bucket: str, key: str) -> dict:
    """Builds an `AssetEvent.extra` payload shaped like Airflow's own `Trigger.submit_event`.

    `Trigger.submit_event` (`airflow/models/trigger.py`) wraps a
    trigger's `TriggerEvent` payload before storing it on the resulting
    `AssetEvent`, nesting it under `extra["payload"]` alongside
    `extra["from_trigger"]` rather than storing it as `extra` directly.
    `S3NewObjectTrigger` yields `TriggerEvent({"bucket": ..., "key":
    ...})`, so a real triggered event's `extra` looks like this, not
    like a flat `{"bucket": ..., "key": ...}`.
    """
    return {"from_trigger": True, "payload": {"bucket": bucket, "key": key}}


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
                    _FakeAssetEvent(_trigger_extra("raw-incoming-data", "orders/date=2026-09-04/orders_2350.csv")),
                    _FakeAssetEvent(_trigger_extra("raw-incoming-data", "orders/date=2026-09-05/orders_0005.csv")),
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
                    _FakeAssetEvent(_trigger_extra("raw-incoming-data", "orders/date=2026-09-05/orders_0000.csv")),
                    _FakeAssetEvent(_trigger_extra("raw-incoming-data", "orders/date=2026-09-05/orders_0005.csv")),
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
                "some-asset": [_FakeAssetEvent(_trigger_extra("raw-incoming-data", "orders/malformed-key.csv"))]
            }
        }

        assert fn(context) == []


def _load_windows_from_triggering_event_fn(load_dag_pure_symbols, dag_filename: str):
    namespace = load_dag_pure_symbols(
        dag_filename,
        {"DATE_PARTITION_PATTERN", "WINDOW_PATTERN", "_windows_from_triggering_event"},
        {"re": re},
    )
    return namespace["_windows_from_triggering_event"]


def _load_resolve_windows_to_ingest_fn(load_dag_pure_symbols, dag_filename: str):
    namespace = load_dag_pure_symbols(dag_filename, {"_resolve_windows_to_ingest"}, {"re": re})
    return namespace["_resolve_windows_to_ingest"]


@pytest.mark.parametrize("dag_filename", DAG_FILENAMES)
class TestWindowsFromTriggeringEvent:
    """Coverage for grouping a run's triggering window tokens by date."""

    def test_groups_single_window_under_its_date(self, load_dag_pure_symbols, dag_filename: str) -> None:
        """A single triggering event's window is grouped under its own date."""
        fn = _load_windows_from_triggering_event_fn(load_dag_pure_symbols, dag_filename)
        key = WINDOW_KEY_TEMPLATES[dag_filename].format(date="2026-09-05", window="20260905T191000Z")
        context = {
            "triggering_asset_events": {
                "some-asset": [_FakeAssetEvent(_trigger_extra("raw-incoming-data", key))]
            }
        }

        assert fn(context) == {"2026-09-05": ["20260905T191000Z"]}

    def test_groups_multiple_windows_coalesced_onto_one_date(
        self, load_dag_pure_symbols, dag_filename: str
    ) -> None:
        """Several coalesced triggering windows for the same date are grouped together, sorted."""
        fn = _load_windows_from_triggering_event_fn(load_dag_pure_symbols, dag_filename)
        template = WINDOW_KEY_TEMPLATES[dag_filename]
        context = {
            "triggering_asset_events": {
                "some-asset": [
                    _FakeAssetEvent(
                        _trigger_extra(
                            "raw-incoming-data", template.format(date="2026-09-05", window="20260905T191500Z")
                        )
                    ),
                    _FakeAssetEvent(
                        _trigger_extra(
                            "raw-incoming-data", template.format(date="2026-09-05", window="20260905T191000Z")
                        )
                    ),
                ]
            }
        }

        assert fn(context) == {"2026-09-05": ["20260905T191000Z", "20260905T191500Z"]}

    def test_splits_windows_spanning_two_dates(self, load_dag_pure_symbols, dag_filename: str) -> None:
        """Coalesced events crossing midnight are grouped under their own respective dates."""
        fn = _load_windows_from_triggering_event_fn(load_dag_pure_symbols, dag_filename)
        template = WINDOW_KEY_TEMPLATES[dag_filename]
        context = {
            "triggering_asset_events": {
                "some-asset": [
                    _FakeAssetEvent(
                        _trigger_extra(
                            "raw-incoming-data", template.format(date="2026-09-04", window="20260904T235500Z")
                        )
                    ),
                    _FakeAssetEvent(
                        _trigger_extra(
                            "raw-incoming-data", template.format(date="2026-09-05", window="20260905T000000Z")
                        )
                    ),
                ]
            }
        }

        assert fn(context) == {
            "2026-09-04": ["20260904T235500Z"],
            "2026-09-05": ["20260905T000000Z"],
        }

    def test_returns_empty_dict_for_manual_trigger(self, load_dag_pure_symbols, dag_filename: str) -> None:
        """A manually triggered run has no triggering Asset event to derive windows from."""
        fn = _load_windows_from_triggering_event_fn(load_dag_pure_symbols, dag_filename)

        assert fn({}) == {}
        assert fn({"triggering_asset_events": {}}) == {}


@pytest.mark.parametrize("dag_filename", DAG_FILENAMES)
class TestResolveWindowsToIngest:
    """Coverage for expanding triggering windows into a bounded lookback."""

    def test_lookback_mid_day_takes_preceding_windows(self, load_dag_pure_symbols, dag_filename: str) -> None:
        """A trigger with enough preceding windows available takes exactly lookback_count of them."""
        fn = _load_resolve_windows_to_ingest_fn(load_dag_pure_symbols, dag_filename)
        all_windows = [f"2026090500{minute:02d}00Z" for minute in range(0, 60, 5)]  # 00:00, 00:05, ..., 00:55
        triggering = [all_windows[6]]  # 00:30

        result = fn(all_windows, triggering, lookback_count=6)

        assert result == all_windows[1:7]
        assert len(result) == 6

    def test_clamps_at_start_of_day_with_fewer_windows_than_lookback(
        self, load_dag_pure_symbols, dag_filename: str
    ) -> None:
        """A trigger near the start of the day, with fewer than lookback_count windows available, is clamped instead of erroring."""
        fn = _load_resolve_windows_to_ingest_fn(load_dag_pure_symbols, dag_filename)
        all_windows = ["20260905T000000Z", "20260905T000500Z", "20260905T001000Z"]
        triggering = [all_windows[1]]  # only one window precedes it

        result = fn(all_windows, triggering, lookback_count=6)

        assert result == ["20260905T000000Z", "20260905T000500Z"]

    def test_unions_multiple_triggering_windows(self, load_dag_pure_symbols, dag_filename: str) -> None:
        """Coalesced triggering windows each expand their own lookback, unioned and sorted."""
        fn = _load_resolve_windows_to_ingest_fn(load_dag_pure_symbols, dag_filename)
        all_windows = [f"2026090500{minute:02d}00Z" for minute in range(0, 60, 5)]
        triggering = [all_windows[2], all_windows[8]]  # two coalesced windows, far apart

        result = fn(all_windows, triggering, lookback_count=3)

        expected = sorted(set(all_windows[0:3]) | set(all_windows[6:9]))
        assert result == expected

    def test_raises_if_triggering_window_not_found(self, load_dag_pure_symbols, dag_filename: str) -> None:
        """A triggering window absent from the listed windows is a loud failure, not silently skipped."""
        fn = _load_resolve_windows_to_ingest_fn(load_dag_pure_symbols, dag_filename)
        all_windows = ["20260905T000000Z", "20260905T000500Z"]

        with pytest.raises(ValueError):
            fn(all_windows, ["20260905T999999Z"], lookback_count=6)
