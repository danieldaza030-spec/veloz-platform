"""Tests for dags/ingest_orders_silver.py.

Structural checks follow the AST-based pattern established by
`tests/test_ingest_orders_bronze_dag.py` (the host venv has no real
Airflow install, so `dags/*.py` modules import `airflow.sdk`, which
can't be imported directly -- see `tests/conftest.py`'s
`load_dag_pure_symbols`). Pure-function coverage of
`_resolve_ingestion_filter`, `_union_bronze_asset_event_extras`, and
`_enumerate_windows` follows the pattern in
`tests/test_ingest_bronze_trigger_dates.py`.
"""

from __future__ import annotations

import ast
from datetime import datetime, timedelta
from pathlib import Path

import pytest

DAG_FILENAME = "ingest_orders_silver.py"
DAG_PATH = Path(__file__).parent.parent / "dags" / DAG_FILENAME


class _FakeAssetEvent:
    """Stand-in for Airflow's `AssetEvent`, carrying only the `extra` dict read by the DAG."""

    def __init__(self, extra: dict | None) -> None:
        self.extra = extra


class TestIngestOrdersSilverDAGStructure:
    """Structural checks: syntax, the DAG decorator, and its Asset schedule."""

    def test_dag_file_syntax_valid(self) -> None:
        """Verify ingest_orders_silver.py has valid Python syntax."""
        ast.parse(DAG_PATH.read_text())

    def test_dag_has_dag_decorator(self) -> None:
        """Verify a function named ingest_orders_silver is @dag-decorated."""
        tree = ast.parse(DAG_PATH.read_text())
        dag_functions = [
            node.name
            for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef)
            for decorator in node.decorator_list
            if (isinstance(decorator, ast.Name) and decorator.id == "dag")
            or (isinstance(decorator, ast.Call) and isinstance(decorator.func, ast.Name) and decorator.func.id == "dag")
        ]
        assert dag_functions == ["ingest_orders_silver"]

    def test_schedule_is_orders_bronze_asset(self) -> None:
        """Verify the DAG's schedule= is exactly [ORDERS_BRONZE_ASSET]."""
        tree = ast.parse(DAG_PATH.read_text())
        dag_calls = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "dag"
        ]
        assert len(dag_calls) == 1
        schedule_keywords = [kw for kw in dag_calls[0].keywords if kw.arg == "schedule"]
        assert len(schedule_keywords) == 1
        schedule_value = schedule_keywords[0].value
        assert isinstance(schedule_value, ast.List)
        names = [elt.id for elt in schedule_value.elts if isinstance(elt, ast.Name)]
        assert names == ["ORDERS_BRONZE_ASSET"]

    def test_defines_orders_bronze_asset(self) -> None:
        """Verify ORDERS_BRONZE_ASSET is defined at module level."""
        tree = ast.parse(DAG_PATH.read_text())
        top_level_targets = {
            target.id
            for node in tree.body
            if isinstance(node, ast.Assign)
            for target in node.targets
            if isinstance(target, ast.Name)
        }
        assert "ORDERS_BRONZE_ASSET" in top_level_targets

    def test_exposes_exactly_two_manual_modes(self) -> None:
        """Verify VALID_MODES names exactly ingestion_window and extract_date, no third."""
        tree = ast.parse(DAG_PATH.read_text())
        valid_modes_assignments = [
            node
            for node in tree.body
            if isinstance(node, ast.Assign)
            for target in node.targets
            if isinstance(target, ast.Name) and target.id == "VALID_MODES"
        ]
        assert len(valid_modes_assignments) == 1
        modes_tuple = valid_modes_assignments[0].value
        assert isinstance(modes_tuple, ast.Tuple)
        modes = {elt.value for elt in modes_tuple.elts if isinstance(elt, ast.Constant)}
        assert modes == {"ingestion_window", "extract_date"}

    def test_params_declare_the_four_manual_rerun_knobs(self) -> None:
        """Verify the DAG's params= declares mode/window_start/window_end/extract_dates."""
        tree = ast.parse(DAG_PATH.read_text())
        dag_calls = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "dag"
        ]
        params_keywords = [kw for kw in dag_calls[0].keywords if kw.arg == "params"]
        assert len(params_keywords) == 1
        params_dict = params_keywords[0].value
        assert isinstance(params_dict, ast.Dict)
        param_names = {key.value for key in params_dict.keys if isinstance(key, ast.Constant)}
        assert param_names == {"mode", "window_start", "window_end", "extract_dates"}


def _load(load_dag_pure_symbols, names: set[str]):
    return load_dag_pure_symbols(DAG_FILENAME, names, {"datetime": datetime, "timedelta": timedelta})


def _load_resolve_fn(load_dag_pure_symbols):
    namespace = _load(
        load_dag_pure_symbols,
        {
            "WINDOW_TOKEN_FORMAT",
            "WINDOW_MINUTES",
            "VALID_MODES",
            "_enumerate_windows",
            "_union_bronze_asset_event_extras",
            "_resolve_ingestion_filter",
        },
    )
    return namespace["_resolve_ingestion_filter"]


class TestEnumerateWindows:
    """Coverage for expanding a manual window_start/window_end range into explicit tokens."""

    def test_enumerates_inclusive_range_at_five_minute_cadence(self, load_dag_pure_symbols) -> None:
        namespace = _load(load_dag_pure_symbols, {"WINDOW_TOKEN_FORMAT", "WINDOW_MINUTES", "_enumerate_windows"})
        fn = namespace["_enumerate_windows"]

        result = fn("20260905T191000Z", "20260905T192000Z")

        assert result == [
            "20260905T191000Z",
            "20260905T191500Z",
            "20260905T192000Z",
        ]

    def test_single_window_when_start_equals_end(self, load_dag_pure_symbols) -> None:
        namespace = _load(load_dag_pure_symbols, {"WINDOW_TOKEN_FORMAT", "WINDOW_MINUTES", "_enumerate_windows"})
        fn = namespace["_enumerate_windows"]

        assert fn("20260905T191000Z", "20260905T191000Z") == ["20260905T191000Z"]

    def test_raises_when_end_precedes_start(self, load_dag_pure_symbols) -> None:
        namespace = _load(load_dag_pure_symbols, {"WINDOW_TOKEN_FORMAT", "WINDOW_MINUTES", "_enumerate_windows"})
        fn = namespace["_enumerate_windows"]

        with pytest.raises(ValueError):
            fn("20260905T191500Z", "20260905T191000Z")


class TestUnionBronzeAssetEventExtras:
    """Coverage for unioning extract_dates/windows across coalesced ORDERS_BRONZE_ASSET events."""

    def test_unions_multiple_triggering_events(self, load_dag_pure_symbols) -> None:
        namespace = _load(load_dag_pure_symbols, {"_union_bronze_asset_event_extras"})
        fn = namespace["_union_bronze_asset_event_extras"]
        context = {
            "triggering_asset_events": {
                "orders-bronze": [
                    _FakeAssetEvent({"extract_dates": ["2026-09-05"], "windows": ["20260905T191000Z"]}),
                    _FakeAssetEvent({"extract_dates": ["2026-09-05"], "windows": ["20260905T191500Z"]}),
                ]
            }
        }

        extract_dates, windows = fn(context)

        assert extract_dates == ["2026-09-05"]
        assert windows == ["20260905T191000Z", "20260905T191500Z"]

    def test_returns_empty_lists_for_manual_trigger(self, load_dag_pure_symbols) -> None:
        namespace = _load(load_dag_pure_symbols, {"_union_bronze_asset_event_extras"})
        fn = namespace["_union_bronze_asset_event_extras"]

        assert fn({}) == ([], [])
        assert fn({"triggering_asset_events": {}}) == ([], [])


class TestResolveIngestionFilter:
    """Coverage for the module's core precedence rule (spec items 1-4)."""

    def test_unions_multiple_triggering_asset_events(self, load_dag_pure_symbols) -> None:
        """Spec 1: coalesced events -- union across ALL of them, not just the latest."""
        fn = _load_resolve_fn(load_dag_pure_symbols)
        context = {
            "triggering_asset_events": {
                "orders-bronze": [
                    _FakeAssetEvent({"extract_dates": [], "windows": ["20260905T191000Z"]}),
                    _FakeAssetEvent({"extract_dates": [], "windows": ["20260905T191500Z"]}),
                ]
            }
        }

        ingestion_windows, extract_dates = fn({"mode": None}, context)

        assert ingestion_windows == ["20260905T191000Z", "20260905T191500Z"]
        assert extract_dates is None

    def test_explicit_params_override_asset_event_extras(self, load_dag_pure_symbols) -> None:
        """Spec 2: an explicit mode param wins even when triggering extras are present."""
        fn = _load_resolve_fn(load_dag_pure_symbols)
        context = {
            "triggering_asset_events": {
                "orders-bronze": [
                    _FakeAssetEvent({"extract_dates": ["2026-01-01"], "windows": ["20260905T191000Z"]})
                ]
            }
        }
        params = {
            "mode": "extract_date",
            "extract_dates": ["2026-09-05"],
            "window_start": None,
            "window_end": None,
        }

        ingestion_windows, extract_dates = fn(params, context)

        assert ingestion_windows is None
        assert extract_dates == ["2026-09-05"]

    def test_raises_when_neither_params_nor_extras_present(self, load_dag_pure_symbols) -> None:
        """Spec 3: no explicit mode, no triggering extras -- raise, never default."""
        fn = _load_resolve_fn(load_dag_pure_symbols)

        with pytest.raises(ValueError):
            fn({"mode": None}, {})

        with pytest.raises(ValueError):
            fn({"mode": None}, {"triggering_asset_events": {}})

    def test_windows_preferred_over_extract_dates_when_both_present(self, load_dag_pure_symbols) -> None:
        """Spec 4: asset-triggered run with both extras set -- windows wins."""
        fn = _load_resolve_fn(load_dag_pure_symbols)
        context = {
            "triggering_asset_events": {
                "orders-bronze": [
                    _FakeAssetEvent({"extract_dates": ["2026-09-05"], "windows": ["20260905T191000Z"]})
                ]
            }
        }

        ingestion_windows, extract_dates = fn({"mode": None}, context)

        assert ingestion_windows == ["20260905T191000Z"]
        assert extract_dates is None

    def test_falls_back_to_extract_dates_when_windows_extra_absent(self, load_dag_pure_symbols) -> None:
        """Asset-triggered run with only extract_dates set (no windows) -- extract_dates used."""
        fn = _load_resolve_fn(load_dag_pure_symbols)
        context = {
            "triggering_asset_events": {
                "orders-bronze": [_FakeAssetEvent({"extract_dates": ["2026-09-05"], "windows": []})]
            }
        }

        ingestion_windows, extract_dates = fn({"mode": None}, context)

        assert ingestion_windows is None
        assert extract_dates == ["2026-09-05"]

    def test_raises_for_unsupported_mode(self, load_dag_pure_symbols) -> None:
        """No third mode: an unrecognized mode value raises rather than being tolerated."""
        fn = _load_resolve_fn(load_dag_pure_symbols)

        with pytest.raises(ValueError):
            fn({"mode": "today"}, {})

    def test_ingestion_window_mode_requires_both_bounds(self, load_dag_pure_symbols) -> None:
        fn = _load_resolve_fn(load_dag_pure_symbols)

        with pytest.raises(ValueError):
            fn({"mode": "ingestion_window", "window_start": "20260905T191000Z", "window_end": None}, {})

    def test_ingestion_window_mode_resolves_explicit_range(self, load_dag_pure_symbols) -> None:
        fn = _load_resolve_fn(load_dag_pure_symbols)
        params = {
            "mode": "ingestion_window",
            "window_start": "20260905T191000Z",
            "window_end": "20260905T192000Z",
        }

        ingestion_windows, extract_dates = fn(params, {})

        assert ingestion_windows == [
            "20260905T191000Z",
            "20260905T191500Z",
            "20260905T192000Z",
        ]
        assert extract_dates is None

    def test_extract_date_mode_requires_non_empty_list(self, load_dag_pure_symbols) -> None:
        fn = _load_resolve_fn(load_dag_pure_symbols)

        with pytest.raises(ValueError):
            fn({"mode": "extract_date", "extract_dates": []}, {})

    def test_extract_date_mode_resolves_explicit_list(self, load_dag_pure_symbols) -> None:
        fn = _load_resolve_fn(load_dag_pure_symbols)
        params = {"mode": "extract_date", "extract_dates": ["2026-09-05", "2026-09-06"]}

        ingestion_windows, extract_dates = fn(params, {})

        assert ingestion_windows is None
        assert extract_dates == ["2026-09-05", "2026-09-06"]
