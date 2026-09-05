"""Runtime tests for `infrastructure/s3_new_object_trigger.py`'s `run()`.

Airflow is not installed in this environment, so these tests rely on the
`airflow.triggers.base` stub installed by `tests/conftest.py`
(`_install_airflow_triggers_base_stub`) to make `S3NewObjectTrigger`
importable and instantiable at all. Everything `run()` calls into
(`compute_seen_diff`, `persist_seen_keys`, `read_manifest_seen_keys`) is
mocked here with plain `MagicMock`s, matching how `run()` actually invokes
them: synchronously, inside a worker thread via `asyncio.to_thread`, not
directly awaited. `asyncio.sleep` is the one call `run()` awaits directly,
so it is the only dependency mocked with `AsyncMock`. No real S3/MinIO is
touched, and `pytest-asyncio` is not installed, so async test bodies are
driven with a small `asyncio.run` wrapper instead of that plugin.

`run()`'s `while True` poll loop never exits on its own, so each test that
needs to observe more than the initial seed step patches `asyncio.sleep` to
raise a sentinel exception on a chosen call, deterministically terminating
iteration at a known point instead of relying on a wall-clock timeout.
"""

from __future__ import annotations

import asyncio
import functools
from collections.abc import Awaitable, Callable
from typing import TypeVar
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from infrastructure.s3_new_object_trigger import S3NewObjectTrigger

BUCKET = "test-bucket"
PREFIX = "orders/"

_F = TypeVar("_F", bound=Callable[..., Awaitable[None]])


def run_async(test_func: _F) -> Callable[..., None]:
    """Runs an async test function to completion via `asyncio.run`.

    Stands in for `pytest.mark.asyncio`, which requires the `pytest-asyncio`
    plugin that is not installed in this environment.

    Args:
        test_func: An `async def` test function.

    Returns:
        A synchronous wrapper pytest can call directly.
    """

    @functools.wraps(test_func)
    def wrapper(*args: object, **kwargs: object) -> None:
        asyncio.run(test_func(*args, **kwargs))

    return wrapper


class _StopLoop(Exception):
    """Sentinel exception used to deterministically break the poll loop."""


def _make_trigger() -> S3NewObjectTrigger:
    """Builds a trigger instance against an unrelated, valid watch prefix."""
    return S3NewObjectTrigger(bucket=BUCKET, prefix=PREFIX, poke_interval=0.0)


class TestRunSeedsFromValidManifest:
    """(a) A valid manifest seeds `seen` and does not refire those keys."""

    @run_async
    async def test_valid_manifest_seed_does_not_refire_and_is_passed_through(self) -> None:
        """No events fire for keys the manifest already recorded as seen."""
        seeded_seen = {"a.txt", "b.txt"}
        mock_read_manifest = MagicMock(return_value=seeded_seen)
        mock_compute_seen_diff = MagicMock(return_value=(seeded_seen, []))
        mock_persist = MagicMock()
        mock_sleep = AsyncMock(side_effect=[None, _StopLoop("stop-test")])

        trigger = _make_trigger()

        with (
            patch(
                "infrastructure.s3_new_object_trigger.read_manifest_seen_keys",
                mock_read_manifest,
            ),
            patch(
                "infrastructure.s3_new_object_trigger.compute_seen_diff",
                mock_compute_seen_diff,
            ),
            patch("infrastructure.s3_new_object_trigger.persist_seen_keys", mock_persist),
            patch("infrastructure.s3_new_object_trigger.asyncio.sleep", mock_sleep),
        ):
            events = []
            with pytest.raises(_StopLoop):
                async for event in trigger.run():
                    events.append(event.payload)

        # No events fired: the seeded manifest already covered every key.
        assert events == []
        # The seeded set was carried into the first poll's diff, unmutated
        # in content (still exactly what the manifest recorded).
        mock_compute_seen_diff.assert_called_once_with(BUCKET, PREFIX, seeded_seen)
        mock_persist.assert_called_once_with(BUCKET, PREFIX, seeded_seen, [])


class TestRunFallsBackToFireAllOnNoneSeed:
    """(b) `None` from the seed read fires one event per currently-listed key."""

    @run_async
    async def test_none_seed_fires_one_event_per_currently_listed_key(self) -> None:
        """A missing/invalid manifest (None) fires every currently-listed key."""
        mock_read_manifest = MagicMock(return_value=None)
        fired_seen = {"a.txt", "b.txt"}
        mock_compute_seen_diff = MagicMock(return_value=(fired_seen, ["a.txt", "b.txt"]))
        mock_persist = MagicMock()
        mock_sleep = AsyncMock(side_effect=_StopLoop("stop-test"))

        trigger = _make_trigger()

        with (
            patch(
                "infrastructure.s3_new_object_trigger.read_manifest_seen_keys",
                mock_read_manifest,
            ),
            patch(
                "infrastructure.s3_new_object_trigger.compute_seen_diff",
                mock_compute_seen_diff,
            ),
            patch("infrastructure.s3_new_object_trigger.persist_seen_keys", mock_persist),
            patch("infrastructure.s3_new_object_trigger.asyncio.sleep", mock_sleep),
        ):
            events = []
            with pytest.raises(_StopLoop):
                async for event in trigger.run():
                    events.append(event.payload)

        assert events == [
            {"bucket": BUCKET, "key": "a.txt"},
            {"bucket": BUCKET, "key": "b.txt"},
        ]
        # N2: the fire-all fallback delegates to compute_seen_diff with a
        # fresh empty seed set, rather than reimplementing the diff inline.
        mock_compute_seen_diff.assert_called_once_with(BUCKET, PREFIX, set())
        mock_persist.assert_called_once_with(BUCKET, PREFIX, fired_seen, ["a.txt", "b.txt"])


class TestRunFallsBackOnUnexpectedSeedReadException:
    """(c) F2 regression: an unexpected seed-read exception falls back to fire-all."""

    @run_async
    async def test_unexpected_seed_read_exception_falls_back_instead_of_propagating(
        self,
    ) -> None:
        """A boto3/connection-style failure from the seed read must not kill the trigger."""
        mock_read_manifest = MagicMock(side_effect=ConnectionError("boto3 timeout"))
        fired_seen = {"a.txt"}
        mock_compute_seen_diff = MagicMock(return_value=(fired_seen, ["a.txt"]))
        mock_persist = MagicMock()
        mock_sleep = AsyncMock(side_effect=_StopLoop("stop-test"))

        trigger = _make_trigger()

        with (
            patch(
                "infrastructure.s3_new_object_trigger.read_manifest_seen_keys",
                mock_read_manifest,
            ),
            patch(
                "infrastructure.s3_new_object_trigger.compute_seen_diff",
                mock_compute_seen_diff,
            ),
            patch("infrastructure.s3_new_object_trigger.persist_seen_keys", mock_persist),
            patch("infrastructure.s3_new_object_trigger.asyncio.sleep", mock_sleep),
        ):
            events = []
            # Only the sentinel from the mocked sleep() should propagate;
            # the ConnectionError from read_manifest_seen_keys must not.
            with pytest.raises(_StopLoop):
                async for event in trigger.run():
                    events.append(event.payload)

        assert events == [{"bucket": BUCKET, "key": "a.txt"}]
        mock_compute_seen_diff.assert_called_once_with(BUCKET, PREFIX, set())
        mock_persist.assert_called_once_with(BUCKET, PREFIX, fired_seen, ["a.txt"])

    @run_async
    async def test_keyerror_from_unset_minio_endpoint_falls_back_instead_of_propagating(
        self,
    ) -> None:
        """A `KeyError` (unset MINIO_ENDPOINT) from the seed read also falls back."""
        mock_read_manifest = MagicMock(side_effect=KeyError("MINIO_ENDPOINT"))
        fired_seen: set[str] = set()
        mock_compute_seen_diff = MagicMock(return_value=(fired_seen, []))
        mock_persist = MagicMock()
        mock_sleep = AsyncMock(side_effect=_StopLoop("stop-test"))

        trigger = _make_trigger()

        with (
            patch(
                "infrastructure.s3_new_object_trigger.read_manifest_seen_keys",
                mock_read_manifest,
            ),
            patch(
                "infrastructure.s3_new_object_trigger.compute_seen_diff",
                mock_compute_seen_diff,
            ),
            patch("infrastructure.s3_new_object_trigger.persist_seen_keys", mock_persist),
            patch("infrastructure.s3_new_object_trigger.asyncio.sleep", mock_sleep),
        ):
            events = []
            with pytest.raises(_StopLoop):
                async for event in trigger.run():
                    events.append(event.payload)

        assert events == []
        mock_compute_seen_diff.assert_called_once_with(BUCKET, PREFIX, set())


class TestRunYieldsBeforePersisting:
    """(d) Events for a poll are yielded before that poll's state is persisted."""

    @run_async
    async def test_events_yielded_before_persist_seen_keys_is_called(self) -> None:
        """Persistence must observably happen after all of a poll's events are yielded."""
        mock_read_manifest = MagicMock(return_value=None)
        fired_seen = {"a.txt", "b.txt"}
        mock_compute_seen_diff = MagicMock(return_value=(fired_seen, ["a.txt", "b.txt"]))
        mock_sleep = AsyncMock(side_effect=_StopLoop("stop-test"))

        call_order: list[tuple[str, str]] = []

        def record_persist(bucket: str, prefix: str, seen: set[str], new_keys: list[str]) -> None:
            call_order.append(("persist", ",".join(new_keys)))

        mock_persist = MagicMock(side_effect=record_persist)
        trigger = _make_trigger()

        with (
            patch(
                "infrastructure.s3_new_object_trigger.read_manifest_seen_keys",
                mock_read_manifest,
            ),
            patch(
                "infrastructure.s3_new_object_trigger.compute_seen_diff",
                mock_compute_seen_diff,
            ),
            patch("infrastructure.s3_new_object_trigger.persist_seen_keys", mock_persist),
            patch("infrastructure.s3_new_object_trigger.asyncio.sleep", mock_sleep),
        ):
            with pytest.raises(_StopLoop):
                async for event in trigger.run():
                    call_order.append(("event", event.payload["key"]))

        assert call_order == [
            ("event", "a.txt"),
            ("event", "b.txt"),
            ("persist", "a.txt,b.txt"),
        ]
