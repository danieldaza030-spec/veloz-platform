"""Runtime tests for `infrastructure/s3_new_object_trigger.py`'s `run()`.

Airflow is not installed in this environment, so these tests rely on the
`airflow.triggers.base` stub installed by `tests/conftest.py`
(`_install_airflow_triggers_base_stub`) to make `S3NewObjectTrigger`
importable and instantiable at all. `list_keys` is mocked here with a plain
`MagicMock`, matching how `run()` actually invokes it: synchronously, inside
a worker thread via `asyncio.to_thread`, not directly awaited. `asyncio.sleep`
is the one call `run()` awaits directly, so it is the only dependency mocked
with `AsyncMock`. No real S3/MinIO is touched, and `pytest-asyncio` is not
installed, so async test bodies are driven with a small `asyncio.run` wrapper
instead of that plugin.

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


class TestRunSeedsFromLiveListing:
    """The seed baseline comes from a single live `list_keys` call, not a manifest."""

    @run_async
    async def test_seed_listing_does_not_refire_on_first_poll(self) -> None:
        """Keys present at the seed listing are not re-emitted when unchanged."""
        seed_keys = ["a.txt", "b.txt"]
        mock_list_keys = MagicMock(side_effect=[seed_keys, set(seed_keys)])
        mock_sleep = AsyncMock(side_effect=[None, _StopLoop("stop-test")])

        trigger = _make_trigger()

        with (
            patch("infrastructure.s3_new_object_trigger.list_keys", mock_list_keys),
            patch("infrastructure.s3_new_object_trigger.asyncio.sleep", mock_sleep),
        ):
            events = []
            with pytest.raises(_StopLoop):
                async for event in trigger.run():
                    events.append(event.payload)

        # No events fired: the first poll saw exactly the seeded baseline.
        assert events == []
        assert mock_list_keys.call_count == 2
        mock_list_keys.assert_called_with(BUCKET, PREFIX)


class TestRunSurvivesTransientPollException:
    """Regression: a transient MinIO error during a poll must not kill the coroutine."""

    @run_async
    async def test_transient_list_keys_exception_is_logged_and_skipped(self) -> None:
        """A `ConnectionError` on one poll is swallowed; the next poll still fires."""
        seed_keys = ["a.txt"]
        new_key = "c.txt"
        mock_list_keys = MagicMock(
            side_effect=[seed_keys, ConnectionError("boto3 timeout"), {*seed_keys, new_key}]
        )
        mock_sleep = AsyncMock(side_effect=[None, None, _StopLoop("stop-test")])

        trigger = _make_trigger()

        with (
            patch("infrastructure.s3_new_object_trigger.list_keys", mock_list_keys),
            patch("infrastructure.s3_new_object_trigger.asyncio.sleep", mock_sleep),
        ):
            events = []
            # The ConnectionError from the second list_keys call must not
            # propagate; only the sentinel from the mocked sleep() should.
            with pytest.raises(_StopLoop):
                async for event in trigger.run():
                    events.append(event.payload)

        # The failed poll yields nothing; the recovered third poll fires
        # the one genuinely new key.
        assert events == [{"bucket": BUCKET, "key": new_key}]
        assert mock_list_keys.call_count == 3
