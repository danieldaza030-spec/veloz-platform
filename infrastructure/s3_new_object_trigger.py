"""Event-driven trigger that fires when a new object lands under an S3/MinIO prefix.

Backs an `Asset` `watcher <https://airflow.apache.org/docs/apache-airflow/stable/
authoring-and-scheduling/event-scheduling.html>`_ so a Bronze ingestion DAG can be
scheduled off new raw files landing in `raw-incoming-data` instead of a fixed cron
guessing when the upstream (now every-5-minutes) generator run will have finished.

Airflow's own `S3KeyTrigger` is explicitly documented as *not* safe for
event-driven scheduling: it checks "does this key exist", which stays true
forever once the key is written, so a Dag scheduled on it would refire on every
poll (see "Avoid infinite scheduling" in the event-scheduling docs). This
trigger instead fires exactly once per newly observed key: it baselines the
keys already present under the prefix at startup, then poll-diffs against that
in-memory set, so each key can only ever cause one Asset update.

The "already seen" set has no durable backing store: `run()` seeds it with a
single live `list_keys` call at startup, so a triggerer restart re-baselines
against whatever is present at that moment and does not re-emit the existing
backlog. This is a deliberate simplification — a manifest was tried and
dropped because nothing downstream ever consumed it, and Bronze ingestion is
already idempotent regardless (`replaceWhere` per extract-date partition —
see `infrastructure/delta_bronze_writer.py`), so a missed or duplicated event
around a restart is harmless. The poll loop wraps each listing in a
try/except so a transient MinIO error (a connection reset, a timeout) is
logged and skipped rather than silently killing the trigger coroutine —
without a coroutine emitting new-file events, the Bronze DAG that depends on
it would simply stop scheduling, with nothing visibly failing.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from typing import Any

from airflow.triggers.base import BaseEventTrigger, TriggerEvent

from infrastructure.s3_object_lister import list_keys

logger = logging.getLogger(__name__)

DEFAULT_POKE_INTERVAL_SECONDS = 30.0


class S3NewObjectTrigger(BaseEventTrigger):
    """Fires a `TriggerEvent` for every object key that newly appears under a prefix.

    Args:
        bucket: S3/MinIO bucket to watch, e.g. `Buckets.RAW_INCOMING_DATA`.
        prefix: Key prefix to watch, e.g. `"orders/"`.
        poke_interval: Seconds between listings.
    """

    def __init__(
        self,
        *,
        bucket: str,
        prefix: str,
        poke_interval: float = DEFAULT_POKE_INTERVAL_SECONDS,
    ) -> None:
        super().__init__()
        self.bucket = bucket
        self.prefix = prefix
        self.poke_interval = poke_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serializes constructor kwargs so the triggerer can recreate this trigger."""
        return (
            "infrastructure.s3_new_object_trigger.S3NewObjectTrigger",
            {"bucket": self.bucket, "prefix": self.prefix, "poke_interval": self.poke_interval},
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Seeds from a live listing, then polls forever, yielding one event per new key.

        `list_keys` is plain boto3 (synchronous), so it runs in a worker
        thread via `asyncio.to_thread` rather than blocking the triggerer's
        event loop, which serves many other triggers concurrently.

        The seed listing is not turned into events: it establishes the
        baseline of keys already present when this run of the trigger
        starts. Every subsequent poll diffs the current listing against
        that (continuously growing) baseline and yields an event per key
        that is new since the last poll.

        A transient error from `list_keys` during the poll loop (e.g. a
        MinIO connection reset or timeout) is logged and the poll is
        skipped rather than allowed to propagate, so the coroutine keeps
        running and picks the listing back up on the next interval.
        """
        seen = set(await asyncio.to_thread(list_keys, self.bucket, self.prefix))

        while True:
            await asyncio.sleep(self.poke_interval)
            try:
                current = set(await asyncio.to_thread(list_keys, self.bucket, self.prefix))
                new_keys = sorted(current - seen)
                seen |= current
                for key in new_keys:
                    yield TriggerEvent({"bucket": self.bucket, "key": key})
            except Exception:
                logger.exception(
                    "S3NewObjectTrigger poll failed for bucket=%s prefix=%s; "
                    "retrying next interval.",
                    self.bucket,
                    self.prefix,
                )
                continue
