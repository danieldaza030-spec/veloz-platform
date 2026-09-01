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

Trade-off accepted deliberately for this scope (G5, one-person team, no extra
infra): the "already seen" set lives in the triggerer process's memory, not in
Airflow's metadata DB. A triggerer restart re-baselines to whatever keys exist
at that moment, so a key that landed in the narrow window between the restart
and its next poll could be silently absorbed into the new baseline instead of
firing an event. Bronze ingestion is unaffected either way in the common case
(idempotent `replaceWhere` per extract-date partition — see
`dags/ingest_orders_bronze.py`), and a missed trigger is still recoverable by
a manual `airflow dags trigger`.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

from airflow.triggers.base import BaseEventTrigger, TriggerEvent

from infrastructure.s3_object_lister import list_keys

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
        """Polls the prefix forever, yielding one event per newly observed key.

        `list_keys` is plain boto3 (synchronous), so it runs in a worker thread
        via `asyncio.to_thread` rather than blocking the triggerer's event loop,
        which serves many other triggers concurrently.
        """
        seen = set(await asyncio.to_thread(list_keys, self.bucket, self.prefix))
        while True:
            await asyncio.sleep(self.poke_interval)
            current = set(await asyncio.to_thread(list_keys, self.bucket, self.prefix))
            new_keys = sorted(current - seen)
            for key in new_keys:
                yield TriggerEvent({"bucket": self.bucket, "key": key})
            seen |= current
