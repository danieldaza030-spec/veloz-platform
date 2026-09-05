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

The "already seen" set is manifest-seeded, not baselined blind: `run()` reads
the existing manifest via `read_manifest_seen_keys` before entering the poll
loop, so a triggerer restart resumes from what a prior run already recorded
instead of silently absorbing whatever keys exist at that moment into a fresh
baseline. Only when no valid manifest exists (first run, the manifest is
missing/corrupt/malformed, or the read itself raises an unexpected error)
does the trigger fall back to firing one event per currently-listed key, on
the reasoning that a duplicate event is cheap and a missed one is not — and
that an unhandled exception from the seed read would yield zero events,
which is strictly worse. Each poll persists the set — and any newly observed
keys —
to object storage via `persist_seen_keys` (manifest + per-poll diff,
best-effort, never raises) *after* yielding events for that poll's new keys,
so a crash between listing and persisting can at worst cause a key to be
refired on the next restart, never dropped. The residual gap is that listing,
yielding, and persisting are not one atomic operation: a crash after events
are yielded but before the manifest write lands means the manifest doesn't
yet reflect those keys, so the next restart's manifest read is missing them
and will refire them once more. Bronze ingestion is unaffected either way in
the common case (idempotent `replaceWhere` per extract-date partition — see
`dags/ingest_orders_bronze.py`), and a missed trigger is still recoverable by
a manual `airflow dags trigger`.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from typing import Any

from airflow.triggers.base import BaseEventTrigger, TriggerEvent

from infrastructure.s3_key_persister import (
    compute_seen_diff,
    persist_seen_keys,
    read_manifest_seen_keys,
    validate_watch_prefix,
)

logger = logging.getLogger(__name__)

DEFAULT_POKE_INTERVAL_SECONDS = 30.0


class S3NewObjectTrigger(BaseEventTrigger):
    """Fires a `TriggerEvent` for every object key that newly appears under a prefix.

    Args:
        bucket: S3/MinIO bucket to watch, e.g. `Buckets.RAW_INCOMING_DATA`.
        prefix: Key prefix to watch, e.g. `"orders/"`. Must be non-empty
            and must not overlap `s3_key_persister`'s own metadata tree
            (`META_PREFIX_ROOT`).
        poke_interval: Seconds between listings.

    Raises:
        ValueError: If `prefix` is empty, or overlaps
            `s3_key_persister`'s metadata tree (see
            `validate_watch_prefix`). This trigger writes its manifest
            into the same bucket it watches, so a prefix covering (or
            covered by) that tree would make the trigger observe its
            own manifest/diff writes as new keys — a self-reinforcing
            loop.
    """

    def __init__(
        self,
        *,
        bucket: str,
        prefix: str,
        poke_interval: float = DEFAULT_POKE_INTERVAL_SECONDS,
    ) -> None:
        super().__init__()
        validate_watch_prefix(prefix)
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
        """Seeds from the manifest, then polls forever, yielding events before persisting.

        `compute_seen_diff`/`persist_seen_keys`/`read_manifest_seen_keys` are
        plain boto3 (synchronous), so each runs in a worker thread via
        `asyncio.to_thread` rather than blocking the triggerer's event loop,
        which serves many other triggers concurrently.

        Seeding tries `read_manifest_seen_keys` first so a restart resumes
        from what a prior run already recorded. If no valid manifest exists
        (first run, missing/corrupt/malformed, or the read itself raises an
        unexpected error — e.g. a boto3 connection/timeout error, or a
        `KeyError` from an unset `MINIO_ENDPOINT`), every currently-listed key
        is treated as new and fired once via `compute_seen_diff(bucket,
        prefix, set())`, since a duplicate event is cheap and a missed one is
        not — firing zero events because the seed read blew up would be
        strictly worse than that fallback.

        Each poll computes the diff and yields events for it *before*
        persisting the updated manifest, so a persistence failure can't mark
        a key durably seen that never actually fired.
        """
        try:
            seen = await asyncio.to_thread(read_manifest_seen_keys, self.bucket, self.prefix)
        except Exception:
            logger.exception(
                "Unexpected failure reading s3_key_persister manifest for "
                "bucket=%s prefix=%s; falling back to firing every "
                "currently-listed key.",
                self.bucket,
                self.prefix,
            )
            seen = None

        if seen is None:
            seen, new_keys = await asyncio.to_thread(
                compute_seen_diff, self.bucket, self.prefix, set()
            )
            for key in new_keys:
                yield TriggerEvent({"bucket": self.bucket, "key": key})
            await asyncio.to_thread(persist_seen_keys, self.bucket, self.prefix, seen, new_keys)

        while True:
            await asyncio.sleep(self.poke_interval)
            seen, new_keys = await asyncio.to_thread(
                compute_seen_diff,
                self.bucket,
                self.prefix,
                seen,
            )
            for key in new_keys:
                yield TriggerEvent({"bucket": self.bucket, "key": key})
            await asyncio.to_thread(persist_seen_keys, self.bucket, self.prefix, seen, new_keys)
