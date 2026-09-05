"""Persists an S3/MinIO "seen keys" snapshot and per-poll diffs to durable storage.

Built for low-volume, human-readable control-plane metadata, not
high-throughput payloads: `S3NewObjectTrigger` keeps its "already seen"
set in the triggerer process's memory (see that module's docstring for
why), which does not survive a triggerer restart. This module gives
that in-memory state a durable, replayable trail in object storage —
a manifest of every key seen so far, and one diff file per poll that
found new keys — so a human (or a future recovery job) can reconstruct
what the trigger saw and when, without needing Airflow's metadata DB.

Writing here is best-effort: a manifest or diff write failure is logged
but never raised, because losing this side-channel record must not
crash the triggerer or block event delivery.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from infrastructure.s3_metadata_writer import write_json
from infrastructure.s3_object_lister import list_keys

logger = logging.getLogger(__name__)


def persist_s3_keys_snapshot(
    bucket: str,
    prefix: str,
    seen: set[str],
) -> tuple[set[str], list[str]]:
    """Diffs the current keys under a prefix against a seen-keys set and persists both.

    Always writes a manifest of every key seen so far (even when there are
    no new keys, so its `updated_at` timestamp stays fresh for replay), and
    additionally writes a diff file when new keys are found. Each write is
    wrapped in its own try/except so a manifest failure never blocks the
    diff attempt or vice versa, and neither can raise out of this function.

    Args:
        bucket: S3/MinIO bucket to list, e.g. `Buckets.RAW_INCOMING_DATA`.
        prefix: Key prefix to list, e.g. `"orders/"`.
        seen: Keys already observed on a prior call. Mutated in place
            (via `|=`) to include the newly observed keys, then returned.

    Returns:
        A tuple of `(seen, new_keys)`: `seen` is the same set object
        passed in, updated to include every currently-listed key;
        `new_keys` is the sorted list of keys present now but absent
        from `seen` before this call.
    """
    current = set(list_keys(bucket, prefix))
    new_keys = sorted(current - seen)
    seen |= current
    updated_at = datetime.now(timezone.utc).isoformat()

    try:
        manifest_key = f"_meta/s3_key_persister/{bucket}/{prefix}manifest.json"
        write_json(
            bucket,
            manifest_key,
            {"seen_keys": sorted(seen), "updated_at": updated_at},
        )
    except Exception:
        logger.exception(
            "Failed to write s3_key_persister manifest for bucket=%s prefix=%s",
            bucket,
            prefix,
        )

    if new_keys:
        try:
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            diff_key = f"_meta/s3_key_persister/{bucket}/{prefix}diffs/{timestamp}.json"
            write_json(
                bucket,
                diff_key,
                {"new_keys": new_keys, "polled_at": updated_at},
            )
        except Exception:
            logger.exception(
                "Failed to write s3_key_persister diff for bucket=%s prefix=%s",
                bucket,
                prefix,
            )

    return seen, new_keys
