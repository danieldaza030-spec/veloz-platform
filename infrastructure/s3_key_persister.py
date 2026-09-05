"""Reads/writes S3/MinIO "seen keys" manifests and per-poll diffs to durable storage.

Built for low-volume, human-readable control-plane metadata, not
high-throughput payloads: `S3NewObjectTrigger` seeds its "already seen"
set from the manifest this module writes (see that module's docstring),
so a triggerer restart doesn't have to re-baseline blind to whatever
keys happen to exist at that moment. This module gives that state a
durable, replayable trail in object storage — a manifest of every key
seen so far, and one diff file per poll that found new keys — so a
human (or a future recovery job) can reconstruct what the trigger saw
and when, without needing Airflow's metadata DB.

The boto3 client here is built the same way `infrastructure/
s3_object_lister.py` builds one (same `MINIO_ENDPOINT` env var, same
reliance on boto3's default credential chain), reimplemented rather
than shared so each module stays a minimal, independently readable
adapter.

Writing here is best-effort: a manifest or diff write failure is logged
but never raised, because losing this side-channel record must not
crash the triggerer or block event delivery. Reading an existing
manifest is likewise best-effort when it feeds a write (a stale or
unreadable manifest must never block a new one from being written), but
`read_json`/`read_manifest_seen_keys` still expose the raw failure
distinctions to callers that need to tell "no manifest yet" apart from
"manifest is corrupt" (see `S3NewObjectTrigger`, which fires every
currently-listed key as new in both cases — duplicates are cheap,
missed events are not).
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

import boto3
from botocore.exceptions import ClientError

from infrastructure.s3_object_lister import list_keys

logger = logging.getLogger(__name__)

META_PREFIX_ROOT = "_meta/s3_key_persister/"
"""Key prefix under which this module's manifests and diffs live.

Exported so callers (notably `S3NewObjectTrigger`) can guard against
watching a bucket+prefix that overlaps this tree, which would make a
trigger observe its own metadata writes as new keys.
"""


def validate_watch_prefix(prefix: str) -> None:
    """Validates a prefix intended for a manifest-backed key watcher.

    Centralizes the two invariants `S3NewObjectTrigger` depends on, so
    they're defined and testable next to `META_PREFIX_ROOT` rather than
    duplicated in a module that also has to import Airflow.

    Args:
        prefix: Key prefix a caller intends to watch and persist a
            manifest for.

    Raises:
        ValueError: If `prefix` is empty, or overlaps
            `META_PREFIX_ROOT` in either direction (a prefix inside the
            metadata tree, or a prefix that is itself an ancestor of
            it). Either case would make a watcher observe its own
            manifest/diff writes as new keys — a self-reinforcing loop.
    """
    if not prefix:
        raise ValueError("prefix must be non-empty.")
    if prefix.startswith(META_PREFIX_ROOT) or META_PREFIX_ROOT.startswith(prefix):
        raise ValueError(
            f"prefix {prefix!r} overlaps the s3_key_persister metadata tree "
            f"({META_PREFIX_ROOT!r}); a watcher on it would observe its own "
            "manifest/diff writes as new keys."
        )


def _get_s3_client():
    """Builds a boto3 S3 client pointed at MinIO.

    Reads `MINIO_ENDPOINT` directly rather than defaulting it, matching
    `infrastructure/s3_object_lister.py`'s convention: every environment
    that actually runs this code (the Airflow containers) sets it
    explicitly. `region_name` is a required-but-functionally-unused
    placeholder for MinIO's S3-compatible API, which has no AWS regions.

    Returns:
        A boto3 S3 client configured for the MinIO endpoint.
    """
    endpoint_url = os.environ["MINIO_ENDPOINT"]
    return boto3.client("s3", endpoint_url=endpoint_url, region_name="us-east-1")


def write_json(bucket: str, key: str, payload: dict[str, Any]) -> None:
    """Writes a dict to a bucket+key as pretty-printed JSON.

    Args:
        bucket: Bucket to write to, e.g. `Buckets.RAW_INCOMING_DATA`.
        key: Full object key to write, e.g.
            `"_meta/s3_key_persister/raw-incoming-data/orders/manifest.json"`.
        payload: JSON-serializable dict to write. Serialized with
            `indent=2` for human readability, since this is
            control-plane metadata meant to be eyeballed in a bucket
            browser, not high-throughput payloads.
    """
    client = _get_s3_client()
    body = json.dumps(payload, indent=2).encode("utf-8")
    client.put_object(Bucket=bucket, Key=key, Body=body)


def read_json(bucket: str, key: str) -> dict[str, Any]:
    """Reads a bucket+key and parses its body as JSON.

    Args:
        bucket: Bucket to read from.
        key: Full object key to read.

    Returns:
        The parsed JSON payload.

    Raises:
        botocore.exceptions.ClientError: If the object does not exist
            (e.g. `NoSuchKey`) or the read otherwise fails at the
            S3/MinIO layer. Propagated rather than swallowed here so
            callers can tell "missing" apart from "corrupt" (see
            `read_manifest_seen_keys`).
        json.JSONDecodeError: If the object body is not valid JSON.
    """
    client = _get_s3_client()
    response = client.get_object(Bucket=bucket, Key=key)
    body = response["Body"].read()
    return json.loads(body)


def _normalize_prefix(prefix: str) -> str:
    """Ensures a key prefix ends with exactly one trailing slash.

    Without this, joining a bare prefix (e.g. `"orders"`, no trailing
    slash) directly against a filename produces a run-together key like
    `"...ordersmanifest.json"` instead of `"...orders/manifest.json"`.

    Args:
        prefix: Key prefix to normalize. Left as-is if empty or already
            slash-terminated.

    Returns:
        `prefix` with a trailing slash appended if it didn't already
        have one; unchanged if empty or already slash-terminated.
    """
    if not prefix or prefix.endswith("/"):
        return prefix
    return f"{prefix}/"


def _meta_root(bucket: str, prefix: str) -> str:
    """Builds the shared metadata-tree root for a bucket+prefix pair.

    Args:
        bucket: S3/MinIO bucket the metadata tracks.
        prefix: Key prefix the metadata tracks; normalized to end with
            a trailing slash (see `_normalize_prefix`).

    Returns:
        The metadata root, e.g.
        `"_meta/s3_key_persister/raw-incoming-data/orders/"`, under
        which the manifest and diff files for that bucket+prefix live.
    """
    return f"{META_PREFIX_ROOT}{bucket}/{_normalize_prefix(prefix)}"


def manifest_key(bucket: str, prefix: str) -> str:
    """Builds the manifest object key for a bucket+prefix pair.

    Args:
        bucket: S3/MinIO bucket the manifest tracks, e.g.
            `Buckets.RAW_INCOMING_DATA`.
        prefix: Key prefix the manifest tracks, e.g. `"orders/"`.

    Returns:
        The full manifest object key, e.g.
        `"_meta/s3_key_persister/raw-incoming-data/orders/manifest.json"`.
    """
    return f"{_meta_root(bucket, prefix)}manifest.json"


def read_manifest_seen_keys(bucket: str, prefix: str) -> set[str] | None:
    """Reads the manifest for a bucket+prefix and returns its seen-keys set.

    Args:
        bucket: S3/MinIO bucket the manifest tracks.
        prefix: Key prefix the manifest tracks.

    Returns:
        The set of keys recorded under the manifest's `seen_keys` field,
        or `None` if the manifest is missing, is not valid JSON, or does
        not match the expected schema (a dict with a `seen_keys` list of
        strings). Each of those three cases is logged with a distinct
        message so an operator can tell a first-run "no manifest yet"
        apart from a corrupted or malformed write.
    """
    key = manifest_key(bucket, prefix)
    try:
        payload = read_json(bucket, key)
    except ClientError:
        logger.info(
            "No existing s3_key_persister manifest at bucket=%s key=%s; "
            "treating this as a first observation.",
            bucket,
            key,
        )
        return None
    except json.JSONDecodeError:
        logger.warning(
            "s3_key_persister manifest at bucket=%s key=%s is not valid JSON; "
            "ignoring it.",
            bucket,
            key,
        )
        return None

    seen_keys = payload.get("seen_keys") if isinstance(payload, dict) else None
    if not isinstance(seen_keys, list) or not all(isinstance(item, str) for item in seen_keys):
        logger.warning(
            "s3_key_persister manifest at bucket=%s key=%s has an invalid or "
            "missing 'seen_keys' field; ignoring it.",
            bucket,
            key,
        )
        return None

    return set(seen_keys)


def _read_existing_manifest_seen_keys(bucket: str, prefix: str) -> set[str]:
    """Best-effort read of the existing manifest's seen keys, for merging.

    Isolated from the manifest write attempt so a read failure (missing,
    corrupt, bad schema, or anything else) can never block that write:
    worst case, the newly written manifest doesn't carry forward keys
    another writer or process recorded, until the next successful read.

    Args:
        bucket: S3/MinIO bucket the manifest tracks.
        prefix: Key prefix the manifest tracks.

    Returns:
        The existing manifest's seen keys, or an empty set if it is
        missing, invalid, or unreadable for any reason.
    """
    try:
        existing = read_manifest_seen_keys(bucket, prefix)
    except Exception:
        logger.exception(
            "Unexpected failure reading existing s3_key_persister manifest "
            "for bucket=%s prefix=%s; proceeding without merging it.",
            bucket,
            prefix,
        )
        return set()
    return existing if existing is not None else set()


def compute_seen_diff(
    bucket: str,
    prefix: str,
    seen: set[str],
) -> tuple[set[str], list[str]]:
    """Diffs the current keys under a prefix against a seen-keys set.

    Pure computation beyond the listing call itself: does not touch the
    manifest or diff files in object storage. Split out from
    `persist_s3_keys_snapshot` so a caller that must act on new keys
    before they're durably recorded — `S3NewObjectTrigger`, which has to
    yield `TriggerEvent`s before persisting so a persistence failure
    can't mark a key durably seen that never fired — can do so.

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
    return seen, new_keys


def persist_seen_keys(
    bucket: str,
    prefix: str,
    seen: set[str],
    new_keys: list[str],
) -> None:
    """Best-effort persistence of a seen-keys manifest and a new-keys diff.

    Always writes a manifest of every key seen so far (even when there
    are no new keys, so its `updated_at` timestamp stays fresh for
    replay), and additionally writes a diff file when `new_keys` is
    non-empty. The manifest write is read-merge-write: it best-effort
    reads whatever manifest is already there and unions its keys into
    `seen` before writing, so a manifest write never truncates history
    another writer previously recorded. Each step is wrapped in its own
    try/except so a read failure, a manifest write failure, or a diff
    write failure never blocks the others, and none of them can raise
    out of this function.

    Args:
        bucket: S3/MinIO bucket to persist metadata for, e.g.
            `Buckets.RAW_INCOMING_DATA`.
        prefix: Key prefix to persist metadata for, e.g. `"orders/"`.
        seen: The full seen-keys set to persist (already updated with
            the current listing by `compute_seen_diff`). Mutated in
            place to include any keys recovered from an existing
            manifest.
        new_keys: Keys to record in this poll's diff file, if any.
    """
    updated_at = datetime.now(timezone.utc).isoformat()
    seen |= _read_existing_manifest_seen_keys(bucket, prefix)

    try:
        write_json(
            bucket,
            manifest_key(bucket, prefix),
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
            diff_key = f"{_meta_root(bucket, prefix)}diffs/{timestamp}.json"
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


def persist_s3_keys_snapshot(
    bucket: str,
    prefix: str,
    seen: set[str],
) -> tuple[set[str], list[str]]:
    """Diffs the current keys under a prefix against a seen-keys set and persists both.

    Thin orchestration over `compute_seen_diff` (the non-writing diff)
    and `persist_seen_keys` (the best-effort manifest/diff write).
    Callers that need to act on `new_keys` before they're durably
    recorded should call those two functions directly instead, with
    their own logic in between.

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
    seen, new_keys = compute_seen_diff(bucket, prefix, seen)
    persist_seen_keys(bucket, prefix, seen, new_keys)
    return seen, new_keys
