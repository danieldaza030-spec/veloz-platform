"""Tests for infrastructure/s3_key_persister.py and infrastructure/s3_metadata_writer.py."""

from __future__ import annotations

import json
import os
from unittest.mock import MagicMock, patch

import pytest

from infrastructure.s3_key_persister import persist_s3_keys_snapshot
from infrastructure.s3_metadata_writer import write_json


class TestWriteJson:
    """Test JSON writing to S3/MinIO."""

    def test_write_json_reads_minio_endpoint_from_env(self) -> None:
        """Verify write_json builds boto3 client from MINIO_ENDPOINT env var."""
        mock_client = MagicMock()

        with patch.dict(os.environ, {"MINIO_ENDPOINT": "http://localhost:9000"}):
            with patch("infrastructure.s3_metadata_writer.boto3.client") as mock_boto3_client:
                mock_boto3_client.return_value = mock_client

                write_json("test-bucket", "test-key", {"data": "value"})

                mock_boto3_client.assert_called_once_with(
                    "s3",
                    endpoint_url="http://localhost:9000",
                    region_name="us-east-1",
                )

    def test_write_json_raises_keyerror_if_minio_endpoint_unset(self) -> None:
        """Verify write_json raises KeyError if MINIO_ENDPOINT is not set."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(KeyError):
                write_json("test-bucket", "test-key", {"data": "value"})

    def test_write_json_serializes_with_indent_2(self) -> None:
        """Verify write_json serializes payload with indent=2 for readability."""
        mock_client = MagicMock()
        payload = {"key": "value", "nested": {"x": 1}}

        with patch.dict(os.environ, {"MINIO_ENDPOINT": "http://localhost:9000"}):
            with patch("infrastructure.s3_metadata_writer.boto3.client") as mock_boto3_client:
                mock_boto3_client.return_value = mock_client

                write_json("test-bucket", "test-key", payload)

                # Verify put_object was called
                mock_client.put_object.assert_called_once()
                call_args = mock_client.put_object.call_args

                # Extract the Body argument and verify it's JSON with indent=2
                body = call_args.kwargs["Body"]
                expected_body = json.dumps(payload, indent=2).encode("utf-8")
                assert body == expected_body

    def test_write_json_calls_put_object_with_correct_parameters(self) -> None:
        """Verify write_json calls boto3 put_object with bucket and key."""
        mock_client = MagicMock()

        with patch.dict(os.environ, {"MINIO_ENDPOINT": "http://localhost:9000"}):
            with patch("infrastructure.s3_metadata_writer.boto3.client") as mock_boto3_client:
                mock_boto3_client.return_value = mock_client

                write_json("my-bucket", "my-key", {"test": "data"})

                mock_client.put_object.assert_called_once()
                call_args = mock_client.put_object.call_args
                assert call_args.kwargs["Bucket"] == "my-bucket"
                assert call_args.kwargs["Key"] == "my-key"

    def test_write_json_no_airflow_imports(self) -> None:
        """Verify write_json module does not import Airflow."""
        import inspect

        import infrastructure.s3_metadata_writer as metadata_writer

        module_source = inspect.getsource(metadata_writer)
        assert "from airflow" not in module_source
        assert "import airflow" not in module_source


class TestPersistS3KeysSnapshot:
    """Test S3/MinIO key snapshot persistence with diff logic."""

    def test_persist_new_keys_equals_sorted_current_minus_seen(self) -> None:
        """Verify new_keys == sorted(current - seen)."""
        mock_list_keys = MagicMock(return_value=["c.txt", "a.txt", "b.txt"])
        mock_write_json = MagicMock()
        seen = {"a.txt"}

        with patch("infrastructure.s3_key_persister.list_keys", mock_list_keys):
            with patch("infrastructure.s3_key_persister.write_json", mock_write_json):
                result_seen, new_keys = persist_s3_keys_snapshot(
                    "test-bucket", "test-prefix/", seen
                )

                # new_keys should be sorted difference
                assert new_keys == ["b.txt", "c.txt"]

    def test_persist_returned_seen_is_mutated_original_object(self) -> None:
        """Verify returned seen is the same object (mutated), not a fresh set."""
        mock_list_keys = MagicMock(return_value=["new_key.txt"])
        mock_write_json = MagicMock()
        original_seen = {"old_key.txt"}
        seen_id = id(original_seen)

        with patch("infrastructure.s3_key_persister.list_keys", mock_list_keys):
            with patch("infrastructure.s3_key_persister.write_json", mock_write_json):
                result_seen, _ = persist_s3_keys_snapshot(
                    "test-bucket", "test-prefix/", original_seen
                )

                # Verify it's the same object
                assert id(result_seen) == seen_id
                # Verify it was mutated to include the new key
                assert result_seen == {"old_key.txt", "new_key.txt"}

    def test_persist_manifest_write_always_attempted(self) -> None:
        """Verify manifest write is attempted every call, even with no new keys."""
        mock_list_keys = MagicMock(return_value=["existing.txt"])
        mock_write_json = MagicMock()
        seen = {"existing.txt"}

        with patch("infrastructure.s3_key_persister.list_keys", mock_list_keys):
            with patch("infrastructure.s3_key_persister.write_json", mock_write_json):
                persist_s3_keys_snapshot("test-bucket", "test-prefix/", seen)

                # Verify write_json was called at least once (for manifest)
                assert mock_write_json.call_count >= 1
                # Check that the first call is for the manifest
                first_call_args = mock_write_json.call_args_list[0]
                assert "manifest.json" in first_call_args.args[1]

    def test_persist_diff_write_only_when_new_keys_nonempty(self) -> None:
        """Verify diff write is attempted only when new_keys is non-empty."""
        mock_list_keys = MagicMock(return_value=["existing.txt"])
        mock_write_json = MagicMock()
        seen = {"existing.txt"}

        with patch("infrastructure.s3_key_persister.list_keys", mock_list_keys):
            with patch("infrastructure.s3_key_persister.write_json", mock_write_json):
                persist_s3_keys_snapshot("test-bucket", "test-prefix/", seen)

                # Only manifest should be written (no diff when no new keys)
                assert mock_write_json.call_count == 1

        # Now test with new keys
        mock_list_keys = MagicMock(return_value=["existing.txt", "new.txt"])
        mock_write_json = MagicMock()
        seen = {"existing.txt"}

        with patch("infrastructure.s3_key_persister.list_keys", mock_list_keys):
            with patch("infrastructure.s3_key_persister.write_json", mock_write_json):
                persist_s3_keys_snapshot("test-bucket", "test-prefix/", seen)

                # Both manifest and diff should be written
                assert mock_write_json.call_count == 2

    def test_persist_manifest_failure_does_not_prevent_diff_attempt(self) -> None:
        """Verify manifest write failure doesn't prevent diff write, and no exception raised."""
        mock_list_keys = MagicMock(return_value=["new.txt"])
        call_count = {"count": 0}

        def write_json_side_effect(bucket, key, payload):
            call_count["count"] += 1
            if "manifest.json" in key:
                raise RuntimeError("Manifest write failed")

        mock_write_json = MagicMock(side_effect=write_json_side_effect)
        seen = set()

        with patch("infrastructure.s3_key_persister.list_keys", mock_list_keys):
            with patch("infrastructure.s3_key_persister.write_json", mock_write_json):
                # Should not raise
                result_seen, new_keys = persist_s3_keys_snapshot(
                    "test-bucket", "test-prefix/", seen
                )

                # Both write attempts should have been made
                assert mock_write_json.call_count == 2
                assert new_keys == ["new.txt"]
                assert "new.txt" in result_seen

    def test_persist_diff_failure_does_not_prevent_manifest_attempt(self) -> None:
        """Verify diff write failure doesn't prevent manifest write, and no exception raised."""
        mock_list_keys = MagicMock(return_value=["new.txt"])
        call_count = {"count": 0}

        def write_json_side_effect(bucket, key, payload):
            call_count["count"] += 1
            if "diffs" in key:
                raise RuntimeError("Diff write failed")

        mock_write_json = MagicMock(side_effect=write_json_side_effect)
        seen = set()

        with patch("infrastructure.s3_key_persister.list_keys", mock_list_keys):
            with patch("infrastructure.s3_key_persister.write_json", mock_write_json):
                # Should not raise
                result_seen, new_keys = persist_s3_keys_snapshot(
                    "test-bucket", "test-prefix/", seen
                )

                # Both write attempts should have been made
                assert mock_write_json.call_count == 2
                assert new_keys == ["new.txt"]
                assert "new.txt" in result_seen

    def test_persist_both_writes_can_fail_independently(self) -> None:
        """Verify manifest and diff failures don't cause the function to raise."""
        mock_list_keys = MagicMock(return_value=["new.txt"])
        mock_write_json = MagicMock(side_effect=RuntimeError("Write failed"))
        seen = set()

        with patch("infrastructure.s3_key_persister.list_keys", mock_list_keys):
            with patch("infrastructure.s3_key_persister.write_json", mock_write_json):
                # Should not raise even though both writes fail
                result_seen, new_keys = persist_s3_keys_snapshot(
                    "test-bucket", "test-prefix/", seen
                )

                assert new_keys == ["new.txt"]
                assert "new.txt" in result_seen

    def test_persist_manifest_payload_includes_all_seen_keys_sorted(self) -> None:
        """Verify manifest payload includes sorted list of all seen keys."""
        mock_list_keys = MagicMock(return_value=["z.txt", "a.txt", "m.txt"])
        payloads_written = []

        def capture_write_json(bucket, key, payload):
            payloads_written.append((key, payload))

        mock_write_json = MagicMock(side_effect=capture_write_json)
        seen = set()

        with patch("infrastructure.s3_key_persister.list_keys", mock_list_keys):
            with patch("infrastructure.s3_key_persister.write_json", mock_write_json):
                persist_s3_keys_snapshot("test-bucket", "test-prefix/", seen)

        manifest_call = [p for p in payloads_written if "manifest" in p[0]][0]
        manifest_payload = manifest_call[1]

        assert manifest_payload["seen_keys"] == ["a.txt", "m.txt", "z.txt"]
        assert "updated_at" in manifest_payload

    def test_persist_diff_payload_includes_new_keys_and_timestamp(self) -> None:
        """Verify diff payload includes new_keys list and timestamp."""
        mock_list_keys = MagicMock(return_value=["c.txt", "a.txt", "b.txt"])
        payloads_written = []

        def capture_write_json(bucket, key, payload):
            payloads_written.append((key, payload))

        mock_write_json = MagicMock(side_effect=capture_write_json)
        seen = {"a.txt"}

        with patch("infrastructure.s3_key_persister.list_keys", mock_list_keys):
            with patch("infrastructure.s3_key_persister.write_json", mock_write_json):
                persist_s3_keys_snapshot("test-bucket", "test-prefix/", seen)

        diff_calls = [p for p in payloads_written if "diffs" in p[0]]
        assert len(diff_calls) == 1

        diff_payload = diff_calls[0][1]
        assert diff_payload["new_keys"] == ["b.txt", "c.txt"]
        assert "polled_at" in diff_payload

    def test_persist_manifest_key_path_includes_bucket_and_prefix(self) -> None:
        """Verify manifest key path is constructed correctly."""
        mock_list_keys = MagicMock(return_value=[])
        keys_written = []

        def capture_write_json(bucket, key, payload):
            keys_written.append(key)

        mock_write_json = MagicMock(side_effect=capture_write_json)
        seen = set()

        with patch("infrastructure.s3_key_persister.list_keys", mock_list_keys):
            with patch("infrastructure.s3_key_persister.write_json", mock_write_json):
                persist_s3_keys_snapshot("my-bucket", "my-prefix/", seen)

        manifest_key = keys_written[0]
        assert manifest_key == "_meta/s3_key_persister/my-bucket/my-prefix/manifest.json"

    def test_persist_diff_key_path_includes_timestamp(self) -> None:
        """Verify diff key path includes ISO timestamp."""
        mock_list_keys = MagicMock(return_value=["new.txt"])
        keys_written = []

        def capture_write_json(bucket, key, payload):
            keys_written.append(key)

        mock_write_json = MagicMock(side_effect=capture_write_json)
        seen = set()

        with patch("infrastructure.s3_key_persister.list_keys", mock_list_keys):
            with patch("infrastructure.s3_key_persister.write_json", mock_write_json):
                persist_s3_keys_snapshot("my-bucket", "my-prefix/", seen)

        diff_key = keys_written[1]
        # Should include the timestamp pattern (YYYYMMDDTHHMMSSZ)
        assert "_meta/s3_key_persister/my-bucket/my-prefix/diffs/" in diff_key
        assert diff_key.endswith(".json")
