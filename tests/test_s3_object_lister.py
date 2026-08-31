"""Tests for infrastructure/s3_object_lister.py."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from infrastructure.s3_object_lister import list_keys


class TestListKeys:
    """Test S3/MinIO object listing."""

    def test_list_keys_multiple_keys_returns_sorted(self) -> None:
        """Verify multiple keys under a prefix are returned sorted."""
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [
            {
                "Contents": [
                    {"Key": "fulfillment/2026-08-30/store_003.csv"},
                    {"Key": "fulfillment/2026-08-30/store_001.csv"},
                    {"Key": "fulfillment/2026-08-30/store_002.csv"},
                ]
            }
        ]

        mock_client = MagicMock()
        mock_client.get_paginator.return_value = mock_paginator

        with patch("infrastructure.s3_object_lister._get_s3_client") as mock_get_client:
            mock_get_client.return_value = mock_client

            result = list_keys("raw-incoming-data", "fulfillment/2026-08-30/")

            assert result == [
                "fulfillment/2026-08-30/store_001.csv",
                "fulfillment/2026-08-30/store_002.csv",
                "fulfillment/2026-08-30/store_003.csv",
            ]
            mock_client.get_paginator.assert_called_once_with("list_objects_v2")
            mock_paginator.paginate.assert_called_once_with(
                Bucket="raw-incoming-data",
                Prefix="fulfillment/2026-08-30/",
            )

    def test_list_keys_empty_prefix_returns_empty_list(self) -> None:
        """Verify empty prefix (no matching keys) returns [] without error."""
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [
            {"Contents": []}
        ]

        mock_client = MagicMock()
        mock_client.get_paginator.return_value = mock_paginator

        with patch("infrastructure.s3_object_lister._get_s3_client") as mock_get_client:
            mock_get_client.return_value = mock_client

            result = list_keys("raw-incoming-data", "nonexistent/prefix/")

            assert result == []

    def test_list_keys_pagination_collects_all_pages(self) -> None:
        """Verify pagination is exercised and all keys across pages are collected."""
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [
            {
                "Contents": [
                    {"Key": "orders/2026-08-30/orders_001.json"},
                    {"Key": "orders/2026-08-30/orders_002.json"},
                ]
            },
            {
                "Contents": [
                    {"Key": "orders/2026-08-30/orders_003.json"},
                    {"Key": "orders/2026-08-30/orders_004.json"},
                ]
            },
            {
                "Contents": [
                    {"Key": "orders/2026-08-30/orders_005.json"},
                ]
            },
        ]

        mock_client = MagicMock()
        mock_client.get_paginator.return_value = mock_paginator

        with patch("infrastructure.s3_object_lister._get_s3_client") as mock_get_client:
            mock_get_client.return_value = mock_client

            result = list_keys("raw-incoming-data", "orders/2026-08-30/")

            assert len(result) == 5
            assert result == [
                "orders/2026-08-30/orders_001.json",
                "orders/2026-08-30/orders_002.json",
                "orders/2026-08-30/orders_003.json",
                "orders/2026-08-30/orders_004.json",
                "orders/2026-08-30/orders_005.json",
            ]

    def test_list_keys_pagination_mixed_empty_pages(self) -> None:
        """Verify pagination handles pages with no Contents gracefully."""
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [
            {
                "Contents": [
                    {"Key": "payments/2026-08-30/payment_001.csv"},
                ]
            },
            # Second page has no "Contents" key
            {},
            {
                "Contents": [
                    {"Key": "payments/2026-08-30/payment_002.csv"},
                ]
            },
        ]

        mock_client = MagicMock()
        mock_client.get_paginator.return_value = mock_paginator

        with patch("infrastructure.s3_object_lister._get_s3_client") as mock_get_client:
            mock_get_client.return_value = mock_client

            result = list_keys("raw-incoming-data", "payments/2026-08-30/")

            assert result == [
                "payments/2026-08-30/payment_001.csv",
                "payments/2026-08-30/payment_002.csv",
            ]

    def test_list_keys_no_contents_key_does_not_raise_error(self) -> None:
        """Verify a page without 'Contents' key doesn't raise KeyError."""
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [
            {
                # This page has no "Contents" key at all
            }
        ]

        mock_client = MagicMock()
        mock_client.get_paginator.return_value = mock_paginator

        with patch("infrastructure.s3_object_lister._get_s3_client") as mock_get_client:
            mock_get_client.return_value = mock_client

            # Should not raise KeyError
            result = list_keys("raw-incoming-data", "some-prefix/")

            assert result == []

    def test_list_keys_uses_list_objects_v2_paginator(self) -> None:
        """Verify list_keys uses the correct paginator type."""
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [
            {"Contents": [{"Key": "test.csv"}]}
        ]

        mock_client = MagicMock()
        mock_client.get_paginator.return_value = mock_paginator

        with patch("infrastructure.s3_object_lister._get_s3_client") as mock_get_client:
            mock_get_client.return_value = mock_client

            list_keys("bucket", "prefix/")

            # Verify the correct paginator was requested
            mock_client.get_paginator.assert_called_once_with("list_objects_v2")

    def test_list_keys_sorted_across_multiple_pages(self) -> None:
        """Verify keys are sorted even when they come from multiple pages."""
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [
            {
                "Contents": [
                    {"Key": "z_file.txt"},
                    {"Key": "a_file.txt"},
                ]
            },
            {
                "Contents": [
                    {"Key": "m_file.txt"},
                    {"Key": "b_file.txt"},
                ]
            },
        ]

        mock_client = MagicMock()
        mock_client.get_paginator.return_value = mock_paginator

        with patch("infrastructure.s3_object_lister._get_s3_client") as mock_get_client:
            mock_get_client.return_value = mock_client

            result = list_keys("bucket", "prefix/")

            # Should be sorted alphabetically
            assert result == [
                "a_file.txt",
                "b_file.txt",
                "m_file.txt",
                "z_file.txt",
            ]
