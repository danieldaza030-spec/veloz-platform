"""Shared `default_args` for Bronze ingestion DAGs.

Centralizes the `owner`/`retries`/`retry_delay`/`execution_timeout`
values required on every DAG's `default_args` so each Bronze ingestion
DAG (orders, fulfillment, payments) doesn't redeclare them inline.
"""

from __future__ import annotations

import pendulum

BRONZE_DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 3,
    "retry_delay": pendulum.duration(minutes=5),
    "execution_timeout": pendulum.duration(minutes=30),
}
"""Default `default_args` dict for Bronze ingestion DAGs.

Individual DAGs may override any key by merging their own dict on top,
e.g. `{**BRONZE_DEFAULT_ARGS, "retries": 5}`.
"""
