"""Shared `default_args` for Bronze and Silver DAGs.

Centralizes the `owner`/`retries`/`retry_delay`/`execution_timeout`
values required on every DAG's `default_args` so individual DAGs don't
redeclare them inline. `BRONZE_DEFAULT_ARGS` covers the three Bronze
ingestion DAGs (orders, fulfillment, payments); `SILVER_DEFAULT_ARGS`
covers Silver's per-batch ingest DAG. Kept as two separate dicts (not one
renamed constant reused everywhere) because Bronze and Silver's per-batch
cost shapes, while numerically similar today, are conceptually distinct
budgets that should be free to diverge without one layer's tuning
silently changing the other's.
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

SILVER_DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 3,
    "retry_delay": pendulum.duration(minutes=5),
    # 30 minutes covers one bounded per-batch MERGE (dags.ingest_orders_silver):
    # ~17k orders per 5-minute window at the 5,000,000-orders/day design
    # target (5,000,000 / (1440 / 5) ~= 17,361), the same per-batch cost
    # envelope BRONZE_DEFAULT_ARGS already budgets for a Bronze ingest run.
    "execution_timeout": pendulum.duration(minutes=30),
}
"""Default `default_args` dict for Silver DAGs doing bounded, per-batch work.

`dags.maintain_orders_silver` -- a full-table OPTIMIZE+ZORDER+VACUUM, not a
bounded batch -- must NOT reuse this dict's `execution_timeout` as-is: see
that DAG's own `default_args` for why it overrides it with a materially
longer value. Individual DAGs may override any other key the same way,
e.g. `{**SILVER_DEFAULT_ARGS, "retries": 5}`.
"""
