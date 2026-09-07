"""Shared ingestion-window cadence assumption for the orders/rider-events pipeline.

`WINDOW_MINUTES` mirrors `generators/s3_io.py`'s own `WINDOW_MINUTES` -- the
actual cadence the emulated upstream `orders`/`rider_events` generators run
at (one window-file landed every `WINDOW_MINUTES` minutes under a
`date=<date>/` partition). It is a separate constant, not an import of
`generators.s3_io.WINDOW_MINUTES`, because `generators/` is mounted into the
Airflow containers as a subprocess-only tree (`./generators:/opt/airflow/
generators` in `docker-compose.yml`, outside `plugins/`) and invoked via
subprocess by `dags.generate_orders_and_rider_events._run_generator`, not
importable as a package from DAG code running inside those containers.

This module exists so every *platform-side* consumer of that cadence reads
one shared constant instead of each redeclaring its own copy that can
silently drift from the others -- previously `dags.ingest_orders_silver`
carried its own private `WINDOW_MINUTES = 5`, duplicating the same
assumption `dags.ingest_orders_bronze`'s `LOOKBACK_WINDOW_COUNT` comment
already documented in prose. A cadence change on the platform side that
isn't applied everywhere is exactly the failure mode this hoist closes: a
`mode="ingestion_window"` manual Silver rerun (`dags.ingest_orders_silver.
_enumerate_windows`) would otherwise silently generate window tokens at the
*old* cadence, which never existed as `_ingestion_window` values in Bronze,
and silently skip the data in between.

`tests/test_ingestion_windows_cadence.py` cross-checks this constant
against `generators.s3_io.WINDOW_MINUTES` directly -- importable in the
test process (run from the repo root), unlike the deployed Airflow
containers -- so a cadence change on either side that isn't mirrored on the
other fails the suite loudly instead of drifting unnoticed.
"""

from __future__ import annotations

WINDOW_MINUTES = 5
"""Minutes-wide cadence of each `_ingestion_window` token emitted by the
`orders`/`rider_events` upstream generators. See module docstring for why
this is a mirrored constant, not a direct import of `generators.s3_io.
WINDOW_MINUTES`."""
