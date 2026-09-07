"""Cross-checks `metadata.ingestion_windows.WINDOW_MINUTES` against the generator it mirrors.

`metadata.ingestion_windows` deliberately does not import `generators.
s3_io.WINDOW_MINUTES` directly (see that module's docstring: `generators/`
is mounted into the Airflow containers as a subprocess-only tree, outside
`plugins/`, and invoked by subprocess rather than imported as a package
from DAG code). This test is the safety net for that choice: it runs in
the plain pytest process (repo root on `sys.path`, not inside an Airflow
container), where both modules import cleanly, and fails loudly the moment
the platform's assumed cadence and the generators' actual cadence
diverge -- exactly the drift that would otherwise let a
`mode="ingestion_window"` manual Silver rerun silently generate window
tokens that never existed in Bronze and silently skip data.
"""

from __future__ import annotations

from generators.s3_io import WINDOW_MINUTES as GENERATOR_WINDOW_MINUTES
from metadata.ingestion_windows import WINDOW_MINUTES as PLATFORM_WINDOW_MINUTES


def test_platform_window_minutes_matches_generator_window_minutes() -> None:
    """The platform's shared cadence assumption must equal the generators' actual cadence."""
    assert PLATFORM_WINDOW_MINUTES == GENERATOR_WINDOW_MINUTES
