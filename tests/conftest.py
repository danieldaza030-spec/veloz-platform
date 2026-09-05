"""Shared test fixtures for unit/integration tests."""

from __future__ import annotations

import ast
import sys
import tempfile
import types
from pathlib import Path

import pytest
from pyspark.sql import SparkSession


# Workaround for Python 3.14 + PySpark 3.5.3 cloudpickle recursion issue
# Increase the recursion limit to avoid stack overflow during serialization
sys.setrecursionlimit(10000)


def _install_airflow_triggers_base_stub() -> None:
    """Injects a minimal `airflow.triggers.base` stub if Airflow is not installed.

    This environment has no Airflow install, so modules like
    `infrastructure.s3_new_object_trigger` (which subclasses
    `airflow.triggers.base.BaseEventTrigger`) cannot be imported, and their
    runtime behavior has only ever been exercised via `py_compile`/AST
    checks rather than real tests. This stub provides just enough of the
    real interface — a no-op base class and a payload-carrying event class —
    for those modules to import and run under test.

    Runs once at collection time, before any test module imports Airflow,
    and only when a real Airflow install is unavailable: if `airflow.
    triggers.base` imports successfully, this is a no-op, so a CI
    environment with real Airflow exercises the genuine base class instead
    of a shadowed stub.
    """
    try:
        import airflow.triggers.base  # noqa: F401

        return
    except ImportError:
        pass

    class BaseEventTrigger:
        """Stand-in for Airflow's real `BaseEventTrigger`; no-op base class."""

        def __init__(self, *args: object, **kwargs: object) -> None:
            pass

    class TriggerEvent:
        """Stand-in for Airflow's real `TriggerEvent`; carries a payload."""

        def __init__(self, payload: object) -> None:
            self.payload = payload

        def __eq__(self, other: object) -> bool:
            return isinstance(other, TriggerEvent) and other.payload == self.payload

        def __repr__(self) -> str:
            return f"TriggerEvent<{self.payload!r}>"

    airflow_module = types.ModuleType("airflow")
    triggers_module = types.ModuleType("airflow.triggers")
    triggers_base_module = types.ModuleType("airflow.triggers.base")
    triggers_base_module.BaseEventTrigger = BaseEventTrigger
    triggers_base_module.TriggerEvent = TriggerEvent
    triggers_module.base = triggers_base_module
    airflow_module.triggers = triggers_module

    sys.modules.setdefault("airflow", airflow_module)
    sys.modules.setdefault("airflow.triggers", triggers_module)
    sys.modules.setdefault("airflow.triggers.base", triggers_base_module)


_install_airflow_triggers_base_stub()


def _exec_dag_pure_symbols(dag_path: Path, names: set[str], namespace: dict) -> dict:
    """Execs selected top-level symbols out of a DAG module's source, without importing it.

    DAG modules under `dags/` import `airflow.sdk`, which requires a real
    Airflow install this test environment does not have (see
    `_install_airflow_triggers_base_stub` above for the same problem one
    level down, at `airflow.triggers.base`). Rather than stubbing the much
    larger `airflow.sdk` surface, pure, Airflow-independent top-level
    constants/functions defined in a DAG module (no dependency on Airflow,
    Spark, or any DAG-scoped object) are unit-tested by parsing the
    module's source with `ast` and exec'ing only the requested top-level
    `Assign`/`FunctionDef` nodes into an isolated namespace, leaving every
    Airflow-dependent line of the module untouched and unimported.

    Args:
        dag_path: Path to the DAG module's source file.
        names: Names of the top-level assignment targets or function
            definitions to pull out of the module.
        namespace: Namespace to exec the selected nodes into; also supplies
            any names those nodes need at exec time (e.g. `re`).

    Returns:
        `namespace`, mutated in place with the requested names now bound.
    """
    tree = ast.parse(dag_path.read_text())
    wanted = [
        node
        for node in tree.body
        if (isinstance(node, ast.FunctionDef) and node.name in names)
        or (
            isinstance(node, ast.Assign)
            and any(isinstance(target, ast.Name) and target.id in names for target in node.targets)
        )
    ]
    module = ast.Module(body=wanted, type_ignores=[])
    exec(compile(module, filename=str(dag_path), mode="exec"), namespace)  # noqa: S102
    return namespace


@pytest.fixture
def load_dag_pure_symbols():
    """Returns a loader for pure, Airflow-independent symbols defined in a `dags/*.py` module.

    See `_exec_dag_pure_symbols` for why this avoids importing the DAG
    module directly.
    """

    def _load(dag_filename: str, names: set[str], extra_globals: dict | None = None) -> dict:
        dag_path = Path(__file__).parent.parent / "dags" / dag_filename
        namespace = dict(extra_globals or {})
        return _exec_dag_pure_symbols(dag_path, names, namespace)

    return _load


@pytest.fixture
def spark_session() -> SparkSession:
    """Build a local Spark session with Delta enabled for testing.

    Uses `local[1]` mode for isolated test execution. Manually configures
    Delta extensions since configure_spark_with_delta_pip has issues in
    test environments.
    """
    try:
        # Try to use configure_spark_with_delta_pip with manual extension config
        from delta import configure_spark_with_delta_pip

        builder = (
            SparkSession.builder
            .appName("veloz-test")
            .master("local[1]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
    except Exception as e:
        # Fallback: manual configuration
        builder = (
            SparkSession.builder
            .appName("veloz-test")
            .master("local[1]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )
        try:
            spark = builder.getOrCreate()
        except Exception as fallback_e:
            print(f"Warning: Delta configuration failed: {e}, {fallback_e}")
            # Last resort: plain Spark
            spark = (
                SparkSession.builder
                .appName("veloz-test")
                .master("local[1]")
                .getOrCreate()
            )

    yield spark
    spark.stop()


@pytest.fixture
def temp_delta_path() -> str:
    """Provide a temporary directory for Delta table writes.

    Each test gets a fresh temp path to avoid cross-test pollution.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        yield tmpdir
