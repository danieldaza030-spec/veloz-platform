"""Tests for dags/maintain_orders_silver.py.

Structural checks follow the AST-based pattern established by
`tests/test_ingest_orders_bronze_dag.py` (no real Airflow install in this
environment -- see `tests/conftest.py`'s `load_dag_pure_symbols`).
`_run_maintenance` itself is exercised against a real local Delta table,
built via a CSV-file round-trip (`spark.createDataFrame(list_of_dicts,
schema=...)` hits a Python 3.14 + PySpark 3.5.3 cloudpickle stack
overflow -- see `tests/test_delta_silver_merge_writer.py` for the same
workaround).
"""

from __future__ import annotations

import ast
import os
import tempfile
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import FloatType, StringType, StructField, StructType

DAG_FILENAME = "maintain_orders_silver.py"
DAG_PATH = Path(__file__).parent.parent / "dags" / DAG_FILENAME

_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), False),
        StructField("store_id", StringType(), False),
        StructField("order_total", FloatType(), False),
    ]
)

_ROWS = [
    {"order_id": "order-1", "store_id": "STORE-001", "order_total": 10.5},
    {"order_id": "order-2", "store_id": "STORE-001", "order_total": 20.0},
    {"order_id": "order-3", "store_id": "STORE-002", "order_total": 30.25},
]


def _build_table(spark: SparkSession, tmp_path: Path, path: str) -> DataFrame:
    """Builds a small Delta table via a CSV round-trip, sidestepping the cloudpickle issue."""
    field_names = [field.name for field in _SCHEMA.fields]
    lines = [",".join(field_names)]
    lines.extend(",".join(str(row[name]) for name in field_names) for row in _ROWS)
    fd, csv_path = tempfile.mkstemp(suffix=".csv", dir=tmp_path)
    os.close(fd)
    Path(csv_path).write_text("\n".join(lines) + "\n")
    df = spark.read.format("csv").schema(_SCHEMA).option("header", "true").load(csv_path)
    df.write.format("delta").save(path)
    return df


class TestMaintainOrdersSilverDAGStructure:
    """Structural checks: syntax, the DAG decorator, and its cron schedule."""

    def test_dag_file_syntax_valid(self) -> None:
        """Verify maintain_orders_silver.py has valid Python syntax."""
        ast.parse(DAG_PATH.read_text())

    def test_dag_has_dag_decorator(self) -> None:
        """Verify a function named maintain_orders_silver is @dag-decorated."""
        tree = ast.parse(DAG_PATH.read_text())
        dag_functions = [
            node.name
            for node in ast.walk(tree)
            if isinstance(node, ast.FunctionDef)
            for decorator in node.decorator_list
            if (isinstance(decorator, ast.Name) and decorator.id == "dag")
            or (isinstance(decorator, ast.Call) and isinstance(decorator.func, ast.Name) and decorator.func.id == "dag")
        ]
        assert dag_functions == ["maintain_orders_silver"]

    def test_schedule_is_time_based_not_asset_scheduled(self) -> None:
        """This is a separate, cron-scheduled DAG -- not scheduled off an Asset."""
        tree = ast.parse(DAG_PATH.read_text())
        dag_calls = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "dag"
        ]
        assert len(dag_calls) == 1
        schedule_keywords = [kw for kw in dag_calls[0].keywords if kw.arg == "schedule"]
        assert len(schedule_keywords) == 1
        schedule_value = schedule_keywords[0].value
        assert isinstance(schedule_value, ast.Constant)
        assert isinstance(schedule_value.value, str)

    def test_default_args_overrides_execution_timeout_materially_longer_than_silver_default(
        self,
    ) -> None:
        """A full-table OPTIMIZE+ZORDER+VACUUM must get a longer timeout than SILVER_DEFAULT_ARGS's per-batch budget."""
        import pendulum

        from plugins.dag_defaults import SILVER_DEFAULT_ARGS

        tree = ast.parse(DAG_PATH.read_text())
        dag_calls = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "dag"
        ]
        assert len(dag_calls) == 1
        default_args_keywords = [kw for kw in dag_calls[0].keywords if kw.arg == "default_args"]
        assert len(default_args_keywords) == 1
        default_args_dict = default_args_keywords[0].value
        assert isinstance(default_args_dict, ast.Dict)

        # `{**SILVER_DEFAULT_ARGS, "execution_timeout": ...}`: one unpacked
        # key (`None` key marks a `**`-unpack) plus one explicit override.
        unpack_targets = [
            value_node.id
            for key_node, value_node in zip(default_args_dict.keys, default_args_dict.values, strict=True)
            if key_node is None and isinstance(value_node, ast.Name)
        ]
        assert unpack_targets == ["SILVER_DEFAULT_ARGS"]

        override_values = [
            value_node
            for key_node, value_node in zip(default_args_dict.keys, default_args_dict.values, strict=True)
            if isinstance(key_node, ast.Constant) and key_node.value == "execution_timeout"
        ]
        assert len(override_values) == 1
        override_call = override_values[0]
        assert isinstance(override_call, ast.Call)
        hours_kwarg = next(kw for kw in override_call.keywords if kw.arg == "hours")
        maintenance_timeout = pendulum.duration(hours=hours_kwarg.value.value)

        assert maintenance_timeout > SILVER_DEFAULT_ARGS["execution_timeout"]

    def test_defines_zorder_columns_order_id_store_id(self) -> None:
        """Verify ZORDER_COLUMNS names exactly (order_id, store_id), in that order."""
        tree = ast.parse(DAG_PATH.read_text())
        assignments = [
            node
            for node in tree.body
            if isinstance(node, ast.Assign)
            for target in node.targets
            if isinstance(target, ast.Name) and target.id == "ZORDER_COLUMNS"
        ]
        assert len(assignments) == 1
        zorder_tuple = assignments[0].value
        assert isinstance(zorder_tuple, ast.Tuple)
        columns = [elt.value for elt in zorder_tuple.elts if isinstance(elt, ast.Constant)]
        assert columns == ["order_id", "store_id"]

    def test_defines_a_positive_vacuum_retention_hours(self) -> None:
        """Verify VACUUM_RETENTION_HOURS is defined and set to a safe (non-zero) value."""
        tree = ast.parse(DAG_PATH.read_text())
        assignments = [
            node
            for node in tree.body
            if isinstance(node, ast.Assign)
            for target in node.targets
            if isinstance(target, ast.Name) and target.id == "VACUUM_RETENTION_HOURS"
        ]
        assert len(assignments) == 1
        retention_hours = assignments[0].value
        assert isinstance(retention_hours, ast.Constant)
        # Delta's own safety default (168h/7 days) -- not shortened below it.
        assert retention_hours.value >= 168

    def test_run_function_calls_optimize_and_vacuum(self) -> None:
        """Verify _run_maintenance's body actually invokes executeZOrderBy and vacuum."""
        source = DAG_PATH.read_text()
        assert "executeZOrderBy" in source
        assert ".vacuum(" in source


class TestRunMaintenance:
    """Exercises `_run_maintenance` against a real local Delta table."""

    def test_optimize_and_vacuum_preserve_all_rows(
        self, spark_session: SparkSession, temp_delta_path: str, tmp_path: Path, load_dag_pure_symbols
    ) -> None:
        """OPTIMIZE ZORDER + VACUUM must not lose or duplicate any row."""
        namespace = load_dag_pure_symbols(
            DAG_FILENAME,
            {"ZORDER_COLUMNS", "VACUUM_RETENTION_HOURS", "_run_maintenance"},
        )
        run_maintenance = namespace["_run_maintenance"]
        zorder_columns = namespace["ZORDER_COLUMNS"]
        vacuum_retention_hours = namespace["VACUUM_RETENTION_HOURS"]

        _build_table(spark_session, tmp_path, temp_delta_path)

        run_maintenance(spark_session, temp_delta_path, zorder_columns, vacuum_retention_hours)

        result = spark_session.read.format("delta").load(temp_delta_path).collect()
        assert {row["order_id"] for row in result} == {"order-1", "order-2", "order-3"}
        assert len(result) == len(_ROWS)

    def test_optimize_recorded_in_delta_history(
        self, spark_session: SparkSession, temp_delta_path: str, tmp_path: Path, load_dag_pure_symbols
    ) -> None:
        """Verify OPTIMIZE actually ran, per the Delta transaction log's own history."""
        namespace = load_dag_pure_symbols(
            DAG_FILENAME,
            {"ZORDER_COLUMNS", "VACUUM_RETENTION_HOURS", "_run_maintenance"},
        )
        run_maintenance = namespace["_run_maintenance"]
        zorder_columns = namespace["ZORDER_COLUMNS"]
        vacuum_retention_hours = namespace["VACUUM_RETENTION_HOURS"]

        _build_table(spark_session, tmp_path, temp_delta_path)

        run_maintenance(spark_session, temp_delta_path, zorder_columns, vacuum_retention_hours)

        history = DeltaTable.forPath(spark_session, temp_delta_path).history().collect()
        operations = {row["operation"] for row in history}
        assert "OPTIMIZE" in operations
