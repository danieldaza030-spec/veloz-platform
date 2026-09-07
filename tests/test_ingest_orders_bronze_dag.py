"""DAG import and structure checks for dags/ingest_orders_bronze.py."""

from __future__ import annotations

import ast
from pathlib import Path


class TestIngestOrdersBronzeDAGImport:
    """Test DAG can be parsed and has correct structure."""

    def test_dag_file_syntax_valid(self) -> None:
        """Verify ingest_orders_bronze.py has valid Python syntax."""
        dag_path = Path(__file__).parent.parent / "dags" / "ingest_orders_bronze.py"
        assert dag_path.exists(), f"DAG file should exist at {dag_path}"

        # Parse the file as AST to verify syntax is valid
        with open(dag_path) as f:
            source_code = f.read()

        try:
            ast.parse(source_code)
        except SyntaxError as e:
            raise AssertionError(f"DAG has syntax error: {e}") from e

    def test_dag_has_expected_constants(self) -> None:
        """Verify DAG has correct constant definitions."""
        dag_path = Path(__file__).parent.parent / "dags" / "ingest_orders_bronze.py"

        with open(dag_path) as f:
            source_code = f.read()

        # Parse and check for specific constants
        tree = ast.parse(source_code)

        # Find assignments to the constants we expect
        constants_found = {}
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        # Get the value of simple string/constant assignments
                        if isinstance(node.value, ast.Constant):
                            constants_found[target.id] = node.value.value

        assert "ORDERS_RAW_PREFIX" in constants_found, "Should define ORDERS_RAW_PREFIX"
        assert constants_found["ORDERS_RAW_PREFIX"] == "orders"

        assert "ORDERS_BRONZE_PREFIX" in constants_found, "Should define ORDERS_BRONZE_PREFIX"
        assert constants_found["ORDERS_BRONZE_PREFIX"] == "orders"

        assert "EXTRACT_DATE_COLUMN" in constants_found, "Should define EXTRACT_DATE_COLUMN"
        assert constants_found["EXTRACT_DATE_COLUMN"] == "_extract_date"

    def test_dag_has_dag_decorator(self) -> None:
        """Verify DAG uses @dag decorator and defines expected function."""
        dag_path = Path(__file__).parent.parent / "dags" / "ingest_orders_bronze.py"

        with open(dag_path) as f:
            source_code = f.read()

        tree = ast.parse(source_code)

        # Look for a function decorated with @dag
        dag_functions = []
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                # Check if it has @dag decorator
                for decorator in node.decorator_list:
                    if isinstance(decorator, ast.Name) and decorator.id == "dag":
                        dag_functions.append(node.name)
                    elif isinstance(decorator, ast.Call):
                        if isinstance(decorator.func, ast.Name) and decorator.func.id == "dag":
                            dag_functions.append(node.name)

        assert len(dag_functions) > 0, "Should have at least one @dag decorated function"
        assert "ingest_orders_bronze" in dag_functions, "Should have ingest_orders_bronze() function"

    def test_dag_defines_orders_bronze_asset(self) -> None:
        """Verify ORDERS_BRONZE_ASSET is defined, separate from ORDERS_RAW_ASSET."""
        dag_path = Path(__file__).parent.parent / "dags" / "ingest_orders_bronze.py"

        with open(dag_path) as f:
            source_code = f.read()

        tree = ast.parse(source_code)

        top_level_assign_targets = {
            target.id
            for node in tree.body
            if isinstance(node, ast.Assign)
            for target in node.targets
            if isinstance(target, ast.Name)
        }

        assert "ORDERS_RAW_ASSET" in top_level_assign_targets
        assert "ORDERS_BRONZE_ASSET" in top_level_assign_targets, "Should define a distinct ORDERS_BRONZE_ASSET"

    def test_run_task_declares_orders_bronze_asset_outlet(self) -> None:
        """Verify the `run` task's @task decorator declares outlets=[ORDERS_BRONZE_ASSET]."""
        dag_path = Path(__file__).parent.parent / "dags" / "ingest_orders_bronze.py"

        with open(dag_path) as f:
            source_code = f.read()

        tree = ast.parse(source_code)

        run_functions = [node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef) and node.name == "run"]
        assert len(run_functions) == 1, "Should define exactly one `run` task function"
        run_function = run_functions[0]

        outlets_values: list[ast.expr] = []
        for decorator in run_function.decorator_list:
            if (
                isinstance(decorator, ast.Call)
                and isinstance(decorator.func, ast.Name)
                and decorator.func.id == "task"
            ):
                for keyword in decorator.keywords:
                    if keyword.arg == "outlets":
                        outlets_values.append(keyword.value)

        assert outlets_values, "The `run` task's @task decorator should declare outlets="
        outlets_list = outlets_values[0]
        assert isinstance(outlets_list, ast.List)
        outlet_names = [elt.id for elt in outlets_list.elts if isinstance(elt, ast.Name)]
        assert outlet_names == ["ORDERS_BRONZE_ASSET"]

    def test_run_task_publishes_outlet_event_extra(self) -> None:
        """Verify `run` sets outlet_events[ORDERS_BRONZE_ASSET].extra with extract_dates/windows keys."""
        dag_path = Path(__file__).parent.parent / "dags" / "ingest_orders_bronze.py"

        with open(dag_path) as f:
            source_code = f.read()

        assert "outlet_events" in source_code
        assert 'context["outlet_events"][ORDERS_BRONZE_ASSET].extra' in source_code

        tree = ast.parse(source_code)
        extra_assignments = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Assign)
            for target in node.targets
            if isinstance(target, ast.Attribute) and target.attr == "extra"
        ]
        assert len(extra_assignments) == 1
        extra_dict = extra_assignments[0].value
        assert isinstance(extra_dict, ast.Dict)
        extra_keys = [key.value for key in extra_dict.keys if isinstance(key, ast.Constant)]
        assert set(extra_keys) == {"extract_dates", "windows"}
