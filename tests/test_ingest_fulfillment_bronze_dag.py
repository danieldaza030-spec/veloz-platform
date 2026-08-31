"""DAG import and structure checks for dags/ingest_fulfillment_bronze.py."""

from __future__ import annotations

import ast
from pathlib import Path


class TestIngestFulfillmentBronzeDAGImport:
    """Test DAG can be parsed and has correct structure."""

    def test_dag_file_syntax_valid(self) -> None:
        """Verify ingest_fulfillment_bronze.py has valid Python syntax."""
        dag_path = Path(__file__).parent.parent / "dags" / "ingest_fulfillment_bronze.py"
        assert dag_path.exists(), f"DAG file should exist at {dag_path}"

        with open(dag_path) as f:
            source_code = f.read()

        try:
            ast.parse(source_code)
        except SyntaxError as e:
            raise AssertionError(f"DAG has syntax error: {e}") from e

    def test_dag_has_expected_constants(self) -> None:
        """Verify DAG has correct constant definitions."""
        dag_path = Path(__file__).parent.parent / "dags" / "ingest_fulfillment_bronze.py"

        with open(dag_path) as f:
            source_code = f.read()

        tree = ast.parse(source_code)

        constants_found = {}
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        if isinstance(node.value, ast.Constant):
                            constants_found[target.id] = node.value.value

        assert "FULFILLMENT_RAW_PREFIX" in constants_found
        assert constants_found["FULFILLMENT_RAW_PREFIX"] == "fulfillment"

        assert "FULFILLMENT_BRONZE_PREFIX" in constants_found
        assert constants_found["FULFILLMENT_BRONZE_PREFIX"] == "fulfillment"

        assert "SOURCE_NAME" in constants_found
        assert constants_found["SOURCE_NAME"] == "fulfillment"

        assert "PARTITION_COLUMN" in constants_found
        assert constants_found["PARTITION_COLUMN"] == "date"

        assert "CORRUPT_RECORD_COLUMN" in constants_found
        assert constants_found["CORRUPT_RECORD_COLUMN"] == "_corrupt_record"

    def test_dag_has_dag_decorator(self) -> None:
        """Verify DAG uses @dag decorator and defines expected function."""
        dag_path = Path(__file__).parent.parent / "dags" / "ingest_fulfillment_bronze.py"

        with open(dag_path) as f:
            source_code = f.read()

        tree = ast.parse(source_code)

        dag_functions = []
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                for decorator in node.decorator_list:
                    if isinstance(decorator, ast.Name) and decorator.id == "dag":
                        dag_functions.append(node.name)
                    elif isinstance(decorator, ast.Call):
                        if isinstance(decorator.func, ast.Name) and decorator.func.id == "dag":
                            dag_functions.append(node.name)

        assert len(dag_functions) > 0, "Should have at least one @dag decorated function"
        assert "ingest_fulfillment_bronze" in dag_functions

    def test_dag_schedule_runs_after_generator(self) -> None:
        """Verify the schedule is set to run after generate_fulfillment's 01:05 UTC run."""
        dag_path = Path(__file__).parent.parent / "dags" / "ingest_fulfillment_bronze.py"

        with open(dag_path) as f:
            source_code = f.read()

        assert '"20 1 * * *"' in source_code
