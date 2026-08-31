"""Application-layer use cases.

Modules here orchestrate `infrastructure` adapters to fulfill a single
use case (e.g. "load a raw extract into Bronze"). No Airflow imports
belong in this package — DAG files call into it from `@task` functions.
"""
