"""Infrastructure-layer adapters: Spark/Delta I/O, external systems.

Modules here talk to concrete external systems (object storage, Delta
tables) on behalf of the `application` layer. No Airflow imports and no
business rules belong in this package.
"""
