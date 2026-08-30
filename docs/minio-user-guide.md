# MinIO — User Guide

MinIO is the object store standing in for S3 in this stack (reference
architecture, G4: local container disk doesn't survive 5x growth). Every Delta
table the platform writes — Bronze, Silver, Gold — lives here over `s3a://`.

It comes up with the rest of the stack (`docker compose up -d`); a one-shot
`minio-init` service creates the buckets, policies and users described below and
exits. That bootstrap is idempotent, so it re-runs harmlessly on every restart.

This is reference material for whoever writes the ingestion DAGs. It documents
what exists and how to reach it — not what to build with it.

---

## Reaching MinIO

| What | Address | Credentials |
|---|---|---|
| Web console | <http://localhost:9001> | `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` |
| S3 API, from the host | <http://localhost:9000> | any of the three (see roles below) |
| S3 API, from inside the stack | `http://minio:9000` (`$MINIO_ENDPOINT`) | ingest role, by default |

Log into the console as **root**. The `veloz-ingest` and `veloz-maintenance`
users are scoped to the three buckets and nothing else, so console views that
need admin APIs won't work for them — they're API identities, not human logins.

From the host, with `mc` installed locally:

```bash
mc alias set veloz-local http://localhost:9000 minioadmin veloz-local-dev
mc ls veloz-local
mc ls --recursive veloz-local/bronze-veloz
```

If you'd rather not install `mc`, the container already has it:

```bash
docker compose exec minio mc alias set veloz http://localhost:9000 minioadmin veloz-local-dev
docker compose exec minio mc ls veloz
```

Airflow and Spark use `http://minio:9000` — the in-Docker-network address.
`localhost:9000` does not resolve from inside a container.

---

## Buckets

One bucket per lakehouse layer, layer name first so they sort together:

| Bucket | Layer | Holds |
|---|---|---|
| `bronze-veloz` | Bronze | raw ingested source data, as landed |
| `silver-veloz` | Silver | cleaned/conformed tables |
| `gold-veloz` | Gold | serving tables behind dashboards and reconciliation |

Paths are addressed as `s3a://bronze-veloz/<table>/`, e.g.
`s3a://bronze-veloz/orders/`.

---

## The two roles

Root credentials are for the console and for `minio-init`. Jobs use one of two
scoped users instead — the split exists so an ordinary pipeline bug cannot
destroy history:

### `veloz-ingest` — the default, and the only one DAGs should use

- **Can:** `GetObject`, `PutObject`, `ListBucket`, `GetBucketLocation`, and the
  multipart operations `s3a` needs to finish or abort an upload in progress.
- **Cannot:** `DeleteObject`, `DeleteObjectVersion`, `DeleteBucket`. Anything
  outside the three buckets above.
- **Credentials:** `MINIO_INGEST_ACCESS_KEY` / `MINIO_INGEST_SECRET_KEY` in
  `.env`, exposed inside Airflow containers as the standard
  `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`.
- **When:** every scheduled Bronze/Silver/Gold write. This is what a Spark
  session picks up by default, so it needs no deliberate action to get right.

Delta still works fully under this role. An `overwrite`, a `MERGE` or a
`DELETE FROM` writes new files and a new commit that marks old ones removed —
none of them physically deletes anything. Only `VACUUM` does, which is the
point: a broken DAG can produce a bad *version*, and the good version is still
there to time-travel back to (G2's audit trail, G3's resilience).

### `veloz-maintenance` — deliberate maintenance only

- **Can:** everything (`s3:*`) on the three buckets, including deletes.
- **Credentials:** `MINIO_MAINTENANCE_ACCESS_KEY` /
  `MINIO_MAINTENANCE_SECRET_KEY` in `.env`, passed into Airflow containers under
  those same names — deliberately *not* mapped to `AWS_*`.
- **When:** `VACUUM` (and `OPTIMIZE`, which rewrites files and eventually wants
  the old ones vacuumed away). Nothing else.

Because these are never the `AWS_*` variables, a job only acquires delete rights
by naming them and feeding them into its own Spark config. Don't wire this role
into a DAG that runs on a schedule alongside ingestion; if a retention job is
added later, it should be its own DAG whose blast radius is obvious from
reading it.

---

## Using it from a Spark session

Env vars available inside every Airflow container:

| Variable | Value |
|---|---|
| `AWS_ACCESS_KEY_ID` | ingest access key |
| `AWS_SECRET_ACCESS_KEY` | ingest secret key |
| `MINIO_ENDPOINT` | `http://minio:9000` |
| `MINIO_MAINTENANCE_ACCESS_KEY` | maintenance access key |
| `MINIO_MAINTENANCE_SECRET_KEY` | maintenance secret key |

The s3a jars are not baked into the image — they're resolved at runtime through
Ivy, the same way Delta's are. `hadoop-aws` must match the Hadoop that pyspark
3.5.3 bundles (3.3.4), and `aws-java-sdk-bundle` must be the version
`hadoop-aws` 3.3.4 was built against (1.12.262). Mismatched versions here fail
as a `NoSuchMethodError` on the first write, not at session start. The first run
after a container rebuild downloads ~200MB and is slow; later runs hit the
in-container Ivy cache.

```python
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

builder = (
    SparkSession.builder.appName("...")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.endpoint", os.environ["MINIO_ENDPOINT"])
    .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
    # MinIO serves every bucket from one host, so bucket-as-subdomain
    # addressing (the S3 default) doesn't resolve.
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
)
spark = configure_spark_with_delta_pip(
    builder,
    extra_packages=[
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    ],
).getOrCreate()
```

`dags/spark_minio_smoke_test.py` is this recipe end-to-end (write a tiny Delta
table to `s3a://bronze-veloz/_smoke_test/`, read it back). Run it after bringing
the stack up to confirm connectivity before debugging anything else:

```bash
docker compose exec airflow-scheduler airflow dags unpause spark_minio_smoke_test
docker compose exec airflow-scheduler airflow dags trigger spark_minio_smoke_test
```

For a maintenance job, the only difference is which credentials go into
`fs.s3a.access.key` / `fs.s3a.secret.key`:

```python
.config("spark.hadoop.fs.s3a.access.key", os.environ["MINIO_MAINTENANCE_ACCESS_KEY"])
.config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_MAINTENANCE_SECRET_KEY"])
```

---

## Notes

- Data lives on the host at `./data/minio/`, a bind mount — inspectable like
  `./data/raw/`, and git-ignored. Deleting that directory resets the object
  store; `minio-init` recreates buckets and users on the next `up`, but the
  objects are gone.
- An `AccessDenied` on a write that should be allowed is usually the ingest
  role hitting a delete: check whether the operation is a real `VACUUM` (use
  the maintenance role) or an accidental one.
- `VACUUM` isn't quite the only deleting operation: Delta also expires its own
  transaction log past `delta.logRetentionDuration` (30 days by default) once a
  table has been checkpointed, which the ingest role can't do either. That's
  weeks away for a table created today, and the fix when it surfaces is a
  maintenance-role job — not widening the ingest policy.
- The server image is pinned to `RELEASE.2025-04-22T22-12-26Z`, the last release
  with the full embedded console. Later community releases moved the console to
  the separate `object-browser` project and dropped user/policy management from
  the UI.
