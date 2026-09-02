# Veloz Platform

Veloz is a quick-commerce startup delivering groceries and everyday
essentials in 15–45 minutes, running about 30 dark stores across Medellín,
Bogotá, and São Paulo — roughly 450 riders, ~180,000 orders a month, and
about to expand into two more cities.

This repository is Veloz's data platform: a local, single-machine pipeline
that pulls in orders, rider location/status events, store fulfillment
reports, and payment/commission files, cleans and reconciles them, and
turns them into dashboards and daily reports for Operations and Finance.

It runs entirely on one laptop or one machine using Docker — no cloud
account, no dedicated infrastructure team required.

## The problem this solves

Today, everything is manual:

- **Operations** finds out a store is falling behind, or a rider has gone
  dark, only when a customer complains — not before.
- **Finance** reconciles rider payouts by hand every day, which is slow and
  error-prone. A rider was recently underpaid for days before anyone
  noticed.
- There's a single analyst running SQL queries by hand each morning. If
  that person is out sick, nothing gets checked.

This platform automates all of that: it ingests the raw data on a
schedule, checks it for problems, and produces two things people can
actually rely on:

1. **A near-live view** for Ops of order and rider status, store by store
   (minutes-old, not hours-old).
2. **A daily reconciliation report** for Finance, ready before their 8am
   standup, that clearly flags any numbers that don't add up instead of
   silently guessing which source is "right."

## How it works — Architecture overview

```mermaid
flowchart TB
    subgraph SRC["External sources"]
        direction LR
        ORD["Orders"]
        RID["Rider events"]
        FUL["Fulfillment"]
        PAY["Payments"]
    end

    AF["Airflow\norchestration"]
    SPARKJOB["Airflow Spark task\nsubmits job"]
    SPARKCLUSTER["Spark Standalone\ncluster"]

    subgraph OUT["Stakeholder outputs"]
        direction LR
        OPSVIEW["Ops status view"]
        FINVIEW["Finance reconciliation"]
    end

    SRC --> AF --> SPARKJOB --> SPARKCLUSTER --> OUT

    subgraph LAKE["MinIO — object storage"]
        direction LR
        BRONZE[("Bronze\nraw, schema-applied")]
        SILVER[("Silver\ndeduplicated, reconciled")]
        GOLD[("Gold\ndashboard-ready")]
        BRONZE --> SILVER --> GOLD
    end

    SPARKCLUSTER -.-> BRONZE
    GOLD -.-> OUT

    classDef external fill:#fde2e2,stroke:#b91c1c,color:#000
    classDef orchestration fill:#e0f2fe,stroke:#0369a1,color:#000
    classDef compute fill:#dcfce7,stroke:#15803d,color:#000
    classDef storage fill:#fef9c3,stroke:#a16207,color:#000
    classDef output fill:#ede9fe,stroke:#6d28d9,color:#000

    class ORD,RID,FUL,PAY external
    class AF,SPARKJOB orchestration
    class SPARKCLUSTER compute
    class BRONZE,SILVER,GOLD storage
    class OPSVIEW,FINVIEW output
```

Orders, rider events, fulfillment, and payments (red) come in, Airflow
(blue) orchestrates the pipeline and submits a Spark job, which the Spark
Standalone cluster (green) actually processes. Below that flow, MinIO
(yellow) holds the Bronze → Silver → Gold storage layers: the Spark cluster
writes into Bronze, and Gold feeds the Ops and Finance-facing outputs
(purple).

This is the high-level version. For the full container/service topology —
every Airflow service, the Spark Standalone cluster, exact bucket and IAM
role names, the streaming path, and what's built versus still designed —
see [`docs/architecture-diagram.md`](docs/architecture-diagram.md) and
[`docs/architecture-topology.md`](docs/architecture-topology.md).

## Quick start

### Prerequisites

- [Docker](https://www.docker.com/) and Docker Compose (v2, bundled with
  Docker Desktop)
- Python 3.14 (only needed if you want to run tests or scripts outside of
  Docker)
- ~8 GB of free RAM for Docker (the platform runs several services plus a
  small Spark cluster locally)

### Steps

1. Clone the repository and move into it:

   ```bash
   git clone <repo-url>
   cd veloz-platform
   ```

2. Start everything:

   ```bash
   docker compose up -d
   ```

   The first run builds the Airflow/Spark image, which can take a few
   minutes. Subsequent starts are fast.

3. Wait for the services to become healthy:

   ```bash
   docker compose ps
   ```

   Everything should show `healthy` or `running`. `airflow-init` is
   expected to run once and exit — that's normal.

4. Open the Airflow UI at **http://localhost:8080** and log in with:

   - **Username:** `admin`
   - **Password:** `veloz-local-dev`

   (These come from `.env` — change them there if you want different
   local credentials.)

### What's running

| Service | What it does | Where to look |
|---|---|---|
| Airflow (scheduler, api-server, dag-processor) | Schedules and runs every pipeline | http://localhost:8080 |
| Spark cluster (1 master + workers) | Does the actual data processing | http://localhost:8090 |
| MinIO | Local stand-in for Amazon S3 — stores raw and processed data | http://localhost:9001 |
| PostgreSQL | Airflow's own internal metadata database | internal only |
| Jupyter (optional) | Notebook for exploring data by hand | http://localhost:8888 |

### Running a pipeline

Pipelines in this project are Airflow DAGs. Most run automatically on a
schedule, but you can trigger one manually to see it work end to end:

1. In the Airflow UI, find a DAG such as `ingest_orders_bronze`.
2. Un-pause it (DAGs start paused by default) using the toggle on the
   left.
3. Click the "Trigger DAG" (play) button to run it once immediately.
4. Watch the task boxes turn green as each step completes, or click into a
   task to read its logs if something goes red.

## Architecture (plain-language version)

### Where the data comes from

| Source | What it is | How it arrives |
|---|---|---|
| **Orders** | The lifecycle of each order (created → assigned → picked up → delivered/cancelled) | Periodic exports from the app's order database |
| **Rider events** | Rider location pings and status changes | A continuous stream of events (like a live feed) |
| **Fulfillment reports** | One spreadsheet per store per day, summarizing what happened | Emailed CSV files — occasionally late or malformed |
| **Payments** | Daily file of rider payouts and commissions | A daily batch file from the finance system |

The full technical spec for each source lives in
[`docs/data-sources.md`](docs/data-sources.md).

### The three-layer pipeline

Data moves through three stages, each one a little more polished than the
last:

1. **Raw data** ("Bronze") — exactly what arrived, unmodified, kept for
   audit purposes. If something upstream was wrong, this layer proves it.
2. **Cleaned data** ("Silver") — duplicates removed, types fixed,
   inconsistent formats normalized, so different sources can be compared
   against each other.
3. **Ready to report** ("Gold") — the numbers Ops and Finance actually
   look at: store-by-store status, rider status, and the daily
   reconciliation report.

### The tools involved

- **Apache Spark** does the heavy lifting — reading, cleaning, joining,
  and summarizing data.
- **Apache Airflow** schedules and coordinates all of that work, retries
  it automatically if something fails, and gives you one place to see
  what ran, what's late, and what broke.
- **MinIO** stores the data files, acting as a local drop-in replacement
  for Amazon S3 (so the same code would work against real S3 later with
  no rewrite).

Everything — Spark, Airflow, and the storage — runs on a single machine
inside Docker containers. There is no separate cluster to provision or
pay for. That keeps this proportionate to a one-person data team while
still working the same way it would at a larger scale.

## Key constraints

These come directly from how the business operates, not from arbitrary
engineering preference:

- **Speed for Ops** — a store falling behind or a rider going dark needs
  to be visible within a few minutes, not after a customer complains.
- **A trustworthy morning number for Finance** — the daily reconciliation
  report must be ready before 8am local time, with no one babysitting it
  overnight.
- **Disagreements are shown, not hidden** — if two data sources disagree
  (e.g., orders say a delivery happened but fulfillment doesn't), the
  report flags the discrepancy instead of silently picking one side.
- **Runs on one machine** — there's no dedicated infrastructure or on-call
  team behind this, so everything has to be recoverable and low-maintenance
  by design.

## Project structure

```
dags/                Airflow workflows — the pipelines that actually run
application/          Core data-processing logic (use cases), independent of Airflow/Spark specifics
infrastructure/       Adapters — Spark I/O, S3/MinIO access, Airflow-specific glue
data/                 Local file storage used by the platform (raw/processed data)
docker/               Docker images and configuration for Airflow, MinIO, etc.
docs/                 Technical reference docs, including the full data source spec
generators/           Emulators that produce realistic upstream data for local development
notebooks/            Jupyter notebooks for ad hoc data exploration
plugins/              Airflow plugin code, shared Spark session helpers
tests/                Automated tests
```

For the full schema and business-rule details behind each upstream data
source, see [`docs/data-sources.md`](docs/data-sources.md).

## Troubleshooting & next steps

- **Monitor pipelines:** the Airflow UI at http://localhost:8080 is the
  main place to check — it shows every DAG run, its status, and how long
  it took.
- **Read logs:** click into any task in the Airflow UI and open its "Logs"
  tab, or check the `logs/` directory on disk if a service won't start.
- **Check the Spark cluster:** http://localhost:8090 shows active Spark
  jobs and worker health.
- **Check MinIO storage:** http://localhost:9001 (login:
  `minioadmin` / `veloz-local-dev` unless overridden in `.env`) lets you
  browse the raw, cleaned, and reporting data directly.
- **Deeper technical reference:** see [`docs/`](docs) for architecture
  decisions ([`docs/ADR.md`](docs/ADR.md)), the data source spec
  ([`docs/data-sources.md`](docs/data-sources.md)), and diagrams
  ([`docs/architecture-diagram.md`](docs/architecture-diagram.md)).

If a service won't come up, run `docker compose ps` to see which
container is unhealthy, then `docker compose logs <service-name>` to see
why.
