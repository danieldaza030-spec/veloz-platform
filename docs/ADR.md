# ADR 0001 — Veloz Data Platform: Reference Architecture

- **Status:** Accepted
- **Date:** 2026-08-27
- **Author:** Daniel Daza (first dedicated data hire, Veloz)

## Context

Veloz runs ~30 dark stores across Medellín, Bogotá and São Paulo, ~450 active
riders, ~180,000 orders/month, and is about to expand into two more cities.
Today, reporting is one analyst running SQL by hand every morning. Ops learns a
store is falling behind from customer complaints, not from the data. Finance
reconciles rider payouts manually and recently underpaid a rider for days
before anyone noticed. There is no infra or platform team behind this build —
it's a one-person data team, and it has to stay that way.

Four upstream systems exist as-is and cannot be modified:

1. **Orders** — Postgres periodic extracts of the order table
   (created → assigned → picked_up → delivered/cancelled). True CDC is not
   available.
2. **Rider app events** — location pings and status changes published to a
   queue, consumable as a stream.
3. **Store fulfillment exports** — one CSV per store per day, emailed.
   Unreliable, and not even consistent in shape between stores.
4. **Payments/commissions ledger** — a daily file of rider payouts and
   commissions for the previous day's completed orders.

## Decision drivers (business goals)

- **G1** — Ops needs near-live visibility into order and rider status, store
  by store (a few minutes of lag is fine; an hour is not).
- **G2** — Finance needs a daily, auditable reconciliation ready before 8am,
  unattended, with disagreements between sources **flagged, not hidden**.
- **G3** — The pipeline must tolerate upstream messiness (late/malformed
  files, dropped events, schema drift) without a human intervening overnight.
- **G4** — Must support at least 5x current order volume over the next 12
  months without a redesign.
- **G5** — Operational complexity and cost must stay proportionate to a
  one-person data team — nothing here may require an on-call or infra
  rotation.

## Decision

| Decision | Choice | Why |
|---|---|---|
| Rider events | Kafka → Spark Structured Streaming | G1 / Marcela's ask ("know within a few minutes") is explicitly about liveness, which only a stream can give — batch extracts of a queue would reintroduce the lag we're trying to remove. |
| Orders / fulfillment / payments | Batch, orchestrated | These three sources only ever arrive as periodic extracts, daily CSVs, or a daily ledger file (G2's cadence). No CDC is available on the orders DB, so treating this as a streaming problem would add complexity with no source to actually stream from. |
| Table format | Delta Lake | G2's audit requirement ("what did yesterday's numbers look like, weeks later") needs time travel on top of plain object storage. ACID writes also protect G3: a half-written batch or a retried task can't leave Silver/Gold in a partially-updated, inconsistent state. |
| Orchestration + quarantine pattern | Airflow | G3 rules out someone manually re-running a failed job at 2am. Airflow gives retries, scheduling, and a place to quarantine malformed files instead of silently dropping or silently accepting them. |
| Storage | Object storage (MinIO standing in for S3) | G4 — local disk doesn't survive 5x order volume or two more cities' worth of data; object storage does, and is a drop-in swap for real S3 later with no redesign. |
| Compute | Spark in-process, no dedicated cluster | G5 rules out standing up and operating a Spark cluster, which would need a platform team this org doesn't have. In-process Spark (driver-only, local mode) is enough at current and 5x volume, and can move to a managed cluster later without changing the code. |

### Orchestration environment

Airflow runs via Docker Compose with the **LocalExecutor** — a single
Postgres metadata DB, one webserver, one scheduler, no Redis/Celery workers.
LocalExecutor runs tasks as subprocesses of the scheduler itself, which is
enough concurrency for this DAG count and volume and avoids operating a
message broker (again, G5). The scheduler's image is a custom build on top of
the official Airflow image with a JDK and `pyspark`/`delta-spark` (pinned to
the same versions used in local development) installed directly into it, so
DAG tasks can call Spark/Delta in-process without shelling out to a separate
cluster or a `SparkSubmitOperator` pointed at infrastructure that doesn't
exist here.

## Consequences

- **What this buys us:** a single orchestrator with retry/alerting built in
  (G3), an audit trail via Delta's transaction log (G2), and a storage layer
  that scales past 5x without a rewrite (G4) — all without adding a service
  this one engineer would have to keep alive overnight (G5).
- **What this costs us:** Spark running in-process inside the Airflow
  scheduler means compute and orchestration aren't isolated from each other —
  a runaway Spark job could affect scheduler stability. Acceptable at this
  volume; called out explicitly as a production-scale gap (see the
  out-of-scope note in the final ADR/demo deliverable).
- **What could change this:** if order volume growth outpaces what in-process
  Spark can handle before the 5x horizon, the next step is an external Spark
  cluster invoked via `SparkSubmitOperator`/`SparkKubernetesOperator` — the
  DAG structure doesn't need to change, only how each Spark task is launched.

## Alternatives considered

- **Batch-only for rider events (no Kafka/streaming):** rejected — directly
  fails G1/Marcela's "within a few minutes" requirement; periodic batch
  extracts of location pings would reintroduce the exact lag Ops is
  complaining about today.
- **Plain Parquet instead of Delta Lake:** rejected — no ACID writes and no
  time travel, which breaks G2's audit requirement and reopens G3's
  partial-write risk on retried batch jobs.
- **A managed Spark cluster (e.g. Databricks, EMR) from day one:** rejected
  for this MVP — real money and more operational surface than G5 allows for a
  one-person team pre-demo; the architecture is deliberately left able to move
  there later without a redesign.
- **Cron instead of Airflow:** rejected — cron has no retry semantics, no
  dependency graph between the four sources, and no quarantine pattern for
  malformed files, all of which G3 explicitly requires.

## Appendix — Generator schemas and how to trigger them

All four upstream sources are simulated by generators under `generators/`,
standing in for systems Veloz doesn't let us touch directly. All read shared,
deterministic dimensions (30 stores, 450 riders, a SKU catalog) from
`generators/reference_data.py` so their output joins sensibly. Full schemas
and business rules (including the payments ledger's commission policy and
settlement-lag pattern) are documented in full in `docs/data-sources.md`.

Every source's schema and messiness pattern is specified upfront in
`docs/data-sources.md` rather than discovered by profiling — see `CLAUDE.md`
for why. All four generators below emit a single, consistent schema — one row
per order, one schema per store, one event shape, one commission policy —
with no cross-row ambiguity or per-store drift baked in.

### Orders extract — `generators/orders.py`

Simulates a periodic Postgres extract of the app's order table: one row per
order reflecting its state *as of extract time*, not a changelog.

| Column | Type | Notes |
|---|---|---|
| `order_id` | string (UUID) | |
| `store_id` | string | one of `STORE-001`..`STORE-030` |
| `rider_id` | string, nullable | `RIDER-0001`..`RIDER-0450`; null until the order reaches `assigned` |
| `status` | string | `created` \| `assigned` \| `picked_up` \| `delivered` \| `cancelled` |
| `created_at` | timestamp | |
| `assigned_at` | timestamp, nullable | null if the order never reached `assigned` |
| `picked_up_at` | timestamp, nullable | null if the order never reached `picked_up` |
| `delivered_at` | timestamp, nullable | populated only when `status == delivered` |
| `order_total` | float | plausible basket size, $8–65 |
| `updated_at` | timestamp | timestamp of the order's last recorded state change |

**Output:** one object per extract, `s3://raw-incoming-data/orders/orders_<date>.csv`
(MinIO) — see the landing-zone note in `docs/data-sources.md` for why this
moved off local `data/raw/` and the open bucket-naming issue.

**Trigger (inside a container with MinIO reachable — boto3/MINIO_ENDPOINT
aren't set up for the bare local `.venv`):**
```bash
docker compose exec airflow-scheduler python generators/orders.py \
  --date 2026-08-27 \
  --num-orders 6000 \
  --bucket raw-incoming-data \
  --output-dir orders \
  --seed 42
```
`--date` defaults to today, `--num-orders` defaults to 6000 (~180k/month ÷ 30
days), `--output-dir` is now an S3 key prefix (default: `orders`, not a local
path), `--bucket` defaults to `raw-incoming-data`, `--seed` defaults to 42.

### Fulfillment feed — `generators/fulfillment.py`

Simulates the daily, per-store, emailed CSV export from each dark store's
inventory system.

| Column | Type | Notes |
|---|---|---|
| `store_id` | string | one of `STORE-001`..`STORE-030` |
| `sku` | string | one of `SKU-0001`..`SKU-0033` |
| `date` | date | the export date |
| `quantity_on_hand` | int, nullable | blank = SKU not counted that day (the one null convention used, consistently, by every store) |
| `exported_at` | timestamp | when that store's daily file was generated |

**Output:** one object per store per day,
`s3://raw-incoming-data/fulfillment/date=<date>/<store_id>.csv` (MinIO) — a
store with no object for a given date means that store's export never
arrived. See the landing-zone note in `docs/data-sources.md`.

**Trigger (normal night — all 30 stores' files land clean):**
```bash
docker compose exec airflow-scheduler python generators/fulfillment.py \
  --date 2026-08-27 \
  --bucket raw-incoming-data \
  --output-dir fulfillment \
  --seed 42
```

**Trigger (`--bad-night` — baseline feed unreliability, G3):**
```bash
docker compose exec airflow-scheduler python generators/fulfillment.py \
  --date 2026-08-27 \
  --bucket raw-incoming-data \
  --output-dir fulfillment \
  --seed 42 \
  --bad-night
```
With `--bad-night`, ~10% of stores' files are missing entirely for the date
and ~15% arrive with a share of rows mechanically corrupted (a row truncated
mid-write, or an extra stray delimiter field) — a downstream reader must
quarantine these rather than crash; confirmed by hand that `pandas.read_csv`
raises `ParserError` on the extra-delimiter rows and silently misaligns the
truncated ones. Same `--seed` reproduces the identical set of affected
stores and corrupted rows.

### Rider events — `generators/rider_events.py`

Simulates the queue of location pings and status changes a rider's phone
publishes while working an active order that day (an order that reached
`assigned` or further in that date's orders extract) — reads that date's
orders extract as input.

| Field | Type | Notes |
|---|---|---|
| `event_id` | string (UUID) | |
| `rider_id` | string | |
| `order_id` | string | the order this event relates to |
| `event_type` | string | `location_ping` \| `status_change` |
| `event_time` | timestamp | event time, not processing time |
| `latitude` | float, nullable | populated only for `location_ping` |
| `longitude` | float, nullable | populated only for `location_ping` |
| `status` | string, nullable | populated only for `status_change`: `assigned` \| `picked_up` \| `delivered` |

**Output:** one object per day, JSON Lines,
`s3://raw-incoming-data/rider_events/rider_events_<date>.jsonl` (MinIO). See
the landing-zone note in `docs/data-sources.md`.

**Trigger:**
```bash
docker compose exec airflow-scheduler python generators/rider_events.py \
  --date 2026-08-27 \
  --bucket raw-incoming-data \
  --orders-dir orders \
  --output-dir rider_events \
  --seed 42
```
`--date` defaults to today, `--orders-dir`/`--output-dir` are now S3 key
prefixes (defaults: `orders`/`rider_events`, not local paths), `--bucket`
defaults to `raw-incoming-data`, `--seed` defaults to 42.
Requires that date's orders extract to already exist; fails with a clear
error otherwise. Per order that reaches `assigned`+: one `status_change`
event per lifecycle timestamp reached, plus 2-5 `location_ping` events
jittered around the order's store city center (±0.05°). Events are written
in natural generation order, not globally sorted by `event_time` — this is
documented, realistic stream behavior (see `docs/data-sources.md`), and
confirmed by hand: a fixed seed's output has adjacent-line timestamp
inversions, i.e. it is not already time-sorted.

### Payments/commissions ledger — `generators/payments.py`

Simulates Finance's daily payments-system export: one commission payout per
delivered order from that date's orders extract, computed from the tiered
commission policy and settlement-lag pattern documented in
`docs/data-sources.md` — reads that date's orders extract as input.

| Column | Type | Notes |
|---|---|---|
| `payment_id` | string (UUID) | |
| `order_id` | string | references a delivered order in that date's extract |
| `rider_id` | string | the rider who completed the order |
| `commission_amount` | float | USD-equivalent, per the commission policy |
| `payment_recorded_at` | timestamp | when Finance's system recorded the payout |

**Output:** one object per day, `s3://raw-incoming-data/payments/payments_<date>.csv`
(MinIO). See the landing-zone note in `docs/data-sources.md`.

**Trigger:**
```bash
docker compose exec airflow-scheduler python generators/payments.py \
  --date 2026-08-27 \
  --bucket raw-incoming-data \
  --orders-dir orders \
  --output-dir payments \
  --seed 42
```
`--date` defaults to today, `--orders-dir`/`--output-dir` are now S3 key
prefixes (defaults: `orders`/`payments`, not local paths), `--bucket`
defaults to `raw-incoming-data`, `--seed` defaults to 42.
Requires that date's orders extract to already exist; fails with a clear
error otherwise. Two deliberate data-quality issues are injected at fixed,
seeded rates (documented in `docs/data-sources.md`, not a row-level answer
key): ~2% of delivered orders get a `commission_amount` that doesn't match
the policy (wrong tier or an arbitrary error factor), ~1% get no payment row
at all. Confirmed by hand against a recomputation from `order_total`/timing:
mismatch and missing rates land close to the documented 2%/1% at order-extract
scale, and every non-injected row's amount matches the policy exactly.

## Appendix — Compute divergence #2: Hadoop YARN → Spark Standalone (2026-08-30)

**A documentation gap this entry does not silently fold in:** the Compute
row above still reads "Spark in-process, no dedicated cluster," unedited
since this ADR's 2026-08-27 acceptance. At some point before this session,
this repo's compute layer was already moved once — off in-process Spark and
onto a real Hadoop YARN cluster (`docker-compose.yml`'s `yarn-resourcemanager`
/`yarn-nodemanager` services, `docker/hadoop/Dockerfile`,
`plugins/spark_session.py`'s `YarnSparkSessionFactory`). Both of those files'
own comments cited "`docs/ADR.md`'s dated appendix entry" for that decision,
and `PROGRESS.md`'s session log cited it too for empirically-verified
behavior (`deploy.replicas` under Compose V2, real cgroup limits via
`docker inspect`) — but no such entry was ever actually written here, and no
YARN line exists in `PROGRESS.md`'s session log either. This entry is not
retroactively inventing one; it's flagging that the YARN divergence itself
was never recorded, and now never will be under its own timestamp — only
its replacement is, below. The engineer should decide whether that gap needs
its own backfilled entry.

**This divergence.** `docker-compose.yml`'s compute block moved a second
time, from that Hadoop YARN cluster to a Spark Standalone cluster
(`spark-master`/`spark-worker` services, built from the same image as the
Airflow containers rather than a bespoke Hadoop image). Reasoning:

- Hadoop YARN added a second cluster technology (ResourceManager/
  NodeManager, a bare-Hadoop image, rendered `core-site`/`yarn-site` XML)
  purely to schedule Spark applications — Spark's own Standalone scheduler
  does the same job without a second technology's operational surface
  (still one thing for a one-person team to reason about, per G5), and
  without needing a shared local-disk staging filesystem for job
  localization (`hadoop-yarn-shared`), which YARN needed and Standalone
  does not: Standalone ships a driver's resolved jars/packages to executors
  itself, over the network, as ordinary task submission.
- The pip `pyspark==3.5.3` wheel this project already installs
  (`docker/airflow/Dockerfile`) does **not** ship `sbin/start-master.sh` /
  `sbin/start-worker.sh` (confirmed directly against this repo's own
  `.venv/lib/python3.14/site-packages/pyspark/sbin` — only
  `spark-daemon.sh`, `spark-config.sh`, and the history-server scripts are
  present). It does ship `bin/spark-class`, the launcher those wrapper
  scripts call internally; `spark-master`/`spark-worker` invoke it directly
  as their foreground command, which turned out to be a better fit for
  Docker than the wrapper scripts would have been anyway (they daemonize via
  `nohup`+pidfile and exit, leaving nothing for Docker to supervise).
- Worker resource caps stay real cgroup limits
  (`deploy.resources.limits.cpus`/`.memory`) feeding the worker's own
  `--cores`/`--memory` flags directly, the same principle
  `yarn.nodemanager.resource.{cpu-vcores,memory-mb}` enforced for YARN — a
  worker still can't advertise more to the scheduler than its container can
  actually deliver.
- Verified end-to-end, not assumed: `spark-master`'s UI (`localhost:8090`,
  the Master UI's own host mapping, since Spark's default 8080 is already
  `airflow-apiserver`'s) reported 5/5 workers registered at exactly
  2 cores/4096 MB each (matching `.env`'s `SPARK_WORKER_CPU_LIMIT`/
  `SPARK_WORKER_MEM_LIMIT` defaults), `docker inspect` confirmed the same
  numbers as real `NanoCpus`/`Memory` cgroup values, and
  `spark_standalone_smoke_test` (renamed from `spark_yarn_smoke_test`)
  round-tripped a Delta table over s3a with `spark.sparkContext.master`
  reporting `spark://spark-master:7077`, not `local[*]`.
- Open item, deliberately not resolved by inference: Spark Standalone has
  no built-in username/password auth. Its one real auth mechanism
  (`spark.authenticate` + a shared secret) is off here, matching the trust-
  the-Docker-network posture this stack already uses everywhere else
  (including the YARN cluster this replaces, which never turned on Kerberos/
  YARN ACLs either) — the engineer should confirm this is the intended
  posture rather than an oversight.
