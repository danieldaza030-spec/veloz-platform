# Veloz Platform — Progress

State, not rules. `CLAUDE.md` holds the rules and doesn't change often;
this file holds where things actually stand and changes every session.
Read it before doing anything. Update it before you stop — future-you
(or the next session, or any agent) starts here, not from memory.

**2026-08-27 — project pivot:** the Challenge/No-Autopilot-by-challenge
framework is retired. Data sources are now a fully-specified handoff (see
`docs/data-sources.md`) that Claude Code emulates completely; the engineer
builds the platform (Bronze→Silver→Gold, reconciliation, dashboards,
streaming) on top of it. See `CLAUDE.md` for the current operating model.
Older session-log entries below predate this and reference the old
Challenge numbering (Ch.1–9) — left as-is, they're history, not current
rules.

## Right now

- **Current focus:** Bronze is live for orders, fulfillment, and
  rider_events; payments Bronze ingestion is still unbuilt. Orders → Silver
  is now live too (`dags/ingest_orders_silver.py` +
  `dags/maintain_orders_silver.py`, Asset-scheduled off Bronze — see
  `docs/ADR.md`'s new appendix for the design reasoning).
- **Currently working on:** nothing in progress.
- **Blocked on:** nothing.
- **Next step:** payments Bronze ingestion, then Fulfillment → Silver. Two
  pre-existing quarantine bugs were flagged during the Silver work
  (`application/bronze_ingestion.py` — see the 2026-09-07 session log entry
  and `docs/ADR.md`'s appendix) and are worth fixing before Silver depends on
  any more Bronze source.

## Data sources (emulation build status)

Schemas and business rules for all four: `docs/data-sources.md`.

| Source | Generator | Status |
|---|---|---|
| Orders | `generators/orders.py` | [x] |
| Fulfillment | `generators/fulfillment.py` | [x] |
| Rider events | `generators/rider_events.py` | [x] |
| Payments/commissions | `generators/payments.py` | [x] |

## Platform build (engineer's own work)

Not started until the data sources above are finished. No hidden traps here
— the schemas and business rules are fully documented in
`docs/data-sources.md`; this is the actual data-engineering work: build it,
test it, be able to explain it.

| Milestone | Target | Status |
|---|---|---|
| Orders → Bronze | Raw extract schema-applied and landed in Delta, append-only per extract date | [x] |
| Orders → Silver | One row per order, deduplicated against re-extracts over time | [x] |
| Fulfillment → Silver | One row per (store, sku, date), quarantine for `--bad-night` malformed/missing files | [ ] |
| Payments reconciliation | Recomputed commission vs. actual, missing payments, lag outliers — all flagged, not hidden | [ ] |
| Ops-facing status view | Store/rider status, a few minutes of lag, not an hour | [ ] |
| Streaming consumer | Rider events into the lakehouse, checkpointed, no status regression on out-of-order arrival | [ ] |
| Query/perf pass | At least one measured before/after fix once data volume makes something slow | [ ] |

## Infra / full-delegate milestones

| Milestone | Status |
|---|---|
| Airflow local-dev (docker-compose, custom image) | [x] |
| Airflow upgraded to 3.3.1 (api-server/dag-processor/triggerer topology) | [x] |
| Generator-orchestration DAGs (`dags/generate_*.py`, interactive Params; orders+rider_events now share one DAG, fulfillment/payments each their own) | [x] |
| Day 1 ADR written | [x] |
| MinIO + `s3a://` — Bronze/Silver/Gold repointed | [x] |
| Generators → MinIO `raw-incoming-data` landing zone | [x] |
| Spark Standalone cluster (`spark-master`/`spark-worker`, replacing Hadoop YARN) | [x] |
| Kafka (KRaft) + Kafka UI + rider-event producer | [ ] |
| Streaming consumer scaffolding with checkpointing | [ ] |
| `OPTIMIZE`/`ZORDER`/`VACUUM` (orders Silver, `dags/maintain_orders_silver.py`) | [x] |
| Time-travel query | [ ] |

## Demo-day deliverables (definition of done)

| # | Deliverable | Status |
|---|---|---|
| 1 | Working pipeline ingesting all 4 sources | [ ] |
| 2 | Ops-facing near-live status view | [ ] |
| 3 | Finance reconciliation report, discrepancies flagged | [ ] |
| 4 | Live-injected failure, recovery/alert demonstrated | [ ] |
| 5 | One-page ADR (tech choices, cleaning judgment calls, perf fixes) | [ ] |
| 6 | Out-of-scope / production-scale note | [ ] |

## Ownership

- Data-source rows: `coder` builds and checks these off — schemas/rules are
  fully specified in `docs/data-sources.md`, nothing to discover.
- Platform-build rows: only the engineer checks these off. Per `CLAUDE.md`,
  Claude Code doesn't write this layer's logic unless explicitly asked to
  change that boundary for a specific piece of work. **Exception on record:**
  the "Orders → Bronze" row above was built by Claude Code — the engineer
  explicitly opted to change the boundary for that one piece of work when
  asked (see 2026-08-30 session log entry). **Second exception, same
  pattern:** the "Orders → Silver" row was also built by Claude Code (see
  the 2026-09-07 session log entry). Neither is a standing precedent for the
  rest of this table; each future row still defaults to closed unless asked
  again.
- Infra/demo rows: `coder` may check these off after finishing full-delegate
  work and saying so in the session log below.

## Session log

Newest first. One or two lines: what happened, what you decided, what's next.

- **2026-09-07 — built and verified the Silver orders layer:**
  `dags/ingest_orders_silver.py` (Bronze→Silver MERGE, Asset-scheduled off
  `ORDERS_BRONZE_ASSET`) and `dags/maintain_orders_silver.py`
  (`OPTIMIZE`+`ZORDER`+`VACUUM`, `30 6 * * *`), backed by
  `application/orders_silver_dedup.py`, `application/orders_silver_ingestion.py`,
  `infrastructure/delta_silver_merge_writer.py`,
  `metadata/orders_silver_schema.py`. One row per `order_id`, hybrid
  sticky/latest-wins MERGE, partitioned by `created_date` only with a
  batch-bounded `[min, max]` predicate injected into the MERGE condition
  (load-bearing at the 5,000,000-orders/day design target — see
  `docs/ADR.md`'s new appendix for the full reasoning and every rejected
  alternative). Also fixed in the same changeset: a Bronze partition-spec
  bug (manual vs. Asset-triggered runs disagreed on `[_extract_date,
  _ingestion_window]`) and a rename of Bronze's `_ingested_at` lineage
  column to `_bronze_ingested_at` (required a Bronze bucket wipe + re-ingest
  rather than `ALTER TABLE RENAME COLUMN`, to avoid a one-way
  `columnMapping` protocol upgrade — see ADR). 162 tests passed / 2
  pre-existing failures (unrelated), host/container parity confirmed.
- Explicitly deferred, not fixed here (see ADR appendix for the full list):
  status-regression detection (latest-wins currently applies silently when
  Bronze offers an earlier status with a newer `updated_at` — in tension
  with `CLAUDE.md`'s "never quietly pick a side"), and two pre-existing
  quarantine bugs in `application/bronze_ingestion.py`
  (`split_clean_and_corrupt_rows()` misses extra-column rows; `_source_file`
  gets corrupt-record content instead of a file path in the fulfillment
  path) — flagged as the next work item, since a malformed row now becomes
  persistent Silver state instead of a one-off bad row in a day's extract.
- **2026-08-31 — switched `ingest_orders_bronze` from a daily cron to
  Asset-based (event-driven) scheduling.** `generate_orders` now lands one
  window-file per 5-minute run at `orders/date=<date>/orders_<run_timestamp>.csv`
  instead of one file per day, so the old `01:10 UTC` cron (waiting on a
  file that no longer gets written) was stale. Added
  `infrastructure/s3_new_object_trigger.py`'s `S3NewObjectTrigger`, a custom
  `BaseEventTrigger` that fires once per newly observed S3 key under a
  prefix (plain `S3KeyTrigger` is explicitly documented as unsafe for
  event-driven scheduling — it stays true forever once a key exists, causing
  infinite re-triggering). `dags/ingest_orders_bronze.py` now declares
  `ORDERS_RAW_ASSET` watched by that trigger and schedules off it; the task
  reads the extract date off the triggering Asset event's key (falling back
  to the `date` param for manual triggers) and re-globs
  `orders/date=<date>/*.csv` for that day rather than a single file, so the
  existing `replaceWhere`-per-partition write stays correct and idempotent
  across repeated per-window triggers. Verified `airflow dags list-import-errors`
  is clean and the DAG's timetable now reports `Asset`/`Triggered by assets`.
  Left `generate_orders`/`generate_fulfillment`/`generate_payments`/
  `generate_rider_events` untouched, per instruction not to touch the raw
  generators. Fulfillment/payments Bronze ingestion (still daily-cadence
  sources) unchanged.

- **2026-08-31 — switched orders and rider_events generators to 5-minute cadence** (`dags/generate_orders.py` and `dags/generate_rider_events.py` now use `schedule="*/5 * * * *"` instead of daily crons). Fulfillment and payments remain daily (they're genuine daily exports in the source systems). Ingestion DAGs unchanged — still daily consumers for now. File naming (`orders_<run_timestamp>.csv`, etc.) already works with 5-min granularity via second-precision timestamps in `%Y%m%dT%H%M%SZ` format.

- **2026-08-30 — built and verified the orders Bronze ingestion DAG
  (`dags/ingest_orders_bronze.py`) — normally engineer-owned platform logic
  per `CLAUDE.md`'s Bronze→Silver→Gold boundary, built by Claude Code only
  because the engineer explicitly opted to change that boundary for this one
  piece of work when asked directly (flagged before proceeding, not assumed).
  Not a standing precedent — see the new "Exception on record" note under
  Ownership above.** Reads `s3a://raw-incoming-data/orders/orders_<date>.csv`
  with `metadata.orders_schema.OrdersSchema.RAW` applied explicitly (no
  `inferSchema`, `mode=FAILFAST` so a row that doesn't match fails the task
  loudly instead of landing corrupted), tags each row with `_extract_date`/
  `_bronze_ingested_at`/`_source_file` (via `input_file_name()`, genuine lineage
  back to the exact raw object read) and writes to
  `s3a://bronze-veloz/orders/`. Deliberately append-only *across* extract
  dates (an order's state can legitimately reappear across days as it
  progresses — collapsing that to one row per order is Silver's documented
  job, not Bronze's), but idempotent *within* one extract date via Delta's
  `replaceWhere` (atomically overwrites just that day's `_extract_date`
  partition), so a retry/backfill/manual re-trigger for the same date can't
  double-count rows.
- **`metadata/` had never been mounted into any container — fixed as a
  needed infra change, not a design decision.** Added
  `./metadata:/opt/airflow/plugins/metadata` to `docker-compose.yml`'s
  `x-airflow-common` volumes (nested under `plugins/`, not its own top-level
  mount, since Airflow's plugin manager already puts `./plugins` on
  `sys.path` — confirmed by how `spark_session.py` imports as bare
  `spark_session` — so nesting `metadata/`, which already has its own
  `__init__.py`, makes `from metadata.orders_schema import OrdersSchema`
  work with zero `PYTHONPATH` wiring). `docker compose up -d` picked up the
  new mount without a rebuild; confirmed via `docker compose exec
  airflow-scheduler ls /opt/airflow/plugins/metadata/` and `airflow dags
  list-import-errors` (empty).
- **Verified end-to-end for real against the live stack, not just
  statically.** Unpaused and triggered `ingest_orders_bronze` for
  `2026-08-30` (a date with a genuine 6000-row orders extract already in
  MinIO from an earlier session). First attempt failed on a transient Ivy
  jar-download flake (`aws-java-sdk-bundle` download failed mid-resolve,
  `JAVA_GATEWAY_EXITED`) — unrelated to this DAG's own logic, and the
  built-in `retries=3` picked it up automatically: attempt 2 succeeded,
  task log confirmed `read 6000 rows for extract_date=2026-08-30` and
  `wrote 6000 rows to s3a://bronze-veloz/orders/`. The DAG's own missed
  `01:10 UTC` scheduled run for the same date fired at the same time
  (unpausing a DAG runs its latest missed interval — same Airflow behavior
  already documented elsewhere in this log) and also succeeded, writing to
  the *same* partition. Read the Delta table back directly with a fresh
  Spark session afterward (not trusted from the task log alone): `mc ls -r`
  showed two physical parquet files under `_extract_date=2026-08-30/` (the
  no-delete `veloz-ingest` role can't remove the superseded one — expected,
  same mechanism documented for every other Bronze/Silver/Gold write in this
  project), but `spark.read.format("delta").load(...)` correctly resolved to
  exactly 6000 logical rows, not 12000 — proving `replaceWhere` genuinely
  made the second write idempotent rather than appending duplicates.
  Separately triggered the DAG for `2020-01-01` (a date with no orders
  extract) to check the documented failure mode isn't just asserted: task
  log showed `[PATH_NOT_FOUND] Path does not exist:
  s3a://raw-incoming-data/orders/orders_2020-01-01.csv.` on the very first
  attempt — clear and immediate, not a hang or a silent empty write. That
  run was left retrying/failing in the Airflow UI on purpose (a real-looking
  failed run is useful evidence, and it's a throwaway date) rather than
  cleared.
- **2026-08-30 — migrated the compute layer from Hadoop YARN to Spark
  Standalone** (`spark-master`/`spark-worker` in `docker-compose.yml`, built
  from the same image as the Airflow containers rather than a bespoke Hadoop
  image — pip's `pyspark==3.5.3` doesn't ship `sbin/start-master.sh`/
  `start-worker.sh`, confirmed directly against this repo's own `.venv`, so
  `bin/spark-class` is invoked directly instead). `docker/hadoop/` deleted,
  `plugins/spark_session.py`'s `YarnSparkSessionFactory` replaced by
  `StandaloneSparkSessionFactory`/`StandaloneClusterConfig`,
  `dags/spark_yarn_smoke_test.py` renamed to
  `dags/spark_standalone_smoke_test.py`. New `.env` knobs:
  `SPARK_WORKER_COUNT`/`SPARK_WORKER_CPU_LIMIT`/`SPARK_WORKER_MEM_LIMIT`/
  `SPARK_MASTER_UI_PORT`. Credentials posture: no `spark.authenticate`
  wired in — kept the trust-the-Docker-network posture already used
  everywhere else in this stack (and already used, unauthenticated, by the
  YARN cluster this replaces) — **flagged for the engineer to confirm, not
  decided silently**; see `docs/ADR.md`'s new appendix entry for the
  reasoning and the exact tradeoff. Executor sizing defaults
  (`spark.executor.cores`/`.memory`/`spark.cores.max`) are sourced from the
  same `SPARK_WORKER_CPU_LIMIT`/`MEM_LIMIT` values driving the worker
  containers' own cgroup caps, so a default request can't exceed what one
  worker can deliver.
  Also surfaced, not silently fixed: the *previous* divergence (in-process
  Spark → Hadoop YARN) was never given its own ADR entry or session-log
  line despite `docker-compose.yml`/`docker/hadoop/Dockerfile` citing one —
  see `docs/ADR.md`'s new appendix for the full note. Verified for real, not
  assumed: `docker compose up -d` brought up 5/5 workers at exactly
  2 cores/4096 MB each (`.env` defaults), `docker inspect` confirmed those
  as real `NanoCpus`/`Memory` cgroup values, editing
  `SPARK_WORKER_COUNT`/`CPU_LIMIT`/`MEM_LIMIT` and re-running
  `docker compose up -d` changed the Master UI's reported worker count and
  per-worker cores/memory accordingly (3 workers at 1 core/2048 MB, then
  reverted), and `airflow dags trigger spark_standalone_smoke_test` (real
  Airflow execution, not a bare script call) reached `success` with its task
  log showing `spark master: spark://spark-master:7077` (not `local[*]`), a
  real application id, and a completed Delta-over-s3a round trip (3 rows
  read back). Stack torn down (`docker compose down`) at the end of this
  session, `.env` restored to checked-in defaults.
- **2026-08-30 — resolved the bucket-name blocker from earlier this session
  and verified the MinIO landing zone for real (not just against a scratch
  bucket).** Engineer chose `raw-incoming-data` (hyphenated) over the
  originally-requested `raw_incoming_data` (underscore, invalid per S3
  bucket-naming rules) when asked directly. Propagated the name across every
  file the prior session had staged behind a single constant, plus the docs:
  `DEFAULT_BUCKET` in `generators/s3_io.py`, `RAW_BUCKET` in all four
  `dags/generate_*.py`, both `docker/minio/policies/*.json` ARNs (already
  present, just needed the string fixed), the `mc mb` line in
  `docker-compose.yml`'s `minio-init`, `docs/data-sources.md`,
  `docs/ADR.md`, and the unused `metadata/buckets.py` (a `Buckets.RAW_INCOMING_DATA`
  constant nothing in the codebase actually imports — left in place rather
  than deleted since `rm` was denied by the permission classifier, but fixed
  its value/docstring for consistency; worth deleting outright in a session
  where that's not blocked, since it duplicates rather than centralizes the
  per-file constants by the repo's own "no shared module" convention for
  DAGs).
- Verified end-to-end against the real stack, not assumed: `docker compose up
  --build -d` (image rebuild needed — `boto3` was added to the Dockerfile
  last session), all 7 services reached healthy. `minio-init` log confirmed
  `Bucket created successfully veloz/raw-incoming-data` alongside the three
  existing buckets. Triggered all four generator DAGs for `2026-08-30` via
  `airflow dags trigger` (real Airflow execution path, not a direct script
  call) — `generate_orders` first, then `generate_payments`/
  `generate_rider_events`/`generate_fulfillment`; all four reached `success`.
  `mc ls -r veloz/raw-incoming-data` shows the exact folder shape the local
  `data/raw/` used to have: `orders/orders_2026-08-30.csv`,
  `fulfillment/date=2026-08-30/STORE-001.csv` through `STORE-030.csv`,
  `payments/payments_2026-08-30.csv`, `rider_events/rider_events_2026-08-30.jsonl`.
  Row-counted each via `mc cat | wc -l`: 6000 orders, 4589 payments (~76%,
  consistent with the delivered-orders commission spec), 34307 rider events
  — confirms `payments`/`rider_events` genuinely read that date's orders
  extract from S3 (not empty, not stale), the specific risk flagged when this
  work was delegated.
- Net effect: the "Generators → MinIO landing zone" infra row is now `[x]`
  for real. Next actual step is unchanged from before this rename detour:
  start the platform build with the orders Bronze ingestion DAG (engineer's
  own work per `CLAUDE.md`), which now reads its input from
  `s3://raw-incoming-data/orders/...` instead of local `data/raw/orders/`.
- **2026-08-30 — `coder`: moved all four generators' raw output from local
  `data/raw/` to MinIO, at the engineer's request — but hit and correctly
  flagged a real naming conflict rather than resolving it unilaterally.**
  Added `generators/s3_io.py` (boto3 helpers: `get_s3_client`, `read_csv`,
  `write_csv`, `write_text`), reading `MINIO_ENDPOINT` from env and using
  boto3's default credential chain (already wired to the no-delete
  `veloz-ingest` role for every Airflow container). All four generators'
  write path (and, for `payments.py`/`rider_events.py`, their orders-extract
  *read* path too — they consume that day's orders file as an input) now go
  through it instead of `Path.mkdir()`/`open()`/`df.to_csv()`. `--output-dir`/
  `--orders-dir` kept their names but became S3 key prefixes; new `--bucket`
  flag added. Updated all four `dags/generate_*.py` to pass the new CLI
  surface; confirmed none of their `subprocess.run()` calls pass an explicit
  `env=`, so the container's MinIO env vars reach the generator subprocess by
  inheritance with no DAG change needed there. Added `boto3==1.43.83` to
  `docker/airflow/Dockerfile`.
- **The real conflict, found empirically and flagged rather than
  silently resolved:** the engineer's requested bucket name,
  `raw_incoming_data` (underscore), is not a valid S3/MinIO bucket name —
  `mc mb veloz/raw_incoming_data` fails with `Bucket name contains invalid
  characters` (the real AWS DNS-compliant naming rule enforced since 2018,
  not a MinIO quirk), confirmed directly against the same `mc` image used by
  `minio-init`, while a hyphenated equivalent created and removed cleanly.
  Per instruction not to silently rename to match the existing
  `bronze-veloz`-style convention, left the bucket uncreated and the decision
  for the engineer: `minio-init` does **not** attempt to create it (that line
  was tried once, broke the whole bootstrap under `set -e`, and was
  reverted — confirmed idempotent again after reverting), while the ARNs for
  it were pre-staged in both `docker/minio/policies/*.json` (harmless even
  unused — `mc admin policy create` doesn't validate that a referenced ARN is
  a creatable bucket). Every generator/DAG targeted the placeholder name by
  default via one constant each (`DEFAULT_BUCKET` in `generators/s3_io.py`,
  `RAW_BUCKET` in each `dags/generate_*.py`), so fixing the name later would
  be a single-line change per file — this is exactly what happened above.
- Verified what could be verified without the real bucket: full I/O
  round-trip against a disposable scratch bucket
  (`raw-incoming-data-scratch`, deleted after) — all four generators ran for
  real, correct row counts, `payments`/`rider_events` genuinely read that
  date's orders from S3, folder shape matched the old local layout via
  `mc ls -r`. Also verified the Airflow-specific path (env inheritance +
  IAM policy scoping) by temporarily pointing the DAGs' `RAW_BUCKET` at the
  scratch bucket and triggering via `airflow dags trigger` (not a bare script
  call) — got `AccessDenied` (not `NoCredentialsError`), proving both
  mechanisms work, then reverted the DAGs back immediately, confirmed via
  `py_compile` + `airflow dags list-import-errors` (empty). Final state
  before this rename: `docker compose ps` all 6 services healthy, `mc ls
  veloz` showed only the original 3 buckets, no import errors.
- Updated `docs/data-sources.md` (all four Output lines, new "Landing zone"
  section with the old-path→new-key mapping and the naming issue written up
  in full) and `docs/ADR.md` (matching Output/Trigger-command updates — the
  trigger command changed to `docker compose exec airflow-scheduler python
  ...` since boto3/`MINIO_ENDPOINT` aren't available to the bare local
  `.venv` anymore). Left existing local `data/raw/**` files untouched —
  historical, not deleted.

- **2026-08-30 — `coder`: moved all four generators' output from local
  `data/raw/` to MinIO (infra + generator work, no platform-layer boundary
  involved) — blocked on one open item, flagged below rather than resolved
  unilaterally.** Added `generators/s3_io.py` (shared boto3 helpers —
  `get_s3_client`/`read_csv`/`write_csv`/`write_text`, same "shared by import"
  pattern as `reference_data.py`), pointed at MinIO via `MINIO_ENDPOINT`
  (required, no default, matching `dags/spark_minio_smoke_test.py`'s
  convention) and picking up `veloz-ingest` credentials automatically through
  boto3's default `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` env chain — no
  new credential wiring needed, containers already have them. All four
  generators (`orders.py`, `fulfillment.py`, `payments.py`, `rider_events.py`)
  now write through it instead of `Path.mkdir()`/`open()`/`df.to_csv(path)`;
  `payments.py`/`rider_events.py`'s orders-extract read moved the same way,
  raising `FileNotFoundError` with an `s3://` path in the message so the
  existing "generate orders first" error stays accurate. CLI ergonomics kept
  close to before: `--output-dir`/`--orders-dir` are unchanged flag names but
  now S3 key prefixes (defaults: the bare source name, e.g. `orders`, not a
  local path) instead of local dirs; a new `--bucket` flag (default
  `raw_incoming_data`) selects the bucket. Updated all four
  `dags/generate_*.py` to pass `--bucket`/the new prefix defaults instead of
  `DATA_DIR / "<source>"`; confirmed none of the four `subprocess.run(...)`
  calls pass an explicit `env=`, so the container's `MINIO_ENDPOINT`/
  `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` reach the generator subprocess
  by inheritance with no DAG change needed there. Added `boto3==1.43.83` (the
  current release on PyPI at build time) to `docker/airflow/Dockerfile`'s pip
  install list, next to the existing pinned `pyspark`/`delta-spark`/`faker`.
- **Real, load-bearing conflict found and flagged, not silently resolved:**
  the engineer's specified bucket name, `raw_incoming_data` (underscore),
  cannot be created — confirmed directly against the deployed MinIO, not
  assumed: `mc mb veloz/raw_incoming_data` fails with `Bucket name contains
  invalid characters`, while the otherwise-identical hyphenated
  `raw-incoming-data-test` creates and removes cleanly in the same command
  sequence. This is the real AWS S3 bucket-naming rule (DNS-compliant names
  only, no underscores, since 2018), which MinIO enforces the same way — not
  a MinIO quirk to route around. Per explicit instruction not to silently
  rename to match the hyphenated `bronze-veloz` convention myself, this is
  left for the engineer to decide. **Current repo state reflects the
  decision as still open, not resolved either way:** `docker-compose.yml`'s
  `minio-init` does **not** attempt to create `raw_incoming_data` (that line
  was added, hit the failure below, then reverted) — a comment at that line
  in the compose file explains why and lists exactly what to touch once a
  name is chosen. The ARNs for `raw_incoming_data` **are** already staged in
  both `docker/minio/policies/{ingest,maintenance}-policy.json`'s `Resource`
  lists (confirmed harmless: `mc admin policy create` doesn't validate that a
  referenced bucket ARN is a creatable name, and re-running `minio-init`
  twice with those ARNs in place still exits 0 both times — forward-declared,
  not actively broken). Every generator/DAG already targets
  `raw_incoming_data` by default via one constant each
  (`DEFAULT_BUCKET` in `generators/s3_io.py`, `RAW_BUCKET` in each
  `dags/generate_*.py`), so finishing this once the name is decided is a
  small, mechanical edit in a handful of known places, not a design change.
- **First attempt at `docker compose up --build -d` with `raw_incoming_data`
  in the `mb` list caught this for real, not in review:** `minio-init` logged
  the three real buckets created successfully, then the `mc: <ERROR>` above,
  then exited 1 (`set -e` aborted the rest of the script — the policy/user
  creation steps never ran that pass). Reverted the `mb` line, re-ran
  `docker compose run --rm minio-init` twice back to back — both exits 0,
  identical output, confirming the stack is back to the same idempotent
  3-bucket state verified in the 2026-08-28 session, undamaged by the
  attempt.
- **S3 I/O layer verified end-to-end anyway, against a disposable scratch
  bucket (`raw-incoming-data-scratch`, deleted afterward)** so the actual
  code change didn't go unverified just because the real bucket name is
  blocked. Ran all four generators for real inside `airflow-scheduler` (root
  MinIO creds, since `veloz-ingest`'s policy doesn't cover a bucket outside
  its ARN list) against a fresh date: `orders.py` wrote 200 rows;
  `fulfillment.py` wrote a clean night (30/30) and a `--bad-night` run
  (23 clean/4 malformed/3 missing of 30, matching the documented rates);
  `payments.py` read the 156 delivered orders from S3 (not empty) and wrote
  155 payments (1 correctly dropped, matching `MISSING_PAYMENT_RATE`);
  `rider_events.py` read the same orders file and wrote 1128 events (498
  status_change/630 location_ping) for 182 active orders. `mc ls -r` on the
  scratch bucket confirmed the exact same relative key shape as the old local
  layout (`orders/orders_<date>.csv`, `fulfillment/date=<date>/<store>.csv`,
  `payments/payments_<date>.csv`, `rider_events/rider_events_<date>.jsonl`).
  Hand-verified with a boto3/pandas script, not just trusted: every
  `payments.csv` `order_id` is a genuine subset of that date's delivered
  orders read back from S3, and the `--bad-night` fulfillment object's
  content is actually the same schema/row shape as before (spot-checked raw
  bytes, not re-derived).
- **Also used the scratch bucket to prove the Airflow-orchestration path
  specifically, not just the generator script in isolation:** temporarily
  pointed all four DAGs' `RAW_BUCKET` constant at the scratch bucket,
  confirmed `airflow dags list-import-errors` stayed empty, then triggered
  all four via `airflow dags trigger` (the real scheduler path, not
  `docker exec`). All four tasks failed — correctly, not a bug: `botocore...
  AccessDenied` on `PutObject`, because the container's real credentials are
  `veloz-ingest`, whose policy doesn't grant the scratch bucket. This is
  actually the useful confirmation: an `AccessDenied` (not
  `NoCredentialsError`) proves `subprocess.run(cmd, capture_output=True,
  text=True)`'s lack of an explicit `env=` really does inherit
  `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`/`MINIO_ENDPOINT` from the
  container into the generator subprocess, and that MinIO's policy scoping
  denies out-of-policy buckets exactly as designed — the same mechanism that
  will gate the real bucket correctly once its ARN is live. Reverted all four
  DAGs' `RAW_BUCKET` back to `raw_incoming_data` immediately after (confirmed
  via `grep`, then `py_compile` + `airflow dags list-import-errors` empty
  again), and deleted the scratch bucket and its contents via `mc rm -r
  --force` + `mc rb`.
- **Docs updated to match:** `docs/data-sources.md` — all four "Output" lines
  now point at `s3://raw_incoming_data/...`, plus a new "Landing zone (MinIO,
  not local `data/raw/`)" section (old-path → new-key mapping table, and the
  open bucket-naming issue written up in full so it isn't lost between
  sessions). `docs/ADR.md`'s appendix — same four Output lines, and every
  Trigger command switched from `.venv/bin/python ...` to `docker compose
  exec airflow-scheduler python ...` (boto3 isn't installed in the local
  `.venv`, only in the Airflow image, and `MINIO_ENDPOINT` is only set inside
  the compose network — direct local-venv invocation of the generators no
  longer works post-migration, which is a real, worth-knowing behavior change
  from before).
- **Final state confirmed healthy:** `docker compose ps` shows all 6 services
  healthy/running, `mc ls veloz` shows the original `bronze-veloz`/
  `silver-veloz`/`gold-veloz` (plus a pre-existing, unrelated
  `tutorial-veloz` from an earlier notebook session) and nothing left behind
  from the scratch-bucket test, `airflow dags list-import-errors` empty.
  **Not verified, because it's the blocked item:** an actual end-to-end run
  against `raw_incoming_data` itself — that's the next step, once the
  engineer decides the real bucket name (see "Open issue" above and in
  `docs/data-sources.md`).
- **2026-08-28 — `coder`: deployed and fully verified the MinIO object-storage
  layer built last session (deploy/verification only, no design changes —
  bucket names, IAM scope and credentials untouched).** `docker compose up
  --build -d`: all 6 services reached healthy/completed
  (`postgres`/`minio`/`airflow-apiserver`/`airflow-scheduler`/
  `airflow-dag-processor`/`airflow-triggerer` healthy; `airflow-init`/
  `minio-init` exited 0). `minio-init` log confirmed every step actually ran,
  not a silent no-op: 3 buckets created, both policies created, both users
  added, both policies attached. Re-ran `minio-init` a second time
  (`docker compose run --rm minio-init`) — exited 0 again with identical
  output and no errors, confirming idempotency for real (previously flagged
  untested). Verified the bucket/IAM state independently via `mc` (not just
  trusting the init log): `mc ls` shows all 3 buckets, `mc admin user list`
  shows both users enabled, `mc admin policy list` shows both custom policies
  alongside MinIO's built-ins, and `mc admin user info` on each user confirms
  the correct policy attached (`veloz-ingest` → `veloz-ingest-no-delete`,
  `veloz-maintenance` → `veloz-maintenance-full`).
- **The load-bearing check, done directly, not inferred from the policy
  JSON:** `mc cp` a throwaway object into `bronze-veloz` using `veloz-ingest`
  credentials (succeeded, as expected — ingest can write), then `mc rm` the
  same object with the same `veloz-ingest` credentials — denied:
  `mc: <ERROR> Failed to remove ... Access Denied.` (exit 1). Re-checked with
  `mc stat` under maintenance creds that the object was still there (ingest's
  denied delete genuinely didn't remove it), then `mc rm` the same object
  with `veloz-maintenance` credentials — succeeded (`Removed ...`, exit 0),
  and a follow-up `mc stat` confirmed it was actually gone. Two-role design
  behaves exactly as documented: ingest can write but never delete,
  maintenance can do both.
- Unpaused and triggered `spark_minio_smoke_test`: run reached `success`
  (confirmed both via task log and `airflow dags state`). Ivy resolved the
  exact pinned versions (`hadoop-aws:3.3.4`, `aws-java-sdk-bundle:1.12.262`,
  first-run cost ~9s for the SDK bundle, well under budget) with no version
  conflicts. Task log confirms the actual round-trip, not just a green run:
  `minio endpoint: http://minio:9000`, `smoke test path:
  s3a://bronze-veloz/_smoke_test/`, a 3-row `+---+` table, and `rows read
  back from MinIO: 3`, matching `spark.range(3)`. No code changes were needed
  — DAG and compose config worked as built.
- One real, expected (not a bug) behavior surfaced in the task log worth
  recording: the Delta overwrite write emitted a non-fatal `WARN
  MultiObjectDeleteSupport: ... AccessDenied` while S3A tried to clean up
  directory markers post-write — the ingest role's lack of `s3:DeleteObject`
  blocking that internal housekeeping call exactly as intended, not a
  malfunction. It didn't fail the task (S3A treats it as a warning), and it's
  the same mechanism the DAG's own docstring already called out: the ingest
  role can't clean up after itself, so `_smoke_test/` is deliberately left
  behind in `bronze-veloz` after this run.
- Nothing needed fixing — no typos, no wrong env var references, no missing
  `depends_on`. Checked off the "MinIO + `s3a://`" row in the Infra table.
  Next actual step is unchanged from before this session: start the platform
  build with the orders Bronze ingestion DAG (engineer's own work).
- **2026-08-27 — `coder`: built the MinIO object-storage layer (build only —
  nothing deployed or verified this session, by instruction).** Added `minio`
  (`minio/minio:RELEASE.2025-04-22T22-12-26Z`) and a one-shot `minio-init`
  (`minio/mc:RELEASE.2025-04-16T18-13-26Z`) to `docker-compose.yml`, plus
  `docker/minio/policies/{ingest,maintenance}-policy.json`,
  `dags/spark_minio_smoke_test.py`, `docs/minio-user-guide.md`, `.env`/
  `.gitignore`/`data/minio/.gitkeep`. Three buckets, one per layer
  (`bronze-veloz`/`silver-veloz`/`gold-veloz`), on a `./data/minio` bind mount
  so the object store is host-inspectable like `./data/raw`. Two IAM roles
  rather than one set of root credentials: `veloz-ingest` (get/put/list +
  multipart, explicitly **no** delete) is wired to `AWS_ACCESS_KEY_ID`/
  `AWS_SECRET_ACCESS_KEY` in `x-airflow-common`, so every Spark session picks
  up the no-delete role by default via the standard credential chain;
  `veloz-maintenance` (`s3:*`) is passed through under its own names only, so
  `OPTIMIZE`/`VACUUM` has to ask for delete rights on purpose and no scheduled
  DAG can acquire them by accident. Delta is unaffected by the default role —
  overwrite/MERGE/DELETE write new commits and never physically delete, which
  is what keeps a bad DAG run recoverable by time travel (G2/G3).
- Version choices are checked, not assumed: `hadoop-aws:3.3.4` matches the
  Hadoop that pyspark 3.5.3 actually bundles (`hadoop-client-api-3.3.4.jar` in
  the venv's pyspark jars) and `aws-java-sdk-bundle:1.12.262` is what
  `hadoop-project-3.3.4.pom` pins for it. Both resolved at runtime via
  `configure_spark_with_delta_pip(..., extra_packages=[...])`, matching the
  existing Delta pattern — no jars baked into the Dockerfile, which is
  therefore untouched. MinIO server pinned to the last release with the full
  embedded console (RELEASE.2025-05-24 deprecated it and moved it to the
  separate object-browser project, dropping user/policy management from the
  UI) — confirmed from MinIO's own release notes, and both image tags pulled
  successfully. `minio-init` idempotency is by construction, confirmed against
  the mc source for that tag: `mb --ignore-existing`, `policy create`
  overwrites, `user add` overwrites, and `policy attach` swallows
  `XMinioAdminPolicyChangeAlreadyApplied`.
- Validated statically only: `docker compose config` parses, the new DAG
  byte-compiles, both policy JSONs parse. **Not verified:** nothing was brought
  up — no bucket exists yet, no s3a write has happened, and the no-delete
  policy has not been exercised. Next session: `docker compose up -d`, check
  `minio-init` exits 0 (twice, to prove idempotency), run
  `spark_minio_smoke_test`, then flip the "MinIO + `s3a://`" row in the Infra
  table — left `[ ]` here deliberately.
- **2026-08-27 — removed Asset-based scheduling between the four generator
  DAGs, at the engineer's explicit direction:** the prior design had
  `generate_orders` declare an Asset outlet (`veloz_orders_extract`) that
  `generate_payments`/`generate_rider_events` scheduled off, so they'd only
  fire after an orders run completed. Engineer's call: these DAGs each
  simulate an independent upstream system, and a real upstream doesn't push a
  completion event into your pipeline — that coupling was an artifact of the
  simulation, not something a real integration would have. Removed the
  `Asset` import/object and `outlets=[...]` from `dags/generate_orders.py`;
  removed the Asset import/object and `schedule=[ORDERS_ASSET]` from
  `dags/generate_payments.py`/`dags/generate_rider_events.py`, replacing
  each with its own fixed daily cron offset after `generate_orders`' 01:00
  UTC run (payments 01:15, rider_events 01:20) — mirrors how
  `generate_fulfillment` (01:05, no orders dependency at all) already worked.
  The generators' own `FileNotFoundError` (unchanged) is now the sole
  backstop if orders hasn't landed by the time payments/rider_events run,
  rather than a backstop for an edge case on top of a guarantee — a real
  behavior change worth knowing: a late or failed orders run now means
  payments/rider_events fail loudly at their scheduled time instead of simply
  not having started yet. Updated `docs/data-sources.md`'s "Generator CLI
  overrides" section to match. Verified for real: `airflow dags
  list-import-errors` empty and `airflow dags list` shows all four DAGs
  loading cleanly after the change.
- **2026-08-27 — docs-only session (no platform code written; engineer
  explicitly scoped this to docs):** reviewed `docs/data-sources.md`,
  `docs/ADR.md`, `docs/architecture-diagram.md`,
  `docs/data-sources-er-diagram.md` and this file, then rewrote
  `docs/veloz-project-manual.md` to be a human-facing companion instead of a
  static roadmap: added a Status column to the Quick Reference table (ADR +
  Airflow infra marked done, everything else not started), added a new
  "Daily Log" section — a lightweight per-session template (Built/Decided/
  Surprised by/Next), pre-filled with a 2026-08-27 entry — distinct from
  this file's exhaustive agent session log, meant for the engineer's own
  portfolio-ready record of decisions and surprises. Marked Day 1's
  Setup/ADR subsection done instead of reading as an open to-do.
- Engineer then flagged a real gap and asked to confirm it before any
  build work: is the Bronze layer accounted for? Checked `dags/` and
  `data/raw/` directly — confirmed no Bronze layer exists anywhere yet. The
  four generator DAGs only produce raw files (`data/raw/{orders,
  fulfillment,payments,rider_events}/`); nothing ingests them into Delta,
  and no `bronze.*` table exists. The manual itself had the same gap: it
  jumped straight from raw files to Silver targets for both Day 1 (orders)
  and Day 2 (fulfillment) with no Bronze step named, even though
  `docs/architecture-diagram.md` and `CLAUDE.md`'s reference architecture
  are both explicit that the lakehouse is Bronze → Silver → Gold. Added an
  explicit "Bronze layer" explainer to the manual (why it's a separate step
  from Silver, tied to G2's audit requirement and G3's resilience) plus
  Bronze-specific targets ahead of the existing Silver targets on both
  days — for fulfillment specifically, clarified that `--bad-night`
  quarantine logic belongs at the Bronze read step (a file/row that won't
  parse can't land in Delta at all), not at Silver.
- Net effect: no facts about what's built changed this session (see table
  below — Bronze ingestion DAGs are still the first unstarted platform-build
  milestone, unchanged), only the documentation's accuracy did. Next actual
  step is unchanged: start the platform build with the orders Bronze
  ingestion DAG, per "Right now" above.
- `coder`: built Airflow DAGs orchestrating the four generators
  (full-delegate infra/generator work, no platform-layer logic). One DAG per
  generator — `dags/generate_orders.py`, `dags/generate_fulfillment.py`,
  `dags/generate_payments.py`, `dags/generate_rider_events.py` — each a
  self-contained file (no shared `_common.py`, per explicit instruction; the
  ~15 lines of subprocess-invocation/date-fallback logic are duplicated
  across the four instead) tagged identically (`["infra", "generator"]`) for
  a consistent UI grouping. Each DAG exposes its generator's tunable knobs as
  `airflow.sdk.Param`s for "Trigger DAG w/ config": `orders` gets
  `status_weights` (previously a hardcoded `STATUS_WEIGHTS` constant);
  `fulfillment` gets the four `--bad-night` severity knobs (previously only
  togglable on/off); `payments` gets the wrong-amount/missing-payment rates
  and settlement-lag mean/stdev; `rider_events` gets ping count and jitter
  radius. All of these were added as new optional CLI flags on the
  generators themselves first (defaulting to the existing constants, so
  direct CLI invocation is unchanged) — the DAGs are a thin pass-through
  onto those flags, not a reimplementation, per the explicit instruction
  that generators stay the source of truth. Each DAG's task shells out via
  `subprocess.run([sys.executable, generators/<script>, *args])` rather than
  importing the generator in-process: keeps the generator's own argparse as
  the single source of truth for its CLI surface, and a generator crash
  can't take the Airflow task's interpreter down with it.
- Scheduling: `generate_orders` runs on a real daily cron (`0 1 * * *`) and
  declares an Asset outlet (`veloz_orders_extract`, matched by name+uri
  across files, not a shared import) on its output directory.
  `generate_fulfillment` has no dependency on orders (it never reads the
  orders extract) and runs on its own cron (`5 1 * * *`).
  `generate_payments`/`generate_rider_events` schedule off that Asset
  instead of a fixed cron — they only run after *some* orders run has
  completed, which is a "real schedule" in Airflow's sense, not manual-only,
  and inherently satisfies the ordering requirement without needing a
  per-date-parameterized Asset URI (which Airflow's Asset model doesn't fit
  cleanly for a daily-renamed file like `orders_<date>.csv` without an
  AssetAlias, judged unnecessary complexity for this scope). The generators'
  pre-existing `FileNotFoundError` when that exact date's file is missing is
  the backstop for the case an Asset-triggered run still doesn't have the
  right day's file (e.g. an out-of-band manual trigger naming a different
  date).
- Real bug caught during validation, not left as "probably fine": asset-
  triggered and manual (no-config) DAG runs have no `logical_date`/`ds` in
  Airflow 3 (only cron-scheduled runs do), so `params["date"] or
  context["ds"]` raised `KeyError: 'ds'` the first time `generate_payments`/
  `generate_rider_events` actually fired off the orders Asset event — caught
  by watching the triggered runs fail, not assumed. Fixed in all four DAGs
  to `params["date"] or context.get("ds") or pendulum.now("UTC").to_date_string()`,
  matching each generator's own "default to today" behavior when no date is
  given.
- Also added `faker==40.37.0` to `docker/airflow/Dockerfile`'s pip install —
  verified empirically the base `apache/airflow:3.3.1-python3.11` image
  ships `pandas` (3.0.5, already matching `.venv`) but not `faker`, which
  `reference_data.py` needs. Rebuilt the image (`docker compose build`, no
  need for `down -v` this time — an image update, not a metadata-schema
  bump) and brought the stack back up: all 5 services healthy,
  `airflow dags list-import-errors` empty for all 5 DAGs (4 new +
  `spark_delta_smoke_test`).
- Validated for real: unpaused all four, triggered `generate_orders` with a
  config override (`num_orders: 500, seed: 99, status_weights: {delivered:
  0.5, cancelled: 0.5, picked_up/assigned/created: 0}`) — run succeeded,
  `orders_2026-08-27.csv` came out as exactly 500 rows, 256
  delivered/244 cancelled, no other statuses, confirming the override
  reached the generator. Triggered `generate_payments`/`generate_rider_events`
  against that same date: both succeeded, and every `order_id` in the
  resulting `payments_2026-08-27.csv`/`rider_events_2026-08-27.jsonl` was
  confirmed (by set-membership check against the orders file) to be a
  genuine subset of that date's delivered/active orders — not stale, not a
  different day. Also caught unpausing `generate_orders` immediately running
  its already-past `0 1 * * *` interval for the day (standard Airflow
  "run the latest missed interval on unpause" behavior, not a bug) and, via
  its Asset outlet, auto-triggering `generate_payments`/`generate_rider_events`
  — which is exactly the point of the Asset-based scheduling, confirmed
  working end-to-end before the `ds` bug above was even found.
- Fixed a pre-existing one-line typo in `docs/data-sources.md` section 3
  (fulfillment) that attributed the generator to `generators/orders.py`
  instead of `generators/fulfillment.py` — noticed while adding the new
  "Generator CLI overrides" section documenting the new flags above (defaults
  unchanged, just the new optional overrides and which DAG/Param exposes
  each one).
- `coder`: upgraded the local-dev Airflow stack from 2.10.5 to 3.3.1
  (full-delegate infra, no platform-layer boundary involved). Bumped
  `docker/airflow/Dockerfile`'s base image to `apache/airflow:3.3.1-python3.11`
  (JDK17/pyspark/delta-spark install untouched). Rewrote `docker-compose.yml`
  for Airflow 3's split topology: renamed `airflow-webserver` →
  `airflow-apiserver` (`api-server` command, healthcheck against
  `/api/v2/monitor/health`), added standalone `airflow-dag-processor` and
  `airflow-triggerer` services (both required/expected in Airflow 3, not
  bundled into the scheduler), added the new required env vars
  (`AIRFLOW__CORE__EXECUTION_API_SERVER_URL` pointing at the api-server,
  placeholder `AIRFLOW__API_AUTH__JWT_SECRET`/`JWT_ISSUER` — same local-dev-only
  insecurity posture as the existing empty `FERNET_KEY`), kept
  `AIRFLOW__CORE__AUTH_MANAGER: FabAuthManager` explicitly so the existing
  `.env` username/password admin flow keeps working (Simple Auth Manager is
  now the default and doesn't support settable credentials), and switched
  `airflow-init` to the env-var-driven pattern (`_AIRFLOW_DB_MIGRATE`,
  `_AIRFLOW_WWW_USER_CREATE` + username/password) with the init command now
  just doing directory setup and `/entrypoint airflow version`/`config list` —
  Airflow 3's built-in entrypoint script handles migrate + user creation.
  Added `./config` and `./plugins` bind mounts (with `.gitkeep`, ignored in
  `.gitignore` same pattern as `logs/`) to match the official reference.
  Updated `dags/spark_delta_smoke_test.py`'s import from
  `airflow.decorators` to `airflow.sdk` per the migration guide.
- Verified empirically, not just written: `apache-airflow-providers-fab`
  (3.8.0) is already bundled in the base 3.3.1 image — no Dockerfile change
  needed for FabAuthManager to load. The old `airflow.decorators import dag,
  task` path still works in 3.3.1 as a deprecated compat shim (emits
  `DeprecatedImportWarning` pointing at `airflow.sdk`), not a hard break —
  switched anyway since that's what the migration guide directs. Tore down
  the 2.10.5 stack with `docker compose down -v` (metadata-only volume, no
  real pipeline data, not worth a cross-major-version migration) — had to
  manually stop/remove one stale `airflow-webserver` container and its
  network left behind by the service rename, since compose no longer knew
  about a service by that name. Rebuilt and brought the 3.3.1 stack up fresh:
  all 5 services (`postgres`, `airflow-apiserver`, `airflow-scheduler`,
  `airflow-dag-processor`, `airflow-triggerer`) reached healthy,
  `airflow-init` exited 0 with confirmed log lines "Database migration done!"
  and `User "admin" created with role "Admin"`. `airflow version` inside the
  scheduler container reports `3.3.1`. `/api/v2/monitor/health` reports all
  four subsystems (metadatabase/scheduler/triggerer/dag_processor) healthy.
  Unpaused and triggered `spark_delta_smoke_test`: run state `success`, task
  log confirms `pyspark version: 3.5.3`, `delta-spark version: 3.2.1`, `spark
  version: 3.5.3` — identical to the pre-upgrade run, proving the Spark/Delta
  layer is unaffected by the Airflow major-version bump. No import errors
  (`airflow dags list-import-errors` empty), no exceptions in
  apiserver/scheduler logs (only routine Ivy dependency-resolution stderr
  noise from the Spark task, same as before). `docs/ADR.md` didn't reference
  the old version number or service name, so no doc changes needed there
  beyond this log entry.
- `coder`: built `generators/payments.py` and `generators/rider_events.py`
  against `docs/data-sources.md` sections 4/2 — the last two of the four
  data-source generators. `payments.py` reads a date's orders extract,
  filters to `delivered`, computes `commission_amount` from the documented
  tiered rate (10/13/16% by `order_total`) with the late-delivery deduction
  (>45min: -3pp, floored at 5%), settles at `delivered_at + lag` (lag ~
  Normal(20h, 6h) clipped to [2h, 48h]), and injects the two documented
  issues (~2% wrong `commission_amount` via a wrong-tier-or-error-factor
  mechanism, ~1% dropped entirely) — output columns are exactly
  `payment_id`/`order_id`/`rider_id`/`commission_amount`/
  `payment_recorded_at`, no derivation fields. `rider_events.py` reads the
  same orders extract, generates one `status_change` event per lifecycle
  timestamp reached (for every order with a non-null `assigned_at`) plus
  2-5 `location_ping` events jittered ±0.05° around the order's store-city
  center, written as JSON Lines in natural per-order generation order
  (deliberately not globally time-sorted, per spec). Both fail clearly if
  the requested date's orders extract doesn't exist.
- Smoke-tested both for real against `orders_2026-08-26.csv` (393 delivered,
  460 active orders) and a fresh 6000-order day for a larger sample: payments
  row count matched delivered × ~98.5-99%; hand-recomputing the commission
  formula for every row confirmed every non-injected row matches exactly and
  the injected-mismatch rate lands at ~1.9-2.4% and missing-row rate at
  ~1-1.5% across runs, consistent with the documented ~2%/~1%. rider_events
  JSONL parsed cleanly line-by-line, every active order had status_change
  coverage, all location_ping lat/lon were non-null and within the jitter
  radius of the correct city center, and adjacent-line timestamps showed
  231/2849 inversions confirming the file is genuinely not globally sorted.
  Caught and fixed two real bugs during validation, not left as "probably
  fine": (1) `uuid.uuid4()` draws from OS entropy, not the seeded `rng`, so
  `payment_id`/`event_id` weren't reproducible under a fixed `--seed` even
  though the row content was — switched both to a `uuid.UUID(int=rng.getrandbits(128), version=4)`
  helper, confirmed byte-identical output across two runs of the same seed
  afterward; (2) `order.order_total` read via pandas `itertuples` is a
  `numpy.float64`, and `round()` on it can disagree with `round()` on the
  bit-identical native Python float for values ending in `x.xx5` — this was
  silently inflating the mismatch rate with spurious non-injected
  discrepancies, fixed by casting to `float()` before computing commission.
- Fixed stale challenge-framing language in `generators/fulfillment.py`'s
  docstring (referenced the old "Day 2 challenge... currently skipped" —
  rewrote to say schema is fully specified in `docs/data-sources.md`, no
  drift by design). No behavior change. `generators/orders.py` had no such
  language. Updated `docs/data-sources.md`'s build-status table and the two
  "(to be built)" generator references, and added matching appendix entries
  (schema table, output path, trigger command, messiness-knob summary) for
  both new generators to `docs/ADR.md`.
- Pivoted away from the Challenge framework at the engineer's explicit
  request ("hated the challenges"). Rewrote `CLAUDE.md`,
  `docs/veloz-project-manual.md`, `docs/ADR.md`'s appendix, and this file's
  structure. Added `docs/data-sources.md` as the authoritative schema/rule
  handoff for all 4 sources, including a newly-designed (by Claude, per the
  engineer's request) commission-tier formula and settlement-lag
  distribution for the payments ledger, and a rider-events schema. Deleted
  `.claude/agents/challenge-tutor.md` (no longer applicable) and stripped
  Challenge-specific language from `coder.md`/`data-explorer.md`/
  `prompt-architect.md`. Next: `coder` builds `generators/payments.py` and
  `generators/rider_events.py` against the new spec.
- `coder`: built the baseline generators for `orders`/`fulfillment` (this
  predates the pivot above — the "Ch.1/Ch.2 skipped" framing below is
  historical). `generators/reference_data.py` — deterministic, fixed-seed
  (independent of each generator's own `--seed`) shared dimensions: 30
  stores round-robin across Medellín/Bogotá/São Paulo, 450 riders split
  evenly per city (`riders_for_store()` picks same-city riders only), a
  33-item SKU catalog across 7 categories. `generators/orders.py` — CLI
  (`--date`, `--num-orders`, `--output-dir`, `--seed`) generating one CSV
  extract row per order's *current* state (created→assigned→picked_up→
  delivered, or cancelled from any pre-delivery stage), realistic status mix
  (~78% delivered, ~7% cancelled, rest in-flight) and in-order lifecycle
  timestamps, to `data/raw/orders/orders_<date>.csv`. `generators/fulfillment.py`
  — CLI (`--date`, `--output-dir`, `--seed`, `--bad-night`) generating one CSV
  per store per day (`store_id,sku,date,quantity_on_hand,exported_at`) to
  `data/raw/fulfillment/date=<date>/<store_id>.csv`, one schema/unit/null
  convention for every store (empty string = "not counted today," same rule
  everywhere). `--bad-night` is baseline unreliability (G3): ~10% of stores'
  files missing entirely, ~15% arriving with a share of rows mechanically
  corrupted (truncated or extra-delimiter), seeded and reproducible.
- Smoke-tested both for real, not just `--help`: generated 500 orders
  (`--seed 7`), confirmed zero timestamp-ordering violations, `rider_id` null
  exactly for `created`/`cancelled`-before-assignment rows and populated
  everywhere else, `order_total` in a plausible $8–65 range. Generated a
  clean fulfillment night (30/30 stores, 0 malformed/missing) and a
  `--bad-night` run (23 clean / 4 malformed / 3 missing of 30) for the same
  seed twice, confirming byte-identical output — reproducible. Verified the
  malformed files are genuinely broken in the way a downstream reader would
  need to quarantine: `pandas.read_csv` raises `ParserError: Expected 5
  fields... saw 6` on the extra-delimiter rows and silently misaligns the
  truncated ones (no error, wrong shape).
- `coder`: built Airflow local-dev infra — `docker-compose.yml` (Postgres +
  airflow-init + webserver + scheduler, LocalExecutor, no Celery/Redis) and
  `docker/airflow/Dockerfile` (apache/airflow:2.10.5-python3.11 + OpenJDK 17 +
  pyspark==3.5.3/delta-spark==3.2.1 pinned to match the local venv). Added a
  trivial `dags/spark_delta_smoke_test.py` to prove Spark+Delta import inside
  the image.
- Wrote `docs/ADR.md` (Day 1 ADR) directly from the reference architecture
  table in `CLAUDE.md`/the manual — six decisions, each tied to a specific
  G1–G5 driver, plus consequences and alternatives considered.
- **Bring-up commands, for reference:**
  `docker compose up --build -d`, then `docker compose logs airflow-init` to
  confirm db migrate + admin user creation, `docker compose ps` for health,
  UI at `http://localhost:8080` (admin / veloz-local-dev from `.env`), then
  `docker compose exec airflow-scheduler airflow dags unpause spark_delta_smoke_test`
  and `... dags trigger spark_delta_smoke_test` (DAGs start paused).
- **Flagged, not fixed:** the local `.venv` is on Python 3.14.6, outside
  pyspark 3.5.3's documented support range (3.8–3.11). Pre-existing, not
  touched by this session — worth resolving before relying on the local venv
  for anything beyond what's already been installed.
- Started OrbStack and ran `docker compose up --build -d` for real: image
  built clean, `postgres`/`airflow-scheduler` healthy, `airflow-init` ran
  `airflow db migrate` + created the `admin` user and exited 0. Unpaused and
  triggered `spark_delta_smoke_test` — run state `success`; task log confirms
  `pyspark version: 3.5.3`, `delta-spark version: 3.2.1`, `spark version:
  3.5.3` printed from inside the container, matching the local venv exactly.
  Airflow local-dev infra is fully verified end-to-end, not just statically
  validated.
