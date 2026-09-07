# The Veloz Project — Client Brief & Build Manual

*This is your working brief, day-by-day build plan, and running log for the
Veloz data platform. It's written for you, not for an agent — for the
agent-facing rules and state, see `CLAUDE.md` (what Claude Code may build
and how) and `PROGRESS.md` (exhaustive session-by-session build state). This
file is where you plan the next piece of architecture to build and, just as
importantly, where you write down — in your own words, a few lines at a
time — what actually happened each day. Weeks from now, when you're
prepping for an interview, this is the file that answers "why did you build
it that way" faster than re-reading diffs.*

*The four upstream data sources are fully specified upfront — schemas,
business rules, and known messiness are documented in `docs/data-sources.md`
and completely emulated by generators under `generators/` (all four are
built — see the Daily Log). The work ahead is building the actual platform
on top of them: ingestion, cleaning, reconciliation, dashboards, and
streaming — the same kind of work it would be against a real system, and
something you should be able to defend choice-by-choice afterward. Two
diagrams are worth having open while you work: `docs/architecture-diagram.md`
(what's built vs. designed) and `docs/data-sources-er-diagram.md` (how the
four sources join).*

---

## Quick reference

| Day | Focus | Ends with | Status |
|---|---|---|---|
| 1 | Architecture decision + orders ingestion | ADR written, Airflow local-dev up, orders Bronze + Silver tables | ADR + Airflow **done**; orders Bronze + Silver **done** |
| 2 | The fulfillment feed | Fulfillment Bronze + Silver tables, `--bad-night` files quarantined at Bronze instead of dropped/crashing | Fulfillment Bronze **done**; Silver **not started** |
| 3 | Finance reconciliation + dashboard | A reconciliation report you can defend, one query rewrite made faster on purpose | Not started |
| 4 | MinIO | Bronze/Silver/Gold on object storage | **Done** (2026-08-28) |
| 5 | Kafka | Live message flow from the rider-events generator into a topic | Not started |
| 6-7 | Streaming into the lakehouse | A streaming job proven correct under load, not just "it ran" | Not started |
| 8 | Buffer + Delta deep features | Time-travel query answering a real question | Not started |
| 9 | Scale test | Measured before/after numbers on at least one performance fix | Not started |
| 10 | Demo day | Full client demo, resume updated | Not started |
| 11-12 | Flex | Overflow for whichever day ran long | Not started |

*Update the Status column yourself as you go — it's the fastest way to see
where you stand without opening `PROGRESS.md`. The four data-source
generators, all pre-built by `coder`, don't have their own row here: they
were front-loaded so every day above is pure platform work, not "wait for
the source I need." See the Daily Log entry below for how that happened.*

**Standing rule:** ~30-45 minutes/day on the job search, same as before.

---

## Daily Log

A few lines per real work session — not a full changelog (`PROGRESS.md`'s
session log already has that, in agent-level detail) but enough that
future-you can answer "what did I actually do that day, and why" without
re-reading code or diffs. Log by calendar date; the Day N label above is a
target chunk of work, not a literal day, so don't force a 1:1 mapping.

Template for a new entry:

```
### Day N — YYYY-MM-DD
- **Built:** what exists now that didn't before
- **Decided:** the one call you'd have to defend in an interview, and why
- **Surprised by:** anything the data/tooling did that you didn't expect
- **Next:** the next concrete thing to build
```

### Day 0-1 — 2026-08-27
- **Built:** Airflow local-dev stack (Docker Compose, later upgraded
  2.10.5 → 3.3.1), all four data-source generators (`orders.py`,
  `fulfillment.py`, `payments.py`, `rider_events.py`) plus their
  orchestrating DAGs, and the Day 1 ADR (`docs/ADR.md`).
- **Decided:** front-load *all four* generators instead of building them
  one per day alongside the matching platform layer, since their schemas
  were fully specified upfront (`docs/data-sources.md`) — nothing to
  discover, so no reason to gate platform work on generator work. Means
  every remaining day below is pure platform-layer work.
- **Surprised by:** Airflow 3's split topology (separate dag-processor/
  triggerer services, no bundled webserver) needed a real compose rewrite,
  not just a version bump — worth having a ready answer for "why upgrade
  mid-build" if asked.
- **Next:** Orders → Bronze first (land raw extracts into Delta, untouched,
  with ingestion metadata — no Bronze table exists yet, only the raw CSVs
  the generator DAGs produce), then Orders → Silver (dedup re-extracts,
  keep the most-advanced lifecycle state per order). Bronze is easy to
  read past in the plan and skip straight to Silver — don't; see "The
  Bronze layer" note under Day 1 below.

---

## Part 0 — Working with Claude Code

The split is by layer, not by difficulty. Claude Code builds the
infrastructure (Airflow, MinIO, Kafka) and **all four data-source
generators** completely and freely — their schemas and business rules are
documented upfront in `docs/data-sources.md`, so there's nothing there to
hold back. The engineer builds the platform itself: the transformation,
reconciliation, dashboard and streaming logic that turns those sources into
something Marcela, Julián and Ana can actually use.

This isn't a puzzle with a hidden answer key — the full spec for what the
data looks like and what the business rules are is written down. The value
of doing this work yourself is the same as it would be on a real system:
building it, testing it, and being able to explain every choice.

Full detail on the split, and the subagents that support it (`coder`,
`data-explorer`, `prompt-architect`), lives in `CLAUDE.md` — read it before
your first session. Agents confirm how much autonomy you want before acting
rather than assuming the maximum (`coder` defaults to controlled
implementation if you don't say), and don't re-ask once you've set it for a
session.

**Where things actually stand:** the roadmap above is fixed; the Daily Log is
where you track state in your own words. `PROGRESS.md` is the exhaustive,
agent-maintained live status file (what's built, what's in progress, what's
next, session by session) — read it before starting a session with Claude
Code, especially if it's been a while. If the Daily Log and `PROGRESS.md`
ever disagree on a fact (what's built, what's not), `PROGRESS.md` wins — it's
updated every session; the Daily Log is updated when you remember to.

**The test:** if you can't rebuild a piece from memory without the tool
open, that piece isn't done yet, regardless of whether it runs.

---

## Part 1 — The brief

### Background

**Veloz** is a quick-commerce startup (groceries + essentials, 15-45 minute delivery) operating out of ~30 dark stores across Medellín, Bogotá, and São Paulo, with about 450 active riders and roughly 180,000 orders/month, growing fast off a recent funding round. You're their first dedicated data hire. There is no infra team behind you.

### Current pain

- All reporting today is a data analyst manually running SQL against the app's database each morning and building spreadsheets. It takes hours and breaks every time the app schema changes.
- Ops has no live visibility — they find out a store is falling behind or a rider has gone dark when a customer complains, not before.
- Finance reconciles rider payouts against orders by hand each week. Last month a reconciliation error underpaid a rider for days before anyone caught it.
- Leadership is about to fund expansion into two more cities and is nervous the current setup (a person and a spreadsheet) won't survive it.

### Stakeholders, in their own words

- **Marcela, COO/Ops:** *"I want to know within a few minutes if a store is falling behind or a rider's gone dark — not find out from a customer complaint."*
- **Julián, CFO:** *"I need a number I can trust every morning before our finance standup. If something doesn't reconcile, tell me it doesn't reconcile — don't quietly pick a side."*
- **Ana, Head of Data (your manager):** *"You're it, for now — there's no platform team behind you. Whatever you build has to survive without you babysitting it every night, and it can't need a dedicated ops team to keep alive. And please don't build us something we have to rip out when we 5x order volume."*

### Business goals

- **G1** — Give Ops near-live visibility into order and rider status, store by store.
- **G2** — Give Finance a daily, auditable reconciliation of orders vs. payouts, ready before their 8am standup, with discrepancies **flagged, not hidden.**
- **G3** — Make the pipeline resilient to upstream messiness (late/malformed files, dropped events, schema drift) without someone manually intervening overnight.
- **G4** — Support at least 5x current order volume over the next 12 months without a redesign.
- **G5** — Keep operational complexity and cost proportionate to a one-person data team.

### Data sources

Full schemas, business rules (commission tiers, settlement-lag pattern) and
generator build status for all four: **`docs/data-sources.md`**. Summary:

1. **Orders database** — Postgres, the app's transactional order table (created → assigned → picked_up → delivered/cancelled). You'll receive periodic extracts; true change-data-capture off this database isn't available.
2. **Rider app event stream** — the rider app continuously fires location pings and status-change events while a delivery is active, published to a queue you can consume as a stream.
3. **Store fulfillment exports** — each of the 30 dark stores' inventory system emails a CSV once a day. It is unreliable — files go missing or arrive malformed on a bad night.
4. **Payments/commissions ledger** — a separate finance system exports a daily file of rider payouts and commissions calculated for the previous day's completed orders.

### Non-functional requirements (targets, not numbers you're locked into)

- Ops-facing status should feel close to live — a few minutes' lag is fine, an hour is not.
- Finance's reconciliation report must be ready before 8am local time, every day, without manual triggering.
- When the two sources feeding reconciliation disagree, the report must **show** the discrepancy.
- If a run fails, the system should retry/recover automatically where possible, and someone should be alerted when it can't.
- You should be able to answer "what did yesterday's numbers look like" weeks later.
- Nothing you build should require a dedicated on-call/infra rotation.

### Constraints

- You're the only engineer on this, for now.
- You cannot modify the upstream systems — you consume what they give you, as-is.
- Leadership wants a working demo in about two weeks.
- Budget favors open-source/low-cost tooling for this MVP phase.

### Deliverables / definition of done

1. A working, running pipeline — not slides — ingesting all four sources.
2. An Ops-facing view showing near-live order/rider/store status.
3. A Finance-facing daily reconciliation report with discrepancies visibly flagged, at a threshold you can defend with evidence from the data.
4. A demonstrated failure, injected live, with the system recovering or alerting appropriately.
5. A one-page Architecture Decision Record explaining your technology choices, your data-cleaning judgment calls, and the query-performance problems you found and fixed — in plain language Ana/Julián/Marcela could each follow.
6. A note on what's out of scope for this MVP and what you'd do differently at real production scale.

---

## Part 2 — Day 1: architecture, then orders ingestion

Before reading further, fill in the requirements→architecture table yourself: for each data source, batch or streaming, and why, tied to a specific G/stakeholder ask. Then compare against the reference architecture below — if you land somewhere different, that's legitimate; adapt the days to your choice.

| Decision | Choice | Justification |
|---|---|---|
| Rider events | Kafka → Spark Structured Streaming | G1/Marcela's ask is explicitly about liveness. |
| Orders / fulfillment / payments | Batch, orchestrated | Daily-cadence requirements (G2); true CDC is explicitly out of scope. |
| Table format | Delta Lake | G2's audit requirement needs time travel; ACID writes protect G3. |
| Orchestration + quarantine pattern | Airflow | G3 rules out manual 2am intervention. |
| Storage | Object storage (MinIO standing in for S3) | G4 — local disk doesn't survive 5x growth. |
| Compute | Spark in-process, no dedicated cluster | G5 rules out anything needing a platform team. |

### Day 1 build

**Learn first:** Airflow local-dev refresher; Delta Lake fundamentals (transaction log, ACID).

**Setup — done (see Daily Log, 2026-08-27):** Airflow's docker-compose
(LocalExecutor, now on 3.3.1) is up as a custom image with Spark/Delta
installed directly into it, and the Day 1 ADR is written (`docs/ADR.md`).
The commands below are kept for reference/rebuild, not as a to-do:
```
brew install --cask orbstack
brew install --cask temurin17
brew install --cask claude-code
mkdir veloz-platform && cd veloz-platform && git init
mkdir -p dags generators docker notebooks docs data/raw
python3 -m venv .venv && source .venv/bin/activate
pip install pyspark==3.5.3 delta-spark==3.2.1 jupyterlab pandas faker streamlit
```

### The Bronze layer — the step before Silver, and easy to skip past

The reference architecture (`CLAUDE.md`, and `docs/architecture-diagram.md`)
is explicit that the lakehouse is **Bronze → Silver → Gold**, not raw file →
Silver directly. It's easy to read the Day 1 target below, see a Silver
table as the finish line, and quietly skip landing a Bronze table on the way
— worth naming so you don't lose it as a line item.

Bronze is deliberately dumb: land each source's raw extract into a Delta
table close to as-is (schema-on-write, no dedup, no business rules, no
joins — that's Silver's job), plus a couple of ingestion-metadata columns
(e.g. `_bronze_ingested_at`, `_source_file`) so you always know when and from what
file a row came from. Two things this buys you, both tied to a specific
stakeholder ask, not "best practice" for its own sake:
- **G2 (Julián's audit requirement):** if Silver logic has a bug and you
  need to recompute, Bronze is an immutable, already-in-the-lakehouse copy
  of exactly what the source handed you that day — you don't need the
  source to still have that day's raw extract sitting around when you
  re-run.
- **G3 (resilience):** reprocessing a day only means re-reading Bronze
  Delta, not re-hitting the raw CSV/extract — which matters once quarantine
  and retries are in the picture (Day 2).

**Status:** live for orders, fulfillment, and rider_events (payments Bronze
is still unbuilt). `dags/ingest_orders_bronze.py`,
`dags/ingest_fulfillment_bronze.py`, and `dags/ingest_rider_events_bronze.py`
each read that source's raw files from MinIO's `raw-incoming-data` bucket
and write Delta to `s3a://bronze-veloz/<source>/`.

### Orders ingestion — Bronze, then Silver (target spec — now satisfied, see below)

Orders arrive as a periodic extract (`docs/data-sources.md` has the exact
schema): one row per order reflecting its state as of extract time, not a
changelog. Because extracts are periodic and CDC isn't available, the same
order will legitimately appear across more than one extract as it
progresses through its lifecycle — that's the real shape of this problem,
not an injected trap.

**Bronze target:** every row from every `orders_<run_timestamp>.csv`
5-minute-window file landing in MinIO's `raw-incoming-data` bucket lands in
the Delta table at `s3a://bronze-veloz/orders/`, untouched (yes, including
the repeated rows for an order that appears across multiple extracts —
dedup is Silver's job, not Bronze's), tagged with ingestion metadata.

**Silver target (self-checkable):**
- `SELECT order_id, COUNT(*) FROM silver.orders GROUP BY order_id HAVING COUNT(*) > 1` returns zero rows once you've ingested more than one day's extract for overlapping orders.
- For an order captured at multiple lifecycle points, Silver keeps the row reflecting the *most advanced business state*.
- A validation query that would catch a regression in this logic later — a real test, not a one-off check.

These now hold — see "Orders ingestion — Silver (built, 2026-09-07)" below
for how.

### Orders ingestion — Silver (built, 2026-09-07)

Silver's orders table lives at `s3a://silver-veloz/orders/`, one row per
`order_id`, reflecting that order's *current* lifecycle state. An order's
lifecycle (`created → assigned → picked_up → delivered`/`cancelled`)
doesn't arrive in one shot — extracts are periodic, so the same order shows
up spread across many 5-minute Bronze ingestion windows as it progresses.
Silver's job is to accumulate evidence for that order across every window
it's ever appeared in, not to keep "whichever row is newest" — see
`docs/ADR.md`'s Silver appendix ("Grain: accumulating snapshot, not
latest-row-wins") for why a plain newest-row-wins dedup doesn't work here.

**The column classes** — how to read a Silver row when something looks
off:
- **Sticky** (`created_at`, `assigned_at`, `picked_up_at`, `delivered_at`,
  `cancelled_at`): filled in the first time a value is seen, never blanked
  afterward — a late-arriving window can fill a gap, but can't erase an
  already-set value. `cancelled_at` has no Bronze-source column of its
  own; Silver derives it as the timestamp of the first row that reaches
  `status == "cancelled"`.
- **Latest-wins** (`status`, `rider_id`, `store_id`, `order_total`,
  `updated_at`): only overwritten when the incoming batch's `updated_at`
  is genuinely newer than what Silver already holds — no fallback.
- **Audit** — `_silver_ingested_at` refreshes on every write (insert or
  update); `_silver_first_seen_at` is set once, on first insert, and never
  touched again. `_bronze_ingested_at` is carried forward for lineage only,
  never as a filter key: Bronze can rewrite the same row up to 6 times
  across its own lookback window, restamping that column each time, so
  filtering an incremental Silver read on it would be non-deterministic
  across reruns (see the ADR appendix's "Incremental filter key" section).

**How it's triggered.** `dags/ingest_orders_bronze.py` publishes
`ORDERS_BRONZE_ASSET` on every run, tagged with the `extract_dates`/
`windows` it actually wrote. `dags/ingest_orders_silver.py` is
Asset-scheduled off `ORDERS_BRONZE_ASSET` and unions the extras from
*every* triggering event, not just the latest one. Keep this distinct from
`ORDERS_RAW_ASSET` — the `S3NewObjectTrigger`-watched Asset that polls
MinIO for new raw files and triggers Bronze: one watches an external raw
feed, the other announces what a task inside this pipeline already wrote.

**Rerunning Silver by hand.** `ingest_orders_silver` takes two mutually
exclusive manual-trigger param modes:
- `mode="ingestion_window"` with `window_start`/`window_end` as
  `%Y%m%dT%H%M%SZ` tokens, e.g. `window_start=20260907T013000Z`,
  `window_end=20260907T014500Z` — reprocesses every 5-minute window in
  that inclusive range.
- `mode="extract_date"` with `extract_dates` as a list, e.g.
  `extract_dates=["2026-09-05", "2026-09-06"]` — reprocesses whole
  `_extract_date` partitions.

Precedence: an explicit `params["mode"]` always wins over whatever
triggered the run; with no explicit mode, Silver falls back to the union
of the triggering `ORDERS_BRONZE_ASSET` event extras; with neither, it
raises `ValueError` — never a silent "today" or "everything" default.

**Maintenance.** `dags/maintain_orders_silver.py` runs daily at 06:30
UTC — off-peak for all three markets and comfortably ahead of Finance's
8am-local deadline in every one of them — doing `OPTIMIZE ... ZORDER BY
(order_id, store_id)` then `VACUUM` at Delta's default 168-hour retention.
It's a separate DAG because `ingest_orders_silver`'s MERGE runs dozens of
times a day and needs to stay fast per batch, not pay a full-table
OPTIMIZE/VACUUM cost on every trigger.

**Running the tests** — currently tribal knowledge, written down here:
the host's default JVM (Temurin 26) breaks every PySpark `SparkSession`
creation, so any Spark-touching test needs JDK 17 pointed at explicitly:
```
JAVA_HOME=/opt/homebrew/Cellar/openjdk@17/17.0.20/libexec/openjdk.jdk/Contents/Home .venv/bin/python -m pytest -q tests/
```
The container path (real Airflow, Python 3.11) needs an ad-hoc mount,
since `tests/` isn't mounted into the image and pytest isn't installed
there:
```
docker compose run --rm --no-deps -v "$(pwd)/tests:/opt/airflow/tests" --entrypoint /bin/bash airflow-scheduler -c "pip install --no-cache-dir --quiet pytest==9.1.1 && cd /opt/airflow && python -m pytest -q tests/"
```
Both currently report 162 passed / 2 failed (pre-existing, unrelated to
Silver). Pass `tests/` explicitly in both — bare `pytest` fails collection
on `dags/*_smoke_test.py`.

**Known limitations**, short version — full detail in the ADR appendix's
"Explicitly deferred" section:
- A sticky column can never be reset to `NULL` once set, so an upstream
  correction-by-nulling (e.g. clearing a wrong `delivered_at`) is ignored.
- Status regressions are applied silently — latest-wins just takes the
  newer batch's value, even when it's an earlier lifecycle status than
  what Silver already holds. Detection is deferred.
- Two pre-existing quarantine bugs in `application/bronze_ingestion.py`
  mean a malformed row can currently enter Bronze marked clean, and with
  Silver now MERGEing off Bronze, that bad row becomes persistent order
  state instead of a one-off defect in a single day's extract.

---

## Day 2 — The fulfillment feed

Fulfillment arrives as one CSV per store per day (`docs/data-sources.md`
has the exact schema and null convention — every store uses the same one).
The realistic messiness here is `--bad-night`: files missing entirely for
some stores, and malformed rows in others that break a naive `read_csv`.

Quarantine has to happen at the Bronze step here, not Silver — a row you
can't parse can't land in a Delta table at all, so "log it and set it
aside instead of crashing the DAG" is Bronze-ingestion logic, not a
cleaning rule. Silver only ever sees rows that made it into Bronze clean.

**Bronze target:**
- Every store file that parses at all lands in the Delta table at
  `s3a://bronze-veloz/fulfillment/`, untouched, with ingestion metadata.
- A missing file for a (store, date) or a row `pandas.read_csv` can't parse
  is logged to a quarantine location (table or path — your call, documented)
  instead of silently dropped or crashing the task.

**Silver target:**
- Your Silver fulfillment table has exactly one row per (store_id, sku, date), built only from what made it through Bronze clean.
- Your README documents, with evidence, what you observed in a `--bad-night` run and how your quarantine logic handles each failure mode.

---

## Day 3 — Finance reconciliation and a query that doesn't scale

### Reconciliation

The commission policy and settlement-lag pattern are documented in full in
`docs/data-sources.md` — recompute expected commission from each delivered
order, join against the payments ledger, and flag: amount mismatches,
missing payments, and lag that falls well outside the normal settlement
pattern.

**Target:** a reconciliation report that flags real discrepancies without
false-positiving on ordinary settlement lag. Justify the lag-outlier
threshold using the *actual distribution* of lag times in your generated
data, stated explicitly in your ADR — the policy doc tells you the
generating distribution's shape, not the cutoff a report should flag on;
that's your call to make and defend, the same as any real anomaly-detection
threshold.

### The reconciliation query that doesn't scale

Here's the query you'll probably write first — it's correct, and it's a trap:

```sql
SELECT
  o.order_id,
  o.order_total,
  (SELECT p.commission_amount
   FROM payments p
   WHERE p.order_id = o.order_id
     AND DATE(p.payment_timestamp) = DATE(o.delivered_at)
   LIMIT 1) AS matched_commission
FROM orders o
```

**Target:** at realistic data volume, make this **at least 5x faster** without changing what it computes. Use `EXPLAIN` / the Spark UI to find out why it's slow — don't guess.

**Hints:**
1. Run `EXPLAIN` (or `.explain(True)`) before changing anything. What does the plan say this query does, per row?
2. A predicate that wraps a join/filter column in a function — like `DATE(...)` — usually blocks the engine from pruning efficiently. Is there an equivalent condition that doesn't wrap the column?
3. Compare a correlated subquery against a plain join conceptually: how many times does each one actually touch the payments table?

---

## Day 4 — Your own private S3 (MinIO) — built, 2026-08-28

**Learn first:** what S3-compatible object storage buys you over local disk; the `s3a://` connector config — budget extra time, this remains the one genuinely fiddly step, and it's a legitimate full-delegate task if it fights you.

**Built:** MinIO added to the compose file, with three layer buckets —
`bronze-veloz`/`silver-veloz`/`gold-veloz` — plus a separate
`raw-incoming-data` bucket as the landing zone for the generators' raw
files. Bronze/Silver/Gold writes are repointed at
`s3a://<layer>-veloz/...` accordingly.

**What you'll see:** the MinIO console filling with real Delta/Parquet files as your DAGs run.

---

## Day 5 — Rider events go live

**Learn first:** Kafka core concepts — topics, partitions, consumer groups, and why partitioning/ordering matter.

**Build:** Kafka (KRaft, `apache/kafka` image, no Zookeeper) + Kafka UI; wire `generators/rider_events.py`'s output into a producer publishing to a topic.

**What you'll see:** Kafka UI showing live messages landing in the topic.

---

## Day 6-7 — Streaming into the lakehouse

**Learn first:** Spark Structured Streaming (`readStream`/`writeStream`, checkpointing, watermarking); event time vs. processing time.

**Build:** the streaming consumer job into `rider_events`, with checkpointing.

### Distance/ETA: UDF vs. native

Compute rider-to-store distance/ETA per event using a haversine-style
calculation. Write it first the way most people instinctively would: a
Python UDF. Then implement it a second way using Spark-native/vectorized
functions, measure checkpoint lag and throughput for both under sustained
producer load, and let the *measured evidence* decide which one ships.
Document both numbers in your README.

**Hints:**
1. A Python UDF crosses the JVM↔Python boundary for every single row. What Spark-native alternative avoids that entirely?
2. Look at what's already built into `pyspark.sql.functions` before reaching for custom Python.
3. If you truly need custom Python logic, compare a vectorized (pandas) UDF against the row-at-a-time version too.

### Out-of-order events

`docs/data-sources.md` notes the rider-events stream isn't strictly sorted
by event time — that's realistic network behavior, not an injected trap.

**Target:** prove that your Ops dashboard never shows a rider's status
regressing (e.g., "delivered" reverting to "en route") because of a
late-arriving event.

**Hints:**
1. Are you currently keying "latest" off event time or processing time? Those are not the same thing in Structured Streaming.
2. Watermarking bounds how late is acceptable — set it too tight and you silently drop real data; too loose and results lag. What does your own producer's actual lag pattern tell you about the right value?

---

## Day 8 — Buffer + Delta deep features

Reserved slack — Days 4-7 commonly run long. If you're on schedule:
`OPTIMIZE`/`VACUUM`, a time-travel query comparing yesterday's
reconciliation to today's, and start Databricks Data Engineer Associate
practice exams.

---

## Day 9 — Scale test

**Learn first:** how to read the Spark UI's stage/task timeline; partitioning trade-offs.

**Build:** generate a 5x-volume dataset and run your pipeline against it. Find whatever real bottleneck shows up — a skewed key, a Gold-layer query that got slow as history grew — using the Spark UI/`EXPLAIN`, not a guess.

**Target:** diagnose the cause, apply a fix, and report a measured before/after improvement. If more than one thing is slow, pick the one with the clearest before/after story to put in your ADR.

**Hints:**
1. A pipeline that "runs" but is unexpectedly slow at a new scale is a job for the Spark UI's task-duration spread within a stage, not a guess.
2. Once you've found a skewed key, there's more than one legitimate fix (broadcast, salting, adaptive query execution's skew handling) — which one is right depends on which side is skewed and by how much.
3. For a Gold-layer query that was fine at low volume and isn't anymore, think about what changes as *history* grows, not just row count — that's often a different mechanism than a skewed key.

---

## Day 10 — Demo day

Rehearse and record the full client demo end to end: all six deliverables, under ~10 minutes, including at least one design decision you can explain in real depth if asked a follow-up question. Update resume/portfolio. Heaviest job-search push of the sprint.

---

## Days 11-12 — Flex

Use these first as overflow for whichever day ran long — that's the expected use, not a fallback. Only if everything above is genuinely finished and defensible, consider the Strategy C teaser (containerize the streaming job, deploy on Kubernetes, engineer and fix a deliberate skew scenario there too) — it's secondary to actually finishing the platform above.
