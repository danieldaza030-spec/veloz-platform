# Veloz Data Sources — Handoff Spec

This is the schema and business-rule handoff for all four upstream systems,
as if delivered by Veloz's engineering/finance stakeholders before build
start. Nothing here is discovered by profiling — it's given, the same way a
real vendor or internal-systems handoff would document what you're about to
receive. All four sources are fully emulated by generators under
`generators/`, driven by shared dimensions in `generators/reference_data.py`
(30 dark stores across Medellín/Bogotá/São Paulo, 450 riders assigned to a
home city, a 33-item SKU catalog).

Building the Bronze → Silver → Gold pipeline against these sources —
deduplication, schema normalization, reconciliation, dashboards, streaming —
is the engineer's own work. This document exists so that work starts from a
known, documented spec instead of a live system whose shape has to be
reverse-engineered first.

---

## 1. Orders — Postgres periodic extract

Simulates a periodic poll of the app's transactional order table. Each row
is one order's state **as of extract time**, not a changelog entry — true
CDC isn't available on this system.

Lifecycle: `created` → `assigned` → `picked_up` → `delivered`, with
cancellation possible from any stage before delivery.

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

**Generator:** `generators/orders.py`. **Output:**
`s3://raw-incoming-data/orders/orders_<date>.csv` (MinIO), one row per order —
see the landing-zone note at the end of this document; the generator no
longer writes to local `data/raw/`.

Status mix: ~78% delivered, ~7% cancelled, rest still in-flight at extract
time. Rider assigned from the store's own city only.

---

## 2. Rider app events — queue stream

Simulates the continuous stream of location pings and status changes a
rider's phone publishes while working an active delivery. In production this
is a Kafka topic; until Kafka infra is stood up, the generator writes the
same event shape to a flat file as a stand-in for what would be produced to
the topic.

One JSON object per line (JSON Lines):

| Field | Type | Notes |
|---|---|---|
| `event_id` | string (UUID) | |
| `rider_id` | string | |
| `order_id` | string, nullable | the order this event relates to; null is not expected in this generator's output (every event is order-linked — see scope note below) |
| `event_type` | string | `location_ping` \| `status_change` |
| `event_time` | timestamp | event time, not processing time |
| `latitude` | float, nullable | populated only for `location_ping` |
| `longitude` | float, nullable | populated only for `location_ping` |
| `status` | string, nullable | populated only for `status_change`: `assigned` \| `picked_up` \| `delivered` |

**Scope note:** events are only generated for riders actively working an
order that day (i.e., an order that reached `assigned` or further in that
day's orders extract) — a rider idling with the app open but no active order
does not appear. This keeps the stream tightly linked to the orders extract
for join purposes; it is a deliberate simplification of the real system, not
a trap.

Per order that reaches `assigned`+: one `status_change` event at each
lifecycle timestamp already present in the orders extract
(`assigned_at`/`picked_up_at`/`delivered_at`), plus 2–5 `location_ping`
events at randomized points between `assigned_at` and the order's last
reached timestamp, jittered around the store's city center:

| City | Center (lat, lon) |
|---|---|
| Medellín | 6.2442, -75.5812 |
| Bogotá | 4.7110, -74.0721 |
| São Paulo | -23.5505, -46.6333 |

**Realistic messiness (in scope, not hidden):** events are written in the
order they're generated per rider, not globally re-sorted by `event_time` —
a real network-delivered stream doesn't arrive in strict event-time order,
and a consumer built against this data needs to handle that honestly, the
same way it would against the real queue. This is ordinary stream behavior,
not an injected puzzle.

**Generator:** `generators/rider_events.py`. **Output:**
`s3://raw-incoming-data/rider_events/rider_events_<date>.jsonl` (MinIO) — see
the landing-zone note at the end of this document.

---

## 3. Store fulfillment exports — daily CSV per store

Simulates the daily, per-store, emailed inventory export.

| Column | Type | Notes |
|---|---|---|
| `store_id` | string | one of `STORE-001`..`STORE-030` |
| `sku` | string | one of `SKU-0001`..`SKU-0033` |
| `date` | date | the export date |
| `quantity_on_hand` | int, nullable | blank = SKU not counted that day (one null convention, used identically by every store) |
| `exported_at` | timestamp | when that store's file was generated, evening of `date` |

**Generator:** `generators/fulfillment.py`. **Output:**
`s3://raw-incoming-data/fulfillment/date=<date>/<store_id>.csv` (MinIO), one
object per store — see the landing-zone note at the end of this document.

Every store uses the identical schema, unit, and null convention — there is
no per-store drift in this source.

**`--bad-night` (baseline feed unreliability, in scope):** ~10% of stores'
files are missing entirely for the date; ~15% arrive with a share of rows
mechanically corrupted (a write cut short, or a stray delimiter splitting a
field) that a downstream reader must quarantine rather than crash on.
Deterministic per `--seed`.

---

## 4. Payments / commissions ledger — daily file

Simulates the daily export from Finance's separate payments system: rider
commissions computed for the previous day's completed (delivered) orders.

| Column | Type | Notes |
|---|---|---|
| `payment_id` | string (UUID) | |
| `order_id` | string | references an order in that day's `delivered` orders |
| `rider_id` | string | the rider who completed the order |
| `commission_amount` | float | USD-equivalent, per the commission policy below |
| `payment_recorded_at` | timestamp | when Finance's system recorded the payout |

**Generator:** `generators/payments.py`. **Output:**
`s3://raw-incoming-data/payments/payments_<date>.csv` (MinIO), one row per
delivered order from that date's orders extract (minus the
deliberately-missing rows below) — see the landing-zone note at the end of
this document.

### Commission policy (Veloz's actual rule — documented, not hidden)

Base commission rate, tiered by `order_total`:

| Order total | Base rate |
|---|---|
| < $15.00 | 10% |
| $15.00 – $34.99 | 13% |
| ≥ $35.00 | 16% |

**Late-delivery deduction:** an order is "late" if
`delivered_at - created_at` exceeds 45 minutes (Veloz's outer promise
window). A late order's effective rate is the base rate minus 3 percentage
points, floored at 5%.

```
effective_rate = base_rate                     if on time
effective_rate = max(base_rate - 0.03, 0.05)    if late
commission_amount = round(order_total * effective_rate, 2)
```

### Settlement lag (Veloz's actual pattern — documented, not hidden)

`payment_recorded_at = delivered_at + lag`, where `lag` (hours) is drawn
from a normal distribution, mean 20h / stdev 6h, clipped to `[2, 48]` hours
for every normally-settled row. This is genuine settlement lag — Finance's
system doesn't post same-second — not a discrepancy.

### Deliberate data-quality issues (in scope, rates documented — not a row-level answer key)

Two failure modes are injected at fixed, seeded, documented rates, the same
way `--bad-night` works for fulfillment — knowing the rate doesn't tell you
which rows are affected; finding them from the data is still the
reconciliation logic's job:

- **~2%** of delivered orders get a `commission_amount` that does **not**
  match the policy above (computed with the wrong tier or an arbitrary
  error factor).
- **~1%** of delivered orders get **no payment row at all** — a payment
  that never landed.

A correct reconciliation check recomputes `commission_amount` from each
order's `order_total`/timing using the policy above, joins it against the
payments file, and flags: amount mismatches, missing payments, and
`payment_recorded_at` lag that falls well outside the normal settlement
pattern. What counts as "well outside normal" is a threshold the engineer
justifies from the actual lag distribution in the generated data — the
policy above tells you the *generating* distribution's shape, not the exact
cutoff a reconciliation report should flag on, which is a judgment call
about acceptable variance, same as any real anomaly-detection threshold.

---

## Landing zone (MinIO, not local `data/raw/`)

All four generators now write directly to MinIO instead of the local
`data/raw/` folder, over S3 via `boto3` (`generators/s3_io.py`), using the
same `veloz-ingest` (read/write/list, no delete) credentials every other
ingest-side process in this project runs as. The relative key structure is
unchanged from the old local layout — only the storage target moved:

| Source | Old local path | New S3 key (same relative structure) |
|---|---|---|
| Orders | `data/raw/orders/orders_<date>.csv` | `orders/orders_<date>.csv` |
| Fulfillment | `data/raw/fulfillment/date=<date>/<store_id>.csv` | `fulfillment/date=<date>/<store_id>.csv` |
| Payments | `data/raw/payments/payments_<date>.csv` | `payments/payments_<date>.csv` |
| Rider events | `data/raw/rider_events/rider_events_<date>.jsonl` | `rider_events/rider_events_<date>.jsonl` |

Existing local `data/raw/**` files are untouched (historical only — nothing
new lands there). Each generator's `--output-dir`/`--orders-dir` flags now
take an S3 key prefix (default: the source name, e.g. `orders`) instead of a
local directory path; a new `--bucket` flag (default: `raw-incoming-data`)
selects the target bucket. `payments.py`/`rider_events.py` read that date's
orders extract the same way, over S3 instead of local disk.

**Open issue, not yet resolved:** MinIO (and real AWS S3) reject
`raw-incoming-data` as a bucket name outright — `mc mb` fails with "Bucket
name contains invalid characters," confirmed directly against the deployed
MinIO. This is the real S3 bucket-naming rule (DNS-compliant names only, no
underscores, since 2018), not a MinIO-specific quirk. The bucket is
therefore **not yet created** in `docker-compose.yml`'s `minio-init`
bootstrap, and the ARNs for it are staged (but currently pointing at a
name nothing can create) in both
`docker/minio/policies/{ingest,maintenance}-policy.json`. Every generator/DAG
already targets `raw-incoming-data` by default via one constant
(`DEFAULT_BUCKET` in `generators/s3_io.py`, `RAW_BUCKET` in each
`dags/generate_*.py`), so once the actual bucket name is decided, wiring it
up is a small, mechanical change in those few places — not a design change.
See `PROGRESS.md`'s session log for the full empirical trail (the failing
`mc mb`, the working hyphenated equivalent, and an end-to-end smoke test of
the generators' S3 I/O against a scratch bucket).

---

## Generator CLI overrides (Airflow-orchestrated)

Each generator's distribution/error-injection constants documented above are
now also optional CLI flags, defaulting to the exact values already
documented — invoking a generator without these flags is unchanged.
`dags/generate_orders.py`, `dags/generate_fulfillment.py`,
`dags/generate_payments.py` and `dags/generate_rider_events.py` (one Airflow
DAG per generator) expose them as `Param`s so a "Trigger DAG w/ config" run
can override the incoming data's distribution or error-injection rate
without editing the generator itself:

| Generator | Flag | Default | Scope |
|---|---|---|---|
| `orders.py` | `--status-weights` | `delivered=0.78,cancelled=0.07,picked_up=0.05,assigned=0.06,created=0.04` | comma-separated `status=weight`; must name exactly those five statuses |
| `fulfillment.py` | `--baseline-missing-quantity-rate` | `0.02` | applies every night |
| `fulfillment.py` | `--missing-file-store-share` | `0.10` | `--bad-night` only |
| `fulfillment.py` | `--malformed-file-store-share` | `0.15` | `--bad-night` only |
| `fulfillment.py` | `--malformed-row-share` | `0.12` | `--bad-night` only |
| `payments.py` | `--wrong-amount-rate` | `0.02` | |
| `payments.py` | `--missing-payment-rate` | `0.01` | |
| `payments.py` | `--lag-mean-hours` | `20.0` | |
| `payments.py` | `--lag-stdev-hours` | `6.0` | |
| `rider_events.py` | `--min-pings` / `--max-pings` | `2` / `5` | |
| `rider_events.py` | `--location-jitter-degrees` | `0.05` | |

All four generator DAGs run on their own independent daily cron — deliberately
no Airflow Asset/event coupling between them, since these DAGs simulate
separate upstream systems and a real upstream doesn't notify your pipeline
when another upstream's extract has landed. `generate_orders` runs at 01:00
UTC; `generate_payments` (01:15) and `generate_rider_events` (01:20) are
offset late enough after it to normally find that day's `orders_<date>.csv`
already there, but that's a scheduling convenience, not a guarantee — the
generators' existing `FileNotFoundError` (unchanged) is the real backstop for
that file still being missing when they run (a late/failed orders run, or a
manual out-of-band trigger naming a different date). `generate_fulfillment`
has no dependency on orders at all (it doesn't read the orders extract) and
runs on its own cron (01:05 UTC).

---

## Build status

| Source | Generator | Status |
|---|---|---|
| Orders | `generators/orders.py` | Built |
| Fulfillment | `generators/fulfillment.py` | Built |
| Rider events | `generators/rider_events.py` | Built |
| Payments | `generators/payments.py` | Built |

See `PROGRESS.md` for current build status and `docs/ADR.md` for the exact
CLI trigger commands for each generator.
