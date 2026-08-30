# Veloz Platform — Project Context

## What this project is

Veloz is a quick-commerce startup (groceries + essentials, 15–45 minute delivery)
running ~30 dark stores across Medellín, Bogotá and São Paulo, with ~450 active
riders and roughly 180,000 orders/month, about to expand into two more cities.
This repository is its data platform. The engineer working here is Veloz's first
dedicated data hire: no infra team, no platform team, a demo due in about two
weeks.

Today all reporting is one analyst running SQL by hand each morning; Ops learns a
store is behind from customer complaints; Finance reconciles payouts manually and
recently underpaid a rider for days before anyone noticed.

This is also a portfolio project: the engineer needs to be able to defend every
design decision in an interview afterward. The upstream data sources are treated
as **given** — fully specified, documented, and emulated — so the engineer's
judgment goes into the platform built on top of them, not into guessing what the
raw data looks like.

## Business goals

- **G1** — Give Ops near-live visibility into order and rider status, store by store.
- **G2** — Give Finance a daily, auditable reconciliation of orders vs. payouts, ready before their 8am standup, with discrepancies **flagged, not hidden**.
- **G3** — Make the pipeline resilient to upstream messiness (late/malformed files, dropped events, schema drift) without someone manually intervening overnight.
- **G4** — Support at least 5x current order volume over the next 12 months without a redesign.
- **G5** — Keep operational complexity and cost proportionate to a one-person data team.

## Stakeholder asks

- **Marcela, COO/Ops:** "I want to know within a few minutes if a store is falling behind or a rider's gone dark — not find out from a customer complaint."
- **Julián, CFO:** "I need a number I can trust every morning before our finance standup. If something doesn't reconcile, tell me it doesn't reconcile — don't quietly pick a side."
- **Ana, Head of Data (manager):** "You're it, for now — there's no platform team behind you. Whatever you build has to survive without you babysitting it every night, and it can't need a dedicated ops team to keep alive. And please don't build us something we have to rip out when we 5x order volume."

Design implication: when two sources disagree, the output shows the disagreement.
Never quietly pick a side.

## Data sources (fixed — upstream systems cannot be modified)

1. **Orders** — Postgres periodic extracts of the app's order table (created → assigned → picked_up → delivered/cancelled). True CDC is out of scope.
2. **Rider app events** — location pings and status changes published to a queue, consumed as a stream.
3. **Store fulfillment exports** — one CSV per store per day, emailed. Unreliable.
4. **Payments/commissions ledger** — daily file of rider payouts and commissions for the previous day's completed orders.

Full schemas, business rules (commission tiers, settlement-lag pattern) and
generator status for all four: **`docs/data-sources.md`**. That document is the
authoritative handoff spec — treat it the way you'd treat a spec a stakeholder
actually handed over, not something to second-guess or rediscover from the data.

## Non-functional targets

- Ops status a few minutes behind is fine; an hour is not.
- Finance reconciliation ready before 8am local, unattended.
- Failed runs retry/recover automatically where possible; alert when they can't.
- "What did yesterday's numbers look like" must be answerable weeks later.
- Nothing here may require an on-call or infra rotation.

## Reference architecture

| Decision | Choice | Why |
|---|---|---|
| Rider events | Kafka → Spark Structured Streaming | G1 / Marcela's ask is about liveness |
| Orders / fulfillment / payments | Batch, orchestrated | Daily cadence (G2); no CDC available |
| Table format | Delta Lake | G2 audit needs time travel; ACID protects G3 |
| Orchestration + quarantine | Airflow | G3 rules out manual 2am intervention |
| Storage | Object storage (MinIO standing in for S3) | G4 — local disk doesn't survive 5x |
| Compute | Spark in-process, no dedicated cluster | G5 rules out anything needing a platform team |

If the engineer deliberately diverges from this table, the ADR records why; that
is legitimate.