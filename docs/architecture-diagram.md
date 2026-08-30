# Veloz Platform — Architecture Diagram

Reflects the reference architecture in `CLAUDE.md` plus actual build status
from `PROGRESS.md` as of 2026-08-27. Solid boxes/arrows are built and
verified; dashed boxes/arrows are designed but not yet built.

```mermaid
flowchart TB
    subgraph SRC["Upstream sources (fixed, fully specified — docs/data-sources.md)"]
        direction LR
        ORD["Orders\nPostgres periodic extract"]
        RID["Rider app events\nqueue stream"]
        FUL["Store fulfillment\ndaily CSV per store"]
        PAY["Payments / commissions\ndaily ledger file"]
    end

    subgraph GEN["Generators — Built"]
        direction LR
        GORD["generators/orders.py"]
        GFUL["generators/fulfillment.py"]
        GPAY["generators/payments.py"]
        GRID["generators/rider_events.py"]
    end

    ORD --> GORD
    FUL --> GFUL
    PAY --> GPAY
    RID --> GRID

    GORD --> RAWORD[("data/raw/orders/\n*.csv")]
    GFUL --> RAWFUL[("data/raw/fulfillment/\ndate=*/*.csv")]
    GPAY --> RAWPAY[("data/raw/payments/\n*.csv")]
    GRID --> RAWRID[("data/raw/rider_events/\n*.jsonl")]

    subgraph AIRFLOW["Airflow — docker-compose, LocalExecutor — Built"]
        SCHED["Scheduler + Webserver\ncustom image: OpenJDK 17 + pyspark 3.5.3 + delta-spark 3.2.1"]
    end

    RAWORD --> SCHED
    RAWFUL --> SCHED
    RAWPAY --> SCHED

    subgraph KAFKASTREAM["Streaming path — Not yet built"]
        KAFKA["Kafka (KRaft) + Kafka UI"]
        SSTREAM["Spark Structured Streaming\ncheckpointed consumer,\nwatermarked for out-of-order events"]
        KAFKA --> SSTREAM
    end

    RAWRID -. "producer wiring (Day 5)" .-> KAFKA

    subgraph LAKE["Delta Lakehouse — Not yet built (Bronze/Silver/Gold ingestion DAGs)"]
        direction LR
        BRONZE[("Bronze")]
        SILVER[("Silver\ndedup, schema-consistent,\nquarantined bad rows")]
        GOLD[("Gold\nreconciliation + dashboards")]
        BRONZE --> SILVER --> GOLD
    end

    SCHED -. "batch ingestion DAGs" .-> BRONZE
    SSTREAM -. "streaming write" .-> BRONZE

    subgraph OBJSTORE["Object storage — Not yet built"]
        MINIO[("MinIO\ns3a://veloz/...")]
    end

    BRONZE -. "repoint from local disk" .-> MINIO
    SILVER -. "repoint from local disk" .-> MINIO
    GOLD -. "repoint from local disk" .-> MINIO

    subgraph CONSUMERS["Stakeholder-facing outputs — Not yet built"]
        direction LR
        OPS["Ops status view\n(Marcela) — a few minutes' lag"]
        FIN["Finance reconciliation report\n(Julián) — ready before 8am,\ndiscrepancies flagged"]
    end

    GOLD -.-> OPS
    GOLD -.-> FIN
```

## Legend

| Style | Meaning |
|---|---|
| Solid box, solid arrow | Built and verified (generators smoke-tested; Airflow stack brought up and confirmed end-to-end via `spark_delta_smoke_test`) |
| Dashed box, dashed arrow | Designed in `CLAUDE.md`'s reference architecture, not yet built |

## What's built vs. what's next

- **Built:** all four data-source generators (`generators/orders.py`,
  `fulfillment.py`, `payments.py`, `rider_events.py`), writing to
  `data/raw/` on local disk; Airflow local-dev (LocalExecutor,
  docker-compose, custom image with Spark/Delta baked in) — up and verified,
  currently running only a smoke-test DAG.
- **Not yet built:** the actual Bronze→Silver→Gold ingestion DAGs, MinIO
  object storage (currently local disk), Kafka + the streaming consumer, and
  both stakeholder-facing outputs (Ops status view, Finance reconciliation
  report). These make up the "Platform build" table in `PROGRESS.md` and are
  the engineer's own work per `CLAUDE.md`'s current operating model.

Source of truth for exact status: `PROGRESS.md`. Source of truth for why
each architectural choice was made: `docs/ADR.md`.
