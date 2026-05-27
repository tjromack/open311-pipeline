# Architecture

A walkthrough of how a single Chicago 311 service request flows from the
public API through Kafka, an LLM classifier, Snowflake, and dbt — and where
each component's failure modes are handled.

## System overview

```mermaid
flowchart LR
    A[Chicago Open311 API] -->|HTTP poll every 60s| B[Open311Poller]
    B -->|publish JSON,<br/>key=service_request_id| C{{Kafka:<br/>civic.requests.raw}}
    C -->|consume<br/>earliest offset| D[ClassifierConsumer]
    D -->|prompt + structured output| E[Claude Haiku 4.5<br/>via langchain-anthropic]
    E -.->|trace| F[(Langfuse)]
    D -->|EnrichedRequest<br/>MERGE upsert| G[(Snowflake<br/>CIVIC_311.RAW)]
    D -.->|exceptions| H{{Kafka:<br/>civic.requests.dlq}}
    G --> I[dbt staging view]
    I --> J[dbt intermediate view<br/>days_to_close, met_sla]
    J --> K[fct_sla_compliance<br/>dim_request_category]
    K --> L[Analytics / portfolio]

    classDef ext fill:#1d3557,color:#fff,stroke:#000;
    classDef proc fill:#2a9d8f,color:#fff,stroke:#000;
    classDef storage fill:#e76f51,color:#fff,stroke:#000;
    classDef analytics fill:#264653,color:#fff,stroke:#000;
    class A,E,F ext;
    class B,D,I,J proc;
    class C,G,H storage;
    class K,L analytics;
```

Two processes run independently: the **poller** (HTTP → Kafka) and the
**classifier consumer** (Kafka → LLM → Snowflake → optional DLQ). They are
decoupled so that polling cadence does not block on LLM latency, and so
that the consumer can scale horizontally on Kafka partitions if needed.

## Request lifecycle (sequence)

```mermaid
sequenceDiagram
    participant API as Chicago Open311
    participant P as Open311Poller
    participant K as Kafka<br/>(raw)
    participant C as ClassifierConsumer
    participant LLM as Claude Haiku 4.5
    participant LF as Langfuse
    participant SF as Snowflake
    participant DLQ as Kafka<br/>(dlq)

    P->>API: GET /requests.json
    API-->>P: [ServiceRequest, ...]
    Note over P: dedupe via seen_ids (10K LRU)
    P->>K: produce(key=service_request_id,<br/>value=ServiceRequest JSON)

    K-->>C: poll() returns Message
    C->>C: ServiceRequest.model_validate(payload)
    alt parse fails
        C->>DLQ: produce(headers={exception_*})
    else parse ok
        C->>LLM: prompt + UrgencyClassification schema
        LLM-->>C: {label, score, reasoning}
        LLM-->>LF: trace (tags=[city, service_code])
        alt LLM fails
            C->>DLQ: produce
        else LLM ok
            C->>SF: MERGE INTO ... USING (VALUES ...)
            alt MERGE fails
                C->>DLQ: produce
            else MERGE ok
                C->>K: commit(offset)
            end
        end
    end

    Note over C: Langfuse flush every N messages
    Note over C: lag log every 50 messages
    Note over C: warn if DLQ > 5 in 5 min
```

## Component responsibilities

| Component | Owns | Key invariants |
|---|---|---|
| `ingestion/open311_poller.py` | HTTP polling, dedup, schedule | Never blocks; service-code 404s do not stop other codes |
| `ingestion/kafka_producer.py` | JSON serialization, keying | Message key is always `service_request_id` (UTF-8 bytes) |
| `classifier/urgency_classifier.py` | LangChain chain, structured output, Langfuse callback | Exactly one of 4 labels; confidence ∈ [0, 1]; one-sentence reasoning |
| `classifier/consumer.py` | Kafka loop, DLQ routing, observability | `auto.offset.reset=earliest`; manual commit only after warehouse success; signal-safe shutdown |
| `warehouse/snowflake_writer.py` | MERGE upsert, batching, connection lifecycle | Idempotent on `service_request_id`; chunks at 100 rows; VARIANT via `PARSE_JSON` |
| `dbt_project/` | SQL transformations, SLA macro, tests | Staging is a view; marts are tables; SLA thresholds live in `vars` only |

## Data contracts

```mermaid
classDiagram
    class ServiceRequest {
        +str service_request_id
        +datetime requested_datetime
        +str service_name
        +str service_code
        +str status
        +str address
        +float? lat
        +float? lon
        +str city
        +dict raw_payload
        +from_api_response(data) ServiceRequest
    }

    class EnrichedRequest {
        +str urgency_label
        +float urgency_score
        +str llm_reasoning
        +str langfuse_trace_id
        +datetime classified_at
        +float? days_to_close
    }

    ServiceRequest <|-- EnrichedRequest : extends
```

`ServiceRequest` is the Kafka payload. `EnrichedRequest` is the row written
to `CIVIC_311.RAW.SERVICE_REQUESTS`. The classifier produces an
`EnrichedRequest` from a `ServiceRequest` plus an `UrgencyClassification`
(model output) — no raw dicts cross module boundaries.

## SLA model

Thresholds (hours) are defined exactly once, in `dbt_project.yml`:

| Urgency | Hours | Days |
|---|---:|---:|
| Critical | 4 | 0.17 |
| High | 24 | 1.0 |
| Medium | 72 | 3.0 |
| Low | 168 | 7.0 |

The `sla_threshold_hours()` macro renders a CASE expression from those
vars. `int_resolved_requests` derives `hours_to_close` from
`raw_payload.updated_datetime - requested_datetime` and sets `met_sla =
hours_to_close <= sla_threshold_hours(urgency_label)`. Unknown urgency →
NULL threshold → excluded from `closed_within_sla` and `classified_requests`
but still counted in `total_requests`. `sla_pct` is NULL when
`classified_requests = 0`.

## Failure handling

```mermaid
flowchart TB
    M[Message from Kafka] --> P{Parse OK?}
    P -- no --> DLQ[DLQ +<br/>exception_type, exception_message<br/>in headers]
    P -- yes --> CL{Classifier OK?}
    CL -- no --> DLQ
    CL -- yes --> WH{Snowflake MERGE OK?}
    WH -- no --> DLQ
    WH -- yes --> CMT[commit offset]
    DLQ --> CMT
    CMT --> NXT[Next message]

    classDef good fill:#2a9d8f,color:#fff;
    classDef bad fill:#e63946,color:#fff;
    class CMT,NXT good;
    class DLQ bad;
```

A failure at any stage routes the **original raw bytes** to the DLQ along
with the exception type and message; the offset commit still happens so
the consumer does not get stuck on a poison message. A separate process
can replay the DLQ once the underlying issue is fixed (LLM outage, schema
drift, transient Snowflake error).

The sliding-window DLQ rate alert (`dlq_rate_alert`, > 5 in 5 min) is a
canary: if it fires, something systemic is wrong — an upstream API change,
expired credentials, a regression in the prompt — and human attention is
warranted.

## Why these choices

- **Kafka, not a queue + DB**: real backpressure, replay, partition-based
  scaling, and log compaction by `service_request_id` if/when needed.
  Realistic for the pattern this project demonstrates.
- **Claude Haiku 4.5, not GPT-4 or a fine-tuned classifier**: ~$0.02 /
  1000 requests at this volume, native structured-output support, and
  excellent semantic reasoning for short policy-style prompts. The
  prompt + macro + schema is portable to any Anthropic model.
- **Langfuse, not a generic APM**: native LangChain callback, free tier
  covers low-volume portfolio work, traces include token counts and
  the structured-output schema by default.
- **Snowflake MERGE, not INSERT or COPY**: idempotency on
  `service_request_id`. Replays do not duplicate. The streaming consumer
  and the bulk backfill use the same writer.
- **dbt staging as view, not incremental**: the source table is small
  (~thousands of rows), and the classifier UPDATEs rows in place without
  bumping `_inserted_at`. A view is always fresh and removes a watermark
  failure mode. Marts that get queried for analytics are tables.
