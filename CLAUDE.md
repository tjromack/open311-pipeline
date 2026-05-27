# CLAUDE.md — Open311 Civic-Request Pipeline

## Project Purpose

A production-pattern streaming pipeline that:
1. **Ingests** live 311 service-request events from Chicago's Open311 API into a Kafka topic on a polling cadence
2. **Classifies** each request's urgency (Critical / High / Medium / Low) via an LLM, with every inference traced in Langfuse
3. **Warehouses** enriched, resolved-request records in Snowflake
4. **Models** department SLA compliance by category using dbt, producing analytics-ready tables

The pipeline is a reusable template for the pattern: **stream → classify → warehouse → model**.

---

## Tech Stack & Rationale

| Layer | Technology | Why |
|-------|-----------|-----|
| Event broker | **Apache Kafka** (Confluent local / Docker) | Decouples polling from processing; realistic backpressure simulation |
| Stream source | **Chicago Open311 REST API** | Well-documented, actively maintained, free, high-volume |
| LLM classifier | **Anthropic Claude Haiku 4.5** (`claude-haiku-4-5-20251001`) via `langchain-anthropic` | Cost-effective; native tool-call / structured-output support; swappable |
| LLM tracing | **Langfuse** (cloud or self-hosted) | Native LangChain integration; traces each classification call |
| Data warehouse | **Snowflake** | Columnar, scales, dbt-native; fulfills portfolio gap |
| Transformation | **dbt Core** | SQL-based SLA models, tests, docs |
| Orchestration | **Python 3.11** with `asyncio` + `schedule` | Lightweight; no Airflow overkill for Phase 1 |
| Containerization | **Docker Compose** | Kafka + Zookeeper + optional local Langfuse |
| Config/secrets | **python-dotenv** + `.env` file | Never hardcode credentials |
| Dependency mgmt | **uv** (or pip + requirements.txt) | Fast installs |

---

## Project Structure

```
open311-pipeline/
├── docker-compose.yml          # Kafka, Zookeeper (+ optional Langfuse)
├── .env.example                # Template for all env vars
├── .env                        # Local secrets — NEVER commit
├── requirements.txt
├── README.md
├── CLAUDE.md
├── TODO.md
│
├── ingestion/
│   ├── __init__.py
│   ├── open311_poller.py        # Polls Chicago 311 API, publishes to Kafka
│   ├── kafka_producer.py       # Wraps confluent-kafka Producer
│   └── schemas.py              # Pydantic models: ServiceRequest, EnrichedRequest
│
├── classifier/
│   ├── __init__.py
│   ├── consumer.py             # Kafka consumer loop
│   ├── urgency_classifier.py   # LangChain chain + Langfuse tracing
│   └── prompts.py              # Prompt templates (versioned)
│
├── warehouse/
│   ├── __init__.py
│   ├── snowflake_writer.py     # Writes enriched records to Snowflake raw table
│   └── ddl/
│       └── raw_service_requests.sql   # CREATE TABLE statement
│
├── dbt_project/
│   ├── dbt_project.yml
│   ├── profiles.yml.example    # Snowflake connection template
│   ├── models/
│   │   ├── staging/
│   │   │   └── stg_service_requests.sql
│   │   ├── intermediate/
│   │   │   └── int_resolved_requests.sql
│   │   └── marts/
│   │       ├── fct_sla_compliance.sql
│   │       └── dim_request_category.sql
│   ├── tests/
│   │   └── assert_sla_pct_between_0_and_1.sql
│   └── macros/
│       └── sla_hours.sql
│
├── scripts/
│   ├── run_pipeline.sh         # Starts poller + classifier in parallel
│   ├── create_kafka_topic.sh   # One-time topic setup
│   └── backfill_historical.py  # Optional: bulk-load past 30 days
│
└── tests/
    ├── test_classifier.py
    ├── test_poller.py
    └── test_snowflake_writer.py
```

---

## Data Model

### Kafka Topic: `civic.requests.raw`
Message key: `service_request_id` (string)
Message value: JSON-serialized `ServiceRequest`

### Pydantic: `ServiceRequest` (ingestion output)
```python
class ServiceRequest(BaseModel):
    service_request_id: str
    requested_datetime: datetime
    service_name: str          # "Pothole in Street", "Street Light Out", etc.
    service_code: str
    status: str                # "open" | "closed"
    address: str
    lat: Optional[float]
    lon: Optional[float]
    city: str = "chicago"
    raw_payload: dict          # original API response preserved
```

### Pydantic: `EnrichedRequest` (classifier output → Snowflake)
```python
class EnrichedRequest(ServiceRequest):
    urgency_label: str         # "Critical" | "High" | "Medium" | "Low"
    urgency_score: float       # 0.0–1.0 confidence
    llm_reasoning: str         # one-sentence explanation
    langfuse_trace_id: str
    classified_at: datetime
    days_to_close: Optional[float]   # null if still open
```

### Snowflake Raw Table: `RAW.SERVICE_REQUESTS`
Mirrors `EnrichedRequest` fields, plus `_inserted_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP`.

### dbt Mart: `fct_sla_compliance`
```
department | category | month | total_requests | closed_within_sla | sla_pct | avg_days_to_close
```
SLA thresholds defined in `dbt_project.yml` vars:
- Critical: 4 hours
- High: 24 hours
- Medium: 72 hours
- Low: 168 hours (7 days)

---

## Key Conventions

- **Kafka messages**: always JSON, always include `service_request_id` as the message key for log compaction compatibility
- **Langfuse tracing**: every LLM call must set `trace_name="urgency_classification"`, `tags=[city, service_code]`, and capture input/output tokens in metadata
- **Idempotency**: the Snowflake writer uses `MERGE INTO` on `service_request_id` — never plain INSERT
- **Error handling**: failed classifications write to a `civic.requests.dlq` dead-letter topic with the exception serialized in the message header
- **Pydantic everywhere**: no raw dicts crossing module boundaries
- **dbt**: all models have `{{ config(materialized='incremental', unique_key='service_request_id') }}` where applicable
- **Logging**: use `structlog` with JSON output; include `trace_id` in every log line from the classifier
- **Polling cadence**: default 60-second interval; configurable via `POLL_INTERVAL_SECONDS`

---

## Scripts

```bash
# Start infrastructure
docker compose up -d

# Create Kafka topics
bash scripts/create_kafka_topic.sh

# Run full pipeline (poller + classifier in parallel)
bash scripts/run_pipeline.sh

# Run only the poller
python -m ingestion.open311_poller

# Run only the classifier/consumer
python -m classifier.consumer

# dbt commands (from dbt_project/ dir)
dbt run
dbt test
dbt docs generate && dbt docs serve

# Backfill last 30 days (one-time)
python scripts/backfill_historical.py --days 30

# Tests
pytest tests/ -v
```

---

## Environment Variables

```dotenv
# Chicago Open311
CHICAGO_311_API_KEY=           # Optional — higher rate limits with key
CHICAGO_311_BASE_URL=https://311api.cityofchicago.org/open311/v2
POLL_INTERVAL_SECONDS=60
SERVICE_CODES=4fd3055e7f3b34730000000b,4ffa9f2d6018277d4000000b  # comma-sep

# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_RAW_TOPIC=civic.requests.raw
KAFKA_DLQ_TOPIC=civic.requests.dlq
KAFKA_CONSUMER_GROUP=urgency-classifier-group

# Anthropic Claude API (classifier)
ANTHROPIC_API_KEY=
ANTHROPIC_MODEL=claude-haiku-4-5-20251001

# Langfuse
LANGFUSE_PUBLIC_KEY=
LANGFUSE_SECRET_KEY=
LANGFUSE_HOST=https://cloud.langfuse.com   # or self-hosted URL

# Snowflake
SNOWFLAKE_ACCOUNT=
SNOWFLAKE_USER=
SNOWFLAKE_PASSWORD=
SNOWFLAKE_DATABASE=CIVIC_311
SNOWFLAKE_SCHEMA=RAW
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_ROLE=SYSADMIN

# dbt (mirrors Snowflake vars, used in profiles.yml)
DBT_SNOWFLAKE_ACCOUNT=${SNOWFLAKE_ACCOUNT}
DBT_SNOWFLAKE_USER=${SNOWFLAKE_USER}
DBT_SNOWFLAKE_PASSWORD=${SNOWFLAKE_PASSWORD}
```

---

## What NOT To Do

- **Do NOT** commit `.env` or any file containing real API keys/passwords — `.gitignore` must cover `.env`, `profiles.yml`
- **Do NOT** use `auto.offset.reset=latest` for the classifier consumer — use `earliest` so no events are dropped on restart
- **Do NOT** make synchronous HTTP calls inside the Kafka consumer loop — the poller and consumer are separate processes
- **Do NOT** skip the dead-letter queue — a bad LLM response or API outage should not crash the consumer
- **Do NOT** use `SELECT *` in dbt staging models — always be explicit about columns
- **Do NOT** create incremental dbt models without a `unique_key` — Snowflake will duplicate rows on re-run
- **Do NOT** call Langfuse `flush()` on every message — batch flush at consumer shutdown or every N messages
- **Do NOT** hardcode SLA thresholds in SQL — they live in `dbt_project.yml` vars only
- **Do NOT** poll the 311 API faster than 30-second intervals — respect rate limits even with an API key
- **Do NOT** store `raw_payload` as a string — use Snowflake `VARIANT` type for the JSON column
