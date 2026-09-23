# CLAUDE.md — Open311 Civic-Request Pipeline

## Project Purpose

A production-pattern streaming pipeline that:
1. **Ingests** live 311 service-request events from Chicago's Open311 API into a Kafka topic on a polling cadence
2. **Classifies** each request's urgency (Critical / High / Medium / Low) via an LLM, with every inference traced in Langfuse
3. **Warehouses** enriched, resolved-request records in DuckDB (local default, no account) or Snowflake
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
| Data warehouse | **DuckDB** (default) / **Snowflake** | DuckDB runs everything with no account (demo, CI); Snowflake is the production target. Same MERGE contract on both |
| Transformation | **dbt Core** (`dbt-duckdb`, `dbt-snowflake`) | Shared SQL models via cross-db macros; tests; docs |
| Orchestration | **Python 3.12** with `asyncio` + `schedule` | Lightweight; no Airflow overkill for Phase 1 |
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
├── requirements.txt            # Pinned; Python 3.12 (.python-version)
├── Makefile                    # `make help` lists every target
├── README.md / ARCHITECTURE.md / CHANGELOG.md / TODO.md / CLAUDE.md
│
├── ingestion/
│   ├── open311_poller.py       # Polls Chicago 311 API, drift check, publishes to Kafka
│   ├── kafka_producer.py       # Wraps confluent-kafka Producer
│   └── schemas.py              # ServiceRequest, EnrichedRequest, observed Open311 key set
│
├── classifier/
│   ├── consumer.py             # Kafka consumer loop, DLQ routing, --exit-when-idle
│   ├── urgency_classifier.py   # LangChain chain + Langfuse tracing (CLASSIFIER_MODE=live)
│   ├── replay_classifier.py    # Recorded labels from the fixture (CLASSIFIER_MODE=replay)
│   └── prompts.py              # Prompt templates (versioned, PROMPT_VERSION)
│
├── warehouse/
│   ├── __init__.py             # COLUMNS contract + build_writer() (WAREHOUSE_BACKEND)
│   ├── duckdb_writer.py        # Local MERGE writer (default)
│   ├── snowflake_writer.py     # Snowflake MERGE writer
│   └── ddl/                    # raw_service_requests.sql (Snowflake), .duckdb.sql
│
├── dbt_project/
│   ├── dbt_project.yml         # vars: sla_thresholds, max_unparseable_close_time_pct
│   ├── profiles.yml.example    # targets: local (duckdb), snowflake
│   ├── models/staging|intermediate|marts/
│   ├── macros/                 # sla_hours.sql, cross_db.sql (json_text, try_to_timestamp)
│   └── tests/                  # grain, sla_pct range, close-time drift tripwire
│
├── data/fixtures/              # 300 real requests + recorded Claude labels (demo, CI, eval)
├── eval/                       # LABELING_GUIDE.md, labels/label_sheet.csv, RESULTS.md
├── docs/                       # demo.gif, screenshots
│
├── scripts/
│   ├── demo_local.sh           # Zero-credential demo (make demo-local / demo-local-nokafka)
│   ├── run_pipeline.sh         # Starts poller + classifier in parallel
│   ├── create_kafka_topic.sh   # One-time topic setup
│   ├── backfill_historical.py  # Bulk-load past N days (unclassified)
│   ├── classify_existing.py    # Classify 'Unknown' rows in place (--sample-seed)
│   ├── export_fixture.py / load_fixture.py
│   ├── sla_report.py           # Print the SLA mart from DuckDB
│   └── make_label_sheet.py / score_labels.py
│
└── tests/                      # pytest: poller, schema drift, dbt tripwire, classifier,
                                # replay, duckdb/snowflake writers, label scoring
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

### Raw Table: `RAW.SERVICE_REQUESTS` (DuckDB and Snowflake)
Mirrors `EnrichedRequest` fields in the order of `warehouse.COLUMNS`, plus `_inserted_at` (default current timestamp). `raw_payload` is `JSON` in DuckDB, `VARIANT` in Snowflake; timestamps are stored as UTC wall-clock.

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
- **Idempotency**: every warehouse writer uses `MERGE INTO` on `service_request_id` — never plain INSERT
- **Error handling**: failed classifications write to a `civic.requests.dlq` dead-letter topic with the exception serialized in the message header
- **Pydantic everywhere**: no raw dicts crossing module boundaries
- **dbt**: staging and intermediate are views, marts are tables. Any incremental model must set `unique_key`. Engine-specific SQL goes through `macros/cross_db.sql`, never inline
- **Replay, not mocks**: the demo and CI replay labels Claude actually produced (`data/fixtures/`). A replay miss goes to the DLQ; never invent a label
- **Schema drift**: new upstream keys are preserved in `raw_payload` and logged; anything the SLA marts depend on gets a dbt test that fails the build
- **Evaluation**: hand-labels are made blind (`eval/LABELING_GUIDE.md`) and committed before scoring; published numbers come from `make score-labels`
- **Logging**: use `structlog` with JSON output; include `trace_id` in every log line from the classifier
- **Polling cadence**: default 60-second interval; configurable via `POLL_INTERVAL_SECONDS`

---

## Scripts

```bash
make help                  # every target
make demo-local            # zero-credential demo through Kafka (needs Docker)
make demo-local-nokafka    # same, straight into DuckDB (what CI runs)

make up && make topics     # Kafka + Zookeeper; civic.requests.raw + .dlq
make run                   # poller + classifier in parallel
python -m ingestion.open311_poller [--dry-run]
python -m classifier.consumer [--exit-when-idle SECONDS]

make backfill DAYS=7                 # historical load, unclassified
make classify LIMIT=300 SEED=311     # classify a seeded sample in place
make export-fixture                  # refresh data/fixtures/

make dbt / make dbt-test / make docs # DBT_TARGET=local (default) or snowflake
make report                          # SLA mart summary from DuckDB
make label-sheet / make score-labels # blind hand-label evaluation
make test                            # pytest
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

# Warehouse / classifier mode
WAREHOUSE_BACKEND=duckdb       # duckdb | snowflake
DUCKDB_PATH=data/civic_311.duckdb
CLASSIFIER_MODE=live           # live | replay
DBT_TARGET=local               # local | snowflake

# Snowflake (only when WAREHOUSE_BACKEND=snowflake)
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

## Case study voice
State plainly what the system is, what it does, the decisions made, and what was learned.

- No disclaimers about the author's experience. Limits belong to the system, stated as scope or cost.
- No honesty signalling ("the honest version", "published as a loss"). State the number.
- No apologising for scale. State the numbers and the design target.
- Real limits, costs, and failures stay — as facts about the system, not confessions.