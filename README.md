# open311-pipeline

> **Stream → Classify → Warehouse → Model** — a production-pattern civic data pipeline using Chicago 311, Kafka, LLM urgency classification with Langfuse tracing, and Snowflake + dbt analytics.

[![CI](https://github.com/tjromack/open311-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/tjromack/open311-pipeline/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/Python_3.12-3776AB?style=flat&logo=python&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?style=flat&logo=apachekafka&logoColor=white)
![Claude](https://img.shields.io/badge/Claude_Haiku_4.5-D97757?style=flat&logo=anthropic&logoColor=white)
![Langfuse](https://img.shields.io/badge/Langfuse-traced-8B5CF6?style=flat)
![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=flat&logo=snowflake&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-FF694B?style=flat&logo=dbt&logoColor=white)
![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)

---

## What it is

Live municipal 311 service requests (potholes, streetlight outages, rodent complaints, etc.) → Apache Kafka → urgency classified by **Claude Haiku 4.5** (Critical/High/Medium/Low + confidence + reasoning, every call traced in **Langfuse**) → persisted to **Snowflake** via idempotent `MERGE` → **dbt** models compute department-level SLA compliance by request category.

A working civic-data product *and* a reusable template for the **stream → classify → warehouse → model** pattern. See [`ARCHITECTURE.md`](ARCHITECTURE.md) for the diagrams and component-by-component walk-through.

---

## Headline results (from the initial verification run)

- **7,200** historical Chicago 311 closed requests ingested over a 7-day window
- **300** rows classified by Claude Haiku 4.5 in a representative sample (~$0.20 spend, ~8 min)
- **54 / 112** `fct_sla_compliance` buckets populated with real `sla_pct` values
- **92.2%** average SLA compliance across populated buckets; **45 buckets at 100%**
- **0** duplicates across two backfill runs (`MERGE INTO` idempotency holds)
- **All 300** classified rows traced in Langfuse with `[city, service_code, urgency_classification]` tags

Real categories where the model surfaces interesting signal:

| Category | Typical urgency | SLA compliance | Avg days to close |
|---|---|---:|---:|
| Graffiti Removal | Low | 100% | 0.98 |
| Yard Waste Pick-Up | Low | 100% | 1.23 |
| Tree Emergency | High | 100% | 1.03 |
| Sanitation Code Violation | Medium | 92% | 1.57 |
| Vicious Animal Complaint | High | 78% | 0.66 |
| Dead Animal Pick-Up | Medium | 63% | 2.90 |
| Abandoned Vehicle | High | 31% | 1.33 |
| **Rodent Baiting / Rat** | **High** | **12%** | **3.30** |

> Rodent Baiting is classified High urgency (24h SLA threshold) but averages 3.3 days to close — the model surfaces a real, actionable gap that a city operations team could use.

---

## Demo

> _📸 Screenshot placeholders — capture from your own run and drop into `docs/`:_
>
> - `docs/langfuse-trace.png` — a Langfuse trace showing the urgency classification span, input prompt, structured output, token counts, and tags.
> - `docs/dbt-lineage.png` — `dbt docs serve` lineage graph: `source.civic_311.service_requests → stg_service_requests → int_resolved_requests → fct_sla_compliance`.
> - `docs/snowflake-sla.png` — a Snowflake worksheet running `SELECT service_name, sla_pct, avg_days_to_close FROM CIVIC_311.ANALYTICS_MARTS.FCT_SLA_COMPLIANCE ORDER BY classified_requests DESC LIMIT 10;`

---

## Tech stack

| Layer | Technology | Why |
|---|---|---|
| Event broker | **Apache Kafka** (Confluent local / Docker) | Decouples polling from processing; realistic backpressure |
| Stream source | **Chicago Open311 REST API** | Free, high-volume, well-documented |
| LLM classifier | **Anthropic Claude Haiku 4.5** via `langchain-anthropic` | Cheap; native `.with_structured_output()` |
| LLM tracing | **Langfuse** (cloud or self-hosted) | Native LangChain callback; per-call token + latency traces |
| Data warehouse | **Snowflake** | `VARIANT` for raw payload, `MERGE` for idempotent upserts |
| Transformation | **dbt Core** | SQL-based SLA models, schema + singular tests, docs |
| Orchestration | Plain **Python 3.12** + `schedule` | Lightweight; the poller and consumer are independent processes |
| Containers | **Docker Compose** | Kafka + Zookeeper, optional self-hosted Langfuse |
| Config | **python-dotenv** + `.env` | Never hardcode credentials |
| CI | **GitHub Actions** | `pytest` + `dbt parse` on every push and PR |

---

## Quickstart

Tested on Windows 10 / Python 3.12. macOS and Linux should work identically once `make` is available (Windows users: install Git Bash).

### 1. Clone & configure

```bash
git clone https://github.com/tjromack/open311-pipeline.git
cd open311-pipeline
cp .env.example .env
# Fill in: ANTHROPIC_API_KEY, LANGFUSE_*, SNOWFLAKE_*.
```

### 2. Python deps

```bash
python -m venv .venv
.\.venv\Scripts\Activate.ps1     # Windows
# source .venv/bin/activate      # macOS / Linux
pip install -r requirements.txt
```

### 3. Provision Snowflake (one time)

In a Snowsight worksheet (or via the snowflake CLI):

```sql
USE ROLE ACCOUNTADMIN;
CREATE DATABASE IF NOT EXISTS CIVIC_311;
CREATE SCHEMA   IF NOT EXISTS CIVIC_311.RAW;
```

Then apply the table DDL programmatically:

```bash
python -c "from pathlib import Path; from warehouse.snowflake_writer import SnowflakeWriter; \
  ddl=Path('warehouse/ddl/raw_service_requests.sql').read_text(); \
  w=SnowflakeWriter(); w.execute_ddl(ddl); w.close()"
```

### 4. Start Kafka + create topics

```bash
make up           # docker compose up -d
make topics       # creates civic.requests.raw + civic.requests.dlq
```

### 5. Run the pipeline

```bash
make run          # poller + consumer in parallel, logs to logs/pipeline.log
# Ctrl+C stops both cleanly
```

Or run them individually:

```bash
python -m ingestion.open311_poller             # poller only
python -m ingestion.open311_poller --dry-run   # poller, prints JSON instead of producing
python -m classifier.consumer                  # consumer only
```

### 6. Historical backfill (one-off)

```bash
make backfill DAYS=7
# or: python scripts/backfill_historical.py --days 7
```

The backfill skips classification (writes `urgency_label='Unknown'`). To classify already-loaded rows in place:

```bash
make classify LIMIT=300
# or: python scripts/classify_existing.py --limit 300
```

The `--limit` flag is optional; without it, the script classifies every row where `urgency_label='Unknown'` until done. Safe to interrupt and resume — it filters on `Unknown` each invocation.

### 7. Build the dbt marts

Copy the example profile (gitignored):

```bash
cp dbt_project/profiles.yml.example dbt_project/profiles.yml
export DBT_PROFILES_DIR=$(pwd)/dbt_project   # or set in your shell
```

Then:

```bash
make dbt          # dbt run
make dbt-test     # dbt test
make docs         # dbt docs generate
make docs-serve   # serve the docs site at http://localhost:8000 (Ctrl+C to stop). Override port with `make docs-serve PORT=8888`.
```

> The dbt docs site is a SPA that fetches `manifest.json` and `catalog.json` via XHR. Browsers block those fetches over `file://`, so you have to serve `dbt_project/target/` over HTTP. `make docs-serve` is a one-line `python -m http.server` wrapper; alternatively, `dbt docs serve --project-dir dbt_project --port 8080` does the same with live reload.

---

## Project layout

```
open311-pipeline/
├── ingestion/                  # Open311 poller + Kafka producer + Pydantic models
├── classifier/                 # Kafka consumer + Claude classifier + prompts
├── warehouse/                  # Snowflake writer + DDL
├── dbt_project/                # dbt models (staging / intermediate / marts), macros, tests
├── scripts/                    # run_pipeline.sh, create_kafka_topic.sh, backfill_historical.py, classify_existing.py
├── tests/                      # pytest unit tests
├── docker-compose.yml          # Kafka, Zookeeper, optional self-hosted Langfuse
├── Makefile                    # make up / topics / run / dbt / docs / test / teardown
└── .github/workflows/ci.yml    # pytest + dbt parse on push/PR
```

---

## Configuration

All configuration lives in `.env`. See [`.env.example`](.env.example) for the full list with comments. Key variables:

| Variable | Purpose |
|---|---|
| `CHICAGO_311_BASE_URL` | Open311 endpoint (default: Chicago) |
| `POLL_INTERVAL_SECONDS` | Poll cadence (default 60; never drop below 30) |
| `SERVICE_CODES` | Comma-separated service codes to filter (leave empty for all categories) |
| `KAFKA_BOOTSTRAP_SERVERS` | Defaults to `localhost:9092` |
| `ANTHROPIC_API_KEY` | Claude API key |
| `ANTHROPIC_MODEL` | Defaults to `claude-haiku-4-5-20251001` |
| `LANGFUSE_PUBLIC_KEY` / `LANGFUSE_SECRET_KEY` | Langfuse project keys |
| `LANGFUSE_HOST` | `https://cloud.langfuse.com` or self-hosted URL |
| `LANGFUSE_FLUSH_EVERY_N` | Batch size for Langfuse trace flushing (default 50) |
| `SNOWFLAKE_*` | Account, user, password, database, schema, warehouse, role |
| `OPEN311_VERIFY_TLS` | Set to `false` to disable hostname verification for Open311 (see Troubleshooting) |

SLA thresholds live exclusively in `dbt_project.yml` under `vars: sla_thresholds:` — never hardcoded in SQL.

---

## Troubleshooting

### Local TLS interception (Norton / Avast / corporate proxies)

If your endpoint security or workplace network re-signs TLS connections — Norton's HTTPS scanning is the most common offender on Windows — you'll see one or more of:

- `SSL: CERTIFICATE_VERIFY_FAILED` from `httpx` or `requests`
- Snowflake connector hanging during login with `OperationalError: 250001`
- "The certificate's CN name does not match the passed value" errors

The fix is to build a combined CA bundle (certifi + your interceptor's root CA) and point Python at it. On Windows with Norton:

```powershell
# 1. Locate your interceptor's root CA in the Windows cert store
Get-ChildItem -Path Cert:\ -Recurse |
  Where-Object { $_.Subject -like '*Norton*' -or $_.Subject -like '*Symantec*' } |
  Select-Object PSPath, Subject

# 2. Export it (replace <thumbprint> with the one from step 1)
$cert = Get-ChildItem Cert:\LocalMachine\Root\<thumbprint>
$b64 = [Convert]::ToBase64String($cert.RawData, [Base64FormattingOptions]::InsertLineBreaks)
"-----BEGIN CERTIFICATE-----`r`n$b64`r`n-----END CERTIFICATE-----" |
  Out-File .certs\interceptor_root.pem -Encoding ASCII

# 3. Concatenate certifi's bundle with the interceptor cert
$certifi = .\.venv\Scripts\python -c "import certifi; print(certifi.where())"
(Get-Content $certifi -Raw) + "`r`n" + (Get-Content .certs\interceptor_root.pem -Raw) |
  Out-File .certs\combined_ca.pem -Encoding ASCII

# 4. Point Python at it (add to .env so it persists)
$env:REQUESTS_CA_BUNDLE = (Resolve-Path .certs\combined_ca.pem).Path
```

For **Snowflake's OCSP** revocation check (which Norton-re-signed certs lack a URL for), set `SF_OCSP_FAIL_OPEN=true` — Snowflake's documented escape hatch for proxy environments.

For the **Chicago 311 API itself**, the upstream CDN occasionally serves a placeholder cert for `empty.spotmobile.net` (a known SeeClickFix/SpotReporters infrastructure quirk). If you hit "CN name does not match", set `OPEN311_VERIFY_TLS=false` in `.env`. The poller honors this opt-out on both `fetch_recent()` and `fetch_window()`.

These are documented in [`.env.example`](.env.example) so they don't surprise anyone setting up locally.

### `dbt run` is hanging

Almost certainly the OCSP issue above. Confirm by checking `dbt_project/logs/dbt.log` — if you see `Opening a new connection, currently in state init` followed by no further activity, set `SF_OCSP_FAIL_OPEN=true` and retry.

### Stale `urgency_label` in dbt marts after running the classifier

Was a real bug in the initial implementation. The staging model was incremental on `_inserted_at`, but the classifier UPDATEs the source row via MERGE without bumping `_inserted_at`, so staging never picked up the change. **Fixed in v1.0.0** by switching staging to a view materialization. If you're on an older commit, run `dbt run --full-refresh` once.

### PowerShell can't load `.env` correctly

`python-dotenv` handles inline comments and `${VAR}` interpolation. If you're loading `.env` directly in PowerShell for ad-hoc commands, use the same library:

```powershell
$envJson = .\.venv\Scripts\python -c "from dotenv import dotenv_values; import json; print(json.dumps({k:v for k,v in dotenv_values('.env').items() if v is not None}))"
($envJson | ConvertFrom-Json).PSObject.Properties | ForEach-Object { Set-Item -Path "Env:$($_.Name)" -Value $_.Value }
```

---

## Pipeline architecture

```
Chicago Open311 API
        │  (HTTP poll, 60s cadence, in-memory dedup)
        ▼
  Kafka Topic: civic.requests.raw
        │  (consumer group: urgency-classifier-group, earliest offset)
        ▼
  ClassifierConsumer
        │  ├── UrgencyClassifier (Claude Haiku 4.5 + Langfuse callback)
        │  ├── SnowflakeWriter (MERGE INTO ... idempotent upsert)
        │  └── DLQ on any failure (civic.requests.dlq)
        ▼
  Snowflake: CIVIC_311.RAW.SERVICE_REQUESTS  (VARIANT raw_payload)
        │
        ▼
  dbt models
        ├── stg_service_requests  (view)
        ├── int_resolved_requests (view: days_to_close, met_sla)
        └── marts/
              ├── dim_request_category   (table)
              └── fct_sla_compliance     (table: SLA % by dept × category × month)
```

See [`ARCHITECTURE.md`](ARCHITECTURE.md) for sequence diagrams and component-level walkthroughs.

---

## Roadmap

See [`TODO.md`](TODO.md). v1.0.0 closes out the first four phases (infrastructure, classifier, warehouse, dbt marts). The phase 5 polish work — Loom demo, real screenshots, and bigger classification runs — is captured there and in [`CHANGELOG.md`](CHANGELOG.md).

---

## License

MIT — see [`LICENSE`](LICENSE).
