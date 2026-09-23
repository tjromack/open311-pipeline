# TODO.md — Open311 Civic-Request Pipeline

Phase 1 → 4 are complete and live-verified against real Snowflake + Langfuse.
Phase 5 is mostly done; the remaining items are user-driven (screenshots,
GitHub push, portfolio post).

---

## Phase 1: Infrastructure & Ingestion Foundation ✅ DONE
_Goal: Chicago 311 events flowing into Kafka, verifiable end-to-end_

- [x] `docker-compose.yml` with Kafka + Zookeeper (Confluent 7.5.0), healthcheck, persistent named volumes
- [x] `.env.example` with every required variable and documentation
- [x] `ingestion/schemas.py` — `ServiceRequest` Pydantic model + `from_api_response()` (maps Open311's `long` → `lon`)
- [x] `ingestion/kafka_producer.py` — `CivicRequestProducer` wraps `confluent-kafka` Producer, JSON serialization, `service_request_id` as key
- [x] `ingestion/open311_poller.py` — polls `GET /requests.json`, in-memory LRU dedup (10K), publishes to `civic.requests.raw`. `--dry-run` flag prints JSON instead of producing. `fetch_window()` paginates for historical backfill.
- [x] `scripts/create_kafka_topic.sh` — creates raw + DLQ topics with reasonable partition/retention config
- [x] `tests/test_poller.py` — 4 tests: valid response, cross-poll dedup, HTTP 429, malformed record
- [x] `requirements.txt` pinned for Phase 1 deps
- [x] `docker compose up && python -m ingestion.open311_poller` produces messages

---

## Phase 2: LLM Urgency Classifier with Langfuse Tracing ✅ DONE
_Goal: Every Kafka message classified and traced; dead-letter queue working_

- [x] `EnrichedRequest` Pydantic model (`ServiceRequest` + `urgency_label/score/reasoning/trace_id/classified_at/days_to_close`)
- [x] `classifier/prompts.py` — versioned `SYSTEM_PROMPT` for 4-class classification with explicit tier definitions
- [x] `classifier/urgency_classifier.py` — `ChatAnthropic(model="claude-haiku-4-5-20251001")` + `.with_structured_output(UrgencyClassification)` + Langfuse `CallbackHandler`
- [x] `classifier/consumer.py` — Kafka consumer loop, `auto.offset.reset=earliest`, manual commit, DLQ routing with `exception_type`/`exception_message` headers
- [x] Langfuse tracing: `trace_name="urgency_classification"`, `tags=[city, service_code]`, batched flush every N
- [x] `structlog` JSON logging throughout
- [x] `tests/test_classifier.py` — classifier behavior + DLQ routing + Langfuse callback wiring
- [x] Manual verification: 300 real requests classified, traces visible in Langfuse with expected tags
- [x] Example Langfuse trace URL in README demo section *(placeholder; user to swap in screenshot)*

---

## Phase 3: Snowflake Warehousing ✅ DONE
_Goal: Enriched records persisted to Snowflake with idempotent MERGE; raw table queryable_

- [x] `warehouse/ddl/raw_service_requests.sql` — `CREATE TABLE IF NOT EXISTS` with `VARIANT raw_payload`, `TIMESTAMP_NTZ`, `_inserted_at` audit column, self-bootstrapping DB/schema
- [x] `warehouse/snowflake_writer.py` — `SnowflakeWriter.upsert_batch()` using `MERGE INTO ... USING (VALUES ...)` on `service_request_id`, chunks at 100 rows, `execute_ddl()` helper
- [x] Writer integrated into `classifier/consumer.py` — per-message MERGE after classification, failures route to DLQ
- [x] `scripts/backfill_historical.py` — `--days N` flag, paginates Chicago 311, skips classification, bulk MERGEs to Snowflake
- [x] `scripts/classify_existing.py` — one-off driver that picks up rows where `urgency_label='Unknown'` and runs them through `UrgencyClassifier` (idempotent on the Unknown filter)
- [x] `tests/test_snowflake_writer.py` — MERGE SQL shape, batching (250 → 3 chunks of 100/100/50), idempotency, JSON serialization, DLQ failure path
- [x] Live: backfill of 7,200 rows + 300 classified; 0 duplicates; `SELECT COUNT(*), urgency_label FROM RAW.SERVICE_REQUESTS GROUP BY urgency_label` shows real distribution
- [x] `snowflake-connector-python[pandas]` pinned in `requirements.txt`

---

## Phase 4: dbt Models & SLA Analytics ✅ DONE
_Goal: Mart tables computing SLA compliance by department and category; dbt tests passing_

- [x] `dbt_project/dbt_project.yml` — project config, schema split (`staging`/`intermediate`/`marts`), `vars: sla_thresholds:` (4/24/72/168h)
- [x] `dbt_project/profiles.yml.example` — env-var driven Snowflake profile
- [x] `models/staging/stg_service_requests.sql` — **view** materialization (originally incremental; switched in v1.0.0 because MERGE updates didn't bump `_inserted_at`)
- [x] `models/intermediate/int_resolved_requests.sql` — derives `hours_to_close` / `days_to_close` / `met_sla` from `raw_payload.updated_datetime`, filters to `status='closed'`
- [x] `macros/sla_hours.sql` — `sla_threshold_hours(col)` macro reads `var('sla_thresholds')`, returns NULL for Unknown
- [x] `models/marts/dim_request_category.sql` — one row per service_code with department + modal urgency
- [x] `models/marts/fct_sla_compliance.sql` — department × service_code × month grain with `total_requests`, `classified_requests`, `closed_within_sla`, `sla_pct`, `avg_days_to_close`
- [x] Singular tests: `assert_sla_pct_between_0_and_1.sql` + `assert_fct_sla_grain_unique.sql`
- [x] Schema tests: `unique` + `not_null` on PKs; `accepted_values` on `urgency_label`
- [x] `dbt run && dbt test` — `PASS=4 ERROR=0` build, `PASS=20 ERROR=0` tests
- [x] `dbt docs generate` — `dbt_project/target/index.html` is a self-contained docs site
- [x] `scripts/run_pipeline.sh` — starts poller + consumer in parallel, logs to `logs/pipeline.log`, SIGINT/SIGTERM clean shutdown

---

## Phase 5: Polish, Deploy & Documentation (mostly done; remaining items user-driven)

- [x] `docker-compose.yml` optional Langfuse self-hosted service (commented-toggled)
- [x] `Makefile` with targets: `make up`, `make topics`, `make run`, `make classify`, `make backfill`, `make dbt`, `make dbt-test`, `make docs`, `make test`, `make teardown`
- [x] GitHub Actions CI: `pytest tests/` + `dbt parse` on push and PR to `main` (`.github/workflows/ci.yml`)
- [x] Observability: consumer lag logged per-partition every N messages; sliding-window DLQ rate alert (>5 in 5 min)
- [x] `ARCHITECTURE.md` with Mermaid diagrams (system overview, sequence, data contracts, SLA model, failure handling)
- [x] `--dry-run` flag on the poller (logs to stdout instead of Kafka)
- [x] `LICENSE` (MIT), `CHANGELOG.md` (Keep-a-Changelog format, v1.0.0)
- [x] Comprehensive `README.md` with real numbers from the verification run, troubleshooting section (Norton TLS, OCSP, dbt staging watermark gotcha), repo URL

### Remaining items (user-driven)

- [x] **Capture real screenshots** — `docs/langfuse-trace.png`, `docs/dbt-lineage.png`, `docs/snowflake-sla.png` captured, committed, and embedded in the README Demo section
- [x] **Push to GitHub** — pushed to https://github.com/tjromack/open311-pipeline (`origin/main` up to date)
- [x] **Set repo description + topics** — one-line pitch and 15 topics applied via `gh repo edit`
- [ ] **Flip repo to public** — currently private; make public once final review passes (`gh repo edit --visibility public`)
- [x] **Tag `v1.0.0`** — tag pushed and GitHub release published at https://github.com/tjromack/open311-pipeline/releases/tag/v1.0.0 (release body from CHANGELOG; tag points at the v1.0.0 code state, commit `4670402`)
- [ ] **Portfolio post** — share Langfuse trace screenshot + SLA compliance chart on LinkedIn/Twitter/personal site

### Portfolio audit remediation (2026-09-23, branch `portfolio/audit-remediation`)

- [x] G1: pin `cryptography` / `pyOpenSSL` so a fresh clone collects tests; state Python 3.12
- [x] G1/G6: local DuckDB mode + zero-credential replay demo (`make demo-local`, CI runs the no-Kafka variant)
- [x] G6: verify `make demo-local` through Kafka with Docker running (300 published, 300 consumed, 0 DLQ, dbt 25/25; re-run clean)
- [x] G4: hand-label the 50-row blind sheet, score (54% exact, κ 0.32, QWK 0.64), error analysis in `eval/RESULTS.md`, README filled
- [ ] v2 prompt: sharpen the Low/Medium boundary, re-score against the same 50 labels
- [ ] Re-label the same 50 a week later (self-agreement), or get a second labeller
- [x] G3: schema-drift tests + dbt tripwire; README documents what fails loudly and what doesn't
- [x] G5/G8: limits section and demonstrates line
- [ ] Record a demo GIF of `make demo-local` for the README / portfolio card

### Stretch / nice-to-have follow-ups (not blocking v1.0.0)

- [ ] **Department mapping** — `dim_request_category.department` is mostly NULL because Open311's `group` field lives only on the services catalog, not on requests. Fix by seeding a `service_code → department` CSV, or pulling `services.json` during backfill and joining.
- [ ] **Classify the remaining ~6,900 Unknown rows** — `make classify` with no `LIMIT` will work through them all (~4 hours sequential, ~$5 in Anthropic API). Or implement concurrent classification (`asyncio` + bounded semaphore around the LLM call) to do it in ~30 min.
- [ ] **Loom recording** — 2-minute walkthrough of the live pipeline: events flowing → Langfuse traces → Snowflake rows → dbt SLA chart
- [ ] **dbt source freshness** — declare `loaded_at_field: _inserted_at` on `civic_311.service_requests` so `dbt source freshness` warns when ingestion stalls
