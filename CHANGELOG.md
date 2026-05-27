# Changelog

All notable changes to this project are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versioning follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.0.0] — 2026-05-27

First end-to-end release. The pipeline ingests live Chicago Open311 events,
classifies urgency with Claude Haiku 4.5 (traced in Langfuse), persists to
Snowflake with idempotent MERGE upserts, and computes SLA compliance via dbt.

### Added

- **Ingestion**: `Open311Poller` polls Chicago 311 on a configurable cadence
  with in-memory deduplication (10K-entry LRU). Publishes `ServiceRequest`
  Pydantic models to `civic.requests.raw` keyed by `service_request_id`.
  `fetch_window(start, end)` paginates over a date range for historical
  backfills. `--dry-run` flag prints JSON to stdout instead of publishing.
- **Classifier**: `UrgencyClassifier` uses `langchain-anthropic`'s
  `ChatAnthropic(model="claude-haiku-4-5-20251001")` with
  `.with_structured_output(UrgencyClassification)` for deterministic
  Critical/High/Medium/Low + confidence score + reasoning. Every call is
  traced in Langfuse with `trace_name="urgency_classification"` and
  `[city, service_code]` tags.
- **Consumer**: `ClassifierConsumer` reads `civic.requests.raw`
  (`auto.offset.reset=earliest`, manual commit), classifies, writes to
  Snowflake. Failures at parse, classify, or warehouse stages route to
  `civic.requests.dlq` with exception type + message in headers. Batched
  Langfuse flush. Per-partition consumer-lag logging every N messages.
  Sliding-window DLQ rate alert (>5 in 5 min).
- **Warehouse**: `SnowflakeWriter.upsert_batch()` uses `MERGE INTO ... USING
  VALUES` keyed on `service_request_id` with `PARSE_JSON` for the VARIANT
  `raw_payload` column. Chunks at 100 rows. Connection-as-context-manager.
  `execute_ddl()` helper applies the bundled DDL programmatically.
- **dbt**: staging (`view`) → intermediate (`view`, derives `hours_to_close`,
  `met_sla`) → marts (`table`): `dim_request_category` (one row per service
  code with department + modal urgency) and `fct_sla_compliance` (department
  × service_code × month grain with `total_requests`,
  `classified_requests`, `closed_within_sla`, `sla_pct`,
  `avg_days_to_close`). SLA thresholds (4/24/72/168h) live in
  `dbt_project.yml` vars and are read via the `sla_threshold_hours` macro.
- **Tests**: 23 pytest tests covering Pydantic parsing, dedup, DLQ routing,
  Langfuse callback wiring, warehouse MERGE shape, batching, idempotency,
  and DLQ rate alert. 20 dbt data tests (`unique`/`not_null`/
  `accepted_values`) plus two singular tests (`sla_pct` ∈ [0,1] and grain
  uniqueness on `fct_sla_compliance`).
- **Infra**: `docker-compose.yml` for Kafka + Zookeeper with healthcheck and
  named volumes. Optional self-hosted Langfuse service block (commented).
  `scripts/create_kafka_topic.sh`, `scripts/run_pipeline.sh`,
  `scripts/backfill_historical.py --days N`, `scripts/classify_existing.py
  --limit N`.
- **CI**: GitHub Actions workflow runs `pytest` + `dbt parse` on push and PR
  to `main`.
- **Docs**: `README.md` quickstart, `ARCHITECTURE.md` with Mermaid diagrams,
  `CLAUDE.md` project conventions, `TODO.md` phased roadmap.

### Notes on the initial verification run

- 7,200 historical Chicago 311 requests backfilled into Snowflake via
  `MERGE INTO`. Re-running the backfill is idempotent (0 duplicate
  `service_request_id`s).
- 300 of those rows classified via Claude Haiku 4.5 (representative
  sample). All 300 have populated `langfuse_trace_id`. Result distribution
  was 162 Low / 90 High / 43 Medium / 5 Critical.
- After `dbt run`, 54 of 112 `fct_sla_compliance` buckets had non-NULL
  `sla_pct`. Average SLA compliance across populated buckets: 92.2%.
  Notable outliers (real signal): Rodent Baiting at ~12% (avg 3.30 days
  vs. 24h High SLA); Abandoned Vehicle at ~31% (avg 1.33 days vs. 24h).

### Known limitations

- `dim_request_category.department` is mostly NULL because Chicago's
  Open311 puts the `group` field on the services catalog, not on
  individual request records. Two follow-ups under consideration:
  seed a `service_code → department` CSV, or pull `services.json`
  during backfill and join.
- Local TLS interception (e.g. Norton/Avast HTTPS scanning) requires
  building a combined CA bundle and pointing `REQUESTS_CA_BUNDLE` at it.
  Documented in `README.md` under Troubleshooting.

[Unreleased]: https://github.com/tjromack/open311-pipeline/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/tjromack/open311-pipeline/releases/tag/v1.0.0
