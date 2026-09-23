# Changelog

All notable changes to this project are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versioning follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.1.0] — 2026-09-23

The pipeline runs from a fresh clone with no accounts, every dbt test runs on
real data in CI, schema drift fails the build, and the classifier's agreement
with blind hand-labels is published.

### Added

- **Local DuckDB mode.** `WAREHOUSE_BACKEND=duckdb` (now the default) writes
  through `DuckDBWriter` with the same `MERGE`-on-`service_request_id`
  contract as Snowflake; dbt gains a `local` (dbt-duckdb) target alongside
  `snowflake`. Two dispatched macros cover the only Snowflake-specific SQL.
- **Zero-credential replay demo.** `make demo-local` / `demo-local-nokafka`
  replay 300 real Chicago requests with their recorded Claude labels
  (`CLASSIFIER_MODE=replay`, fixture in `data/fixtures/`). CI runs it.
- **Schema-drift detection.** Poller logs an aggregated
  `open311_schema_drift` warning against the observed key set; dbt test
  `assert_closed_requests_have_close_time` fails the build if close times
  disappear. Tests cover 10 drift scenarios plus an end-to-end dbt run.
- **Blind hand-label harness.** `make label-sheet` / `make score-labels`
  (agreement, Cohen's and quadratic-weighted kappa, confusion matrix).
- `scripts/sla_report.py`, `scripts/export_fixture.py`,
  `scripts/load_fixture.py`; `classify_existing --sample-seed`;
  `classifier.consumer --exit-when-idle`.
- **Evaluation results.** 50 blind hand-labels scored against the model:
  54% exact agreement, 98% within one tier, Cohen's kappa 0.32,
  quadratic-weighted kappa 0.64 (`eval/RESULTS.md`).
- README: try-it path with a recorded demo (`docs/demo.gif`), results from the
  2026-09-23 run, "How it's verified", payload-drift behaviour, Langfuse drift
  signals, and a limits section. ARCHITECTURE.md covers both warehouse
  backends, replay mode and drift handling.

### Changed

- Headline results come from a 2026-09-23 run (7,493 requests, 300
  classified) reproducible from the committed fixture. The v1.0.0 Snowflake
  run is kept as history; that trial account has expired.
- Classification cost is measured, not estimated: ~1,290 input and ~100
  output tokens per request, ~$1.80 per 1,000 at Haiku 4.5 pricing (the
  previous "~$0.02 / 1000" figure was wrong).
- The rodent-baiting SLA gap is reported as a lead: the hand-labels put that
  category at Medium, not the model's High.
- CI uses the Node 24 releases of `actions/checkout` and `actions/setup-python`.

### Fixed

- Fresh installs failed test collection: an unpinned resolve paired
  `cryptography` 46 with `pyOpenSSL` 22, which crash the Snowflake connector
  at import. Both are now pinned.
- `python scripts/<name>.py` failed with `ModuleNotFoundError` outside a
  shell that had `PYTHONPATH` set.
- A non-object record in an Open311 response crashed the whole poll tick.
- Shell scripts and the Makefile are pinned to LF so they run after a
  Windows checkout.

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
