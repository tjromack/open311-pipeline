# open311-pipeline — convenience targets.
# All targets assume the project venv is active (or that the venv binaries
# are on PATH). On Windows, run from Git Bash.

SHELL := /usr/bin/env bash
PYTHON ?= python
DBT_PROJECT_DIR ?= dbt_project
export DBT_PROFILES_DIR ?= $(CURDIR)/$(DBT_PROJECT_DIR)

.PHONY: help demo-local demo-local-nokafka up topics run classify backfill export-fixture report label-sheet score-labels dbt dbt-test docs docs-serve test lint teardown

help:                  ## Show this help.
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' Makefile | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

demo-local:            ## Zero-credential demo: fixture -> Kafka -> replayed labels -> DuckDB -> dbt build.
	PYTHON=$(PYTHON) bash scripts/demo_local.sh

demo-local-nokafka:    ## Same demo without Docker: fixture straight into DuckDB, then dbt build.
	PYTHON=$(PYTHON) bash scripts/demo_local.sh --no-kafka

up:                    ## Start Kafka + Zookeeper in Docker.
	docker compose up -d

topics:                ## Create civic.requests.raw + civic.requests.dlq topics.
	bash scripts/create_kafka_topic.sh

run:                   ## Start poller + classifier consumer in parallel (logs/pipeline.log).
	bash scripts/run_pipeline.sh

classify:              ## Classify warehouse rows where urgency_label='Unknown' (LIMIT=N, SEED=N for a random sample).
	$(PYTHON) scripts/classify_existing.py $(if $(LIMIT),--limit $(LIMIT),) $(if $(SEED),--sample-seed $(SEED),)

backfill:              ## One-off historical backfill: --days N (defaults to 7).
	$(PYTHON) scripts/backfill_historical.py --days $(or $(DAYS),7)

export-fixture:        ## Export classified DuckDB rows to data/fixtures/ for the replay demo.
	$(PYTHON) scripts/export_fixture.py

report:                ## Print the SLA compliance mart from the local DuckDB warehouse.
	$(PYTHON) scripts/sla_report.py

label-sheet:           ## Write the blind 50-row hand-labeling sheet (eval/labels/label_sheet.csv).
	$(PYTHON) scripts/make_label_sheet.py

score-labels:          ## Score model labels vs. hand-labels -> eval/RESULTS.md.
	$(PYTHON) scripts/score_labels.py

$(DBT_PROJECT_DIR)/profiles.yml:
	cp $(DBT_PROJECT_DIR)/profiles.yml.example $@

dbt: $(DBT_PROJECT_DIR)/profiles.yml   ## Build dbt models (DuckDB by default; DBT_TARGET=snowflake for Snowflake).
	dbt run --project-dir $(DBT_PROJECT_DIR)

dbt-test: $(DBT_PROJECT_DIR)/profiles.yml   ## Run dbt tests (schema + singular).
	dbt test --project-dir $(DBT_PROJECT_DIR)

docs: $(DBT_PROJECT_DIR)/profiles.yml   ## Generate the dbt docs site (then run `make docs-serve` to view).
	dbt docs generate --project-dir $(DBT_PROJECT_DIR)

docs-serve:            ## Serve the generated docs at http://localhost:8000. Ctrl+C to stop. Override with PORT=N.
	$(PYTHON) -m http.server $(or $(PORT),8000) -d $(DBT_PROJECT_DIR)/target

test:                  ## Run pytest unit tests.
	$(PYTHON) -m pytest tests/ -v

teardown:              ## Stop containers and remove their volumes.
	docker compose down -v
