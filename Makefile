# open311-pipeline — convenience targets.
# All targets assume the project venv is active (or that the venv binaries
# are on PATH). On Windows, run from Git Bash.

SHELL := /usr/bin/env bash
PYTHON ?= python
DBT_PROJECT_DIR ?= dbt_project

.PHONY: help up topics run classify backfill dbt dbt-test docs docs-serve test lint teardown

help:                  ## Show this help.
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' Makefile | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

up:                    ## Start Kafka + Zookeeper in Docker.
	docker compose up -d

topics:                ## Create civic.requests.raw + civic.requests.dlq topics.
	bash scripts/create_kafka_topic.sh

run:                   ## Start poller + classifier consumer in parallel (logs/pipeline.log).
	bash scripts/run_pipeline.sh

classify:              ## Classify rows in Snowflake where urgency_label='Unknown' (use LIMIT=N for a sample).
	$(PYTHON) scripts/classify_existing.py $(if $(LIMIT),--limit $(LIMIT),)

backfill:              ## One-off historical backfill: --days N (defaults to 7).
	$(PYTHON) scripts/backfill_historical.py --days $(or $(DAYS),7)

dbt:                   ## Build all dbt models against Snowflake.
	dbt run --project-dir $(DBT_PROJECT_DIR)

dbt-test:              ## Run dbt tests (schema + singular).
	dbt test --project-dir $(DBT_PROJECT_DIR)

docs:                  ## Generate the dbt docs site (then run `make docs-serve` to view).
	dbt docs generate --project-dir $(DBT_PROJECT_DIR)

docs-serve:            ## Serve the generated docs at http://localhost:8000. Ctrl+C to stop. Override with PORT=N.
	$(PYTHON) -m http.server $(or $(PORT),8000) -d $(DBT_PROJECT_DIR)/target

test:                  ## Run pytest unit tests.
	$(PYTHON) -m pytest tests/ -v

teardown:              ## Stop containers and remove their volumes.
	docker compose down -v
