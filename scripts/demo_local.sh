#!/usr/bin/env bash
# Zero-credential local demo:
#   fixture -> Kafka -> consumer (replayed Claude labels) -> DuckDB -> dbt build -> SLA report
#
# Needs Docker + Python deps. No Anthropic, Langfuse or Snowflake account.
# Uses its own topic, consumer group and DuckDB file so it never mixes with a
# live pipeline's data.
#
#   bash scripts/demo_local.sh             # full path through Kafka
#   bash scripts/demo_local.sh --no-kafka  # skip Docker; load the fixture straight into DuckDB

set -euo pipefail
cd "$(dirname "$0")/.."

PYTHON="${PYTHON:-python}"
DBT="${DBT:-dbt}"
USE_KAFKA=1
[ "${1:-}" = "--no-kafka" ] && USE_KAFKA=0

export PYTHONPATH="$(pwd)"
export WAREHOUSE_BACKEND=duckdb
export CLASSIFIER_MODE=replay
export DUCKDB_PATH="${DEMO_DUCKDB_PATH:-data/demo.duckdb}"
export KAFKA_RAW_TOPIC=civic.requests.demo
export KAFKA_DLQ_TOPIC=civic.requests.dlq
export KAFKA_CONSUMER_GROUP=open311-demo
export DBT_TARGET=local
export DBT_PROFILES_DIR="$(pwd)/dbt_project"

step() { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }

rm -f "$DUCKDB_PATH" "$DUCKDB_PATH.wal"
[ -f dbt_project/profiles.yml ] || cp dbt_project/profiles.yml.example dbt_project/profiles.yml

if [ "$USE_KAFKA" = 1 ]; then
  step "Starting Kafka (docker compose)"
  docker compose up -d zookeeper kafka
  printf 'waiting for kafka to be healthy'
  for _ in $(seq 1 60); do
    [ "$(docker inspect -f '{{.State.Health.Status}}' kafka 2>/dev/null)" = healthy ] && break
    printf '.'; sleep 3
  done
  echo
  [ "$(docker inspect -f '{{.State.Health.Status}}' kafka)" = healthy ] \
    || { echo "kafka did not become healthy; see: docker compose logs kafka"; exit 1; }

  step "Creating topics"
  bash scripts/create_kafka_topic.sh
  docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists \
    --topic "$KAFKA_RAW_TOPIC" --partitions 3 --replication-factor 1 >/dev/null
  echo "  ✓ $KAFKA_RAW_TOPIC ready"

  step "Publishing fixture requests to $KAFKA_RAW_TOPIC (labels stripped)"
  "$PYTHON" scripts/load_fixture.py --via kafka 2>&1 | grep -v kafka_delivery_success

  step "Consuming: replayed classification -> MERGE into $DUCKDB_PATH"
  "$PYTHON" -m classifier.consumer --exit-when-idle 15
else
  step "Loading fixture straight into $DUCKDB_PATH (no Kafka)"
  "$PYTHON" scripts/load_fixture.py --via duckdb
fi

step "dbt build (models + data tests) on DuckDB"
"$DBT" build --project-dir dbt_project

step "SLA compliance mart"
"$PYTHON" scripts/sla_report.py
