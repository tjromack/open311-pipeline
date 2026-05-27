#!/usr/bin/env bash
# Create Kafka topics for the civic-request pipeline.
# Idempotent: re-runs print "already exists" rather than failing.

set -u

CONTAINER="${KAFKA_CONTAINER:-kafka}"
BOOTSTRAP="${KAFKA_BOOTSTRAP_INTERNAL:-localhost:9092}"

create_topic() {
  local name="$1"
  local partitions="$2"
  local retention_ms="$3"

  echo "→ Creating topic: ${name} (partitions=${partitions}, retention_ms=${retention_ms})"
  output=$(docker exec "${CONTAINER}" kafka-topics \
    --bootstrap-server "${BOOTSTRAP}" \
    --create \
    --topic "${name}" \
    --partitions "${partitions}" \
    --replication-factor 1 \
    --config "retention.ms=${retention_ms}" 2>&1)
  status=$?

  if [ ${status} -eq 0 ]; then
    echo "  ✓ created ${name}"
  elif echo "${output}" | grep -qi "already exists"; then
    echo "  • ${name} already exists — skipping"
  else
    echo "  ✗ failed to create ${name}:"
    echo "${output}"
    exit ${status}
  fi
}

# civic.requests.raw — 3 partitions, 7-day retention
create_topic "civic.requests.raw" 3 604800000

# civic.requests.dlq — 1 partition, 30-day retention
create_topic "civic.requests.dlq" 1 2592000000

echo "Done."
