"""Feed the recorded fixture into the pipeline — the zero-credential demo path.

    --via kafka   Publish each request (without its label) to civic.requests.raw.
                  Run the consumer with CLASSIFIER_MODE=replay to classify and
                  write them. This is the full Kafka -> classify -> DuckDB path.
    --via duckdb  Skip Kafka and MERGE the recorded rows straight into DuckDB.
                  For CI and machines without Docker; the dbt layer is identical.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Let `python scripts/<name>.py` import the project packages from a fresh clone.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import structlog
from dotenv import load_dotenv

from classifier.replay_classifier import DEFAULT_FIXTURE, load_fixture
from ingestion.schemas import ServiceRequest

load_dotenv()

log = structlog.get_logger(__name__)


def via_kafka(fixture: str) -> int:
    from ingestion.kafka_producer import CivicRequestProducer

    records = load_fixture(fixture)
    producer = CivicRequestProducer()
    for rec in records:
        # Publish only the ServiceRequest fields: the label must come from the
        # consumer's classifier, exactly as it would for a live poll.
        producer.publish(ServiceRequest(**rec.model_dump(include=set(ServiceRequest.model_fields))))
    undelivered = producer.flush(30.0)
    if undelivered:
        raise SystemExit(f"{undelivered} messages were not delivered to Kafka")
    log.info("fixture_published", topic=producer.topic, records=len(records))
    return len(records)


def via_duckdb(fixture: str) -> int:
    from warehouse.duckdb_writer import DuckDBWriter

    records = load_fixture(fixture)
    DuckDBWriter().upsert_batch(records)
    log.info("fixture_loaded", records=len(records))
    return len(records)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--via", choices=("kafka", "duckdb"), default="kafka")
    parser.add_argument("--fixture", default=DEFAULT_FIXTURE)
    args = parser.parse_args()
    (via_kafka if args.via == "kafka" else via_duckdb)(args.fixture)


if __name__ == "__main__":
    main()
