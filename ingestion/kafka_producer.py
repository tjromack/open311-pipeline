"""Thin wrapper around confluent-kafka Producer for publishing ServiceRequests."""

from __future__ import annotations

import os
from typing import Optional

import structlog
from confluent_kafka import Producer
from dotenv import load_dotenv

from ingestion.schemas import ServiceRequest

load_dotenv()

log = structlog.get_logger(__name__)


def _delivery_callback(err, msg) -> None:
    if err is not None:
        log.error(
            "kafka_delivery_failed",
            error=str(err),
            topic=msg.topic() if msg else None,
            key=msg.key().decode("utf-8") if msg and msg.key() else None,
        )
    else:
        log.info(
            "kafka_delivery_success",
            topic=msg.topic(),
            partition=msg.partition(),
            offset=msg.offset(),
            key=msg.key().decode("utf-8") if msg.key() else None,
        )


class CivicRequestProducer:
    """Publishes ServiceRequest events to a Kafka topic, keyed by service_request_id."""

    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        topic: Optional[str] = None,
    ) -> None:
        self.bootstrap_servers = bootstrap_servers or os.environ.get(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        )
        self.topic = topic or os.environ.get("KAFKA_RAW_TOPIC", "civic.requests.raw")
        self._producer = Producer({"bootstrap.servers": self.bootstrap_servers})

    def publish(self, request: ServiceRequest) -> None:
        payload = request.model_dump_json().encode("utf-8")
        key = request.service_request_id.encode("utf-8")
        self._producer.produce(
            topic=self.topic,
            key=key,
            value=payload,
            on_delivery=_delivery_callback,
        )
        self._producer.poll(0)

    def flush(self, timeout: float = 10.0) -> int:
        return self._producer.flush(timeout)
