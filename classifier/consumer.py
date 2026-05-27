"""Kafka consumer loop: classify ServiceRequests, route failures to the DLQ."""

from __future__ import annotations

import json
import os
import signal
import time
from collections import deque
from typing import Optional

import structlog
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from dotenv import load_dotenv

from classifier.urgency_classifier import UrgencyClassifier
from ingestion.schemas import EnrichedRequest, ServiceRequest
from warehouse.snowflake_writer import SnowflakeWriter

load_dotenv()

log = structlog.get_logger(__name__)


def _build_langfuse_handler():
    """Create a Langfuse CallbackHandler if creds are configured; else None."""
    public_key = os.environ.get("LANGFUSE_PUBLIC_KEY")
    secret_key = os.environ.get("LANGFUSE_SECRET_KEY")
    if not (public_key and secret_key):
        log.warning("langfuse_not_configured", reason="missing public/secret key")
        return None
    try:
        from langfuse.callback import CallbackHandler  # type: ignore[import-not-found]
    except ImportError:
        log.warning("langfuse_import_failed")
        return None
    return CallbackHandler(
        public_key=public_key,
        secret_key=secret_key,
        host=os.environ.get("LANGFUSE_HOST", "https://cloud.langfuse.com"),
    )


def _flush_langfuse(handler) -> None:
    if handler is None:
        return
    for attr in ("flush", "shutdown"):
        fn = getattr(handler, attr, None)
        if callable(fn):
            try:
                fn()
                return
            except Exception as exc:  # best-effort flush; never crash the consumer
                log.warning("langfuse_flush_failed", method=attr, error=str(exc))


class ClassifierConsumer:
    """Consumes raw service requests, classifies them, and produces to a sink."""

    # Observability defaults
    DLQ_ALERT_WINDOW_SECONDS = 5 * 60
    DLQ_ALERT_THRESHOLD = 5
    LAG_LOG_EVERY_N_MESSAGES = 50

    def __init__(
        self,
        classifier: Optional[UrgencyClassifier] = None,
        consumer: Optional[Consumer] = None,
        dlq_producer: Optional[Producer] = None,
        flush_every_n: Optional[int] = None,
        snowflake_writer: Optional[SnowflakeWriter] = None,
        lag_log_every_n: Optional[int] = None,
    ) -> None:
        self.raw_topic = os.environ.get("KAFKA_RAW_TOPIC", "civic.requests.raw")
        self.dlq_topic = os.environ.get("KAFKA_DLQ_TOPIC", "civic.requests.dlq")
        bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        group_id = os.environ.get("KAFKA_CONSUMER_GROUP", "urgency-classifier-group")

        self.langfuse_handler = _build_langfuse_handler()
        self.classifier = classifier or UrgencyClassifier(
            langfuse_handler=self.langfuse_handler
        )

        self.consumer = consumer or Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        self.dlq_producer = dlq_producer or Producer({"bootstrap.servers": bootstrap})

        self.flush_every_n = int(
            flush_every_n
            if flush_every_n is not None
            else os.environ.get("LANGFUSE_FLUSH_EVERY_N", "50")
        )
        self._messages_since_flush = 0
        self._running = False
        self.snowflake_writer = snowflake_writer

        # Observability state
        self.lag_log_every_n = int(lag_log_every_n or self.LAG_LOG_EVERY_N_MESSAGES)
        self._messages_processed = 0
        self._dlq_timestamps: deque[float] = deque()

    def _send_to_dlq(self, raw_value: bytes, key: Optional[bytes], exc: Exception) -> None:
        headers = [
            ("exception_type", type(exc).__name__.encode("utf-8")),
            ("exception_message", str(exc).encode("utf-8", errors="replace")),
        ]
        try:
            self.dlq_producer.produce(
                topic=self.dlq_topic,
                key=key,
                value=raw_value,
                headers=headers,
            )
            self.dlq_producer.poll(0)
            self._record_dlq_event()
        except Exception as inner:
            log.error("dlq_produce_failed", error=str(inner), original_error=str(exc))

    def _record_dlq_event(self) -> None:
        """Track DLQ sends in a sliding window; warn if rate exceeds threshold."""
        now = time.monotonic()
        cutoff = now - self.DLQ_ALERT_WINDOW_SECONDS
        while self._dlq_timestamps and self._dlq_timestamps[0] < cutoff:
            self._dlq_timestamps.popleft()
        self._dlq_timestamps.append(now)
        if len(self._dlq_timestamps) > self.DLQ_ALERT_THRESHOLD:
            log.warning(
                "dlq_rate_alert",
                dlq_messages_in_window=len(self._dlq_timestamps),
                window_seconds=self.DLQ_ALERT_WINDOW_SECONDS,
                threshold=self.DLQ_ALERT_THRESHOLD,
            )

    def _log_consumer_lag(self) -> None:
        """Log per-partition lag = (high watermark) - (current position)."""
        try:
            assignment = self.consumer.assignment()
        except Exception as exc:
            log.debug("consumer_lag_unavailable", error=str(exc))
            return
        if not assignment:
            return
        try:
            positions = self.consumer.position(assignment)
        except Exception as exc:
            log.debug("consumer_position_unavailable", error=str(exc))
            return

        partitions: list[dict] = []
        total_lag = 0
        for tp, pos_tp in zip(assignment, positions):
            try:
                _, high = self.consumer.get_watermark_offsets(tp, timeout=1.0, cached=True)
            except Exception:
                continue
            pos = pos_tp.offset if pos_tp.offset >= 0 else 0
            lag = max(0, high - pos)
            total_lag += lag
            partitions.append(
                {"topic": tp.topic, "partition": tp.partition, "position": pos, "high": high, "lag": lag}
            )
        if partitions:
            log.info(
                "consumer_lag",
                total_lag=total_lag,
                partitions=partitions,
                messages_processed=self._messages_processed,
            )

    def _handle_message(self, msg) -> None:
        raw_value = msg.value()
        key = msg.key()
        trace_log = log.bind(
            topic=msg.topic(),
            partition=msg.partition(),
            offset=msg.offset(),
            key=key.decode("utf-8") if key else None,
        )

        try:
            payload = json.loads(raw_value.decode("utf-8"))
            request = ServiceRequest.model_validate(payload)
        except Exception as exc:
            trace_log.error("message_parse_error", error=str(exc))
            self._send_to_dlq(raw_value, key, exc)
            return

        try:
            enriched: EnrichedRequest = self.classifier.classify(request)
        except Exception as exc:
            trace_log.error(
                "classification_error",
                service_request_id=request.service_request_id,
                error=str(exc),
            )
            self._send_to_dlq(raw_value, key, exc)
            return

        trace_log.info(
            "classified",
            service_request_id=enriched.service_request_id,
            urgency_label=enriched.urgency_label,
            urgency_score=enriched.urgency_score,
            trace_id=enriched.langfuse_trace_id,
        )

        if self.snowflake_writer is not None:
            try:
                self.snowflake_writer.upsert_batch([enriched])
            except Exception as exc:
                trace_log.error(
                    "warehouse_write_error",
                    service_request_id=enriched.service_request_id,
                    error=str(exc),
                )
                self._send_to_dlq(raw_value, key, exc)
                return

        self._messages_since_flush += 1
        self._messages_processed += 1
        if self._messages_since_flush >= self.flush_every_n:
            _flush_langfuse(self.langfuse_handler)
            self._messages_since_flush = 0
        if self.lag_log_every_n > 0 and self._messages_processed % self.lag_log_every_n == 0:
            self._log_consumer_lag()

    def run(self) -> None:
        """Subscribe and loop until SIGINT/SIGTERM."""
        self.consumer.subscribe([self.raw_topic])
        self._install_signal_handlers()
        self._running = True
        log.info("consumer_started", topic=self.raw_topic, dlq=self.dlq_topic)

        try:
            while self._running:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(msg.error())
                self._handle_message(msg)
                self.consumer.commit(msg, asynchronous=False)
        finally:
            self.shutdown()

    def _install_signal_handlers(self) -> None:
        def _stop(_signum, _frame):
            log.info("consumer_stopping")
            self._running = False

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                signal.signal(sig, _stop)
            except (ValueError, OSError):
                # Signal handlers can't be installed off the main thread (e.g. tests)
                pass

    def shutdown(self) -> None:
        log.info("consumer_shutdown_starting")
        try:
            self.dlq_producer.flush(10.0)
        except Exception as exc:
            log.warning("dlq_flush_failed", error=str(exc))
        _flush_langfuse(self.langfuse_handler)
        if self.snowflake_writer is not None:
            try:
                self.snowflake_writer.close()
            except Exception as exc:
                log.warning("snowflake_close_failed", error=str(exc))
        try:
            self.consumer.close()
        except Exception as exc:
            log.warning("consumer_close_failed", error=str(exc))
        log.info("consumer_shutdown_complete")


def _build_writer_from_env() -> Optional[SnowflakeWriter]:
    """Construct a SnowflakeWriter if Snowflake creds are configured; else None."""
    required = ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD")
    if not all(os.environ.get(k) for k in required):
        log.warning("snowflake_not_configured", reason="missing required env vars")
        return None
    return SnowflakeWriter()


if __name__ == "__main__":
    ClassifierConsumer(snowflake_writer=_build_writer_from_env()).run()
