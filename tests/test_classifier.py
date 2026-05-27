"""Tests for the urgency classifier and the classifier consumer loop."""

from __future__ import annotations

import json
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from classifier import urgency_classifier as uc_module
from classifier.consumer import ClassifierConsumer
from classifier.urgency_classifier import UrgencyClassification, UrgencyClassifier
from ingestion.schemas import EnrichedRequest, ServiceRequest


def _make_request(**overrides) -> ServiceRequest:
    payload = {
        "service_request_id": "R-1",
        "requested_datetime": datetime(2026, 5, 20, 12, 0, 0),
        "service_name": "Pothole in Street",
        "service_code": "4fd3055e7f3b34730000000b",
        "status": "open",
        "address": "1234 W Main St, Chicago, IL",
        "lat": 41.88,
        "lon": -87.63,
        "raw_payload": {"x": 1},
    }
    payload.update(overrides)
    return ServiceRequest(**payload)


def _stub_classifier(monkeypatch, classification: UrgencyClassification) -> MagicMock:
    """Patch ChatAnthropic so no network / no API key is needed."""
    fake_structured = MagicMock()
    fake_structured.invoke.return_value = classification

    fake_llm = MagicMock()
    fake_llm.with_structured_output.return_value = fake_structured

    monkeypatch.setattr(uc_module, "ChatAnthropic", MagicMock(return_value=fake_llm))
    return fake_structured


def test_classifier_returns_enriched_request_with_label(monkeypatch):
    classification = UrgencyClassification(
        urgency_label="High",
        urgency_score=0.87,
        llm_reasoning="Large pothole on an arterial road poses an immediate hazard.",
    )
    stub = _stub_classifier(monkeypatch, classification)

    classifier = UrgencyClassifier(langfuse_handler=None)
    request = _make_request()
    enriched = classifier.classify(request)

    assert isinstance(enriched, EnrichedRequest)
    assert enriched.urgency_label == "High"
    assert enriched.urgency_score == pytest.approx(0.87)
    assert enriched.llm_reasoning.startswith("Large pothole")
    assert enriched.langfuse_trace_id
    assert enriched.service_request_id == request.service_request_id

    # Original ServiceRequest fields preserved
    assert enriched.address == request.address
    assert enriched.raw_payload == request.raw_payload

    stub.invoke.assert_called_once()
    _, kwargs = stub.invoke.call_args
    config = kwargs["config"]
    assert config["run_name"] == "urgency_classification"
    assert "chicago" in config["tags"]
    assert request.service_code in config["tags"]


def test_classifier_passes_langfuse_handler_as_callback(monkeypatch):
    classification = UrgencyClassification(
        urgency_label="Medium",
        urgency_score=0.7,
        llm_reasoning="Residential side-street pothole; quality-of-life impact.",
    )
    stub = _stub_classifier(monkeypatch, classification)

    handler = MagicMock(name="LangfuseCallbackHandler")
    classifier = UrgencyClassifier(langfuse_handler=handler)
    classifier.classify(_make_request())

    _, kwargs = stub.invoke.call_args
    callbacks = kwargs["config"].get("callbacks")
    assert callbacks == [handler]


def _kafka_message(value: bytes, key: bytes = b"R-1") -> MagicMock:
    msg = MagicMock()
    msg.value.return_value = value
    msg.key.return_value = key
    msg.error.return_value = None
    msg.topic.return_value = "civic.requests.raw"
    msg.partition.return_value = 0
    msg.offset.return_value = 42
    return msg


def _good_payload() -> bytes:
    return _make_request().model_dump_json().encode("utf-8")


def test_consumer_routes_classification_failure_to_dlq():
    failing_classifier = MagicMock()
    failing_classifier.classify.side_effect = RuntimeError("upstream API exploded")

    consumer = MagicMock()
    dlq = MagicMock()
    cc = ClassifierConsumer(
        classifier=failing_classifier,
        consumer=consumer,
        dlq_producer=dlq,
        flush_every_n=1000,
    )

    cc._handle_message(_kafka_message(_good_payload()))

    failing_classifier.classify.assert_called_once()
    dlq.produce.assert_called_once()
    _, kwargs = dlq.produce.call_args
    assert kwargs["topic"] == cc.dlq_topic
    headers = dict(kwargs["headers"])
    assert headers["exception_type"] == b"RuntimeError"
    assert b"upstream API exploded" in headers["exception_message"]


def test_consumer_routes_parse_failure_to_dlq():
    classifier = MagicMock()
    consumer = MagicMock()
    dlq = MagicMock()
    cc = ClassifierConsumer(
        classifier=classifier,
        consumer=consumer,
        dlq_producer=dlq,
        flush_every_n=1000,
    )

    cc._handle_message(_kafka_message(b"{not valid json"))

    classifier.classify.assert_not_called()
    dlq.produce.assert_called_once()
    headers = dict(dlq.produce.call_args.kwargs["headers"])
    # JSON parse failure
    assert headers["exception_type"] in (b"JSONDecodeError", b"ValueError")


def test_consumer_passes_successful_classification(monkeypatch):
    enriched = EnrichedRequest(
        **_make_request().model_dump(),
        urgency_label="Critical",
        urgency_score=0.95,
        llm_reasoning="Gas leak reported at a residential address.",
        langfuse_trace_id="trace-xyz",
        classified_at=datetime(2026, 5, 20, 12, 5, 0),
        days_to_close=None,
    )
    classifier = MagicMock()
    classifier.classify.return_value = enriched

    consumer = MagicMock()
    dlq = MagicMock()
    cc = ClassifierConsumer(
        classifier=classifier,
        consumer=consumer,
        dlq_producer=dlq,
        flush_every_n=1000,
    )

    cc._handle_message(_kafka_message(_good_payload()))

    classifier.classify.assert_called_once()
    dlq.produce.assert_not_called()


def test_dlq_rate_alert_fires_above_threshold():
    """DLQ_ALERT_THRESHOLD+1 DLQ sends within the window should log an alert."""
    cc = ClassifierConsumer(
        classifier=MagicMock(),
        consumer=MagicMock(),
        dlq_producer=MagicMock(),
        flush_every_n=1000,
    )

    with patch("classifier.consumer.log") as mock_log:
        for _ in range(cc.DLQ_ALERT_THRESHOLD + 1):
            cc._send_to_dlq(b"payload", b"K-1", RuntimeError("boom"))

    warning_events = [c.args[0] for c in mock_log.warning.call_args_list]
    assert "dlq_rate_alert" in warning_events


def test_dlq_rate_alert_silent_below_threshold():
    cc = ClassifierConsumer(
        classifier=MagicMock(),
        consumer=MagicMock(),
        dlq_producer=MagicMock(),
        flush_every_n=1000,
    )

    with patch("classifier.consumer.log") as mock_log:
        for _ in range(cc.DLQ_ALERT_THRESHOLD):
            cc._send_to_dlq(b"payload", b"K-1", RuntimeError("boom"))

    warning_events = [c.args[0] for c in mock_log.warning.call_args_list]
    assert "dlq_rate_alert" not in warning_events


def test_consumer_flushes_langfuse_at_threshold():
    classifier = MagicMock()
    classifier.classify.return_value = EnrichedRequest(
        **_make_request().model_dump(),
        urgency_label="Low",
        urgency_score=0.6,
        llm_reasoning="Cosmetic maintenance item.",
        langfuse_trace_id="trace-1",
        classified_at=datetime(2026, 5, 20, 12, 5, 0),
        days_to_close=None,
    )

    handler = MagicMock()
    cc = ClassifierConsumer(
        classifier=classifier,
        consumer=MagicMock(),
        dlq_producer=MagicMock(),
        flush_every_n=2,
    )
    cc.langfuse_handler = handler  # inject after construction

    cc._handle_message(_kafka_message(_good_payload()))
    assert handler.flush.call_count == 0

    cc._handle_message(_kafka_message(_good_payload()))
    assert handler.flush.call_count == 1
    # Counter resets after flush
    assert cc._messages_since_flush == 0
