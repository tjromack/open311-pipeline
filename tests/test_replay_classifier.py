"""Tests for ReplayClassifier — recorded labels replay; unknown IDs go to the DLQ."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from classifier.consumer import ClassifierConsumer
from classifier.replay_classifier import ReplayClassifier, ReplayMiss
from ingestion.schemas import EnrichedRequest, ServiceRequest


def _request(request_id: str = "R-1") -> ServiceRequest:
    return ServiceRequest(
        service_request_id=request_id,
        requested_datetime=datetime(2026, 9, 20, 12, 0, tzinfo=timezone.utc),
        service_name="Tree Emergency",
        service_code="code-tree",
        status="closed",
        address="100 N State St",
        raw_payload={"service_request_id": request_id},
    )


@pytest.fixture
def fixture_path(tmp_path):
    recorded = EnrichedRequest(
        **_request("R-1").model_dump(),
        urgency_label="High",
        urgency_score=0.9,
        llm_reasoning="Fallen limb may block the roadway.",
        langfuse_trace_id="abc123",
        classified_at=datetime(2026, 9, 23, 15, 0, tzinfo=timezone.utc),
    )
    path = tmp_path / "fixture.jsonl"
    path.write_text(recorded.model_dump_json() + "\n", encoding="utf-8")
    return str(path)


def test_replay_returns_recorded_label(fixture_path):
    enriched = ReplayClassifier(fixture_path).classify(_request("R-1"))
    assert enriched.urgency_label == "High"
    assert enriched.langfuse_trace_id == "abc123"
    assert enriched.service_request_id == "R-1"


def test_replay_miss_raises_rather_than_inventing_a_label(fixture_path):
    with pytest.raises(ReplayMiss):
        ReplayClassifier(fixture_path).classify(_request("R-not-recorded"))


def test_replay_miss_routes_to_dlq(fixture_path):
    dlq = MagicMock()
    cc = ClassifierConsumer(
        classifier=ReplayClassifier(fixture_path),
        consumer=MagicMock(),
        dlq_producer=dlq,
        flush_every_n=1000,
    )
    msg = MagicMock()
    msg.value.return_value = _request("R-not-recorded").model_dump_json().encode("utf-8")
    msg.key.return_value = b"R-not-recorded"
    msg.error.return_value = None

    cc._handle_message(msg)

    dlq.produce.assert_called_once()
    headers = dict(dlq.produce.call_args.kwargs["headers"])
    assert headers["exception_type"] == b"ReplayMiss"
