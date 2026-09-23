"""Replay recorded classifications instead of calling the LLM.

Drop-in for UrgencyClassifier (same `.classify()` signature) so the full
Kafka -> consumer -> warehouse -> dbt path runs with no API key. Every label
it returns was produced by a real Claude Haiku 4.5 call, recorded in
data/fixtures/ by scripts/export_fixture.py. It never invents a label: a
request with no recorded classification raises, and the consumer routes it
to the DLQ like any other classification failure.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

import structlog

from ingestion.schemas import EnrichedRequest, ServiceRequest

log = structlog.get_logger(__name__)

DEFAULT_FIXTURE = "data/fixtures/chicago_311_classified.jsonl"


class ReplayMiss(LookupError):
    """No recorded classification exists for this service_request_id."""


class ReplayClassifier:
    def __init__(self, fixture_path: Optional[str] = None) -> None:
        self.fixture_path = Path(
            fixture_path or os.environ.get("REPLAY_FIXTURE") or DEFAULT_FIXTURE
        )
        self._by_id: dict[str, EnrichedRequest] = {}
        with self.fixture_path.open(encoding="utf-8") as fh:
            for line in fh:
                if line.strip():
                    rec = EnrichedRequest.model_validate_json(line)
                    self._by_id[rec.service_request_id] = rec
        log.info("replay_classifier_loaded", path=str(self.fixture_path), records=len(self._by_id))

    def classify(self, request: ServiceRequest) -> EnrichedRequest:
        recorded = self._by_id.get(request.service_request_id)
        if recorded is None:
            raise ReplayMiss(
                f"no recorded classification for {request.service_request_id} "
                f"in {self.fixture_path}"
            )
        return EnrichedRequest(
            **request.model_dump(),
            urgency_label=recorded.urgency_label,
            urgency_score=recorded.urgency_score,
            llm_reasoning=recorded.llm_reasoning,
            langfuse_trace_id=recorded.langfuse_trace_id,
            classified_at=recorded.classified_at,
            days_to_close=recorded.days_to_close,
        )


def load_fixture(fixture_path: Optional[str] = None) -> list[EnrichedRequest]:
    """Read every recorded EnrichedRequest from the fixture file."""
    path = Path(fixture_path or os.environ.get("REPLAY_FIXTURE") or DEFAULT_FIXTURE)
    with path.open(encoding="utf-8") as fh:
        return [EnrichedRequest.model_validate_json(line) for line in fh if line.strip()]
