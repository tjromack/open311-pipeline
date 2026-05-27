"""LangChain + Claude Haiku 4.5 urgency classifier with Langfuse tracing."""

from __future__ import annotations

import os
import uuid
from datetime import datetime, timezone
from typing import Literal, Optional

import structlog
from dotenv import load_dotenv
from langchain_anthropic import ChatAnthropic
from langchain_core.messages import HumanMessage, SystemMessage
from pydantic import BaseModel, Field

from classifier.prompts import PROMPT_VERSION, SYSTEM_PROMPT, build_user_prompt
from ingestion.schemas import EnrichedRequest, ServiceRequest

load_dotenv()

log = structlog.get_logger(__name__)

UrgencyLabel = Literal["Critical", "High", "Medium", "Low"]


class UrgencyClassification(BaseModel):
    """Structured output the LLM is constrained to return."""

    urgency_label: UrgencyLabel = Field(
        ...,
        description="Exactly one of: Critical, High, Medium, Low.",
    )
    urgency_score: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Confidence in the chosen tier, 0.0-1.0.",
    )
    llm_reasoning: str = Field(
        ...,
        description="One-sentence explanation naming the specific risk or impact.",
    )


class UrgencyClassifier:
    """Classifies ServiceRequests into urgency tiers via Claude Haiku 4.5."""

    def __init__(
        self,
        model: Optional[str] = None,
        langfuse_handler=None,
    ) -> None:
        self.model_name = model or os.environ.get(
            "ANTHROPIC_MODEL", "claude-haiku-4-5-20251001"
        )
        self.langfuse_handler = langfuse_handler

        llm = ChatAnthropic(
            model=self.model_name,
            temperature=0,
            max_tokens=512,
        )
        self._chain = llm.with_structured_output(UrgencyClassification)

    def classify(self, request: ServiceRequest) -> EnrichedRequest:
        """Run the classifier on one request and return an EnrichedRequest."""
        trace_id = uuid.uuid4().hex
        user_prompt = build_user_prompt(
            service_name=request.service_name,
            service_code=request.service_code,
            address=request.address,
            status=request.status,
        )
        messages = [
            SystemMessage(content=SYSTEM_PROMPT),
            HumanMessage(content=user_prompt),
        ]

        config = {
            "run_id": uuid.UUID(trace_id),
            "metadata": {
                "service_request_id": request.service_request_id,
                "service_code": request.service_code,
                "city": request.city,
                "prompt_version": PROMPT_VERSION,
            },
            "tags": [request.city, request.service_code, "urgency_classification"],
            "run_name": "urgency_classification",
        }
        if self.langfuse_handler is not None:
            config["callbacks"] = [self.langfuse_handler]

        result: UrgencyClassification = self._chain.invoke(messages, config=config)

        days_to_close: Optional[float] = None
        # Phase 3 will compute days_to_close from updated_datetime when closed.

        return EnrichedRequest(
            **request.model_dump(),
            urgency_label=result.urgency_label,
            urgency_score=result.urgency_score,
            llm_reasoning=result.llm_reasoning,
            langfuse_trace_id=trace_id,
            classified_at=datetime.now(timezone.utc),
            days_to_close=days_to_close,
        )
