"""Pydantic models for Chicago Open311 ingestion and downstream enrichment."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional, Union

from pydantic import BaseModel, ConfigDict, field_validator


class ServiceRequest(BaseModel):
    """A single Chicago Open311 service request, as published to Kafka."""

    model_config = ConfigDict(populate_by_name=True)

    service_request_id: str
    requested_datetime: datetime
    service_name: str
    service_code: str
    status: str
    address: str
    lat: Optional[float] = None
    lon: Optional[float] = None
    city: str = "chicago"
    raw_payload: dict

    @field_validator("requested_datetime", mode="before")
    @classmethod
    def _parse_requested_datetime(cls, value: Union[str, datetime]) -> datetime:
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            return datetime.fromisoformat(value)
        raise TypeError(f"requested_datetime must be str or datetime, got {type(value).__name__}")

    @classmethod
    def from_api_response(cls, data: dict[str, Any]) -> "ServiceRequest":
        """Build a ServiceRequest from a single Chicago Open311 API response record.

        Open311 uses `long` for longitude; we map it to `lon`. Coordinates may
        arrive as strings; cast them to float when present.
        """
        lat_raw = data.get("lat")
        lon_raw = data.get("long", data.get("lon"))
        return cls(
            service_request_id=str(data["service_request_id"]),
            requested_datetime=data["requested_datetime"],
            service_name=data.get("service_name", ""),
            service_code=str(data.get("service_code", "")),
            status=data.get("status", "open"),
            address=data.get("address", ""),
            lat=float(lat_raw) if lat_raw not in (None, "") else None,
            lon=float(lon_raw) if lon_raw not in (None, "") else None,
            raw_payload=data,
        )


class EnrichedRequest(ServiceRequest):
    """A ServiceRequest enriched with LLM urgency classification (Phase 2)."""

    urgency_label: str
    urgency_score: float
    llm_reasoning: str
    langfuse_trace_id: str
    classified_at: datetime
    days_to_close: Optional[float] = None
