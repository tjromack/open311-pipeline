"""Pydantic models for Chicago Open311 ingestion and downstream enrichment."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional, Union

from pydantic import BaseModel, ConfigDict, field_validator

# Keys Chicago's Open311 feed sent on every record in a 2026-09-23 survey of
# 7,493 closed requests (7 days, all 125 service codes). A record missing any
# of these, or carrying a key outside this set and the spec's optional fields,
# is schema drift: the poller logs it rather than guessing.
OPEN311_EXPECTED_KEYS = frozenset({
    "service_request_id", "status", "service_name", "service_code",
    "requested_datetime", "updated_datetime", "address", "lat", "long", "token",
})
# Optional in the Open311 GeoReport v2 spec, so their appearance is not drift
# (media_url appeared on 1 of the 7,493 surveyed records).
OPEN311_OPTIONAL_KEYS = frozenset({
    "description", "status_notes", "agency_responsible", "service_notice",
    "expected_datetime", "zipcode", "address_id", "media_url",
})


def schema_drift(record: dict[str, Any]) -> tuple[set[str], set[str]]:
    """Return (missing_expected_keys, unexpected_keys) for one Open311 record."""
    keys = set(record)
    return set(OPEN311_EXPECTED_KEYS - keys), keys - OPEN311_EXPECTED_KEYS - OPEN311_OPTIONAL_KEYS


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
