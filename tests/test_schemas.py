"""Tests for ingestion.schemas — ServiceRequest parsing from Open311 API shape."""

from __future__ import annotations

from datetime import datetime

import pytest

from ingestion.schemas import ServiceRequest


def _api_record(**overrides) -> dict:
    base = {
        "service_request_id": "12-00345678",
        "status": "open",
        "service_name": "Pothole in Street",
        "service_code": "4fd3055e7f3b34730000000b",
        "requested_datetime": "2026-05-20T08:15:30-05:00",
        "updated_datetime": "2026-05-21T09:00:00-05:00",
        "address": "1234 W Main St, Chicago, IL",
        "lat": 41.881832,
        "long": -87.623177,
        "media_url": "https://311.chicago.gov/media/abc.jpg",
    }
    base.update(overrides)
    return base


def test_from_api_response_full_record():
    record = _api_record()
    req = ServiceRequest.from_api_response(record)

    assert req.service_request_id == "12-00345678"
    assert req.service_name == "Pothole in Street"
    assert req.service_code == "4fd3055e7f3b34730000000b"
    assert req.status == "open"
    assert req.address == "1234 W Main St, Chicago, IL"
    assert req.lat == pytest.approx(41.881832)
    assert req.lon == pytest.approx(-87.623177)
    assert req.city == "chicago"
    assert isinstance(req.requested_datetime, datetime)
    assert req.requested_datetime.year == 2026
    assert req.raw_payload == record


def test_from_api_response_missing_optional_fields():
    record = _api_record()
    record.pop("lat")
    record.pop("long")
    req = ServiceRequest.from_api_response(record)
    assert req.lat is None
    assert req.lon is None


def test_from_api_response_accepts_datetime_instance():
    record = _api_record(requested_datetime=datetime(2026, 5, 20, 8, 15, 30))
    req = ServiceRequest.from_api_response(record)
    assert req.requested_datetime == datetime(2026, 5, 20, 8, 15, 30)
