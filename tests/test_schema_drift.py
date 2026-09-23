"""What happens when Chicago changes the Open311 payload.

Each test is one realistic drift. The contract: the poller never crashes, never
guesses, and never drops data silently — every drift produces a structured log
event naming the keys involved, and records it cannot parse are logged and
skipped while the rest of the batch still flows.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import httpx
import pytest
from structlog.testing import capture_logs

from ingestion.open311_poller import Open311Poller


def _record(request_id: str = "SR26-1", **overrides) -> dict:
    """A record shaped exactly like the 2026-09 Chicago feed."""
    rec = {
        "service_request_id": request_id,
        "status": "closed",
        "service_name": "Pothole in Street Complaint",
        "service_code": "4fd3055e7f3b34730000000b",
        "requested_datetime": "2026-09-18T13:51:06Z",
        "updated_datetime": "2026-09-23T15:01:28Z",
        "address": "1908 N Campbell Ave",
        "lat": 41.916,
        "long": -87.689,
        "token": "6942d9f5426ae19b6f516ef3",
    }
    rec.update(overrides)
    return rec


def _poll(payload) -> tuple[list, list[dict]]:
    poller = Open311Poller(base_url="https://example.test/open311/v2", service_codes=["c"])
    response = MagicMock(spec=httpx.Response)
    response.status_code = 200
    response.json.return_value = payload
    response.raise_for_status.return_value = None
    with patch("ingestion.open311_poller.httpx.Client") as client_cls:
        client = MagicMock()
        client.__enter__.return_value = client
        client.get.return_value = response
        client_cls.return_value = client
        with capture_logs() as logs:
            results = poller.fetch_recent()
    return results, logs


def _events(logs: list[dict], name: str) -> list[dict]:
    return [e for e in logs if e["event"] == name]


def test_current_feed_shape_logs_no_drift():
    results, logs = _poll([_record("A"), _record("B", media_url="https://x/y.jpg")])
    assert len(results) == 2
    assert _events(logs, "open311_schema_drift") == []


def test_renamed_key_is_logged_loudly_not_guessed():
    # Chicago renames `long` -> `lng`. The record still parses, but lon cannot be
    # recovered — so it is NULL, and the drift is reported once for the batch.
    renamed = {k: v for k, v in _record().items() if k != "long"} | {"lng": -87.689}
    results, logs = _poll([renamed, dict(renamed, service_request_id="SR26-2")])

    assert [r.lon for r in results] == [None, None]
    [drift] = _events(logs, "open311_schema_drift")
    assert drift["log_level"] == "warning"
    assert drift["missing_keys"] == {"long": 2}
    assert drift["unexpected_keys"] == {"lng": 2}


def test_new_field_is_preserved_in_raw_payload_and_reported():
    results, logs = _poll([_record(priority="HIGH")])
    assert results[0].raw_payload["priority"] == "HIGH"
    [drift] = _events(logs, "open311_schema_drift")
    assert drift["unexpected_keys"] == {"priority": 1}


def test_missing_required_field_drops_that_record_only():
    no_timestamp = {k: v for k, v in _record("BAD").items() if k != "requested_datetime"}
    results, logs = _poll([no_timestamp, _record("GOOD")])

    assert [r.service_request_id for r in results] == ["GOOD"]
    [err] = _events(logs, "open311_record_parse_error")
    assert err["service_request_id"] == "BAD"
    assert _events(logs, "open311_schema_drift")[0]["missing_keys"] == {"requested_datetime": 1}


@pytest.mark.parametrize("bad_value", ["09/18/2026 01:51 PM", "", "yesterday"])
def test_reformatted_timestamp_drops_that_record_with_an_error(bad_value):
    results, logs = _poll([_record("BAD", requested_datetime=bad_value), _record("GOOD")])
    assert [r.service_request_id for r in results] == ["GOOD"]
    assert _events(logs, "open311_record_parse_error")[0]["service_request_id"] == "BAD"


def test_non_numeric_coordinate_drops_that_record_with_an_error():
    results, logs = _poll([_record("BAD", lat="north-ish"), _record("GOOD")])
    assert [r.service_request_id for r in results] == ["GOOD"]
    assert _events(logs, "open311_record_parse_error")


def test_non_object_record_is_skipped_not_fatal():
    # Previously `record.get(...)` on a string raised and lost the whole poll tick.
    results, logs = _poll(["not-a-record", _record("GOOD")])
    assert [r.service_request_id for r in results] == ["GOOD"]
    assert _events(logs, "open311_record_not_object")


def test_envelope_change_logs_and_returns_nothing():
    # The API wraps results in an object instead of returning a bare list.
    results, logs = _poll({"requests": [_record()]})
    assert results == []
    [err] = _events(logs, "open311_unexpected_payload")
    assert err["payload_type"] == "dict"
