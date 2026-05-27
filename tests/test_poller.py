"""Tests for ingestion.open311_poller — mocked HTTP, dedup, error paths."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import httpx
import pytest

from ingestion.open311_poller import Open311Poller


def _record(request_id: str, requested_datetime: str = "2026-05-20T08:15:30-05:00") -> dict:
    return {
        "service_request_id": request_id,
        "status": "open",
        "service_name": "Pothole in Street",
        "service_code": "4fd3055e7f3b34730000000b",
        "requested_datetime": requested_datetime,
        "address": "1234 W Main St, Chicago, IL",
        "lat": 41.88,
        "long": -87.62,
    }


def _mock_response(json_data, status_code: int = 200) -> MagicMock:
    response = MagicMock(spec=httpx.Response)
    response.status_code = status_code
    response.json.return_value = json_data
    if status_code >= 400:
        response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "error", request=MagicMock(), response=response
        )
    else:
        response.raise_for_status.return_value = None
    return response


@pytest.fixture
def poller():
    # Single service code; no API key
    return Open311Poller(
        base_url="https://example.test/open311/v2",
        api_key=None,
        service_codes=["code-a"],
    )


def test_valid_response_returns_service_requests(poller):
    payload = [_record("R-1"), _record("R-2"), _record("R-3")]
    with patch("ingestion.open311_poller.httpx.Client") as mock_client_cls:
        mock_client = MagicMock()
        mock_client.__enter__.return_value = mock_client
        mock_client.__exit__.return_value = False
        mock_client.get.return_value = _mock_response(payload)
        mock_client_cls.return_value = mock_client

        results = poller.fetch_recent()

    assert len(results) == 3
    assert {r.service_request_id for r in results} == {"R-1", "R-2", "R-3"}


def test_duplicates_deduplicated_across_polls(poller):
    payload = [_record("R-1"), _record("R-2")]
    with patch("ingestion.open311_poller.httpx.Client") as mock_client_cls:
        mock_client = MagicMock()
        mock_client.__enter__.return_value = mock_client
        mock_client.__exit__.return_value = False
        mock_client.get.return_value = _mock_response(payload)
        mock_client_cls.return_value = mock_client

        first = poller.fetch_recent()
        second = poller.fetch_recent()

    assert len(first) == 2
    assert second == []


def test_http_429_logs_and_returns_empty(poller):
    with patch("ingestion.open311_poller.httpx.Client") as mock_client_cls, \
         patch("ingestion.open311_poller.log") as mock_log:
        mock_client = MagicMock()
        mock_client.__enter__.return_value = mock_client
        mock_client.__exit__.return_value = False
        mock_client.get.return_value = _mock_response([], status_code=429)
        mock_client_cls.return_value = mock_client

        results = poller.fetch_recent()

    assert results == []
    mock_log.error.assert_called()
    error_event = mock_log.error.call_args_list[0].args[0]
    assert error_event == "open311_http_error"


def test_malformed_record_skipped_others_kept(poller):
    payload = [
        _record("R-good-1"),
        _record("R-bad", requested_datetime="not-a-date"),
        _record("R-good-2"),
    ]
    with patch("ingestion.open311_poller.httpx.Client") as mock_client_cls, \
         patch("ingestion.open311_poller.log") as mock_log:
        mock_client = MagicMock()
        mock_client.__enter__.return_value = mock_client
        mock_client.__exit__.return_value = False
        mock_client.get.return_value = _mock_response(payload)
        mock_client_cls.return_value = mock_client

        results = poller.fetch_recent()

    ids = {r.service_request_id for r in results}
    assert ids == {"R-good-1", "R-good-2"}
    # The bad record should have triggered an error log
    error_events = [c.args[0] for c in mock_log.error.call_args_list]
    assert "open311_record_parse_error" in error_events
