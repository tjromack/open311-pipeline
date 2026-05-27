"""Tests for SnowflakeWriter — mock the connector, assert MERGE SQL + batching + DLQ path."""

from __future__ import annotations

import json
from datetime import datetime
from unittest.mock import MagicMock

import pytest

from ingestion.schemas import EnrichedRequest, ServiceRequest


def _enriched(suffix: str = "1", **overrides) -> EnrichedRequest:
    base = dict(
        service_request_id=f"R-{suffix}",
        requested_datetime=datetime(2026, 5, 20, 12, 0, 0),
        service_name="Pothole in Street",
        service_code="4fd3055e7f3b34730000000b",
        status="closed",
        address="1234 W Main St, Chicago, IL",
        lat=41.88,
        lon=-87.63,
        city="chicago",
        raw_payload={"k": "v"},
        urgency_label="Medium",
        urgency_score=0.8,
        llm_reasoning="Test reasoning.",
        langfuse_trace_id="t-abc",
        classified_at=datetime(2026, 5, 20, 12, 5, 0),
        days_to_close=2.5,
    )
    base.update(overrides)
    return EnrichedRequest(**base)


@pytest.fixture
def mock_snowflake(monkeypatch):
    """Patch the snowflake connect call and return (mock_conn, mock_cursor)."""
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "test-acct")
    monkeypatch.setenv("SNOWFLAKE_USER", "test-user")
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "test-pass")

    mock_cursor = MagicMock(name="cursor")
    mock_conn = MagicMock(name="connection")
    mock_conn.cursor.return_value = mock_cursor

    from warehouse import snowflake_writer as sw_mod

    monkeypatch.setattr(sw_mod, "snowflake_connect", MagicMock(return_value=mock_conn))
    return mock_conn, mock_cursor


def test_upsert_single_record_emits_merge_sql(mock_snowflake):
    mock_conn, mock_cursor = mock_snowflake
    from warehouse.snowflake_writer import SnowflakeWriter

    writer = SnowflakeWriter()
    n = writer.upsert_batch([_enriched()])

    assert n == 1
    mock_cursor.execute.assert_called_once()
    sql, params = mock_cursor.execute.call_args.args
    assert "MERGE INTO CIVIC_311.RAW.SERVICE_REQUESTS" in sql
    assert "ON tgt.service_request_id = src.service_request_id" in sql
    assert "WHEN MATCHED THEN UPDATE SET" in sql
    assert "WHEN NOT MATCHED THEN INSERT" in sql
    assert "PARSE_JSON(column10)" in sql
    # 16 columns × 1 row = 16 bind params
    assert len(params) == 16
    mock_conn.commit.assert_called_once()


def test_upsert_chunks_at_batch_size(mock_snowflake):
    _, mock_cursor = mock_snowflake
    from warehouse.snowflake_writer import SnowflakeWriter

    writer = SnowflakeWriter(batch_size=100)
    records = [_enriched(suffix=str(i)) for i in range(250)]
    n = writer.upsert_batch(records)

    assert n == 250
    # 250 → 100 + 100 + 50 → 3 MERGE statements
    assert mock_cursor.execute.call_count == 3
    expected_row_counts = [100, 100, 50]
    for call, expected_rows in zip(mock_cursor.execute.call_args_list, expected_row_counts):
        _, params = call.args
        assert len(params) == expected_rows * 16


def test_upsert_empty_is_noop(mock_snowflake):
    mock_conn, mock_cursor = mock_snowflake
    from warehouse.snowflake_writer import SnowflakeWriter

    writer = SnowflakeWriter()
    n = writer.upsert_batch([])

    assert n == 0
    mock_cursor.execute.assert_not_called()
    mock_conn.commit.assert_not_called()


def test_upsert_serializes_raw_payload_as_json(mock_snowflake):
    _, mock_cursor = mock_snowflake
    from warehouse.snowflake_writer import SnowflakeWriter

    rec = _enriched(raw_payload={"foo": "bar", "n": 42, "nested": {"a": 1}})
    writer = SnowflakeWriter()
    writer.upsert_batch([rec])

    _, params = mock_cursor.execute.call_args.args
    # raw_payload is column 10 (index 9) — should be a JSON string
    raw_json = params[9]
    assert isinstance(raw_json, str)
    assert json.loads(raw_json) == {"foo": "bar", "n": 42, "nested": {"a": 1}}


def test_replay_emits_identical_merge(mock_snowflake):
    """Re-running the same batch produces identical SQL+params — MERGE is naturally idempotent."""
    _, mock_cursor = mock_snowflake
    from warehouse.snowflake_writer import SnowflakeWriter

    records = [_enriched(suffix="A"), _enriched(suffix="B")]
    writer = SnowflakeWriter()

    writer.upsert_batch(records)
    first_sql, first_params = mock_cursor.execute.call_args.args

    writer.upsert_batch(records)
    second_sql, second_params = mock_cursor.execute.call_args.args

    assert first_sql == second_sql
    assert first_params == second_params


def test_close_disconnects(mock_snowflake):
    mock_conn, _ = mock_snowflake
    from warehouse.snowflake_writer import SnowflakeWriter

    writer = SnowflakeWriter()
    writer.connect()
    writer.close()
    mock_conn.close.assert_called_once()
    assert writer._conn is None


def test_consumer_routes_warehouse_failure_to_dlq(mock_snowflake):
    """If the warehouse upsert raises, the message's raw bytes go to DLQ."""
    from classifier.consumer import ClassifierConsumer

    writer = MagicMock(name="SnowflakeWriter")
    writer.upsert_batch.side_effect = RuntimeError("snowflake offline")

    classifier = MagicMock(name="UrgencyClassifier")
    classifier.classify.return_value = _enriched()

    dlq = MagicMock(name="dlq_producer")
    cc = ClassifierConsumer(
        classifier=classifier,
        consumer=MagicMock(),
        dlq_producer=dlq,
        flush_every_n=1000,
        snowflake_writer=writer,
    )

    sr = ServiceRequest(
        service_request_id="R-1",
        requested_datetime=datetime(2026, 5, 20, 12, 0, 0),
        service_name="Pothole",
        service_code="x",
        status="open",
        address="1 Main",
        lat=None,
        lon=None,
        city="chicago",
        raw_payload={},
    )

    msg = MagicMock()
    msg.value.return_value = sr.model_dump_json().encode("utf-8")
    msg.key.return_value = b"R-1"
    msg.error.return_value = None
    msg.topic.return_value = "civic.requests.raw"
    msg.partition.return_value = 0
    msg.offset.return_value = 1

    cc._handle_message(msg)

    writer.upsert_batch.assert_called_once()
    dlq.produce.assert_called_once()
    headers = dict(dlq.produce.call_args.kwargs["headers"])
    assert headers["exception_type"] == b"RuntimeError"
    assert b"snowflake offline" in headers["exception_message"]


def test_consumer_writes_to_warehouse_on_success(mock_snowflake):
    """Successful classification path passes the enriched record to the writer once."""
    from classifier.consumer import ClassifierConsumer

    enriched = _enriched()
    classifier = MagicMock(name="UrgencyClassifier")
    classifier.classify.return_value = enriched

    writer = MagicMock(name="SnowflakeWriter")
    dlq = MagicMock(name="dlq_producer")
    cc = ClassifierConsumer(
        classifier=classifier,
        consumer=MagicMock(),
        dlq_producer=dlq,
        flush_every_n=1000,
        snowflake_writer=writer,
    )

    sr = ServiceRequest(
        service_request_id="R-1",
        requested_datetime=datetime(2026, 5, 20, 12, 0, 0),
        service_name="Pothole",
        service_code="x",
        status="open",
        address="1 Main",
        lat=None,
        lon=None,
        city="chicago",
        raw_payload={},
    )

    msg = MagicMock()
    msg.value.return_value = sr.model_dump_json().encode("utf-8")
    msg.key.return_value = b"R-1"
    msg.error.return_value = None
    msg.topic.return_value = "civic.requests.raw"
    msg.partition.return_value = 0
    msg.offset.return_value = 1

    cc._handle_message(msg)

    writer.upsert_batch.assert_called_once_with([enriched])
    dlq.produce.assert_not_called()
