"""Tests for DuckDBWriter against a real on-disk DuckDB file (no mocks)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import duckdb
import pytest

from ingestion.schemas import EnrichedRequest
from warehouse.duckdb_writer import DuckDBWriter


def _enriched(suffix: str = "1", **overrides) -> EnrichedRequest:
    base = dict(
        service_request_id=f"R-{suffix}",
        requested_datetime=datetime(2026, 5, 20, 12, 0, 0, tzinfo=timezone.utc),
        service_name="Pothole in Street",
        service_code="4fd3055e7f3b34730000000b",
        status="closed",
        address="1234 W Main St, Chicago, IL",
        lat=41.88,
        lon=-87.63,
        city="chicago",
        raw_payload={"updated_datetime": "2026-05-21T12:00:00Z", "group": "CDOT"},
        urgency_label="Medium",
        urgency_score=0.8,
        llm_reasoning="Test reasoning.",
        langfuse_trace_id="t-abc",
        classified_at=datetime(2026, 5, 20, 12, 5, 0, tzinfo=timezone.utc),
        days_to_close=None,
    )
    base.update(overrides)
    return EnrichedRequest(**base)


@pytest.fixture
def writer(tmp_path):
    return DuckDBWriter(path=str(tmp_path / "test.duckdb"), batch_size=2)


def _query(writer: DuckDBWriter, sql: str):
    with duckdb.connect(writer.path, read_only=True) as conn:
        return conn.execute(sql).fetchall()


def test_upsert_is_idempotent_and_updates_in_place(writer):
    writer.upsert_batch([_enriched("1", urgency_label="Unknown")])
    writer.upsert_batch([_enriched("1", urgency_label="High")])

    rows = _query(writer, "select service_request_id, urgency_label from raw.service_requests")
    assert rows == [("R-1", "High")]


def test_batches_split_by_batch_size(writer):
    n = writer.upsert_batch([_enriched(str(i)) for i in range(5)])  # 2 + 2 + 1
    assert n == 5
    assert _query(writer, "select count(*) from raw.service_requests") == [(5,)]


def test_aware_timestamps_stored_as_utc_wall_clock(writer):
    # 07:00 in Chicago (CDT, UTC-5) is 12:00 UTC — must land as 12:00, like Snowflake NTZ.
    cdt = timezone(timedelta(hours=-5))
    writer.upsert_batch([_enriched(requested_datetime=datetime(2026, 5, 20, 7, 0, tzinfo=cdt))])

    [(stored,)] = _query(writer, "select requested_datetime from raw.service_requests")
    assert stored == datetime(2026, 5, 20, 12, 0, 0)


def test_raw_payload_stored_as_queryable_json(writer):
    writer.upsert_batch([_enriched()])
    [(group,)] = _query(writer, "select raw_payload->>'group' from raw.service_requests")
    assert group == "CDOT"


def test_fetch_unclassified_seeded_order_is_deterministic(writer):
    writer.upsert_batch([_enriched(str(i), urgency_label="Unknown") for i in range(20)])
    writer.upsert_batch([_enriched("classified", urgency_label="Low")])

    first = [r.service_request_id for r in writer.fetch_unclassified(limit=5, sample_seed=311)]
    second = [r.service_request_id for r in writer.fetch_unclassified(limit=5, sample_seed=311)]
    assert first == second
    assert "R-classified" not in first
    assert len(first) == 5


def test_fetch_classified_round_trips_records(writer):
    original = _enriched("7", urgency_label="Critical")
    writer.upsert_batch([original, _enriched("8", urgency_label="Unknown")])

    [restored] = writer.fetch_classified()
    assert restored == original


def test_build_writer_defaults_to_duckdb(monkeypatch, tmp_path):
    from warehouse import build_writer

    monkeypatch.delenv("WAREHOUSE_BACKEND", raising=False)
    monkeypatch.setenv("DUCKDB_PATH", str(tmp_path / "x.duckdb"))
    assert isinstance(build_writer(), DuckDBWriter)


def test_build_writer_rejects_unknown_backend(monkeypatch):
    from warehouse import build_writer

    monkeypatch.setenv("WAREHOUSE_BACKEND", "bigquery")
    with pytest.raises(ValueError, match="WAREHOUSE_BACKEND"):
        build_writer()
