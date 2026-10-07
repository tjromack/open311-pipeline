"""Tests for ingestion.portal_loader — a fake portal, a real DuckDB file."""

from __future__ import annotations

import csv
import io
from datetime import date

import duckdb
import httpx
import pytest

from ingestion import portal_loader as pl


def _csv(rows: list[dict]) -> str:
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=pl.PORTAL_COLUMNS, quoting=csv.QUOTE_NONNUMERIC)
    w.writeheader()
    for r in rows:
        w.writerow({c: r.get(c, "") for c in pl.PORTAL_COLUMNS})
    return buf.getvalue()


def _row(n: int, day: str, **kw) -> dict:
    base = {"sr_number": f"SR26-{n:08d}", "sr_type": "Pothole in Street Complaint", "status": "Open",
            "created_date": f"{day}T10:00:00.000", "last_modified_date": f"{day}T11:00:00.000",
            "duplicate": "false", "legacy_record": "false", "ward": "27"}
    base.update(kw)
    return base


class FakePortal:
    """Serves a fixed set of rows per day; can misreport counts or fail transiently."""

    def __init__(self, by_day: dict[str, list[dict]], count_skew: dict[str, int] | None = None):
        self.by_day = by_day
        self.count_skew = count_skew or {}
        self.fetches = 0

    def _day(self, where: str) -> str:
        return where.split("'")[1][:10]

    def count(self, where: str) -> int:
        d = self._day(where)
        return len(self.by_day.get(d, [])) + self.count_skew.get(d, 0)

    def fetch_csv(self, where: str, offset: int = 0, limit: int = pl.PAGE_SIZE) -> str:
        self.fetches += 1
        rows = self.by_day.get(self._day(where), [])
        return _csv(rows[offset:offset + limit])

    def fetch_by_ids(self, ids):
        return [{"sr_number": i, "sr_type": "Pothole in Street Complaint",
                 "created_date": "2025-09-20T08:00:00.000"} for i in ids if i.endswith("1")]


@pytest.fixture
def db(tmp_path):
    return str(tmp_path / "portal.duckdb")


def test_fiscal_year_bounds_are_federal():
    assert pl.fiscal_year_bounds(2026) == (date(2025, 10, 1), date(2026, 9, 30))


def test_every_partition_verified_when_counts_agree(db):
    fake = FakePortal({"2026-03-10": [_row(1, "2026-03-10"), _row(2, "2026-03-10")],
                       "2026-03-11": [_row(3, "2026-03-11")]})
    loader = pl.PortalLoader(client=fake, db_path=db)
    res = loader.pull("t", date(2026, 3, 10), date(2026, 3, 12))  # 03-12 is an empty day
    loader.close()

    assert (res.partitions, res.expected, res.landed) == (3, 3, 3)
    with duckdb.connect(db, read_only=True) as c:
        statuses = c.execute("SELECT status, count(*) FROM raw.portal_pulls GROUP BY 1").fetchall()
        types = c.execute("SELECT typeof(created_date), typeof(duplicate), typeof(ward) "
                          "FROM raw.portal_requests LIMIT 1").fetchone()
    assert statuses == [("verified", 3)]
    assert types == ("TIMESTAMP", "BOOLEAN", "INTEGER")


def test_count_mismatch_is_recorded_and_raised(db):
    # The portal reports one more row than it serves for 03-10 (e.g. a record added mid-pull).
    fake = FakePortal({"2026-03-10": [_row(1, "2026-03-10")]}, count_skew={"2026-03-10": 1})
    loader = pl.PortalLoader(client=fake, db_path=db)
    with pytest.raises(pl.PortalCountMismatch):
        loader.pull("t", date(2026, 3, 10), date(2026, 3, 10))
    loader.close()
    with duckdb.connect(db, read_only=True) as c:
        row = c.execute("SELECT expected_count, landed_count, status FROM raw.portal_pulls").fetchone()
    assert row == (2, 1, "count_mismatch")


def test_pages_within_a_day_past_the_page_size(db, monkeypatch):
    monkeypatch.setattr(pl, "PAGE_SIZE", 2)
    rows = [_row(i, "2026-03-10") for i in range(5)]
    fake = FakePortal({"2026-03-10": rows})
    loader = pl.PortalLoader(client=fake, db_path=db)
    res = loader.pull("t", date(2026, 3, 10), date(2026, 3, 10))
    loader.close()
    assert res.landed == 5
    assert fake.fetches == 3  # 2 + 2 + 1


def test_duplicate_row_in_one_pull_is_rejected(db):
    # The same sr_number served twice within a pull violates the (pull_id, sr_number) key.
    fake = FakePortal({"2026-03-10": [_row(1, "2026-03-10"), _row(1, "2026-03-10")]})
    loader = pl.PortalLoader(client=fake, db_path=db)
    with pytest.raises(duckdb.ConstraintException):
        loader.pull("t", date(2026, 3, 10), date(2026, 3, 10))
    status = loader.conn.execute("SELECT status FROM raw.portal_pulls").fetchone()[0]
    loader.close()
    assert status == "failed"


def test_resolve_parents_records_found_and_missing(db):
    fake = FakePortal({"2026-03-10": [
        _row(1, "2026-03-10", parent_sr_number="SR25-00000001", duplicate="true"),   # found outside window
        _row(2, "2026-03-10", parent_sr_number="SR25-00000002", duplicate="true"),   # not found
        _row(3, "2026-03-10", parent_sr_number="SR26-00000001", duplicate="true"),   # in extract
    ]})
    loader = pl.PortalLoader(client=fake, db_path=db)
    loader.pull("t", date(2026, 3, 10), date(2026, 3, 10))
    out = loader.resolve_parents()
    loader.close()
    assert out == {"looked_up": 2, "found": 1}


def test_client_retries_transient_errors_then_succeeds():
    calls = {"n": 0}

    def handler(request):
        calls["n"] += 1
        if calls["n"] < 3:
            return httpx.Response(503)
        return httpx.Response(200, json=[{"count": "7"}])

    client = pl.PortalClient(client=httpx.Client(transport=httpx.MockTransport(handler)), sleep=lambda s: None)
    assert client.count("created_date >= '2026-01-01T00:00:00'") == 7
    assert calls["n"] == 3


def test_client_does_not_retry_a_bad_request():
    calls = {"n": 0}

    def handler(request):
        calls["n"] += 1
        return httpx.Response(400, json={"message": "bad $where"})

    client = pl.PortalClient(client=httpx.Client(transport=httpx.MockTransport(handler)), sleep=lambda s: None)
    with pytest.raises(httpx.HTTPStatusError):
        client.count("nonsense")
    assert calls["n"] == 1
