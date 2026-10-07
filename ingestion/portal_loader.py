"""Load Chicago 311 requests from the city's data portal (Socrata dataset v6vf-nfxy) into DuckDB.

The Open311 API only serves recent requests; the portal holds the full history plus fields the
API lacks (closed_date, owner_department, duplicate, parent_sr_number, legacy_record).

Paging. The window is split into one partition per calendar day of created_date. Each day is
fetched with a stable sort (created_date, sr_number); a day never comes close to the 50,000-row
page limit, but the loop pages within a day anyway. Ordering the whole year by sr_number on the
server is not viable (a 1,000-row page timed out at 300 s), while a filtered day returns in ~1 s.

Verification. For every day the portal's own count is taken before and after paging, and the
rows landed for that (pull, day) are counted in DuckDB. A partition is `verified` only when all
three agree; anything else is recorded as `count_mismatch` and the run exits non-zero. Staging
reads verified partitions only, so a partial or drifting pull can't leak into the marts.

    python -m ingestion.portal_loader --fy 2026                 # full federal FY2026
    python -m ingestion.portal_loader --fy 2026 --last-days 30  # re-pull the FY's last 30 days
    python -m ingestion.portal_loader --resolve-parents          # look up out-of-window parents
"""

from __future__ import annotations

import argparse
import os
import tempfile
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import duckdb
import httpx
import structlog
from dotenv import load_dotenv

load_dotenv()

log = structlog.get_logger(__name__)

PORTAL_BASE = "https://data.cityofchicago.org/resource/v6vf-nfxy"
DEFAULT_DUCKDB_PATH = "data/civic_311.duckdb"
DDL_PATH = Path(__file__).resolve().parents[1] / "warehouse" / "ddl" / "portal_requests.duckdb.sql"

PORTAL_COLUMNS: tuple[str, ...] = (
    "sr_number", "sr_type", "sr_short_code", "owner_department", "status", "origin",
    "created_date", "last_modified_date", "closed_date", "duplicate", "legacy_record",
    "parent_sr_number", "street_address", "zip_code", "ward", "community_area",
    "latitude", "longitude",
)
PAGE_SIZE = 50_000
_RETRIES = 6


class PortalCountMismatch(RuntimeError):
    """At least one partition landed a different number of rows than the portal reports."""


def fiscal_year_bounds(fy: int) -> tuple[date, date]:
    """Federal fiscal year: Oct 1 of the prior calendar year through Sep 30 (inclusive)."""
    return date(fy - 1, 10, 1), date(fy, 9, 30)


def day_where(day: date) -> str:
    nxt = day + timedelta(days=1)
    return f"created_date >= '{day.isoformat()}T00:00:00' AND created_date < '{nxt.isoformat()}T00:00:00'"


class PortalClient:
    """Thin SODA client with retry/backoff on timeouts, 429 and 5xx."""

    def __init__(self, base_url: str = PORTAL_BASE, timeout: float = 120.0,
                 client: Optional[httpx.Client] = None, sleep=time.sleep) -> None:
        self.base_url = base_url.rstrip("/")
        headers = {}
        token = os.environ.get("SOCRATA_APP_TOKEN")
        if token:
            headers["X-App-Token"] = token
        verify = os.environ.get("REQUESTS_CA_BUNDLE") or True
        self._client = client or httpx.Client(timeout=timeout, headers=headers, verify=verify)
        self._sleep = sleep

    def _get(self, suffix: str, params: dict) -> httpx.Response:
        delay = 2.0
        for attempt in range(1, _RETRIES + 1):
            try:
                resp = self._client.get(f"{self.base_url}{suffix}", params=params)
                if resp.status_code == 429 or resp.status_code >= 500:
                    raise httpx.HTTPStatusError(f"HTTP {resp.status_code}", request=resp.request, response=resp)
                resp.raise_for_status()
                return resp
            except (httpx.TimeoutException, httpx.TransportError, httpx.HTTPStatusError) as exc:
                retryable = not isinstance(exc, httpx.HTTPStatusError) or (
                    exc.response.status_code == 429 or exc.response.status_code >= 500)
                if not retryable or attempt == _RETRIES:
                    raise
                log.warning("portal_retry", attempt=attempt, delay_s=delay, error=str(exc)[:200])
                self._sleep(delay)
                delay = min(delay * 2, 60.0)
        raise AssertionError("unreachable")

    def count(self, where: str) -> int:
        rows = self._get(".json", {"$select": "count(*)", "$where": where}).json()
        return int(rows[0]["count"])

    def fetch_csv(self, where: str, offset: int = 0, limit: int = PAGE_SIZE) -> str:
        params = {
            "$select": ",".join(PORTAL_COLUMNS),
            "$where": where,
            "$order": "created_date, sr_number",   # stable, total within a day
            "$limit": limit,
            "$offset": offset,
        }
        return self._get(".csv", params).text

    def fetch_by_ids(self, ids: list[str]) -> list[dict]:
        quoted = ",".join("'" + i.replace("'", "''") + "'" for i in ids)
        params = {"$select": "sr_number,sr_type,created_date", "$where": f"sr_number in ({quoted})",
                  "$limit": len(ids)}
        return self._get(".json", params).json()


@dataclass
class PullResult:
    pull_id: str
    window_label: str
    partitions: int = 0
    expected: int = 0
    landed: int = 0
    mismatched_days: list[str] = field(default_factory=list)


class PortalLoader:
    def __init__(self, client: Optional[PortalClient] = None, db_path: Optional[str] = None) -> None:
        self.client = client or PortalClient()
        self.db_path = db_path or os.environ.get("DUCKDB_PATH") or DEFAULT_DUCKDB_PATH
        if self.db_path != ":memory:":
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = duckdb.connect(self.db_path)
        self.conn.execute("SET TimeZone = 'UTC'")  # portal timestamps are stored as published
        self.conn.execute(DDL_PATH.read_text(encoding="utf-8"))

    # -- loading ---------------------------------------------------------------------------
    def _land_page(self, csv_text: str, pull_id: str, pulled_at: datetime, day: date) -> int:
        """Insert one CSV page; returns rows in the page (0 for a header-only page)."""
        if csv_text.strip().count("\n") == 0:
            return 0
        with tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False, encoding="utf-8", newline="") as fh:
            fh.write(csv_text)
            tmp = fh.name
        try:
            before = self._count(pull_id, day)
            self.conn.execute(
                """
                INSERT INTO raw.portal_requests
                SELECT ?, ?, ?, sr_number, sr_type, sr_short_code, owner_department, status, origin,
                       TRY_CAST(created_date AS TIMESTAMP), TRY_CAST(last_modified_date AS TIMESTAMP),
                       TRY_CAST(closed_date AS TIMESTAMP), TRY_CAST(duplicate AS BOOLEAN),
                       TRY_CAST(legacy_record AS BOOLEAN), parent_sr_number, street_address, zip_code,
                       TRY_CAST(ward AS INTEGER), TRY_CAST(community_area AS INTEGER),
                       TRY_CAST(latitude AS DOUBLE), TRY_CAST(longitude AS DOUBLE)
                FROM read_csv(?, header = true, all_varchar = true, quote = '"', escape = '"',
                              columns = {cols})
                """.replace("{cols}", "{" + ", ".join(f"'{c}': 'VARCHAR'" for c in PORTAL_COLUMNS) + "}"),
                [pull_id, pulled_at, day, tmp],
            )
            return self._count(pull_id, day) - before
        finally:
            os.unlink(tmp)

    def _count(self, pull_id: str, day: date) -> int:
        return self.conn.execute(
            "SELECT count(*) FROM raw.portal_requests WHERE pull_id = ? AND partition_date = ?",
            [pull_id, day]).fetchone()[0]

    def pull(self, window_label: str, start: date, end: date) -> PullResult:
        """Pull every day in [start, end]; record each partition in raw.portal_pulls."""
        started = datetime.now(timezone.utc).replace(tzinfo=None)
        pull_id = f"{window_label}-{started:%Y%m%dT%H%M%SZ}"
        result = PullResult(pull_id=pull_id, window_label=window_label)
        log.info("portal_pull_start", pull_id=pull_id, start=str(start), end=str(end))
        day = start
        while day <= end:
            where = day_where(day)
            t0 = datetime.now(timezone.utc).replace(tzinfo=None)
            self.conn.execute(
                "INSERT INTO raw.portal_pulls VALUES (?, ?, ?, ?, NULL, NULL, NULL, 'loading', ?, NULL)",
                [pull_id, window_label, day, where, t0])
            try:
                expected = self.client.count(where)
                offset = 0
                while True:
                    landed_page = self._land_page(
                        self.client.fetch_csv(where, offset, PAGE_SIZE), pull_id, started, day)
                    offset += landed_page
                    if landed_page < PAGE_SIZE:
                        break
                expected_after = self.client.count(where)
                landed = self._count(pull_id, day)
                status = "verified" if expected == landed == expected_after else "count_mismatch"
            except Exception as exc:
                self.conn.execute(
                    "UPDATE raw.portal_pulls SET status='failed', finished_at=? WHERE pull_id=? AND partition_date=?",
                    [datetime.now(timezone.utc).replace(tzinfo=None), pull_id, day])
                log.error("portal_partition_failed", day=str(day), error=str(exc)[:300])
                raise
            self.conn.execute(
                """UPDATE raw.portal_pulls SET expected_count=?, expected_count_after=?, landed_count=?,
                   status=?, finished_at=? WHERE pull_id=? AND partition_date=?""",
                [expected, expected_after, landed, status,
                 datetime.now(timezone.utc).replace(tzinfo=None), pull_id, day])
            result.partitions += 1
            result.expected += expected
            result.landed += landed
            if status != "verified":
                result.mismatched_days.append(str(day))
                log.error("portal_count_mismatch", day=str(day), expected=expected,
                          expected_after=expected_after, landed=landed)
            if result.partitions % 30 == 0:
                log.info("portal_pull_progress", day=str(day), partitions=result.partitions,
                         landed=result.landed)
            day += timedelta(days=1)
        log.info("portal_pull_done", pull_id=pull_id, partitions=result.partitions,
                 expected=result.expected, landed=result.landed,
                 mismatched_days=len(result.mismatched_days))
        if result.mismatched_days:
            raise PortalCountMismatch(
                f"{len(result.mismatched_days)} partition(s) disagree with the portal count: "
                f"{result.mismatched_days[:10]}")
        return result

    # -- parents -------------------------------------------------------------------------
    def resolve_parents(self, batch: int = 50) -> dict:
        """Look up parent_sr_numbers that aren't in any verified pull, by ID, outside the window."""
        missing = [r[0] for r in self.conn.execute(
            """
            WITH landed AS (
                SELECT DISTINCT r.sr_number FROM raw.portal_requests r
                JOIN raw.portal_pulls p ON p.pull_id = r.pull_id AND p.partition_date = r.partition_date
                WHERE p.status = 'verified')
            SELECT DISTINCT r.parent_sr_number FROM raw.portal_requests r
            WHERE r.parent_sr_number IS NOT NULL AND r.parent_sr_number <> ''
              AND r.parent_sr_number NOT IN (SELECT sr_number FROM landed)
              AND r.parent_sr_number NOT IN (SELECT sr_number FROM raw.portal_parent_lookup)
            ORDER BY 1
            """).fetchall()]
        found = 0
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        for i in range(0, len(missing), batch):
            chunk = missing[i:i + batch]
            rows = {r["sr_number"]: r for r in self.client.fetch_by_ids(chunk)}
            for sr in chunk:
                r = rows.get(sr)
                self.conn.execute(
                    "INSERT OR REPLACE INTO raw.portal_parent_lookup VALUES (?, ?, ?, TRY_CAST(? AS TIMESTAMP), ?)",
                    [sr, r is not None, r and r.get("sr_type"), r and r.get("created_date"), now])
                found += r is not None
        log.info("portal_parents_resolved", looked_up=len(missing), found=found)
        return {"looked_up": len(missing), "found": found}

    def close(self) -> None:
        self.conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--fy", type=int, help="Federal fiscal year to pull (e.g. 2026 = 2025-10-01..2026-09-30).")
    parser.add_argument("--last-days", type=int, help="Only the last N days of the fiscal year (a re-pull).")
    parser.add_argument("--start", type=date.fromisoformat, help="Custom window start (YYYY-MM-DD).")
    parser.add_argument("--end", type=date.fromisoformat, help="Custom window end, inclusive.")
    parser.add_argument("--label", help="Window label recorded in raw.portal_pulls.")
    parser.add_argument("--resolve-parents", action="store_true", help="Look up out-of-window parent requests.")
    args = parser.parse_args()

    loader = PortalLoader()
    try:
        if args.fy or args.start:
            if args.fy:
                start, end = fiscal_year_bounds(args.fy)
                label = args.label or f"fy{args.fy}"
            else:
                start, end, label = args.start, args.end or args.start, args.label or "custom"
            if args.last_days:
                start = max(start, end - timedelta(days=args.last_days - 1))
                label = args.label or f"{label}-last{args.last_days}d"
            loader.pull(label, start, end)
        if args.resolve_parents:
            loader.resolve_parents()
    finally:
        loader.close()


if __name__ == "__main__":
    main()
