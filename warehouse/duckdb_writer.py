"""Local DuckDB writer for EnrichedRequest records — the no-Snowflake-account path.

Mirrors SnowflakeWriter's interface and its idempotency contract: every write
is a MERGE INTO on service_request_id, never a plain INSERT.

DuckDB allows one read-write process per database file, and dbt needs to open
the same file. So connections are short-lived: opened per batch and closed
immediately, with a brief retry if another process holds the lock.
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

import duckdb
import structlog
from dotenv import load_dotenv

from ingestion.schemas import EnrichedRequest, ServiceRequest
from warehouse import COLUMNS as _COLUMNS

load_dotenv()

log = structlog.get_logger(__name__)

DEFAULT_BATCH_SIZE = 100
DEFAULT_DUCKDB_PATH = "data/civic_311.duckdb"
FQ_TABLE = "raw.service_requests"
DDL_PATH = Path(__file__).parent / "ddl" / "raw_service_requests.duckdb.sql"

_LOCK_RETRIES = 20
_LOCK_BACKOFF_SECONDS = 0.5


def _to_utc_naive(value: Optional[datetime]) -> Optional[datetime]:
    """Match Snowflake TIMESTAMP_NTZ semantics: store UTC wall-clock, no zone.

    DuckDB would otherwise convert an aware datetime using the session
    TimeZone, silently shifting every timestamp by the local UTC offset.
    """
    if value is None or value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _record_to_row(rec: EnrichedRequest) -> tuple:
    return (
        rec.service_request_id,
        _to_utc_naive(rec.requested_datetime),
        rec.service_name,
        rec.service_code,
        rec.status,
        rec.address,
        rec.lat,
        rec.lon,
        rec.city,
        json.dumps(rec.raw_payload, default=str),
        rec.urgency_label,
        rec.urgency_score,
        rec.llm_reasoning,
        rec.langfuse_trace_id,
        _to_utc_naive(rec.classified_at),
        rec.days_to_close,
    )


def _build_merge_sql(staging_table: str) -> str:
    insert_cols = ", ".join(_COLUMNS)
    insert_vals = ", ".join(f"src.{c}" for c in _COLUMNS)
    update_assignments = ", ".join(
        f"{c} = src.{c}" for c in _COLUMNS if c != "service_request_id"
    )
    return f"""
MERGE INTO {FQ_TABLE} tgt
USING {staging_table} src
ON tgt.service_request_id = src.service_request_id
WHEN MATCHED THEN UPDATE SET {update_assignments}
WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
""".strip()


class DuckDBWriter:
    """Idempotent MERGE writer targeting a local DuckDB file."""

    def __init__(
        self,
        path: Optional[str] = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> None:
        self.path = path or os.environ.get("DUCKDB_PATH") or DEFAULT_DUCKDB_PATH
        self.batch_size = batch_size
        self._bootstrapped = False

    @property
    def fq_table(self) -> str:
        return FQ_TABLE

    def _open(self) -> duckdb.DuckDBPyConnection:
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        for attempt in range(1, _LOCK_RETRIES + 1):
            try:
                conn = duckdb.connect(self.path)
                break
            except duckdb.IOException as exc:
                # Another process (usually a dbt run) holds the file lock.
                if attempt == _LOCK_RETRIES:
                    raise
                log.debug("duckdb_locked_retrying", attempt=attempt, error=str(exc))
                time.sleep(_LOCK_BACKOFF_SECONDS)
        conn.execute("SET TimeZone = 'UTC'")
        if not self._bootstrapped:
            conn.execute(DDL_PATH.read_text())
            self._bootstrapped = True
        return conn

    def connect(self) -> duckdb.DuckDBPyConnection:
        """Return a fresh connection. Caller closes it (or use it as a context manager)."""
        return self._open()

    def upsert_batch(self, records: Iterable[EnrichedRequest]) -> int:
        """MERGE the given records into raw.service_requests. Returns rows merged."""
        records = list(records)
        if not records:
            return 0

        placeholders = ", ".join(["?"] * len(_COLUMNS))
        total = 0
        with self._open() as conn:
            for start in range(0, len(records), self.batch_size):
                chunk = records[start : start + self.batch_size]
                conn.execute(
                    f"CREATE OR REPLACE TEMP TABLE _merge_src AS "
                    f"SELECT * FROM {FQ_TABLE} LIMIT 0"
                )
                conn.executemany(
                    f"INSERT INTO _merge_src ({', '.join(_COLUMNS)}) VALUES ({placeholders})",
                    [_record_to_row(r) for r in chunk],
                )
                conn.execute(_build_merge_sql("_merge_src"))
                total += len(chunk)
        log.info("duckdb_upsert_batch", rows=total, table=FQ_TABLE, path=self.path)
        return total

    def fetch_unclassified(
        self, limit: Optional[int] = None, sample_seed: Optional[int] = None
    ) -> list[ServiceRequest]:
        """Rows still labelled 'Unknown', oldest first or in a seeded pseudo-random order."""
        order = (
            f"hash(service_request_id || '{int(sample_seed)}')"
            if sample_seed is not None
            else "requested_datetime"
        )
        sql = f"""
            SELECT service_request_id, requested_datetime, service_name, service_code,
                   status, address, lat, lon, city, raw_payload
            FROM {FQ_TABLE}
            WHERE urgency_label = 'Unknown'
            ORDER BY {order}
        """
        if limit is not None:
            sql += f" LIMIT {int(limit)}"
        with self._open() as conn:
            cur = conn.execute(sql)
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        return [_row_to_service_request(r) for r in rows]

    def fetch_classified(self) -> list[EnrichedRequest]:
        """Every classified row (urgency_label <> 'Unknown'), for fixture export."""
        sql = f"""
            SELECT {', '.join(_COLUMNS)}
            FROM {FQ_TABLE}
            WHERE urgency_label <> 'Unknown'
            ORDER BY service_request_id
        """
        with self._open() as conn:
            cur = conn.execute(sql)
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        out = []
        for r in rows:
            r["raw_payload"] = _load_json(r["raw_payload"])
            r["requested_datetime"] = r["requested_datetime"].replace(tzinfo=timezone.utc)
            r["classified_at"] = r["classified_at"].replace(tzinfo=timezone.utc)
            out.append(EnrichedRequest(**r))
        return out

    def execute_ddl(self, ddl_sql: str) -> None:
        with self._open() as conn:
            conn.execute(ddl_sql)

    def close(self) -> None:
        """No-op: connections are per-call. Kept for SnowflakeWriter interface parity."""

    def __enter__(self) -> "DuckDBWriter":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False


def _load_json(value: Any) -> dict:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return {}
    return {}


def _row_to_service_request(row: dict[str, Any]) -> ServiceRequest:
    requested = row["requested_datetime"]
    if isinstance(requested, datetime) and requested.tzinfo is None:
        requested = requested.replace(tzinfo=timezone.utc)
    return ServiceRequest(
        service_request_id=row["service_request_id"],
        requested_datetime=requested,
        service_name=row.get("service_name") or "",
        service_code=row.get("service_code") or "",
        status=row.get("status") or "open",
        address=row.get("address") or "",
        lat=row.get("lat"),
        lon=row.get("lon"),
        city=row.get("city") or "chicago",
        raw_payload=_load_json(row.get("raw_payload")),
    )
