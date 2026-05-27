"""Idempotent Snowflake writer for EnrichedRequest records (MERGE on service_request_id)."""

from __future__ import annotations

import json
import os
from typing import Iterable, Optional

import structlog
from dotenv import load_dotenv
from snowflake.connector import connect as snowflake_connect

from ingestion.schemas import EnrichedRequest

load_dotenv()

log = structlog.get_logger(__name__)

DEFAULT_BATCH_SIZE = 100

_COLUMNS: tuple[str, ...] = (
    "service_request_id",
    "requested_datetime",
    "service_name",
    "service_code",
    "status",
    "address",
    "lat",
    "lon",
    "city",
    "raw_payload",
    "urgency_label",
    "urgency_score",
    "llm_reasoning",
    "langfuse_trace_id",
    "classified_at",
    "days_to_close",
)
_NUM_COLUMNS = len(_COLUMNS)


def _record_to_row(rec: EnrichedRequest) -> tuple:
    return (
        rec.service_request_id,
        rec.requested_datetime,
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
        rec.classified_at,
        rec.days_to_close,
    )


def _build_merge_sql(fq_table: str, row_count: int) -> str:
    """Construct a MERGE statement parameterized for `row_count` rows.

    Uses Snowflake's `VALUES (...)` with pyformat (%s) placeholders. PARSE_JSON
    is applied to the raw_payload column so the connector can ship it as a
    plain string and Snowflake parses it into VARIANT.
    """
    placeholder_row = "(" + ", ".join(["%s"] * _NUM_COLUMNS) + ")"
    values_clause = ", ".join([placeholder_row] * row_count)

    insert_cols = ", ".join(_COLUMNS)
    insert_vals = ", ".join(f"src.{c}" for c in _COLUMNS)
    update_assignments = ", ".join(
        f"{c} = src.{c}" for c in _COLUMNS if c != "service_request_id"
    )

    return f"""
MERGE INTO {fq_table} tgt
USING (
    SELECT
        column1 AS service_request_id,
        column2::TIMESTAMP_NTZ AS requested_datetime,
        column3 AS service_name,
        column4 AS service_code,
        column5 AS status,
        column6 AS address,
        column7::FLOAT AS lat,
        column8::FLOAT AS lon,
        column9 AS city,
        PARSE_JSON(column10) AS raw_payload,
        column11 AS urgency_label,
        column12::FLOAT AS urgency_score,
        column13 AS llm_reasoning,
        column14 AS langfuse_trace_id,
        column15::TIMESTAMP_NTZ AS classified_at,
        column16::FLOAT AS days_to_close
    FROM VALUES {values_clause}
) src
ON tgt.service_request_id = src.service_request_id
WHEN MATCHED THEN UPDATE SET {update_assignments}
WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
""".strip()


class SnowflakeWriter:
    """Idempotent MERGE writer for EnrichedRequest records, batched in chunks."""

    def __init__(
        self,
        account: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        warehouse: Optional[str] = None,
        role: Optional[str] = None,
        table: str = "SERVICE_REQUESTS",
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> None:
        self.account = account or os.environ["SNOWFLAKE_ACCOUNT"]
        self.user = user or os.environ["SNOWFLAKE_USER"]
        self.password = password or os.environ["SNOWFLAKE_PASSWORD"]
        self.database = database or os.environ.get("SNOWFLAKE_DATABASE", "CIVIC_311")
        self.schema = schema or os.environ.get("SNOWFLAKE_SCHEMA", "RAW")
        self.warehouse = warehouse or os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
        self.role = role or os.environ.get("SNOWFLAKE_ROLE", "SYSADMIN")
        self.table = table
        self.batch_size = batch_size
        self._conn = None

    @property
    def fq_table(self) -> str:
        return f"{self.database}.{self.schema}.{self.table}"

    def connect(self):
        if self._conn is None:
            self._conn = snowflake_connect(
                account=self.account,
                user=self.user,
                password=self.password,
                database=self.database,
                schema=self.schema,
                warehouse=self.warehouse,
                role=self.role,
            )
            log.info(
                "snowflake_connected",
                account=self.account,
                database=self.database,
                schema=self.schema,
            )
        return self._conn

    def upsert_batch(self, records: Iterable[EnrichedRequest]) -> int:
        """MERGE the given records into the target table. Returns rows merged."""
        records = list(records)
        if not records:
            return 0

        conn = self.connect()
        total = 0
        for start in range(0, len(records), self.batch_size):
            chunk = records[start : start + self.batch_size]
            sql = _build_merge_sql(self.fq_table, len(chunk))
            params: list = []
            for rec in chunk:
                params.extend(_record_to_row(rec))

            cur = conn.cursor()
            try:
                cur.execute(sql, params)
            finally:
                cur.close()
            total += len(chunk)

        conn.commit()
        log.info("snowflake_upsert_batch", rows=total, table=self.fq_table)
        return total

    def execute_ddl(self, ddl_sql: str) -> None:
        """Apply DDL statements (semicolon-separated) against the connection.

        Statements consisting only of `--` line comments are skipped so that
        leading file-level comments don't trip Snowflake's "empty SQL" error.
        """
        conn = self.connect()
        for stmt in (s.strip() for s in ddl_sql.split(";")):
            if not stmt:
                continue
            non_comment = "\n".join(
                line for line in stmt.splitlines() if not line.lstrip().startswith("--")
            ).strip()
            if not non_comment:
                continue
            cur = conn.cursor()
            try:
                cur.execute(stmt)
            finally:
                cur.close()
        conn.commit()

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception as exc:
                log.warning("snowflake_close_failed", error=str(exc))
            self._conn = None

    def __enter__(self) -> "SnowflakeWriter":
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
