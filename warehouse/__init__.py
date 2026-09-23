"""Warehouse sinks. `build_writer()` picks the backend from WAREHOUSE_BACKEND."""

from __future__ import annotations

import os

BACKENDS = ("duckdb", "snowflake")

# Column contract for RAW.SERVICE_REQUESTS, shared by every writer. Order
# matters: writers bind parameters positionally in this order.
COLUMNS: tuple[str, ...] = (
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


def warehouse_backend() -> str:
    backend = os.environ.get("WAREHOUSE_BACKEND", "duckdb").strip().lower()
    if backend not in BACKENDS:
        raise ValueError(f"WAREHOUSE_BACKEND must be one of {BACKENDS}, got {backend!r}")
    return backend


def build_writer(batch_size: int = 100):
    """Return a DuckDBWriter (default, no account needed) or a SnowflakeWriter.

    Imports are deferred so each backend only loads its own driver.
    """
    if warehouse_backend() == "snowflake":
        from warehouse.snowflake_writer import SnowflakeWriter

        return SnowflakeWriter(batch_size=batch_size)

    from warehouse.duckdb_writer import DuckDBWriter

    return DuckDBWriter(batch_size=batch_size)
