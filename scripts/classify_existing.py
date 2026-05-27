"""Classify rows in Snowflake that were backfilled with urgency_label='Unknown'.

Reuses the streaming pipeline's UrgencyClassifier (Claude Haiku 4.5 + Langfuse
tracing) and SnowflakeWriter (MERGE on service_request_id). One-off; the
streaming consumer remains the primary path for new requests.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from typing import Any, Optional

import structlog
from dotenv import load_dotenv
from snowflake.connector import DictCursor

from classifier.urgency_classifier import UrgencyClassifier
from ingestion.schemas import EnrichedRequest, ServiceRequest
from warehouse.snowflake_writer import SnowflakeWriter

load_dotenv()

log = structlog.get_logger(__name__)


def _build_langfuse_handler():
    """Construct a Langfuse CallbackHandler if creds are configured; else None."""
    public_key = os.environ.get("LANGFUSE_PUBLIC_KEY")
    secret_key = os.environ.get("LANGFUSE_SECRET_KEY")
    if not (public_key and secret_key):
        log.warning("langfuse_not_configured")
        return None
    try:
        from langfuse.callback import CallbackHandler  # type: ignore[import-not-found]
    except ImportError:
        log.warning("langfuse_import_failed")
        return None
    return CallbackHandler(
        public_key=public_key,
        secret_key=secret_key,
        host=os.environ.get("LANGFUSE_HOST", "https://cloud.langfuse.com"),
    )


def _row_to_service_request(row: dict[str, Any]) -> ServiceRequest:
    """Reconstruct a ServiceRequest from a Snowflake row.

    VARIANT columns come back as JSON-serialized strings from the connector;
    deserialize raw_payload so the Pydantic model receives a real dict.
    """
    raw_payload = row.get("RAW_PAYLOAD")
    if isinstance(raw_payload, str):
        try:
            raw_payload = json.loads(raw_payload)
        except json.JSONDecodeError:
            raw_payload = {}
    elif raw_payload is None:
        raw_payload = {}

    return ServiceRequest(
        service_request_id=row["SERVICE_REQUEST_ID"],
        requested_datetime=row["REQUESTED_DATETIME"],
        service_name=row.get("SERVICE_NAME") or "",
        service_code=row.get("SERVICE_CODE") or "",
        status=row.get("STATUS") or "open",
        address=row.get("ADDRESS") or "",
        lat=row.get("LAT"),
        lon=row.get("LON"),
        city=row.get("CITY") or "chicago",
        raw_payload=raw_payload,
    )


def classify_existing(
    limit: Optional[int] = None,
    write_batch_size: int = 50,
    progress_every: int = 10,
) -> int:
    """Classify all rows where urgency_label='Unknown'. Returns rows classified."""
    handler = _build_langfuse_handler()
    classifier = UrgencyClassifier(langfuse_handler=handler)
    writer = SnowflakeWriter(batch_size=write_batch_size)
    conn = writer.connect()

    sql = """
        SELECT
            service_request_id, requested_datetime, service_name, service_code,
            status, address, lat, lon, city, raw_payload
        FROM CIVIC_311.RAW.SERVICE_REQUESTS
        WHERE urgency_label = 'Unknown'
        ORDER BY requested_datetime
    """
    if limit is not None:
        sql += f"\n        LIMIT {int(limit)}"

    cur = conn.cursor(DictCursor)
    cur.execute(sql)
    rows = cur.fetchall()
    cur.close()

    total = len(rows)
    log.info("classify_existing_starting", total=total, limit=limit)
    if total == 0:
        writer.close()
        return 0

    buffer: list[EnrichedRequest] = []
    started = time.monotonic()
    classified_count = 0

    for i, row in enumerate(rows, start=1):
        try:
            req = _row_to_service_request(row)
            enriched = classifier.classify(req)
        except Exception as exc:
            log.error(
                "classify_failed",
                service_request_id=row.get("SERVICE_REQUEST_ID"),
                error=str(exc),
            )
            continue

        buffer.append(enriched)
        classified_count += 1

        if len(buffer) >= write_batch_size:
            writer.upsert_batch(buffer)
            buffer.clear()

        if i % progress_every == 0 or i == total:
            elapsed = time.monotonic() - started
            rate = i / elapsed if elapsed > 0 else 0.0
            remaining = (total - i) / rate if rate > 0 else float("inf")
            log.info(
                "classify_progress",
                done=i,
                total=total,
                rate_per_sec=round(rate, 2),
                eta_seconds=int(remaining) if remaining != float("inf") else None,
            )

    if buffer:
        writer.upsert_batch(buffer)
        buffer.clear()

    # Best-effort flush of any pending Langfuse traces
    if handler is not None:
        for attr in ("flush", "shutdown"):
            fn = getattr(handler, attr, None)
            if callable(fn):
                try:
                    fn()
                    break
                except Exception as exc:
                    log.warning("langfuse_flush_failed", method=attr, error=str(exc))

    writer.close()
    log.info(
        "classify_existing_complete",
        classified=classified_count,
        skipped=total - classified_count,
        seconds=round(time.monotonic() - started, 1),
    )
    return classified_count


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Classify Snowflake rows where urgency_label='Unknown'."
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max rows to classify. Omit to process all unclassified rows.",
    )
    parser.add_argument(
        "--write-batch-size",
        type=int,
        default=50,
        help="Rows per Snowflake MERGE statement (default: 50).",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=10,
        help="Log progress every N rows (default: 10).",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    classify_existing(
        limit=args.limit,
        write_batch_size=args.write_batch_size,
        progress_every=args.progress_every,
    )


if __name__ == "__main__":
    main()
