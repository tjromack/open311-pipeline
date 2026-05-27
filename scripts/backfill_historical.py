"""Backfill Chicago 311 service requests into Snowflake. Skips LLM classification."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from typing import Optional

import structlog
from dotenv import load_dotenv

from ingestion.open311_poller import Open311Poller
from ingestion.schemas import EnrichedRequest, ServiceRequest
from warehouse.snowflake_writer import SnowflakeWriter

load_dotenv()

log = structlog.get_logger(__name__)

UNCLASSIFIED_LABEL = "Unknown"
UNCLASSIFIED_REASONING = "Backfilled — LLM classification skipped."


def _wrap_as_enriched(req: ServiceRequest, now: datetime) -> EnrichedRequest:
    return EnrichedRequest(
        **req.model_dump(),
        urgency_label=UNCLASSIFIED_LABEL,
        urgency_score=0.0,
        llm_reasoning=UNCLASSIFIED_REASONING,
        langfuse_trace_id="",
        classified_at=now,
        days_to_close=None,
    )


def backfill(
    days: int,
    status: Optional[str] = "closed",
    batch_size: int = 100,
    page_size: int = 200,
) -> int:
    """Pull the past `days` of requests from Chicago 311 and MERGE them into Snowflake."""
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=days)
    now = end_date
    log.info(
        "backfill_starting",
        start=start_date.isoformat(),
        end=end_date.isoformat(),
        status=status,
        batch_size=batch_size,
    )

    poller = Open311Poller()

    total = 0
    buffer: list[EnrichedRequest] = []

    with SnowflakeWriter(batch_size=batch_size) as writer:
        for req in poller.fetch_window(
            start_date=start_date,
            end_date=end_date,
            status=status,
            page_size=page_size,
        ):
            buffer.append(_wrap_as_enriched(req, now))
            if len(buffer) >= batch_size:
                writer.upsert_batch(buffer)
                total += len(buffer)
                buffer.clear()
        if buffer:
            writer.upsert_batch(buffer)
            total += len(buffer)

    log.info("backfill_complete", total_rows=total)
    return total


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill Chicago 311 service requests into Snowflake."
    )
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Number of days of history to pull (default: 7).",
    )
    parser.add_argument(
        "--status",
        type=str,
        default="closed",
        help="Status filter: 'closed' (default), 'open', or 'all'.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Rows per Snowflake MERGE statement (default: 100).",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=200,
        help="Records per Open311 API page (default: 200).",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    status = None if args.status.lower() == "all" else args.status
    backfill(
        days=args.days,
        status=status,
        batch_size=args.batch_size,
        page_size=args.page_size,
    )


if __name__ == "__main__":
    main()
