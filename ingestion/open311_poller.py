"""Polls Chicago Open311 for recent service requests and publishes them to Kafka."""

from __future__ import annotations

import argparse
import os
import time
from collections import Counter, deque
from datetime import datetime
from typing import Iterator, Optional

import httpx
import schedule
import structlog
from dotenv import load_dotenv

from ingestion.kafka_producer import CivicRequestProducer
from ingestion.schemas import ServiceRequest, schema_drift

load_dotenv()

log = structlog.get_logger(__name__)

_SEEN_CAP = 10_000


class _DriftTally:
    """Aggregate schema drift across one fetch so a feed-wide change logs once, not per record."""

    def __init__(self) -> None:
        self.records = 0
        self.missing: Counter[str] = Counter()
        self.unexpected: Counter[str] = Counter()

    def observe(self, record: dict) -> None:
        missing, unexpected = schema_drift(record)
        self.records += 1
        self.missing.update(missing)
        self.unexpected.update(unexpected)

    def report(self, **context) -> None:
        if self.missing or self.unexpected:
            log.warning(
                "open311_schema_drift",
                records=self.records,
                missing_keys=dict(self.missing),
                unexpected_keys=dict(self.unexpected),
                **context,
            )


class Open311Poller:
    """Polls Chicago's Open311 API for recent service requests with in-memory dedup."""

    def __init__(
        self,
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
        service_codes: Optional[list[str]] = None,
    ) -> None:
        self.base_url = (base_url or os.environ.get(
            "CHICAGO_311_BASE_URL", "https://311api.cityofchicago.org/open311/v2"
        )).rstrip("/")
        self.api_key = api_key if api_key is not None else os.environ.get("CHICAGO_311_API_KEY") or None
        # Some Chicago 311 endpoints serve a placeholder cert via their AWS CDN; allow
        # explicit opt-out of hostname verification when the upstream is known broken.
        self.verify_tls = os.environ.get("OPEN311_VERIFY_TLS", "true").lower() != "false"

        if service_codes is None:
            raw = os.environ.get("SERVICE_CODES", "")
            service_codes = [c.strip() for c in raw.split(",") if c.strip()]
        self.service_codes = service_codes

        self._seen_order: deque[str] = deque(maxlen=_SEEN_CAP)
        self.seen_ids: set[str] = set()

    def _mark_seen(self, request_id: str) -> None:
        if len(self._seen_order) == _SEEN_CAP:
            evicted = self._seen_order[0]
            self.seen_ids.discard(evicted)
        self._seen_order.append(request_id)
        self.seen_ids.add(request_id)

    def fetch_recent(self, page_size: int = 100) -> list[ServiceRequest]:
        """Fetch recent requests for all configured service codes; return only new ones."""
        url = f"{self.base_url}/requests.json"
        new_requests: list[ServiceRequest] = []

        codes = self.service_codes or [None]  # type: ignore[list-item]
        for code in codes:
            params: dict[str, str | int] = {"page_size": page_size}
            if code:
                params["service_code"] = code
            if self.api_key:
                params["api_key"] = self.api_key

            try:
                with httpx.Client(timeout=30.0, verify=self.verify_tls) as client:
                    response = client.get(url, params=params)
                response.raise_for_status()
                records = response.json()
            except httpx.HTTPStatusError as exc:
                log.error(
                    "open311_http_error",
                    status_code=exc.response.status_code,
                    service_code=code,
                    error=str(exc),
                )
                continue
            except httpx.HTTPError as exc:
                log.error("open311_request_error", service_code=code, error=str(exc))
                continue
            except ValueError as exc:
                log.error("open311_invalid_json", service_code=code, error=str(exc))
                continue

            if not isinstance(records, list):
                log.error("open311_unexpected_payload", service_code=code, payload_type=type(records).__name__)
                continue

            drift = _DriftTally()
            for record in records:
                if not isinstance(record, dict):
                    log.error("open311_record_not_object", service_code=code, record=repr(record)[:200])
                    continue
                drift.observe(record)
                request_id = str(record.get("service_request_id", "")).strip()
                if not request_id:
                    log.warning("open311_record_missing_id", record=record)
                    continue
                if request_id in self.seen_ids:
                    continue
                try:
                    parsed = ServiceRequest.from_api_response(record)
                except Exception as exc:  # bad datetime, type errors, etc.
                    log.error(
                        "open311_record_parse_error",
                        service_request_id=request_id,
                        error=str(exc),
                    )
                    continue
                self._mark_seen(request_id)
                new_requests.append(parsed)
            drift.report(service_code=code)

        return new_requests

    def fetch_window(
        self,
        start_date: datetime,
        end_date: datetime,
        status: Optional[str] = None,
        page_size: int = 200,
    ) -> Iterator[ServiceRequest]:
        """Paginate through requests in [start_date, end_date], yielding ServiceRequests.

        Bypasses the in-memory dedup cache — backfill is intentionally a bulk
        operation. Iterates every configured service_code and walks pages until
        the API returns a short page.
        """
        url = f"{self.base_url}/requests.json"
        codes = self.service_codes or [None]  # type: ignore[list-item]

        for code in codes:
            page = 1
            while True:
                params: dict[str, str | int] = {
                    "page_size": page_size,
                    "page": page,
                    "start_date": start_date.isoformat(),
                    "end_date": end_date.isoformat(),
                }
                if code:
                    params["service_code"] = code
                if self.api_key:
                    params["api_key"] = self.api_key
                if status:
                    params["status"] = status

                try:
                    with httpx.Client(timeout=60.0, verify=self.verify_tls) as client:
                        response = client.get(url, params=params)
                    response.raise_for_status()
                    records = response.json()
                except httpx.HTTPStatusError as exc:
                    log.error(
                        "backfill_http_error",
                        status_code=exc.response.status_code,
                        service_code=code,
                        page=page,
                        error=str(exc),
                    )
                    break
                except httpx.HTTPError as exc:
                    log.error("backfill_request_error", service_code=code, page=page, error=str(exc))
                    break
                except ValueError as exc:
                    log.error("backfill_invalid_json", service_code=code, page=page, error=str(exc))
                    break

                if not isinstance(records, list) or not records:
                    break

                drift = _DriftTally()
                for record in records:
                    if not isinstance(record, dict):
                        log.error("backfill_record_not_object", service_code=code, page=page)
                        continue
                    drift.observe(record)
                    try:
                        parsed = ServiceRequest.from_api_response(record)
                    except Exception as exc:
                        log.error(
                            "backfill_record_parse_error",
                            service_request_id=record.get("service_request_id"),
                            error=str(exc),
                        )
                        continue
                    yield parsed
                drift.report(service_code=code, page=page)

                if len(records) < page_size:
                    break
                page += 1


def run_poller(dry_run: bool = False) -> None:
    """Entry point: poll on a schedule and publish each new request to Kafka."""
    interval = int(os.environ.get("POLL_INTERVAL_SECONDS", "60"))
    poller = Open311Poller()
    producer: Optional[CivicRequestProducer] = None if dry_run else CivicRequestProducer()

    log.info(
        "poller_starting",
        interval_seconds=interval,
        service_codes=poller.service_codes,
        dry_run=dry_run,
    )

    def _tick() -> None:
        try:
            new_requests = poller.fetch_recent()
        except Exception as exc:
            log.error("poller_tick_error", error=str(exc))
            return
        log.info("poller_tick", new_requests=len(new_requests))
        for req in new_requests:
            if dry_run:
                print(req.model_dump_json())
            else:
                assert producer is not None
                producer.publish(req)
        if producer is not None:
            producer.flush()

    _tick()
    schedule.every(interval).seconds.do(_tick)

    while True:
        schedule.run_pending()
        time.sleep(1)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Chicago Open311 → Kafka poller")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print serialized requests to stdout instead of publishing to Kafka.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    run_poller(dry_run=args.dry_run)
