-- Local DuckDB mirror of CIVIC_311.RAW.SERVICE_REQUESTS (see raw_service_requests.sql).
-- Same columns and key; VARIANT becomes JSON, TIMESTAMP_NTZ becomes TIMESTAMP
-- (stored as UTC wall-clock, matching Snowflake's NTZ behaviour).
--
-- Applied automatically by DuckDBWriter on first connect; safe to re-run.

CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.service_requests (
    service_request_id   VARCHAR       NOT NULL PRIMARY KEY,
    requested_datetime   TIMESTAMP,
    service_name         VARCHAR,
    service_code         VARCHAR,
    status               VARCHAR,
    address              VARCHAR,
    lat                  DOUBLE,
    lon                  DOUBLE,
    city                 VARCHAR,
    raw_payload          JSON,
    urgency_label        VARCHAR,
    urgency_score        DOUBLE,
    llm_reasoning        VARCHAR,
    langfuse_trace_id    VARCHAR,
    classified_at        TIMESTAMP,
    days_to_close        DOUBLE,
    _inserted_at         TIMESTAMP     DEFAULT CAST(now() AS TIMESTAMP)
);
