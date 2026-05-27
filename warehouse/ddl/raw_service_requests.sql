-- Snowflake raw table for enriched Chicago 311 service requests.
-- Idempotent upserts are keyed on service_request_id. Datetimes are stored
-- as TIMESTAMP_NTZ and the original Open311 payload is preserved as VARIANT.
--
-- Apply once per Snowflake environment via snowsql, the Snowsight worksheet,
-- or SnowflakeWriter.execute_ddl().

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
CREATE DATABASE IF NOT EXISTS CIVIC_311;
CREATE SCHEMA   IF NOT EXISTS CIVIC_311.RAW;

CREATE TABLE IF NOT EXISTS CIVIC_311.RAW.SERVICE_REQUESTS (
    service_request_id   STRING        NOT NULL,
    requested_datetime   TIMESTAMP_NTZ,
    service_name         STRING,
    service_code         STRING,
    status               STRING,
    address              STRING,
    lat                  FLOAT,
    lon                  FLOAT,
    city                 STRING,
    raw_payload          VARIANT,
    urgency_label        STRING,
    urgency_score        FLOAT,
    llm_reasoning        STRING,
    langfuse_trace_id    STRING,
    classified_at        TIMESTAMP_NTZ,
    days_to_close        FLOAT,
    _inserted_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_service_requests PRIMARY KEY (service_request_id)
);
