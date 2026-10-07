-- Chicago Data Portal 311 extract (dataset v6vf-nfxy), landed by ingestion/portal_loader.py.
--
-- raw.portal_requests is append-only: one row per request PER PULL, so a re-pull keeps every
-- version it saw. Staging collapses to the latest version per sr_number. Portal timestamps are
-- floating Chicago local time, exactly as published; they are never mixed with the Open311
-- path's UTC timestamps.
--
-- raw.portal_pulls is the ledger: one row per pull, per day partition, with the portal's own
-- count taken before and after paging and the landed count. Only partitions whose counts agree
-- are marked verified, and staging reads verified partitions only.

CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.portal_requests (
    pull_id             VARCHAR    NOT NULL,
    pulled_at           TIMESTAMP  NOT NULL,
    partition_date      DATE       NOT NULL,
    sr_number           VARCHAR    NOT NULL,
    sr_type             VARCHAR,
    sr_short_code       VARCHAR,
    owner_department    VARCHAR,
    status              VARCHAR,
    origin              VARCHAR,
    created_date        TIMESTAMP,
    last_modified_date  TIMESTAMP,
    closed_date         TIMESTAMP,
    duplicate           BOOLEAN,
    legacy_record       BOOLEAN,
    parent_sr_number    VARCHAR,
    street_address      VARCHAR,
    zip_code            VARCHAR,
    ward                INTEGER,
    community_area      INTEGER,
    latitude            DOUBLE,
    longitude           DOUBLE,
    PRIMARY KEY (pull_id, sr_number)
);

CREATE TABLE IF NOT EXISTS raw.portal_pulls (
    pull_id               VARCHAR    NOT NULL,
    window_label          VARCHAR    NOT NULL,
    partition_date        DATE       NOT NULL,
    where_clause          VARCHAR    NOT NULL,
    expected_count        BIGINT,
    expected_count_after  BIGINT,
    landed_count          BIGINT,
    status                VARCHAR    NOT NULL,   -- loading | verified | count_mismatch | failed
    started_at            TIMESTAMP  NOT NULL,
    finished_at           TIMESTAMP,
    PRIMARY KEY (pull_id, partition_date)
);

-- Parents referenced by landed rows but created outside the pulled window, looked up by ID.
CREATE TABLE IF NOT EXISTS raw.portal_parent_lookup (
    sr_number       VARCHAR    NOT NULL PRIMARY KEY,
    found           BOOLEAN    NOT NULL,
    sr_type         VARCHAR,
    created_date    TIMESTAMP,
    looked_up_at    TIMESTAMP  NOT NULL
);
