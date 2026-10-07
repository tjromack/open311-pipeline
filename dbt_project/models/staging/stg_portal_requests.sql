{#
    One row per Chicago 311 request: the latest version seen across all VERIFIED portal pulls.

    raw.portal_requests is append-only (one row per request per pull), so a re-pull of the last
    30 days adds a second version of every request it touches. The latest version wins, with a
    total, deterministic tiebreak:
        1. last_modified_date desc  (the portal's own change timestamp; nulls last)
        2. pulled_at desc            (later pull wins when the record didn't change)
        3. pull_id desc              (two pulls in the same second still resolve)

    Partitions whose counts didn't reconcile (status <> 'verified') are never read.
    Timestamps are floating Chicago local time, as published by the portal.
#}

with verified as (

    select r.*
    from {{ source('chicago_portal', 'portal_requests') }} r
    inner join {{ source('chicago_portal', 'portal_pulls') }} p
        on p.pull_id = r.pull_id
       and p.partition_date = r.partition_date
    where p.status = 'verified'

),

ranked as (

    select
        sr_number,
        sr_type,
        sr_short_code,
        owner_department,
        status,
        origin,
        created_date,
        last_modified_date,
        closed_date,
        duplicate,
        legacy_record,
        nullif(parent_sr_number, '') as parent_sr_number,
        ward,
        community_area,
        pull_id,
        pulled_at,
        count(*) over (partition by sr_number) as versions_seen,
        row_number() over (
            partition by sr_number
            order by last_modified_date desc nulls last, pulled_at desc, pull_id desc
        ) as version_rank
    from verified

)

select
    sr_number,
    sr_type,
    sr_short_code,
    owner_department,
    status,
    origin,
    created_date,
    last_modified_date,
    closed_date,
    duplicate,
    legacy_record,
    parent_sr_number,
    ward,
    community_area,
    pull_id as latest_pull_id,
    pulled_at as latest_pulled_at,
    versions_seen
from ranked
where version_rank = 1
