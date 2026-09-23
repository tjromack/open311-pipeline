-- Singular test: schema-drift tripwire for the SLA marts.
--
-- Every SLA number is computed from raw_payload.updated_datetime. If Chicago
-- renames or reformats that field, int_resolved_requests drops the affected
-- rows (updated_datetime is null), the marts quietly shrink, and every other
-- test still passes on the smaller data. This test makes that failure loud:
-- it returns a row — failing `dbt build` — when more than
-- var('max_unparseable_close_time_pct') of closed requests lack a parseable
-- close time. The 2026-09-23 survey had 0 of 7,493.

with closed as (

    select
        {{ try_to_timestamp(json_text('raw_payload', 'updated_datetime')) }} as updated_datetime
    from {{ ref('stg_service_requests') }}
    where status = 'closed'

),

summary as (

    select
        count(*) as closed_rows,
        count_if(updated_datetime is null) as unparseable_rows
    from closed

)

select
    closed_rows,
    unparseable_rows,
    unparseable_rows::float / closed_rows as unparseable_pct
from summary
where closed_rows > 0
  and unparseable_rows::float / closed_rows > {{ var('max_unparseable_close_time_pct') }}
