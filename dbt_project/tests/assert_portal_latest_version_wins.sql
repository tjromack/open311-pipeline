-- Portal add-on: re-pull dedup keeps exactly the latest version of each request.
-- (1) Staging holds one row per distinct sr_number in the verified extract (none lost).
-- (2) No verified version has a later last_modified_date than the one staging kept.
with verified as (
    select r.sr_number, r.last_modified_date
    from {{ source('chicago_portal', 'portal_requests') }} r
    join {{ source('chicago_portal', 'portal_pulls') }} p
      on p.pull_id = r.pull_id
     and p.partition_date = r.partition_date
    where p.status = 'verified'
),

distinct_verified as (
    select count(distinct sr_number) as n from verified
),

staged as (
    select count(*) as n from {{ ref('stg_portal_requests') }}
)

select 'row_count' as check_name, d.n as expected, s.n as actual
from distinct_verified d cross join staged s
where d.n <> s.n

union all

select 'newer_version_dropped', 1, 0
from verified v
join {{ ref('stg_portal_requests') }} s on s.sr_number = v.sr_number
where v.last_modified_date > s.last_modified_date
   or (s.last_modified_date is null and v.last_modified_date is not null)
