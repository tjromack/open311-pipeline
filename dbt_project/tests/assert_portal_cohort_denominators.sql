-- Portal add-on: cohort denominators equal created counts.
-- (1) In every mart bucket the SLA states partition the created requests.
-- (2) The marts' created total equals the requests created in the FY in staging.
with fy as (
    select min(calendar_date) as fy_start, max(calendar_date) as fy_end
    from {{ ref('fiscal_calendar') }}
    where fiscal_year = {{ var('portal_fiscal_year') }}
),

bucket_breaks as (
    select
        'bucket' as check_name,
        owner_department || ' | ' || sr_type || ' | ' || cast(month_start as varchar) as detail,
        created_requests as expected,
        excluded_requests + met + missed + pending as actual
    from {{ ref('fct_portal_sla_monthly') }}
    where created_requests <> excluded_requests + met + missed + pending
),

staged as (
    select count(*) as n
    from {{ ref('stg_portal_requests') }} s
    cross join fy
    where cast(s.created_date as date) between fy.fy_start and fy.fy_end
),

marts as (
    select sum(created_requests) as n from {{ ref('fct_portal_sla_monthly') }}
)

select * from bucket_breaks
union all
select 'fy_total', 'all', staged.n, marts.n
from staged cross join marts
where staged.n <> marts.n
