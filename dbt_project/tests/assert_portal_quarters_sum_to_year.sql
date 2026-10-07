-- Portal add-on: fiscal quarters sum to the year, for every count the SLA mart publishes,
-- against a year total computed directly from the cohort.
with q as (
    select fiscal_year, fiscal_quarter,
           sum(created_requests) as created, sum(met) as met, sum(missed) as missed,
           sum(pending) as pending, sum(excluded_requests) as excluded
    from {{ ref('fct_portal_sla_monthly') }}
    group by 1, 2
),

from_quarters as (
    select fiscal_year, count(*) as quarters, sum(created) as created, sum(met) as met,
           sum(missed) as missed, sum(pending) as pending, sum(excluded) as excluded
    from q
    group by 1
),

year_direct as (
    select fiscal_year, count(*) as created,
           count_if(sla_state = 'met') as met,
           count_if(sla_state = 'missed') as missed,
           count_if(sla_state = 'pending') as pending,
           count_if(sla_state = 'excluded') as excluded
    from {{ ref('int_portal_cohort') }}
    group by 1
)

select y.fiscal_year, f.quarters, y.created, f.created as created_from_quarters
from year_direct y
left join from_quarters f on f.fiscal_year = y.fiscal_year
where f.fiscal_year is null
   or f.quarters <> 4
   or y.created <> f.created
   or y.met <> f.met
   or y.missed <> f.missed
   or y.pending <> f.pending
   or y.excluded <> f.excluded
