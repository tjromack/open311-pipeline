{#
    The fiscal-year side-by-side, one row per in-scope sr_type: closed-only vs cohort compliance.
    This is the published comparison. Built from the monthly mart so the two can't disagree.
#}

with m as (

    select * from {{ ref('fct_portal_sla_monthly') }}

)

select
    sr_type,
    max(owner_department) as owner_department,
    fiscal_year,
    sum(created_requests) as created_requests,
    sum(excluded_requests) as excluded_requests,
    sum(met) as met,
    sum(missed) as missed,
    sum(pending) as pending,
    sum(closed_requests) as closed_requests,
    sum(closed_within_sla) as closed_within_sla,
    case when sum(closed_requests) > 0
        then sum(closed_within_sla)::float / sum(closed_requests) end as closed_only_sla_pct,
    case when sum(met) + sum(missed) > 0
        then sum(met)::float / (sum(met) + sum(missed)) end as cohort_sla_pct,
    case when sum(closed_requests) > 0 and sum(met) + sum(missed) > 0
        then 100.0 * (sum(closed_within_sla)::float / sum(closed_requests)
                      - sum(met)::float / (sum(met) + sum(missed))) end as bias_pct_points
from m
group by sr_type, fiscal_year
having sum(met) + sum(missed) + sum(pending) > 0
