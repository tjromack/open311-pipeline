{#
    SLA compliance two ways, side by side, at owner_department x sr_type x fiscal month
    (month of creation).

      closed_only_sla_pct  closed_within_sla / closed_requests: the original method. It only sees
                           requests that already closed, so slow requests are missing from it.
      cohort_sla_pct       met / (met + missed): every request created in the month counts; one
                           still open past its deadline is a miss. Pending (not yet due) requests
                           are left out of the rate and counted separately.
      bias_pct_points      closed_only minus cohort, in percentage points.

    created_requests = excluded + met + missed + pending (asserted by a singular test).
#}

select
    coalesce(owner_department, 'Unknown') as owner_department,
    sr_type,
    fiscal_year,
    fiscal_quarter,
    fiscal_month,
    month_start,
    count(*) as created_requests,
    count_if(sla_state = 'excluded') as excluded_requests,
    count_if(sla_state = 'met') as met,
    count_if(sla_state = 'missed') as missed,
    count_if(sla_state = 'pending') as pending,
    count_if(closed_within_sla is not null) as closed_requests,
    count_if(closed_within_sla) as closed_within_sla,
    case when count_if(closed_within_sla is not null) > 0
        then count_if(closed_within_sla)::float / count_if(closed_within_sla is not null)
    end as closed_only_sla_pct,
    case when count_if(sla_state in ('met', 'missed')) > 0
        then count_if(sla_state = 'met')::float / count_if(sla_state in ('met', 'missed'))
    end as cohort_sla_pct,
    case when count_if(closed_within_sla is not null) > 0 and count_if(sla_state in ('met', 'missed')) > 0
        then 100.0 * (
            count_if(closed_within_sla)::float / count_if(closed_within_sla is not null)
            - count_if(sla_state = 'met')::float / count_if(sla_state in ('met', 'missed')))
    end as bias_pct_points
from {{ ref('int_portal_cohort') }}
group by 1, 2, 3, 4, 5, 6
