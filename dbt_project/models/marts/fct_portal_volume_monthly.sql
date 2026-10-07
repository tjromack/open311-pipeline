{#
    Requests created per fiscal month, with a running total through the fiscal year.
    The running total uses an explicit ROWS frame: the default RANGE frame would lump months
    together if two rows ever shared an ordering value. A singular test recomputes the running
    total with a join (no window function) and asserts the two agree.
#}

with monthly as (

    select
        fiscal_year,
        fiscal_quarter,
        fiscal_month,
        month_start,
        count(*) as created_requests,
        count_if(sla_state <> 'excluded') as in_scope_requests
    from {{ ref('int_portal_cohort') }}
    group by 1, 2, 3, 4

)

select
    *,
    sum(created_requests) over (
        partition by fiscal_year order by fiscal_month
        rows between unbounded preceding and current row
    ) as created_running_total
from monthly
