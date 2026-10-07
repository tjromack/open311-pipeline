-- Portal add-on: monthly running totals reconcile to the monthly group-by. The mart's window
-- running total is recomputed with a self-join (no window function) from an independent
-- monthly group-by of the cohort.
with monthly as (
    select fiscal_year, fiscal_month, count(*) as created_requests
    from {{ ref('int_portal_cohort') }}
    group by 1, 2
),

recomputed as (
    select a.fiscal_year, a.fiscal_month, sum(b.created_requests) as running_total
    from monthly a
    join monthly b
      on b.fiscal_year = a.fiscal_year
     and b.fiscal_month <= a.fiscal_month
    group by 1, 2
)

select v.fiscal_year, v.fiscal_month, v.created_running_total, r.running_total
from {{ ref('fct_portal_volume_monthly') }} v
full outer join recomputed r
    on r.fiscal_year = v.fiscal_year
   and r.fiscal_month = v.fiscal_month
where v.created_running_total is null
   or r.running_total is null
   or v.created_running_total <> r.running_total
