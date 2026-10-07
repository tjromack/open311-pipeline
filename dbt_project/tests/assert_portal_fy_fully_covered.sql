-- Portal add-on: every day of the fiscal year has at least one verified partition. A failed or
-- mismatched partition is excluded from staging, so without this check a gap would silently
-- shrink the cohort.
select cal.calendar_date
from {{ ref('fiscal_calendar') }} cal
left join {{ source('chicago_portal', 'portal_pulls') }} p
    on p.partition_date = cal.calendar_date
   and p.status = 'verified'
where cal.fiscal_year = {{ var('portal_fiscal_year') }}
group by cal.calendar_date
having count(p.pull_id) = 0
