{#
    Weekly backlog of in-scope requests created in the fiscal year, by owner_department.

    A request is open at the end of a week when its [created, closed) range overlaps that moment:
        created_day <= week_end  and  (closed_day is null  or  closed_day > week_end)
    A still-open request has no end date and stays in every week after it was created. Weeks run
    Monday-Sunday from the fiscal calendar, from the week containing the first FY day through
    the last week that has ended by the as-of date.

    The backlog only counts requests created in the fiscal year (the extract holds nothing
    earlier), so the first week starts from zero. Flow identity, tested:
        open_at_week_end = previous open_at_week_end + opened_in_week - closed_in_week
#}

with reqs as (

    select
        coalesce(owner_department, 'Unknown') as owner_department,
        created_day,
        cast(closed_date as date) as closed_day,
        as_of_ts
    from {{ ref('int_portal_cohort') }}
    where sla_state <> 'excluded'

),

weeks as (

    select distinct week_start, week_end
    from {{ ref('fiscal_calendar') }}
    where week_end >= (select min(created_day) from reqs)
      and week_end <= (select cast(max(as_of_ts) as date) from reqs)

),

depts as (

    select distinct owner_department from reqs

),

grid as (

    select d.owner_department, w.week_start, w.week_end
    from depts d
    cross join weeks w

)

select
    g.owner_department,
    g.week_start,
    g.week_end,
    count_if(r.created_day <= g.week_end and (r.closed_day is null or r.closed_day > g.week_end))
        as open_at_week_end,
    count_if(r.created_day between g.week_start and g.week_end) as opened_in_week,
    count_if(r.closed_day between g.week_start and g.week_end) as closed_in_week
from grid g
left join reqs r
    on r.owner_department = g.owner_department
   and r.created_day <= g.week_end
group by 1, 2, 3
