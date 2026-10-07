-- Portal add-on: weekly backlog obeys open_end = previous open_end + opened - closed.
-- A failure means the overlap logic double-counts or drops requests, or a request's closed
-- date precedes its created date.
with b as (
    select
        owner_department, week_end, open_at_week_end, opened_in_week, closed_in_week,
        lag(open_at_week_end) over (partition by owner_department order by week_end) as prev_open
    from {{ ref('fct_portal_backlog_weekly') }}
)

select *
from b
where open_at_week_end <> coalesce(prev_open, 0) + opened_in_week - closed_in_week
