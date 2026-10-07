{#
    The fiscal-year cohort: every request CREATED in var('portal_fiscal_year'), whether or not it
    has closed, with its tier, its SLA threshold and an explicit SLA state.

    This is the fix for the closed-only bias. Measuring compliance over requests that already
    closed drops exactly the slow ones (they are still open). Here every created request has a
    state, and the states partition the cohort:

      excluded  out-of-scope type (seed), a duplicate (tracked under its parent), or canceled
      met       closed within its tier's threshold
      missed    closed after the threshold, OR still open and already past it at the as-of time
      pending   still open and not yet past its threshold at the as-of time

    met + missed + pending + excluded = created, and that identity is a test.

    The as-of time is the latest last_modified_date in the extract: the moment the data reflects,
    in the portal's own (Chicago local) clock.
#}

with fy as (

    select
        min(calendar_date) as fy_start,
        max(calendar_date) as fy_end
    from {{ ref('fiscal_calendar') }}
    where fiscal_year = {{ var('portal_fiscal_year') }}

),

as_of as (

    select max(last_modified_date) as as_of_ts
    from {{ ref('stg_portal_requests') }}

),

cohort as (

    select
        r.sr_number,
        r.sr_type,
        r.owner_department,
        r.status,
        r.created_date,
        r.closed_date,
        r.duplicate,
        r.parent_sr_number,
        cast(r.created_date as date) as created_day
    from {{ ref('stg_portal_requests') }} r
    cross join fy
    where cast(r.created_date as date) between fy.fy_start and fy.fy_end

),

typed as (

    select
        c.*,
        cal.fiscal_year,
        cal.fiscal_quarter,
        cal.fiscal_month,
        cal.month_start,
        cal.week_start,
        coalesce(t.in_sla_scope, false) as in_sla_scope,
        t.urgency_label,
        {{ sla_threshold_hours('t.urgency_label') }} as sla_threshold_hours,
        datediff('second', c.created_date, c.closed_date) / 3600.0 as hours_to_close,
        datediff('second', c.created_date, a.as_of_ts) / 3600.0 as hours_open_at_as_of,
        a.as_of_ts
    from cohort c
    inner join {{ ref('fiscal_calendar') }} cal
        on cal.calendar_date = c.created_day
    left join {{ ref('portal_sr_types') }} t
        on t.sr_type = c.sr_type
    cross join as_of a

)

select
    *,
    case
        when not in_sla_scope or sla_threshold_hours is null then 'out_of_scope'
        when duplicate then 'duplicate'
        when status = 'Canceled' then 'canceled'
    end as exclusion_reason,
    case
        when not in_sla_scope or sla_threshold_hours is null or duplicate or status = 'Canceled'
            then 'excluded'
        when closed_date is not null and hours_to_close <= sla_threshold_hours then 'met'
        when closed_date is not null then 'missed'
        when hours_open_at_as_of > sla_threshold_hours then 'missed'
        else 'pending'
    end as sla_state,
    -- The closed-only measure the original marts used, kept for the side-by-side comparison.
    case
        when not in_sla_scope or sla_threshold_hours is null or duplicate or status = 'Canceled'
            then null
        when closed_date is null then null
        else hours_to_close <= sla_threshold_hours
    end as closed_within_sla
from typed
