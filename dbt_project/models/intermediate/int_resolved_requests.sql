{#
    Resolved (closed) service requests with derived close-time fields and
    a per-row SLA verdict. Drives fct_sla_compliance.

    days_to_close / hours_to_close are derived from raw_payload.updated_datetime
    (Open311's "last status change" timestamp), since the streaming pipeline
    does not currently populate days_to_close on the EnrichedRequest.
#}

with closed as (

    select *
    from {{ ref('stg_service_requests') }}
    where status = 'closed'

),

with_close_time as (

    select
        *,
        try_to_timestamp_ntz(raw_payload:updated_datetime::string) as updated_datetime
    from closed

),

final as (

    select
        service_request_id,
        service_code,
        service_name,
        address,
        lat,
        lon,
        city,
        requested_datetime,
        updated_datetime,
        datediff('millisecond', requested_datetime, updated_datetime) / 3600000.0
            as hours_to_close,
        datediff('millisecond', requested_datetime, updated_datetime) / 86400000.0
            as days_to_close,
        urgency_label,
        urgency_score,
        classified_at,
        raw_payload:group::string as department,
        {{ sla_threshold_hours('urgency_label') }} as sla_threshold_hours,
        case
            when {{ sla_threshold_hours('urgency_label') }} is null then null
            when datediff('millisecond', requested_datetime, updated_datetime) / 3600000.0
                 <= {{ sla_threshold_hours('urgency_label') }}
                then true
            else false
        end as met_sla
    from with_close_time
    where updated_datetime is not null
      and updated_datetime >= requested_datetime

)

select * from final
