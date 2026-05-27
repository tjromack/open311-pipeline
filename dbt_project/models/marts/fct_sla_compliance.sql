{#
    SLA compliance fact at department + service_code + month grain.

    - total_requests:      every resolved request in the period
    - classified_requests: requests with a known urgency tier (excludes 'Unknown')
    - closed_within_sla:   classified requests whose hours_to_close <= SLA threshold
    - sla_pct:             closed_within_sla / classified_requests (null if zero classified)
    - avg_days_to_close:   over all resolved requests in the bucket
#}

with resolved as (

    select
        ir.service_request_id,
        ir.service_code,
        coalesce(d.department, 'Unknown') as department,
        d.service_name,
        date_trunc('month', ir.requested_datetime) as month,
        ir.urgency_label,
        ir.met_sla,
        ir.days_to_close
    from {{ ref('int_resolved_requests') }} ir
    left join {{ ref('dim_request_category') }} d
        on ir.service_code = d.service_code

),

agg as (

    select
        department,
        service_code,
        any_value(service_name) as service_name,
        month,
        count(*) as total_requests,
        count_if(urgency_label is not null and urgency_label <> 'Unknown') as classified_requests,
        count_if(met_sla = true) as closed_within_sla,
        avg(days_to_close) as avg_days_to_close
    from resolved
    group by department, service_code, month

)

select
    department,
    service_code,
    service_name,
    month,
    total_requests,
    classified_requests,
    closed_within_sla,
    case
        when classified_requests > 0
            then closed_within_sla::float / classified_requests
        else null
    end as sla_pct,
    avg_days_to_close
from agg
