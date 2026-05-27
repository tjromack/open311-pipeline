{#
    Dimension table: one row per service_code. Department is taken from the
    Open311 record's `group` field. typical_urgency_label is the most-common
    classified label observed for this category (excluding 'Unknown').
#}

with src as (

    select
        service_code,
        service_name,
        raw_payload:group::string as department,
        urgency_label
    from {{ ref('stg_service_requests') }}
    where service_code is not null

),

classified as (

    select service_code, urgency_label, count(*) as n
    from src
    where urgency_label is not null
      and urgency_label <> 'Unknown'
    group by service_code, urgency_label

),

typical as (

    select
        service_code,
        urgency_label as typical_urgency_label
    from (
        select
            service_code,
            urgency_label,
            row_number() over (partition by service_code order by n desc, urgency_label) as rn
        from classified
    )
    where rn = 1

),

base as (

    select
        service_code,
        any_value(service_name) as service_name,
        any_value(department) as department,
        count(*) as total_observed
    from src
    group by service_code

)

select
    base.service_code,
    base.service_name,
    base.department,
    coalesce(typical.typical_urgency_label, 'Unknown') as typical_urgency_label,
    base.total_observed
from base
left join typical on base.service_code = typical.service_code
