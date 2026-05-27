{#
    Staging materializes as a view (configured in dbt_project.yml).

    An earlier iteration of this model was incremental on `_inserted_at`, but
    the streaming classifier UPDATEs rows in place via MERGE without bumping
    that column, so incremental never picked up classification changes and
    required `--full-refresh` after every classifier run. The source table is
    small enough that a view is the simpler, correct choice — the downstream
    marts that need durable storage are materialized as tables.
#}

with source as (

    select
        service_request_id,
        requested_datetime,
        service_name,
        service_code,
        status,
        address,
        lat,
        lon,
        city,
        raw_payload,
        urgency_label,
        urgency_score,
        llm_reasoning,
        langfuse_trace_id,
        classified_at,
        days_to_close,
        _inserted_at
    from {{ source('civic_311', 'service_requests') }}
    where service_request_id is not null

)

select * from source
