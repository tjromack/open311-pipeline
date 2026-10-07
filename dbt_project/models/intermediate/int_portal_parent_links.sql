{#
    Every request that names a parent, with where that parent was found.

      in_extract      the parent is in the landed extract
      outside_window  not in the extract, but the portal has it (looked up by ID): it was
                      created before the window began. An artifact of the extract window,
                      not bad data.
      not_found       looked up by ID and the portal has no such request
      not_looked_up   `python -m ingestion.portal_loader --resolve-parents` hasn't covered it yet
#}

with children as (

    select sr_number, sr_type, created_date, duplicate, parent_sr_number
    from {{ ref('stg_portal_requests') }}
    where parent_sr_number is not null

)

select
    c.sr_number,
    c.sr_type,
    c.created_date,
    c.duplicate,
    c.parent_sr_number,
    coalesce(p.created_date, l.created_date) as parent_created_date,
    coalesce(p.sr_type, l.sr_type) as parent_sr_type,
    case
        when p.sr_number is not null then 'in_extract'
        when l.found then 'outside_window'
        when l.sr_number is not null then 'not_found'
        else 'not_looked_up'
    end as parent_location
from children c
left join {{ ref('stg_portal_requests') }} p
    on p.sr_number = c.parent_sr_number
left join {{ source('chicago_portal', 'portal_parent_lookup') }} l
    on l.sr_number = c.parent_sr_number
