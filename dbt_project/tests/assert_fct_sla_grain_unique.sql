-- Singular test: fct_sla_compliance grain (department, service_code, month) must be unique.
-- Returns rows when the invariant is violated.

select
    department,
    service_code,
    month,
    count(*) as duplicate_count
from {{ ref('fct_sla_compliance') }}
group by 1, 2, 3
having count(*) > 1
