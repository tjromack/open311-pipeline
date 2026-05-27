-- Singular test: sla_pct must be NULL or in [0, 1].
-- Returns rows when the invariant is violated.

select
    department,
    service_code,
    month,
    sla_pct
from {{ ref('fct_sla_compliance') }}
where sla_pct is not null
  and (sla_pct < 0 or sla_pct > 1)
