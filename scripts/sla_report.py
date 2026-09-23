"""Print the SLA compliance mart from the local DuckDB warehouse.

Reads ANALYTICS_marts.fct_sla_compliance + dim_request_category after
`dbt build`. Every number in the README's "Headline results" comes from here.

    python scripts/sla_report.py [--min-classified 3]
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# Let `python scripts/<name>.py` import the project packages from a fresh clone.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import duckdb

from warehouse.duckdb_writer import DEFAULT_DUCKDB_PATH

HEADLINE_SQL = """
select
    (select count(*) from raw.service_requests)                                  as raw_rows,
    (select count(*) from raw.service_requests where urgency_label <> 'Unknown') as classified_rows,
    (select count(*) from ANALYTICS_marts.fct_sla_compliance)                    as buckets,
    (select count(*) from ANALYTICS_marts.fct_sla_compliance
       where sla_pct is not null)                                                as buckets_with_sla,
    (select sum(closed_within_sla)::double / nullif(sum(classified_requests), 0)
       from ANALYTICS_marts.fct_sla_compliance)                                  as pooled_sla_pct
"""

CATEGORY_SQL = """
select
    f.service_name,
    d.typical_urgency_label,
    sum(f.classified_requests)                                         as classified,
    sum(f.closed_within_sla)::double / nullif(sum(f.classified_requests), 0) as sla_pct,
    sum(f.avg_days_to_close * f.total_requests) / sum(f.total_requests) as avg_days_to_close
from ANALYTICS_marts.fct_sla_compliance f
join ANALYTICS_marts.dim_request_category d using (service_code)
group by 1, 2
having sum(f.classified_requests) >= ?
order by sla_pct desc, classified desc
"""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--min-classified",
        type=int,
        default=3,
        help="Hide categories with fewer classified requests than this (default: 3).",
    )
    args = parser.parse_args()

    path = os.environ.get("DUCKDB_PATH") or DEFAULT_DUCKDB_PATH
    with duckdb.connect(path, read_only=True) as conn:
        raw, classified, buckets, with_sla, pooled = conn.execute(HEADLINE_SQL).fetchone()
        rows = conn.execute(CATEGORY_SQL, [args.min_classified]).fetchall()

    print(f"\nWarehouse: {path}")
    print(f"  raw rows:            {raw:,}")
    print(f"  classified rows:     {classified:,}")
    print(f"  SLA buckets:         {buckets} ({with_sla} with a classified request)")
    print(f"  pooled SLA compliance (classified, closed): "
          f"{'n/a' if pooled is None else f'{pooled:.1%}'}\n")

    header = f"{'Category':<40} {'Urgency':<9} {'n':>4} {'SLA %':>7} {'Avg days':>9}"
    print(header)
    print("-" * len(header))
    for name, urgency, n, sla_pct, avg_days in rows:
        print(f"{name[:40]:<40} {urgency:<9} {n:>4} {sla_pct:>7.0%} {avg_days:>9.2f}")
    print(f"\n(categories with >= {args.min_classified} classified requests; "
          f"SLA thresholds from dbt_project.yml vars)")


if __name__ == "__main__":
    main()
