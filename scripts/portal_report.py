"""Print the fiscal-year SLA side-by-side (closed-only vs cohort) and the backlog from DuckDB.

Reads the portal marts after `dbt build`. Every number in docs/A2-FISCAL-YEAR.md's results
section comes from here.

    python scripts/portal_report.py [--min-requests 500] [--top 15]
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

M = "ANALYTICS_marts"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--min-requests", type=int, default=500,
                        help="Only categories with at least this many cohort requests (default 500).")
    parser.add_argument("--top", type=int, default=15)
    args = parser.parse_args()
    path = os.environ.get("DUCKDB_PATH") or DEFAULT_DUCKDB_PATH

    with duckdb.connect(path, read_only=True) as c:
        tot = c.execute(f"""
            select sum(created_requests), sum(excluded_requests), sum(met), sum(missed), sum(pending),
                   sum(closed_requests), sum(closed_within_sla), min(fiscal_year)
            from {M}.fct_portal_sla_monthly""").fetchone()
        created, excluded, met, missed, pending, closed, closed_ok, fy = tot
        as_of = c.execute("select max(as_of_ts) from ANALYTICS_intermediate.int_portal_cohort").fetchone()[0]
        excl = c.execute("""select exclusion_reason, count(*) from ANALYTICS_intermediate.int_portal_cohort
                            where sla_state = 'excluded' group by 1 order by 2 desc""").fetchall()
        rows = c.execute(f"""
            select sr_type, met + missed + pending as cohort, pending, closed_only_sla_pct,
                   cohort_sla_pct, bias_pct_points
            from {M}.fct_portal_sla_by_category
            where met + missed + pending >= ?
            order by bias_pct_points desc nulls last
            limit ?""", [args.min_requests, args.top]).fetchall()
        backlog = c.execute(f"""
            select week_end, sum(open_at_week_end), sum(opened_in_week), sum(closed_in_week)
            from {M}.fct_portal_backlog_weekly group by 1 order by 1""").fetchall()

    print(f"\nWarehouse: {path}   ·   federal FY{fy}   ·   as of {as_of}")
    print(f"  created in FY:          {created:,}")
    print(f"  excluded:               {excluded:,}  ({', '.join(f'{r} {n:,}' for r, n in excl)})")
    print(f"  in SLA cohort:          {met + missed + pending:,}  (met {met:,} · missed {missed:,} · pending {pending:,})")
    print(f"  closed-only compliance: {closed_ok / closed:.1%}  ({closed_ok:,} of {closed:,} closed)")
    print(f"  cohort compliance:      {met / (met + missed):.1%}  ({met:,} of {met + missed:,} due)")
    print(f"  bias of closed-only:    {100 * (closed_ok / closed - met / (met + missed)):+.1f} pct points\n")

    hdr = f"{'Category':<44} {'cohort':>8} {'pending':>8} {'closed-only':>12} {'cohort':>8} {'bias pp':>8}"
    print(f"Largest closed-only bias (categories with >= {args.min_requests:,} cohort requests)")
    print(hdr)
    print("-" * len(hdr))
    for name, n, pend, co, ch, bias in rows:
        print(f"{name[:44]:<44} {n:>8,} {pend:>8,} {co:>12.1%} {ch:>8.1%} {bias:>+8.1f}")

    print("\nBacklog of in-scope FY requests (open at week end), every 4th week")
    for i, (wk, open_n, opened, closed_n) in enumerate(backlog):
        if i % 4 == 0 or i == len(backlog) - 1:
            print(f"  {wk}  open {open_n:>8,}   opened {opened:>7,}   closed {closed_n:>7,}")


if __name__ == "__main__":
    main()
