"""Write dbt_project/seeds/fiscal_calendar.csv: one row per day, federal fiscal attributes.

The federal fiscal year runs Oct 1 - Sep 30 and is named for the calendar year it ends in
(FY2026 = 2025-10-01 .. 2026-09-30). Fiscal quarter 1 = Oct-Dec. Weeks start on Monday.
A seed rather than a generated spine keeps the models engine-neutral (DuckDB and Snowflake
build date series differently).

    python scripts/make_fiscal_calendar.py [--first-fy 2025] [--last-fy 2027]
"""

from __future__ import annotations

import argparse
import csv
from datetime import date, timedelta
from pathlib import Path

OUT = Path(__file__).resolve().parents[1] / "dbt_project" / "seeds" / "fiscal_calendar.csv"


def rows(first_fy: int, last_fy: int):
    day, end = date(first_fy - 1, 10, 1), date(last_fy, 9, 30)
    while day <= end:
        fy = day.year + 1 if day.month >= 10 else day.year
        fiscal_month = (day.month - 10) % 12 + 1          # Oct = 1 ... Sep = 12
        week_start = day - timedelta(days=day.weekday())  # Monday
        yield {
            "calendar_date": day.isoformat(),
            "fiscal_year": fy,
            "fiscal_quarter": (fiscal_month - 1) // 3 + 1,
            "fiscal_month": fiscal_month,
            "month_start": day.replace(day=1).isoformat(),
            "week_start": week_start.isoformat(),
            "week_end": (week_start + timedelta(days=6)).isoformat(),
            "day_of_week": day.isoweekday(),
        }
        day += timedelta(days=1)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--first-fy", type=int, default=2025)
    parser.add_argument("--last-fy", type=int, default=2027)
    a = parser.parse_args()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    data = list(rows(a.first_fy, a.last_fy))
    with OUT.open("w", encoding="utf-8", newline="\n") as fh:
        w = csv.DictWriter(fh, fieldnames=list(data[0]))
        w.writeheader()
        w.writerows(data)
    print(f"wrote {len(data)} days ({a.first_fy}..FY{a.last_fy}) to {OUT}")


if __name__ == "__main__":
    main()
