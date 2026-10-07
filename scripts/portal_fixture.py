"""Export / load the committed portal fixture used by CI and the no-accounts demo.

The full fiscal year (~2.1M rows) is too large to commit, so the fixture is EVERY version of
every request, across every verified pull, for a handful of real service types, over the whole
fiscal year. Because whole types are kept (never sampled), each day's row count in the fixture
is exact, and the fixture's pull ledger carries those per-day counts.

Honesty note on the ledger: the fixture's expected counts are derived from the verified full
pull (filtered to the kept types), not re-queried from the portal for that filter. The
full pull's partitions were each verified against the portal's own count.

    python scripts/portal_fixture.py export --types "Wire Down" "Water in Basement Complaint"
    python scripts/portal_fixture.py load      # into $DUCKDB_PATH (CI / demo)
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# Let `python scripts/<name>.py` import the project packages from a fresh clone.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import duckdb

from ingestion.portal_loader import DDL_PATH

FIX = Path(__file__).resolve().parents[1] / "data" / "fixtures"
REQ = FIX / "portal_requests_sample.csv.gz"
PULLS = FIX / "portal_pulls_sample.csv"
PARENTS = FIX / "portal_parent_lookup_sample.csv"


def export(db: str, types: list[str]) -> None:
    with duckdb.connect(db, read_only=True) as c:
        c.execute("CREATE TEMP TABLE keep AS SELECT unnest(?::VARCHAR[]) AS sr_type", [types])
        c.execute(f"""
            COPY (
                SELECT r.* FROM raw.portal_requests r
                JOIN raw.portal_pulls p ON p.pull_id = r.pull_id AND p.partition_date = r.partition_date
                WHERE p.status = 'verified' AND r.sr_type IN (SELECT sr_type FROM keep)
                ORDER BY r.pull_id, r.sr_number
            ) TO '{REQ.as_posix()}' (HEADER, COMPRESSION gzip)""")
        # One ledger row per verified partition, with the kept types' exact landed count.
        c.execute(f"""
            COPY (
                SELECT p.pull_id, p.window_label, p.partition_date,
                       p.where_clause || ' AND sr_type IN (fixture types)' AS where_clause,
                       count(r.sr_number) AS expected_count, count(r.sr_number) AS expected_count_after,
                       count(r.sr_number) AS landed_count, 'verified' AS status, p.started_at, p.finished_at
                FROM raw.portal_pulls p
                LEFT JOIN raw.portal_requests r
                  ON r.pull_id = p.pull_id AND r.partition_date = p.partition_date
                 AND r.sr_type IN (SELECT sr_type FROM keep)
                WHERE p.status = 'verified'
                GROUP BY ALL ORDER BY p.pull_id, p.partition_date
            ) TO '{PULLS.as_posix()}' (HEADER)""")
        c.execute(f"""
            COPY (
                SELECT l.* FROM raw.portal_parent_lookup l
                WHERE l.sr_number IN (
                    SELECT parent_sr_number FROM raw.portal_requests WHERE sr_type IN (SELECT sr_type FROM keep))
                ORDER BY 1
            ) TO '{PARENTS.as_posix()}' (HEADER)""")
        n = c.execute(f"SELECT count(*) FROM read_csv('{REQ.as_posix()}')").fetchone()[0]
    print(f"exported {n:,} request versions for {len(types)} types -> {REQ.name}, {PULLS.name}, {PARENTS.name}")


def load(db: str) -> None:
    Path(db).parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(db) as c:
        c.execute(DDL_PATH.read_text(encoding="utf-8"))
        c.execute("DELETE FROM raw.portal_requests; DELETE FROM raw.portal_pulls; DELETE FROM raw.portal_parent_lookup;")
        c.execute(f"INSERT INTO raw.portal_requests SELECT * FROM read_csv('{REQ.as_posix()}', header=true, auto_detect=true)")
        c.execute(f"INSERT INTO raw.portal_pulls SELECT * FROM read_csv('{PULLS.as_posix()}', header=true, auto_detect=true)")
        if PARENTS.exists():
            c.execute(f"INSERT INTO raw.portal_parent_lookup SELECT * FROM read_csv('{PARENTS.as_posix()}', header=true, auto_detect=true)")
        n = c.execute("SELECT count(*) FROM raw.portal_requests").fetchone()[0]
    print(f"loaded {n:,} portal request versions into {db}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="cmd", required=True)
    e = sub.add_parser("export")
    e.add_argument("--types", nargs="+", required=True)
    sub.add_parser("load")
    args = parser.parse_args()
    db = os.environ.get("DUCKDB_PATH") or "data/civic_311.duckdb"
    export(db, args.types) if args.cmd == "export" else load(db)


if __name__ == "__main__":
    main()
