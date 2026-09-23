"""Export every classified row from the local DuckDB warehouse to the replay fixture.

The fixture is what lets a stranger run the whole pipeline with no API key:
real public Chicago 311 records plus the labels Claude Haiku 4.5 actually
assigned them, replayed by classifier.replay_classifier.

    python scripts/export_fixture.py            # -> data/fixtures/chicago_311_classified.jsonl
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Let `python scripts/<name>.py` import the project packages from a fresh clone.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import structlog

from classifier.replay_classifier import DEFAULT_FIXTURE
from warehouse.duckdb_writer import DuckDBWriter

log = structlog.get_logger(__name__)


def export_fixture(out_path: str = DEFAULT_FIXTURE) -> int:
    records = DuckDBWriter().fetch_classified()
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8", newline="\n") as fh:
        for rec in records:
            fh.write(rec.model_dump_json() + "\n")
    log.info("fixture_exported", path=str(out), records=len(records))
    return len(records)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", default=DEFAULT_FIXTURE, help=f"Output path (default: {DEFAULT_FIXTURE}).")
    export_fixture(parser.parse_args().out)


if __name__ == "__main__":
    main()
