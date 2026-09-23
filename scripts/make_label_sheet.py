"""Draw a seeded random sample from the replay fixture into a blind labeling sheet.

The sheet carries only the fields the classifier's prompt sees (service_name,
status, address) and never the model's label, so human labels aren't anchored
to it. See eval/LABELING_GUIDE.md.

    python scripts/make_label_sheet.py [--n 50] [--seed 20260923]
"""

from __future__ import annotations

import argparse
import csv
import random
import sys
from pathlib import Path

# Let `python scripts/<name>.py` import the project packages from a fresh clone.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from classifier.replay_classifier import DEFAULT_FIXTURE, load_fixture

DEFAULT_SHEET = "eval/labels/label_sheet.csv"
SHEET_COLUMNS = ["item", "service_request_id", "service_name", "status", "address", "human_label", "notes"]


def make_sheet(n: int, seed: int, fixture: str, out: str, force: bool = False) -> Path:
    out_path = Path(out)
    if out_path.exists() and not force:
        raise SystemExit(f"{out_path} already exists; refusing to overwrite labels (use --force).")

    records = sorted(load_fixture(fixture), key=lambda r: r.service_request_id)
    sample = random.Random(seed).sample(records, n)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(SHEET_COLUMNS)
        for i, rec in enumerate(sample, start=1):
            writer.writerow([i, rec.service_request_id, rec.service_name, rec.status, rec.address, "", ""])
    print(f"wrote {n} blind rows to {out_path} (seed={seed}, from {len(records)} in {fixture})")
    return out_path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--n", type=int, default=50)
    parser.add_argument("--seed", type=int, default=20260923)
    parser.add_argument("--fixture", default=DEFAULT_FIXTURE)
    parser.add_argument("--out", default=DEFAULT_SHEET)
    parser.add_argument("--force", action="store_true", help="Overwrite an existing sheet.")
    args = parser.parse_args()
    make_sheet(args.n, args.seed, args.fixture, args.out, args.force)


if __name__ == "__main__":
    main()
