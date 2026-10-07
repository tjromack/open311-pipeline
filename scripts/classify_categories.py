"""Classify each portal service type once, writing dbt_project/seeds/portal_sr_types.csv.

A full fiscal year is ~2.1M requests; classifying each one at the measured ~$0.0018/request
would cost ~$3,800. The blind evaluation (eval/RESULTS.md) showed the urgency label is largely
determined by the category, so here each sr_type is classified once, with the same model and
the same v1 rubric, and every request inherits its type's tier.

The category-level labels are then compared with the per-request labels in the 300-row replay
fixture, for every category the two share, and the agreement is printed.

Some types are not service work and are kept out of SLA scope (they still land and are counted):
see OUT_OF_SCOPE below.

    python scripts/classify_categories.py            # needs ANTHROPIC_API_KEY (~105 calls)
    python scripts/classify_categories.py --compare  # agreement check only, no API calls
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

# Let `python scripts/<name>.py` import the project packages from a fresh clone.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import duckdb
from dotenv import load_dotenv

from classifier.prompts import PROMPT_VERSION
from classifier.replay_classifier import load_fixture

load_dotenv()

SEED = Path(__file__).resolve().parents[1] / "dbt_project" / "seeds" / "portal_sr_types.csv"
FIELDS = ["sr_type", "sr_short_code", "in_sla_scope", "scope_note", "urgency_label",
          "urgency_score", "llm_reasoning", "prompt_version", "classified_at"]

# Types that land in the warehouse but are closed at intake rather than completed by a
# department. Rule, from the FY2026 extract: 90% of the type's requests close within six
# minutes of creation. Exactly these four types meet it; the next fastest type is far slower.
# Counting them would add ~1.19M instant "met" results to the SLA marts.
OUT_OF_SCOPE = {
    "311 INFORMATION ONLY CALL": "closed at intake (FY2026 p90 0.0 h); information call, no service",
    "Aircraft Noise Complaint": "closed at intake (FY2026 p90 0.0 h); routed to the airport noise program",
    "Tree Trim Request (NO LONGER BEING ACCEPTED)": "closed at intake (FY2026 p90 0.003 h); request type retired",
    "Finance Parking Code Enforcement Review": "closed at intake (FY2026 p90 0.0 h); administrative review record",
}


def portal_types(db_path: str) -> list[tuple[str, str]]:
    with duckdb.connect(db_path, read_only=True) as c:
        return c.execute(
            """SELECT sr_type, any_value(sr_short_code) FROM raw.portal_requests
               WHERE sr_type IS NOT NULL GROUP BY sr_type ORDER BY sr_type""").fetchall()


def classify(types: list[tuple[str, str]]) -> list[dict]:
    from classifier.urgency_classifier import UrgencyClassifier
    from ingestion.schemas import ServiceRequest

    clf = UrgencyClassifier()
    out = []
    for sr_type, code in types:
        row = {"sr_type": sr_type, "sr_short_code": code or "", "prompt_version": PROMPT_VERSION,
               "classified_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}
        if sr_type in OUT_OF_SCOPE:
            row.update(in_sla_scope="false", scope_note=OUT_OF_SCOPE[sr_type],
                       urgency_label="", urgency_score="", llm_reasoning="")
        else:
            req = ServiceRequest(
                service_request_id=f"category:{code or sr_type}",
                requested_datetime=datetime.now(timezone.utc),
                service_name=sr_type, service_code=code or "", status="open",
                address="(category-level classification; no specific address)",
                raw_payload={},
            )
            res = clf.classify(req)
            row.update(in_sla_scope="true", scope_note="", urgency_label=res.urgency_label,
                       urgency_score=f"{res.urgency_score:.2f}", llm_reasoning=res.llm_reasoning)
        out.append(row)
        print(f"{row['urgency_label'] or '-':<9} {sr_type}")
    return out


def compare() -> None:
    """Category label vs. the modal per-request label in the replay fixture."""
    with SEED.open(encoding="utf-8") as fh:
        cat = {r["sr_type"]: r["urgency_label"] for r in csv.DictReader(fh) if r["urgency_label"]}
    by_type: dict[str, Counter] = {}
    for rec in load_fixture():
        by_type.setdefault(rec.service_name, Counter())[rec.urgency_label] += 1
    shared = sorted(set(cat) & set(by_type))
    req_agree = sum(by_type[t][cat[t]] for t in shared)
    req_total = sum(sum(by_type[t].values()) for t in shared)
    modal_agree = sum(by_type[t].most_common(1)[0][0] == cat[t] for t in shared)
    print(f"categories shared with the fixture: {len(shared)} of {len(cat)} in-scope types")
    print(f"category label == modal per-request label: {modal_agree}/{len(shared)}")
    print(f"per-request labels matching their category label: {req_agree}/{req_total} "
          f"({req_agree / req_total:.0%})")
    for t in shared:
        modal = by_type[t].most_common(1)[0][0]
        if modal != cat[t]:
            print(f"  differs: {t}: category={cat[t]} fixture={dict(by_type[t])}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--compare", action="store_true", help="Only run the fixture agreement check.")
    args = parser.parse_args()
    if not args.compare:
        rows = classify(portal_types(os.environ.get("DUCKDB_PATH") or "data/civic_311.duckdb"))
        SEED.parent.mkdir(parents=True, exist_ok=True)
        with SEED.open("w", encoding="utf-8", newline="\n") as fh:
            w = csv.DictWriter(fh, fieldnames=FIELDS)
            w.writeheader()
            w.writerows(rows)
        print(f"wrote {len(rows)} types to {SEED}")
    compare()


if __name__ == "__main__":
    main()
