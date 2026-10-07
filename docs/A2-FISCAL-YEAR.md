# A2 — One full fiscal year from the city portal, and the SLA-bias fix

Working notes for the playbook v3 add-on A2 (page 21). Updated as the work lands.

## Goal (from the playbook)

- Load one **federal fiscal year** from the Chicago Data Portal (dataset `v6vf-nfxy`), paged with a
  stable sort order. Assert the landed row count equals the portal's own count.
- **Fix the closed-only SLA bias**: a cohort version where still-open requests count as not met,
  published side by side with the closed-only version.
- **Backlog by week** using date-range overlap, with open-ended (still-open) requests handled.
- **Duplicates and parents**: resolve parent links; document that some orphans are an artifact of
  the extract window, not bad data.
- **Re-pull dedup**: a second pull of the last 30 days, collapsed to the latest version per request
  with a deterministic tiebreak.

Verification required: landed count = portal count · cohort denominators = created counts ·
monthly running totals reconcile to the monthly group-by · fiscal quarters sum to the year.

## Source facts (probed 2026-10-06)

| Fact | Value |
|---|---|
| Dataset | Chicago 311 Service Requests, `https://data.cityofchicago.org/resource/v6vf-nfxy` (Socrata SODA) |
| Fiscal year | Federal FY2026 = 2025-10-01 00:00 through 2026-09-30 23:59:59 (Chicago local time, as published) |
| Rows created in FY2026 | 2,124,780 (`$select=count(*)`) |
| Distinct `sr_type` | 104 |
| Largest types | 311 INFORMATION ONLY CALL 682,153 · Aircraft Noise Complaint 446,184 · Graffiti Removal Request 96,820 · Pothole in Street Complaint 59,767 · Abandoned Vehicle Complaint 52,987 |
| Fields the Open311 API lacks | `closed_date`, `owner_department`, `duplicate`, `parent_sr_number`, `legacy_record`, `origin`, `last_modified_date` |
| Status values | e.g. `Completed`, `Open`, `Canceled` (portal), not Open311's `open`/`closed` |

Portal aggregate queries are slow (a grouped count timed out at 300 s), so profiling happens
locally after the load, not against the API.

## Decisions

- **Land everything, scope later.** All 2.1M rows land so the count assertion is exact. The SLA
  marts exclude non-service types (information-only calls, aircraft noise) through a versioned
  seed, not a `WHERE` buried in SQL.
- **Keyset paging on `sr_number`** (`$where sr_number > :last ... $order sr_number`), not
  `$offset`. It is stable while the dataset changes and doesn't degrade at deep offsets.
- **Raw is append-only versions.** `raw.portal_requests` keeps one row per request *per pull*;
  `raw.portal_pulls` records each pull's window, the portal's count before and after, and the
  landed count. Staging collapses to the latest version per `sr_number`.
- **Tiebreak:** `last_modified_date desc, pulled_at desc, pull_id desc`, which is total and
  deterministic.
- **Urgency for 2.1M rows comes from the category, not per request.** Per-request classification
  would cost ~$3,800 at the measured $0.0018/request. The blind evaluation showed labels are
  largely category-determined, so each in-scope `sr_type` is classified once with the same model
  and rubric. The category labels are checked against the 300 per-request labels where the
  categories overlap, and the agreement is published.
- **Cohort SLA states:** `met` (closed within threshold), `missed` (closed late, or still open past
  the deadline at the as-of time), `pending` (still open, deadline after the as-of time; excluded
  from the rate and counted separately). `met + missed + pending = created`.
- **DuckDB only for the portal path.** The models stay ANSI so the Snowflake target still parses,
  but the loader writes DuckDB.

## Results (FY2026, extract as of 2026-10-06 08:24 Chicago time)

All numbers from `make portal-report` and the queries recorded in the progress log.

**The load.** 2,124,780 requests created in FY2026, landed in 365 day partitions; every partition
verified (portal count before = after = landed). Re-pull of the last 30 days: 182,600 rows, 30/30
verified. Raw holds 2,307,380 versions; staging keeps 2,124,780 (one per request).

**The SLA fix, headline.**

| | Requests | Compliance |
|---|---:|---:|
| Closed-only (the original method) | 711,810 closed | **51.0%** |
| Cohort (still-open past deadline = missed) | 810,671 due | **44.8%** |
| Bias of closed-only | | **+6.2 pct points** |

The cohort is 812,036 in-scope requests (362,993 met, 447,678 missed, 1,365 pending). 1,312,744 are
excluded: 1,191,982 of four types closed at intake, 91,600 duplicates (tracked under the parent),
and 29,162 canceled. Of the 100 in-scope categories, **85 look better closed-only and none look
worse**. The bias is one-directional, as the mechanism predicts.

By tier (category-level labels):

| Tier | Cohort | Closed-only | Cohort | Pending | Missed while still open |
|---|---:|---:|---:|---:|---:|
| Low (168 h) | 385,399 | 63.2% | 51.1% | 1,365 | 73,433 |
| Medium (72 h) | 213,947 | 45.7% | 41.9% | 0 | 17,846 |
| High (24 h) | 200,856 | 38.2% | 36.9% | 0 | 6,626 |
| Critical (4 h) | 11,834 | 28.4% | 26.1% | 0 | 956 |

The bias is largest for **Low**: the long window lets slow requests sit open, which is exactly
the population closed-only drops. Largest category gaps (≥1,000 cohort requests): Stray Animal
Complaint +18.1 pp, Sewer Cleaning Inspection +13.2, Inspect Public Way +12.9, Pet Wellness Check
+12.7, Alley Sewer Inspection +11.4.

**Rodent baiting at scale.** The 300-row sample's "0 of 4 within 24 h" lead, measured on the full
year: 41,502 cohort requests, **18.1%** within the High (24 h) window (closed-only 18.5%). The
category label is the model's (High); the blind hand-label check put rodent baiting at Medium,
so the finding still depends on that tier.

**By department** (cohort ≥ 10,000; closed-only → cohort): Streets and Sanitation 51.9% → 47.3%
(482,788) · CDOT 53.7% → 45.3% (164,097) · Water Management 46.6% → 33.7% (64,451) · Buildings
18.5% → 15.1% (41,114) · Animal Care and Control 57.9% → 53.2% (24,744). The portal's
`owner_department` fills the gap the Open311 path had (`department` mostly NULL).

**Backlog.** In-scope FY-created requests open at week end rose from 6,252 (week ending
2025-10-05) to a peak of 107,911 (2026-09-20); 53 weeks. Flow identity holds every week.

**Parents.** All 91,600 parent links come from duplicates. 86,291 point inside the extract;
3,475 (2,592 distinct parents) point to requests created before the window, found by ID:
**window artifacts, not bad data**. 1,834 links (1,066 distinct parents) point to IDs the public
portal doesn't return even when queried one at a time; 766 of those carry FY2026 numbers, so the
window doesn't explain them. *Inference:* the parent records aren't published (the children are
mostly water and street categories).

**Re-pull dedup, and stale reads.** 182,600 requests have two versions. The re-pull's version
won for 182,569. For the other **31**, the re-pull (taken 25 minutes after the base pull) served
an *older* record (`Open`, last modified in September) where the base pull already had it
`Completed` with a closed date. The tiebreak ranks `last_modified_date` first, so staging keeps
the Completed version; "latest pull wins" would have silently reopened them.

**Category labels vs. per-request labels.** 44 of the 100 in-scope types appear in the 300-row
fixture; the category label equals the modal per-request label for 40 of 44 (two of the four
differences are 2–2 ties), and matches 232 of 240 per-request labels (97%).

**Data quality.** Every Completed and Canceled request has a `closed_date`; no Open one does. One
request closes before it was created (SR26-00411461, same day, 33 minutes), flagged by a
warn-level test and kept.

**Build.** `dbt build` on the full year: 58 nodes, PASS=57 WARN=1 ERROR=0 (the warn is the one
closed-before-created request, by design), in ~20 s. pytest: 61 passed.

## What this lets the project claim, and what it doesn't

- Claims: a reproducible, count-verified full fiscal year; a cohort SLA measure with the bias
  quantified against the closed-only one; reconciliations that hold on 2.1M rows.
- Does not claim: per-request urgency for the full year (tiers are per category); Chicago's own
  SLAs (thresholds are the project's); a final FY picture (1,365 requests are still pending as of
  the extract, and later re-pulls will change open/closed states); anything about the 1,066
  unpublished parents beyond their absence.

## Progress log

- 2026-10-06 — Branch `a2-fiscal-year`. Portal probed (above).
- 2026-10-06 — **Paging changed from keyset-on-`sr_number` to one partition per day.** Ordering
  the year by `sr_number` on the server timed out (a 1,000-row page exceeded 300 s), while a
  day-filtered request returns ~6,400 rows in ~1 s and its count in ~4 s. Each day is fetched
  with a stable `created_date, sr_number` sort, and each day is its own reconciliation unit:
  the portal count before and after paging must equal the rows landed.
- 2026-10-06 — Loader (`ingestion/portal_loader.py`) + DDL + 8 tests (count agreement,
  mismatch recorded and raised, paging past the page size, duplicate-in-pull rejected, parent
  lookup, retry on 503, no retry on 400). Writing the tests caught a real bug: the page limit
  was a default argument frozen at definition time, so paging would ignore a changed page size.
  Smoke pull of two days: 6,436 + 6,477 rows, both verified.
- 2026-10-06 — dbt layer: `fiscal_calendar` seed (FY2025–FY2027), `stg_portal_requests`
  (latest verified version), `int_portal_cohort` (SLA states), `int_portal_parent_links`, marts
  `fct_portal_sla_monthly`, `fct_portal_sla_by_category`, `fct_portal_volume_monthly`,
  `fct_portal_backlog_weekly`, and seven singular tests: the four the playbook names plus full
  FY coverage, latest-version-wins and the backlog flow identity.
- 2026-10-06 — Full FY2026 pull started (365 partitions, ~1 day per 2 s after warm-up).
- 2026-10-06 — Merged to `main` (CI green: 61 tests, dbt 58/58 on the fixture). Scheduled the
  follow-up re-pull: Windows Task Scheduler task **"Open311 portal re-pull (A2)"**, once, Tue
  2026-10-13 09:00 (runs at next availability if the PC is off), executing
  `scripts/scheduled_repull.ps1`: re-pull last 30 days → resolve parents → `dbt build` → report,
  logged to `logs/scheduled_repull_<timestamp>.log`. Success: Windows notification. Failure (any
  step): Windows notification + a GitHub issue on the repo with the step and log tail. Both paths
  tested on a scratch copy of the warehouse (success in 30 s; failure via `-SimulateFailure
  -DryRunIssue`).
