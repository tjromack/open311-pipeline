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
