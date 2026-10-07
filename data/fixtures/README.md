# Replay fixture

`chicago_311_classified.jsonl` holds 300 real Chicago 311 service requests,
each with the urgency label Claude Haiku 4.5 actually assigned it. One
`EnrichedRequest` JSON object per line.

It powers the zero-credential demo (`make demo-local`) and the CI build, and it
is the population the blind hand-label sample is drawn from (`eval/`).

## Provenance

| | |
|---|---|
| Source | City of Chicago Open311 GeoReport v2 API, `GET /requests.json` (public, no key) |
| Window | Requests opened 2026-09-16 15:02 → 2026-09-23 14:50 UTC, `status=closed`, all service codes |
| Population | 7,493 unique requests (`make backfill DAYS=7`) |
| Sample | 300 rows, seeded pseudo-random (`make classify LIMIT=300 SEED=311`) |
| Model | `claude-haiku-4-5-20251001`, temperature 0, structured output, prompt `v1` |
| Classified | 2026-09-23, 300/300 succeeded, 376 s sequential |
| Export | `make export-fixture` |

`raw_payload` is the untouched API record. Addresses are exactly as the city
publishes them. There is no free-text description in Chicago's feed, so the
model saw only `service_name`, `service_code`, `status` and `address`.

## Rebuilding it

```bash
make backfill DAYS=7              # needs network, no keys
make classify LIMIT=300 SEED=311  # needs ANTHROPIC_API_KEY (~$0.54 at ~1,290 in / ~100 out tokens per call)
make export-fixture
```

The API returns a moving window, so a rebuild produces a different sample.
The committed file is the one every published number was computed from.

---

# Portal fiscal-year fixture (add-on A2)

`portal_requests_sample.csv.gz`, `portal_pulls_sample.csv` and `portal_parent_lookup_sample.csv`
feed the portal models in CI and in the no-accounts demo (`python scripts/portal_fixture.py load`).

| | |
|---|---|
| Source | Chicago Data Portal, 311 Service Requests (`v6vf-nfxy`), public |
| Window | Federal FY2026: requests created 2025-10-01 through 2026-09-30, every day |
| Pulls | the full-year pull and the last-30-days re-pull of 2026-10-06 (both 100% verified) |
| Kept | **every** version of every request of five whole service types: Water in Basement Complaint, Vicious Animal Complaint, No Water Complaint, Sign Repair Request - Stop Sign, Street Light Pole Damage Complaint |
| Size | 29,684 request versions (27,736 requests; 1,948 re-pull versions); 1,408 duplicates, 950 open, 2,497 canceled |
| Ledger | 395 partitions (365 + 30 re-pull days) |

Whole types are kept, never sampled, so each day's count is exact. The ledger's expected counts
are **derived** from the verified full pull filtered to these five types. They weren't
re-queried from the portal with that filter. Each partition of the full pull was verified against the
portal's own count.

Rebuild (after `make portal-load`, `make portal-repull`, `make portal-parents`):

```bash
python scripts/portal_fixture.py export --types "Water in Basement Complaint" "Vicious Animal Complaint" \
  "No Water Complaint" "Sign Repair Request - Stop Sign" "Street Light Pole Damage Complaint"
```
