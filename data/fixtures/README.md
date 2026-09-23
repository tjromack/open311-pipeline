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
make classify LIMIT=300 SEED=311  # needs ANTHROPIC_API_KEY (~$0.20)
make export-fixture
```

The API returns a moving window, so a rebuild produces a different sample.
The committed file is the one every published number was computed from.
