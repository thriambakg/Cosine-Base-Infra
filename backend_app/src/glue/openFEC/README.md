# openFEC Glue indexing

Materializes FEC profiles into DynamoDB and schedule line items into S3. **Run via Step Functions** (recommended) or directly as a Glue job.

## Step Functions input

All fields are optional except `mode` for clarity. **Use empty strings `""` for unused fields — not JSON `null`.** Glue job arguments must be strings; `cycle` may be `"2026"` or `2026` (Step Functions coerces via `States.Format`).

Glue derives `cycle` from today's date when `cycle` is `""`.

### Nightly (scheduler — 23:00 UTC)

```json
{
  "source": "scheduler-nightly",
  "mode": "nightly",
  "cycle": "",
  "min_receipt_date": "",
  "testing_limit": "",
  "entity_type": "",
  "entity_id": ""
}
```

### Bootstrap (manual)

```json
{
  "source": "manual",
  "mode": "bootstrap",
  "cycle": "2026",
  "min_receipt_date": "",
  "testing_limit": "10",
  "entity_type": "",
  "entity_id": ""
}
```

### Single entity (smoke test)

```json
{
  "source": "manual-test",
  "mode": "single",
  "cycle": "2026",
  "min_receipt_date": "",
  "testing_limit": "",
  "entity_type": "candidate",
  "entity_id": "H2KY04121"
}
```

### Fields

| Field | Description |
|-------|-------------|
| `source` | Provenance label (logged only) |
| `mode` | `bootstrap`, `nightly`, or `single` |
| `cycle` | FEC cycle year (e.g. `2026`); null → derived from date |
| `min_receipt_date` | Nightly delta; null → yesterday UTC |
| `testing_limit` | Cap entities processed |
| `entity_type` / `entity_id` | Required for `single` |

## AWS CLI examples

```bash
# Bootstrap (after terraform apply + secret populated)
aws stepfunctions start-execution \
  --state-machine-arn <openfec-indexing-state-machine-arn> \
  --input '{"source":"manual","mode":"bootstrap","cycle":2026}'

# Nightly-style run
aws stepfunctions start-execution \
  --state-machine-arn <arn> \
  --input '{"source":"manual","mode":"nightly","cycle":2026}'

# Single candidate
aws stepfunctions start-execution \
  --state-machine-arn <arn> \
  --input '{"source":"manual","mode":"single","cycle":2026,"entity_type":"candidate","entity_id":"H2KY04121"}'
```

## Secrets (`fec-api`)

One field: a JSON array of openFEC API keys. Worker threads are pinned to keys (`worker_id % len(keys)`), each with its own hourly rate limiter.

```json
{
  "api_keys": ["key-one", "key-two", "..."]
}
```

In the AWS console you can use a real JSON array. Terraform’s placeholder stores `api_keys` as a stringified array because the secrets module only accepts `map(string)` values.

## Parallelism and API keys

The Glue **driver** uses a thread pool for I/O-bound openFEC calls (extra Glue `number_of_workers` does not help HTTP here).

| Glue arg | Default | Meaning |
|----------|---------|---------|
| `MAX_CALLS_PER_HOUR_PER_KEY` | 900 | Per-key sliding window (openFEC hard limit: 1,000/hr/key) |
| `WORKERS_PER_KEY` | 2 | Threads = `min(keys × this, 24, entity_count)` |
| `MAX_PARALLEL_WORKERS` | 24 | Cap on concurrent entity indexes |
| `RATE_LIMIT_DELAY` | 0.1 | Extra sleep after each call (seconds) |
| `REQUEST_TIMEOUT` | 120 | Default HTTP timeout (seconds) |
| `SCHEDULE_REQUEST_TIMEOUT` | 180 | Timeout for `/schedules/schedule_*` paths |
| `REQUEST_RETRIES` | 4 | Retries; timeouts use exponential backoff (5s → 90s cap) |

At startup the job logs parallelism, hourly API budget, and a rough ETA.

### How many keys?

| Keys | ~calls/hour | Notes |
|------|-------------|--------|
| 1 | 900 | OK for nightly; slow bootstrap |
| 10 | 9,000 | Solid default for full bootstrap |
| 15–20 | 13.5k–18k | Usually enough; thread cap is 24 |

You do not need dozens of keys. Add keys only if logs show **HTTP 429** or threads waiting on the limiter. Register separate keys at [api.data.gov](https://api.data.gov/signup/) (same email can hold multiple).

Logs include entity and pagination context, e.g. `[key 2/10 candidate/H2KY04121] FEC /schedules/schedule_a/ page 4/12 — ok`.

## Scheduler

EventBridge rule `openfec-indexing-nightly` sends the fixed nightly JSON above at **23:00 UTC**. It is **`enabled = false`** in Terraform until bootstrap completes.

## Storage

- DynamoDB `fec-profiles`: `PK=CANDIDATE#…` / `COMMITTEE#…`, `SK=PROFILE#{cycle}`
- S3 `fec-data`: `{cycle}/committee/{id}/schedule_a.json.gz` (and `schedule_b`, `schedule_e` for super PACs)
