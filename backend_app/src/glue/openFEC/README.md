# openFEC Glue indexing

Materializes FEC profiles into DynamoDB and schedule line items into S3. **Run via Step Functions** (recommended) or directly as a Glue job.

## Step Functions input

All fields are optional except `mode` for clarity; Glue derives `cycle` from today's date when `cycle` is null.

### Nightly (scheduler — 23:00 UTC)

```json
{
  "source": "scheduler-nightly",
  "mode": "nightly",
  "cycle": null,
  "min_receipt_date": null,
  "testing_limit": null,
  "entity_type": null,
  "entity_id": null
}
```

### Bootstrap (manual)

```json
{
  "source": "manual",
  "mode": "bootstrap",
  "cycle": 2026,
  "min_receipt_date": null,
  "testing_limit": null,
  "entity_type": null,
  "entity_id": null
}
```

### Single entity (smoke test)

```json
{
  "source": "manual",
  "mode": "single",
  "cycle": 2026,
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

One field: a JSON array of openFEC API keys (round-robin per request).

```json
{
  "api_keys": ["key-one", "key-two"]
}
```

In the AWS console you can use a real JSON array. Terraform’s placeholder stores `api_keys` as a stringified array because the secrets module only accepts `map(string)` values.

## Scheduler

EventBridge rule `openfec-indexing-nightly` sends the fixed nightly JSON above at **23:00 UTC**. It is **`enabled = false`** in Terraform until bootstrap completes.

## Storage

- DynamoDB `fec-profiles`: `PK=CANDIDATE#…` / `COMMITTEE#…`, `SK=PROFILE#{cycle}`
- S3 `fec-data`: `{cycle}/committee/{id}/schedule_a.json.gz` (and `schedule_b`, `schedule_e` for super PACs)
