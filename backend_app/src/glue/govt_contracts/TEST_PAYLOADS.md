# USAspending Bulk Indexing Glue Job - Test Payloads

## Overview

The Glue job can run for up to **48 hours** (2880 minutes) and supports backdating by accepting optional `START_DATE` and `END_DATE` parameters.

## Job Capabilities

- ✅ **Long-running**: Timeout set to 2880 minutes (48 hours) - can process large date ranges
- ✅ **Backdating**: Accepts `START_DATE` and `END_DATE` parameters (YYYY-MM-DD format)
- ✅ **Default behavior**: If no dates provided, defaults to yesterday
- ✅ **Date range validation**: Validates date formats and ensures END_DATE >= START_DATE

## Test Payloads

### 1. Default Behavior (Yesterday - No Parameters)

**Use Case**: Daily scheduled run (default behavior)

```json
{
  "JobName": "cosine-usaspending-bulk-indexing-production",
  "AWARDS_TABLE_NAME": "cosine-usaspending-awards-index-production",
  "S3_BUCKET_NAME": "cosine-usaspending-data-production"
}
```

**Expected**: Processes contracts from yesterday only

---

### 2. Single Day Backdate

**Use Case**: Index contracts from a specific past date

```json
{
  "JobName": "cosine-usaspending-bulk-indexing-production",
  "AWARDS_TABLE_NAME": "cosine-usaspending-awards-index-production",
  "S3_BUCKET_NAME": "cosine-usaspending-data-production",
  "START_DATE": "2024-01-15",
  "END_DATE": "2024-01-15"
}
```

**Expected**: Processes all contracts from January 15, 2024

---

### 3. Date Range Backdate (7 Days)

**Use Case**: Index contracts from a week-long period

```json
{
  "JobName": "cosine-usaspending-bulk-indexing-production",
  "AWARDS_TABLE_NAME": "cosine-usaspending-awards-index-production",
  "S3_BUCKET_NAME": "cosine-usaspending-data-production",
  "START_DATE": "2024-01-01",
  "END_DATE": "2024-01-07"
}
```

**Expected**: Processes all contracts from January 1-7, 2024 (7 days)

---

### 4. Date Range Backdate (30 Days - Long Running Test)

**Use Case**: Test long-running job with a month of data

```json
{
  "JobName": "cosine-usaspending-bulk-indexing-production",
  "AWARDS_TABLE_NAME": "cosine-usaspending-awards-index-production",
  "S3_BUCKET_NAME": "cosine-usaspending-data-production",
  "START_DATE": "2024-01-01",
  "END_DATE": "2024-01-30"
}
```

**Expected**: Processes all contracts from January 2024 (30 days) - may take several hours

---

### 5. Date Range Backdate (90 Days - Extended Test)

**Use Case**: Test maximum capacity with a quarter of data

```json
{
  "JobName": "cosine-usaspending-bulk-indexing-production",
  "AWARDS_TABLE_NAME": "cosine-usaspending-awards-index-production",
  "S3_BUCKET_NAME": "cosine-usaspending-data-production",
  "START_DATE": "2023-10-01",
  "END_DATE": "2023-12-31"
}
```

**Expected**: Processes all contracts from Q4 2023 (92 days) - will likely take many hours, good for testing timeout and capacity

---

### 6. Recent Date Range (Last 7 Days)

**Use Case**: Catch up on recent contracts

```json
{
  "JobName": "cosine-usaspending-bulk-indexing-production",
  "AWARDS_TABLE_NAME": "cosine-usaspending-awards-index-production",
  "S3_BUCKET_NAME": "cosine-usaspending-data-production",
  "START_DATE": "2024-12-20",
  "END_DATE": "2024-12-26"
}
```

**Note**: Update dates to recent dates when testing

---

## How to Test

### Via AWS Console (Step Functions)

1. Go to AWS Step Functions Console
2. Find the state machine: `cosine-usaspending-bulk-indexing-production`
3. Click "Start execution"
4. Paste one of the test payloads above
5. Click "Start execution"

### Via AWS CLI

```bash
aws stepfunctions start-execution \
  --state-machine-arn "arn:aws:states:REGION:ACCOUNT:stateMachine:cosine-usaspending-bulk-indexing-production" \
  --input '{
    "JobName": "cosine-usaspending-bulk-indexing-production",
    "AWARDS_TABLE_NAME": "cosine-usaspending-awards-index-production",
    "S3_BUCKET_NAME": "cosine-usaspending-data-production",
    "START_DATE": "2024-01-01",
    "END_DATE": "2024-01-07"
  }'
```

### Direct Glue Job Invocation (Alternative)

You can also invoke the Glue job directly with arguments:

```bash
aws glue start-job-run \
  --job-name "cosine-usaspending-bulk-indexing-production" \
  --arguments '{
    "--USASPENDING_BASE_URL": "https://api.usaspending.gov",
    "--USASPENDING_USER_AGENT": "Cosine Financial Platform (contact@cosine.financial)",
    "--AWARDS_TABLE_NAME": "cosine-usaspending-awards-index-production",
    "--S3_BUCKET_NAME": "cosine-usaspending-data-production",
    "--REQUEST_TIMEOUT": "30",
    "--START_DATE": "2024-01-01",
    "--END_DATE": "2024-01-07"
  }'
```

## Job Execution Flow

1. **Date Range Processing**: Validates and processes START_DATE to END_DATE
2. **Bulk Download Initiation**: Initiates bulk download for the date range
3. **Polling**: Polls download status (max 1 hour wait, 30s intervals)
4. **CSV Download & Parse**: Downloads CSV and extracts award IDs
5. **Award Indexing**: For each award:
   - Checks if already indexed (skips if complete)
   - Fetches full award details
   - Fetches all transactions (paginated)
   - Fetches all subawards (paginated)
   - Stores award metadata in DynamoDB
   - Stores combined transactions/subawards in S3 (gzipped JSON)
6. **Progress Logging**: Logs progress every 100 awards
7. **Completion**: Reports total indexed/skipped counts

## Monitoring

- **CloudWatch Logs**: `/aws/glue/jobs/cosine-usaspending-bulk-indexing-production`
- **Step Functions Execution**: View in Step Functions console
- **DynamoDB**: Check `cosine-usaspending-awards-index-production` table
- **S3**: Check `cosine-usaspending-data-production/award-details/` prefix

## Notes

- The job will **skip** awards that are already fully indexed (checks `full_indexing_complete` or `award_details_s3_key`)
- Large date ranges will take significant time - monitor CloudWatch logs for progress
- The job will **fail and stop** if any API call fails (no partial completion)
- Bulk downloads can take time to prepare - the job polls for up to 1 hour


