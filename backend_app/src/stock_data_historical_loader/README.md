# Stock Data Historical Loader

## Overview

This Lambda function, orchestrated by AWS Step Functions, loads up to 5 years of historical stock data from Yahoo Finance and stores it in S3 for all stocks in the highcap, midcap, and lowcap CSV files.

## Architecture

```
AWS Console (Manual Start)
  ↓
Step Functions State Machine (Zero-Config)
  ↓
Step 1: Generate Batches (Lambda reads CSVs)
  ↓
Step 2: Process Batches in Parallel (5 concurrent)
  ├─ Batch 1 (150 stocks) → S3
  ├─ Batch 2 (150 stocks) → S3
  ├─ Batch 3 (150 stocks) → S3
  ├─ Batch 4 (150 stocks) → S3
  └─ Batch 5 (150 stocks) → S3
  ↓
Step 3: Aggregate Results
  ↓
Success!
```

## Features

- ✅ **Zero-Configuration Execution**: Just click "Start execution" with no input JSON required
- ✅ **Automatic Batch Generation**: Lambda reads CSV files and creates batches automatically
- ✅ **Parallel Processing**: 5 concurrent batches for faster completion (~2.5 hours total)
- ✅ **Rate Limiting**: 2 requests/second to Yahoo Finance (compliant with API limits)
- ✅ **Automatic Retries**: Built-in retry logic for failed stocks
- ✅ **Error Handling**: Individual batch failures don't stop the entire process
- ✅ **Progress Tracking**: View execution progress in Step Functions console

## Data Loaded

- **High Priority (highcap.csv)**: ~505 stocks (Fortune 500 + top companies)
- **Medium Priority (midcap.csv)**: ~1,335 mid-cap stocks
- **Low Priority (lowcap.csv)**: ~6,508 small-cap stocks
- **Total**: ~8,348 stocks

For each stock:
- 5 years of daily OHLCV (Open, High, Low, Close, Volume) data
- Metadata (symbol, exchange, currency, instrument type)
- ~1,825 data points per stock (365 days × 5 years)

## S3 Storage Structure

```
s3://cosine-stock-historical-{environment}/
  historical/
    high/
      AAPL.json
      MSFT.json
      NVDA.json
      ...
    medium/
      FRHC.json
      GNRC.json
      ...
    low/
      AACB.json
      AACI.json
      ...
```

## Data Format (JSON)

```json
{
  "symbol": "AAPL",
  "currency": "USD",
  "exchange": "NASDAQ",
  "instrument_type": "EQUITY",
  "data_points": 1825,
  "first_date": "2020-10-12T00:00:00",
  "last_date": "2025-10-12T00:00:00",
  "history": [
    {
      "timestamp": 1602460800,
      "date": "2020-10-12T00:00:00",
      "open": 120.50,
      "high": 125.30,
      "low": 119.80,
      "close": 124.40,
      "volume": 89234500
    },
    ...
  ]
}
```

## How to Use

### Step 1: Deploy Infrastructure

```bash
cd Cosine-Base-Infra/terraform
terraform apply
```

This will create:
- S3 bucket for historical data
- Lambda function (historical loader)
- Step Functions state machine
- IAM roles and policies

### Step 2: Run Historical Load (One-Time)

1. Go to AWS Console → Step Functions
2. Find state machine: `cosine-stock-historical-loader-{environment}`
3. Click "Start execution"
4. Leave input JSON **empty** (or use `{}`)
5. Click "Start execution"

That's it! No configuration needed.

### Step 3: Monitor Progress

- **Step Functions Console**: Visual workflow diagram showing progress
- **CloudWatch Logs**: `/aws/stepfunctions/cosine-stock-historical-loader-{environment}`
- **Lambda Logs**: `/aws/lambda/cosine-stock-data-historical-loader-{environment}`

### Expected Duration

- **Total Stocks**: ~8,348
- **Batch Size**: 150 stocks/batch
- **Total Batches**: ~56 batches
- **Concurrency**: 5 batches at a time
- **Duration per Batch**: ~12-15 minutes
- **Total Time**: ~2.5 hours

## Cost Estimate

### One-Time Historical Load

| Component | Cost |
|-----------|------|
| Lambda Execution (56 batches × 15 min × 3008 MB) | $2.50 |
| S3 Storage (8,348 stocks × 100 KB/stock) | $0.02/month |
| Step Functions State Transitions (58 transitions) | $0.001 |
| **Total One-Time** | **$2.50** |
| **Monthly Ongoing** | **$0.02** |

### After Glacier Transition (90 days)

- Storage cost drops to $0.004/month (5x cheaper)

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `S3_BUCKET` | (set by Terraform) | S3 bucket for historical data |
| `RATE_LIMIT` | `2.0` | Requests per second to Yahoo Finance |
| `MAX_WORKERS` | `5` | Parallel threads per Lambda batch |

## Troubleshooting

### Batch Failures

If a batch fails, Step Functions will:
1. Retry 3 times with exponential backoff
2. Mark the batch as failed but continue with other batches
3. Log failed symbols in the batch result

To retry failed symbols:
1. Check Step Functions execution output for `failed_symbols`
2. Create a new execution with:
   ```json
   {
     "symbols": ["FAILED_SYMBOL_1", "FAILED_SYMBOL_2"],
     "priority": "high"
   }
   ```

### Rate Limiting Errors

If you see 429 errors from Yahoo Finance:
1. Reduce `RATE_LIMIT` environment variable (e.g., `1.5`)
2. Reduce Step Functions `MaxConcurrency` (e.g., `3`)
3. Redeploy with `terraform apply`

### Lambda Timeout

If batches are timing out:
1. Reduce batch size in `lambda_function.py` (change `batch_size = 150` to `100`)
2. Redeploy Lambda

## Re-running Historical Load

The historical loader is idempotent - it will overwrite existing data in S3. To refresh historical data:

1. Start the Step Functions execution again (same process as initial load)
2. All S3 files will be overwritten with fresh data

## Next Steps

After historical load completes:

1. **Deploy Daily Incremental Updater**: Use existing `stock_data_batch_fetcher` + `stock_data_processor` to append new data daily
2. **Deploy Aggregator Lambda**: Calculate metrics from S3 and populate DynamoDB for fast screening
3. **Monitor S3 Storage**: Check CloudWatch metrics for S3 bucket size

## Files

- `lambda_function.py`: Main Lambda code
- `requirements.txt`: Python dependencies
- `highcap.csv`: High-priority stocks (Fortune 500)
- `midcap.csv`: Medium-priority stocks
- `lowcap.csv`: Low-priority stocks (NASDAQ + NYSE)

## Support

For issues or questions, check:
- CloudWatch Logs: `/aws/lambda/cosine-stock-data-historical-loader-{environment}`
- Step Functions Execution History
- S3 bucket contents: `s3://cosine-stock-historical-{environment}/historical/`

