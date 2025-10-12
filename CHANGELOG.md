# Changelog - Stock Data Architecture Refactor

## 2025-10-12 - Major Architecture Change: Step Functions for Historical Data Loading

### Overview
Replaced EventBridge-based scheduled stock data fetching with a hybrid S3 + DynamoDB architecture for better scalability, cost efficiency, and historical data retention.

### Changes

#### ✅ Added

1. **Generic Step Functions Module** (`terraform/modules/step-functions/`)
   - Reusable module for creating Step Functions state machines
   - Built-in CloudWatch logging and IAM role management
   - Configurable retry logic and error handling

2. **Historical Loader Lambda** (`backend_app/src/stock_data_historical_loader/`)
   - Dual-mode Lambda:
     - **Batch Generation**: Reads CSVs and creates batch configs
     - **Batch Processing**: Fetches 5 years of data from Yahoo Finance
   - Bundles `highcap.csv`, `midcap.csv`, `lowcap.csv` (8,348 total stocks)
   - Stores historical data in S3: `s3://bucket/historical/{priority}/{symbol}.json`
   - Zero-configuration execution (no input JSON required)

3. **Step Functions State Machine** (`terraform/main.tf`)
   - 3-step workflow:
     1. Generate batches from CSVs
     2. Process 56 batches in parallel (5 concurrent workers)
     3. Aggregate and log results
   - Rate limiting: 2 requests/second to Yahoo Finance
   - Automatic retries with exponential backoff
   - Expected duration: ~2.5 hours for all 8,348 stocks

4. **S3 Bucket for Historical Data**
   - Lifecycle rules:
     - Glacier transition after 90 days (5x cost savings)
     - Expiration after 5 years (1,825 days)
     - Abort incomplete multipart uploads after 7 days
   - KMS encryption
   - Cost: $0.02/month (drops to $0.004/month after Glacier transition)

5. **Architecture Documentation**
   - `STOCK_DATA_ARCHITECTURE.md`: Complete 4-phase architecture guide
   - `backend_app/src/stock_data_historical_loader/README.md`: Usage instructions

#### ❌ Removed

1. **12 EventBridge Schedulers** (lines 869-1153 in `terraform/main.tf`)
   - Removed all schedulers for `stock_data_batch_fetcher`:
     - `stock_data_batch_fetcher_high_priority_scheduler`
     - `stock_data_batch_fetcher_medium_priority_scheduler`
     - `stock_data_batch_fetcher_low_priority_scheduler`
     - `stock_data_batch_fetcher_high_7d_scheduler`
     - `stock_data_batch_fetcher_medium_7d_scheduler`
     - `stock_data_batch_fetcher_low_7d_scheduler`
     - `stock_data_batch_fetcher_high_30d_scheduler`
     - `stock_data_batch_fetcher_medium_30d_scheduler`
     - `stock_data_batch_fetcher_low_30d_scheduler`
     - `stock_data_batch_fetcher_high_1y_scheduler`
     - `stock_data_batch_fetcher_medium_1y_scheduler`
     - `stock_data_batch_fetcher_low_1y_scheduler`
   - These schedulers are no longer needed with the new architecture

### Architecture Phases

#### Phase 1: One-Time Historical Load (✅ COMPLETED)
- Step Functions + Lambda for 5-year data load
- Cost: $2.50 one-time + $0.02/month storage

#### Phase 2: Daily Incremental Updates (🔄 ALREADY IMPLEMENTED - NEEDS UPDATE)
- `stock_data_batch_fetcher` + `stock_data_processor` (existing)
- **TODO**: Update processor to APPEND to S3 instead of writing to DynamoDB

#### Phase 3: Nightly Aggregation (⏳ TO BE BUILT)
- New `stock_data_aggregator` Lambda
- Read from S3, calculate metrics, write to DynamoDB
- Run nightly at 5 PM EST (after market close)

#### Phase 4: Real-Time Screening (✅ ALREADY IMPLEMENTED)
- `stock_screener` Lambda (existing)
- Query DynamoDB with GSIs for <200ms response times

### Cost Comparison

| Architecture | One-Time | Monthly | Features |
|--------------|----------|---------|----------|
| **Old (DynamoDB-only)** | $0 | $35 | No historical data |
| **New (Hybrid S3 + DynamoDB)** | $2.50 | $44 | 5 years of history |

**Cost Increase**: +$9/month for unlimited historical data retention

### Migration Steps

1. **Deploy Infrastructure**:
   ```bash
   cd Cosine-Base-Infra/terraform
   terraform apply
   ```

2. **Run Historical Load** (one-time):
   - Go to AWS Console → Step Functions
   - Find: `cosine-stock-historical-loader-{environment}`
   - Click "Start execution"
   - Leave input empty: `{}`
   - Click "Start execution"
   - Wait ~2.5 hours

3. **Monitor Progress**:
   - Step Functions console (visual workflow)
   - CloudWatch Logs: `/aws/stepfunctions/cosine-stock-historical-loader-{environment}`
   - Lambda Logs: `/aws/lambda/cosine-stock-data-historical-loader-{environment}`

4. **Verify Data**:
   ```bash
   # Check S3 bucket
   aws s3 ls s3://cosine-stock-historical-production/historical/high/ | head
   aws s3 ls s3://cosine-stock-historical-production/historical/medium/ | head
   aws s3 ls s3://cosine-stock-historical-production/historical/low/ | head
   
   # Download sample file
   aws s3 cp s3://cosine-stock-historical-production/historical/high/AAPL.json - | jq .
   ```

### Breaking Changes

None. The new architecture is additive and doesn't affect existing functionality.

### Terraform State Impact

- **Added Resources**:
  - `module.stock_data_historical_s3` (S3 bucket)
  - `module.stock_data_historical_loader` (Lambda function)
  - `module.stock_data_historical_loader_state_machine` (Step Functions)

- **Removed Resources**:
  - 12 EventBridge schedulers (will be destroyed on `terraform apply`)

### Rollback Plan

If needed, revert to previous commit and re-enable the EventBridge schedulers:

```bash
git revert HEAD
terraform apply
```

### Next Steps

1. ✅ Deploy Step Functions infrastructure
2. ✅ Run one-time historical load
3. ⏳ Build `stock_data_aggregator` Lambda (Phase 3)
4. ⏳ Update `stock_data_processor` to append to S3 (Phase 2)

### References

- **Architecture Guide**: `STOCK_DATA_ARCHITECTURE.md`
- **Historical Loader README**: `backend_app/src/stock_data_historical_loader/README.md`
- **Step Functions Module**: `terraform/modules/step-functions/`
- **Stock Screener Lambda**: `Cosine2.0/backend_app/src/stocks/stock_screener/`

---

## Previous Changes

(Add previous changelog entries here as needed)

