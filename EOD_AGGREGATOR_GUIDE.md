# EOD (End of Day) Aggregator System

## 📋 Overview

The EOD Aggregator runs **daily at 4:30 PM ET** (after market close) to aggregate historical stock data from S3 into DynamoDB for fast querying. It processes **all stocks in parallel** using AWS Step Functions.

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  EventBridge Scheduler: Daily at 4:30 PM ET (Mon-Fri)           │
└────────────────────┬────────────────────────────────────────────┘
                     │ Triggers
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│  Step Functions State Machine: eod-aggregator                    │
│                                                                   │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │ Step 1: GenerateBatches                                     │ │
│  │  Lambda: eod-batch-generator                                │ │
│  │  • Lists all S3 stock files (stock-data/{priority}/*.json) │ │
│  │  • Groups into batches of 200 stocks                        │ │
│  │  • Returns batch configs                                    │ │
│  │  Output: 42 batches (8,346 stocks ÷ 200)                    │ │
│  └────────────────────┬───────────────────────────────────────┘ │
│                       │                                           │
│                       ▼                                           │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │ Step 2: ProcessBatches (Map State - 40 parallel)            │ │
│  │  For each batch (200 stocks):                               │ │
│  │  ┌──────────────────────────────────────────────────────┐  │ │
│  │  │ Lambda: eod-aggregator                                │  │ │
│  │  │  • Read 200 S3 files                                  │  │ │
│  │  │  • For each stock, calculate metrics for 4 timeframes │  │ │
│  │  │    (1d, 7d, 30d, 1y)                                  │  │ │
│  │  │  • Create 800 DynamoDB items (200 × 4)                │  │ │
│  │  │  • Batch write to DynamoDB                            │  │ │
│  │  └──────────────────────────────────────────────────────┘  │ │
│  │  40 batches run in parallel = ~2-3 minutes total            │ │
│  └────────────────────┬───────────────────────────────────────┘ │
│                       │                                           │
│                       ▼                                           │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │ Step 3: AggregateResults                                    │ │
│  │  • Log summary (total stocks, batches, success/fail)       │ │
│  │  • Return final results                                     │ │
│  └────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

## 📊 Data Flow

### Input (S3 Historical Files)
```json
{
  "symbol": "AAPL",
  "sector": "Information Technology",
  "industry": "Consumer Electronics",
  "market_cap": 3456789012345,
  "shares_outstanding": 15204052000,
  "history": [
    {
      "timestamp": 1603200600,
      "date": "2020-10-20T13:30:00",
      "close": 117.51,
      "volume": 124423700,
      "market_cap": 2000000000000
    },
    // ... 1000+ historical data points
  ]
}
```

### Output (DynamoDB Items - 4 per stock)
```json
// Item 1: 1d timeframe
{
  "PK": "STOCK#AAPL",
  "SK": "1d#CURRENT",
  "symbol": "AAPL",
  "timeframe": "1d",
  "current_price": 227.50,
  "market_cap": 3456789012345,
  "shares_outstanding": 15204052000,
  "sector": "Information Technology",
  "industry": "Consumer Electronics",
  "volatility": 0.0234,
  "price_change_percent": 1.23,
  "annual_return": 15.67,
  "week_return": 2.34,
  // ... more fields
  "GSI1PK": "INDUSTRY#Consumer Electronics#1d",
  "GSI1SK": 0.0234,  // Sort by volatility
  "GSI2PK": "VOLATILITY#1d",
  "GSI2SK": 0.0234,
  "GSI3PK": "PRICE_CHANGE#1d",
  "GSI3SK": 1.23,
  "GSI4PK": "MARKET_CAP#1d",
  "GSI4SK": 3456789012345,
  "GSI5PK": "PRICE#1d",
  "GSI5SK": 227.50
}

// Item 2: 7d timeframe
// Item 3: 30d timeframe
// Item 4: 1y timeframe
```

## 🔢 Performance Metrics

### Processing Time
- **8,346 stocks** × **4 timeframes** = **33,384 DynamoDB items**
- **42 batches** of 200 stocks each
- **40 parallel executions**
- **Estimated time: 2-3 minutes** ✅

### Cost Estimate (per day)
- Step Functions: $0.025 per 1,000 state transitions
  - ~50 transitions per execution = $0.00125/day
- Lambda invocations:
  - Batch generator: 1 × $0.0000002 = negligible
  - Aggregator: 42 × $0.0000002 = negligible
  - Compute time: 42 batches × 30 seconds × $0.0000166667/GB-sec = ~$0.02/day
- DynamoDB writes: 33,384 items × $1.25 per million = $0.04/day
- **Total: ~$0.06/day = $22/year** 💰

## 📅 Schedule

| Priority | Update Frequency | Daily Triggers | Purpose |
|----------|-----------------|----------------|---------|
| **High** | Every 30 min | 15 times | Real-time tracking (Fortune 500) |
| **Medium** | Every hour | 8 times | Regular updates (mid-cap) |
| **Low** | Every 2 hours | 4 times | Basic updates (small-cap) |
| **EOD** | Once daily | 1 time | **Aggregate all to DynamoDB** |

## 🚀 Deployment

```bash
cd C:\Users\Thriambak\Documents\Code\Cosine-Base-Infra\terraform

# Review changes
terraform plan

# Deploy
terraform apply

# Verify deployment
aws stepfunctions list-state-machines --query "stateMachines[?contains(name, 'eod-aggregator')]"
```

## 🧪 Manual Testing

### Test the EOD Aggregator

```bash
# Start Step Functions execution
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:us-east-1:ACCOUNT_ID:stateMachine:cosine-eod-aggregator-production \
  --input '{"source":"manual-test","timestamp":"'$(date -u +%Y-%m-%dT%H:%M:%SZ)'"}'

# Check execution status
aws stepfunctions describe-execution \
  --execution-arn EXECUTION_ARN

# View CloudWatch logs
aws logs tail /aws/lambda/cosine-eod-aggregator-production --follow
```

### Test Single Batch (Lambda directly)

```bash
# Create test payload
echo '{
  "symbols": ["AAPL", "MSFT", "GOOGL"],
  "priority": "high",
  "batch_number": 1,
  "total_in_batch": 3
}' > test_eod_batch.json

# Invoke aggregator Lambda
aws lambda invoke \
  --function-name cosine-eod-aggregator-production \
  --payload file://test_eod_batch.json \
  response.json

# Check result
cat response.json
```

## 📊 DynamoDB Query Examples

Once EOD aggregator runs, you can query the data:

### Get all stocks in an industry sorted by volatility
```python
import boto3
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('cosine-stock-data-production')

response = table.query(
    IndexName='IndustryVolatilityIndex',
    KeyConditionExpression='GSI1PK = :pk',
    ExpressionAttributeValues={
        ':pk': 'INDUSTRY#Consumer Electronics#1d'
    },
    ScanIndexForward=False,  # Highest volatility first
    Limit=10
)
```

### Get stocks by market cap range
```python
response = table.query(
    IndexName='MarketCapRangeIndex',
    KeyConditionExpression='GSI4PK = :pk AND GSI4SK BETWEEN :min AND :max',
    ExpressionAttributeValues={
        ':pk': 'MARKET_CAP#1d',
        ':min': 1000000000000,    # $1T
        ':max': 5000000000000     # $5T
    }
)
```

### Get top gainers
```python
response = table.query(
    IndexName='PriceChangeRangeIndex',
    KeyConditionExpression='GSI3PK = :pk AND GSI3SK > :min',
    ExpressionAttributeValues={
        ':pk': 'PRICE_CHANGE#1d',
        ':min': Decimal('5.0')  # Stocks up > 5%
    },
    ScanIndexForward=False  # Highest gains first
)
```

## 🔧 Key Features

### ✅ Included in DynamoDB Items
- Current price, volume, market cap
- **Sector and industry** (from S3 metadata)
- **Shares outstanding** (from SEC filings)
- Price changes (%, absolute)
- Returns (week, annual)
- Volatility (annualized)
- 52-week high/low
- Day high/low
- Average volume
- **5 GSI indexes** for efficient querying

### ✅ Historically Accurate Market Cap
- Uses the **correct shares outstanding for each time period**
- Accounts for stock splits, buybacks, new issuances
- Each historical data point has accurate market cap

### ✅ Parallel Processing
- **40 concurrent batch executions**
- Completes in **2-3 minutes** vs 70+ minutes sequential
- Handles failures gracefully (retries, error handling)

## ⚠️ Important Notes

1. **Historical Loader is Commented Out**
   - The one-time historical loader is now commented out in `main.tf`
   - **Do not uncomment** unless you want to reload all historical data
   - It would overwrite your existing S3 files

2. **Timezone Handling**
   - Schedules use UTC time adjusted for ET Daylight Saving Time
   - During Standard Time (Nov-Mar), times will be 1 hour off
   - Consider adding market hours check in Lambda code

3. **Data Freshness**
   - DynamoDB data is updated **once daily at 4:30 PM ET**
   - Intraday updates come from the bi-hourly S3 updater
   - TTL set to 7 days (refreshed daily)

4. **Cost Optimization**
   - Uses on-demand pricing for DynamoDB (no reserved capacity needed)
   - Lambda cold starts minimized by Step Functions warm-up
   - S3 historical files transition to Glacier after 90 days

## 📁 File Structure

```
Cosine-Base-Infra/
├── backend_app/src/
│   ├── eod_batch_generator/app/
│   │   ├── lambda_function.py
│   │   └── requirements.txt
│   ├── eod_aggregator/app/
│   │   ├── lambda_function.py
│   │   └── requirements.txt
│   └── stock_data_processor/app/
│       ├── lambda_function.py  (bi-hourly S3 updater)
│       └── requirements.txt
└── terraform/
    └── main.tf  (contains all EOD infrastructure)
```

## 🎯 Next Steps

1. **Deploy the infrastructure**
   ```bash
   terraform apply
   ```

2. **Test the EOD aggregator manually**
   ```bash
   aws stepfunctions start-execution \
     --state-machine-arn <ARN> \
     --input '{}'
   ```

3. **Monitor first scheduled run** (next trading day at 4:30 PM ET)
   - Check CloudWatch logs
   - Verify DynamoDB has data
   - Query test data

4. **Create aggregator frontend API** (future work)
   - API Gateway endpoints
   - Query stocks by sector, volatility, market cap
   - Real-time screeners

---

**Created:** October 12, 2025  
**Status:** Ready for deployment

