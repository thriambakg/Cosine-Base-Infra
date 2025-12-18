# Stock Data Architecture - Complete System Design

## Overview

This document describes the complete stock data system architecture, from initial historical data loading through daily updates and real-time screening.

## Architecture Phases

### Phase 1: One-Time Historical Load (S3 + Step Functions)

**Purpose**: Load 5 years of historical data for all 8,348 stocks

```
Manual Trigger (AWS Console)
  ↓
Step Functions State Machine
  ↓
Step 1: Generate Batches
  Lambda reads CSVs (highcap, midcap, lowcap)
  Creates 56 batches of 150 stocks each
  ↓
Step 2: Process Batches (Parallel)
  5 concurrent Lambda workers
  Each fetches 5 years of data from Yahoo Finance
  Stores to S3: s3://bucket/historical/{priority}/{symbol}.json
  ↓
Step 3: Aggregate Results
  Log summary statistics
  Return success/failure report
```

**Duration**: ~2.5 hours (one-time)
**Cost**: $2.50 (one-time) + $0.02/month storage

**S3 Structure**:
```
s3://cosine-stock-historical-{environment}/
  historical/
    high/
      AAPL.json (1,825 data points)
      MSFT.json
      ...
    medium/
      FRHC.json
      ...
    low/
      AACB.json
      ...
```

**Data Format** (per symbol):
```json
{
  "symbol": "AAPL",
  "currency": "USD",
  "exchange": "NASDAQ",
  "data_points": 1825,
  "first_date": "2020-10-12",
  "last_date": "2025-10-12",
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
    ...1825 entries...
  ]
}
```

---

### Phase 2: Daily Incremental Updates (EventBridge + SQS + Lambda)

**Purpose**: Append new daily data to S3 historical files

```
EventBridge Schedulers (Trading Days Only)
  ├─ High Priority: Every 30 min (1d timeframe)
  ├─ Medium Priority: Every 2 hours (7d timeframe)
  └─ Low Priority: Every 6 hours (30d, 1y timeframes)
  ↓
stock_data_batch_fetcher Lambda
  Reads CSV files
  Creates SQS batches (100-150 stocks each)
  ↓
SQS Queue: cosine-stock-data-{environment}
  Message format:
  {
    "symbols": ["AAPL", "MSFT", ...],
    "priority": "high",
    "timeframe": "1d",
    "timestamp": "2025-10-12T16:00:00Z"
  }
  ↓
stock_data_processor Lambda (SQS trigger)
  Fetch latest data from Yahoo Finance
  Read existing S3 file: historical/{priority}/{symbol}.json
  APPEND new data point to history array
  Write back to S3 (overwrite with extended data)
```

**Frequency**:
- High Priority (505 stocks): Every 30 minutes during market hours
- Medium Priority (1,335 stocks): Every 2 hours
- Low Priority (6,508 stocks): Every 6 hours

**Data Append Example**:
```json
{
  "symbol": "AAPL",
  "data_points": 1826,  // Incremented
  "last_date": "2025-10-13T00:00:00",  // Updated
  "history": [
    ...1825 existing entries...,
    {
      "timestamp": 1728777600,  // NEW
      "date": "2025-10-13T00:00:00",
      "open": 175.20,
      "high": 178.50,
      "low": 174.80,
      "close": 177.90,
      "volume": 95432100
    }
  ]
}
```

**Cost**: $15-25/month (Lambda + SQS)

---

### Phase 3: Nightly Aggregation (DynamoDB for Screening)

**Purpose**: Calculate screening metrics from S3 and populate DynamoDB for fast queries

```
EventBridge Scheduler (Nightly at 5 PM EST)
  cron(0 17 * * ? *)  // After market close
  ↓
stock_data_aggregator Lambda (NEW - TO BE BUILT)
  For each symbol:
    1. Read full history from S3
    2. Calculate metrics for EACH timeframe:
       - 1d: volatility, price change, market cap, etc.
       - 7d: volatility, price change, market cap, etc.
       - 30d: volatility, price change, market cap, etc.
       - 1y: volatility, price change, market cap, etc.
    3. Write to DynamoDB (4 rows per symbol)
  ↓
DynamoDB Table: cosine-stock-data-{environment}
  Structure:
    PK: STOCK#{symbol}
    SK: {timeframe}#CURRENT
    
  Example Rows for AAPL:
    {
      "PK": "STOCK#AAPL",
      "SK": "1d#CURRENT",
      "GSI1PK": "INDUSTRY#Technology#1d",
      "GSI1SK": 0.024,  // Volatility (numeric)
      "GSI2PK": "VOLATILITY#1d",
      "GSI2SK": 0.024,  // For range queries
      "GSI3PK": "PRICE_CHANGE#1d",
      "GSI3SK": 2.3,    // Price change %
      "GSI4PK": "MARKET_CAP#1d",
      "GSI4SK": 2500000000000,  // Market cap
      "GSI5PK": "PRICE#1d",
      "GSI5SK": 177.90,  // Current price
      "symbol": "AAPL",
      "name": "Apple Inc.",
      "industry": "Technology",
      "current_price": 177.90,
      "price_change_pct": 2.3,
      "volatility": 0.024,
      "market_cap": 2500000000000,
      "volume": 95432100,
      "updated_at": 1728777600
    }
    // ... 3 more rows for 7d, 30d, 1y
```

**Why Nightly (Not Real-Time)?**:
- Calculating volatility, price change % requires analyzing full history arrays
- Reading 1,825 data points from S3 per stock is expensive for real-time
- Nightly aggregation = pre-compute once, query fast forever
- Stock data only changes once per day (after market close)

**Cost**: $10-15/month (DynamoDB storage + writes)

---

### Phase 4: Real-Time Stock Screening (User-Facing API)

**Purpose**: Fast, sub-200ms stock screening for users

```
User Request (Frontend)
  POST /stock-screener
  {
    "timeframe": "7d",
    "volatility_min": 0.01,
    "volatility_max": 0.05,
    "price_change_min": -5,
    "price_change_max": 10,
    "market_cap_min": 1000000000,
    "industries": ["Technology", "Healthcare"]
  }
  ↓
API Gateway → stock_screener Lambda
  Query DynamoDB using GSIs:
    - VolatilityRangeIndex (GSI2)
      KeyConditionExpression: 
        GSI2PK = "VOLATILITY#7d"
        GSI2SK BETWEEN 0.01 AND 0.05
    
    - Filter in-memory for other criteria:
      - price_change_min/max
      - market_cap_min/max
      - industries
  
  Return top 100 results
  ↓
User receives results in <200ms
```

**Performance**:
- Query time: <100ms (DynamoDB GSI)
- In-memory filtering: <50ms
- Total response time: <200ms

**Cost**: ~$5/month (DynamoDB reads)

---

## Complete Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│ PHASE 1: ONE-TIME HISTORICAL LOAD (Manual Trigger)              │
│ Duration: 2.5 hours | Cost: $2.50 one-time                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  AWS Console → Step Functions                                   │
│    ↓                                                             │
│  GenerateBatches (Lambda)                                       │
│    Reads: highcap.csv, midcap.csv, lowcap.csv                  │
│    Output: 56 batches × 150 symbols                             │
│    ↓                                                             │
│  ProcessBatches (5 parallel Lambdas)                            │
│    Fetch 5 years from Yahoo Finance                             │
│    Rate limit: 2 req/sec                                        │
│    ↓                                                             │
│  S3: historical/{priority}/{symbol}.json                        │
│    8,348 stocks × 1,825 data points                             │
│    Storage: ~800 MB total                                       │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│ PHASE 2: DAILY INCREMENTAL UPDATES (Automated)                  │
│ Duration: Continuous | Cost: $20/month                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  EventBridge Schedulers                                         │
│    High: Every 30min | Medium: Every 2hr | Low: Every 6hr      │
│    ↓                                                             │
│  batch_fetcher Lambda                                           │
│    Creates SQS batches                                          │
│    ↓                                                             │
│  SQS Queue                                                      │
│    Buffers batches                                              │
│    ↓                                                             │
│  processor Lambda (10 concurrent)                               │
│    Fetch latest data from Yahoo Finance                         │
│    Read S3 → APPEND new data → Write back                      │
│    ↓                                                             │
│  S3: historical/{priority}/{symbol}.json (UPDATED)              │
│    Data points: 1,826, 1,827, 1,828...                         │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│ PHASE 3: NIGHTLY AGGREGATION (Automated)                        │
│ Duration: ~30 minutes | Cost: $15/month                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  EventBridge Scheduler (5 PM EST daily)                         │
│    cron(0 17 * * ? *)                                           │
│    ↓                                                             │
│  aggregator Lambda (TO BE BUILT)                                │
│    For each symbol:                                             │
│      1. Read S3: historical/{priority}/{symbol}.json            │
│      2. Calculate metrics for 1d, 7d, 30d, 1y                  │
│      3. Write 4 rows to DynamoDB                                │
│    ↓                                                             │
│  DynamoDB Table: cosine-stock-data-{environment}                │
│    33,392 rows (8,348 stocks × 4 timeframes)                   │
│    With 5 GSIs for fast filtering                               │
│      - IndustryVolatilityIndex (GSI1)                           │
│      - VolatilityRangeIndex (GSI2)                              │
│      - PriceChangeRangeIndex (GSI3)                             │
│      - MarketCapRangeIndex (GSI4)                               │
│      - PriceRangeIndex (GSI5)                                   │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│ PHASE 4: USER QUERIES (Real-Time)                               │
│ Duration: <200ms | Cost: $5/month                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  User → API Gateway → stock_screener Lambda                     │
│    ↓                                                             │
│  Query DynamoDB with GSIs                                       │
│    BETWEEN queries on numeric sort keys                         │
│    ↓                                                             │
│  Filter in-memory (industries, etc.)                            │
│    ↓                                                             │
│  Return top 100 results                                         │
│    Response time: <200ms                                        │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Cost Breakdown

| Phase | Component | One-Time | Monthly |
|-------|-----------|----------|---------|
| **Phase 1** | Step Functions | $0.001 | - |
| | Lambda (historical loader) | $2.50 | - |
| | S3 Storage (800 MB) | - | $0.02 |
| **Phase 2** | EventBridge Schedulers | - | $1 |
| | Lambda (batch fetcher) | - | $5 |
| | Lambda (processor) | - | $10 |
| | SQS Queue | - | $2 |
| | S3 Writes | - | $2 |
| **Phase 3** | Lambda (aggregator) | - | $5 |
| | DynamoDB Writes | - | $5 |
| | DynamoDB Storage (33k rows) | - | $5 |
| **Phase 4** | DynamoDB Reads | - | $5 |
| | Lambda (stock screener) | - | $2 |
| **TOTAL** | | **$2.50** | **$44/month** |

**Compared to Current (DynamoDB-only)**:
- Current: $35/month (no historical data)
- New Hybrid: $44/month (+$9/month for unlimited history)

---

## Implementation Checklist

### ✅ Phase 1: Historical Load (COMPLETED)
- [x] Create Step Functions module
- [x] Create historical loader Lambda
- [x] Bundle CSV files with Lambda
- [x] Configure S3 bucket with lifecycle rules
- [x] Add to Terraform main.tf
- [ ] Deploy: `terraform apply`
- [ ] Execute: AWS Console → Step Functions → Start execution (empty input)

### 🔄 Phase 2: Daily Updates (ALREADY IMPLEMENTED)
- [x] batch_fetcher Lambda (existing)
- [x] processor Lambda (existing)
- [x] SQS Queue (existing)
- [x] EventBridge Schedulers (existing)
- [ ] **TODO**: Update processor to APPEND to S3 instead of writing to DynamoDB

### ⏳ Phase 3: Aggregator (TO BE BUILT)
- [ ] Create aggregator Lambda
- [ ] Read from S3
- [ ] Calculate metrics for 1d/7d/30d/1y
- [ ] Write to DynamoDB with numeric GSI sort keys
- [ ] Add nightly EventBridge scheduler
- [ ] Deploy and test

### ✅ Phase 4: Screening (ALREADY IMPLEMENTED)
- [x] stock_screener Lambda (existing)
- [x] DynamoDB GSIs (existing)
- [ ] **TODO**: Update queries to use numeric BETWEEN operators

---

## Benefits of This Architecture

### ✅ Scalability
- S3 can handle millions of data points
- DynamoDB GSIs enable fast queries at any scale
- Decoupled components (changes to one don't affect others)

### ✅ Cost Optimization
- S3 storage is 10x cheaper than DynamoDB
- Glacier transition after 90 days (5x additional savings)
- Pre-computed metrics = fewer Lambda invocations

### ✅ Flexibility
- Add new timeframes? Just update aggregator
- Change calculation logic? Recalculate from S3
- Historical analysis? Query S3 directly

### ✅ Performance
- User queries: <200ms (DynamoDB)
- Historical analytics: 1-5s (Athena, optional)
- Real-time updates: Every 30min-6hr

### ✅ Reliability
- Automatic retries (Step Functions + SQS)
- Dead-letter queues for failed messages
- CloudWatch logging for debugging

---

## Next Steps

1. **Deploy Phase 1**: Run `terraform apply` and execute Step Functions
2. **Monitor Progress**: Check CloudWatch logs and Step Functions console
3. **Build Phase 3**: Create aggregator Lambda (priority after historical load completes)
4. **Test End-to-End**: Verify data flow from S3 → DynamoDB → API

---

## Support

- **Historical Loader**: See `backend_app/src/stock_data_historical_loader/README.md`
- **Terraform**: See `terraform/README.md`
- **DynamoDB Schema**: See `terraform/modules/dynamodb/stock_data_table.tf`

