# Stock Data Pipeline - Complete System Design

## Overview
A scalable, cost-effective pipeline for fetching, processing, and serving stock data for ~8,500 stocks across multiple timeframes using AWS serverless architecture.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│          EventBridge Schedulers (12 total)              │
│  Different intervals for priority/timeframe combos      │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────┐
│       Stock Data Batch Fetcher Lambda                   │
│  - Loads symbols from CSVs (Fortune 500, Mid-cap, All)  │
│  - Creates batches based on priority tier               │
│  - Sends batches to SQS queue                           │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────┐
│                  SQS Queue                               │
│  - Decouples fetching from processing                   │
│  - Automatic retries with DLQ                           │
│  - Batch size: 10 messages                              │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────┐
│       Stock Data Processor Lambda                       │
│  - Fetches data via Yahoo Finance HTTP                  │
│  - Parallel processing (ThreadPoolExecutor)             │
│  - Rate limiting (1 req/sec per thread)                 │
│  - Writes to DynamoDB with numeric GSI keys             │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────┐
│         DynamoDB Stock Data Table                       │
│  PK: STOCK#{symbol}                                     │
│  SK: {timeframe}#CURRENT                                │
│  GSIs with NUMERIC sort keys for BETWEEN queries        │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────┐
│           Stock Screener Lambda                         │
│  - Queries DynamoDB using numeric range queries         │
│  - Sub-second response times                            │
│  - No external API calls needed                         │
└─────────────────────────────────────────────────────────┘
```

## Priority Tiers

### High Priority (504 stocks)
- **Source**: `highcap.csv`
- **Companies**: Fortune 500 / High-cap companies (AAPL, MSFT, NVDA, etc.)
- **Update Frequency**: Most frequent
- **Batch Size**: 100 stocks/batch

### Medium Priority (978 stocks)
- **Source**: `midcap.csv`
- **Companies**: Mid-cap stocks (FRHC, GNRC, SWK, etc.)
- **Update Frequency**: Moderate
- **Batch Size**: 75 stocks/batch

### Low Priority (6,508 stocks)
- **Source**: `lowcap.csv` (deduplicated NYSE + NASDAQ, excluding high/medium)
- **Companies**: All other listed stocks
- **Update Frequency**: Least frequent
- **Batch Size**: 50 stocks/batch

**Total Coverage**: 7,990 unique stocks

## EventBridge Schedule Matrix

| Timeframe | High (F500) | Medium (Mid-cap) | Low (Others) |
|-----------|-------------|------------------|--------------|
| **1 Day** | Every 5 min | Every 15 min | Every 60 min |
| **7 Day** | Every 30 min | Every 60 min | Every 3 hours |
| **30 Day** | Every 2 hours | Every 4 hours | Every 6 hours |
| **1 Year** | Every 6 hours | Every 12 hours | Every 24 hours |

**Total Schedulers**: 12 (3 priorities × 4 timeframes)

## DynamoDB Schema

### Primary Key Structure
```python
{
    'PK': 'STOCK#AAPL',
    'SK': '1d#CURRENT'  # Timeframe included in sort key
}
```

### GSI Structure (Numeric Sort Keys)

**GSI1: IndustryVolatilityIndex**
- Hash Key: `INDUSTRY#{industry}#{timeframe}` (e.g., "INDUSTRY#Technology#1d")
- Sort Key: Volatility (Numeric - Decimal)
- Use Case: Find tech stocks with volatility 0.1-0.3

**GSI2: VolatilityRangeIndex**
- Hash Key: `VOLATILITY#{timeframe}` (e.g., "VOLATILITY#1d")
- Sort Key: Volatility value (Numeric - Decimal)
- Use Case: Find all stocks with volatility 0.1-0.3

**GSI3: PriceChangeRangeIndex**
- Hash Key: `PRICE_CHANGE#{timeframe}` (e.g., "PRICE_CHANGE#1d")
- Sort Key: Price change % (Numeric - Decimal)
- Use Case: Find stocks with -5% to +5% change

**GSI4: MarketCapRangeIndex**
- Hash Key: `MARKET_CAP#{timeframe}` (e.g., "MARKET_CAP#1d")
- Sort Key: Market capitalization (Numeric - Decimal)
- Use Case: Find stocks with cap $1B-$100B

**GSI5: PriceRangeIndex**
- Hash Key: `PRICE#{timeframe}` (e.g., "PRICE#1d")
- Sort Key: Current price (Numeric - Decimal)
- Use Case: Find stocks priced $50-$200

### Example DynamoDB Item

```python
{
    # Primary Key
    'PK': 'STOCK#AAPL',
    'SK': '1d#CURRENT',
    
    # Stock Data
    'symbol': 'AAPL',
    'timeframe': '1d',
    'current_price': Decimal('175.43'),
    'previous_close': Decimal('173.50'),
    'price_change': Decimal('1.93'),
    'price_change_percent': Decimal('1.11'),
    'week_return': Decimal('2.45'),
    'annual_return': Decimal('15.32'),
    'volatility': Decimal('0.15'),  # 15% annualized
    'volume': 52000000,
    'avg_volume': 50000000,
    'market_cap': Decimal('2800000000000'),  # $2.8T
    'pe_ratio': Decimal('28.5'),
    'beta': Decimal('1.2'),
    'dividend_yield': Decimal('0.005'),
    'eps': Decimal('6.15'),
    'industry': 'Technology',
    'sector': 'Consumer Electronics',
    'day_high': Decimal('176.12'),
    'day_low': Decimal('174.23'),
    'year_high': Decimal('198.23'),
    'year_low': Decimal('164.08'),
    
    # GSI Keys (Numeric sort keys for BETWEEN queries)
    'GSI1PK': 'INDUSTRY#Technology#1d',
    'GSI1SK': Decimal('0.15'),  # ← Numeric volatility
    
    'GSI2PK': 'VOLATILITY#1d',
    'GSI2SK': Decimal('0.15'),  # ← Numeric volatility
    
    'GSI3PK': 'PRICE_CHANGE#1d',
    'GSI3SK': Decimal('1.11'),  # ← Numeric price change %
    
    'GSI4PK': 'MARKET_CAP#1d',
    'GSI4SK': Decimal('2800000000000'),  # ← Numeric market cap
    
    'GSI5PK': 'PRICE#1d',
    'GSI5SK': Decimal('175.43'),  # ← Numeric price
    
    # Metadata
    'data_source': 'Yahoo Finance HTTP',
    'last_updated': '2025-10-11T12:00:00.000Z',
    'expires_at': 1730476800  # TTL: 30 days
}
```

## Query Examples

### 1. Find stocks with volatility between 10% and 30%
```python
table.query(
    IndexName='VolatilityRangeIndex',
    KeyConditionExpression=Key('GSI2PK').eq('VOLATILITY#1d') & 
                         Key('GSI2SK').between(Decimal('0.10'), Decimal('0.30'))
)
```

### 2. Find tech stocks with low volatility
```python
table.query(
    IndexName='IndustryVolatilityIndex',
    KeyConditionExpression=Key('GSI1PK').eq('INDUSTRY#Technology#1d') & 
                         Key('GSI1SK').lt(Decimal('0.20'))
)
```

### 3. Find stocks priced between $50-$200
```python
table.query(
    IndexName='PriceRangeIndex',
    KeyConditionExpression=Key('GSI5PK').eq('PRICE#1d') & 
                         Key('GSI5SK').between(Decimal('50'), Decimal('200'))
)
```

### 4. Find mega-cap stocks (market cap > $1T)
```python
table.query(
    IndexName='MarketCapRangeIndex',
    KeyConditionExpression=Key('GSI4PK').eq('MARKET_CAP#1d') & 
                         Key('GSI4SK').gt(Decimal('1000000000000'))
)
```

## Lambda Functions

### 1. Stock Data Batch Fetcher
- **Runtime**: Python 3.11
- **Memory**: 1024 MB
- **Timeout**: 300 seconds (5 minutes)
- **Trigger**: EventBridge Scheduler (12 schedules)
- **Purpose**: Load symbols from CSVs and send batches to SQS
- **Environment Variables**:
  - `BATCH_SIZE_HIGH=100`
  - `BATCH_SIZE_MEDIUM=75`
  - `BATCH_SIZE_LOW=50`
  - `SQS_QUEUE_URL`

### 2. Stock Data Processor
- **Runtime**: Python 3.11
- **Memory**: 512 MB
- **Timeout**: 60 seconds (1 minute)
- **Trigger**: SQS Queue (batch size: 10 messages)
- **Purpose**: Fetch from Yahoo Finance and store in DynamoDB
- **Environment Variables**:
  - `DYNAMODB_TABLE_NAME`
  - `SQS_QUEUE_URL`
  - `MAX_PARALLEL_THREADS=10`
  - `REQUEST_RATE_LIMIT=1.0`
  - `BATCH_TIMEOUT=50`

### 3. Stock Screener
- **Runtime**: Python 3.11
- **Memory**: 1024 MB
- **Timeout**: 300 seconds (5 minutes)
- **Trigger**: API Gateway (POST /stock-screener)
- **Purpose**: Query DynamoDB cache for fast screening
- **Environment Variables**:
  - `STOCK_DATA_TABLE_NAME`

## Performance Estimates

### Data Fetching Timeline

**High Priority (504 stocks)**
- Batch size: 100
- Number of batches: 6
- Processing time: ~30-40 seconds (parallel)
- Update frequency (1d): Every 5 minutes

**Medium Priority (978 stocks)**
- Batch size: 75
- Number of batches: 13
- Processing time: ~65-80 seconds (parallel)
- Update frequency (1d): Every 15 minutes

**Low Priority (6,508 stocks)**
- Batch size: 50
- Number of batches: 131
- Processing time: ~655-790 seconds (parallel, ~11-13 minutes)
- Update frequency (1d): Every 60 minutes

### Query Performance
- **DynamoDB Query** (with BETWEEN): <100ms
- **In-memory filtering**: <50ms
- **Total response time**: <200ms (vs 15+ seconds with live API calls)

## Cost Optimization

### Why This Design is Cost-Effective

1. **Pre-computation**: Fetch data once, serve many times
2. **Caching**: 30-day TTL reduces redundant fetches
3. **Priority-based scheduling**: High-value stocks updated more frequently
4. **Batch processing**: Reduces Lambda invocations
5. **DynamoDB GSIs**: Fast queries without expensive scans
6. **No external API costs**: Free Yahoo Finance endpoints

### Estimated AWS Costs (Monthly)

**Lambda Invocations**:
- Batch Fetcher: ~8,640/month (12 schedulers × 24 hours × 30 days)
- Processor: ~90,000/month (SQS triggers)
- Total: ~$5-10/month

**DynamoDB**:
- Storage: ~2GB (8,500 stocks × 4 timeframes × ~150 bytes)
- Reads: ~1M/month (stock screener queries)
- Writes: ~100K/month (data updates)
- Total: ~$5-15/month (depends on billing mode)

**SQS**:
- Messages: ~90,000/month
- Total: <$1/month

**Total Estimated Cost**: $10-25/month for ~8,500 stocks

## Benefits

### ✅ Performance
- **Fast queries**: Sub-second stock screening
- **Scalable**: Handles thousands of stocks
- **Reliable**: Automatic retries with DLQ

### ✅ Accuracy
- **Precise filtering**: BETWEEN queries at database level
- **No over-fetching**: Get exactly what you need
- **Multi-criteria**: Combine filters efficiently

### ✅ Cost-Effective
- **Free data source**: Yahoo Finance HTTP
- **Minimal AWS costs**: Serverless, pay-per-use
- **Optimized queries**: Read only what you need

### ✅ Maintainable
- **Parameterized**: Easy to adjust batch sizes
- **Modular**: Separate concerns (fetch, process, query)
- **Observable**: CloudWatch logs at each stage

## Future Enhancements

### Phase 2 (API Gateway for Processor)
- Add REST API to Stock Data Processor
- Enable on-demand refresh for specific stocks
- Build admin dashboard for pipeline monitoring

### Phase 3 (Advanced Features)
- Adaptive batch sizing based on success rates
- User-initiated refresh capability
- Real-time WebSocket updates for stock tiles
- Machine learning for priority tier optimization

### Phase 4 (GSI Optimization)
- Migrate to fully numeric GSI sort keys
- Remove categorical filters completely
- Add composite indexes for common query patterns

## Files Modified

### Infrastructure (Terraform)
- `Cosine-Base-Infra/terraform/main.tf`
  - Updated Stock Data Table GSI definitions (numeric sort keys)
  - Added environment variables to Batch Fetcher Lambda
  - Added environment variables to Processor Lambda
  - Created 12 EventBridge schedulers

### CSV Data Files
- `highcap.csv` - 504 high-cap companies (Fortune 500)
- `midcap.csv` - 978 mid-cap companies
- `lowcap.csv` - 6,508 low-cap companies (deduplicated NYSE + NASDAQ)
- **Total**: 7,990 unique stocks

**Note**: All CSVs use format `Symbol,Security Name` where Symbol is unquoted and Security Name is always quoted.

### Lambda Functions

**Batch Fetcher** (`Cosine-Base-Infra/backend_app/src/stock_data_batch_fetcher/app/`)
- `lambda_function.py` - Completely rewritten
- Loads symbols from CSVs
- Creates batches based on priority
- Sends to SQS

**Processor** (`Cosine-Base-Infra/backend_app/src/stock_data_processor/app/`)
- `lambda_function.py` - Completely rewritten
- Fetches from Yahoo Finance HTTP
- Parallel processing with rate limiting
- Writes numeric GSI keys to DynamoDB

**Screener** (`Cosine2.0/backend_app/src/stocks/stock_screener/app/`)
- `dynamodb_query.py` - Updated with numeric BETWEEN queries
- `lambda_function.py` - Updated to use timeframe parameter

## Deployment Steps

1. **Deploy Terraform changes**:
   ```bash
   cd Cosine-Base-Infra/terraform
   terraform init
   terraform plan
   terraform apply
   ```

2. **Verify DynamoDB table**:
   - Check GSIs are created with numeric (N) sort keys
   - Verify TTL is enabled on `expires_at` field

3. **Test Batch Fetcher**:
   - Manually invoke with test event
   - Check SQS queue receives messages

4. **Test Processor**:
   - Wait for SQS trigger or manually invoke
   - Verify DynamoDB receives data with correct GSI keys

5. **Test Screener**:
   - Call stock screener API with criteria
   - Verify fast response times (<1 second)

6. **Monitor schedulers**:
   - Check CloudWatch logs for all 12 schedulers
   - Verify batches are being processed

## Monitoring & Alerts

### CloudWatch Metrics to Monitor
- Lambda invocations (Batch Fetcher, Processor, Screener)
- Lambda duration and errors
- SQS queue depth and age of oldest message
- DynamoDB consumed read/write capacity
- DynamoDB throttling events

### Recommended Alarms
- SQS DLQ messages > 0
- Lambda error rate > 5%
- DynamoDB throttling > 10 events/minute
- Processor Lambda duration > 50 seconds

## Troubleshooting

### Common Issues

**1. Empty DynamoDB Table**
- Check EventBridge schedulers are enabled
- Verify Batch Fetcher has SQS permissions
- Check Processor has SQS and DynamoDB permissions

**2. Slow Queries**
- Verify using correct GSI with numeric sort keys
- Check if query uses BETWEEN operator
- Avoid full table scans

**3. Yahoo Finance Rate Limiting**
- Increase `REQUEST_RATE_LIMIT` delay
- Reduce `MAX_PARALLEL_THREADS`
- Check CloudWatch logs for 429 errors

**4. SQS Backlog**
- Increase Processor Lambda concurrency
- Reduce batch size in Batch Fetcher
- Check for errors in Processor Lambda

## Contact & Support
For questions or issues, check:
- CloudWatch Logs: `/aws/lambda/cosine-*`
- SQS DLQ: `cosine-stock-data-production-dlq`
- DynamoDB Table: `cosine-stock-data-production`

