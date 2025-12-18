# Politician Trades Aggregation System

## Purpose

Daily batch job that aggregates politician stock trades from public SEC filings and Congressional disclosures. Fetches Forms 3, 4, 5 from SEC EDGAR, scrapes House and Senate Periodic Transaction Reports (PTRs), matches trades to politicians using fuzzy name matching, and stores aggregated data in DynamoDB for dashboard querying.

## Architecture Overview

**3-Step Step Functions Workflow:**
1. **Fetch Forms** - Downloads SEC forms and Congressional PTRs, stores raw files in S3
2. **Match Trades** - Parses forms, extracts trades, matches to politicians
3. **Save to Database** - Batch writes matched trades to DynamoDB with idempotency

**Trigger:** EventBridge scheduler runs daily at 2:00 AM EST

---

## Resources

### S3 Bucket: `{project}-politician-trades-{environment}`

**Purpose:** Stores raw SEC forms, Congressional PTRs, and politician reference CSV

**Structure:**
```
politicians.csv                    # Reference list of politicians
trades/
  ├── 2024-01-15/
  │   ├── sec/
  │   │   ├── 4-0001234567-20240115.xml
  │   │   └── 3-0000987654-20240115.xml
  │   ├── house/
  │   │   └── rep-john-doe-20240115.pdf
  │   └── senate/
  │       └── sen-jane-smith-20240115.pdf
  └── 2024-01-16/
      └── ...
```

**Lifecycle:**
- Transitions to IA after 30 days
- Transitions to Glacier after 90 days
- Expires after 2 years

---

### DynamoDB Table: `{project}-politician-trades-{environment}`

**Purpose:** Stores matched politician trades for dashboard querying

**Primary Key:**
- `tradeId` (String) - Unique identifier: `trade_{date}_{cik}_{sequence}`

**Attributes:**
- `politicianName`, `party`, `position` - From politician CSV
- `formType` - "3", "4", "5", "house_ptr", "senate_ptr"
- `transactionDate` (Number) - Unix timestamp, sort key for all GSIs
- `transactionTime` (String, optional) - Time if available
- `securitySymbol`, `securityName` - Stock ticker and company name
- `transactionType` - "Purchase", "Sale", "Grant", "Exercise", etc.
- `shares`, `pricePerShare`, `totalAmount` - Trade details
- `formS3Key` - Reference to original form in S3
- `matchConfidence` - Name matching score (0.0 to 1.0)
- `source` - "sec", "house", "senate"

**Global Secondary Indexes (6):**

1. **PoliticianTradeDateIndex** - Query trades by politician, sorted by date
   - Hash Key: `politicianName`
   - Sort Key: `transactionDate`

2. **PositionTradeDateIndex** - Query trades by position (House, Senate, Judiciary, Executive)
   - Hash Key: `position`
   - Sort Key: `transactionDate`

3. **PartyTradeDateIndex** - Query trades by party (Republican, Democrat, Independent)
   - Hash Key: `party`
   - Sort Key: `transactionDate`

4. **SecurityTradeDateIndex** - Query all trades for a specific stock
   - Hash Key: `securitySymbol`
   - Sort Key: `transactionDate`

5. **FormTypeTradeDateIndex** - Filter by form type
   - Hash Key: `formType`
   - Sort Key: `transactionDate`

6. **TransactionTypeTradeDateIndex** - Filter by buy/sell (Purchase vs Sale)
   - Hash Key: `transactionType`
   - Sort Key: `transactionDate`

**Idempotency:** Uses `ConditionExpression: attribute_not_exists(tradeId)` to prevent duplicate entries

---

### Lambda Functions (3)

#### 1. `{project}-politician-trades-fetcher-{environment}`

**Purpose:** Fetches SEC forms and Congressional PTRs, stores in S3

**Runtime:** Python 3.11  
**Timeout:** 15 minutes  
**Memory:** 1024 MB  
**Layers:** Core layer (boto3, requests)

**Responsibilities:**
- Queries SEC EDGAR API for Forms 3, 4, 5 from previous day
- Downloads XML/PDF files from SEC
- Scrapes House Clerk's website for PTR PDFs
- Scrapes Senate Ethics website for PTR PDFs
- Stores all files in S3 with date-organized structure
- Returns metadata about fetched forms

**Helper Class:** `webscraper.py` - Contains `CongressionalPTRScraper` class for House/Senate scraping

**Output:**
```json
{
  "date": "2024-01-15",
  "secFormsFetched": 1250,
  "housePTRsFetched": 15,
  "senatePTRsFetched": 8,
  "secForms": [...],
  "housePTRs": [...],
  "senatePTRs": [...]
}
```

---

#### 2. `{project}-politician-trades-matcher-{environment}`

**Purpose:** Parses forms, extracts trades, matches to politicians

**Runtime:** Python 3.11  
**Timeout:** 15 minutes  
**Memory:** 2048 MB (higher for PDF parsing)  
**Layers:** Core layer

**Responsibilities:**
- Loads politician CSV from S3
- Parses SEC XML forms to extract trades
- Parses SEC PDF forms (requires Textract or pdf library)
- Parses House/Senate PTR PDFs
- Uses fuzzy name matching (Levenshtein distance) to match filer names to politicians
- Returns matched trades ready for database insertion

**Name Matching:**
- Exact match on primary name
- Checks alternative names from CSV
- Fuzzy match with 85% similarity threshold
- Returns match confidence score

**Output:**
```json
{
  "date": "2024-01-15",
  "matchedTrades": [...],
  "totalMatched": 45,
  "unmatchedForms": 3
}
```

---

#### 3. `{project}-politician-trades-saver-{environment}`

**Purpose:** Batch writes matched trades to DynamoDB with idempotency

**Runtime:** Python 3.11  
**Timeout:** 5 minutes  
**Memory:** 512 MB  
**Layers:** Core layer

**Responsibilities:**
- Converts trade data to DynamoDB format (Decimal for numbers)
- Batch writes to DynamoDB (25 items per batch)
- Uses `ConditionExpression` to prevent duplicates (idempotency)
- Handles retries and errors gracefully
- Returns save summary

**Idempotency:** Uses `attribute_not_exists(tradeId)` to skip duplicate trades

**Output:**
```json
{
  "date": "2024-01-15",
  "tradesSaved": 45,
  "tradesSkipped": 2,
  "errors": 0
}
```

---

### Step Functions State Machine: `{project}-politician-trades-{environment}`

**Purpose:** Orchestrates the 3-step workflow with error handling

**Definition:**
- **FetchForms** → **MatchTrades** → **SaveTrades**
- Each step has retry logic (3 attempts with exponential backoff)
- Error states route to Fail states with descriptive errors
- CloudWatch Logs integration for debugging

**Retry Policy:**
- 3 retries per step
- 30 second interval
- Exponential backoff (2x multiplier)

**Logging:**
- Level: ERROR in production, ALL in other environments
- Retention: 7 days
- Includes execution data for debugging

---

### EventBridge Scheduler: `{project}-politician-trades-{environment}`

**Purpose:** Triggers daily execution of Step Functions

**Schedule:** `cron(0 6 ? * * *)` - 2:00 AM EST (6:00 AM UTC DST / 7:00 AM UTC Standard)

**Target:** Step Functions state machine

**Why 2:00 AM EST:** SEC filings are typically complete by this time, and Congressional PTRs are usually filed during business hours the previous day.

---

## Data Flow

```
EventBridge (Daily 2:00 AM EST)
    │
    ▼
Step Function Start
    │
    ├─► Step 1: Fetch Forms (Lambda)
    │   ├─► SEC EDGAR API → Download Forms 3, 4, 5
    │   ├─► House Clerk Website → Scrape PTR PDFs
    │   ├─► Senate Ethics Website → Scrape PTR PDFs
    │   └─► S3: Store raw files in trades/{date}/
    │
    ├─► Step 2: Match Trades (Lambda)
    │   ├─► S3: Load politician CSV
    │   ├─► S3: Read form files
    │   ├─► Parse XML/PDF → Extract trades
    │   ├─► Fuzzy name matching → Match to politicians
    │   └─► Return matched trades array
    │
    └─► Step 3: Save to Database (Lambda)
        ├─► DynamoDB: Batch write trades
        ├─► Idempotency check (skip duplicates)
        └─► Return save summary
```

---

## IAM Permissions

**S3 Access:**
- Read/write to politician trades bucket
- Download forms, upload parsed data

**DynamoDB Access:**
- Batch write to politician-trades table
- Conditional writes for idempotency

**KMS Access:**
- Decrypt/encrypt S3 and DynamoDB data

---

## Monitoring

**CloudWatch Metrics:**
- Forms fetched per day
- Trades matched per day
- Trades saved per day
- Error rates per step
- Processing time per step

**CloudWatch Alarms:**
- Step Function execution failures
- High error rate (>5%)
- No trades saved (data issue)
- Processing time exceeds threshold

---

## Future Enhancements

1. **PDF Parsing:**
   - Integrate AWS Textract for accurate PDF parsing
   - Handle form-specific parsing logic

2. **Name Matching:**
   - Improve fuzzy matching algorithm
   - Build machine learning model for name matching
   - Manual review queue for low-confidence matches

3. **Historical Backfill:**
   - One-time migration job to process historical filings
   - Incremental backfill for missed dates

4. **Real-time Updates:**
   - Webhook from SEC (if available)
   - More frequent runs (every 6 hours)

5. **Dashboard Integration:**
   - New tile type: "Politician Trades"
   - Filters by politician, party, position, stock, buy/sell
   - Charts and visualizations

