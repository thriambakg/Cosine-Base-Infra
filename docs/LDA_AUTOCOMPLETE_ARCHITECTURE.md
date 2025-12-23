# LDA Autocomplete Architecture

## Overview

This architecture provides fast, client-side autocomplete for LDA search fields without hitting the LDA API (120/min rate limit). It uses pre-generated CSV files stored in S3.

## Architecture Components

### 1. Constants Fetching (Glue Job Start)
- **When**: At the start of each Glue job run
- **What**: Fetches 4 constants lists from LDA API:
  - General Issues (`/api/v1/constants/filing/lobbyingactivityissues/`)
  - Government Entities (`/api/v1/constants/filing/governmententities/`)
  - Contribution Item Types (`/api/v1/constants/contribution/itemtypes/`)
  - Filing Types (`/api/v1/constants/filing/filingtypes/`)
- **Output**: JSON files stored in S3 for frontend use

### 2. Autocomplete String Extraction (Glue Job)
- **When**: During processing of each filing/contribution
- **What**: Extracts all searchable strings:
  - `registrant_name`
  - `client_name`
  - `lobbyist_name`
  - `general_issue_code_display` (from lobbying activities)
  - Government entity names (from lobbying activities)
- **Action**: Sends batches of strings to SQS queue

### 3. SQS Queue
- **Name**: `lda-autocomplete-strings`
- **Purpose**: Decouple Glue job from Lambda processing
- **Message Format**: `{"strings": ["string1", "string2", ...]}`

### 4. Lambda Processor
- **Function**: `lda-autocomplete-processor`
- **Trigger**: SQS event source mapping
- **Actions**:
  1. Receives batches of strings from SQS
  2. Deduplicates against existing S3 CSV
  3. Maintains in-memory set of unique strings
  4. Periodically writes sorted CSV to S3
  5. Final flush when queue is empty

### 5. S3 Storage
- **Constants CSVs**: `s3://bucket/lda-autocomplete/constants/*.csv`
  - `general_issues.csv`
  - `government_entities.csv`
  - `contribution_item_types.csv`
  - `filing_types.csv`
- **Autocomplete Strings**: `s3://bucket/lda-autocomplete/autocomplete_strings.csv`
  - Single column: `string`
  - Sorted alphabetically (case-insensitive)
  - Deduplicated

## Frontend Integration

### Option 1: CSV + Binary Search (Recommended for < 100K strings)
- Download CSV on app load
- Parse into sorted array
- Use binary search for autocomplete
- **Performance**: O(log n) lookup, very fast
- **Limitation**: Memory usage (~1MB per 100K strings)

### Option 2: CSV + Trie Data Structure
- Download CSV on app load
- Build Trie (prefix tree) in memory
- Use Trie for autocomplete
- **Performance**: O(m) where m = query length, instant results
- **Memory**: Higher than binary search but still reasonable

### Option 3: S3 + CloudFront + Client-Side Caching
- Serve CSV via CloudFront
- Cache in browser localStorage/sessionStorage
- Update cache periodically (daily/weekly)
- **Performance**: Fast after initial load
- **Scalability**: Handles large datasets

### Option 4: DynamoDB GSI (For > 1M strings)
- Store strings in DynamoDB with GSI
- Query with `begins_with` filter
- **Performance**: Fast, scalable
- **Cost**: Higher than S3

### Option 5: Elasticsearch/OpenSearch (For complex search)
- Index all strings in OpenSearch
- Use fuzzy matching, typo tolerance
- **Performance**: Excellent for complex queries
- **Cost**: Higher, requires cluster

## Implementation Steps

1. ✅ Create Lambda function for processing autocomplete strings
2. ✅ Create SQS queue
3. ✅ Add Terraform resources
4. ⏳ Modify Glue script to:
   - Fetch constants at start
   - Extract searchable strings
   - Send to SQS
5. ⏳ Create CSV generator script for constants
6. ⏳ Add frontend autocomplete component

## File Structure

```
s3://bucket/lda-autocomplete/
├── constants/
│   ├── general_issues.csv
│   ├── government_entities.csv
│   ├── contribution_item_types.csv
│   └── filing_types.csv
└── autocomplete_strings.csv
```

## CSV Format

**Constants CSVs** (varies by type):
- `general_issues.csv`: `value,name`
- `government_entities.csv`: `id,name`
- `contribution_item_types.csv`: `value,name`
- `filing_types.csv`: `value,name`

**Autocomplete Strings CSV**:
```csv
string
ACME Corporation
Agriculture
Department of Defense
John Smith
...
```

## Performance Considerations

- **CSV Size**: ~1MB per 100K strings (compressed: ~200KB)
- **Load Time**: < 1 second for 100K strings
- **Search Time**: < 10ms for binary search
- **Memory**: ~10MB for 100K strings in memory

## Recommendations

For **< 100K strings**: Use CSV + Binary Search (Option 1)
For **100K - 1M strings**: Use CSV + Trie (Option 2) or S3 + CloudFront (Option 3)
For **> 1M strings**: Use DynamoDB GSI (Option 4) or OpenSearch (Option 5)





