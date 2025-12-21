# LDA Constants Indexing Analysis

## Current State

### Already Indexed (via GSIs)
1. ✅ **Registrant Names** → `RegistrantPostedDateIndex` (hash_key: `registrant_name`)
2. ✅ **Client Names** → `ClientPostedDateIndex` (hash_key: `client_name`)
3. ✅ **Lobbyist Names** → `LobbyistPostedDateIndex` (hash_key: `lobbyist_name`)
4. ✅ **General Issue Codes** → `GeneralIssueCodePostedDateIndex` (hash_key: `general_issue_code`)
5. ✅ **Filing Types** → `ReportTypePostedDateIndex` (hash_key: `report_type` = `filing_type`)

### Not Currently Indexed (Should Add)

1. ❌ **Government Entity IDs** - In `lobbying_activities` → `government_entities` array
2. ❌ **Contribution Item Types** - In `contribution_items` → `contribution_type`

## CSV Field Mappings to Constants

### 1. Filing Types (`filing_types_constants.json`)
**CSV Field**: `filing_type` / `filing_type_display`
**Example Values**:
- `"Q4"` → `"4th Quarter - Report"`
- `"4TY"` → `"4th Quarter - Termination (No Activity)"`
- `"RA"` → `"Registration - Amendment"`
- `"YY"` → `"Year-End Report"`

**Status**: ✅ Already indexed as `report_type` (GSI: `ReportTypePostedDateIndex`)

**Autocomplete**: Query `ReportTypePostedDateIndex` GSI to get unique `report_type` values

### 2. General Issue Codes (`general_issues_constants.json`)
**CSV Field**: `lobbying_activities` → `general_issue_code` / `general_issue_code_display`
**Example Values**:
- `"AGR"` → `"Agriculture"`
- `"CAW"` → `"Clean Air and Water (quality)"`
- `"CSP"` → `"Consumer Issues/Safety/Products"`
- `"FOO"` → `"Food Industry (safety, labeling, etc.)"`

**Status**: ✅ Already indexed (GSI: `GeneralIssueCodePostedDateIndex`)

**Autocomplete**: Query `GeneralIssueCodePostedDateIndex` GSI to get unique `general_issue_code` values

### 3. Government Entities (`government_entities_constants.json`)
**CSV Field**: `lobbying_activities` → `government_entities` array
**Structure**:
```json
{
  "government_entities": [
    {"id": 23, "name": "Agriculture, Dept of (USDA)"},
    {"id": 45, "name": "Commodity Futures Trading Commission (CFTC)"},
    {"id": 49, "name": "Environmental Protection Agency (EPA)"}
  ]
}
```

**Status**: ❌ NOT indexed - Need to add GSI

**Recommendation**: 
- Extract first government entity ID from first lobbying activity
- Add field: `government_entity_id` (N type)
- Create GSI: `GovernmentEntityPostedDateIndex` (hash_key: `government_entity_id`, range_key: `dt_posted`)

**Autocomplete**: Query `GovernmentEntityPostedDateIndex` GSI to get unique `government_entity_id` values, then map to names via constants JSON

### 4. Contribution Item Types (`contribution_item_types_constants.json`)
**CSV Field**: `contribution_items` → `contribution_type` / `contribution_type_display`
**Example Values**:
- `"feca"` → `"FECA"`
- `"he"` → `"Honorary Expenses"`
- `"me"` → `"Meeting Expenses"`

**Status**: ❌ NOT indexed - Need to add field

**Recommendation**:
- Extract first contribution type from `contribution_items`
- Add field: `contribution_item_type` (S type)
- Create GSI: `ContributionItemTypePostedDateIndex` (hash_key: `contribution_item_type`, range_key: `dt_posted`)

**Autocomplete**: Query `ContributionItemTypePostedDateIndex` GSI to get unique `contribution_item_type` values

## Autocomplete Strategy

### Option 1: Query Existing GSIs (Recommended)
Instead of building a separate CSV, query the DynamoDB table's GSIs to get unique values:

```python
# Get unique registrant names
response = table.query(
    IndexName='RegistrantPostedDateIndex',
    Select='SPECIFIC_ATTRIBUTES',
    ProjectionExpression='registrant_name',
    Limit=1000  # Get first 1000 unique values
)
# Deduplicate in application layer

# Get unique client names
response = table.query(
    IndexName='ClientPostedDateIndex',
    Select='SPECIFIC_ATTRIBUTES',
    ProjectionExpression='client_name',
    Limit=1000
)

# Get unique lobbyist names
response = table.query(
    IndexName='LobbyistPostedDateIndex',
    Select='SPECIFIC_ATTRIBUTES',
    ProjectionExpression='lobbyist_name',
    Limit=1000
)

# Get unique general issue codes
response = table.query(
    IndexName='GeneralIssueCodePostedDateIndex',
    Select='SPECIFIC_ATTRIBUTES',
    ProjectionExpression='general_issue_code',
    Limit=1000
)
```

**Pros**:
- No separate CSV needed
- Always up-to-date with latest data
- Uses existing infrastructure

**Cons**:
- Requires multiple queries
- May need pagination for large datasets
- DynamoDB query limits (1MB per query)

### Option 2: Scan with Projection (For Small Datasets)
```python
# Scan table with projection to get unique values
response = table.scan(
    ProjectionExpression='registrant_name, client_name, lobbyist_name, general_issue_code',
    Limit=1000
)
# Deduplicate in application layer
```

**Pros**: Single query
**Cons**: Expensive for large tables, limited to 1MB per scan

### Option 3: Pre-built CSV (Original Plan)
Build CSV from SQS messages, store in S3, download on frontend load.

**Pros**: Fast client-side search, no API calls
**Cons**: Requires separate infrastructure, may be stale

## Recommended Approach

**For Autocomplete**: Use **Option 1** (Query GSIs) because:
1. We already have the GSIs
2. Data is always current
3. No additional infrastructure needed
4. Can cache results in frontend

**For Constants Mapping**: 
- Store constants JSON files in S3 (already fetched)
- Frontend loads constants JSON on app load
- When user selects a code/ID, map to display name via constants

## Implementation Plan

### Phase 1: Add Missing Indexes
1. Add `government_entity_id` extraction to Glue script
2. Add `contribution_item_type` extraction to Glue script
3. Add GSIs to Terraform:
   - `GovernmentEntityPostedDateIndex`
   - `ContributionItemTypePostedDateIndex`

### Phase 2: Autocomplete API Endpoint
Create Lambda function that:
1. Queries each GSI to get unique values
2. Deduplicates results
3. Returns sorted list for autocomplete
4. Caches results (e.g., 1 hour TTL)

### Phase 3: Frontend Integration
1. Load constants JSON files from S3
2. Call autocomplete API for suggestions
3. Map codes/IDs to display names using constants

## CSV Row Analysis

Based on `selected (9).csv`, here's what can be mapped:

| CSV Field | Constants JSON | Index Status | Action Needed |
|-----------|---------------|--------------|---------------|
| `filing_type` | `filing_types_constants.json` | ✅ Indexed | None - use existing GSI |
| `general_issue_code` | `general_issues_constants.json` | ✅ Indexed | None - use existing GSI |
| `lobbying_activities[].government_entities[].id` | `government_entities_constants.json` | ❌ Not indexed | Add `government_entity_id` field + GSI |
| `contribution_items[].contribution_type` | `contribution_item_types_constants.json` | ❌ Not indexed | Add `contribution_item_type` field + GSI |



