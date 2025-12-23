# LDA Autocomplete Query Strategy

## Answer: Yes, Query Existing Table!

Since we're already indexing text fields in GSIs, we can **query the DynamoDB table directly** for autocomplete suggestions instead of building a separate CSV. This is more efficient and always up-to-date.

## Current GSI-Based Autocomplete Fields

### Already Available (No Changes Needed)
1. **Registrant Names** → `RegistrantPostedDateIndex`
2. **Client Names** → `ClientPostedDateIndex`
3. **Lobbyist Names** → `LobbyistPostedDateIndex`
4. **General Issue Codes** → `GeneralIssueCodePostedDateIndex`
5. **Filing Types** → `ReportTypePostedDateIndex` (uses `report_type` = `filing_type`)

### Missing (Need to Add)
1. **Government Entity IDs** → Need new GSI
2. **Contribution Item Types** → Need new GSI

## CSV Field Mappings to Constants JSON

### Mapping Table

| CSV Field Path | Constants JSON | Current Index | Action |
|----------------|----------------|---------------|--------|
| `filing_type` | `filing_types_constants.json` | ✅ `ReportTypePostedDateIndex` | Use existing GSI |
| `lobbying_activities[].general_issue_code` | `general_issues_constants.json` | ✅ `GeneralIssueCodePostedDateIndex` | Use existing GSI |
| `lobbying_activities[].government_entities[].id` | `government_entities_constants.json` | ❌ None | **Add GSI** |
| `contribution_items[].contribution_type` | `contribution_item_types_constants.json` | ❌ None | **Add GSI** |

## CSV Row Examples

### Row 2 (FILING) - Mappings Found:
```json
{
  "filing_type": "Q4",                    // → filing_types_constants.json ✅
  "filing_type_display": "4th Quarter - Report",
  "lobbying_activities": [
    {
      "general_issue_code": "AGR",        // → general_issues_constants.json ✅
      "general_issue_code_display": "Agriculture",
      "government_entities": [
        {"id": 23, "name": "Agriculture, Dept of (USDA)"},      // → government_entities_constants.json ❌
        {"id": 45, "name": "Commodity Futures Trading Commission (CFTC)"},
        {"id": 49, "name": "Environmental Protection Agency (EPA)"}
      ]
    }
  ]
}
```

### Row 4 (CONTRIBUTION) - Mappings Found:
```json
{
  "filing_type": "YY",                    // → filing_types_constants.json ✅
  "filing_type_display": "Year-End Report",
  "contribution_items": [
    {
      "contribution_type": "feca",        // → contribution_item_types_constants.json ❌
      "contribution_type_display": "FECA"
    }
  ]
}
```

## Recommended Implementation

### 1. Add Missing Indexes

**Government Entity ID**:
- Extract first `government_entity.id` from first `lobbying_activity`
- Add field: `government_entity_id` (N type)
- Add GSI: `GovernmentEntityPostedDateIndex`

**Contribution Item Type**:
- Extract first `contribution_type` from `contribution_items`
- Add field: `contribution_item_type` (S type)
- Add GSI: `ContributionItemTypePostedDateIndex`

### 2. Autocomplete Query Strategy

Create a Lambda function that queries all GSIs:

```python
def get_autocomplete_suggestions(field_type: str, query: str):
    """
    Query DynamoDB GSIs to get autocomplete suggestions
    
    field_type: 'registrant', 'client', 'lobbyist', 'issue_code', 
                'filing_type', 'government_entity', 'contribution_type'
    query: partial string to match
    """
    
    # Map field types to GSIs
    gsi_map = {
        'registrant': 'RegistrantPostedDateIndex',
        'client': 'ClientPostedDateIndex',
        'lobbyist': 'LobbyistPostedDateIndex',
        'issue_code': 'GeneralIssueCodePostedDateIndex',
        'filing_type': 'ReportTypePostedDateIndex',
        'government_entity': 'GovernmentEntityPostedDateIndex',
        'contribution_type': 'ContributionItemTypePostedDateIndex'
    }
    
    # Query GSI with begins_with filter
    # Note: DynamoDB doesn't support partial string matching on hash keys
    # So we need to scan the GSI or use a different approach
    
    # Option 1: Scan GSI with FilterExpression (for small datasets)
    # Option 2: Query all items, filter client-side
    # Option 3: Use DynamoDB Streams to maintain a separate autocomplete table
```

**Challenge**: DynamoDB hash keys require exact matches. For partial matching, we need to:
- Scan the GSI and filter client-side, OR
- Use a separate autocomplete table with a different key structure

### 3. Alternative: Separate Autocomplete Table

Create a lightweight table specifically for autocomplete:

```hcl
# Autocomplete table structure
hash_key: "field_type" (S)  # e.g., "registrant", "client", etc.
range_key: "value" (S)      # The actual string value
attributes:
  - field_type (S)
  - value (S)
  - display_name (S)  # Optional: for mapping codes to names
  - count (N)        # Optional: frequency for ranking
```

**Pros**:
- Fast prefix searches with `begins_with`
- Can store display names for code mapping
- Can rank by frequency

**Cons**:
- Additional table to maintain
- Need to update when new records are indexed

## Final Recommendation

### Short-term (Immediate):
1. ✅ Use existing GSIs for exact matches (user types full name/code)
2. ✅ Load constants JSON files in frontend for dropdowns
3. ✅ Query GSIs to get unique values for dropdown population

### Medium-term (Better UX):
1. Add missing GSIs (`government_entity_id`, `contribution_item_type`)
2. Create autocomplete Lambda that:
   - Scans GSIs with pagination
   - Filters client-side for partial matches
   - Returns top N matches
   - Caches results (1 hour TTL)

### Long-term (If Scale Demands):
1. Create separate autocomplete table
2. Use DynamoDB Streams to keep it updated
3. Support fuzzy matching and ranking

## Next Steps

1. **Add missing indexes** to Glue script and Terraform
2. **Create autocomplete Lambda** that queries GSIs
3. **Frontend**: Load constants JSON + call autocomplete API
4. **Skip SQS/CSV approach** - use direct GSI queries instead





