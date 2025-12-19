# LDA General Issue Code Indexing

## Overview

The LDA filings contain `lobbying_activities` which include a `general_issue_code` field that categorizes the type of lobbying activity. This field has been indexed to enable efficient searching and filtering by issue type.

## Data Structure

### Filings (LD-1, LD-2)
- **Field**: `general_issue_code` (e.g., "AGR", "FOO", "DEF", "CAW", "CSP", "CDT", "ECN", "ENG", "TRU", "URB", "GOV", "RRR", "ACC", "TRA", "AUT", "MAN", "ENV", "UTI")
- **Display Field**: `general_issue_code_display` (e.g., "Agriculture", "Food Industry (safety, labeling, etc.)", "Defense")
- **Source**: Extracted from the first `lobbying_activity` in the `lobbying_activities` array
- **Indexed**: Yes - `GeneralIssueCodePostedDateIndex` GSI (hash_key: `general_issue_code`, range_key: `dt_posted`)

### Contributions (LD-203)
- **Field**: `general_issue_code` - **NOT APPLICABLE**
- **Reason**: Contributions don't have `lobbying_activities` - they contain `contribution_items` (political contributions to candidates/PACs)
- **Indexed**: No - field is omitted for contributions

## DynamoDB Index

### GSI: `GeneralIssueCodePostedDateIndex`
- **Hash Key**: `general_issue_code` (S)
- **Range Key**: `dt_posted` (S)
- **Projection Type**: `KEYS_ONLY`
- **Use Case**: Query filings by issue code and date range

### Example Query
```python
# Query all Agriculture (AGR) filings from 2024
response = table.query(
    IndexName='GeneralIssueCodePostedDateIndex',
    KeyConditionExpression=Key('general_issue_code').eq('AGR') & 
                          Key('dt_posted').between('2024-01-01', '2024-12-31')
)
```

## Autocomplete Constants Lists

The LDA API provides static lists for various constants. The script fetches all six:

1. **General Issues** (`/api/v1/constants/filing/lobbyingactivityissues/`)
2. **Government Entities** (`/api/v1/constants/filing/governmententities/`)
3. **Contribution Item Types** (`/api/v1/constants/contribution/itemtypes/`)
4. **Lobbyist Prefixes** (`/api/v1/constants/lobbyist/prefixes/`)
5. **Lobbyist Suffixes** (`/api/v1/constants/lobbyist/suffixes/`)
6. **Filing Types** (`/api/v1/constants/filing/filingtypes/`)

### Fetching the Constants Lists

Use the provided script to fetch and save all constants lists:
```bash
cd Cosine-Base-Infra/scripts/LDA
export LDA_API_KEY=your_api_key
python fetch_general_issues_constants.py --output-dir .
```

This will create six separate files:
- `general_issues_constants.json`
- `government_entities_constants.json`
- `contribution_item_types_constants.json`
- `lobbyist_prefixes_constants.json`
- `lobbyist_suffixes_constants.json`
- `filing_types_constants.json`

### Output Formats

**General Issues** (`general_issues_constants.json`):
```json
[
  {
    "name": "Agriculture",
    "value": "AGR"
  },
  {
    "name": "Food Industry (safety, labeling, etc.)",
    "value": "FOO"
  },
  ...
]
```

**Government Entities** (`government_entities_constants.json`):
```json
[
  {
    "id": 1,
    "name": "SENATE"
  },
  {
    "id": 2,
    "name": "HOUSE OF REPRESENTATIVES"
  },
  ...
]
```

**Contribution Item Types** (`contribution_item_types_constants.json`):
```json
[
  {
    "name": "FECA",
    "value": "feca"
  },
  ...
]
```

**Lobbyist Prefixes** (`lobbyist_prefixes_constants.json`):
```json
[
  {
    "name": "MR.",
    "value": "mr"
  },
  {
    "name": "MRS.",
    "value": "mrs"
  },
  ...
]
```

**Lobbyist Suffixes** (`lobbyist_suffixes_constants.json`):
```json
[
  {
    "name": "JR.",
    "value": "jr"
  },
  {
    "name": "SR.",
    "value": "sr"
  },
  ...
]
```

**Filing Types** (`filing_types_constants.json`):
```json
[
  {
    "name": "4th Quarter - Report",
    "value": "Q4"
  },
  {
    "name": "Year-End Report",
    "value": "YY"
  },
  ...
]
```

### Frontend Integration

1. **Load Constants**: Fetch or load the `general_issues_constants.json` file
2. **Autocomplete**: Use the list to populate a dropdown/autocomplete field
3. **Search**: When user selects an issue code, query the `GeneralIssueCodePostedDateIndex` GSI

## Implementation Details

### Glue Script Changes
- `extract_indexed_fields_filing()`: Extracts `general_issue_code` and `general_issue_code_display` from the first lobbying activity
- `save_filing_to_dynamodb()`: Sets `general_issue_code` field (omits if not present)
- `save_contribution_to_dynamodb()`: Explicitly removes `general_issue_code` field (not applicable)

### Terraform Changes
- Added `general_issue_code` attribute (type: S)
- Added `GeneralIssueCodePostedDateIndex` GSI

## Notes

- **Single Issue Code**: Currently, only the first `general_issue_code` from the first `lobbying_activity` is indexed. A filing can have multiple activities with different issue codes, but only the first is used for the GSI.
- **Contributions**: Contributions don't have issue codes - they track political contributions, not lobbying activities.
- **Constants List**: The constants list should be refreshed periodically (e.g., monthly) to capture any new issue codes added by the LDA.

