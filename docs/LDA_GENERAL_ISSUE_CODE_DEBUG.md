# LDA General Issue Code - Debugging Guide

## How `general_issue_code` is Stored

### Data Type
- **Type**: String (S) in DynamoDB
- **Value**: Short code like "AGR", "FOO", "DEF", "CAW", etc.
- **Source**: Extracted from `lobbying_activities[].general_issue_code`

### Extraction Flow

1. **In `extract_indexed_fields_filing()`** (lines 292-300):
   ```python
   lobbying_activities = filing.get('lobbying_activities', [])
   if lobbying_activities:
       for activity in lobbying_activities:
           general_issue_code = activity.get('general_issue_code')
           if general_issue_code:
               indexed['general_issue_code'] = general_issue_code
               indexed['general_issue_code_display'] = activity.get('general_issue_code_display')
               break  # Use first issue code
   ```

2. **In `save_filing_to_dynamodb()`** (line 460):
   ```python
   item.update(indexed_fields)  # Adds general_issue_code to item
   ```

3. **Conditional Check** (lines 544-548):
   ```python
   if not indexed_fields.get('general_issue_code'):
       item.pop('general_issue_code', None)  # Remove if not present
   ```

### GSI Configuration

**Terraform** (line 3220-3226):
```hcl
{
  name            = "GeneralIssueCodePostedDateIndex"
  hash_key        = "general_issue_code"  # String type
  range_key       = "dt_posted"
  projection_type = "KEYS_ONLY"
}
```

## Why You Might Not See It

### 1. Table Created Before GSI Was Added
**Symptom**: Field exists in items, but GSI doesn't exist
**Solution**: Recreate table or add GSI manually

### 2. Glue Job Hasn't Run Since Code Change
**Symptom**: Old records don't have the field
**Solution**: Re-run Glue job to reindex existing records

### 3. Filings Without Lobbying Activities
**Symptom**: Some filings don't have `general_issue_code`
**Expected**: Only filings with `lobbying_activities` will have this field

### 4. Field Exists But GSI Not Populated
**Symptom**: Field in item, but can't query GSI
**Solution**: Check if GSI exists in table definition

## Verification Steps

### Check if Field Exists in Items
```python
import boto3

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('lda-filings')

# Get a sample filing
response = table.get_item(
    Key={'PK': 'FILING#26bb350c-0d4e-4216-a780-ae10c89b17e0', 'SK': 'FILING#26bb350c-0d4e-4216-a780-ae10c89b17e0'}
)

item = response.get('Item', {})
print(f"general_issue_code: {item.get('general_issue_code')}")
print(f"general_issue_code_display: {item.get('general_issue_code_display')}")
```

### Check if GSI Exists
```python
table = dynamodb.Table('lda-filings')
table.load()

gsis = [gsi['IndexName'] for gsi in table.global_secondary_indexes]
print(f"GSIs: {gsis}")
# Should include: 'GeneralIssueCodePostedDateIndex'
```

### Query GSI
```python
# Query by general issue code
response = table.query(
    IndexName='GeneralIssueCodePostedDateIndex',
    KeyConditionExpression=Key('general_issue_code').eq('AGR')
)
print(f"Found {len(response['Items'])} filings with AGR")
```

## Expected Values

Based on your CSV, filings should have values like:
- `"AGR"` (Agriculture)
- `"CAW"` (Clean Air and Water)
- `"CSP"` (Consumer Issues/Safety/Products)
- `"FOO"` (Food Industry)
- `"DEF"` (Defense)
- etc.

## Troubleshooting

If the field is missing:
1. Check Glue job logs for extraction errors
2. Verify `lobbying_activities` exists in API response
3. Check if table has the GSI defined
4. Re-run Glue job to reindex




