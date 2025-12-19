# LDA Autocomplete Implementation Plan

## Summary

This document outlines the implementation steps for the LDA autocomplete system that extracts searchable strings during Glue job processing and builds sorted CSV files for frontend autocomplete.

## Implementation Checklist

### Phase 1: Infrastructure (Terraform)
- [ ] Create SQS queue: `lda-autocomplete-strings`
- [ ] Create Lambda function: `lda-autocomplete-processor`
- [ ] Create SQS event source mapping
- [ ] Add IAM policies for Glue job to send to SQS
- [ ] Add IAM policies for Lambda to read from SQS and write to S3
- [ ] Add S3 bucket/key configuration

### Phase 2: Glue Script Modifications
- [ ] Add `AUTOCOMPLETE_SQS_URL` as optional parameter
- [ ] Add function `fetch_constants()` to fetch 4 constants lists at job start
- [ ] Add function `send_autocomplete_strings()` to send batches to SQS
- [ ] Add function `extract_searchable_strings()` to extract strings from filings/contributions
- [ ] Integrate string extraction into `process_all_filings()` and `process_all_contributions()`
- [ ] Save constants JSON files to S3

### Phase 3: Lambda Function
- [x] Create Lambda function to process SQS messages
- [ ] Handle deduplication
- [ ] Write sorted CSV to S3
- [ ] Handle incremental updates

### Phase 4: CSV Generator Script
- [ ] Create script to convert constants JSON to CSV
- [ ] Generate 4 CSV files for constants
- [ ] Upload to S3

### Phase 5: Frontend Integration
- [ ] Create autocomplete component
- [ ] Implement CSV loading
- [ ] Implement binary search or Trie
- [ ] Add caching strategy

## Key Functions to Add to Glue Script

### 1. Fetch Constants
```python
def fetch_constants(session: requests.Session) -> Dict:
    """Fetch constants lists from LDA API at job start"""
    constants = {
        'general_issues': [],
        'government_entities': [],
        'contribution_item_types': [],
        'filing_types': []
    }
    
    endpoints = {
        'general_issues': '/constants/filing/lobbyingactivityissues/',
        'government_entities': '/constants/filing/governmententities/',
        'contribution_item_types': '/constants/contribution/itemtypes/',
        'filing_types': '/constants/filing/filingtypes/'
    }
    
    for key, endpoint in endpoints.items():
        try:
            data = call_api(session, endpoint)
            constants[key] = data
            log_print(f"✅ Fetched {len(data)} {key}")
        except Exception as e:
            log_print(f"⚠️ Error fetching {key}: {e}")
    
    return constants
```

### 2. Save Constants to S3
```python
def save_constants_to_s3(constants: Dict):
    """Save constants JSON files to S3"""
    for key, data in constants.items():
        if data:
            s3_key = f"lda-autocomplete/constants/{key}.json"
            s3_client.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=s3_key,
                Body=json.dumps(data, indent=2).encode('utf-8'),
                ContentType='application/json'
            )
            log_print(f"💾 Saved {key} to s3://{S3_BUCKET_NAME}/{s3_key}")
```

### 3. Extract Searchable Strings
```python
def extract_searchable_strings(filing_or_contribution: Dict) -> List[str]:
    """Extract all searchable strings from a filing or contribution"""
    strings = []
    
    # Registrant name
    if filing_or_contribution.get('registrant', {}).get('name'):
        strings.append(filing_or_contribution['registrant']['name'])
    
    # Client name
    if filing_or_contribution.get('client', {}).get('name'):
        strings.append(filing_or_contribution['client']['name'])
    
    # Lobbyist names (from lobbying_activities for filings)
    if filing_or_contribution.get('lobbying_activities'):
        for activity in filing_or_contribution['lobbying_activities']:
            for lobbyist_obj in activity.get('lobbyists', []):
                lobbyist = lobbyist_obj.get('lobbyist', {})
                if lobbyist:
                    name_parts = [
                        lobbyist.get('prefix_display', ''),
                        lobbyist.get('first_name', ''),
                        lobbyist.get('middle_name', ''),
                        lobbyist.get('last_name', ''),
                        lobbyist.get('suffix_display', '')
                    ]
                    full_name = ' '.join(filter(None, name_parts))
                    if full_name:
                        strings.append(full_name)
            
            # General issue code display
            if activity.get('general_issue_code_display'):
                strings.append(activity['general_issue_code_display'])
            
            # Government entity names
            for entity in activity.get('government_entities', []):
                if entity.get('name'):
                    strings.append(entity['name'])
    
    # Lobbyist name (direct for contributions)
    if filing_or_contribution.get('lobbyist'):
        lobbyist = filing_or_contribution['lobbyist']
        name_parts = [
            lobbyist.get('prefix_display', ''),
            lobbyist.get('first_name', ''),
            lobbyist.get('middle_name', ''),
            lobbyist.get('last_name', ''),
            lobbyist.get('suffix_display', '')
        ]
        full_name = ' '.join(filter(None, name_parts))
        if full_name:
            strings.append(full_name)
    
    return list(set(strings))  # Deduplicate
```

### 4. Send Strings to SQS
```python
def send_autocomplete_strings(strings: List[str], sqs_url: Optional[str] = None):
    """Send batch of autocomplete strings to SQS"""
    if not sqs_url or not strings:
        return
    
    try:
        # Batch strings (SQS limit: 256KB per message)
        batch_size = 1000
        for i in range(0, len(strings), batch_size):
            batch = strings[i:i+batch_size]
            message_body = json.dumps({'strings': batch})
            
            sqs_client.send_message(
                QueueUrl=sqs_url,
                MessageBody=message_body
            )
        
        log_print(f"📤 Sent {len(strings)} strings to SQS")
    except Exception as e:
        log_print(f"⚠️ Error sending to SQS: {e}")
```

## Integration Points

### In `main()` function:
1. Fetch constants at start
2. Save constants to S3
3. Get AUTOCOMPLETE_SQS_URL from args

### In `process_all_filings()`:
- After processing each filing, extract strings and send to SQS

### In `process_all_contributions()`:
- After processing each contribution, extract strings and send to SQS

## Next Steps

1. Implement Terraform resources
2. Add functions to Glue script
3. Test with small dataset
4. Create CSV generator script
5. Deploy and test end-to-end

