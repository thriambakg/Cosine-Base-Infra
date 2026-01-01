"""
Lambda Function: LDA Autocomplete Processor
Processes autocomplete strings (PAC names, client names, lobbyist names, registrant names) 
from SQS and maintains sorted, deduplicated TXT files in S3.

This Lambda:
1. Receives autocomplete strings from SQS messages (with field_type)
2. Reads existing TXT file from S3 (if it exists)
3. Merges new strings with existing ones (deduplicates)
4. Sorts the list alphabetically
5. Writes updated TXT file back to S3 (one value per line, preserves commas and special characters)

Supports multiple field types:
- pac_name -> lists/pacs.txt
- client_name -> lists/client_names.txt
- lobbyist_name -> lists/lobbyist_names.txt
- registrant_name -> lists/registrant_names.txt
"""

import json
import os
import boto3
from typing import Set, List, Dict
from collections import defaultdict

# AWS clients
s3_client = boto3.client('s3')

# Environment variables
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME')

# Field type to S3 key mapping
FIELD_TYPE_TO_S3_KEY = {
    'pac_name': 'lists/pacs.txt',
    'client_name': 'lists/client_names.txt',
    'lobbyist_name': 'lists/lobbyist_names.txt',
    'registrant_name': 'lists/registrant_names.txt'
}

def clean_value(value: str) -> str:
    """
    Clean autocomplete value by removing surrounding quotes and normalizing whitespace.
    Preserves commas and other special characters as they come from the API.
    
    Args:
        value: Raw value string
    
    Returns:
        Cleaned value with surrounding quotes removed and whitespace normalized, but preserving commas and special characters
    """
    if not value:
        return ''
    
    # Strip whitespace first
    cleaned = value.strip()
    
    # Remove surrounding double quotes if present
    if cleaned.startswith('"') and cleaned.endswith('"'):
        cleaned = cleaned[1:-1]
    
    # Normalize whitespace (multiple spaces to single space)
    cleaned = ' '.join(cleaned.split())
    
    return cleaned.strip()

def read_existing_txt(bucket: str, key: str, field_name: str) -> Set[str]:
    """Read existing values from S3 TXT file (one value per line), return as a set for deduplication"""
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        txt_content = response['Body'].read().decode('utf-8')
        # Split by newlines and process each line
        values = set()
        for line in txt_content.split('\n'):
            line = line.strip()
            # Skip empty lines and header lines (if present)
            if line and line.lower() not in ['value', field_name]:
                cleaned_value = clean_value(line)
                if cleaned_value:
                    values.add(cleaned_value)
        return values
    except s3_client.exceptions.NoSuchKey:
        # TXT file doesn't exist yet, return empty set
        return set()
    except Exception as e:
        print(f"⚠️ Error reading existing TXT file: {str(e)}")
        return set()

def write_txt_to_s3(bucket: str, key: str, values: List[str], field_name: str):
    """
    Write sorted values to S3 as TXT file (one value per line).
    Preserves commas and special characters as they come from the API.
    """
    # Sort alphabetically
    sorted_values = sorted(values)
    
    # Write TXT file - one value per line
    # No header needed for TXT files, but we can add it for compatibility
    txt_lines = []
    
    for value in sorted_values:
        # Clean value (removes surrounding quotes, normalizes whitespace, but preserves commas)
        cleaned = clean_value(value) if value else ''
        if cleaned:
            txt_lines.append(cleaned)
    
    # Join with newlines
    txt_content = '\n'.join(txt_lines) + '\n'
    
    # Upload to S3
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=txt_content.encode('utf-8'),
        ContentType='text/plain',
        CacheControl='max-age=3600'  # Cache for 1 hour
    )
    
    print(f"✅ Wrote {len(sorted_values)} {field_name} values to s3://{bucket}/{key}")

def lambda_handler(event, context):
    """Process SQS messages containing autocomplete strings (PAC names, client names, etc.)"""
    if not S3_BUCKET_NAME:
        print("❌ S3_BUCKET_NAME environment variable not set")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'S3_BUCKET_NAME not configured'})
        }
    
    # Collect all values by field type from SQS messages
    # Structure: {field_type: set of values}
    new_values_by_type: Dict[str, Set[str]] = defaultdict(set)
    
    for record in event.get('Records', []):
        try:
            # Parse message body
            body = json.loads(record['body'])
            
            # Support both old format (pac_name) and new format (field_type + value)
            if 'field_type' in body and 'value' in body:
                # New format: {"field_type": "client_name", "value": "ACME Corp"}
                field_type = body.get('field_type', '').strip()
                value = clean_value(body.get('value', ''))
                
                if field_type and value and field_type in FIELD_TYPE_TO_S3_KEY:
                    new_values_by_type[field_type].add(value)
            else:
                # Legacy format: {"pac_name": "BakePAC"} (backward compatibility)
                pac_name = clean_value(body.get('pac_name', ''))
                if pac_name:
                    new_values_by_type['pac_name'].add(pac_name)
        except Exception as e:
            print(f"⚠️ Error processing SQS record: {str(e)}")
            continue
    
    if not new_values_by_type:
        print("ℹ️ No new autocomplete values to process")
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'No new values'})
        }
    
    # Process each field type separately
    results = {}
    
    for field_type, new_values in new_values_by_type.items():
        if not new_values:
            continue
        
        s3_key = FIELD_TYPE_TO_S3_KEY.get(field_type)
        if not s3_key:
            print(f"⚠️ Unknown field_type: {field_type}, skipping")
            continue
        
        print(f"📝 Processing {len(new_values)} new {field_type} values")
        
        # Read existing values from S3
        existing_values = read_existing_txt(S3_BUCKET_NAME, s3_key, field_type)
        print(f"📖 Found {len(existing_values)} existing {field_type} values in TXT file")
        
        # Merge new with existing (set automatically deduplicates)
        all_values = existing_values.union(new_values)
        
        # Calculate how many were actually new
        new_count = len(all_values) - len(existing_values)
        
        if new_count > 0:
            print(f"✨ Added {new_count} new {field_type} values (total: {len(all_values)})")
            
            # Write updated TXT file to S3
            write_txt_to_s3(S3_BUCKET_NAME, s3_key, list(all_values), field_type)
            
            results[field_type] = {
                'total': len(all_values),
                'new': new_count
            }
        else:
            print(f"ℹ️ All {field_type} values already exist in TXT file (no updates needed)")
            results[field_type] = {
                'total': len(all_values),
                'new': 0
            }
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'Autocomplete TXT files updated',
            'results': results
        })
    }

