"""
Lambda Function: LDA PAC Autocomplete Processor
Processes PAC names from SQS and maintains a sorted, deduplicated CSV in S3.

This Lambda:
1. Receives PAC names from SQS messages
2. Reads existing CSV from S3 (if it exists)
3. Merges new PAC names with existing ones (deduplicates)
4. Sorts the list alphabetically
5. Writes updated CSV back to S3
"""

import json
import csv
import os
import boto3
from io import StringIO
from typing import Set, List

# AWS clients
s3_client = boto3.client('s3')

# Environment variables
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME')
S3_KEY = os.environ.get('S3_KEY', 'autocomplete/pacs.csv')

def read_existing_csv(bucket: str, key: str) -> Set[str]:
    """Read existing PAC names from S3 CSV, return as a set for deduplication"""
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        csv_content = response['Body'].read().decode('utf-8')
        reader = csv.reader(StringIO(csv_content))
        # Skip header if present
        next(reader, None)
        # Collect all PAC names
        pac_names = set()
        for row in reader:
            if row and row[0].strip():  # First column is PAC name
                pac_names.add(row[0].strip())
        return pac_names
    except s3_client.exceptions.NoSuchKey:
        # CSV doesn't exist yet, return empty set
        return set()
    except Exception as e:
        print(f"⚠️ Error reading existing CSV: {str(e)}")
        return set()

def write_csv_to_s3(bucket: str, key: str, pac_names: List[str]):
    """Write sorted PAC names to S3 as CSV"""
    # Sort alphabetically
    sorted_pacs = sorted(pac_names)
    
    # Create CSV content
    csv_buffer = StringIO()
    writer = csv.writer(csv_buffer)
    writer.writerow(['pac_name'])  # Header
    for pac_name in sorted_pacs:
        writer.writerow([pac_name])
    
    csv_content = csv_buffer.getvalue()
    
    # Upload to S3
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=csv_content.encode('utf-8'),
        ContentType='text/csv',
        CacheControl='max-age=3600'  # Cache for 1 hour
    )
    
    print(f"✅ Wrote {len(sorted_pacs)} PAC names to s3://{bucket}/{key}")

def lambda_handler(event, context):
    """Process SQS messages containing PAC names"""
    if not S3_BUCKET_NAME:
        print("❌ S3_BUCKET_NAME environment variable not set")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'S3_BUCKET_NAME not configured'})
        }
    
    # Collect all PAC names from SQS messages
    new_pac_names = set()
    
    for record in event.get('Records', []):
        try:
            # Parse message body
            body = json.loads(record['body'])
            pac_name = body.get('pac_name', '').strip()
            
            if pac_name:
                new_pac_names.add(pac_name)
        except Exception as e:
            print(f"⚠️ Error processing SQS record: {str(e)}")
            continue
    
    if not new_pac_names:
        print("ℹ️ No new PAC names to process")
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'No new PAC names'})
        }
    
    print(f"📝 Processing {len(new_pac_names)} new PAC names")
    
    # Read existing PAC names from S3
    existing_pac_names = read_existing_csv(S3_BUCKET_NAME, S3_KEY)
    print(f"📖 Found {len(existing_pac_names)} existing PAC names in CSV")
    
    # Merge new with existing (set automatically deduplicates)
    all_pac_names = existing_pac_names.union(new_pac_names)
    
    # Calculate how many were actually new
    new_count = len(all_pac_names) - len(existing_pac_names)
    
    if new_count > 0:
        print(f"✨ Added {new_count} new PAC names (total: {len(all_pac_names)})")
        
        # Write updated CSV to S3
        write_csv_to_s3(S3_BUCKET_NAME, S3_KEY, list(all_pac_names))
    else:
        print(f"ℹ️ All PAC names already exist in CSV (no updates needed)")
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'PAC autocomplete CSV updated',
            'total_pacs': len(all_pac_names),
            'new_pacs': new_count
        })
    }

