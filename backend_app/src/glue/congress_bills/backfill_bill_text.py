"""
AWS Glue Job: Congress Bills Bill Text Backfill
Temporary job to backfill bill_text_html_s3_key for existing bills in DynamoDB.

This job:
1. Scans all items in the congress-bills DynamoDB table
2. For each item without bill_text_html_s3_key:
   - Fetches bill text versions from Congress.gov API
   - Downloads HTML version of the bill text
   - Stores it in S3 under billtext/{bill_id}.html
   - Updates the DynamoDB item with bill_text_html_s3_key

This is a one-time migration job.
"""

import sys
import json
import logging
import time
import requests
import re
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Tuple

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

import boto3

# ============================================================================
# Configuration
# ============================================================================

# Get required job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'PROJECT_NAME',
    'ENVIRONMENT',
    'CONGRESS_API_BASE_URL',
    'BILLS_TABLE_NAME',
    'S3_BUCKET_NAME',
    'REQUEST_TIMEOUT'
])

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def log_print(message):
    """Print to both logger and stdout for maximum visibility in Glue"""
    logger.info(message)
    print(message, file=sys.stdout, flush=True)

log_print("=" * 80)
log_print("✅ Congress Bills Bill Text Backfill Glue Job - Script Loaded Successfully")
log_print("=" * 80)

# Environment variables
PROJECT_NAME = args.get('PROJECT_NAME', 'cosine')
ENVIRONMENT = args.get('ENVIRONMENT', 'staging')
API_BASE_URL = args.get('CONGRESS_API_BASE_URL', 'https://api.congress.gov/v3')
BILLS_TABLE_NAME = args.get('BILLS_TABLE_NAME')
S3_BUCKET_NAME = args.get('S3_BUCKET_NAME')
REQUEST_TIMEOUT = int(args.get('REQUEST_TIMEOUT', '30'))
MAX_RETRIES = int(args.get('MAX_RETRIES', '5'))
RETRY_DELAY = int(args.get('RETRY_DELAY', '2'))

# AWS clients
dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')
secrets_client = boto3.client('secretsmanager')
bills_table = dynamodb.Table(BILLS_TABLE_NAME) if BILLS_TABLE_NAME else None

# ============================================================================
# Helper Functions
# ============================================================================

def get_congress_api_key() -> str:
    """Get Congress.gov API key from AWS Secrets Manager"""
    try:
        secret_name = f"{PROJECT_NAME}-congress-api-{ENVIRONMENT}"
        response = secrets_client.get_secret_value(SecretId=secret_name)
        secret_data = json.loads(response['SecretString'])
        return secret_data['api_key']
    except Exception as e:
        log_print(f"❌ Error retrieving Congress API key from Secrets Manager: {str(e)}")
        raise ValueError(f"Failed to retrieve Congress API key from Secrets Manager: {str(e)}")

def make_api_request(url: str, params: Dict[str, Any], api_key: str, retries: int = MAX_RETRIES) -> Optional[Dict]:
    """Make API request with retry logic and exponential backoff for rate limiting."""
    if "api_key" not in params:
        params["api_key"] = api_key
    for attempt in range(retries):
        try:
            response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            
            # Handle 429 Too Many Requests with exponential backoff
            if response.status_code == 429:
                if attempt < retries - 1:
                    # Exponential backoff: 2^attempt seconds, with a minimum of 5 seconds for 429
                    wait_time = max(5, (2 ** attempt) * RETRY_DELAY)
                    log_print(f"      ⚠️ Rate limited (429) - waiting {wait_time}s before retry {attempt + 1}/{retries}")
                    time.sleep(wait_time)
                    continue
                else:
                    log_print(f"      ❌ Rate limited (429) after {retries} attempts")
                    return None
            
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            if attempt < retries - 1:
                # For other HTTP errors, use linear backoff
                wait_time = RETRY_DELAY * (attempt + 1)
                log_print(f"      ⚠️ Request failed (attempt {attempt + 1}/{retries}): {str(e)[:100]}")
                time.sleep(wait_time)
            else:
                log_print(f"      ❌ Request failed after {retries} attempts: {str(e)[:100]}")
                return None
        except requests.exceptions.RequestException as e:
            if attempt < retries - 1:
                wait_time = RETRY_DELAY * (attempt + 1)
                log_print(f"      ⚠️ Request failed (attempt {attempt + 1}/{retries}): {str(e)[:100]}")
                time.sleep(wait_time)
            else:
                log_print(f"      ❌ Request failed after {retries} attempts: {str(e)[:100]}")
                return None
    return None

def fetch_bill_text_versions(congress: int, bill_type: str, bill_number: int, api_key: str) -> List[Dict]:
    """Fetch all text versions available for a bill."""
    url = f"{API_BASE_URL}/bill/{congress}/{bill_type.lower()}/{bill_number}/text"
    params = {"format": "json"}
    
    data = make_api_request(url, params, api_key)
    if not data:
        return []
    
    # Handle different response structures
    text_versions = []
    if isinstance(data, list):
        text_versions = data
    elif "textVersions" in data:
        versions_data = data["textVersions"]
        if isinstance(versions_data, dict):
            text_versions = versions_data.get("item", [])
        elif isinstance(versions_data, list):
            text_versions = versions_data
    
    return text_versions if isinstance(text_versions, list) else []

def download_bill_text_file(text_url: str, retries: int = MAX_RETRIES) -> Optional[bytes]:
    """Download bill text file (XML/HTML) from Congress.gov with exponential backoff for rate limiting."""
    for attempt in range(retries):
        try:
            response = requests.get(text_url, timeout=REQUEST_TIMEOUT * 2)
            
            # Handle 429 Too Many Requests with exponential backoff
            if response.status_code == 429:
                if attempt < retries - 1:
                    # Exponential backoff: 2^attempt seconds, with a minimum of 10 seconds for 429
                    wait_time = max(10, (2 ** attempt) * RETRY_DELAY * 2)
                    log_print(f"      ⚠️ Rate limited (429) - waiting {wait_time}s before retry {attempt + 1}/{retries}")
                    time.sleep(wait_time)
                    continue
                else:
                    log_print(f"      ❌ Rate limited (429) after {retries} attempts")
                    return None
            
            response.raise_for_status()
            return response.content
        except requests.exceptions.HTTPError as e:
            if attempt < retries - 1:
                # For other HTTP errors, use linear backoff
                wait_time = RETRY_DELAY * (attempt + 1)
                log_print(f"      ⚠️ Download failed (attempt {attempt + 1}/{retries}): {str(e)[:100]}")
                time.sleep(wait_time)
            else:
                log_print(f"      ❌ Download failed after {retries} attempts: {str(e)[:100]}")
                return None
        except requests.exceptions.RequestException as e:
            if attempt < retries - 1:
                wait_time = RETRY_DELAY * (attempt + 1)
                log_print(f"      ⚠️ Download failed (attempt {attempt + 1}/{retries}): {str(e)[:100]}")
                time.sleep(wait_time)
            else:
                log_print(f"      ❌ Download failed after {retries} attempts: {str(e)[:100]}")
                return None
    return None

def store_bill_text_to_s3(bill_id: str, text_content: bytes) -> str:
    """
    Store bill text HTML file to S3 in billtext/ folder.
    
    Args:
        bill_id: Bill ID (e.g., "119-HR-303")
        text_content: Bill text HTML content as bytes
        
    Returns:
        S3 key where the file was stored
    """
    # Create S3 key: billtext/{bill_id}.html
    s3_key = f"billtext/{bill_id}.html"
    
    # Upload to S3
    s3_client.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=s3_key,
        Body=text_content,
        ContentType='text/html'
    )
    
    log_print(f"      💾 Stored HTML bill text for {bill_id} to S3: {s3_key} ({len(text_content):,} bytes)")
    return s3_key

def process_bill_item(item: Dict[str, Any], api_key: str) -> Tuple[bool, Optional[str]]:
    """
    Process a single bill item to fetch and store bill text.
    
    Returns:
        (success: bool, error_message: Optional[str])
    """
    bill_id = item.get('bill_id', 'unknown')
    
    # Skip if already has bill_text_html_s3_key
    if item.get('bill_text_html_s3_key'):
        log_print(f"      ⏭️  {bill_id} already has HTML bill text S3 key, skipping")
        return True, None
    
    try:
        # Extract congress, bill_type, bill_number from bill_id (format: "119-HR-303")
        parts = bill_id.split('-')
        if len(parts) != 3:
            return False, f"Invalid bill_id format: {bill_id}"
        
        congress = int(parts[0])
        bill_type = parts[1]
        bill_number = int(parts[2])
        
        log_print(f"      📋 Processing {bill_id}...")
        
        # Fetch text versions
        text_versions = fetch_bill_text_versions(congress, bill_type, bill_number, api_key)
        if not text_versions:
            log_print(f"      ⚠️  No text versions found for {bill_id}, setting key to empty")
            # Update DynamoDB with empty key
            if not item.get('bill_text_html_s3_key'):
                bills_table.update_item(
                    Key={'bill_id': bill_id},
                    UpdateExpression="SET bill_text_html_s3_key = :html_key",
                    ExpressionAttributeValues={':html_key': ""}
                )
                log_print(f"      ✅ Updated {bill_id} with empty bill text S3 key")
            return True, None
        
        # Find the "Introduced" version first, fallback to first available
        introduced_version = None
        for version in text_versions:
            version_type = version.get("type", "").lower()
            if "introduced" in version_type:
                introduced_version = version
                break
        
        selected_version = introduced_version if introduced_version else text_versions[0]
        
        # Get the formats
        formats = selected_version.get("formats", {})
        format_items = []
        
        # Handle different formats structures
        if isinstance(formats, list):
            # formats is already a list
            format_items = formats
        elif isinstance(formats, dict):
            # formats is a dict, might have "item" key or be the list itself
            if "item" in formats:
                item_data = formats["item"]
                if isinstance(item_data, list):
                    format_items = item_data
                elif isinstance(item_data, dict):
                    # Single item wrapped in dict
                    format_items = [item_data]
            else:
                # Check if dict values are format items
                format_items = list(formats.values()) if formats else []
        
        if format_items:
            html_url = None
            
            # Find HTML URL (Formatted Text)
            for fmt_item in format_items:
                if isinstance(fmt_item, dict):
                    fmt_type = fmt_item.get("type", "")
                    fmt_url = fmt_item.get("url")
                    
                    if fmt_type == "Formatted Text" and fmt_url and not html_url:
                        html_url = fmt_url
            
            # Download and store HTML (with error handling)
            if not item.get('bill_text_html_s3_key'):
                if html_url:
                    try:
                        log_print(f"      📄 Downloading HTML bill text from {html_url}...")
                        html_content = download_bill_text_file(html_url)
                        if html_content:
                            bill_text_html_s3_key = store_bill_text_to_s3(bill_id, html_content)
                            bills_table.update_item(
                                Key={'bill_id': bill_id},
                                UpdateExpression="SET bill_text_html_s3_key = :html_key",
                                ExpressionAttributeValues={':html_key': bill_text_html_s3_key}
                            )
                            log_print(f"      ✅ Stored HTML bill text to S3: {bill_text_html_s3_key}")
                            return True, None
                        else:
                            log_print(f"      ⚠️  Failed to download HTML bill text, setting key to empty")
                            bills_table.update_item(
                                Key={'bill_id': bill_id},
                                UpdateExpression="SET bill_text_html_s3_key = :html_key",
                                ExpressionAttributeValues={':html_key': ""}
                            )
                            return True, None
                    except Exception as e:
                        log_print(f"      ⚠️  Error downloading HTML bill text: {str(e)}, setting key to empty")
                        bills_table.update_item(
                            Key={'bill_id': bill_id},
                            UpdateExpression="SET bill_text_html_s3_key = :html_key",
                            ExpressionAttributeValues={':html_key': ""}
                        )
                        return True, None
                else:
                    log_print(f"      ⚠️  No HTML URL found, setting key to empty")
                    bills_table.update_item(
                        Key={'bill_id': bill_id},
                        UpdateExpression="SET bill_text_html_s3_key = :html_key",
                        ExpressionAttributeValues={':html_key': ""}
                    )
                    return True, None
            else:
                # Key already exists, nothing to update
                return True, None
        else:
            # No format items found, set key to empty
            log_print(f"      ⚠️  No format items found, setting key to empty")
            bills_table.update_item(
                Key={'bill_id': bill_id},
                UpdateExpression="SET bill_text_html_s3_key = :html_key",
                ExpressionAttributeValues={':html_key': ""}
            )
            log_print(f"      ✅ Updated {bill_id} with empty bill text S3 key")
            return True, None
        
    except Exception as e:
        error_msg = f"Error processing {bill_id}: {str(e)}"
        log_print(f"      ❌ {error_msg}")
        return False, error_msg

# ============================================================================
# Main Execution
# ============================================================================

def main():
    log_print("=" * 80)
    log_print("Congress Bills Bill Text Backfill Glue Job - Starting")
    log_print("=" * 80)
    
    if not BILLS_TABLE_NAME:
        raise ValueError("BILLS_TABLE_NAME job parameter not set")
    
    # Get API key from Secrets Manager
    try:
        api_key = get_congress_api_key()
        log_print("✅ Retrieved Congress API key from Secrets Manager")
    except Exception as e:
        log_print(f"❌ Failed to retrieve API key: {str(e)}")
        raise
    
    # Scan all items from DynamoDB
    log_print("📋 Scanning DynamoDB table for bills without bill_text_html_s3_key...")
    log_print("-" * 80)
    
    items_to_process = []
    last_evaluated_key = None
    scan_count = 0
    
    while True:
        scan_params = {}
        if last_evaluated_key:
            scan_params['ExclusiveStartKey'] = last_evaluated_key
        
        response = bills_table.scan(**scan_params)
        items = response.get('Items', [])
        
        # Filter items that don't have bill_text_html_s3_key
        for item in items:
            bill_text_html_s3_key = item.get('bill_text_html_s3_key', '')
            if not bill_text_html_s3_key or bill_text_html_s3_key == '':
                items_to_process.append(item)
        
        scan_count += len(items)
        
        log_print(f"   📊 Scanned {scan_count} items, found {len(items_to_process)} items missing HTML bill text S3 key")
        
        last_evaluated_key = response.get('LastEvaluatedKey')
        if not last_evaluated_key:
            break
    
    log_print(f"\n✅ Total items to process: {len(items_to_process)}")
    log_print("")
    
    if len(items_to_process) == 0:
        log_print("✅ No items to process. All bills already have HTML bill text S3 key.")
        job.commit()
        return
    
    # Process items sequentially with backoff
    log_print("🔍 Processing bills and fetching bill text...")
    log_print("-" * 80)
    log_print(f"   📋 Processing {len(items_to_process)} bills sequentially with backoff...")
    
    processed_count = 0
    error_count = 0
    skipped_count = 0
    
    # Process bills sequentially
    for idx, bill_item in enumerate(items_to_process, 1):
        try:
            success, error_msg = process_bill_item(bill_item, api_key)
            skipped = (bill_item.get('bill_text_html_s3_key') is not None and bill_item.get('bill_text_html_s3_key') != '')
            
            if skipped:
                skipped_count += 1
            elif success:
                processed_count += 1
            else:
                error_count += 1
                if error_msg:
                    log_print(f"      ❌ {error_msg}")
        except Exception as e:
            error_count += 1
            log_print(f"      ❌ Exception processing bill {bill_item.get('bill_id', 'unknown')}: {str(e)}")
        
        if idx % 10 == 0:
            log_print(f"      ✅ Processed {idx}/{len(items_to_process)} bills... (success: {processed_count}, errors: {error_count}, skipped: {skipped_count})")
        
        # Rate limiting - be respectful to Congress.gov API
        # Delay between requests to avoid rate limiting
        if idx < len(items_to_process):  # Don't sleep after the last item
            time.sleep(1.0)  # 1 second delay between items
    
    log_print(f"\n✅ Processed {processed_count} bills successfully")
    if error_count > 0:
        log_print(f"⚠️ {error_count} bills had errors")
    if skipped_count > 0:
        log_print(f"⏭️  {skipped_count} bills were skipped (already have HTML bill text S3 key)")
    
    log_print("")
    log_print("=" * 80)
    log_print("✅ Backfill job completed successfully!")
    log_print("=" * 80)
    
    job.commit()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        error_msg = f"CRITICAL ERROR in Congress bills bill text backfill job: {str(e)}"
        error_traceback = traceback.format_exc()
        log_print(f"❌ {error_msg}")
        log_print(f"❌ Traceback:\n{error_traceback}")
        logger.error(f"❌ {error_msg}", exc_info=True)
        logger.error(f"❌ Traceback:\n{error_traceback}")
        raise



