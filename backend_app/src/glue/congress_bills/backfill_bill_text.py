"""
AWS Glue Job: Congress Bills Bill Text Backfill
Temporary job to backfill bill_text_s3_key for existing bills in DynamoDB.

This job:
1. Scans all items in the congress-bills DynamoDB table
2. For each item without bill_text_s3_key:
   - Fetches bill text versions from Congress.gov API
   - Downloads the bill text file (XML/HTML)
   - Stores it in S3
   - Updates the DynamoDB item with bill_text_s3_key

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
from concurrent.futures import ThreadPoolExecutor, as_completed

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
    """Make API request with retry logic."""
    if "api_key" not in params:
        params["api_key"] = api_key
    for attempt in range(retries):
        try:
            response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return response.json()
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
    """Download bill text file (XML/HTML) from Congress.gov."""
    for attempt in range(retries):
        try:
            response = requests.get(text_url, timeout=REQUEST_TIMEOUT * 2)
            response.raise_for_status()
            return response.content
        except requests.exceptions.RequestException as e:
            if attempt < retries - 1:
                wait_time = RETRY_DELAY * (attempt + 1)
                log_print(f"      ⚠️ Download failed (attempt {attempt + 1}/{retries}): {str(e)[:100]}")
                time.sleep(wait_time)
            else:
                log_print(f"      ❌ Download failed after {retries} attempts: {str(e)[:100]}")
                return None
    return None

def store_bill_text_to_s3(bill_id: str, text_content: bytes, text_version_type: str = "introduced") -> str:
    """Store bill text file to S3 in bill_text/ folder."""
    # Determine file extension
    file_ext = ".xml"
    if text_content.startswith(b'<!DOCTYPE html') or text_content.startswith(b'<html'):
        file_ext = ".html"
    
    # Create S3 key
    safe_version_type = re.sub(r'[^a-zA-Z0-9_-]', '_', text_version_type.lower())
    s3_key = f"bill_text/{bill_id}_{safe_version_type}{file_ext}"
    
    # Upload to S3
    s3_client.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=s3_key,
        Body=text_content,
        ContentType='application/xml' if file_ext == '.xml' else 'text/html'
    )
    
    log_print(f"      💾 Stored bill text for {bill_id} to S3: {s3_key} ({len(text_content):,} bytes)")
    return s3_key

def process_bill_item(item: Dict[str, Any], api_key: str) -> Tuple[bool, Optional[str]]:
    """
    Process a single bill item to fetch and store bill text.
    
    Returns:
        (success: bool, error_message: Optional[str])
    """
    bill_id = item.get('bill_id', 'unknown')
    
    # Skip if already has bill_text_s3_key
    if item.get('bill_text_s3_key'):
        log_print(f"      ⏭️  {bill_id} already has bill_text_s3_key, skipping")
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
            log_print(f"      ⚠️  No text versions found for {bill_id}")
            return False, "No text versions available"
        
        # Find the "Introduced" version first, fallback to first available
        introduced_version = None
        for version in text_versions:
            version_type = version.get("type", "").lower()
            if "introduced" in version_type:
                introduced_version = version
                break
        
        selected_version = introduced_version if introduced_version else text_versions[0]
        
        # Get the Formatted XML URL
        formats = selected_version.get("formats", {})
        if isinstance(formats, dict):
            format_items = formats.get("item", [])
            if isinstance(format_items, list):
                for fmt_item in format_items:
                    if fmt_item.get("type") == "Formatted XML":
                        text_url = fmt_item.get("url")
                        if text_url:
                            log_print(f"      📄 Downloading bill text from {text_url}...")
                            text_content = download_bill_text_file(text_url)
                            if text_content:
                                version_type_name = selected_version.get("type", "introduced")
                                bill_text_s3_key = store_bill_text_to_s3(
                                    bill_id,
                                    text_content,
                                    version_type_name
                                )
                                
                                # Update DynamoDB item
                                bills_table.update_item(
                                    Key={'bill_id': bill_id},
                                    UpdateExpression='SET bill_text_s3_key = :s3_key',
                                    ExpressionAttributeValues={':s3_key': bill_text_s3_key}
                                )
                                
                                log_print(f"      ✅ Updated {bill_id} with bill_text_s3_key: {bill_text_s3_key}")
                                return True, None
                            else:
                                return False, "Failed to download bill text"
                        break
        
        return False, "No Formatted XML URL found"
        
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
    log_print("📋 Scanning DynamoDB table for bills without bill_text_s3_key...")
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
        
        # Filter items that don't have bill_text_s3_key or have empty value
        for item in items:
            bill_text_s3_key = item.get('bill_text_s3_key', '')
            if not bill_text_s3_key or bill_text_s3_key == '':
                items_to_process.append(item)
        
        scan_count += len(items)
        
        log_print(f"   📊 Scanned {scan_count} items, found {len(items_to_process)} items without bill_text_s3_key")
        
        last_evaluated_key = response.get('LastEvaluatedKey')
        if not last_evaluated_key:
            break
    
    log_print(f"\n✅ Total items to process: {len(items_to_process)}")
    log_print("")
    
    if len(items_to_process) == 0:
        log_print("✅ No items to process. All bills already have bill_text_s3_key.")
        job.commit()
        return
    
    # Process items in parallel
    log_print("🔍 Processing bills and fetching bill text...")
    log_print("-" * 80)
    log_print(f"   🚀 Using parallel processing with up to 10 concurrent workers...")
    
    processed_count = 0
    error_count = 0
    skipped_count = 0
    
    def process_bill(bill_item: Dict) -> Tuple[bool, Optional[str], bool]:
        """Process a single bill and return (success, error_message, skipped)"""
        success, error_msg = process_bill_item(bill_item, api_key)
        skipped = bill_item.get('bill_text_s3_key') is not None and bill_item.get('bill_text_s3_key') != ''
        return success, error_msg, skipped
    
    # Process bills in parallel
    log_print(f"   📤 Submitting {len(items_to_process)} bills for parallel processing...")
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_bill = {}
        for bill_item in items_to_process:
            future = executor.submit(process_bill, bill_item)
            future_to_bill[future] = bill_item
        
        log_print(f"   ✅ All {len(future_to_bill)} tasks submitted, processing in parallel...")
        
        completed = 0
        for future in as_completed(future_to_bill):
            completed += 1
            try:
                success, error_msg, skipped = future.result()
                
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
                bill_item = future_to_bill.get(future, {})
                log_print(f"      ❌ Exception processing bill {bill_item.get('bill_id', 'unknown')}: {str(e)}")
            
            if completed % 10 == 0:
                log_print(f"      ✅ Processed {completed}/{len(items_to_process)} bills... (success: {processed_count}, errors: {error_count}, skipped: {skipped_count})")
            
            # Rate limiting - be respectful to Congress.gov API
            time.sleep(0.1)
    
    log_print(f"\n✅ Processed {processed_count} bills successfully")
    if error_count > 0:
        log_print(f"⚠️ {error_count} bills had errors")
    if skipped_count > 0:
        log_print(f"⏭️  {skipped_count} bills were skipped (already have bill_text_s3_key)")
    
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
