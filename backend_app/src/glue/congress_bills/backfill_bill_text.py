"""
AWS Glue Job: Congress Bills Bill Text Backfill
Temporary job to backfill bill_text_xml_s3_key and bill_text_html_s3_key for existing bills in DynamoDB.

This job:
1. Scans all items in the congress-bills DynamoDB table
2. For each item without bill_text_xml_s3_key or bill_text_html_s3_key:
   - Fetches bill text versions from Congress.gov API
   - Downloads both XML and HTML versions of the bill text
   - Stores them in S3 under congress-bills/files/{bill_id}/xml/ and congress-bills/files/{bill_id}/html/
   - Updates the DynamoDB item with bill_text_xml_s3_key and bill_text_html_s3_key

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

def store_bill_text_to_s3(bill_id: str, text_content: bytes, format_type: str = "xml") -> str:
    """
    Store bill text file to S3 in congress-bills/files/{bill_id}/{format_type}/ folder.
    
    Args:
        bill_id: Bill ID (e.g., "119-HR-303")
        text_content: Bill text content as bytes
        format_type: Format type - "xml" or "html"
        
    Returns:
        S3 key where the file was stored
    """
    # Determine file extension and content type
    if format_type.lower() == "html":
        file_ext = ".html"
        content_type = 'text/html'
    else:
        file_ext = ".xml"
        content_type = 'application/xml'
    
    # Create S3 key: congress-bills/files/{bill_id}/{format_type}/bill.{ext}
    s3_key = f"congress-bills/files/{bill_id}/{format_type.lower()}/bill{file_ext}"
    
    # Upload to S3
    s3_client.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=s3_key,
        Body=text_content,
        ContentType=content_type
    )
    
    log_print(f"      💾 Stored {format_type.upper()} bill text for {bill_id} to S3: {s3_key} ({len(text_content):,} bytes)")
    return s3_key

def process_bill_item(item: Dict[str, Any], api_key: str) -> Tuple[bool, Optional[str]]:
    """
    Process a single bill item to fetch and store bill text.
    
    Returns:
        (success: bool, error_message: Optional[str])
    """
    bill_id = item.get('bill_id', 'unknown')
    
    # Skip if already has both bill_text_xml_s3_key and bill_text_html_s3_key
    if item.get('bill_text_xml_s3_key') and item.get('bill_text_html_s3_key'):
        log_print(f"      ⏭️  {bill_id} already has both XML and HTML bill text S3 keys, skipping")
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
            log_print(f"      ⚠️  No text versions found for {bill_id}, setting both keys to empty")
            # Update DynamoDB with empty keys
            update_expression_parts = []
            expression_attribute_values = {}
            
            if not item.get('bill_text_xml_s3_key'):
                update_expression_parts.append("bill_text_xml_s3_key = :xml_key")
                expression_attribute_values[':xml_key'] = ""
            if not item.get('bill_text_html_s3_key'):
                update_expression_parts.append("bill_text_html_s3_key = :html_key")
                expression_attribute_values[':html_key'] = ""
            
            if update_expression_parts:
                bills_table.update_item(
                    Key={'bill_id': bill_id},
                    UpdateExpression=f"SET {', '.join(update_expression_parts)}",
                    ExpressionAttributeValues=expression_attribute_values
                )
                log_print(f"      ✅ Updated {bill_id} with empty bill text S3 keys")
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
            xml_url = None
            html_url = None
            
            # Find both XML and HTML URLs
            for fmt_item in format_items:
                if isinstance(fmt_item, dict):
                    fmt_type = fmt_item.get("type", "")
                    fmt_url = fmt_item.get("url")
                    
                    if fmt_type == "Formatted XML" and fmt_url and not xml_url:
                        xml_url = fmt_url
                    elif fmt_type == "Formatted Text" and fmt_url and not html_url:
                        html_url = fmt_url
            
            # Track what we need to update
            update_expression_parts = []
            expression_attribute_values = {}
            
            # Download and store XML (with error handling)
            if not item.get('bill_text_xml_s3_key'):
                if xml_url:
                    try:
                        log_print(f"      📄 Downloading XML bill text from {xml_url}...")
                        xml_content = download_bill_text_file(xml_url)
                        if xml_content:
                            bill_text_xml_s3_key = store_bill_text_to_s3(bill_id, xml_content, "xml")
                            update_expression_parts.append("bill_text_xml_s3_key = :xml_key")
                            expression_attribute_values[':xml_key'] = bill_text_xml_s3_key
                            log_print(f"      ✅ Stored XML bill text to S3: {bill_text_xml_s3_key}")
                        else:
                            log_print(f"      ⚠️  Failed to download XML bill text, setting key to empty")
                            update_expression_parts.append("bill_text_xml_s3_key = :xml_key")
                            expression_attribute_values[':xml_key'] = ""
                    except Exception as e:
                        log_print(f"      ⚠️  Error downloading XML bill text: {str(e)}, setting key to empty")
                        update_expression_parts.append("bill_text_xml_s3_key = :xml_key")
                        expression_attribute_values[':xml_key'] = ""
                else:
                    log_print(f"      ⚠️  No XML URL found, setting key to empty")
                    update_expression_parts.append("bill_text_xml_s3_key = :xml_key")
                    expression_attribute_values[':xml_key'] = ""
            
            # Download and store HTML (with error handling)
            if not item.get('bill_text_html_s3_key'):
                if html_url:
                    try:
                        log_print(f"      📄 Downloading HTML bill text from {html_url}...")
                        html_content = download_bill_text_file(html_url)
                        if html_content:
                            bill_text_html_s3_key = store_bill_text_to_s3(bill_id, html_content, "html")
                            update_expression_parts.append("bill_text_html_s3_key = :html_key")
                            expression_attribute_values[':html_key'] = bill_text_html_s3_key
                            log_print(f"      ✅ Stored HTML bill text to S3: {bill_text_html_s3_key}")
                        else:
                            log_print(f"      ⚠️  Failed to download HTML bill text, setting key to empty")
                            update_expression_parts.append("bill_text_html_s3_key = :html_key")
                            expression_attribute_values[':html_key'] = ""
                    except Exception as e:
                        log_print(f"      ⚠️  Error downloading HTML bill text: {str(e)}, setting key to empty")
                        update_expression_parts.append("bill_text_html_s3_key = :html_key")
                        expression_attribute_values[':html_key'] = ""
                else:
                    log_print(f"      ⚠️  No HTML URL found, setting key to empty")
                    update_expression_parts.append("bill_text_html_s3_key = :html_key")
                    expression_attribute_values[':html_key'] = ""
            
            # Update DynamoDB item (always update, even if keys are empty)
            if update_expression_parts:
                bills_table.update_item(
                    Key={'bill_id': bill_id},
                    UpdateExpression=f"SET {', '.join(update_expression_parts)}",
                    ExpressionAttributeValues=expression_attribute_values
                )
                
                log_print(f"      ✅ Updated {bill_id} with bill text S3 keys")
                return True, None
            else:
                # Both keys already exist, nothing to update
                return True, None
        else:
            # No format items found, set both keys to empty
            log_print(f"      ⚠️  No format items found, setting both keys to empty")
            bills_table.update_item(
                Key={'bill_id': bill_id},
                UpdateExpression="SET bill_text_xml_s3_key = :xml_key, bill_text_html_s3_key = :html_key",
                ExpressionAttributeValues={
                    ':xml_key': "",
                    ':html_key': ""
                }
            )
            log_print(f"      ✅ Updated {bill_id} with empty bill text S3 keys")
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
    log_print("📋 Scanning DynamoDB table for bills without bill_text_xml_s3_key or bill_text_html_s3_key...")
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
        
        # Filter items that don't have both bill_text_xml_s3_key and bill_text_html_s3_key
        for item in items:
            bill_text_xml_s3_key = item.get('bill_text_xml_s3_key', '')
            bill_text_html_s3_key = item.get('bill_text_html_s3_key', '')
            if not bill_text_xml_s3_key or bill_text_xml_s3_key == '' or not bill_text_html_s3_key or bill_text_html_s3_key == '':
                items_to_process.append(item)
        
        scan_count += len(items)
        
        log_print(f"   📊 Scanned {scan_count} items, found {len(items_to_process)} items missing XML or HTML bill text S3 keys")
        
        last_evaluated_key = response.get('LastEvaluatedKey')
        if not last_evaluated_key:
            break
    
    log_print(f"\n✅ Total items to process: {len(items_to_process)}")
    log_print("")
    
    if len(items_to_process) == 0:
        log_print("✅ No items to process. All bills already have both XML and HTML bill text S3 keys.")
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
        skipped = (bill_item.get('bill_text_xml_s3_key') is not None and bill_item.get('bill_text_xml_s3_key') != '' and
                   bill_item.get('bill_text_html_s3_key') is not None and bill_item.get('bill_text_html_s3_key') != '')
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
        log_print(f"⏭️  {skipped_count} bills were skipped (already have both XML and HTML bill text S3 keys)")
    
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



