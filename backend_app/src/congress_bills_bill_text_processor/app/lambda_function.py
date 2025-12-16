"""
Congress Bills Bill Text Processor Lambda
Processes bill text download messages from SQS, downloads HTML bill text, and stores in S3/DynamoDB.
Processes messages sequentially with batch size of 10.
"""


import json
import os
import logging
import boto3
import requests
import time
from typing import Dict, Any, Optional
from datetime import datetime, timezone

# Configure logging
logger = logging.getLogger()
logger.setLevel(os.environ.get('LOG_LEVEL', 'INFO').upper())

# AWS clients
dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')
sqs_client = boto3.client('sqs')
secrets_client = boto3.client('secretsmanager')

# Environment variables
BILLS_TABLE_NAME = os.environ.get('BILLS_TABLE_NAME', 'congress-bills')
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', '')
PROJECT_NAME = os.environ.get('PROJECT_NAME', 'cosine')
ENVIRONMENT = os.environ.get('ENVIRONMENT', 'staging')
CONGRESS_API_BASE_URL = os.environ.get('CONGRESS_API_BASE_URL', 'https://api.congress.gov/v3')
BILL_TEXT_QUEUE_URL = os.environ.get('BILL_TEXT_QUEUE_URL', '')
REQUEST_TIMEOUT = int(os.environ.get('REQUEST_TIMEOUT', '30'))
MAX_RETRIES = int(os.environ.get('MAX_RETRIES', '5'))
RETRY_DELAY = int(os.environ.get('RETRY_DELAY', '2'))

# Get DynamoDB table
bills_table = dynamodb.Table(BILLS_TABLE_NAME) if BILLS_TABLE_NAME else None

# Cache for API key
_cached_api_key = None


def get_congress_api_key() -> str:
    """Get Congress.gov API key from AWS Secrets Manager (with caching)"""
    global _cached_api_key
    if _cached_api_key:
        return _cached_api_key
    
    try:
        secret_name = f"{PROJECT_NAME}-congress-api-{ENVIRONMENT}"
        response = secrets_client.get_secret_value(SecretId=secret_name)
        secret_data = json.loads(response['SecretString'])
        _cached_api_key = secret_data['api_key']
        return _cached_api_key
    except Exception as e:
        logger.error(f"❌ Error retrieving Congress API key from Secrets Manager: {str(e)}")
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
                    wait_time = max(5, (2 ** attempt) * RETRY_DELAY)
                    logger.warning(f"Rate limited (429) - waiting {wait_time}s before retry {attempt + 1}/{retries}")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"Rate limited (429) after {retries} attempts")
                    return None
            
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            if attempt < retries - 1:
                wait_time = RETRY_DELAY * (attempt + 1)
                logger.warning(f"Request failed (attempt {attempt + 1}/{retries}): {str(e)[:100]}")
                time.sleep(wait_time)
            else:
                logger.error(f"Request failed after {retries} attempts: {str(e)[:100]}")
                return None
        except requests.exceptions.RequestException as e:
            if attempt < retries - 1:
                wait_time = RETRY_DELAY * (attempt + 1)
                logger.warning(f"Request failed (attempt {attempt + 1}/{retries}): {str(e)[:100]}")
                time.sleep(wait_time)
            else:
                logger.error(f"Request failed after {retries} attempts: {str(e)[:100]}")
                return None
    return None


def fetch_bill_text_versions(congress: int, bill_type: str, bill_number: int, api_key: str) -> list:
    """Fetch all text versions available for a bill."""
    url = f"{CONGRESS_API_BASE_URL}/bill/{congress}/{bill_type.lower()}/{bill_number}/text"
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
    """Download bill text file (HTML) from Congress.gov with exponential backoff for rate limiting."""
    for attempt in range(retries):
        try:
            response = requests.get(text_url, timeout=REQUEST_TIMEOUT * 2)
            
            # Handle 429 Too Many Requests with exponential backoff
            if response.status_code == 429:
                if attempt < retries - 1:
                    wait_time = max(10, (2 ** attempt) * RETRY_DELAY * 2)
                    logger.warning(f"Rate limited (429) - waiting {wait_time}s before retry {attempt + 1}/{retries}")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"Rate limited (429) after {retries} attempts")
                    return None
            
            response.raise_for_status()
            return response.content
        except requests.exceptions.HTTPError as e:
            if attempt < retries - 1:
                wait_time = RETRY_DELAY * (attempt + 1)
                logger.warning(f"Download failed (attempt {attempt + 1}/{retries}): {str(e)[:100]}")
                time.sleep(wait_time)
            else:
                logger.error(f"Download failed after {retries} attempts: {str(e)[:100]}")
                return None
        except requests.exceptions.RequestException as e:
            if attempt < retries - 1:
                wait_time = RETRY_DELAY * (attempt + 1)
                logger.warning(f"Download failed (attempt {attempt + 1}/{retries}): {str(e)[:100]}")
                time.sleep(wait_time)
            else:
                logger.error(f"Download failed after {retries} attempts: {str(e)[:100]}")
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
    
    logger.info(f"💾 Stored HTML bill text for {bill_id} to S3: {s3_key} ({len(text_content):,} bytes)")
    return s3_key


def process_bill_text_download(bill_id: str, api_key: str) -> tuple[bool, Optional[str]]:
    """
    Process a single bill text download.
    
    Args:
        bill_id: Bill ID (e.g., "119-HR-303")
        api_key: Congress.gov API key
        
    Returns:
        (success: bool, s3_key: Optional[str])
    """
    try:
        # Extract congress, bill_type, bill_number from bill_id (format: "119-HR-303")
        parts = bill_id.split('-')
        if len(parts) != 3:
            return False, f"Invalid bill_id format: {bill_id}"
        
        congress = int(parts[0])
        bill_type = parts[1]
        bill_number = int(parts[2])
        
        logger.info(f"📋 Processing {bill_id}...")
        
        # Fetch text versions
        text_versions = fetch_bill_text_versions(congress, bill_type, bill_number, api_key)
        if not text_versions:
            logger.warning(f"⚠️  No text versions found for {bill_id}, setting key to empty")
            # Update DynamoDB with empty key
            bills_table.update_item(
                Key={'bill_id': bill_id},
                UpdateExpression="SET bill_text_html_s3_key = :html_key",
                ExpressionAttributeValues={':html_key': ""}
            )
            return True, ""
        
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
            format_items = formats
        elif isinstance(formats, dict):
            if "item" in formats:
                item_data = formats["item"]
                if isinstance(item_data, list):
                    format_items = item_data
                elif isinstance(item_data, dict):
                    format_items = [item_data]
            else:
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
            if html_url:
                try:
                    logger.info(f"📄 Downloading HTML bill text from {html_url}...")
                    html_content = download_bill_text_file(html_url)
                    if html_content:
                        bill_text_html_s3_key = store_bill_text_to_s3(bill_id, html_content)
                        # Update DynamoDB
                        bills_table.update_item(
                            Key={'bill_id': bill_id},
                            UpdateExpression="SET bill_text_html_s3_key = :html_key",
                            ExpressionAttributeValues={':html_key': bill_text_html_s3_key}
                        )
                        logger.info(f"✅ Stored HTML bill text to S3: {bill_text_html_s3_key}")
                        return True, bill_text_html_s3_key
                    else:
                        logger.warning(f"⚠️  Failed to download HTML bill text, setting key to empty")
                        bills_table.update_item(
                            Key={'bill_id': bill_id},
                            UpdateExpression="SET bill_text_html_s3_key = :html_key",
                            ExpressionAttributeValues={':html_key': ""}
                        )
                        return True, ""
                except Exception as e:
                    logger.error(f"⚠️  Error downloading HTML bill text: {str(e)}, setting key to empty")
                    bills_table.update_item(
                        Key={'bill_id': bill_id},
                        UpdateExpression="SET bill_text_html_s3_key = :html_key",
                        ExpressionAttributeValues={':html_key': ""}
                    )
                    return True, ""
            else:
                logger.warning(f"⚠️  No HTML URL found, setting key to empty")
                bills_table.update_item(
                    Key={'bill_id': bill_id},
                    UpdateExpression="SET bill_text_html_s3_key = :html_key",
                    ExpressionAttributeValues={':html_key': ""}
                )
                return True, ""
        else:
            logger.warning(f"⚠️  No format items found, setting key to empty")
            bills_table.update_item(
                Key={'bill_id': bill_id},
                UpdateExpression="SET bill_text_html_s3_key = :html_key",
                ExpressionAttributeValues={':html_key': ""}
            )
            return True, ""
        
    except Exception as e:
        error_msg = f"Error processing {bill_id}: {str(e)}"
        logger.error(f"❌ {error_msg}", exc_info=True)
        return False, None


def process_message(record: Dict[str, Any], api_key: str) -> tuple[bool, Optional[str]]:
    """
    Process a single SQS message.
    
    Expected message format:
    {
        "bill_id": "119-HR-303"
    }
    
    Returns:
        (success: bool, bill_id: Optional[str])
    """
    try:
        # Parse message body
        if isinstance(record.get('body'), str):
            message_body = json.loads(record['body'])
        else:
            message_body = record.get('body', {})
        
        bill_id = message_body.get('bill_id')
        if not bill_id:
            logger.error("❌ Message missing bill_id")
            return (False, None)
        
        success, s3_key = process_bill_text_download(bill_id, api_key)
        return (success, bill_id)
        
    except Exception as e:
        logger.error(f"❌ Error processing SQS record: {str(e)}", exc_info=True)
        return (False, None)


def lambda_handler(event, context):
    """
    Lambda handler for processing bill text download messages from SQS.
    Processes up to 10 messages per invocation sequentially.
    
    Expected message format:
    {
        "bill_id": "119-HR-303"
    }
    """
    records = event.get('Records', [])
    total_messages = len(records)
    
    logger.info(f"Processing {total_messages} message(s) sequentially")
    
    # Get API key once for all messages
    try:
        api_key = get_congress_api_key()
    except Exception as e:
        logger.error(f"❌ Failed to retrieve API key: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'success': False,
                'error': f"Failed to retrieve API key: {str(e)}"
            })
        }
    
    success_count = 0
    failure_count = 0
    
    # Process messages sequentially (one at a time)
    for i, record in enumerate(records, 1):
        logger.info(f"Processing message {i}/{total_messages}")
        success, bill_id = process_message(record, api_key)
        
        if success:
            success_count += 1
            logger.info(f"✅ Successfully processed {bill_id}")
        else:
            failure_count += 1
            logger.error(f"❌ Failed to process message {i}")
        
        # Small delay between sequential downloads to avoid rate limiting
        if i < total_messages:
            time.sleep(1.0)  # 1 second delay between downloads
    
    logger.info(f"Processing complete: {total_messages} total processed, {success_count} successful, {failure_count} failed")
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'success': True,
            'processed': total_messages,
            'successful': success_count,
            'failed': failure_count
        })
    }
