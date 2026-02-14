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

# API Key Rotator for load balancing across multiple keys
import threading

class ApiKeyRotator:
    """Thread-safe API key rotator for round-robin distribution across multiple keys"""
    def __init__(self, api_keys: list):
        self.api_keys = api_keys
        self.current_index = 0
        self.lock = threading.Lock()
    
    def get_key(self) -> str:
        """Get next API key in round-robin fashion"""
        with self.lock:
            key = self.api_keys[self.current_index]
            self.current_index = (self.current_index + 1) % len(self.api_keys)
            return key
    
    def switch_key(self) -> str:
        """Switch to next API key (for retries)"""
        return self.get_key()
    
    def count(self) -> int:
        """Get number of available keys"""
        return len(self.api_keys)

# Global rotator instance
_api_key_rotator = None
_rotator_lock = threading.Lock()

def get_congress_api_keys() -> ApiKeyRotator:
    """Get Congress.gov API keys from AWS Secrets Manager and return rotator"""
    global _api_key_rotator
    
    with _rotator_lock:
        if _api_key_rotator:
            return _api_key_rotator
        
        try:
            secret_name = f"{PROJECT_NAME}-congress-api-{ENVIRONMENT}"
            response = secrets_client.get_secret_value(SecretId=secret_name)
            secret_data = json.loads(response['SecretString'])
            
            # Extract all API keys (api_key, api_key_2, api_key_3, etc.)
            api_keys = []
            key_index = 1
            while True:
                if key_index == 1:
                    key_name = 'api_key'
                else:
                    key_name = f'api_key_{key_index}'
                
                if key_name in secret_data and secret_data[key_name]:
                    api_keys.append(secret_data[key_name])
                    key_index += 1
                else:
                    break
            
            if not api_keys:
                raise ValueError("No API keys found in secret")
            
            _api_key_rotator = ApiKeyRotator(api_keys)
            logger.info(f"✅ Initialized API key rotator with {len(api_keys)} key(s)")
            return _api_key_rotator
        except Exception as e:
            logger.error(f"❌ Error retrieving Congress API keys from Secrets Manager: {str(e)}")
            raise ValueError(f"Failed to retrieve Congress API keys from Secrets Manager: {str(e)}")

def get_congress_api_key() -> str:
    """Get next API key from rotator"""
    rotator = get_congress_api_keys()
    return rotator.get_key()


def make_api_request(url: str, params: Dict[str, Any], retries: int = MAX_RETRIES) -> Optional[Dict]:
    """Make API request with retry logic, exponential backoff, and API key rotation."""
    rotator = get_congress_api_keys()
    current_key = rotator.get_key()
    
    for attempt in range(retries):
        try:
            # Use current key for this attempt
            params_with_key = params.copy()
            params_with_key["api_key"] = current_key
            
            response = requests.get(url, params=params_with_key, timeout=REQUEST_TIMEOUT)
            
            # Handle 429 Too Many Requests with exponential backoff and key switching
            if response.status_code == 429:
                if attempt < retries - 1:
                    # Switch to different key for retry
                    current_key = rotator.switch_key()
                    wait_time = max(5, (2 ** attempt) * RETRY_DELAY)
                    logger.warning(f"⚠️ Rate limited (429) with key - switching to different key and waiting {wait_time}s before retry {attempt + 1}/{retries}")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.error(f"Rate limited (429) after {retries} attempts")
                    return None
            
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            if attempt < retries - 1:
                # Switch to different key for retry
                current_key = rotator.switch_key()
                wait_time = RETRY_DELAY * (attempt + 1)
                logger.warning(f"Request failed (attempt {attempt + 1}/{retries}): {str(e)[:100]}. Switching API key and retrying...")
                time.sleep(wait_time)
            else:
                logger.error(f"Request failed after {retries} attempts: {str(e)[:100]}")
                return None
        except requests.exceptions.RequestException as e:
            if attempt < retries - 1:
                # Switch to different key for retry
                current_key = rotator.switch_key()
                wait_time = RETRY_DELAY * (attempt + 1)
                logger.warning(f"Request failed (attempt {attempt + 1}/{retries}): {str(e)[:100]}. Switching API key and retrying...")
                time.sleep(wait_time)
            else:
                logger.error(f"Request failed after {retries} attempts: {str(e)[:100]}")
                return None
    return None


def fetch_bill_text_versions(congress: int, bill_type: str, bill_number: int) -> list:
    """Fetch all text versions available for a bill."""
    url = f"{CONGRESS_API_BASE_URL}/bill/{congress}/{bill_type.lower()}/{bill_number}/text"
    params = {"format": "json"}
    
    data = make_api_request(url, params)
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
            response = requests.get(text_url, timeout=(10, REQUEST_TIMEOUT * 2))  # nosec B113 - timeout set
            
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


def store_bill_text_to_s3(bill_id: str, text_content: bytes, file_index: int = 1) -> str:
    """
    Store bill text HTML file to S3 in billtext/{bill_id}/ folder (e.g. 1.html).

    Args:
        bill_id: Bill ID (e.g., "119-HR-303")
        text_content: Bill text HTML content as bytes
        file_index: 1-based file number (default 1) for naming 1.html, 2.html, ...

    Returns:
        S3 key where the file was stored (e.g. billtext/119-HR-303/1.html)
    """
    s3_key = f"billtext/{bill_id}/{file_index}.html"
    s3_client.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=s3_key,
        Body=text_content,
        ContentType='text/html'
    )
    logger.info(f"💾 Stored HTML bill text for {bill_id} to S3: {s3_key} ({len(text_content):,} bytes)")
    return s3_key


def _get_format_items(version: Dict[str, Any]) -> list:
    """Extract format items list from a text version (handles list or dict with item)."""
    formats = version.get("formats", {})
    if isinstance(formats, list):
        return formats
    if isinstance(formats, dict):
        if "item" in formats:
            item_data = formats["item"]
            if isinstance(item_data, list):
                return item_data
            if isinstance(item_data, dict):
                return [item_data]
        return list(formats.values()) if formats else []
    return []


def _find_html_url(format_items: list) -> Optional[str]:
    """Find first 'Formatted Text' URL in format items."""
    for fmt_item in format_items:
        if isinstance(fmt_item, dict):
            fmt_type = (fmt_item.get("type") or "").strip()
            fmt_url = fmt_item.get("url")
            if fmt_type == "Formatted Text" and fmt_url:
                return fmt_url
    return None


def process_bill_text_download_from_versions(
    bill_id: str, text_versions: list, search_index_sk: Optional[str] = None
) -> tuple[bool, Optional[str]]:
    """
    Download and store all bill text versions from the provided list (no API call).
    Message payload from fetcher: bill_id + text_versions from bulk XML.

    Returns:
        (success: bool, last_s3_key: Optional[str])
    """
    if not bill_id or not isinstance(text_versions, list):
        return False, None
    search_index_sk = search_index_sk or bill_id
    max_versions = 20
    stored = []

    for idx, version in enumerate(text_versions):
        if len(stored) >= max_versions:
            break
        version_type = (version.get("type") or "").strip() or f"Version {len(stored) + 1}"
        format_items = _get_format_items(version)
        html_url = _find_html_url(format_items)
        if not html_url:
            continue
        file_index = len(stored) + 1
        try:
            logger.info(f"📄 Downloading bill text ({version_type}) for {bill_id}...")
            content = download_bill_text_file(html_url)
            if content:
                s3_key = store_bill_text_to_s3(bill_id, content, file_index=file_index)
                stored.append({
                    "name": f"{file_index}.html",
                    "s3_key": s3_key,
                    "type": version_type,
                })
            else:
                logger.warning(f"⚠️  Failed to download HTML for version: {version_type}")
        except Exception as e:
            logger.warning(f"⚠️  Error downloading version '{version_type}': {str(e)}")

    try:
        bills_table.update_item(
            Key={"bill_id": bill_id, "search_index_sk": search_index_sk},
            UpdateExpression="SET bill_texts = :bt REMOVE bill_text_html_s3_key, bill_text_versions_s3_json",
            ExpressionAttributeValues={":bt": stored},
        )
        if stored:
            logger.info(f"✅ Stored {len(stored)} bill text file(s) for {bill_id}")
        else:
            logger.info(f"✅ Updated {bill_id} with empty bill_texts")
        return True, stored[-1]["s3_key"] if stored else None
    except Exception as e:
        logger.error(f"❌ DynamoDB update failed for {bill_id}: {str(e)}")
        return False, None


def process_bill_text_download(bill_id: str) -> tuple[bool, Optional[str]]:
    """
    Process a single bill text download.
    
    Args:
        bill_id: Bill ID (e.g., "119-HR-303")
        
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
        
        # Fetch text versions (API key rotation handled internally)
        text_versions = fetch_bill_text_versions(congress, bill_type, bill_number)
        if not text_versions:
            logger.warning(f"⚠️  No text versions found for {bill_id}, setting bill_texts to []")
            bills_table.update_item(
                Key={'bill_id': bill_id, 'search_index_sk': bill_id},
                UpdateExpression="SET bill_texts = :empty REMOVE bill_text_html_s3_key, bill_text_versions_s3_json",
                ExpressionAttributeValues={':empty': []}
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
        version_type = (selected_version.get("type") or "").strip() or "Bill text"

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
                        s3_key = store_bill_text_to_s3(bill_id, html_content, file_index=1)
                        bill_texts = [{"name": "1.html", "s3_key": s3_key, "type": version_type}]
                        bills_table.update_item(
                            Key={'bill_id': bill_id, 'search_index_sk': bill_id},
                            UpdateExpression="SET bill_texts = :bt REMOVE bill_text_html_s3_key, bill_text_versions_s3_json",
                            ExpressionAttributeValues={':bt': bill_texts}
                        )
                        logger.info(f"✅ Stored HTML bill text to S3: {s3_key}")
                        return True, s3_key
                    else:
                        logger.warning(f"⚠️  Failed to download HTML bill text, setting bill_texts to []")
                        bills_table.update_item(
                            Key={'bill_id': bill_id, 'search_index_sk': bill_id},
                            UpdateExpression="SET bill_texts = :empty REMOVE bill_text_html_s3_key, bill_text_versions_s3_json",
                            ExpressionAttributeValues={':empty': []}
                        )
                        return True, ""
                except Exception as e:
                    logger.error(f"⚠️  Error downloading HTML bill text: {str(e)}, setting bill_texts to []")
                    bills_table.update_item(
                        Key={'bill_id': bill_id, 'search_index_sk': bill_id},
                        UpdateExpression="SET bill_texts = :empty REMOVE bill_text_html_s3_key, bill_text_versions_s3_json",
                        ExpressionAttributeValues={':empty': []}
                    )
                    return True, ""
            else:
                logger.warning(f"⚠️  No HTML URL found, setting bill_texts to []")
                bills_table.update_item(
                    Key={'bill_id': bill_id, 'search_index_sk': bill_id},
                    UpdateExpression="SET bill_texts = :empty REMOVE bill_text_html_s3_key, bill_text_versions_s3_json",
                    ExpressionAttributeValues={':empty': []}
                )
                return True, ""
        else:
            logger.warning(f"⚠️  No format items found, setting bill_texts to []")
            bills_table.update_item(
                Key={'bill_id': bill_id, 'search_index_sk': bill_id},
                UpdateExpression="SET bill_texts = :empty REMOVE bill_text_html_s3_key, bill_text_versions_s3_json",
                ExpressionAttributeValues={':empty': []}
            )
            return True, ""
        
    except Exception as e:
        error_msg = f"Error processing {bill_id}: {str(e)}"
        logger.error(f"❌ {error_msg}", exc_info=True)
        return False, None


def process_message(record: Dict[str, Any]) -> tuple[bool, Optional[str]]:
    """
    Process a single SQS message.

    Preferred format (from fetcher): {"bill_id": "...", "text_versions": [...]}
    Fallback: {"bill_id": "..."} -> fetch versions from API, store one.
    """
    try:
        if isinstance(record.get('body'), str):
            message_body = json.loads(record['body'])
        else:
            message_body = record.get('body', {})

        bill_id = message_body.get('bill_id')
        if not bill_id:
            logger.error("❌ Message missing bill_id")
            return (False, None)

        text_versions = message_body.get('text_versions')
        if isinstance(text_versions, list) and len(text_versions) > 0:
            search_index_sk = message_body.get('search_index_sk') or bill_id
            success, _ = process_bill_text_download_from_versions(
                bill_id, text_versions, search_index_sk=search_index_sk
            )
            return (success, bill_id)

        success, _ = process_bill_text_download(bill_id)
        return (success, bill_id)

    except Exception as e:
        logger.error(f"❌ Error processing SQS record: {str(e)}", exc_info=True)
        return (False, None)


def lambda_handler(event, context):
    """
    Lambda handler for processing bill text download messages from SQS.
    Processes up to 10 messages per invocation sequentially.

    Preferred message format (from fetcher; no API call):
    {"bill_id": "119-HR-303", "search_index_sk": "119-HR-303", "text_versions": [{type, date, formats: [{url, type}]}, ...]}
    Fallback: {"bill_id": "119-HR-303"} -> fetch versions from Congress API, store one.
    """
    records = event.get('Records', [])
    total_messages = len(records)
    
    logger.info(f"Processing {total_messages} message(s) sequentially")
    
    # Initialize API key rotator (will be used by make_api_request)
    try:
        get_congress_api_keys()
    except Exception as e:
        logger.error(f"❌ Failed to retrieve API keys: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'success': False,
                'error': f"Failed to retrieve API keys: {str(e)}"
            })
        }
    
    success_count = 0
    failure_count = 0
    
    # Process messages sequentially (one at a time)
    for i, record in enumerate(records, 1):
        logger.info(f"Processing message {i}/{total_messages}")
        success, bill_id = process_message(record)
        
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
