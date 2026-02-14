"""
AWS Glue Job: Congress Bills Bill Text Backfill
Daily job to fetch bill text for bills that weren't processed during regular bill fetching.

This job:
   - Scans the table and filters for items where bill_text_html_s3_key is empty or doesn't exist
   - Only processes bills that need bill text
   - Uses FilterExpression to find items with empty bill_text_html_s3_key

For each item processed:
   - Fetches bill text versions from Congress.gov API
   - Downloads HTML version of the bill text
   - Stores it in S3 under billtext/{bill_id}.html
   - Updates the DynamoDB item with bill_text_html_s3_key

This job should run daily after the regular bill fetching job to catch any bills
that didn't get their bill text downloaded (e.g., SQS failures, API errors, etc.).

Roll call maintenance (all bills, excluding search indices):
   - Scans the table for all bill items (bill_id does NOT start with "SEARCH#")
   - For each bill, fetches bill actions from Congress.gov API and extracts recordedVotes
   - For each House roll call, fetches member-level vote data (house-vote/{congress}/{session}/{voteNumber}/members)
   - Updates each item with three attributes:
     - roll_call_number (N): first House roll call number, or omitted if none
     - roll_call_votes (S): JSON string of roll call(s) with member votes
     - has_roll_call (N): 1 if bill has at least one roll call, 0 otherwise (GSI key for HasRollCallIndex)
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
from boto3.dynamodb.conditions import Attr

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

# Global API key rotator instance (initialized once)
_api_key_rotator = None


def get_congress_api_keys() -> 'ApiKeyRotator':
    """
    Get all Congress.gov API keys from AWS Secrets Manager and return a rotator.
    Supports api_key, api_key_2, api_key_3, etc. - any number of keys.
    Returns a thread-safe rotator that distributes calls equally across all keys.
    """
    global _api_key_rotator
    
    # Return cached rotator if already initialized
    if _api_key_rotator is not None:
        return _api_key_rotator
    
    try:
        secret_name = f"{PROJECT_NAME}-congress-api-{ENVIRONMENT}"
        response = secrets_client.get_secret_value(SecretId=secret_name)
        secret_data = json.loads(response['SecretString'])
        
        # Collect all API keys (api_key, api_key_2, api_key_3, etc.)
        api_keys = []
        
        # Always include api_key if present
        if 'api_key' in secret_data and secret_data['api_key']:
            api_keys.append(secret_data['api_key'])
        
        # Collect additional keys (api_key_2, api_key_3, etc.)
        key_index = 2
        while f'api_key_{key_index}' in secret_data:
            key_value = secret_data[f'api_key_{key_index}']
            if key_value and key_value.strip():  # Only add non-empty keys
                api_keys.append(key_value)
            key_index += 1
        
        if not api_keys:
            raise ValueError("No valid API keys found in secret")
        
        # Initialize the rotator (simple round-robin, no threading needed for sequential processing)
        class SimpleApiKeyRotator:
            def __init__(self, keys):
                self.keys = keys
                self.current_index = 0
                log_print(f"✅ Retrieved {len(keys)} Congress API key(s) from Secrets Manager")
            
            def get_key(self):
                key = self.keys[self.current_index]
                self.current_index = (self.current_index + 1) % len(self.keys)
                return key
            
            def get_key_count(self):
                return len(self.keys)
        
        _api_key_rotator = SimpleApiKeyRotator(api_keys)
        return _api_key_rotator
        
    except Exception as e:
        log_print(f"❌ Error retrieving Congress API keys from Secrets Manager: {str(e)}")
        raise ValueError(f"Failed to retrieve Congress API keys from Secrets Manager: {str(e)}")


def get_congress_api_key() -> str:
    """
    Get a single Congress.gov API key using round-robin rotation.
    This function maintains backward compatibility while using the rotator.
    """
    rotator = get_congress_api_keys()
    return rotator.get_key()

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
    # Add headers to mimic browser request (may help with 403 errors)
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive',
    }
    
    for attempt in range(retries):
        try:
            response = requests.get(text_url, timeout=(10, REQUEST_TIMEOUT * 2), headers=headers)  # nosec B113 - timeout set
            
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
            
            # Handle 403 Forbidden specifically
            if response.status_code == 403:
                log_print(f"      ❌ 403 Forbidden - Access denied for {text_url}")
                log_print(f"      Response body (first 200 chars): {response.text[:200]}")
                if attempt < retries - 1:
                    wait_time = RETRY_DELAY * (attempt + 1)
                    log_print(f"      Waiting {wait_time}s before retry...")
                    time.sleep(wait_time)
                    continue
                else:
                    log_print(f"      ❌ 403 Forbidden after {retries} attempts")
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


# ---------------------------------------------------------------------------
# Roll call helpers (Congress.gov: bill actions -> recordedVotes -> house-vote/.../members)
# ---------------------------------------------------------------------------

def _parse_bill_id(bill_id: str) -> Optional[Tuple[str, str, str]]:
    """Parse bill_id (e.g. '119-HR-2189') into (congress, bill_type, bill_number). Returns None if invalid."""
    if not bill_id or bill_id.startswith("SEARCH#"):
        return None
    parts = bill_id.split("-")
    if len(parts) != 3:
        return None
    congress, bill_type, bill_number = parts[0], parts[1], parts[2]
    if not congress.isdigit() or not bill_number.isdigit():
        return None
    return (congress, bill_type, bill_number)


def _fetch_bill_actions(congress: str, bill_type: str, bill_number: str, api_key: str) -> List[Dict]:
    """Fetch all bill actions (paginated) from /bill/{congress}/{billType}/{billNumber}/actions."""
    actions = []
    offset = 0
    limit = 250
    while True:
        url = f"{API_BASE_URL}/bill/{congress}/{bill_type.lower()}/{bill_number}/actions"
        params = {"format": "json", "offset": offset, "limit": limit}
        data = make_api_request(url, params, api_key)
        if not data or not isinstance(data, dict):
            break
        raw = data.get("actions")
        if isinstance(raw, dict):
            items = raw.get("item", [])
        elif isinstance(raw, list):
            items = raw
        else:
            items = []
        if not items:
            break
        actions.extend(items if isinstance(items, list) else [])
        pagination = (data.get("actions") or data or {}).get("pagination") if isinstance(data.get("actions"), dict) else data.get("pagination") or {}
        count = pagination.get("count", 0) if isinstance(pagination, dict) else 0
        if count and offset + limit >= count:
            break
        if len(items) < limit if isinstance(items, list) else True:
            break
        offset += limit
    return actions


def _extract_recorded_votes(actions: List[Dict]) -> List[Dict]:
    """From bill actions extract recordedVotes; return list of { chamber, sessionNumber, rollNumber } (House only for member fetch)."""
    seen = set()
    out = []
    for action in actions or []:
        votes = (action.get("recordedVotes") or action.get("recordedvotes")) or []
        if not isinstance(votes, list):
            continue
        for v in votes:
            if not isinstance(v, dict):
                continue
            chamber = (v.get("chamber") or "").strip()
            session = v.get("sessionNumber")
            roll = v.get("rollNumber")
            key = (chamber, session, roll)
            if key in seen:
                continue
            seen.add(key)
            out.append({"chamber": chamber, "sessionNumber": session, "rollNumber": roll})
    return out


def _fetch_house_vote_members(congress: str, session: int, roll_number: int, api_key: str) -> List[Dict]:
    """Fetch house roll call member votes (paginated). Returns list of member vote dicts."""
    all_members = []
    offset = 0
    limit = 250
    while True:
        url = f"{API_BASE_URL}/house-vote/{congress}/{session}/{roll_number}/members"
        params = {"format": "json", "offset": offset, "limit": limit}
        data = make_api_request(url, params, api_key)
        if not data or not isinstance(data, dict):
            break
        raw = data.get("houseRollCallVoteMemberVotes")
        if isinstance(raw, list):
            results = raw
        elif isinstance(raw, dict):
            results = raw.get("item") or raw.get("memberVotes") or raw.get("houseRollCallVoteMemberVote")
            if not isinstance(results, list):
                results = [raw] if (raw.get("voteCast") or raw.get("bioguideID")) else []
        else:
            results = data.get("results") or data.get("members") or []
        if isinstance(results, list):
            all_members.extend(results)
        pagination = data.get("pagination") or {}
        count = pagination.get("count", 0) if isinstance(pagination, dict) else 0
        if count and offset + limit >= count:
            break
        if not (isinstance(results, list) and len(results) == limit):
            break
        offset += limit
    return all_members


def _build_roll_call_data_for_bill(bill_id: str, api_key: str) -> Dict[str, Any]:
    """
    For a bill: fetch actions -> recordedVotes -> for each House vote fetch members.
    Returns dict: roll_call_number (int or None), roll_call_votes (list of {roll, session, members}), has_roll_call (1 or 0).
    """
    parsed = _parse_bill_id(bill_id)
    if not parsed:
        return {"roll_call_number": None, "roll_call_votes": [], "has_roll_call": 0}
    congress, bill_type, bill_number = parsed
    actions = _fetch_bill_actions(congress, bill_type, bill_number, api_key)
    recorded = _extract_recorded_votes(actions)
    roll_call_number = None
    roll_call_votes = []
    for r in recorded:
        chamber = (r.get("chamber") or "").strip().upper()
        if chamber != "HOUSE":
            continue
        session = r.get("sessionNumber")
        roll = r.get("rollNumber")
        if session is None or roll is None:
            continue
        members = _fetch_house_vote_members(congress, session, roll, api_key)
        roll_call_votes.append({"roll": roll, "session": session, "members": members})
        if roll_call_number is None:
            roll_call_number = roll
    return {
        "roll_call_number": roll_call_number,
        "roll_call_votes": roll_call_votes,
        "has_roll_call": 1 if roll_call_votes else 0,
    }


def process_bill_roll_call(
    item: Dict[str, Any],
    table_name: str,
    api_key: str,
) -> Tuple[bool, Optional[str], Optional[Dict]]:
    """
    For a bill item, fetch roll call data and return update payload for DynamoDB.
    Returns (success, error_message, update_dict).
    update_dict: DynamoDB UpdateExpression and ExpressionAttributeValues for roll_call_number, roll_call_votes, has_roll_call.
    """
    bill_id = (item.get("bill_id") or "").strip()
    if not bill_id or bill_id.startswith("SEARCH#"):
        return True, None, None
    search_index_sk = (item.get("search_index_sk") or bill_id)
    try:
        data = _build_roll_call_data_for_bill(bill_id, api_key)
    except Exception as e:
        return False, str(e), None
    has_roll = data["has_roll_call"]
    if has_roll:
        update_expr = "SET has_roll_call = :h, roll_call_number = :n, roll_call_votes = :v"
        attr_vals = {
            ":h": 1,
            ":n": data["roll_call_number"],
            ":v": json.dumps(data["roll_call_votes"]),
        }
    else:
        update_expr = "SET has_roll_call = :h REMOVE roll_call_number, roll_call_votes"
        attr_vals = {":h": 0}
    return True, None, {
        "bill_id": bill_id,
        "search_index_sk": str(search_index_sk),
        "UpdateExpression": update_expr,
        "ExpressionAttributeValues": attr_vals,
    }


def process_bill_item(item: Dict[str, Any], api_key: str) -> Tuple[bool, Optional[str]]:
    """
    Process a single bill item to fetch and store bill text.
    
    Returns:
        (success: bool, error_message: Optional[str])
    """
    bill_id = item.get('bill_id', 'unknown')
    
    # Skip search index items (cosponsor search indices, etc.)
    if bill_id.startswith('SEARCH#'):
        log_print(f"      ⏭️  Skipping search index item: {bill_id}")
        return True, None  # Return success to not count as error, but skip processing
    
    try:
        # Extract congress, bill_type, bill_number from bill_id (format: "119-HR-303")
        parts = bill_id.split('-')
        if len(parts) != 3:
            return False, f"Invalid bill_id format: {bill_id}"
        
        # Get search_index_sk - for regular bills, it equals bill_id
        # Ensure we have the search_index_sk from the item (it's part of the composite key)
        search_index_sk = item.get('search_index_sk')
        if not search_index_sk:
            # If search_index_sk is missing, use bill_id as fallback (for regular bills, they should be equal)
            search_index_sk = bill_id
            log_print(f"      ⚠️  search_index_sk not found in item for {bill_id}, using bill_id as fallback")
        
        # Ensure both keys are strings (DynamoDB requires string type for keys)
        bill_id = str(bill_id)
        search_index_sk = str(search_index_sk)
        
        congress = int(parts[0])
        bill_type = parts[1]
        bill_number = int(parts[2])
        
        log_print(f"      📋 Processing {bill_id} (search_index_sk: {search_index_sk})...")
        
        # Fetch text versions
        text_versions = fetch_bill_text_versions(congress, bill_type, bill_number, api_key)
        if not text_versions:
            log_print(f"      ⚠️  No text versions found for {bill_id}, setting key to empty")
            # Update DynamoDB with empty key (overwrite existing) - must include both keys
            try:
                bills_table.update_item(
                    Key={
                        'bill_id': bill_id,
                        'search_index_sk': search_index_sk
                    },
                    UpdateExpression="SET bill_text_html_s3_key = :html_key",
                    ExpressionAttributeValues={':html_key': ""}
                )
                log_print(f"      ✅ Updated {bill_id} with empty bill text S3 key")
                return True, None
            except Exception as update_error:
                log_print(f"      ❌ Error updating DynamoDB (no text versions): {str(update_error)}")
                log_print(f"      🔍 Debug - bill_id: {bill_id} (type: {type(bill_id).__name__}), search_index_sk: {search_index_sk} (type: {type(search_index_sk).__name__})")
                log_print(f"      🔍 Debug - item has search_index_sk: {'search_index_sk' in item}")
                if 'search_index_sk' in item:
                    log_print(f"      🔍 Debug - item search_index_sk value: {item['search_index_sk']} (type: {type(item['search_index_sk']).__name__})")
                raise
        
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
            
            # Download and store HTML (with error handling) - always overwrite
            if html_url:
                try:
                    log_print(f"      📄 Downloading HTML bill text from {html_url}...")
                    html_content = download_bill_text_file(html_url)
                    if html_content:
                        bill_text_html_s3_key = store_bill_text_to_s3(bill_id, html_content)
                        try:
                            bills_table.update_item(
                                Key={
                                    'bill_id': bill_id,
                                    'search_index_sk': search_index_sk
                                },
                                UpdateExpression="SET bill_text_html_s3_key = :html_key",
                                ExpressionAttributeValues={':html_key': bill_text_html_s3_key}
                            )
                            log_print(f"      ✅ Stored HTML bill text to S3: {bill_text_html_s3_key}")
                            return True, None
                        except Exception as update_error:
                            log_print(f"      ❌ Error updating DynamoDB item: {str(update_error)}")
                            log_print(f"      🔍 Debug - bill_id: {bill_id} (type: {type(bill_id).__name__}), search_index_sk: {search_index_sk} (type: {type(search_index_sk).__name__})")
                            log_print(f"      🔍 Debug - item keys present: {list(item.keys())[:10]}...")
                            raise  # Re-raise to be caught by outer exception handler
                    else:
                        log_print(f"      ⚠️  Failed to download HTML bill text, setting key to empty")
                        try:
                            bills_table.update_item(
                                Key={
                                    'bill_id': bill_id,
                                    'search_index_sk': search_index_sk
                                },
                                UpdateExpression="SET bill_text_html_s3_key = :html_key",
                                ExpressionAttributeValues={':html_key': ""}
                            )
                            return True, None
                        except Exception as update_error:
                            log_print(f"      ❌ Error updating DynamoDB (download failed): {str(update_error)}")
                            raise
                except Exception as e:
                    log_print(f"      ⚠️  Error downloading HTML bill text: {str(e)}, setting key to empty")
                    try:
                        bills_table.update_item(
                            Key={
                                'bill_id': bill_id,
                                'search_index_sk': search_index_sk
                            },
                            UpdateExpression="SET bill_text_html_s3_key = :html_key",
                            ExpressionAttributeValues={':html_key': ""}
                        )
                        return True, None
                    except Exception as update_error:
                        log_print(f"      ❌ Error updating DynamoDB (download error): {str(update_error)}")
                        raise
            else:
                log_print(f"      ⚠️  No HTML URL found, setting key to empty")
                try:
                    bills_table.update_item(
                        Key={
                            'bill_id': bill_id,
                            'search_index_sk': search_index_sk
                        },
                        UpdateExpression="SET bill_text_html_s3_key = :html_key",
                        ExpressionAttributeValues={':html_key': ""}
                    )
                    return True, None
                except Exception as update_error:
                    log_print(f"      ❌ Error updating DynamoDB (no HTML URL): {str(update_error)}")
                    raise
        else:
            # No format items found, set key to empty
            log_print(f"      ⚠️  No format items found, setting key to empty")
            try:
                bills_table.update_item(
                    Key={
                        'bill_id': bill_id,
                        'search_index_sk': search_index_sk
                    },
                    UpdateExpression="SET bill_text_html_s3_key = :html_key",
                    ExpressionAttributeValues={':html_key': ""}
                )
                log_print(f"      ✅ Updated {bill_id} with empty bill text S3 key")
                return True, None
            except Exception as update_error:
                log_print(f"      ❌ Error updating DynamoDB (no format items): {str(update_error)}")
                raise
        
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
    log_print("🔍 MODE: Roll call maintenance (all bills) + bills with empty bill_text_html_s3_key")
    log_print("=" * 80)
    
    if not BILLS_TABLE_NAME:
        raise ValueError("BILLS_TABLE_NAME job parameter not set")
    
    # Get API key rotator from Secrets Manager
    try:
        api_key_rotator = get_congress_api_keys()
        log_print(f"✅ Retrieved {api_key_rotator.get_key_count()} Congress API key(s) from Secrets Manager")
        log_print(f"   Will rotate through {api_key_rotator.get_key_count()} key(s) during processing")
    except Exception as e:
        log_print(f"❌ Failed to retrieve API keys: {str(e)}")
        raise
    
    # -------------------------------------------------------------------------
    # Phase 1: Roll call maintenance (all bill rows, exclude search indices)
    # -------------------------------------------------------------------------
    log_print("")
    log_print("📋 Phase 1: Roll call maintenance - scanning all bills (excluding SEARCH#)...")
    log_print("-" * 80)
    roll_call_items = []
    last_key = None
    while True:
        scan_params = {}
        if last_key:
            scan_params["ExclusiveStartKey"] = last_key
        scan_params["FilterExpression"] = (
            Attr("bill_id").exists() & ~Attr("bill_id").begins_with("SEARCH#")
        )
        response = bills_table.scan(**scan_params)
        roll_call_items.extend(response.get("Items", []))
        last_key = response.get("LastEvaluatedKey")
        if not last_key:
            break
    log_print(f"   Found {len(roll_call_items)} bill items for roll call maintenance.")
    roll_ok = 0
    roll_err = 0
    for idx, bill_item in enumerate(roll_call_items, 1):
        try:
            api_key = api_key_rotator.get_key()
            success, err, payload = process_bill_roll_call(bill_item, BILLS_TABLE_NAME, api_key)
            if payload is None:
                continue  # skipped (e.g. search index)
            if not success:
                roll_err += 1
                if err:
                    log_print(f"      ❌ Roll call {bill_item.get('bill_id', '?')}: {err}")
                continue
            bills_table.update_item(
                Key={"bill_id": payload["bill_id"], "search_index_sk": payload["search_index_sk"]},
                UpdateExpression=payload["UpdateExpression"],
                ExpressionAttributeValues=payload["ExpressionAttributeValues"],
            )
            roll_ok += 1
        except Exception as e:
            roll_err += 1
            log_print(f"      ❌ Roll call {bill_item.get('bill_id', '?')}: {e}")
        if idx % 50 == 0:
            log_print(f"      Roll call: {idx}/{len(roll_call_items)} (ok: {roll_ok}, err: {roll_err})")
        if idx < len(roll_call_items):
            time.sleep(0.5)
    log_print(f"✅ Phase 1 done. Updated {roll_ok} bills with roll call data, {roll_err} errors.")
    log_print("")
    
    # -------------------------------------------------------------------------
    # Phase 2: Bill text backfill (empty or missing bill_text_html_s3_key)
    # -------------------------------------------------------------------------
    # Scan DynamoDB table for bills missing bill text
    log_print("📋 Scanning DynamoDB table for bills with empty or missing bill_text_html_s3_key...")
    log_print("-" * 80)
    
    items_to_process = []
    last_evaluated_key = None
    scan_count = 0
    filtered_count = 0
    
    while True:
        scan_params = {}
        if last_evaluated_key:
            scan_params['ExclusiveStartKey'] = last_evaluated_key
        
        # Filter for items where:
        # 1. bill_id exists (to ensure we have a valid bill)
        # 2. bill_id does NOT start with "SEARCH#" (exclude search index items)
        # 3. bill_text_html_s3_key is empty or doesn't exist
        scan_params['FilterExpression'] = (
            Attr('bill_id').exists() &
            ~Attr('bill_id').begins_with('SEARCH#') &
            (Attr('bill_text_html_s3_key').not_exists() | 
             Attr('bill_text_html_s3_key').eq(''))
        )
        
        response = bills_table.scan(**scan_params)
        items = response.get('Items', [])
        
        items_to_process.extend(items)
        filtered_count += len(items)
        scan_count += response.get('ScannedCount', len(items))
        
        log_print(f"   📊 Scanned {scan_count} items, found {filtered_count} items with empty bill_text_html_s3_key")
        
        last_evaluated_key = response.get('LastEvaluatedKey')
        if not last_evaluated_key:
            break
    
    log_print(f"\n✅ Total items to process (with empty or missing bill_text_html_s3_key): {len(items_to_process)}")
    log_print("")
    
    if len(items_to_process) == 0:
        log_print("✅ No items to process.")
        job.commit()
        return
    
    # Process items sequentially with backoff
    log_print("🔍 Processing bills and fetching bill text...")
    log_print("-" * 80)
    log_print(f"   📋 Processing {len(items_to_process)} bills sequentially with backoff...")
    
    processed_count = 0
    error_count = 0
    
    # Process bills sequentially
    for idx, bill_item in enumerate(items_to_process, 1):
        try:
            # Get API key for this bill (rotates through all available keys)
            api_key = api_key_rotator.get_key()
            success, error_msg = process_bill_item(bill_item, api_key)
            
            if success:
                processed_count += 1
            else:
                error_count += 1
                if error_msg:
                    log_print(f"      ❌ {error_msg}")
        except Exception as e:
            error_count += 1
            log_print(f"      ❌ Exception processing bill {bill_item.get('bill_id', 'unknown')}: {str(e)}")
        
        if idx % 10 == 0:
            log_print(f"      ✅ Processed {idx}/{len(items_to_process)} bills... (success: {processed_count}, errors: {error_count})")
        
        # Rate limiting - be respectful to Congress.gov API
        # Delay between requests to avoid rate limiting
        if idx < len(items_to_process):  # Don't sleep after the last item
            time.sleep(1.0)  # 1 second delay between items
    
    log_print(f"\n✅ Processed {processed_count} bills successfully")
    if error_count > 0:
        log_print(f"⚠️ {error_count} bills had errors")
    
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



