"""
AWS Glue Job: USAspending Daily Bulk Indexing (V2)
Downloads bulk CSV files from USAspending, parses prime awards and sub-awards from separate CSV files,
and indexes everything to DynamoDB and S3.

This job runs daily via Step Functions to:
1. Get date range from parameters
2. For each agency, initiate bulk download with sub-awards included
3. Download and extract all CSV files from ZIP (prime awards + sub-awards)
4. Parse prime award CSV files to extract award records
5. Parse sub-award CSV files and link to parent awards
6. Store all award data (all columns) in DynamoDB and transaction/subaward details in S3
"""

import sys
import json
import logging
import time
import csv
import zipfile
import urllib3
import gc
import codecs
import gzip
import re
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional
from decimal import Decimal
from io import StringIO, BytesIO
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

import boto3
import requests

# Disable SSL warnings (common in Glue environments with certificate issues)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ============================================================================
# Configuration
# ============================================================================

# Get required job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'USASPENDING_BASE_URL',
    'USASPENDING_USER_AGENT',
    'AWARDS_TABLE_NAME',
    'S3_BUCKET_NAME',
    'REQUEST_TIMEOUT'
])

# Get optional date parameters
try:
    optional_args = getResolvedOptions(sys.argv, ['START_DATE', 'END_DATE'])
    args.update(optional_args)
    print(f"✅ Successfully parsed optional arguments: START_DATE={optional_args.get('START_DATE')}, END_DATE={optional_args.get('END_DATE')}", flush=True)
except Exception as e:
    print(f"ℹ️ Optional date arguments not provided (will default in main()): {str(e)[:200]}", flush=True)
    pass

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
log_print("✅ USAspending Bulk Indexing Glue Job V2 - Script Loaded Successfully")
log_print("=" * 80)

# Environment variables
USASPENDING_BASE_URL = args.get('USASPENDING_BASE_URL', 'https://api.usaspending.gov')
USASPENDING_USER_AGENT = args.get('USASPENDING_USER_AGENT', 'Cosine Financial Platform (contact@cosine.financial)')
AWARDS_TABLE_NAME = args.get('AWARDS_TABLE_NAME', 'usaspending-awards-index')
S3_BUCKET_NAME = args.get('S3_BUCKET_NAME', 'cosine-usaspending-data-production')
REQUEST_TIMEOUT = int(args.get('REQUEST_TIMEOUT', '30'))
MAX_RETRIES = int(args.get('MAX_RETRIES', '5'))
RETRY_BASE_DELAY = float(args.get('RETRY_BASE_DELAY', '2.0'))

# AWS clients
dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')
awards_table = dynamodb.Table(AWARDS_TABLE_NAME)

log_print(f"ℹ️ Configuration: Table={AWARDS_TABLE_NAME}, S3 Bucket={S3_BUCKET_NAME}, API={USASPENDING_BASE_URL}")

# Global session for connection pooling
_global_session = None
_last_api_call_time = 0

# Thread-safe progress tracking
_progress_lock = Lock()
_progress_counter = {'indexed': 0, 'total': 0, 'processed': 0}

# ============================================================================
# Helper Functions
# ============================================================================

def create_session():
    """Create a requests session with proper headers, SSL handling, and retry logic"""
    global _global_session
    
    if _global_session is not None:
        return _global_session
    
    session = requests.Session()
    session.headers.update({
        'User-Agent': USASPENDING_USER_AGENT,
        'Accept': 'application/json',
        'Content-Type': 'application/json',
    })
    session.verify = False
    
    retry_strategy = Retry(
        total=MAX_RETRIES,
        backoff_factor=RETRY_BASE_DELAY,
        status_forcelist=[429, 500, 502, 503, 504],
        method_whitelist=["GET", "POST"],
        raise_on_status=False
    )
    
    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=20,
        pool_maxsize=30
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    _global_session = session
    return session

def rate_limit():
    """Enforce rate limiting between API calls"""
    global _last_api_call_time
    current_time = time.time()
    time_since_last_call = current_time - _last_api_call_time
    
    parallel_rate_limit = 0.1
    if time_since_last_call < parallel_rate_limit:
        sleep_time = parallel_rate_limit - time_since_last_call
        time.sleep(sleep_time)
    
    _last_api_call_time = time.time()

def call_usaspending_api(endpoint: str, method: str = 'GET', body: Optional[Dict] = None, params: Optional[Dict] = None) -> Dict[str, Any]:
    """Call USAspending API endpoint with retry logic and rate limiting"""
    url = f"{USASPENDING_BASE_URL}{endpoint}"
    session = create_session()
    
    rate_limit()
    
    last_exception = None
    for attempt in range(MAX_RETRIES + 1):
        try:
            if method.upper() == 'POST':
                response = session.post(url, json=body, timeout=REQUEST_TIMEOUT)
            else:
                response = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', RETRY_BASE_DELAY * (2 ** attempt)))
                if attempt < MAX_RETRIES:
                    log_print(f"⚠️ Rate limited (429). Waiting {retry_after}s before retry {attempt + 1}/{MAX_RETRIES}")
                    time.sleep(retry_after)
                    continue
                else:
                    response.raise_for_status()
            
            if response.status_code >= 500:
                if attempt < MAX_RETRIES:
                    backoff_delay = RETRY_BASE_DELAY * (2 ** attempt)
                    log_print(f"⚠️ Server error {response.status_code}. Retrying in {backoff_delay}s (attempt {attempt + 1}/{MAX_RETRIES})")
                    time.sleep(backoff_delay)
                    continue
                else:
                    response.raise_for_status()
            
            response.raise_for_status()
            return response.json()
            
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout, 
                requests.exceptions.ChunkedEncodingError, urllib3.exceptions.ProtocolError,
                urllib3.exceptions.NewConnectionError) as e:
            last_exception = e
            if attempt < MAX_RETRIES:
                backoff_delay = RETRY_BASE_DELAY * (2 ** attempt)
                log_print(f"⚠️ Connection error: {str(e)[:100]}. Retrying in {backoff_delay}s (attempt {attempt + 1}/{MAX_RETRIES})")
                time.sleep(backoff_delay)
            else:
                log_print(f"❌ Connection error after {MAX_RETRIES} retries: {str(e)}")
                raise
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                continue
            log_print(f"❌ HTTP error {e.response.status_code}: {str(e)}")
            raise
        except Exception as e:
            log_print(f"❌ Unexpected error: {str(e)}")
            raise
    
    if last_exception:
        raise last_exception
    raise Exception(f"Failed to call API after {MAX_RETRIES} retries")

def extract_fiscal_year(date_str: Optional[str]) -> Optional[int]:
    """Extract fiscal year from date string (YYYY-MM-DD)"""
    if not date_str:
        return None
    try:
        date_obj = datetime.strptime(date_str.split('T')[0], '%Y-%m-%d')
        if date_obj.month >= 10:
            return date_obj.year + 1
        return date_obj.year
    except:
        return None

def convert_floats_to_decimal(obj: Any) -> Any:
    """Recursively convert all float values to Decimal for DynamoDB compatibility"""
    if isinstance(obj, float):
        return Decimal(str(obj))
    elif isinstance(obj, dict):
        return {key: convert_floats_to_decimal(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_floats_to_decimal(item) for item in obj]
    else:
        return obj

def normalize_common_fields(record: Dict[str, Any], record_type: str = "prime") -> Dict[str, Any]:
    """
    Normalize common fields between contracts and assistance to reduce blanks in DynamoDB.
    Maps contract-specific and assistance-specific fields to common field names.
    Original fields are preserved, common fields are added to reduce blanks.
    
    Args:
        record: Dictionary of CSV fields
        record_type: "prime", "transaction", or "subaward"
    
    Returns:
        Dictionary with normalized fields added (original fields preserved)
    """
    normalized = record.copy()
    
    if record_type == "prime":
        # Award identifier mappings - both represent the award ID
        if 'award_id_piid' in record and record['award_id_piid']:
            normalized['award_identifier'] = record['award_id_piid']
        elif 'award_id_fain' in record and record['award_id_fain']:
            normalized['award_identifier'] = record['award_id_fain']
        
        # Obligation amount mappings - both represent total obligation
        if 'total_dollars_obligated' in record and record['total_dollars_obligated']:
            if 'total_obligated_amount' not in normalized or not normalized.get('total_obligated_amount'):
                normalized['total_obligated_amount'] = record['total_dollars_obligated']
        elif 'total_obligated_amount' in record and record['total_obligated_amount']:
            if 'total_dollars_obligated' not in normalized or not normalized.get('total_dollars_obligated'):
                normalized['total_dollars_obligated'] = record['total_obligated_amount']
        
        # Award type mappings - both represent award type
        if 'assistance_type_description' in record and record['assistance_type_description']:
            if 'award_type' not in normalized or not normalized.get('award_type'):
                normalized['award_type'] = record['assistance_type_description']
        elif 'award_type' in record and record['award_type']:
            if 'assistance_type_description' not in normalized or not normalized.get('assistance_type_description'):
                normalized['assistance_type_description'] = record['award_type']
        
        # State code mappings (already handled in GSI mapping, but ensure both exist)
        if 'recipient_state_code' in record and record['recipient_state_code']:
            if 'recipient_location_state' not in normalized or not normalized.get('recipient_location_state'):
                normalized['recipient_location_state'] = record['recipient_state_code']
    
    elif record_type == "transaction":
        # Transaction obligation mappings - both represent transaction obligation
        if 'total_dollars_obligated' in record and record['total_dollars_obligated']:
            if 'total_obligated_amount' not in normalized or not normalized.get('total_obligated_amount'):
                normalized['total_obligated_amount'] = record['total_dollars_obligated']
        elif 'total_obligated_amount' in record and record['total_obligated_amount']:
            if 'total_dollars_obligated' not in normalized or not normalized.get('total_dollars_obligated'):
                normalized['total_dollars_obligated'] = record['total_obligated_amount']
        
        # Transaction unique key mappings - both represent transaction identifier
        if 'contract_transaction_unique_key' in record and record['contract_transaction_unique_key']:
            normalized['transaction_unique_key'] = record['contract_transaction_unique_key']
        elif 'assistance_transaction_unique_key' in record and record['assistance_transaction_unique_key']:
            normalized['transaction_unique_key'] = record['assistance_transaction_unique_key']
    
    elif record_type == "subaward":
        # Parent award identifier mappings - both represent parent award ID
        if 'prime_award_piid' in record and record['prime_award_piid']:
            normalized['prime_award_identifier'] = record['prime_award_piid']
        elif 'prime_award_fain' in record and record['prime_award_fain']:
            normalized['prime_award_identifier'] = record['prime_award_fain']
        
        # Subawardee name mappings (both use same field name, but ensure consistency)
        # Both contracts and assistance subawards have subawardee_name, so no mapping needed
    
    return normalized

def extract_gsi_fields_only(full_item: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract only GSI fields and essential metadata for DynamoDB.
    Used when item exceeds 400KB limit.
    
    GSI fields to preserve:
    - Primary key: award_id
    - GSI hash keys: awarding_agency_code, awarding_agency_name, recipient_name_normalized, 
                     recipient_location_state, award_type, fiscal_year
    - GSI range keys: fiscal_year, total_obligated_amount, period_start_date, period_end_date
    - Essential metadata: transaction_count, subaward_count, full_indexing_complete, 
                          last_updated, indexed_at, data_source, api_version, ttl
    """
    gsi_fields = {
        # Primary key (required)
        'award_id': full_item.get('award_id'),
        
        # GSI hash keys
        'awarding_agency_code': full_item.get('awarding_agency_code'),
        'awarding_agency_name': full_item.get('awarding_agency_name'),
        'recipient_name_normalized': full_item.get('recipient_name_normalized'),
        'recipient_location_state': full_item.get('recipient_location_state'),
        'award_type': full_item.get('award_type'),
        'fiscal_year': full_item.get('fiscal_year'),
        
        # GSI range keys (use total_obligated_amount from CSV)
        'total_obligated_amount': (full_item.get('total_obligated_amount') or 
                                  full_item.get('total_dollars_obligated')),
        # Map period_of_performance_start_date to period_start_date for GSI
        'period_start_date': (full_item.get('period_start_date') or 
                             full_item.get('period_of_performance_start_date')),
        # Map period_of_performance_end_date to period_end_date for GSI
        'period_end_date': (full_item.get('period_end_date') or 
                           full_item.get('period_of_performance_end_date')),
        
        # Essential metadata
        'transaction_count': full_item.get('transaction_count', 0),
        'subaward_count': full_item.get('subaward_count', 0),
        'full_indexing_complete': full_item.get('full_indexing_complete', True),
        'last_updated': full_item.get('last_updated'),
        'indexed_at': full_item.get('indexed_at'),
        'data_source': full_item.get('data_source', 'usaspending_bulk_download'),
        'api_version': full_item.get('api_version', 'bulk_csv_v2'),
        'ttl': full_item.get('ttl'),
        
        # Oversize flags
        'is_oversized': True,
    }
    
    # Remove None values (but keep False/0 values)
    cleaned = {}
    for k, v in gsi_fields.items():
        if v is not None:
            cleaned[k] = v
    
    return cleaned


def convert_decimal_for_json(obj: Any) -> Any:
    """Recursively convert Decimal to float/string for JSON serialization"""
    if isinstance(obj, Decimal):
        # Convert Decimal to float for JSON (preserves precision for most cases)
        try:
            return float(obj)
        except (OverflowError, ValueError):
            # If float conversion fails, use string representation
            return str(obj)
    elif isinstance(obj, dict):
        return {key: convert_decimal_for_json(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_decimal_for_json(item) for item in obj]
    else:
        return obj


def store_oversized_item_to_s3(award_id: str, full_item: Dict[str, Any]) -> str:
    """
    Store oversized item to S3 in oversize/ folder.
    Returns the S3 key.
    """
    # Create S3 key: oversize/{award_id}.json.gz
    s3_key = f"oversize/{award_id}.json.gz"
    
    # Convert Decimal values to JSON-serializable types
    json_ready_item = convert_decimal_for_json(full_item)
    
    # Convert to JSON
    json_data = json.dumps(json_ready_item, ensure_ascii=False, indent=2)
    
    # Compress and upload to S3
    json_bytes = json_data.encode('utf-8')
    compressed_data = gzip.compress(json_bytes)
    
    s3_client.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=s3_key,
        Body=compressed_data,
        ContentType='application/json',
        ContentEncoding='gzip'
    )
    
    log_print(f"   💾 Stored oversized award {award_id} to S3: {s3_key} ({len(compressed_data):,} bytes compressed, {len(json_bytes):,} bytes uncompressed)")
    return s3_key


def normalize_string(value: Any) -> Optional[str]:
    """Normalize string value for DynamoDB (handle None, empty strings, etc.)"""
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        return value if value else None
    return str(value).strip() if str(value).strip() else None

# ============================================================================
# Bulk Download Functions
# ============================================================================

def get_all_agencies() -> List[Dict[str, Any]]:
    """Get list of all award agencies from USAspending API"""
    log_print("📋 Fetching list of all agencies...")
    
    response = call_usaspending_api(
        "/api/v2/bulk_download/list_agencies",
        method='POST',
        body={
            "type": "award_agencies"
        }
    )
    
    if not response:
        raise Exception("Failed to get agencies list")
    
    agencies = []
    cfo_agencies = response.get("agencies", {}).get("cfo_agencies", [])
    other_agencies = response.get("agencies", {}).get("other_agencies", [])
    
    agencies.extend(cfo_agencies)
    agencies.extend(other_agencies)
    
    log_print(f"✅ Found {len(agencies)} agencies")
    return agencies

def initiate_bulk_download(start_date: str, end_date: str, agency: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Initiate bulk download for all award types with sub-awards included"""
    if agency:
        agency_name = agency.get('name', 'Unknown')
        log_print(f"📥 Initiating bulk download for {agency_name} - Date Range: {start_date} to {end_date}")
    else:
        log_print("=" * 80)
        log_print(f"📥 CHECKPOINT: Beginning Bulk Download - Date Range: {start_date} to {end_date}")
        log_print("=" * 80)
    
    bulk_filters = {
        "date_range": {
            "start_date": start_date,
            "end_date": end_date
        },
        "date_type": "action_date",
        "prime_award_types": [
            # Contract types
            "A", "B", "C", "D",
            # IDV types
            "IDV_A", "IDV_B", "IDV_B_A", "IDV_B_B", "IDV_B_C", "IDV_C", "IDV_D", "IDV_E",
            # Grant types
            "02", "03", "04", "05", "06", "07", "08", "09", "10", "11",
            # Other/Unknown
            "-1"
        ],
        "sub_award_types": ["grant", "procurement"]
    }
    
    # Add agency filter if provided (both awarding and funding)
    if agency:
        agency_name = agency.get('name')
        bulk_filters["agencies"] = [
            {
                "type": "awarding",
                "tier": "toptier",
                "name": agency_name
            },
            {
                "type": "funding",
                "tier": "toptier",
                "name": agency_name
            }
        ]
    
    request_body = {
        "filters": bulk_filters,
        "file_format": "csv"
    }
    
    log_print(f"📤 Bulk download request payload: {json.dumps(request_body, indent=2)}")
    
    response = call_usaspending_api(
        "/api/v2/bulk_download/awards/",
        method='POST',
        body=request_body
    )
    
    file_name = response.get("file_name")
    if not file_name:
        raise Exception("No file_name in bulk download response")
    
    if agency:
        log_print(f"✅ Bulk download initiated for {agency.get('name')}: {file_name}")
    else:
        log_print(f"✅ Bulk download initiated successfully: {file_name}")
    
    return {'file_name': file_name, 'response': response, 'agency': agency}

def poll_download_status(file_name: str, max_wait: int = 14400, poll_interval: int = 30, agency_name: Optional[str] = None) -> Dict[str, Any]:
    """Poll bulk download status until ready with improved error handling"""
    agency_prefix = f"[{agency_name}] " if agency_name else ""
    log_print(f"⏳ {agency_prefix}Starting to poll download status for {file_name}")
    log_print(f"   Max wait: {max_wait // 60} minutes | Poll interval: {poll_interval}s")
    start_time = time.time()
    attempt = 0
    consecutive_errors = 0
    max_consecutive_errors = 5
    last_logged_attempt = 0
    
    while time.time() - start_time < max_wait:
        attempt += 1
        elapsed_seconds = int(time.time() - start_time)
        elapsed_minutes = elapsed_seconds // 60
        
        try:
            log_print(f"📡 {agency_prefix}Polling attempt {attempt} (elapsed: {elapsed_minutes}m {elapsed_seconds % 60}s)...")
            
            response = call_usaspending_api(
                "/api/v2/bulk_download/status/",
                method='GET',
                params={"file_name": file_name}
            )
            
            consecutive_errors = 0
            
            status = response.get("status")
            message = response.get("message", "")
            seconds_elapsed_raw = response.get("seconds_elapsed")
            
            seconds_elapsed = None
            if seconds_elapsed_raw is not None:
                try:
                    seconds_elapsed = float(seconds_elapsed_raw)
                except (ValueError, TypeError):
                    seconds_elapsed = None
            
            file_url_check = response.get("file_url", "N/A")
            
            # Log status on every attempt
            log_print(f"📊 {agency_prefix}Status check {attempt}: status='{status}'")
            if message:
                log_print(f"   Message: {message[:200]}")
            if seconds_elapsed_raw is not None:
                log_print(f"   API elapsed time: {seconds_elapsed_raw}s")
            log_print(f"   File URL: {'✅ present' if file_url_check != 'N/A' else '❌ missing'}")
            
            if status not in ["running", "ready", "finished", "failed"]:
                log_print(f"⚠️ {agency_prefix}Unexpected status '{status}'. Full response: {json.dumps(response, default=str)[:500]}")
            
            if status == "ready" or status == "finished":
                file_url = response.get("file_url")
                if not file_url:
                    log_print(f"⚠️ {agency_prefix}Status is '{status}' but no file_url in response. Continuing to poll...")
                    time.sleep(poll_interval)
                    continue
                
                log_print("=" * 80)
                log_print(f"✅ {agency_prefix}DOWNLOAD STATUS: File Ready for Download")
                log_print(f"   File Name: {file_name}")
                if seconds_elapsed is not None:
                    minutes = int(seconds_elapsed // 60)
                    secs = int(seconds_elapsed % 60)
                    log_print(f"   Processing Time: {int(seconds_elapsed)} seconds ({minutes}m {secs}s)")
                log_print(f"   Total Polling Time: {elapsed_minutes}m {elapsed_seconds % 60}s ({attempt} attempts)")
                log_print(f"   File URL: {file_url}")
                log_print("=" * 80)
                
                return response
            elif status == "failed":
                raise Exception(f"{agency_prefix}Bulk download failed: {message or 'Unknown error'}")
            elif status == "running":
                log_print(f"⏳ {agency_prefix}File still processing... (will check again in {poll_interval}s)")
            else:
                log_print(f"⚠️ {agency_prefix}Unknown status '{status}' - continuing to poll... (response: {json.dumps(response, default=str)[:200]})")
            
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout,
                urllib3.exceptions.ProtocolError, urllib3.exceptions.NewConnectionError) as e:
            consecutive_errors += 1
            if consecutive_errors >= max_consecutive_errors:
                log_print(f"❌ {agency_prefix}Too many consecutive connection errors ({consecutive_errors}). Aborting.")
                raise
            
            error_backoff = min(poll_interval * (2 ** (consecutive_errors - 1)), 300)
            log_print(f"⚠️ {agency_prefix}Connection error during status check (attempt {attempt}, consecutive errors: {consecutive_errors}): {str(e)[:100]}")
            log_print(f"⏳ {agency_prefix}Waiting {error_backoff}s before next status check...")
            time.sleep(error_backoff)
            continue
        
        except Exception as e:
            log_print(f"⚠️ {agency_prefix}Unexpected error during status check (attempt {attempt}): {str(e)[:200]}")
            log_print(f"⏳ {agency_prefix}Continuing to poll...")
            time.sleep(poll_interval)
            continue
        
        time.sleep(poll_interval)
    
    raise Exception(f"Bulk download timeout after {max_wait} seconds ({max_wait // 60} minutes)")

def format_agency_name_for_s3(agency_name: str) -> str:
    """
    Convert agency name to S3-safe format using full name.
    Sanitizes for S3 compatibility while keeping it readable.
    """
    if not agency_name:
        return "Unknown"
    
    # Keep the full name, just sanitize for S3
    # Replace spaces with hyphens, remove special characters
    sanitized = agency_name.strip()
    # Replace multiple spaces with single space
    sanitized = " ".join(sanitized.split())
    # Replace spaces with hyphens
    sanitized = sanitized.replace(" ", "-")
    # Remove or replace special characters that aren't S3-safe
    sanitized = re.sub(r'[^a-zA-Z0-9\-_]', '', sanitized)
    # Remove multiple consecutive hyphens
    sanitized = re.sub(r'-+', '-', sanitized)
    # Remove leading/trailing hyphens
    sanitized = sanitized.strip('-')
    
    return sanitized if sanitized else "Unknown"

def format_agency_name_old_abbreviation(agency_name: str) -> str:
    """
    Convert agency name to old abbreviation format matching the original Glue script logic.
    Pattern: First letter of each word + rest of last word (capitalized).
    Special handling for "Department of " and "Department " prefixes.
    
    Examples:
    - "Department of Agriculture" -> "DOAgriculture"
    - "Department Defense" -> "DDefense"
    - "Administrative Board of Management Commission" -> "ABOMCommission"
    - "Administrative Board" -> "ABOard"
    """
    if not agency_name:
        return "Unknown"
    
    # Special handling for "Department of " prefix
    if agency_name.startswith("Department of "):
        rest = agency_name.replace("Department of ", "").strip()
        return "DO" + rest.capitalize()
    
    # Special handling for "Department " prefix
    if agency_name.startswith("Department "):
        rest = agency_name.replace("Department ", "").strip()
        return "D" + rest.capitalize()
    
    # Split into words (no filtering - uses ALL words)
    words = agency_name.strip().split()
    if not words:
        return "Unknown"
    
    # Single word - just capitalize it
    if len(words) == 1:
        return words[0].capitalize()
    
    # Get first letter of each word (uppercase)
    first_letters = "".join([word[0].upper() for word in words if word])
    
    # Append rest of last word (capitalized)
    last_word = words[-1]
    if len(last_word) > 1:
        rest_of_last = last_word[1:].capitalize()
        return first_letters + rest_of_last
    else:
        return first_letters

def format_date_range_path(start_date: str, end_date: str) -> str:
    """Format date range as MMDDYYYY-MMDDYYYY for S3 path"""
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    
    start_formatted = f"{start_dt.month}{start_dt.day}{start_dt.year}"
    end_formatted = f"{end_dt.month}{end_dt.day}{end_dt.year}"
    
    return f"{start_formatted}-{end_formatted}"

def check_s3_zip_exists(start_date: str, end_date: str, agency_name: str) -> Optional[str]:
    """
    Check if a ZIP file exists in S3 for the given date range and agency.
    Checks both new format (hyphenated full name) and old format (abbreviation).
    
    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        agency_name: Agency name
    
    Returns:
        S3 key of ZIP file if exists (in either format), None otherwise
    """
    date_range_path = format_date_range_path(start_date, end_date)
    
    # Check new format first (hyphenated full name)
    agency_filename_new = format_agency_name_for_s3(agency_name)
    zip_s3_key_new = f"{date_range_path}/{agency_filename_new}.zip"
    
    # Check old format (abbreviation)
    agency_filename_old = format_agency_name_old_abbreviation(agency_name)
    zip_s3_key_old = f"{date_range_path}/{agency_filename_old}.zip"
    
    zip_keys_to_check = [zip_s3_key_new, zip_s3_key_old]
    
    for zip_s3_key in zip_keys_to_check:
        try:
            s3_client.head_object(Bucket=S3_BUCKET_NAME, Key=zip_s3_key)
            log_print(f"✅ Found existing ZIP file in S3: {zip_s3_key}")
            return zip_s3_key
        except s3_client.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                continue
            else:
                log_print(f"⚠️ Error checking S3 for ZIP file {zip_s3_key}: {str(e)[:200]}")
                continue
    
    return None

def extract_zip_from_s3_to_folder(zip_s3_key: str, start_date: str, end_date: str, agency_name: str) -> Dict[str, str]:
    """
    Extract ZIP file from S3 and extract CSV files to a folder with hyphenated agency name.
    Checks if files already exist in the folder before extracting to avoid duplicates.
    
    Args:
        zip_s3_key: S3 key of the ZIP file
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        agency_name: Agency name
    
    Returns:
        Dict mapping CSV filename -> S3 key of extracted CSV file
    """
    agency_prefix = f"[{agency_name}] " if agency_name else ""
    date_range_path = format_date_range_path(start_date, end_date)
    agency_filename = format_agency_name_for_s3(agency_name)
    folder_prefix = f"{date_range_path}/{agency_filename}/"
    
    # Check if folder already has CSV files
    existing_csv_s3_keys = list_csv_files_from_folder(start_date, end_date, agency_name)
    
    if existing_csv_s3_keys:
        log_print(f"✅ {agency_prefix}CSV files already exist in folder: {folder_prefix}")
        log_print(f"📄 {agency_prefix}Found {len(existing_csv_s3_keys)} existing CSV file(s), skipping extraction")
        return existing_csv_s3_keys
    
    log_print(f"📦 {agency_prefix}Extracting ZIP file from S3 to folder: {folder_prefix}")
    
    # Download ZIP from S3
    log_print(f"📥 {agency_prefix}Downloading ZIP from S3: {zip_s3_key}")
    zip_obj = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=zip_s3_key)
    zip_content = zip_obj['Body'].read()
    
    # Extract ZIP and upload CSV files to folder
    csv_s3_keys = {}
    zip_file = BytesIO(zip_content)
    
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        file_list = zip_ref.namelist()
        log_print(f"📋 {agency_prefix}ZIP contains {len(file_list)} file(s)")
        
        # Find CSV files
        csv_files = [f for f in file_list if f.lower().endswith('.csv')]
        if not csv_files:
            raise Exception("No CSV files found in ZIP")
        
        log_print(f"📄 {agency_prefix}Extracting {len(csv_files)} CSV file(s) to S3 folder...")
        
        for csv_file in csv_files:
            try:
                csv_content = zip_ref.read(csv_file)
                csv_s3_key = f"{folder_prefix}{csv_file}"
                
                # Check if file already exists before uploading
                try:
                    s3_client.head_object(Bucket=S3_BUCKET_NAME, Key=csv_s3_key)
                    log_print(f"   ⏭️  Skipping {csv_file} - already exists in S3")
                    # Extract just the filename from the path
                    filename = csv_file.split('/')[-1]
                    csv_s3_keys[filename] = csv_s3_key
                    del csv_content
                    continue
                except s3_client.exceptions.ClientError as e:
                    if e.response['Error']['Code'] != '404':
                        raise
                
                # File doesn't exist, upload it
                s3_client.put_object(
                    Bucket=S3_BUCKET_NAME,
                    Key=csv_s3_key,
                    Body=csv_content,
                    ContentType='text/csv'
                )
                
                # Extract just the filename from the path
                filename = csv_file.split('/')[-1]
                csv_s3_keys[filename] = csv_s3_key
                log_print(f"   ✅ Extracted {filename} to {csv_s3_key}")
                
                # Clear CSV content from memory immediately
                del csv_content
            except Exception as e:
                log_print(f"   ⚠️ Failed to extract {csv_file} to S3: {str(e)[:200]}")
    
    # Clear ZIP from memory
    del zip_content
    del zip_file
    gc.collect()
    
    log_print(f"✅ {agency_prefix}Extracted {len(csv_s3_keys)} CSV file(s) to S3 folder: {folder_prefix}")
    return csv_s3_keys

def save_zip_to_s3(zip_content: bytes, start_date: str, end_date: str, agency_name: str) -> str:
    """Save downloaded ZIP file to S3"""
    date_range_path = format_date_range_path(start_date, end_date)
    agency_filename = format_agency_name_for_s3(agency_name)
    s3_key = f"{date_range_path}/{agency_filename}.zip"
    
    log_print(f"💾 Saving ZIP file to S3: s3://{S3_BUCKET_NAME}/{s3_key}")
    
    s3_client.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=s3_key,
        Body=zip_content,
        ContentType='application/zip'
    )
    
    log_print(f"✅ ZIP file saved to S3: s3://{S3_BUCKET_NAME}/{s3_key}")
    return s3_key

# ============================================================================
# CSV Parsing Functions
# ============================================================================

def parse_prime_award_csv_streaming(csv_file_obj, csv_filename: str) -> Dict[str, Dict[str, Any]]:
    """
    Parse prime award CSV file from stream and return award records with all columns preserved.
    Groups transactions by award_id. Uses streaming to avoid loading entire file into memory.
    
    Args:
        csv_file_obj: File-like object (stream) containing CSV content
        csv_filename: Name of the CSV file (for logging)
    
    Returns:
        Dict mapping award_id -> award_record with all CSV columns
    """
    awards = {}
    row_count = 0
    
    reader = csv.DictReader(csv_file_obj)
    
    for row in reader:
        row_count += 1
        
        # Log progress for large files
        if row_count % 100000 == 0:
            log_print(f"   📊 Processing row {row_count:,} of {csv_filename}...")
        
        # Get award ID - primary key for grouping and DynamoDB primary key
        # For contracts: uses contract_award_unique_key
        # For assistance: uses assistance_award_unique_key (this becomes the award_id primary key)
        # Check both contract and assistance award unique keys
        award_id = (
            row.get('contract_award_unique_key') or      # For contracts
            row.get('assistance_award_unique_key') or    # For assistance awards (becomes primary key award_id)
            row.get('generated_unique_award_id') or     # Generic fallback (works for both)
            row.get('award_id') or                       # Generic fallback
            None
        )
        
        if not award_id:
            continue
        
        # Initialize award record if first time seeing this award
        if award_id not in awards:
            # Extract fiscal year from period start date
            period_start = row.get('period_of_performance_start_date')
            fiscal_year = extract_fiscal_year(period_start)
            
            # Create award record with ALL columns from CSV
            # Convert all values to appropriate types
            award_record = {}
            for key, value in row.items():
                if value is None or value == '':
                    continue
                
                # Try to convert numeric values
                if key in ['federal_action_obligation', 'total_dollars_obligated', 'total_obligated_amount',
                          'total_outlayed_amount_for_overall_award', 'base_and_exercised_options_value',
                          'current_total_value_of_award', 'base_and_all_options_value',
                          'potential_total_value_of_award', 'action_date_fiscal_year']:
                    try:
                        award_record[key] = Decimal(str(value))
                    except:
                        award_record[key] = normalize_string(value)
                else:
                    award_record[key] = normalize_string(value)
            
            # Normalize common fields to reduce blanks (maps contract/assistance fields to common names)
            award_record = normalize_common_fields(award_record, record_type="prime")
            
            # Add computed fields
            # award_id is the primary key in DynamoDB - for assistance awards this matches assistance_award_unique_key
            award_record['award_id'] = award_id
            award_record['fiscal_year'] = fiscal_year
            award_record['transaction_count'] = 0
            award_record['transactions'] = []
            award_record['subawards'] = []
            award_record['indexed_at'] = datetime.now(timezone.utc).isoformat()
            award_record['last_updated'] = datetime.now(timezone.utc).isoformat()
            award_record['data_source'] = 'usaspending_bulk_download'
            award_record['api_version'] = 'bulk_csv_v2'
            award_record['award_details_indexed'] = False
            award_record['full_indexing_complete'] = False
            award_record['ttl'] = int((datetime.now(timezone.utc).timestamp() + (90 * 24 * 60 * 60)))
            
            awards[award_id] = award_record
        
        # Add transaction to award
        award = awards[award_id]
        award['transaction_count'] += 1
        
        # Store transaction record (all columns)
        transaction_record = {}
        for key, value in row.items():
            if value is None or value == '':
                continue
            if key in ['federal_action_obligation', 'total_dollars_obligated', 'total_obligated_amount']:
                try:
                    transaction_record[key] = Decimal(str(value))
                except:
                    transaction_record[key] = normalize_string(value)
            else:
                transaction_record[key] = normalize_string(value)
        
        # Normalize common fields to reduce blanks
        transaction_record = normalize_common_fields(transaction_record, record_type="transaction")
        
        award['transactions'].append(transaction_record)
    
    log_print(f"✅ Parsed {csv_filename}: {len(awards)} unique awards, {sum(a['transaction_count'] for a in awards.values())} total transactions, {row_count:,} rows processed")
    return awards

def parse_prime_award_csv(csv_content: str, csv_filename: str) -> Dict[str, Dict[str, Any]]:
    """
    Parse prime award CSV file from string (legacy method for backward compatibility).
    Groups transactions by award_id.
    
    Args:
        csv_content: CSV content as string
        csv_filename: Name of the CSV file (for logging)
    
    Returns:
        Dict mapping award_id -> award_record with all CSV columns
    """
    return parse_prime_award_csv_streaming(StringIO(csv_content), csv_filename)

def parse_subaward_csv_streaming(csv_file_obj, csv_filename: str) -> Dict[str, List[Dict[str, Any]]]:
    """
    Parse sub-award CSV file from stream and return sub-awards grouped by parent award ID.
    Uses streaming to avoid loading entire file into memory.
    
    Args:
        csv_file_obj: File-like object (stream) containing CSV content
        csv_filename: Name of the CSV file (for logging)
    
    Returns:
        Dict mapping parent_award_id -> list of sub-award records
    """
    subawards_by_parent = {}
    row_count = 0
    
    reader = csv.DictReader(csv_file_obj)
    
    for row in reader:
        row_count += 1
        
        # Log progress for large files
        if row_count % 100000 == 0:
            log_print(f"   📊 Processing row {row_count:,} of {csv_filename}...")
        
        # Get parent award ID (links sub-award to prime award)
        parent_award_id = (
            row.get('prime_award_unique_key') or
            row.get('prime_award_piid') or  # For contracts
            row.get('prime_award_fain') or  # For assistance
            None
        )
        
        if not parent_award_id:
            continue
        
        # Create sub-award record with ALL columns from CSV
        subaward_record = {}
        for key, value in row.items():
            if value is None or value == '':
                continue
            
            # Try to convert numeric values
            if key in ['subaward_amount', 'prime_award_amount', 'subaward_action_date_fiscal_year',
                      'prime_award_base_action_date_fiscal_year', 'prime_award_latest_action_date_fiscal_year']:
                try:
                    subaward_record[key] = Decimal(str(value))
                except:
                    subaward_record[key] = normalize_string(value)
            else:
                subaward_record[key] = normalize_string(value)
        
        # Normalize common fields to reduce blanks
        subaward_record = normalize_common_fields(subaward_record, record_type="subaward")
        
        # Group sub-awards by parent award
        if parent_award_id not in subawards_by_parent:
            subawards_by_parent[parent_award_id] = []
        
        subawards_by_parent[parent_award_id].append(subaward_record)
    
    total_subawards = sum(len(subs) for subs in subawards_by_parent.values())
    log_print(f"✅ Parsed {csv_filename}: {len(subawards_by_parent)} parent awards, {total_subawards} total sub-awards, {row_count:,} rows processed")
    return subawards_by_parent

def parse_subaward_csv(csv_content: str, csv_filename: str) -> Dict[str, List[Dict[str, Any]]]:
    """
    Parse sub-award CSV file from string (legacy method for backward compatibility).
    
    Args:
        csv_content: CSV content as string
        csv_filename: Name of the CSV file (for logging)
    
    Returns:
        Dict mapping parent_award_id -> list of sub-award records
    """
    return parse_subaward_csv_streaming(StringIO(csv_content), csv_filename)

def download_and_parse_all_csvs(file_url: str, agency_name: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None) -> Dict[str, Any]:
    """
    Download ZIP file, extract all CSV files, and parse prime awards and sub-awards separately.
    
    Returns:
        Dict with:
        - 'prime_awards': Dict of award_id -> award_record
        - 'subawards_by_parent': Dict of parent_award_id -> list of sub-award records
    """
    agency_prefix = f"[{agency_name}] " if agency_name else ""
    parse_start_time = time.time()
    
    log_print(f"📥 {agency_prefix}Downloading file from {file_url}")
    
    session = create_session()
    
    # Verify file is accessible
    log_print(f"🔍 {agency_prefix}Verifying file accessibility before download...")
    max_verification_retries = 15
    verification_retry_delay = 60
    
    file_verified = False
    for verify_attempt in range(max_verification_retries):
        try:
            log_print(f"   Verification attempt {verify_attempt + 1}/{max_verification_retries}...")
            head_response = session.head(file_url, timeout=60, allow_redirects=True)
            if head_response.status_code == 200:
                file_verified = True
                log_print(f"✅ {agency_prefix}File verification successful - file is accessible")
                break
            elif head_response.status_code == 403:
                if verify_attempt < max_verification_retries - 1:
                    wait_time = min(60 * (2 ** min(verify_attempt, 5)), 600)
                    log_print(f"⚠️ {agency_prefix}File not yet accessible (403 Forbidden). Waiting {wait_time}s before retry...")
                    time.sleep(wait_time)
                    continue
                else:
                    raise Exception(f"File verification failed: 403 Forbidden after {max_verification_retries} attempts")
            else:
                head_response.raise_for_status()
        except Exception as e:
            if verify_attempt < max_verification_retries - 1:
                wait_time = min(60 * (2 ** min(verify_attempt, 5)), 600)
                log_print(f"⚠️ {agency_prefix}Verification error (attempt {verify_attempt + 1}): {str(e)[:100]}")
                log_print(f"   Waiting {wait_time}s before retry...")
                time.sleep(wait_time)
                continue
            raise
    
    if not file_verified:
        raise Exception(f"File not accessible after {max_verification_retries} verification attempts")
    
    # Download file
    log_print(f"📥 {agency_prefix}Starting file download from URL...")
    log_print(f"   URL: {file_url}")
    download_start_time = time.time()
    max_download_retries = 5
    
    response = None
    content = None
    
    for attempt in range(max_download_retries):
        try:
            log_print(f"   Download attempt {attempt + 1}/{max_download_retries}...")
            response = session.get(file_url, timeout=300, stream=True)
            response.raise_for_status()
            
            # Read the content
            log_print(f"   Reading response content...")
            content = response.content
            download_duration = time.time() - download_start_time
            file_size_mb = len(content) / (1024 * 1024)
            file_size_bytes = len(content)
            
            log_print("=" * 80)
            log_print(f"✅ {agency_prefix}FILE DOWNLOAD SUCCESSFUL")
            log_print(f"   File Size: {file_size_mb:.2f} MB ({file_size_bytes:,} bytes)")
            log_print(f"   Download Time: {int(download_duration // 60)}m {int(download_duration % 60)}s")
            if download_duration > 0:
                log_print(f"   Download Speed: {file_size_mb / download_duration:.2f} MB/s")
            log_print("=" * 80)
            break
        except Exception as e:
            if attempt < max_download_retries - 1:
                wait_time = 30 * (attempt + 1)
                log_print(f"❌ {agency_prefix}Download attempt {attempt + 1} failed: {str(e)[:200]}")
                log_print(f"   Retrying in {wait_time}s...")
                time.sleep(wait_time)
                continue
            log_print(f"❌ {agency_prefix}Download failed after {max_download_retries} attempts")
            raise
    
    if content is None:
        raise Exception(f"{agency_prefix}Download failed - no content received")
    
    # Extract ZIP
    zip_content = BytesIO(content)
    
    # Initialize variables outside the with block
    csv_s3_keys = {}
    prime_file_list = []
    subaward_file_list = []
    date_range_path = format_date_range_path(start_date, end_date) if start_date and end_date else "unknown"
    agency_filename = format_agency_name_for_s3(agency_name) if agency_name else "unknown"
    
    with zipfile.ZipFile(zip_content, 'r') as zip_ref:
        file_list = zip_ref.namelist()
        log_print(f"📋 {agency_prefix}ZIP contains {len(file_list)} file(s)")
        
        # Find CSV files
        csv_files = [f for f in file_list if f.lower().endswith('.csv')]
        if not csv_files:
            raise Exception("No CSV files found in ZIP")
        
        # Separate prime and sub-award files
        prime_files = [f for f in csv_files if 'subaward' not in f.lower()]
        subaward_files = [f for f in csv_files if 'subaward' in f.lower()]
        
        log_print(f"📄 {agency_prefix}Found {len(csv_files)} CSV file(s):")
        log_print(f"   - {len(prime_files)} Prime award file(s)")
        log_print(f"   - {len(subaward_files)} Sub-award file(s)")
        
        # Save ZIP to S3 if agency name and dates provided
        s3_zip_key = None
        if agency_name and start_date and end_date:
            try:
                s3_zip_key = save_zip_to_s3(content, start_date, end_date, agency_name)
                log_print(f"💾 {agency_prefix}ZIP saved to S3, will extract CSV files to S3 to avoid memory issues")
            except Exception as e:
                log_print(f"⚠️ {agency_prefix}Failed to save ZIP to S3 (non-critical): {str(e)[:200]}")
        
        # Extract CSV files to S3 individually to avoid loading all into memory
        # Check if folder already has files first
        folder_prefix = f"{date_range_path}/{agency_filename}/"
        existing_csv_s3_keys = list_csv_files_from_folder(start_date, end_date, agency_name)
        
        if existing_csv_s3_keys:
            log_print(f"✅ {agency_prefix}CSV files already exist in folder: {folder_prefix}")
            log_print(f"📄 {agency_prefix}Found {len(existing_csv_s3_keys)} existing CSV file(s), skipping extraction")
            csv_s3_keys = existing_csv_s3_keys
        else:
            log_print(f"📦 {agency_prefix}Extracting CSV files to S3...")
            for csv_file in csv_files:
                try:
                    csv_content = zip_ref.read(csv_file)
                    # Use full department name in path for readability
                    csv_s3_key = f"{folder_prefix}{csv_file}"
                    
                    # Check if file already exists before uploading
                    try:
                        s3_client.head_object(Bucket=S3_BUCKET_NAME, Key=csv_s3_key)
                        log_print(f"   ⏭️  Skipping {csv_file} - already exists in S3")
                        csv_s3_keys[csv_file] = csv_s3_key
                        del csv_content
                        continue
                    except s3_client.exceptions.ClientError as e:
                        if e.response['Error']['Code'] != '404':
                            raise
                    
                    # File doesn't exist, upload it
                    s3_client.put_object(
                        Bucket=S3_BUCKET_NAME,
                        Key=csv_s3_key,
                        Body=csv_content,
                        ContentType='text/csv'
                    )
                    csv_s3_keys[csv_file] = csv_s3_key
                    log_print(f"   ✅ Extracted {csv_file} to S3")
                    # Clear CSV content from memory immediately
                    del csv_content
                except Exception as e:
                    log_print(f"   ⚠️ Failed to extract {csv_file} to S3: {str(e)[:200]}")
        
        # Store file lists for later reading
        prime_file_list = prime_files.copy()
        subaward_file_list = subaward_files.copy()
    
    # ZIP file is now closed, clear from memory
    del zip_content
    del content
    del response
    gc.collect()
    
    # Validate that we have S3 keys for the files we need
    if not csv_s3_keys:
        raise Exception(f"{agency_prefix}No CSV files were successfully extracted to S3. Cannot proceed with parsing.")
    
    missing_prime_files = [f for f in prime_file_list if f not in csv_s3_keys]
    missing_subaward_files = [f for f in subaward_file_list if f not in csv_s3_keys]
    
    if missing_prime_files:
        log_print(f"⚠️ {agency_prefix}Warning: {len(missing_prime_files)} prime file(s) not extracted to S3: {missing_prime_files}")
    if missing_subaward_files:
        log_print(f"⚠️ {agency_prefix}Warning: {len(missing_subaward_files)} sub-award file(s) not extracted to S3: {missing_subaward_files}")
    
    # Only parse files that were successfully extracted
    prime_file_list = [f for f in prime_file_list if f in csv_s3_keys]
    subaward_file_list = [f for f in subaward_file_list if f in csv_s3_keys]
    
    return _parse_csvs_from_s3(csv_s3_keys, prime_file_list, subaward_file_list, agency_name, start_date, end_date)

def list_csv_files_from_folder(start_date: str, end_date: str, agency_name: str) -> Dict[str, str]:
    """
    List all CSV files in the S3 folder for the given date range and agency.
    
    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        agency_name: Agency name
    
    Returns:
        Dict mapping CSV filename -> S3 key
    """
    date_range_path = format_date_range_path(start_date, end_date)
    agency_filename = format_agency_name_for_s3(agency_name)
    folder_prefix = f"{date_range_path}/{agency_filename}/"
    
    csv_s3_keys = {}
    
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix=folder_prefix)
        
        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    key = obj['Key']
                    if key.lower().endswith('.csv'):
                        # Extract just the filename from the S3 key
                        filename = key.split('/')[-1]
                        csv_s3_keys[filename] = key
        
        return csv_s3_keys
    except Exception as e:
        log_print(f"⚠️ Error listing S3 CSV files for {folder_prefix}: {str(e)[:200]}")
        return csv_s3_keys

def parse_csvs_from_s3(start_date: str, end_date: str, agency_name: str) -> Dict[str, Any]:
    """
    Parse CSV files from S3 folder for the given date range and agency.
    This is used when CSV files already exist in S3 folder (extracted from ZIP).
    
    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        agency_name: Agency name
    
    Returns:
        Dict with:
        - 'prime_awards': Dict of award_id -> award_record
        - 'subawards_by_parent': Dict of parent_award_id -> list of sub-award records
    """
    agency_prefix = f"[{agency_name}] " if agency_name else ""
    
    log_print(f"📂 {agency_prefix}Parsing CSV files from S3 folder...")
    
    # List CSV files from S3 folder
    csv_s3_keys = list_csv_files_from_folder(start_date, end_date, agency_name)
    
    if not csv_s3_keys:
        raise Exception(f"{agency_prefix}No CSV files found in S3 folder for date range {start_date} to {end_date}")
    
    log_print(f"📄 {agency_prefix}Found {len(csv_s3_keys)} CSV file(s) in S3 folder")
    
    # Separate prime and sub-award files
    prime_file_list = [f for f in csv_s3_keys.keys() if 'subaward' not in f.lower()]
    subaward_file_list = [f for f in csv_s3_keys.keys() if 'subaward' in f.lower()]
    
    log_print(f"   - {len(prime_file_list)} Prime award file(s)")
    log_print(f"   - {len(subaward_file_list)} Sub-award file(s)")
    
    return _parse_csvs_from_s3(csv_s3_keys, prime_file_list, subaward_file_list, agency_name, start_date, end_date)

def _parse_csvs_from_s3(csv_s3_keys: Dict[str, str], prime_file_list: List[str], subaward_file_list: List[str], 
                        agency_name: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None) -> Dict[str, Any]:
    """
    Internal function to parse CSV files from S3 keys.
    Used by both download_and_parse_all_csvs and parse_csvs_from_s3.
    
    Args:
        csv_s3_keys: Dict mapping CSV filename -> S3 key
        prime_file_list: List of prime award CSV filenames
        subaward_file_list: List of subaward CSV filenames
        agency_name: Optional agency name for logging
        start_date: Optional start date for logging
        end_date: Optional end date for logging
    
    Returns:
        Dict with:
        - 'prime_awards': Dict of award_id -> award_record
        - 'subawards_by_parent': Dict of parent_award_id -> list of sub-award records
    """
    agency_prefix = f"[{agency_name}] " if agency_name else ""
    parse_start_time = time.time()
    
    # Parse all prime award files (read from S3)
    # Use same parallel processing as before (no sequential processing needed since we deduplicated)
    all_prime_awards = {}
    parse_workers = len(prime_file_list) * 5
    
    if len(prime_file_list) > 0:
        log_print(f"📊 {agency_prefix}Parsing {len(prime_file_list)} prime award file(s) with {parse_workers} workers (5x files)...")
        log_print(f"   Reading CSV files from S3 (extracted individually)")
        
        def parse_prime_file_from_s3(file_name):
            try:
                # Read CSV directly from S3 using streaming to avoid memory issues
                csv_s3_key = csv_s3_keys.get(file_name)
                if not csv_s3_key:
                    error_msg = f"S3 key not found for {file_name}. Available keys: {list(csv_s3_keys.keys())}"
                    log_print(f"❌ {agency_prefix}{error_msg}")
                    raise Exception(error_msg)
                
                log_print(f"   📥 Streaming {file_name} from S3: {csv_s3_key}")
                csv_obj = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=csv_s3_key)
                
                # Stream CSV content in chunks to avoid loading entire file into memory
                stream = csv_obj['Body']
                decoder = codecs.getreader('utf-8')
                csv_file_obj = decoder(stream)
                
                # Parse CSV directly from stream
                return parse_prime_award_csv_streaming(csv_file_obj, file_name)
            except Exception as e:
                error_msg = f"Error parsing {file_name}: {str(e)}"
                log_print(f"❌ {agency_prefix}{error_msg}")
                logger.error(f"❌ {agency_prefix}{error_msg}", exc_info=True)
                return {}
        
        with ThreadPoolExecutor(max_workers=parse_workers) as executor:
            future_to_file = {
                executor.submit(parse_prime_file_from_s3, prime_file): prime_file
                for prime_file in prime_file_list
            }
            
            for future in as_completed(future_to_file):
                prime_file = future_to_file[future]
                try:
                    prime_awards = future.result()
                    # Merge into all_prime_awards (handle duplicates)
                    for award_id, award_data in prime_awards.items():
                        if award_id in all_prime_awards:
                            # Merge transactions
                            all_prime_awards[award_id]['transactions'].extend(award_data['transactions'])
                            all_prime_awards[award_id]['transaction_count'] += award_data['transaction_count']
                        else:
                            all_prime_awards[award_id] = award_data
                except Exception as e:
                    log_print(f"❌ {agency_prefix}Error processing results from {prime_file}: {str(e)[:200]}")
    
    # Parse all sub-award files in parallel (read from S3)
    all_subawards_by_parent = {}
    subaward_parse_workers = len(subaward_file_list) * 5
    
    if len(subaward_file_list) > 0:
        log_print(f"📊 {agency_prefix}Parsing {len(subaward_file_list)} sub-award file(s) with {subaward_parse_workers} workers (5x files)...")
        log_print(f"   Reading CSV files from S3 (extracted individually)")
        
        def parse_subaward_file_from_s3(file_name):
            try:
                # Read CSV directly from S3 using streaming to avoid memory issues
                csv_s3_key = csv_s3_keys.get(file_name)
                if not csv_s3_key:
                    error_msg = f"S3 key not found for {file_name}. Available keys: {list(csv_s3_keys.keys())}"
                    log_print(f"❌ {agency_prefix}{error_msg}")
                    raise Exception(error_msg)
                
                log_print(f"   📥 Streaming {file_name} from S3: {csv_s3_key}")
                csv_obj = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=csv_s3_key)
                
                # Stream CSV content in chunks to avoid loading entire file into memory
                stream = csv_obj['Body']
                decoder = codecs.getreader('utf-8')
                csv_file_obj = decoder(stream)
                
                # Parse CSV directly from stream
                return parse_subaward_csv_streaming(csv_file_obj, file_name)
            except Exception as e:
                error_msg = f"Error parsing {file_name}: {str(e)}"
                log_print(f"❌ {agency_prefix}{error_msg}")
                logger.error(f"❌ {agency_prefix}{error_msg}", exc_info=True)
                return {}
        
        with ThreadPoolExecutor(max_workers=subaward_parse_workers) as executor:
            future_to_file = {
                executor.submit(parse_subaward_file_from_s3, subaward_file): subaward_file
                for subaward_file in subaward_file_list
            }
            
            for future in as_completed(future_to_file):
                subaward_file = future_to_file[future]
                try:
                    subawards_by_parent = future.result()
                    # Merge into all_subawards_by_parent
                    for parent_id, subawards in subawards_by_parent.items():
                        if parent_id in all_subawards_by_parent:
                            all_subawards_by_parent[parent_id].extend(subawards)
                        else:
                            all_subawards_by_parent[parent_id] = subawards
                except Exception as e:
                    log_print(f"❌ {agency_prefix}Error processing results from {subaward_file}: {str(e)[:200]}")
    
    # Link sub-awards to their parent awards
    log_print(f"🔗 {agency_prefix}Linking sub-awards to parent awards...")
    linked_count = 0
    unlinked_subawards = {}
    
    for parent_id, subawards in all_subawards_by_parent.items():
        if parent_id in all_prime_awards:
            all_prime_awards[parent_id]['subawards'] = subawards
            linked_count += 1
        else:
            # Parent not found in bulk file - check DynamoDB
            unlinked_subawards[parent_id] = subawards
    
    # Check DynamoDB for missing parent awards
    if unlinked_subawards:
        log_print(f"🔍 {agency_prefix}Checking DynamoDB for {len(unlinked_subawards)} missing parent awards...")
        dynamodb_linked = 0
        
        for parent_id, subawards in unlinked_subawards.items():
            try:
                # Try to get parent award from DynamoDB
                response = awards_table.get_item(Key={'award_id': parent_id})
                if 'Item' in response:
                    # Parent exists in DynamoDB - add subawards to it via update
                    log_print(f"✅ {agency_prefix}Found parent award {parent_id} in DynamoDB (will update during indexing)")
                    # Store subawards to be added during indexing
                    # We'll handle this by creating a minimal award record for indexing
                    if parent_id not in all_prime_awards:
                        # Create a minimal award record that will trigger an update
                        all_prime_awards[parent_id] = {
                            'award_id': parent_id,
                            'subawards': subawards,
                            'subaward_count': len(subawards),
                            'update_from_dynamodb': True,
                            'existing_item': response['Item']
                        }
                    else:
                        all_prime_awards[parent_id]['subawards'] = subawards
                    dynamodb_linked += 1
                else:
                    log_print(f"⚠️ {agency_prefix}Parent award {parent_id} not found in bulk file or DynamoDB for {len(subawards)} sub-awards")
            except Exception as e:
                error_msg = f"Error checking DynamoDB for parent {parent_id}: {str(e)}"
                log_print(f"⚠️ {agency_prefix}{error_msg}")
                logger.error(f"⚠️ {agency_prefix}{error_msg}", exc_info=True)
                # Continue processing other parents even if one fails
        
        if dynamodb_linked > 0:
            log_print(f"✅ {agency_prefix}Found {dynamodb_linked} parent awards in DynamoDB")
    
    log_print(f"✅ {agency_prefix}Linked {linked_count} parent awards with sub-awards from bulk file")
    
    parse_duration = time.time() - parse_start_time
    log_print(f"✅ {agency_prefix}CSV Parsing Completed:")
    log_print(f"   📊 Prime Awards: {len(all_prime_awards):,}")
    log_print(f"   📊 Sub-Awards: {sum(len(subs) for subs in all_subawards_by_parent.values()):,}")
    log_print(f"   ⏱️ Parse Time: {int(parse_duration // 60)}m {int(parse_duration % 60)}s")
    
    return {
        'prime_awards': all_prime_awards,
        'subawards_by_parent': all_subawards_by_parent
    }

# ============================================================================
# Indexing Functions
# ============================================================================

def index_award_complete(award_record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Complete award indexing: store all columns to DynamoDB and upload details to S3.
    
    Args:
        award_record: Award record with all CSV columns plus transactions and subawards
    
    Returns:
        Dict with success status and counts
    """
    try:
        award_id = award_record['award_id']
        
        # Prepare DynamoDB item - preserve ALL columns from CSV
        db_item = {}
        
        # Copy all fields from award_record, converting types appropriately
        for key, value in award_record.items():
            # Skip internal fields that shouldn't be in DB
            if key in ['transactions', 'subawards']:
                continue
            
            if value is None:
                continue
            
            # Convert to DynamoDB-compatible types
            if isinstance(value, (int, float)):
                try:
                    db_item[key] = Decimal(str(value))
                except:
                    db_item[key] = normalize_string(value)
            elif isinstance(value, bool):
                db_item[key] = value
            elif isinstance(value, str):
                normalized = normalize_string(value)
                if normalized:
                    db_item[key] = normalized
            elif isinstance(value, list):
                # Convert list elements
                converted_list = []
                for item in value:
                    if isinstance(item, (int, float)):
                        try:
                            converted_list.append(Decimal(str(item)))
                        except:
                            converted_list.append(normalize_string(item))
                    else:
                        converted_list.append(normalize_string(item))
                db_item[key] = converted_list
            elif isinstance(value, dict):
                db_item[key] = convert_floats_to_decimal(value)
            else:
                db_item[key] = normalize_string(value)
        
        # Ensure required GSI fields are present
        # GSI hash keys: awarding_agency_code, awarding_agency_name, recipient_name_normalized, 
        #                recipient_location_state, award_type
        # GSI range keys: fiscal_year, total_obligated_amount, period_start_date, period_end_date
        
        # Set fiscal_year if missing (required for most GSIs)
        if 'fiscal_year' not in db_item or db_item.get('fiscal_year') is None:
            period_start = db_item.get('period_of_performance_start_date')
            fiscal_year = extract_fiscal_year(period_start)
            if fiscal_year:
                db_item['fiscal_year'] = fiscal_year
            else:
                # Use current fiscal year as default
                now = datetime.now(timezone.utc)
                db_item['fiscal_year'] = now.year + 1 if now.month >= 10 else now.year
        
        # Normalize recipient_name for GSI
        recipient_name = db_item.get('recipient_name') or db_item.get('prime_awardee_name')
        if recipient_name:
            # Replace "REDACTED DUE TO PII" with full expansion
            if recipient_name.upper() == "REDACTED DUE TO PII":
                recipient_name = "REDACTED DUE TO PERSONALLY IDENTIFIABLE INFORMATION"
                db_item['recipient_name'] = recipient_name
                # Also update recipient_name_raw if it exists
                if 'recipient_name_raw' in db_item:
                    db_item['recipient_name_raw'] = recipient_name
            db_item['recipient_name_normalized'] = recipient_name.lower().strip()
        
        # Map assistance-specific fields to common GSI fields
        # For assistance: assistance_type_description → award_type
        if 'assistance_type_description' in db_item and 'award_type' not in db_item:
            db_item['award_type'] = db_item['assistance_type_description']
        # For assistance: recipient_state_code → recipient_location_state
        if 'recipient_state_code' in db_item and 'recipient_location_state' not in db_item:
            db_item['recipient_location_state'] = db_item['recipient_state_code']
        # For assistance: action_date_fiscal_year → fiscal_year (if fiscal_year not already set)
        if 'action_date_fiscal_year' in db_item and ('fiscal_year' not in db_item or db_item.get('fiscal_year') is None):
            try:
                db_item['fiscal_year'] = int(db_item['action_date_fiscal_year'])
            except:
                pass
        
        # Ensure total_obligated_amount is Decimal (use from CSV, don't calculate)
        # Normalize: use total_obligated_amount if available, otherwise total_dollars_obligated
        if 'total_obligated_amount' not in db_item or not db_item.get('total_obligated_amount'):
            if 'total_dollars_obligated' in db_item and db_item.get('total_dollars_obligated'):
                db_item['total_obligated_amount'] = db_item['total_dollars_obligated']
        
        if 'total_obligated_amount' in db_item and not isinstance(db_item['total_obligated_amount'], Decimal):
            try:
                db_item['total_obligated_amount'] = Decimal(str(db_item['total_obligated_amount']))
            except:
                db_item['total_obligated_amount'] = Decimal('0')
        
        # Map period_of_performance fields to period_start_date/period_end_date for GSI compatibility
        if 'period_of_performance_start_date' in db_item and 'period_start_date' not in db_item:
            db_item['period_start_date'] = db_item['period_of_performance_start_date']
        if 'period_of_performance_end_date' in db_item and 'period_end_date' not in db_item:
            db_item['period_end_date'] = db_item['period_of_performance_end_date']
        
        # Set is_assistance binary field (0 = contract, 1 = assistance)
        # Check if it's an assistance award by looking for assistance-specific fields
        # Also check award_id format (ASST_ prefix indicates assistance)
        is_assistance = False
        if (db_item.get('assistance_award_unique_key') or 
            db_item.get('award_id_fain') or 
            award_record.get('assistance_award_unique_key') or 
            award_record.get('award_id_fain')):
            is_assistance = True
        elif award_id and award_id.startswith('ASST_'):
            # Award ID format indicates assistance
            is_assistance = True
        elif award_id and award_id.startswith('CONT_'):
            # Award ID format indicates contract
            is_assistance = False
        else:
            # Default to contract if we can't determine
            is_assistance = False
        
        db_item['is_assistance'] = Decimal('1') if is_assistance else Decimal('0')
        
        # Get transactions and subawards (already in award_record from CSV parsing)
        transactions = award_record.get('transactions', [])
        subawards = award_record.get('subawards', [])
        transaction_count = len(transactions)
        subaward_count = len(subawards)
        
        # Check if award already exists in DynamoDB (for transaction merging)
        try:
            existing_response = awards_table.get_item(Key={'award_id': award_id})
            existing_item = existing_response.get('Item')
        except Exception as e:
            log_print(f"⚠️ Error checking DynamoDB for existing award {award_id}: {str(e)[:200]}")
            existing_item = None
        
        if existing_item:
            # Update existing item with subawards AND transactions (if present in bulk download)
            # This handles cases where:
            # 1. Orphan sub-award: parent not in bulk download but exists in DynamoDB
            # 2. Transaction adjustments: parent appears in bulk download with updated transactions
            db_item = existing_item.copy()
            
            # Merge transactions if present in bulk download (transaction adjustments)
            if transactions:
                existing_transactions = db_item.get('transactions', [])
                if existing_transactions:
                    # Deduplicate transactions by transaction_unique_key, assistance_transaction_unique_key, or contract_transaction_unique_key
                    existing_tx_keys = set()
                    for tx in existing_transactions:
                        # Try multiple transaction key fields
                        tx_key = (
                            tx.get('transaction_unique_key') or
                            tx.get('assistance_transaction_unique_key') or
                            tx.get('contract_transaction_unique_key') or
                            None
                        )
                        if tx_key:
                            existing_tx_keys.add(str(tx_key))
                        else:
                            # Fallback: use action_date + modification_number as unique key
                            action_date = tx.get('action_date')
                            mod_num = tx.get('modification_number')
                            if action_date and mod_num:
                                existing_tx_keys.add(f"{action_date}_{mod_num}")
                    
                    new_transactions = []
                    for tx in transactions:
                        # Try multiple transaction key fields
                        tx_key = (
                            tx.get('transaction_unique_key') or
                            tx.get('assistance_transaction_unique_key') or
                            tx.get('contract_transaction_unique_key') or
                            None
                        )
                        if not tx_key:
                            # Fallback: use action_date + modification_number as unique key
                            action_date = tx.get('action_date')
                            mod_num = tx.get('modification_number')
                            if action_date and mod_num:
                                tx_key = f"{action_date}_{mod_num}"
                        
                        if not tx_key or tx_key not in existing_tx_keys:
                            new_transactions.append(tx)
                    
                    if new_transactions:
                        db_item['transactions'] = existing_transactions + convert_floats_to_decimal(new_transactions)
                        db_item['transaction_count'] = len(db_item['transactions'])
                        # Update total_obligated_amount from the latest CSV row (don't recalculate)
                        if 'total_obligated_amount' in award_record and award_record.get('total_obligated_amount'):
                            db_item['total_obligated_amount'] = award_record['total_obligated_amount']
                        elif 'total_dollars_obligated' in award_record and award_record.get('total_dollars_obligated'):
                            db_item['total_obligated_amount'] = award_record['total_dollars_obligated']
                        log_print(f"   📊 Updated award {award_id}: Added {len(new_transactions)} new transaction(s), updated total_obligated_amount from CSV")
                else:
                    # No existing transactions, use new ones
                    db_item['transactions'] = convert_floats_to_decimal(transactions)
                    db_item['transaction_count'] = transaction_count
                    # Use total_obligated_amount from CSV (don't calculate)
                    if 'total_obligated_amount' in award_record and award_record.get('total_obligated_amount'):
                        db_item['total_obligated_amount'] = award_record['total_obligated_amount']
                    elif 'total_dollars_obligated' in award_record and award_record.get('total_dollars_obligated'):
                        db_item['total_obligated_amount'] = award_record['total_dollars_obligated']
                    log_print(f"   📊 Updated award {award_id}: Added {transaction_count} transaction(s) from bulk download")
            
            # Merge subawards (append to existing if any)
            existing_subawards = db_item.get('subawards', [])
            if existing_subawards and subawards:
                # Combine and deduplicate subawards by subaward_id or subaward_number
                existing_ids = set()
                for sub in existing_subawards:
                    sub_id = sub.get('subaward_id') or sub.get('subaward_number') or sub.get('subaward_sam_report_id')
                    if sub_id:
                        existing_ids.add(str(sub_id))
                
                new_subawards = []
                for sub in subawards:
                    sub_id = sub.get('subaward_id') or sub.get('subaward_number') or sub.get('subaward_sam_report_id')
                    if not sub_id or str(sub_id) not in existing_ids:
                        new_subawards.append(sub)
                
                if new_subawards:
                    db_item['subawards'] = existing_subawards + convert_floats_to_decimal(new_subawards)
                    log_print(f"   📊 Updated award {award_id}: Added {len(new_subawards)} new sub-award(s)")
                else:
                    db_item['subawards'] = existing_subawards
            elif subawards:
                db_item['subawards'] = convert_floats_to_decimal(subawards)
                log_print(f"   📊 Updated award {award_id}: Added {len(subawards)} sub-award(s)")
            
            # Update counts and timestamp
            db_item['transaction_count'] = len(db_item.get('transactions', []))
            db_item['subaward_count'] = len(db_item.get('subawards', []))
            db_item['last_updated'] = datetime.now(timezone.utc).isoformat()
            
            # Ensure all fields are properly converted
            db_item = convert_floats_to_decimal(db_item)
            
            # Update item in DynamoDB with retry logic for throttling and oversized items
            max_put_retries = 3
            for put_attempt in range(max_put_retries):
                try:
                    awards_table.put_item(Item=db_item)
                    break
                except Exception as put_error:
                    error_str = str(put_error)
                    
                    # Handle oversized items (ValidationException)
                    if 'ValidationException' in error_str and 'Item size has exceeded' in error_str:
                        log_print(f"⚠️ Updated award {award_id} exceeds DynamoDB size limit, storing to S3...")
                        
                        # Store full item to S3
                        oversize_s3_key = store_oversized_item_to_s3(award_id, db_item)
                        
                        # Extract only GSI fields for DynamoDB
                        gsi_only_item = extract_gsi_fields_only(db_item)
                        gsi_only_item['oversize_s3_key'] = oversize_s3_key
                        
                        # Try to store GSI-only item
                        try:
                            awards_table.put_item(Item=gsi_only_item)
                            log_print(f"✅ Stored GSI fields for oversized award {award_id} to DynamoDB, full data in S3")
                            break
                        except Exception as gsi_error:
                            log_print(f"❌ Even GSI-only item too large for {award_id}: {str(gsi_error)}")
                            raise
                    
                    # Handle throttling
                    elif 'ThrottlingException' in error_str or 'ProvisionedThroughputExceededException' in error_str:
                        if put_attempt < max_put_retries - 1:
                            wait_time = (put_attempt + 1) * 2  # 2s, 4s, 6s
                            log_print(f"⚠️ DynamoDB throttled for award {award_id}, waiting {wait_time}s before retry {put_attempt + 1}/{max_put_retries}")
                            time.sleep(wait_time)
                            continue
                    
                    # Re-raise if not throttling or out of retries
                    raise
        else:
            # New item - store transactions and subawards directly in DynamoDB
            # Convert to DynamoDB-compatible format
            if transactions:
                db_item['transactions'] = convert_floats_to_decimal(transactions)
            if subawards:
                db_item['subawards'] = convert_floats_to_decimal(subawards)
            
            # Convert all floats to Decimal
            db_item = convert_floats_to_decimal(db_item)
            
            # Add completion flags and counts
            db_item['transaction_count'] = transaction_count
            db_item['subaward_count'] = subaward_count
            db_item['full_indexing_complete'] = True
            db_item['last_updated'] = datetime.now(timezone.utc).isoformat()
            
            # Store to DynamoDB with retry logic for throttling and oversized items
            max_put_retries = 3
            for put_attempt in range(max_put_retries):
                try:
                    awards_table.put_item(Item=db_item)
                    break
                except Exception as put_error:
                    error_str = str(put_error)
                    
                    # Handle oversized items (ValidationException)
                    if 'ValidationException' in error_str and 'Item size has exceeded' in error_str:
                        log_print(f"⚠️ Award {award_id} exceeds DynamoDB size limit, storing to S3...")
                        
                        # Store full item to S3
                        oversize_s3_key = store_oversized_item_to_s3(award_id, db_item)
                        
                        # Extract only GSI fields for DynamoDB
                        gsi_only_item = extract_gsi_fields_only(db_item)
                        gsi_only_item['oversize_s3_key'] = oversize_s3_key
                        
                        # Try to store GSI-only item
                        try:
                            awards_table.put_item(Item=gsi_only_item)
                            log_print(f"✅ Stored GSI fields for oversized award {award_id} to DynamoDB, full data in S3")
                            break
                        except Exception as gsi_error:
                            log_print(f"❌ Even GSI-only item too large for {award_id}: {str(gsi_error)}")
                            raise
                    
                    # Handle throttling
                    elif 'ThrottlingException' in error_str or 'ProvisionedThroughputExceededException' in error_str:
                        if put_attempt < max_put_retries - 1:
                            wait_time = (put_attempt + 1) * 2  # 2s, 4s, 6s
                            log_print(f"⚠️ DynamoDB throttled for award {award_id}, waiting {wait_time}s before retry {put_attempt + 1}/{max_put_retries}")
                            time.sleep(wait_time)
                            continue
                    
                    # Re-raise if not throttling or out of retries
                    raise
        
        return {
            'success': True,
            'award_id': award_id,
            'transaction_count': transaction_count,
            'subaward_count': subaward_count
        }
    
    except Exception as e:
        log_print(f"❌ Error indexing award {award_record.get('award_id', 'unknown')}: {str(e)}")
        logger.error(f"❌ Error indexing award: {str(e)}", exc_info=True)
        raise

# ============================================================================
# Main Job Logic
# ============================================================================

def main():
    """Main Glue job execution"""
    try:
        # Get date range from parameters or default to yesterday
        start_date = args.get('START_DATE') or args.get('--START_DATE')
        end_date = args.get('END_DATE') or args.get('--END_DATE')
        
        if not start_date:
            yesterday = datetime.now(timezone.utc) - timedelta(days=1)
            start_date = yesterday.strftime('%Y-%m-%d')
            log_print(f"ℹ️ No START_DATE provided, defaulting to yesterday: {start_date}")
        else:
            try:
                datetime.strptime(start_date, '%Y-%m-%d')
            except ValueError:
                raise ValueError(f"Invalid START_DATE format: {start_date}. Expected YYYY-MM-DD")
        
        if not end_date:
            end_date = start_date
            log_print(f"ℹ️ No END_DATE provided, defaulting to START_DATE: {end_date}")
        else:
            try:
                datetime.strptime(end_date, '%Y-%m-%d')
            except ValueError:
                raise ValueError(f"Invalid END_DATE format: {end_date}. Expected YYYY-MM-DD")
        
        # Validate date range
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        if end_dt < start_dt:
            raise ValueError(f"END_DATE ({end_date}) must be >= START_DATE ({start_date})")
        
        days_diff = (end_dt - start_dt).days + 1
        log_print(f"📅 Starting bulk indexing for date range: {start_date} to {end_date} ({days_diff} day(s))")
        
        # Step 1: Get all agencies
        log_print("=" * 80)
        log_print("📋 Step 1: Fetching All Agencies")
        log_print("=" * 80)
        agencies = get_all_agencies()
        
        if not agencies:
            raise Exception("No agencies found. Cannot proceed without agencies.")
        
        log_print(f"✅ Found {len(agencies)} agencies - starting processing...")
        
        # TEST MODE: Limit to first department (Department of Agriculture)
        test_mode = False
        if test_mode:
            agencies = [a for a in agencies if a.get('name') == 'Department of Agriculture']
            log_print(f"🧪 TEST MODE: Processing only Department of Agriculture (first department)")
            if not agencies:
                raise Exception("Department of Agriculture not found in agencies list")
        
        # Process each agency
        job_start_time = time.time()
        log_print("=" * 80)
        log_print(f"📥 Step 2: Processing Agencies Sequentially")
        log_print(f"   Each agency will be fully downloaded, parsed, and indexed before the next begins")
        log_print(f"   Total Agencies: {len(agencies)}")
        log_print("=" * 80)
        
        total_indexed = 0
        total_subawards_all_agencies = 0
        total_transactions_all_agencies = 0
        successful_agencies = 0
        
        for i, agency in enumerate(agencies, 1):
            agency_name = agency.get('name', 'Unknown')
            agency_start_time = time.time()
            log_print(f"\n{'=' * 80}")
            log_print(f"📦 Processing Agency {i}/{len(agencies)}: {agency_name}")
            log_print(f"📅 Date Range: {start_date} to {end_date}")
            log_print(f"{'=' * 80}")
            
            # Check if ZIP file already exists in S3
            log_print(f"\n🔍 Checking if ZIP file already exists in S3...")
            zip_s3_key = check_s3_zip_exists(start_date, end_date, agency_name)
            
            if zip_s3_key:
                log_print(f"✅ Found existing ZIP file in S3 for date range {start_date} to {end_date}")
                log_print(f"⏭️  Skipping bulk download - extracting ZIP and using CSV files from S3")
                
                # Extract ZIP to folder
                log_print(f"\n📦 Extracting ZIP file to S3 folder...")
                extract_phase_start = time.time()
                csv_s3_keys = extract_zip_from_s3_to_folder(zip_s3_key, start_date, end_date, agency_name)
                extract_phase_duration = time.time() - extract_phase_start
                log_print(f"✅ Extraction Complete: {int(extract_phase_duration // 60)}m {int(extract_phase_duration % 60)}s")
                
                # PHASE 2: PARSE - Parse CSV files from extracted folder
                log_print(f"\n🟡 PHASE 2: PARSE - Parsing CSV Files from S3 for {agency_name}")
                log_print(f"{'─' * 80}")
                parse_phase_start = time.time()
                
                # Separate prime and sub-award files
                prime_file_list = [f for f in csv_s3_keys.keys() if 'subaward' not in f.lower()]
                subaward_file_list = [f for f in csv_s3_keys.keys() if 'subaward' in f.lower()]
                
                parse_results = _parse_csvs_from_s3(csv_s3_keys, prime_file_list, subaward_file_list, agency_name, start_date, end_date)
            else:
                # PHASE 1: GET - Request and wait for bulk download
                log_print(f"\n🔵 PHASE 1: GET - Requesting Bulk Download for {agency_name}")
                log_print(f"{'─' * 80}")
                get_phase_start = time.time()
                
                download_info = initiate_bulk_download(start_date, end_date, agency=agency)
                file_name = download_info['file_name']
                log_print(f"📋 File Name: {file_name}")
                
                status_info = poll_download_status(file_name, max_wait=14400, poll_interval=30, agency_name=agency_name)
                file_url = status_info.get('file_url')
                
                if not file_url:
                    raise Exception(f"No file_url in download status response for {agency_name}")
                
                get_phase_duration = time.time() - get_phase_start
                log_print(f"✅ GET Phase Complete: {int(get_phase_duration // 60)}m {int(get_phase_duration % 60)}s")
                log_print(f"📁 File URL: {file_url}")
                
                # PHASE 2: PARSE - Download and parse all CSV files
                log_print(f"\n🟡 PHASE 2: PARSE - Downloading and Parsing All CSV Files for {agency_name}")
                log_print(f"{'─' * 80}")
                parse_phase_start = time.time()
                
                parse_results = download_and_parse_all_csvs(file_url, agency_name=agency_name, start_date=start_date, end_date=end_date)
            prime_awards = parse_results['prime_awards']
            
            if not prime_awards:
                parse_phase_duration = time.time() - parse_phase_start
                agency_duration = time.time() - agency_start_time
                log_print(f"ℹ️ {agency_name}: No awards found for date range (this is OK)")
                log_print(f"⏱️ Parse Phase: {int(parse_phase_duration // 60)}m {int(parse_phase_duration % 60)}s")
                log_print(f"⏱️ Total Agency Time: {int(agency_duration // 60)}m {int(agency_duration % 60)}s")
                successful_agencies += 1
                continue
            
            parse_phase_duration = time.time() - parse_phase_start
            log_print(f"✅ PARSE Phase Complete: {int(parse_phase_duration // 60)}m {int(parse_phase_duration % 60)}s")
            log_print(f"📊 Extracted {len(prime_awards)} unique award records from CSV")
            
            # PHASE 3: STORE - Index awards with all columns
            log_print(f"\n🟢 PHASE 3: STORE - Indexing Awards for {agency_name}")
            log_print(f"{'─' * 80}")
            store_phase_start = time.time()
            
            # TEST MODE: Limit to first 10 contracts AND 10 assistance awards (20 total)
            test_mode = False
            if test_mode:
                # Separate contracts and assistance awards from prime_awards
                contracts = []
                assistance = []
                
                for award_id, award in prime_awards.items():
                    # Check if it's an assistance award (has assistance_award_unique_key)
                    if award.get('assistance_award_unique_key') or award.get('award_id_fain'):
                        assistance.append(award)
                    else:
                        # Assume it's a contract (has contract_award_unique_key or award_id_piid)
                        contracts.append(award)
                
                # Limit to first 10 of each
                contracts = contracts[:10]
                assistance = assistance[:10]
                award_list = contracts + assistance
                
                log_print(f"🧪 TEST MODE: Limiting to first 10 contracts and 10 assistance awards")
                log_print(f"   📊 Contracts: {len(contracts)}/{len([a for a in prime_awards.values() if not (a.get('assistance_award_unique_key') or a.get('award_id_fain'))])}")
                log_print(f"   📊 Assistance: {len(assistance)}/{len([a for a in prime_awards.values() if (a.get('assistance_award_unique_key') or a.get('award_id_fain'))])}")
                log_print(f"   📊 Total: {len(award_list)}/{len(prime_awards)} awards")
            else:
                award_list = list(prime_awards.values())
            
            log_print(f"\n📦 Indexing {len(award_list)} awards in parallel (all columns preserved)")
            
            # Reduce write workers to avoid DynamoDB throttling
            max_workers = min(10, len(award_list))
            log_print(f"⚙️ Parallel Processing: {max_workers} workers (reduced to avoid throttling)")
            
            agency_indexed = 0
            agency_errors = []
            total_transactions = 0
            total_subawards = 0
            
            with _progress_lock:
                _progress_counter['processed'] = 0
                _progress_counter['indexed'] = 0
                _progress_counter['total'] = len(award_list)
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_award = {
                    executor.submit(index_award_complete, award_record): award_record['award_id']
                    for award_record in award_list
                }
                
                for future in as_completed(future_to_award):
                    award_id = future_to_award[future]
                    try:
                        result = future.result()
                        with _progress_lock:
                            _progress_counter['processed'] += 1
                            _progress_counter['indexed'] += 1
                            agency_indexed += 1
                            total_transactions += result.get('transaction_count', 0)
                            total_subawards += result.get('subaward_count', 0)
                            
                            processed = _progress_counter['processed']
                            
                            if processed % 50 == 0 or processed == len(award_list):
                                elapsed = time.time() - store_phase_start
                                rate = processed / elapsed if elapsed > 0 else 0
                                remaining = (len(award_list) - processed) / rate if rate > 0 else 0
                                log_print(f"  📊 Progress: {processed}/{len(award_list)} awards ({agency_indexed} indexed)")
                                log_print(f"     ⚡ Rate: {rate:.2f} awards/sec | ⏱️ ETA: {int(remaining // 60)}m {int(remaining % 60)}s")
                    except Exception as e:
                        error_msg = f"Award {award_id}: {str(e)[:200]}"
                        agency_errors.append(error_msg)
                        log_print(f"❌ {error_msg}")
            
            store_phase_duration = time.time() - store_phase_start
            agency_duration = time.time() - agency_start_time
            
            if agency_errors:
                log_print(f"⚠️ {len(agency_errors)} errors occurred during indexing (see logs above)")
            
            log_print(f"\n{'─' * 80}")
            log_print(f"✅ STORE Phase Complete: {int(store_phase_duration // 60)}m {int(store_phase_duration % 60)}s")
            log_print(f"📊 Total transactions indexed: {total_transactions:,}")
            log_print(f"📊 Total subawards indexed: {total_subawards:,}")
            log_print(f"📊 {agency_name} Summary:")
            log_print(f"   📥 Parsed from CSV: {len(award_list):,} unique awards")
            log_print(f"   ✅ Indexed: {agency_indexed}")
            log_print(f"   📦 Total Processed: {len(award_list):,}")
            log_print(f"⏱️ Total Agency Time: {int(agency_duration // 60)}m {int(agency_duration % 60)}s")
            log_print(f"{'─' * 80}")
            
            total_indexed += agency_indexed
            total_subawards_all_agencies += total_subawards
            total_transactions_all_agencies += total_transactions
            successful_agencies += 1
            
            # Clear memory before next agency
            log_print(f"🧹 {agency_name}: Clearing memory before next agency...")
            del prime_awards
            del parse_results
            del award_list
            del agency_errors
            gc.collect()
            log_print(f"✅ {agency_name}: Memory cleared")
        
        total_job_duration = time.time() - job_start_time
        log_print("\n" + "=" * 80)
        log_print(f"📊 FINAL SUMMARY - All Agencies Processed")
        log_print("=" * 80)
        log_print(f"  ✅ Agencies Processed: {successful_agencies}/{len(agencies)}")
        log_print(f"  📦 Total Awards Indexed: {total_indexed:,}")
        log_print(f"  📊 Total Transactions Indexed: {total_transactions_all_agencies:,}")
        log_print(f"  📊 Total Subawards Indexed: {total_subawards_all_agencies:,}")
        if successful_agencies > 0:
            avg_per_agency = total_indexed / successful_agencies
            log_print(f"  📈 Average Awards per Agency: {avg_per_agency:.1f}")
        log_print(f"  ⏱️ Total Job Duration: {int(total_job_duration // 3600)}h {int((total_job_duration % 3600) // 60)}m {int(total_job_duration % 60)}s")
        log_print("=" * 80)
        
        log_print("✅ Job completed successfully - committing")
        job.commit()
        
    except Exception as e:
        import traceback
        error_msg = f"CRITICAL ERROR in bulk indexing job: {str(e)}"
        error_traceback = traceback.format_exc()
        log_print(f"❌ {error_msg}")
        log_print(f"❌ Traceback:\n{error_traceback}")
        logger.error(f"❌ {error_msg}", exc_info=True)
        logger.error(f"❌ Traceback:\n{error_traceback}")
        # Re-raise to trigger Glue job failure (exit code 10)
        raise

if __name__ == "__main__":
    main()
