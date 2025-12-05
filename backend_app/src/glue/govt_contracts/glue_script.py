"""
AWS Glue Job: USAspending Daily Bulk Indexing
Downloads bulk CSV files from USAspending, parses them directly (no API calls for award data),
fetches subawards via API, and indexes everything to DynamoDB and S3.

This job runs daily via Step Functions to:
1. Get date range from parameters
2. For each agency, initiate bulk download
3. Download and parse CSV file directly from S3/bulk download
4. Extract award records from CSV (transactions included)
5. For each award, fetch subawards via API
6. Store award metadata in DynamoDB and transaction/subaward details in S3
"""

import sys
import json
import logging
import time
import csv
import gzip
import zipfile
import urllib3
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

# Get optional date parameters (if provided)
# getResolvedOptions requires all listed args, so we parse optional ones separately
# Note: getResolvedOptions expects argument names WITHOUT the -- prefix in the list
# (even though they're passed with -- in the command line)
try:
    optional_args = getResolvedOptions(sys.argv, ['START_DATE', 'END_DATE'])
    # getResolvedOptions returns keys without -- prefix
    args.update(optional_args)
    print(f"✅ Successfully parsed optional arguments: START_DATE={optional_args.get('START_DATE')}, END_DATE={optional_args.get('END_DATE')}", flush=True)
except Exception as e:
    # Optional args not provided - will default in main()
    # Note: log_print not yet defined, using print() directly
    print(f"ℹ️ Optional date arguments not provided (will default in main()): {str(e)[:200]}", flush=True)
    pass

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configure logging
# Glue jobs benefit from both logger and print() for visibility
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Also use print() for critical messages - Glue shows these more reliably
def log_print(message):
    """Print to both logger and stdout for maximum visibility in Glue"""
    logger.info(message)
    print(message, file=sys.stdout, flush=True)

# Log script initialization
log_print("=" * 80)
log_print("✅ USAspending Bulk Indexing Glue Job - Script Loaded Successfully")
log_print("=" * 80)

# Environment variables
USASPENDING_BASE_URL = args.get('USASPENDING_BASE_URL', 'https://api.usaspending.gov')
USASPENDING_USER_AGENT = args.get('USASPENDING_USER_AGENT', 'Cosine Financial Platform (contact@cosine.financial)')
AWARDS_TABLE_NAME = args.get('AWARDS_TABLE_NAME', 'usaspending-awards-index')
S3_BUCKET_NAME = args.get('S3_BUCKET_NAME', 'cosine-usaspending-data-production')
REQUEST_TIMEOUT = int(args.get('REQUEST_TIMEOUT', '30'))
# Rate limiting: minimum seconds between API calls
API_RATE_LIMIT_DELAY = float(args.get('API_RATE_LIMIT_DELAY', '0.5'))  # 500ms default
# Max retries for connection errors
MAX_RETRIES = int(args.get('MAX_RETRIES', '5'))
# Base delay for exponential backoff (seconds)
RETRY_BASE_DELAY = float(args.get('RETRY_BASE_DELAY', '2.0'))

# AWS clients
dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')
awards_table = dynamodb.Table(AWARDS_TABLE_NAME)

log_print(f"ℹ️ Configuration: Table={AWARDS_TABLE_NAME}, S3 Bucket={S3_BUCKET_NAME}, API={USASPENDING_BASE_URL}")
log_print(f"ℹ️ Rate Limiting: {API_RATE_LIMIT_DELAY}s delay, {MAX_RETRIES} max retries, {RETRY_BASE_DELAY}s base delay")

# Global session for connection pooling
_global_session = None
_last_api_call_time = 0

# Thread-safe progress tracking for parallel processing
_progress_lock = Lock()
_progress_counter = {'indexed': 0, 'total': 0, 'processed': 0}

# ============================================================================
# Helper Functions (ported from Lambda)
# ============================================================================

def create_session():
    """Create a requests session with proper headers, SSL handling, and retry logic for Glue environment"""
    global _global_session
    
    if _global_session is not None:
        return _global_session
    
    session = requests.Session()
    session.headers.update({
        'User-Agent': USASPENDING_USER_AGENT,
        'Accept': 'application/json',
        'Content-Type': 'application/json',
    })
    # Disable SSL verification for Glue environment (sometimes has certificate chain issues)
    # This is safe for public APIs like USAspending.gov
    session.verify = False
    
    # Configure retry strategy for connection errors
    # Note: Glue environment uses older urllib3, so use method_whitelist instead of allowed_methods
    retry_strategy = Retry(
        total=MAX_RETRIES,
        backoff_factor=RETRY_BASE_DELAY,
        status_forcelist=[429, 500, 502, 503, 504],  # Retry on rate limit and server errors
        method_whitelist=["GET", "POST"],  # Older urllib3 parameter name (allowed_methods in newer versions)
        raise_on_status=False  # We'll handle status codes manually
    )
    
    # Increased pool sizes to handle parallel requests better
    # But keep reasonable limits to avoid overwhelming the API
    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=20,  # Increased from 10 - more connection pools for parallel requests
        pool_maxsize=30       # Increased from 20 - more connections per pool
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
    
    # Reduced rate limit delay for parallel processing (0.1s instead of 0.5s)
    # Since we're already controlling concurrency with ThreadPoolExecutor,
    # we can use a smaller delay here to avoid unnecessary waiting
    parallel_rate_limit = 0.1  # 100ms between calls per thread
    if time_since_last_call < parallel_rate_limit:
        sleep_time = parallel_rate_limit - time_since_last_call
        time.sleep(sleep_time)
    
    _last_api_call_time = time.time()


def call_usaspending_api(endpoint: str, method: str = 'GET', body: Optional[Dict] = None, params: Optional[Dict] = None) -> Dict[str, Any]:
    """Call USAspending API endpoint with retry logic and rate limiting"""
    url = f"{USASPENDING_BASE_URL}{endpoint}"
    session = create_session()
    
    # Enforce rate limiting
    rate_limit()
    
    # Retry logic with exponential backoff for connection errors
    last_exception = None
    for attempt in range(MAX_RETRIES + 1):
        try:
            if method.upper() == 'POST':
                response = session.post(url, json=body, timeout=REQUEST_TIMEOUT)
            else:
                response = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            
            # Check for rate limiting (429)
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', RETRY_BASE_DELAY * (2 ** attempt)))
                if attempt < MAX_RETRIES:
                    log_print(f"⚠️ Rate limited (429). Waiting {retry_after}s before retry {attempt + 1}/{MAX_RETRIES}")
                    time.sleep(retry_after)
                    continue
                else:
                    response.raise_for_status()
            
            # Check for server errors (5xx)
            if response.status_code >= 500:
                if attempt < MAX_RETRIES:
                    backoff_delay = RETRY_BASE_DELAY * (2 ** attempt)
                    log_print(f"⚠️ Server error {response.status_code}. Retrying in {backoff_delay}s (attempt {attempt + 1}/{MAX_RETRIES})")
                    time.sleep(backoff_delay)
                    continue
                else:
                    response.raise_for_status()
            
            # Success - raise for any other HTTP errors
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
            # Don't retry on 4xx errors (except 429 which is handled above)
            if e.response.status_code == 429:
                continue  # Will be handled by rate limit logic above
            log_print(f"❌ HTTP error {e.response.status_code}: {str(e)}")
            raise
        except Exception as e:
            # Unexpected errors - don't retry
            log_print(f"❌ Unexpected error: {str(e)}")
            raise
    
    # If we get here, all retries failed
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




def fetch_all_subawards(award_id: str) -> List[Dict[str, Any]]:
    """Fetch all subawards for an award (paginated)"""
    all_subawards = []
    page = 1
    limit = 100
    
    while True:
        response = call_usaspending_api(
            '/api/v2/subawards/',
            method='POST',
            body={
                'award_id': award_id,
                'page': page,
                'limit': limit,
                'sort': 'amount',
                'order': 'desc'
            }
        )
        
        if not response:
            break
        
        subawards = response.get('results', [])
        if not subawards:
            break
        
        all_subawards.extend(subawards)
        
        page_metadata = response.get('page_metadata', {})
        if not page_metadata.get('hasNext', False):
            break
        
        page += 1
    
    return all_subawards


def upload_award_details_to_s3(award_id: str, transactions: List[Dict], subawards: List[Dict]) -> str:
    """Upload combined transactions and subawards to S3"""
    transaction_count = len(transactions)
    subaward_count = len(subawards)
    
    combined_data = {
        'transactions': transactions,
        'subawards': subawards,
        'indexed_at': datetime.now(timezone.utc).isoformat(),
        'transaction_count': transaction_count,
        'subaward_count': subaward_count
    }
    
    combined_json = json.dumps(combined_data, default=str)
    combined_gzipped = gzip.compress(combined_json.encode('utf-8'))
    s3_key = f"award-details/{award_id}/details.json.gz"
    
    s3_client.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=s3_key,
        Body=combined_gzipped,
        ContentType='application/json',
        ContentEncoding='gzip',
        ServerSideEncryption='aws:kms'
    )
    
    return s3_key


def check_s3_key_exists(s3_key: str) -> bool:
    """Check if an S3 key exists in the bucket"""
    try:
        s3_client.head_object(Bucket=S3_BUCKET_NAME, Key=s3_key)
        return True
    except Exception as e:
        # Check if it's a 404 (NoSuchKey) error
        error_code = getattr(e, 'response', {}).get('Error', {}).get('Code', '')
        if error_code == '404' or 'NoSuchKey' in str(e):
            return False
        # For other errors, log and assume it doesn't exist to be safe
        log_print(f"⚠️ Warning: Error checking S3 key {s3_key}: {str(e)[:200]}")
        return False


def index_award_metadata(award_record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Index award metadata to DynamoDB only (fast, parallel, no API calls).
    This is Phase 3a - just stores the award metadata.
    """
    try:
        award_id = award_record['award_id']
        
        # Create a copy for DynamoDB (without transactions list)
        award_db_record = award_record.copy()
        award_db_record.pop('transactions', None)  # Remove transactions from DB record
        
        # Remove None/empty values for sparse GSI fields
        # DynamoDB sparse indexes don't allow NULL values - field must be omitted entirely
        # Only include these fields if they have actual values
        sparse_gsi_fields = ['cfda_number', 'naics_code', 'psc_code', 'awarding_agency_code', 
                             'funding_agency_code', 'recipient_id', 'recipient_location_state']
        for field in sparse_gsi_fields:
            if award_db_record.get(field) is None or award_db_record.get(field) == '':
                award_db_record.pop(field, None)
        
        # Convert to Decimal for DynamoDB
        award_db_record['total_obligation'] = Decimal(str(award_db_record.get('total_obligation', 0)))
        award_db_record = convert_floats_to_decimal(award_db_record)
        
        # Store award metadata in DynamoDB (no S3 key or completion flags yet)
        awards_table.put_item(Item=award_db_record)
        
        return {'success': True, 'award_id': award_id}
    
    except Exception as e:
        log_print(f"❌ Error indexing award metadata {award_record.get('award_id', 'unknown')}: {str(e)}")
        logger.error(f"❌ Error indexing award metadata: {str(e)}", exc_info=True)
        raise


def index_award_complete(award_record: Dict[str, Any], delay: float = 0) -> Dict[str, Any]:
    """
    Complete award indexing in a single function: metadata, transactions, and subawards.
    This function does everything:
    1. Index metadata to DynamoDB
    2. Fetch subawards from API
    3. Upload transactions + subawards to S3
    4. Update DynamoDB with completion flags
    5. Optional delay to respect API rate limits for large batches
    
    Designed to be called in parallel with ThreadPoolExecutor.
    Rate limiting is handled by the API retry logic in call_usaspending_api.
    
    Args:
        award_record: The award record to index
        delay: Optional delay in seconds to add after processing (for rate limiting large batches)
    """
    try:
        award_id = award_record['award_id']
        
        # Step 1: Index metadata to DynamoDB
        # Create a copy for DynamoDB (without transactions list)
        award_db_record = award_record.copy()
        award_db_record.pop('transactions', None)  # Remove transactions from DB record
        
        # Remove None/empty values for sparse GSI fields
        sparse_gsi_fields = ['cfda_number', 'naics_code', 'psc_code', 'awarding_agency_code', 
                             'funding_agency_code', 'recipient_id', 'recipient_location_state']
        for field in sparse_gsi_fields:
            if award_db_record.get(field) is None or award_db_record.get(field) == '':
                award_db_record.pop(field, None)
        
        # Convert to Decimal for DynamoDB
        award_db_record['total_obligation'] = Decimal(str(award_db_record.get('total_obligation', 0)))
        award_db_record = convert_floats_to_decimal(award_db_record)
        
        # Store award metadata in DynamoDB
        awards_table.put_item(Item=award_db_record)
        
        # Step 2: Get transactions from CSV record (already parsed)
        transactions = award_record.get('transactions', [])
        transaction_count = len(transactions)
        
        # Step 3: Fetch subawards from API
        try:
            subawards = fetch_all_subawards(award_id)
            subaward_count = len(subawards)
        except Exception as e:
            logger.error(f"⚠️ Failed to fetch subawards for {award_id}: {str(e)}", exc_info=True)
            subawards = []
            subaward_count = 0
        
        # Step 4: Upload transactions and subawards to S3
        s3_key = upload_award_details_to_s3(award_id, transactions, subawards)
        
        # Step 5: Update DynamoDB with S3 key and completion flags
        update_expression = "SET award_details_s3_key = :s3_key, award_details_indexed = :indexed, transaction_count = :tx_count, subaward_count = :sub_count, full_indexing_complete = :complete, last_updated = :updated"
        expression_values = {
            ':s3_key': s3_key,
            ':indexed': True,
            ':tx_count': transaction_count,
            ':sub_count': subaward_count,
            ':complete': True,
            ':updated': datetime.now(timezone.utc).isoformat()
        }
        awards_table.update_item(
            Key={'award_id': award_id},
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_values
        )
        
        # Add delay if specified (for rate limiting large batches)
        if delay > 0:
            time.sleep(delay)
        
        return {'success': True, 'award_id': award_id, 'transaction_count': transaction_count, 'subaward_count': subaward_count}
    
    except Exception as e:
        log_print(f"❌ Error indexing award {award_record.get('award_id', 'unknown')}: {str(e)}")
        logger.error(f"❌ Error indexing award: {str(e)}", exc_info=True)
        raise




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
    
    # Get both CFO agencies and other agencies
    agencies = []
    cfo_agencies = response.get("agencies", {}).get("cfo_agencies", [])
    other_agencies = response.get("agencies", {}).get("other_agencies", [])
    
    agencies.extend(cfo_agencies)
    agencies.extend(other_agencies)
    
    log_print(f"✅ Found {len(agencies)} agencies")
    return agencies


def initiate_bulk_download(start_date: str, end_date: str, agency: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Initiate bulk download for contracts in date range, optionally filtered by agency
    
    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        agency: Optional agency dict with 'name', 'toptier_agency_id', 'toptier_code'
    """
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
        "prime_award_types": ["A", "B", "C", "D"]  # Contract types
    }
    
    # Add agency filter if provided
    if agency:
        bulk_filters["agencies"] = [
            {
                "type": "awarding",
                "tier": "toptier",
                "name": agency.get('name')
            }
        ]
    
    # Log the exact request payload for debugging
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
    log_print(f"⏳ {agency_prefix}Polling download status for {file_name} (max wait: {max_wait // 60} minutes, poll interval: {poll_interval}s)")
    start_time = time.time()
    attempt = 0
    consecutive_errors = 0
    max_consecutive_errors = 5
    
    while time.time() - start_time < max_wait:
        attempt += 1
        elapsed_minutes = int((time.time() - start_time) / 60)
        
        try:
            # Status endpoint uses GET with query parameters, not POST
            response = call_usaspending_api(
                "/api/v2/bulk_download/status/",
                method='GET',
                params={"file_name": file_name}
            )
            
            # Reset error counter on success
            consecutive_errors = 0
            
            status = response.get("status")
            message = response.get("message", "")
            seconds_elapsed_raw = response.get("seconds_elapsed")
            
            # Convert seconds_elapsed to float if it's a string or number
            seconds_elapsed = None
            if seconds_elapsed_raw is not None:
                try:
                    seconds_elapsed = float(seconds_elapsed_raw)
                except (ValueError, TypeError):
                    seconds_elapsed = None
            
            # Enhanced logging to diagnose issues
            if attempt % 5 == 0 or status not in ["running", "ready", "finished", "failed"]:
                file_url_check = response.get("file_url", "N/A")
                log_print(f"📊 {agency_prefix}Status check {attempt}: status='{status}', message='{message[:100] if message else 'N/A'}', seconds_elapsed={seconds_elapsed_raw}, elapsed_minutes={elapsed_minutes}, file_url={'present' if file_url_check != 'N/A' else 'missing'}")
                if status not in ["running", "ready", "finished", "failed"]:
                    log_print(f"⚠️ {agency_prefix}Unexpected status '{status}'. Full response: {json.dumps(response, default=str)[:500]}")
            
            # Check for both "ready" and "finished" status (USAspending uses both)
            if status == "ready" or status == "finished":
                file_url = response.get("file_url")
                if not file_url:
                    log_print(f"⚠️ {agency_prefix}Status is '{status}' but no file_url in response. Continuing to poll...")
                    time.sleep(poll_interval)
                    continue
                
                # File is ready - log success (wrap in try-except so logging errors don't prevent return)
                try:
                    log_print("=" * 80)
                    log_print(f"✅ {agency_prefix}CHECKPOINT: Bulk Download Completed - File Ready")
                    if seconds_elapsed is not None:
                        minutes = int(seconds_elapsed // 60)
                        secs = int(seconds_elapsed % 60)
                        log_print(f"⏱️ {agency_prefix}Total time: {int(seconds_elapsed)} seconds ({minutes}m {secs}s)")
                    log_print(f"📁 {agency_prefix}File URL: {file_url}")
                    log_print("=" * 80)
                except Exception as log_error:
                    # Log error but don't fail - file is ready, we should return
                    log_print(f"⚠️ {agency_prefix}Error in logging (non-critical): {str(log_error)[:200]}")
                    log_print(f"✅ {agency_prefix}File is ready, proceeding with download...")
                
                # Add a longer delay to ensure file is fully available on CDN
                # CDN propagation can take 30-60 seconds even after status says "ready"
                log_print(f"⏳ {agency_prefix}Waiting 30 seconds for file to be fully available on CDN...")
                time.sleep(30)
                return response
            elif status == "failed":
                raise Exception(f"{agency_prefix}Bulk download failed: {message or 'Unknown error'}")
            elif status == "running":
                # Log every 5th attempt for better visibility (was 10th)
                if attempt % 5 == 0:
                    log_print(f"⏳ {agency_prefix}Status check {attempt}: Still processing... (elapsed: {elapsed_minutes} min, API reports: {seconds_elapsed}s elapsed)")
            else:
                # Unknown status - log but continue polling
                log_print(f"⚠️ {agency_prefix}Unknown status '{status}' - continuing to poll... (response: {json.dumps(response, default=str)[:200]})")
            
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout,
                urllib3.exceptions.ProtocolError, urllib3.exceptions.NewConnectionError) as e:
            consecutive_errors += 1
            if consecutive_errors >= max_consecutive_errors:
                log_print(f"❌ {agency_prefix}Too many consecutive connection errors ({consecutive_errors}). Aborting.")
                raise
            
            # Exponential backoff for connection errors during polling
            error_backoff = min(poll_interval * (2 ** (consecutive_errors - 1)), 300)  # Max 5 minutes
            log_print(f"⚠️ {agency_prefix}Connection error during status check (attempt {attempt}, consecutive errors: {consecutive_errors}): {str(e)[:100]}")
            log_print(f"⏳ {agency_prefix}Waiting {error_backoff}s before next status check...")
            time.sleep(error_backoff)
            continue  # Skip the normal sleep and retry immediately after backoff
        
        except Exception as e:
            # Catch any other unexpected errors and log them
            log_print(f"⚠️ {agency_prefix}Unexpected error during status check (attempt {attempt}): {str(e)[:200]}")
            log_print(f"⏳ Continuing to poll...")
            time.sleep(poll_interval)
            continue
        
        # Normal sleep between successful status checks
        time.sleep(poll_interval)
    
    raise Exception(f"Bulk download timeout after {max_wait} seconds ({max_wait // 60} minutes)")


def format_agency_name_for_s3(agency_name: str) -> str:
    """Convert agency name to S3 filename format (e.g., 'Department of Agriculture' -> 'DOAgriculture')"""
    # Handle "Department of X" format
    if agency_name.startswith("Department of "):
        # "Department of Agriculture" -> "DO" + "Agriculture"
        rest = agency_name.replace("Department of ", "").strip()
        return "DO" + rest.capitalize()
    
    # Handle "Department X" format
    if agency_name.startswith("Department "):
        # "Department Defense" -> "DDefense"
        rest = agency_name.replace("Department ", "").strip()
        return "D" + rest.capitalize()
    
    # For other formats, take first letter of each word
    words = agency_name.split()
    if not words:
        return "Unknown"
    
    if len(words) == 1:
        return words[0].capitalize()
    
    # Multiple words: first letters + rest of last word
    first_letters = "".join([word[0].upper() for word in words if word])
    if len(words[-1]) > 1:
        return first_letters + words[-1][1:].capitalize()
    return first_letters


def format_date_range_path(start_date: str, end_date: str) -> str:
    """Format date range as MMDDYYYY-MMDDYYYY for S3 path (no leading zeros)"""
    from datetime import datetime
    
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    
    # Format as MMDDYYYY without leading zeros (e.g., 1152025 for 1/15/2025)
    start_formatted = f"{start_dt.month}{start_dt.day}{start_dt.year}"
    end_formatted = f"{end_dt.month}{end_dt.day}{end_dt.year}"
    
    return f"{start_formatted}-{end_formatted}"


def save_zip_to_s3(zip_content: bytes, start_date: str, end_date: str, agency_name: str) -> str:
    """Save downloaded ZIP file to S3 with naming convention: {daterange}/{AgencyName}.zip"""
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


def parse_csv_to_award_records(csv_content: str) -> Dict[str, Dict[str, Any]]:
    """
    Parse CSV content and group transactions by award to create award-level records.
    Returns a dict mapping award_id -> award_record with aggregated data.
    This avoids API calls by using data directly from the bulk download CSV.
    """
    reader = csv.DictReader(StringIO(csv_content))
    awards = {}  # award_id -> award data
    
    for row in reader:
        # Get award ID - primary key for grouping
        award_id = (
            row.get('contract_award_unique_key') or
            row.get('generated_unique_award_id') or
            row.get('award_id') or
            None
        )
        
        if not award_id:
            continue
        
        # Initialize award record if first time seeing this award
        if award_id not in awards:
            # Extract award-level fields (use first transaction's values)
            period_start = row.get('period_of_performance_start_date')
            period_end = row.get('period_of_performance_current_end_date')
            fiscal_year = None
            if period_start:
                try:
                    date_obj = datetime.strptime(period_start.split('T')[0], '%Y-%m-%d')
                    fiscal_year = date_obj.year + 1 if date_obj.month >= 10 else date_obj.year
                except:
                    pass
            
            awards[award_id] = {
                'award_id': award_id,
                'award_type': (row.get('award_type') or 'contract').lower(),
                'period_start_date': period_start,
                'period_end_date': period_end,
                'fiscal_year': fiscal_year,
                'description': row.get('transaction_description') or row.get('prime_award_base_transaction_description') or '',
                'awarding_agency_code': row.get('awarding_agency_code'),
                'awarding_agency_name': row.get('awarding_agency_name'),
                'funding_agency_code': row.get('funding_agency_code'),
                'funding_agency_name': row.get('funding_agency_name'),
                'recipient_name': row.get('recipient_name'),
                'recipient_name_normalized': (row.get('recipient_name') or '').lower().strip(),
                'recipient_unique_id': row.get('recipient_uei') or row.get('recipient_duns'),
                'recipient_location_country': row.get('recipient_country_code'),
                'recipient_location_state': row.get('recipient_state_code'),
                'naics_code': row.get('naics_code'),
                'naics_description': row.get('naics_description'),
                'psc_code': row.get('product_or_service_code'),
                'psc_description': row.get('product_or_service_code_description'),
                # Note: cfda_number not included for contracts (sparse GSI - only include when present)
                'total_obligation': Decimal('0'),
                'transaction_count': 0,
                'transactions': [],  # Store all transactions for this award
                'indexed_at': datetime.now(timezone.utc).isoformat(),
                'last_updated': datetime.now(timezone.utc).isoformat(),
                'data_source': 'usaspending_bulk_download',
                'api_version': 'bulk_csv',
                'award_details_indexed': False,
                'full_indexing_complete': False,
                'ttl': int((datetime.now(timezone.utc).timestamp() + (90 * 24 * 60 * 60)))
            }
        
        # Aggregate transaction data
        award = awards[award_id]
        award['transaction_count'] += 1
        
        # Sum up obligations (use total_dollars_obligated if available, else federal_action_obligation)
        obligation_str = row.get('total_dollars_obligated') or row.get('federal_action_obligation') or '0'
        try:
            obligation = Decimal(str(obligation_str))
            award['total_obligation'] += obligation
        except:
            pass
        
        # Store transaction record (for S3 upload later)
        transaction_record = {
            'transaction_id': row.get('contract_transaction_unique_key'),
            'action_date': row.get('action_date'),
            'federal_action_obligation': row.get('federal_action_obligation'),
            'transaction_description': row.get('transaction_description'),
            'action_type': row.get('action_type'),
            'modification_number': row.get('modification_number'),
            'transaction_number': row.get('transaction_number')
        }
        award['transactions'].append(transaction_record)
    
    return awards


def download_and_parse_csv(file_url: str, agency_name: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None, return_award_records: bool = False) -> Any:
    """Download CSV/ZIP file and parse to extract award IDs"""
    agency_prefix = f"[{agency_name}] " if agency_name else ""
    parse_start_time = time.time()
    
    log_print(f"📥 {agency_prefix}Downloading file from {file_url}")
    
    # Add a delay to ensure file is fully available after status says "ready"
    # Files are uploaded to CDN which may take time to propagate
    # Increased wait time based on test script that works
    log_print(f"⏳ {agency_prefix}Waiting 30 seconds for file to be fully available on CDN...")
    time.sleep(30)
    
    # Use session with SSL verification disabled for Glue environment
    session = create_session()
    
    # Retry logic for 403 errors (file might not be immediately available on CDN)
    # Increased retries and delays based on test script
    max_download_retries = 10  # Increased from 5
    download_retry_delay = 30  # Increased from 15 - CDN propagation can take longer
    
    for attempt in range(max_download_retries):
        try:
            response = session.get(file_url, timeout=300, stream=True)
            
            # Check for 403 - might need to wait longer for CDN propagation
            if response.status_code == 403:
                if attempt < max_download_retries - 1:
                    # Exponential backoff: 30s, 60s, 90s, 120s, etc. (capped at 5 minutes)
                    wait_time = min(download_retry_delay * (attempt + 1), 300)
                    log_print(f"⚠️ Got 403 Forbidden. Waiting {wait_time}s before retry {attempt + 1}/{max_download_retries}...")
                    log_print(f"   (CDN propagation may take several minutes after status shows 'ready')")
                    time.sleep(wait_time)
                    continue
                else:
                    response.raise_for_status()
            else:
                response.raise_for_status()
            
            # Check if it's a ZIP file
            content_type = response.headers.get('Content-Type', '').lower()
            is_zip = file_url.lower().endswith('.zip') or 'zip' in content_type or 'application/zip' in content_type
            
            download_size_mb = len(response.content) / (1024 * 1024)
            log_print(f"✅ {agency_prefix}Download complete: {download_size_mb:.2f} MB")
            
            if is_zip:
                log_print(f"📦 {agency_prefix}Detected ZIP file, extracting...")
                # Download as binary for ZIP
                zip_content_bytes = response.content
                zip_content = BytesIO(zip_content_bytes)
                
                # Save ZIP file to S3 if agency name and dates are provided
                if agency_name and start_date and end_date:
                    try:
                        s3_key = save_zip_to_s3(zip_content_bytes, start_date, end_date, agency_name)
                    except Exception as e:
                        log_print(f"⚠️ {agency_prefix}Failed to save ZIP to S3 (non-critical): {str(e)[:200]}")
                        # Continue processing even if S3 save fails
                
                with zipfile.ZipFile(zip_content, 'r') as zip_ref:
                    # Find CSV file in the ZIP
                    csv_file = None
                    zip_files = zip_ref.namelist()
                    log_print(f"📋 {agency_prefix}ZIP contains {len(zip_files)} file(s)")
                    for file_name in zip_files:
                        if file_name.lower().endswith('.csv'):
                            csv_file = file_name
                            break
                    
                    if not csv_file:
                        raise Exception("No CSV file found in ZIP archive")
                    
                    log_print(f"📄 {agency_prefix}Found CSV file in ZIP: {csv_file}")
                    csv_content = zip_ref.read(csv_file).decode('utf-8')
            else:
                # Regular CSV file
                log_print(f"📄 {agency_prefix}Processing CSV file directly")
                csv_content = response.text
            
            csv_size_mb = len(csv_content.encode('utf-8')) / (1024 * 1024)
            log_print(f"📊 {agency_prefix}CSV size: {csv_size_mb:.2f} MB")
            
            if return_award_records:
                log_print(f"🔄 {agency_prefix}Parsing CSV to extract award records (no API calls needed)...")
                awards = parse_csv_to_award_records(csv_content)
                parse_duration = time.time() - parse_start_time
                log_print(f"✅ {agency_prefix}CSV Parsing Completed:")
                log_print(f"   📊 CSV Size: {len(csv_content):,} bytes")
                log_print(f"   🆔 Unique Awards Found in CSV: {len(awards):,}")
                log_print(f"   ⏱️ Parse Time: {int(parse_duration // 60)}m {int(parse_duration % 60)}s")
                return awards
            else:
                log_print(f"🔄 {agency_prefix}Parsing CSV to extract award IDs...")
                # Parse CSV
                reader = csv.DictReader(StringIO(csv_content))
                row_count = 0
                award_ids = []
                
                for row in reader:
                    row_count += 1
                    # Extract award ID from CSV row
                    # Bulk download CSV uses 'contract_award_unique_key' which contains the full award ID
                    # Format: CONT_AWD_<PIID>_<agency>_<parent_id>_<parent_agency>
                    # Also check for other possible column names as fallback
                    award_id = (
                        row.get('contract_award_unique_key') or  # Primary: bulk download format
                        row.get('generated_unique_award_id') or  # Alternative format
                        row.get('award_id') or                   # Simple format
                        row.get('Award ID')                      # Header format
                    )
                    if award_id and award_id not in award_ids:
                        award_ids.append(award_id)
                    
                    # Log progress for large files
                    if row_count % 100000 == 0:
                        log_print(f"  📊 {agency_prefix}Parsed {row_count:,} rows, found {len(award_ids):,} unique award IDs so far...")
                
                parse_duration = time.time() - parse_start_time
                log_print(f"✅ {agency_prefix}CSV Parsing Completed:")
                log_print(f"   📊 Total Rows: {row_count:,}")
                log_print(f"   🆔 Unique Award IDs: {len(award_ids):,}")
                log_print(f"   ⏱️ Parse Time: {int(parse_duration // 60)}m {int(parse_duration % 60)}s")
                return award_ids
            
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 403 and attempt < max_download_retries - 1:
                # Exponential backoff: 30s, 60s, 90s, 120s, etc. (capped at 5 minutes)
                wait_time = min(download_retry_delay * (attempt + 1), 300)
                log_print(f"⚠️ HTTP 403 error: {str(e)[:200]}. Waiting {wait_time}s before retry {attempt + 1}/{max_download_retries}...")
                log_print(f"   (CDN propagation may take several minutes after status shows 'ready')")
                time.sleep(wait_time)
                continue
            else:
                log_print(f"❌ HTTP error downloading file: {str(e)[:200]}")
                raise
        except Exception as e:
            log_print(f"❌ Error downloading/parsing file: {str(e)[:200]}")
            raise
    
    raise Exception(f"Failed to download file after {max_download_retries} retries")


# ============================================================================
# Main Job Logic
# ============================================================================

def main():
    """Main Glue job execution"""
    try:
        # Get date range from parameters or default to yesterday
        # getResolvedOptions returns keys without -- prefix, but check both formats for safety
        start_date = args.get('START_DATE') or args.get('--START_DATE')
        end_date = args.get('END_DATE') or args.get('--END_DATE')
        
        # Debug: Log all args keys to help diagnose parsing issues
        if not start_date or not end_date:
            log_print(f"🔍 Debug: Available args keys: {list(args.keys())}")
            log_print(f"🔍 Debug: START_DATE value: {args.get('START_DATE')} or {args.get('--START_DATE')}")
            log_print(f"🔍 Debug: END_DATE value: {args.get('END_DATE')} or {args.get('--END_DATE')}")
        
        if not start_date:
            # Default to yesterday if not provided
            yesterday = datetime.now(timezone.utc) - timedelta(days=1)
            start_date = yesterday.strftime('%Y-%m-%d')
            log_print(f"ℹ️ No START_DATE provided, defaulting to yesterday: {start_date}")
        else:
            # Validate date format
            try:
                datetime.strptime(start_date, '%Y-%m-%d')
            except ValueError:
                raise ValueError(f"Invalid START_DATE format: {start_date}. Expected YYYY-MM-DD")
        
        if not end_date:
            # Default to start_date if not provided
            end_date = start_date
            log_print(f"ℹ️ No END_DATE provided, defaulting to START_DATE: {end_date}")
        else:
            # Validate date format
            try:
                datetime.strptime(end_date, '%Y-%m-%d')
            except ValueError:
                raise ValueError(f"Invalid END_DATE format: {end_date}. Expected YYYY-MM-DD")
        
        # Validate date range
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        if end_dt < start_dt:
            raise ValueError(f"END_DATE ({end_date}) must be >= START_DATE ({start_date})")
        
        # Calculate number of days
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
        
        # Process each agency completely (download, parse, index) before moving to next
        job_start_time = time.time()
        log_print("=" * 80)
        log_print(f"📥 Step 2: Processing Agencies Sequentially")
        log_print(f"   Each agency will be fully downloaded, parsed, and indexed before the next begins")
        log_print(f"   Total Agencies: {len(agencies)}")
        log_print("=" * 80)
        
        total_indexed = 0
        successful_agencies = 0
        
        for i, agency in enumerate(agencies, 1):
            agency_name = agency.get('name', 'Unknown')
            agency_start_time = time.time()
            log_print(f"\n{'=' * 80}")
            log_print(f"📦 Processing Agency {i}/{len(agencies)}: {agency_name}")
            log_print(f"📅 Date Range: {start_date} to {end_date}")
            log_print(f"{'=' * 80}")
            
            # ========================================================================
            # PHASE 1: GET - Request and wait for bulk download
            # ========================================================================
            log_print(f"\n🔵 PHASE 1: GET - Requesting Bulk Download for {agency_name}")
            log_print(f"{'─' * 80}")
            get_phase_start = time.time()
            
            download_info = initiate_bulk_download(start_date, end_date, agency=agency)
            file_name = download_info['file_name']
            log_print(f"📋 File Name: {file_name}")
            
            # Poll for download completion
            status_info = poll_download_status(file_name, max_wait=14400, poll_interval=30, agency_name=agency_name)  # 4 hours per agency
            file_url = status_info.get('file_url')
            
            if not file_url:
                raise Exception(f"No file_url in download status response for {agency_name}")
            
            get_phase_duration = time.time() - get_phase_start
            log_print(f"✅ GET Phase Complete: {int(get_phase_duration // 60)}m {int(get_phase_duration % 60)}s")
            log_print(f"📁 File URL: {file_url}")
            
            # ========================================================================
            # PHASE 2: PARSE - Download and extract award records from CSV
            # ========================================================================
            log_print(f"\n🟡 PHASE 2: PARSE - Downloading and Parsing CSV for {agency_name}")
            log_print(f"{'─' * 80}")
            parse_phase_start = time.time()
            
            # Use CSV-based parsing to get full award records (no API calls needed!)
            agency_awards = download_and_parse_csv(file_url, agency_name=agency_name, start_date=start_date, end_date=end_date, return_award_records=True)
            
            if not agency_awards:
                parse_phase_duration = time.time() - parse_phase_start
                agency_duration = time.time() - agency_start_time
                log_print(f"ℹ️ {agency_name}: No awards found for date range (this is OK)")
                log_print(f"⏱️ Parse Phase: {int(parse_phase_duration // 60)}m {int(parse_phase_duration % 60)}s")
                log_print(f"⏱️ Total Agency Time: {int(agency_duration // 60)}m {int(agency_duration % 60)}s")
                successful_agencies += 1
                # Delay before next agency to give API time to rest
                if i < len(agencies):
                    delay_seconds = 60  # 1 minute between departments
                    log_print(f"⏳ Waiting {delay_seconds}s (1 minute) before next agency...")
                    time.sleep(delay_seconds)
                continue
            
            parse_phase_duration = time.time() - parse_phase_start
            log_print(f"✅ PARSE Phase Complete: {int(parse_phase_duration // 60)}m {int(parse_phase_duration % 60)}s")
            log_print(f"📊 Extracted {len(agency_awards)} unique award records from CSV")
            
            # ========================================================================
            # PHASE 3: STORE - Index awards (metadata + transactions + subawards) in parallel
            # ========================================================================
            log_print(f"\n🟢 PHASE 3: STORE - Indexing Awards for {agency_name}")
            log_print(f"{'─' * 80}")
            store_phase_start = time.time()
            
            # Convert awards dict to list for processing
            award_list = list(agency_awards.values())
            
            log_print(f"\n📦 Indexing {len(award_list)} awards in parallel (metadata + transactions + subawards)")
            log_print(f"⚙️ Rate limiting handled by API retry logic")
            
            max_workers = min(20, len(award_list))
            
            # Set delay for large batches (>5000 awards) to help with rate limiting
            thread_delay = 3.0 if len(award_list) > 5000 else 0.0
            if thread_delay > 0:
                log_print(f"⏸️ Large batch detected ({len(award_list)} awards): Adding {thread_delay}s delay per thread")
            
            log_print(f"⚙️ Parallel Processing: {max_workers} workers")
            
            agency_indexed = 0
            agency_errors = []
            total_transactions = 0
            total_subawards = 0
            
            # Reset progress counter
            with _progress_lock:
                _progress_counter['processed'] = 0
                _progress_counter['indexed'] = 0
                _progress_counter['total'] = len(award_list)
            
            # Process all awards in parallel - each thread handles one award at a time
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all tasks - each does: metadata + subawards + S3 upload
                # Pass delay parameter for large batches
                future_to_award = {
                    executor.submit(index_award_complete, award_record, thread_delay): award_record['award_id']
                    for award_record in award_list
                }
                
                # Process completed tasks
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
                            
                            # Progress logging every 50 awards or at end
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
                        # Continue processing other awards instead of stopping
            
            store_phase_duration = time.time() - store_phase_start
            agency_duration = time.time() - agency_start_time
            
            # Log any errors that occurred
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
            successful_agencies += 1
            
            # Delay before next agency to give API time to rest
            if i < len(agencies):
                delay_seconds = 60  # 1 minute between departments
                log_print(f"\n⏳ Waiting {delay_seconds}s (1 minute) before next agency...")
                time.sleep(delay_seconds)
        
        total_job_duration = time.time() - job_start_time
        log_print("\n" + "=" * 80)
        log_print(f"📊 FINAL SUMMARY - All Agencies Processed")
        log_print("=" * 80)
        log_print(f"  ✅ Agencies Processed: {successful_agencies}/{len(agencies)}")
        log_print(f"  📦 Total Awards Indexed: {total_indexed:,}")
        if successful_agencies > 0:
            avg_per_agency = total_indexed / successful_agencies
            log_print(f"  📈 Average Awards per Agency: {avg_per_agency:.1f}")
        log_print(f"  ⏱️ Total Job Duration: {int(total_job_duration // 3600)}h {int((total_job_duration % 3600) // 60)}m {int(total_job_duration % 60)}s")
        log_print("=" * 80)
        
        # Job success
        log_print("✅ Job completed successfully - committing")
        job.commit()
        
    except Exception as e:
        log_print(f"❌ CRITICAL ERROR in bulk indexing job: {str(e)}")
        logger.error(f"❌ CRITICAL ERROR in bulk indexing job: {str(e)}", exc_info=True)
        raise  # Re-raise to mark job as failed


if __name__ == "__main__":
    main()

