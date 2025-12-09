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
    """Convert agency name to S3 filename format"""
    if agency_name.startswith("Department of "):
        rest = agency_name.replace("Department of ", "").strip()
        return "DO" + rest.capitalize()
    
    if agency_name.startswith("Department "):
        rest = agency_name.replace("Department ", "").strip()
        return "D" + rest.capitalize()
    
    words = agency_name.split()
    if not words:
        return "Unknown"
    
    if len(words) == 1:
        return words[0].capitalize()
    
    first_letters = "".join([word[0].upper() for word in words if word])
    if len(words[-1]) > 1:
        return first_letters + words[-1][1:].capitalize()
    return first_letters

def format_date_range_path(start_date: str, end_date: str) -> str:
    """Format date range as MMDDYYYY-MMDDYYYY for S3 path"""
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    
    start_formatted = f"{start_dt.month}{start_dt.day}{start_dt.year}"
    end_formatted = f"{end_dt.month}{end_dt.day}{end_dt.year}"
    
    return f"{start_formatted}-{end_formatted}"

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

def parse_prime_award_csv(csv_content: str, csv_filename: str) -> Dict[str, Dict[str, Any]]:
    """
    Parse prime award CSV file and return award records with all columns preserved.
    Groups transactions by award_id.
    
    Args:
        csv_content: CSV content as string
        csv_filename: Name of the CSV file (for logging)
    
    Returns:
        Dict mapping award_id -> award_record with all CSV columns
    """
    awards = {}
    lines = csv_content.split('\n')
    
    if len(lines) < 2:
        return awards
    
    reader = csv.DictReader(StringIO(csv_content))
    
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
                if key in ['federal_action_obligation', 'total_dollars_obligated', 
                          'total_outlayed_amount_for_overall_award', 'base_and_exercised_options_value',
                          'current_total_value_of_award', 'base_and_all_options_value',
                          'potential_total_value_of_award', 'action_date_fiscal_year']:
                    try:
                        award_record[key] = Decimal(str(value))
                    except:
                        award_record[key] = normalize_string(value)
                else:
                    award_record[key] = normalize_string(value)
            
            # Add computed fields
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
            if key in ['federal_action_obligation', 'total_dollars_obligated']:
                try:
                    transaction_record[key] = Decimal(str(value))
                except:
                    transaction_record[key] = normalize_string(value)
            else:
                transaction_record[key] = normalize_string(value)
        
        award['transactions'].append(transaction_record)
        
        # Update total obligation (sum from transactions)
        obligation_str = row.get('total_dollars_obligated') or row.get('federal_action_obligation') or '0'
        try:
            obligation = Decimal(str(obligation_str))
            if 'total_obligation' not in award or award.get('total_obligation') is None:
                award['total_obligation'] = Decimal('0')
            award['total_obligation'] += obligation
        except:
            pass
    
    log_print(f"✅ Parsed {csv_filename}: {len(awards)} unique awards, {sum(a['transaction_count'] for a in awards.values())} total transactions")
    return awards

def parse_subaward_csv(csv_content: str, csv_filename: str) -> Dict[str, List[Dict[str, Any]]]:
    """
    Parse sub-award CSV file and return sub-awards grouped by parent award ID.
    
    Args:
        csv_content: CSV content as string
        csv_filename: Name of the CSV file (for logging)
    
    Returns:
        Dict mapping parent_award_id -> list of sub-award records
    """
    subawards_by_parent = {}
    lines = csv_content.split('\n')
    
    if len(lines) < 2:
        return subawards_by_parent
    
    reader = csv.DictReader(StringIO(csv_content))
    
    for row in reader:
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
        
        # Group sub-awards by parent award
        if parent_award_id not in subawards_by_parent:
            subawards_by_parent[parent_award_id] = []
        
        subawards_by_parent[parent_award_id].append(subaward_record)
    
    total_subawards = sum(len(subs) for subs in subawards_by_parent.values())
    log_print(f"✅ Parsed {csv_filename}: {len(subawards_by_parent)} parent awards, {total_subawards} total sub-awards")
    return subawards_by_parent

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
        log_print(f"📦 {agency_prefix}Extracting CSV files to S3...")
        for csv_file in csv_files:
            try:
                csv_content = zip_ref.read(csv_file)
                csv_s3_key = f"{date_range_path}/{agency_filename}/{csv_file}"
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
    
    # Parse all prime award files in parallel (read from S3)
    all_prime_awards = {}
    parse_workers = len(prime_file_list) * 5
    
    if len(prime_file_list) > 0:
        log_print(f"📊 {agency_prefix}Parsing {len(prime_file_list)} prime award file(s) with {parse_workers} workers (5x files)...")
        log_print(f"   Reading CSV files from S3 (extracted individually)")
        
        def parse_prime_file_from_s3(file_name):
            try:
                # Read CSV directly from S3
                csv_s3_key = csv_s3_keys.get(file_name)
                if csv_s3_key:
                    csv_obj = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=csv_s3_key)
                    csv_content = csv_obj['Body'].read().decode('utf-8')
                    return parse_prime_award_csv(csv_content, file_name)
                else:
                    raise Exception(f"S3 key not found for {file_name}")
            except Exception as e:
                log_print(f"❌ {agency_prefix}Error parsing {file_name}: {str(e)[:200]}")
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
                # Read CSV directly from S3
                csv_s3_key = csv_s3_keys.get(file_name)
                if csv_s3_key:
                    csv_obj = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=csv_s3_key)
                    csv_content = csv_obj['Body'].read().decode('utf-8')
                    return parse_subaward_csv(csv_content, file_name)
                else:
                    raise Exception(f"S3 key not found for {file_name}")
            except Exception as e:
                log_print(f"❌ {agency_prefix}Error parsing {file_name}: {str(e)[:200]}")
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
                    # Note: We'll update it during indexing, but for now just log
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
                log_print(f"⚠️ {agency_prefix}Error checking DynamoDB for parent {parent_id}: {str(e)[:200]}")
        
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
        # GSI range keys: fiscal_year, total_obligation, period_start_date, period_end_date
        
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
            db_item['recipient_name_normalized'] = recipient_name.lower().strip()
        
        # Ensure total_obligation is Decimal
        if 'total_obligation' in db_item and not isinstance(db_item['total_obligation'], Decimal):
            try:
                db_item['total_obligation'] = Decimal(str(db_item['total_obligation']))
            except:
                db_item['total_obligation'] = Decimal('0')
        
        # Get transactions and subawards (already in award_record from CSV parsing)
        transactions = award_record.get('transactions', [])
        subawards = award_record.get('subawards', [])
        transaction_count = len(transactions)
        subaward_count = len(subawards)
        
        # Check if this is an update to an existing DynamoDB item
        update_from_dynamodb = award_record.get('update_from_dynamodb', False)
        existing_item = award_record.get('existing_item')
        
        if update_from_dynamodb and existing_item:
            # Update existing item with subawards only (preserve all other fields)
            db_item = existing_item.copy()
            
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
                else:
                    db_item['subawards'] = existing_subawards
            elif subawards:
                db_item['subawards'] = convert_floats_to_decimal(subawards)
            
            # Update counts and timestamp
            db_item['subaward_count'] = len(db_item.get('subawards', []))
            db_item['last_updated'] = datetime.now(timezone.utc).isoformat()
            
            # Ensure all fields are properly converted
            db_item = convert_floats_to_decimal(db_item)
            
            # Update item in DynamoDB
            awards_table.put_item(Item=db_item)
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
            
            # Store to DynamoDB
            awards_table.put_item(Item=db_item)
        
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
        log_print(f"❌ CRITICAL ERROR in bulk indexing job: {str(e)}")
        logger.error(f"❌ CRITICAL ERROR in bulk indexing job: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    main()
