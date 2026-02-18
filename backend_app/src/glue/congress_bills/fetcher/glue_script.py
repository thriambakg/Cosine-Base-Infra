"""
AWS Glue Job: Congress.gov Bill Data Fetcher
Fetches comprehensive bill data from Congress.gov API and stores in DynamoDB.

This job is designed for date ranges > 2 days (2 day timeout).
For shorter ranges (≤2 days), use the Lambda function instead.

Fetches for each bill:
- Full bill details
- All actions (history)
- All amendments with sponsors
- All cosponsors
- All summaries (for text search)
- Subjects and policy area

Stores one row per bill in DynamoDB with all related data.
"""


import sys
import json
import logging
import time
import requests
import gc
import csv
import re
import gzip
import zipfile
import threading
import defusedxml
import defusedxml.ElementTree as ET
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional, Tuple
from io import StringIO, BytesIO
from difflib import SequenceMatcher
from concurrent.futures import ThreadPoolExecutor, as_completed

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

import boto3

try:
    import roll_call_indexing
except ImportError:
    roll_call_indexing = None

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
    'REQUEST_TIMEOUT',
    'BILL_TEXT_SQS_URL'
])

# Get optional date parameters (parse manually to avoid errors if not provided)
# getResolvedOptions requires all arguments, so we parse manually for optional ones
optional_params = ['CONGRESS', 'START_DATE', 'END_DATE', 'SOURCE']
for param in optional_params:
    for i, arg in enumerate(sys.argv):
        if arg == f'--{param}' and i + 1 < len(sys.argv):
            args[param] = sys.argv[i + 1]
            break

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


def format_state_district(politician: Dict[str, Any]) -> Optional[str]:
    """
    Format state/district for a politician (same format as politician trades matchers)
    - Senators: Just state (e.g., "IL", "WA")
    - House reps: State + district (e.g., "IL02", "TX31")
    
    Args:
        politician: Politician dict with state, district, and position fields
        
    Returns:
        Formatted state/district string or None
    """
    state = (politician.get('state') or '').strip()
    if not state:
        return None
    
    position = (politician.get('position') or '').strip()
    district = (politician.get('district') or '').strip()
    
    # Senators don't have districts
    if position == 'Senate':
        return state
    
    # House reps have districts
    if position == 'House' and district:
        # Format district with zero-padding if needed (e.g., "2" -> "02", "31" -> "31")
        try:
            district_num = int(district)
            return f"{state}{district_num:02d}"
        except (ValueError, TypeError):
            # If district is not a number, just append it
            return f"{state}{district}"
    
    # Fallback: just return state if we can't format properly
    return state


def load_legislators_csv() -> List[Dict[str, Any]]:
    """
    Load congress-legislators CSV from S3 (bills data bucket root level)
    
    Returns:
        List of politician dicts with name, party, state, district, position, and alternativeNames
    """
    try:
        if not S3_BUCKET_NAME:
            log_print("⚠️ S3_BUCKET_NAME not set, skipping CSV load")
            return []
        
        # Download congress-legislators.csv from S3 (root level of bills data bucket)
        response = s3_client.get_object(
            Bucket=S3_BUCKET_NAME,
            Key='congress-legislators.csv'
        )
        
        csv_content = response['Body'].read().decode('utf-8')
        csv_reader = csv.DictReader(StringIO(csv_content))
        
        politicians = []
        for row in csv_reader:
            # Construct full name from components
            name_parts = []
            if row.get('first_name'):
                name_parts.append(row['first_name'])
            if row.get('middle_name'):
                name_parts.append(row['middle_name'])
            if row.get('last_name'):
                name_parts.append(row['last_name'])
            if row.get('suffix'):
                name_parts.append(row['suffix'])
            
            # Use constructed name or fall back to full_name
            if row.get('full_name') and row.get('full_name').strip():
                primary_name = row.get('full_name').strip()
            else:
                primary_name = ' '.join(name_parts) if name_parts else ''
            
            # Build alternative names
            alt_names = []
            if row.get('nickname'):
                alt_names.append(row['nickname'])
            if row.get('full_name') and row.get('full_name').strip() != primary_name:
                alt_names.append(row['full_name'].strip())
            
            # Determine position from type
            leg_type = row.get('type', '').lower().strip()
            if leg_type == 'sen':
                position = 'Senate'
            elif leg_type == 'rep':
                position = 'House'
            else:
                position = leg_type  # fallback
            
            # Get party (first character: D, R, I)
            party_full = row.get('party', '').strip()
            party = party_full[0].upper() if party_full else ''
            
            # Get state and district
            state = row.get('state', '').strip() if row.get('state') else ''
            district = row.get('district', '').strip() if row.get('district') else ''
            
            # Get first and last name from CSV columns
            first_name = row.get('first_name', '').strip() if row.get('first_name') else ''
            last_name = row.get('last_name', '').strip() if row.get('last_name') else ''
            
            politicians.append({
                'name': primary_name,
                'first_name': first_name,
                'last_name': last_name,
                'party': party,
                'party_full': party_full,
                'state': state,
                'district': district,
                'position': position,
                'alternativeNames': alt_names,
                'bioguide_id': row.get('bioguide_id', '').strip() if row.get('bioguide_id') else None
            })
        
        return politicians
        
    except Exception as e:
        log_print(f"❌ Error loading congress-legislators list: {e}")
        return []


def fuzzy_match_name(name: str, politician: Dict[str, Any]) -> float:
    """
    Fuzzy match a name to a politician using Levenshtein distance
    Handles various name formats including API format like "Rep. Begich, Nicholas J. [R-AK-At Large]"
    """
    # Normalize names (lowercase, strip)
    name_normalized = name.lower().strip()
    politician_normalized = politician['name'].lower().strip()
    
    # Remove position markers like "(Senator)", "(Representative)" from name
    name_clean = re.sub(r'\s*\([^)]*(?:senator|representative)[^)]*\)', '', name_normalized, flags=re.IGNORECASE)
    
    # Remove API format markers like "Rep. ", "Sen. ", and brackets like "[R-AK-At Large]"
    name_clean = re.sub(r'^(?:rep\.|sen\.|representative|senator)\s+', '', name_clean, flags=re.IGNORECASE)
    name_clean = re.sub(r'\s*\[[^\]]+\]', '', name_clean)  # Remove [R-AK-At Large] type brackets
    name_clean = name_clean.strip()
    
    # Check exact match first (after cleaning)
    if name_clean == politician_normalized:
        return 1.0
    
    # Check alternative names
    for alt_name in politician.get('alternativeNames', []):
        alt_normalized = alt_name.lower().strip()
        if name_clean == alt_normalized:
            return 1.0
    
    # Handle "Last, First" vs "First Last" format differences
    if ',' in name_clean:
        parts = [p.strip() for p in name_clean.split(',')]
        if len(parts) == 2:
            name_reversed = f"{parts[1]} {parts[0]}".strip()
            if name_reversed == politician_normalized:
                return 1.0
            reversed_similarity = SequenceMatcher(None, name_reversed, politician_normalized).ratio()
            if reversed_similarity > 0.9:
                return reversed_similarity
    
    # Calculate similarity using SequenceMatcher
    similarity = SequenceMatcher(None, name_clean, politician_normalized).ratio()
    
    # Also check if names are subsets (e.g., "John Doe" vs "John A. Doe")
    if name_clean in politician_normalized or politician_normalized in name_clean:
        similarity = max(similarity, 0.9)
    
    # If similarity is still low, try with reversed name format
    if similarity < 0.85 and ',' in name_clean:
        parts = [p.strip() for p in name_clean.split(',')]
        if len(parts) == 2:
            name_reversed = f"{parts[1]} {parts[0]}".strip()
            reversed_similarity = SequenceMatcher(None, name_reversed, politician_normalized).ratio()
            similarity = max(similarity, reversed_similarity)
    
    return similarity


def find_politician_by_bioguide_id(bioguide_id: str, politicians: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Find politician by bioguide_id (most reliable matching method).
    
    Args:
        bioguide_id: Bioguide ID to match
        politicians: List of politician dicts
    
    Returns:
        Matched politician dict, or None
    """
    if not bioguide_id or not politicians:
        return None
    
    bioguide_id_clean = str(bioguide_id).strip().upper()
    for politician in politicians:
        pol_bioguide = politician.get('bioguide_id')
        if pol_bioguide and str(pol_bioguide).strip().upper() == bioguide_id_clean:
            return {
                **politician,
                'matchScore': 1.0  # Perfect match
            }
    return None


def find_matching_politician(name: str, politicians: List[Dict[str, Any]], 
                            first_name: str = "", last_name: str = "", 
                            party: str = "", state: str = "") -> Optional[Dict[str, Any]]:
    """
    Find best matching politician using multiple criteria:
    1. Name matching (fuzzy)
    2. First name match
    3. Last name match
    4. Party match
    5. State match
    
    Args:
        name: Full name to match
        politicians: List of politician dicts
        first_name: First name from API (optional)
        last_name: Last name from API (optional)
        party: Party from API (optional, first character: R, D, I)
        state: State from API (optional)
    
    Returns:
        Matched politician dict with matchScore, or None
    """
    if not name or not politicians:
        return None
    
    best_match = None
    best_score = 0.0
    
    # Normalize input criteria
    first_name_norm = first_name.strip().lower() if first_name else ""
    last_name_norm = last_name.strip().lower() if last_name else ""
    party_norm = party.strip().upper()[0] if party and len(party.strip()) > 0 else ""
    state_norm = state.strip().upper() if state else ""
    
    for politician in politicians:
        score = 0.0
        match_count = 0
        
        # 1. Name matching (weighted heavily)
        name_score = fuzzy_match_name(name, politician)
        if name_score > 0.7:  # Only count if reasonably close
            score += name_score * 0.5  # 50% weight
            match_count += 1
        
        # 2. First name match (exact or fuzzy)
        if first_name_norm:
            pol_first = (politician.get('first_name') or '').strip().lower()
            if pol_first:
                if pol_first == first_name_norm:
                    score += 0.2  # Exact match
                    match_count += 1
                elif first_name_norm in pol_first or pol_first in first_name_norm:
                    score += 0.15  # Partial match
                    match_count += 1
        
        # 3. Last name match (exact or fuzzy)
        if last_name_norm:
            pol_last = (politician.get('last_name') or '').strip().lower()
            if pol_last:
                if pol_last == last_name_norm:
                    score += 0.2  # Exact match
                    match_count += 1
                elif last_name_norm in pol_last or pol_last in last_name_norm:
                    score += 0.15  # Partial match
                    match_count += 1
        
        # 4. Party match (exact)
        if party_norm:
            pol_party = (politician.get('party') or '').strip().upper()
            if pol_party and pol_party[0] == party_norm:
                score += 0.1  # Party match
                match_count += 1
        
        # 5. State match (exact)
        if state_norm:
            pol_state = (politician.get('state') or '').strip().upper()
            if pol_state == state_norm:
                score += 0.1  # State match
                match_count += 1
        
        # Bonus for multiple criteria matching
        if match_count >= 3:
            score += 0.1  # Bonus for multiple matches
        
        if score > best_score:
            best_score = score
            best_match = politician
    
    # Return match if above threshold (lower threshold since we have multiple criteria)
    threshold = 0.6 if (first_name_norm or last_name_norm or party_norm or state_norm) else NAME_MATCH_THRESHOLD
    if best_score >= threshold:
        return {
            **best_match,
            'matchScore': best_score
        }
    
    return None

log_print("=" * 80)
log_print("✅ Congress.gov Bill Fetcher Glue Job - Script Loaded Successfully")
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
BILL_TEXT_SQS_URL = args.get('BILL_TEXT_SQS_URL', '')

# Name matching threshold (0.0 to 1.0)
NAME_MATCH_THRESHOLD = 0.85  # 85% similarity

# Bill types to fetch
BILL_TYPES = ["HR", "S", "HJRES", "SJRES", "HCONRES", "SCONRES", "HRES", "SRES"]

# Bulk data repository base URL (public, no API key required)
BULK_DATA_BASE_URL = "https://www.govinfo.gov/bulkdata/BILLSTATUS"

# AWS clients
dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')
secrets_client = boto3.client('secretsmanager')
sqs_client = boto3.client('sqs')
bills_table = dynamodb.Table(BILLS_TABLE_NAME) if BILLS_TABLE_NAME else None

# ============================================================================
# Helper Functions (same as Lambda)
# ============================================================================

class ApiKeyRotator:
    """
    Thread-safe round-robin API key rotator.
    Distributes API calls equally across all available API keys.
    """
    def __init__(self, api_keys: List[str]):
        if not api_keys:
            raise ValueError("At least one API key is required")
        self.api_keys = api_keys
        self.lock = threading.Lock()
        self.current_index = 0
        log_print(f"✅ Initialized API key rotator with {len(api_keys)} key(s)")
    
    def get_key(self) -> str:
        """Get the next API key in round-robin fashion (thread-safe)"""
        with self.lock:
            key = self.api_keys[self.current_index]
            self.current_index = (self.current_index + 1) % len(self.api_keys)
            return key
    
    def get_key_count(self) -> int:
        """Get the number of available API keys"""
        return len(self.api_keys)


# Global API key rotator instance (initialized once)
_api_key_rotator: Optional[ApiKeyRotator] = None


def get_congress_api_keys() -> ApiKeyRotator:
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
        
        # Initialize the rotator
        _api_key_rotator = ApiKeyRotator(api_keys)
        log_print(f"✅ Retrieved {len(api_keys)} Congress API key(s) from Secrets Manager")
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

def calculate_congress_from_date(date: datetime) -> int:
    """
    Calculate congress number from a date.
    
    Formula: congress = ((year - 1789) // 2) + 1
    Each congress spans 2 years (odd-numbered years start new congress).
    Example: 118th Congress = 2023-2024, 119th Congress = 2025-2026
    
    Args:
        date: datetime object (with or without timezone)
        
    Returns:
        Congress number (integer)
    """
    year = date.year
    congress = ((year - 1789) // 2) + 1
    return congress


def get_congresses_from_date_range(start_date: datetime, end_date: datetime) -> List[int]:
    """
    Get all congress numbers that overlap with the date range.
    
    Args:
        start_date: Start date of the range
        end_date: End date of the range
        
    Returns:
        List of congress numbers (integers) that overlap with the date range
    """
    start_congress = calculate_congress_from_date(start_date)
    end_congress = calculate_congress_from_date(end_date)
    congresses = list(range(start_congress, end_congress + 1))
    return congresses


# ============================================================================
# API Call Functions - REMOVED (using bulk downloads instead)
# ============================================================================
# The following functions have been removed as we now use bulk downloads:
# - get_current_congress()
# - make_api_request()
# - fetch_bills_list()
# - fetch_bill_details()
# - fetch_bill_actions()
# - fetch_bill_amendments()
# - fetch_bill_cosponsors()
# - fetch_bill_summaries()
# - fetch_bill_subjects()
# - fetch_bill_titles()
# - fetch_bill_text_versions()
# - download_bill_text_file()
# - store_bill_text_to_s3()
# - build_comprehensive_bill_record()
# 
# API key management functions (ApiKeyRotator, get_congress_api_keys) are kept
# for use in other parts of the system.
# ============================================================================

# ============================================================================
# Bulk Download Functions
# ============================================================================

def format_date_range_path(start_date: str, end_date: str) -> str:
    """Format date range as YYYYMMDD-YYYYMMDD for S3 path"""
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    
    start_formatted = start_dt.strftime('%Y%m%d')
    end_formatted = end_dt.strftime('%Y%m%d')
    
    return f"{start_formatted}-{end_formatted}"


# All API call functions removed - using bulk downloads instead
# Removed functions:
# - make_api_request
# - fetch_bills_list
# - fetch_bill_details
# - fetch_bill_actions
# - fetch_bill_amendments
# - fetch_bill_cosponsors
# - fetch_bill_summaries
# - fetch_bill_subjects
# - fetch_bill_titles
# - fetch_bill_text_versions
# - download_bill_text_file
# - store_bill_text_to_s3
# - build_comprehensive_bill_record

def download_bulk_zip(congress: int, bill_type: str, max_retries: int = 5) -> Optional[bytes]:
    """
    Download ZIP file from bulk data repository for a specific Congress and bill type.
    No API key required - bulk data is public.
    
    Based on govinfo.gov bulk data structure:
    - ZIP files may not be available for current Congress (119)
    - Individual XML files are available at: https://www.govinfo.gov/bulkdata/BILLSTATUS/{congress}/{bill_type}/BILLSTATUS-{congress}{bill_type}{number}.xml
    - For completed Congresses, ZIP files may be available with pattern: BILLSTATUS-{congress}-{bill_type}.zip
    
    Args:
        congress: Congress number (e.g., 119)
        bill_type: Bill type (e.g., "hr", "s", "hjres", etc.)
        max_retries: Maximum number of retry attempts
        
    Returns:
        ZIP file content as bytes, or None if download fails
    """
    # Convert bill type to lowercase for URL (e.g., "HR" -> "hr")
    bill_type_lower = bill_type.lower()
    
    # Try ZIP file patterns based on govinfo.gov structure
    # Note: ZIP files may only be available for completed Congresses
    # Current Congress (119) may only have individual XML files
    zip_urls = [
        # Pattern 1: BILLSTATUS-{congress}-{bill_type}.zip (most common for completed Congresses)
        f"{BULK_DATA_BASE_URL}/{congress}/{bill_type_lower}/BILLSTATUS-{congress}-{bill_type_lower}.zip",
        # Pattern 2: {bill_type}.zip in subdirectory
        f"{BULK_DATA_BASE_URL}/{congress}/{bill_type_lower}/{bill_type_lower}.zip",
        # Pattern 3: BILLSTATUS-{congress}{bill_type}.zip (no hyphen)
        f"{BULK_DATA_BASE_URL}/{congress}/{bill_type_lower}/BILLSTATUS-{congress}{bill_type_lower}.zip",
        # Pattern 4: ZIP at bill_type level (without subdirectory)
        f"{BULK_DATA_BASE_URL}/{congress}/{bill_type_lower}.zip",
    ]
    
    log_print(f"📥 Downloading bulk ZIP for Congress {congress}, Bill Type {bill_type}...")
    
    for zip_url in zip_urls:
        for attempt in range(max_retries):
            try:
                log_print(f"   Attempting: {zip_url} (attempt {attempt + 1}/{max_retries})")
                response = requests.get(zip_url, timeout=(10, REQUEST_TIMEOUT * 2), stream=True)  # nosec B113 - timeout set
                
                if response.status_code == 200:
                    content = response.content
                    log_print(f"   ✅ Successfully downloaded {len(content):,} bytes from {zip_url}")
                    return content
                elif response.status_code == 404:
                    log_print(f"   ⚠️ ZIP not found at {zip_url}, trying next URL...")
                    break  # Try next URL
                else:
                    response.raise_for_status()
            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    wait_time = RETRY_DELAY * (attempt + 1)
                    log_print(f"   ⚠️ Download error: {str(e)[:100]}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
            else:
                    log_print(f"   ❌ Failed to download from {zip_url} after {max_retries} attempts")
    
    # ZIP files not available - this is expected for current Congress (119)
    # Individual XML files are available but would require directory listing or bill number iteration
    log_print(f"   ⚠️ ZIP file not available for Congress {congress}, Bill Type {bill_type}")
    log_print(f"   ℹ️  Note: ZIP files may only be available for completed Congresses")
    log_print(f"   ℹ️  Individual XML files are available but require different download approach")
    return None


def check_s3_zip_exists(start_date: str, end_date: str, congress: int, bill_type: str) -> Optional[str]:
    """
    Check if a ZIP file exists in S3 for the given date range, Congress, and bill type.
    
    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        congress: Congress number
        bill_type: Bill type (e.g., "hr", "s", etc.)
        
    Returns:
        S3 key if file exists, None otherwise
    """
    date_range_path = format_date_range_path(start_date, end_date)
    bill_type_lower = bill_type.lower()
    s3_key = f"downloads/{date_range_path}/BILLSTATUS-{congress}-{bill_type_lower}.zip"
    
    try:
        s3_client.head_object(Bucket=S3_BUCKET_NAME, Key=s3_key)
        return s3_key
    except s3_client.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            return None
        raise


def extract_zip_from_s3(zip_s3_key: str) -> Dict[str, bytes]:
    """
    Extract ZIP file from S3 and return dict of XML filename -> XML content.
    
    Returns:
        Dict mapping XML filename -> XML content as bytes
    """
    log_print(f"📦 Extracting ZIP file from S3: {zip_s3_key}")
    
    # Download ZIP from S3
    zip_obj = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=zip_s3_key)
    zip_content = zip_obj['Body'].read()
    
    # Extract XML files
    xml_files = {}
    zip_file = BytesIO(zip_content)
    
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        file_list = zip_ref.namelist()
        log_print(f"📋 ZIP contains {len(file_list)} file(s)")
        
        # Find XML files
        xml_file_list = [f for f in file_list if f.lower().endswith('.xml')]
        log_print(f"📄 Found {len(xml_file_list)} XML file(s)")
        
        for xml_file in xml_file_list:
            try:
                xml_content = zip_ref.read(xml_file)
                # Store with just the filename (not full path)
                filename = xml_file.split('/')[-1]
                xml_files[filename] = xml_content
            except Exception as e:
                log_print(f"   ⚠️ Failed to extract {xml_file}: {str(e)[:200]}")
    
    log_print(f"✅ Extracted {len(xml_files)} XML file(s) from ZIP")
    return xml_files


def save_cosponsor_search_index_item(
    cosponsor_name: str,
    bill_id: str,
    introduced_date: str
):
    """
    Save a materialized search index item for cosponsors (many-to-many relationship).
    
    Structure:
    - bill_id = SEARCH#COSPONSOR#<cosponsor_name> (hash key)
    - search_index_sk = INTRODUCED_DATE#<date>#<original_bill_id> (range key)
    
    This allows efficient querying of bills by cosponsor name with native DynamoDB pagination.
    The table now has a range key (search_index_sk) to support this pattern.
    
    Args:
        cosponsor_name: Name of the cosponsor (normalized)
        bill_id: Bill ID (e.g., "119-HR-1234")
        introduced_date: Introduced date in ISO format (e.g., "2023-01-15T00:00:00Z")
    """
    if not bills_table or not cosponsor_name or not bill_id:
        return
    
    try:
        # Normalize cosponsor name (trim whitespace)
        normalized_name = str(cosponsor_name).strip()
        if not normalized_name:
            return
        
        # Create search index hash key: "SEARCH#COSPONSOR#<name>"
        search_bill_id = f"SEARCH#COSPONSOR#{normalized_name}"
        
        # Create sort key: "INTRODUCED_DATE#<date>#<original_bill_id>"
        # Extract date part (YYYY-MM-DD) for consistent sorting
        if introduced_date:
            if 'T' in introduced_date:
                date_part = introduced_date.split('T')[0]
            elif ' ' in introduced_date:
                date_part = introduced_date.split(' ')[0]
            else:
                date_part = introduced_date[:10] if len(introduced_date) >= 10 else introduced_date
        else:
            date_part = "1970-01-01"  # Default to epoch if no date
        
        search_sk = f"INTRODUCED_DATE#{date_part}#{bill_id}"
        
        item = {
            'bill_id': search_bill_id,  # Hash key (required)
            'search_index_sk': search_sk,  # Range key (required)
            'search_type': 'COSPONSOR',
            'search_value': normalized_name,
            'entity_bill_id': bill_id,  # Store the actual bill_id for reference
            'introduced_date': date_part,
            'is_search_index': True  # Flag to identify search index items
        }
        
        bills_table.put_item(Item=item)
        
    except Exception as e:
        log_print(f"      ⚠️ Failed to save cosponsor search index {normalized_name} for {bill_id}: {str(e)[:200]}")


def convert_decimal_for_json(obj: Any) -> Any:
    """Recursively convert Decimal to float/string for JSON serialization"""
    from decimal import Decimal
    if isinstance(obj, Decimal):
        try:
            return float(obj)
        except (OverflowError, ValueError):
            return str(obj)
    elif isinstance(obj, dict):
        return {key: convert_decimal_for_json(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_decimal_for_json(item) for item in obj]
    else:
        return obj


def store_oversized_item_to_s3(bill_id: str, full_item: Dict[str, Any]) -> str:
    """
    Store oversized bill item to S3 in oversize/ folder.
    Returns the S3 key.
    """
    # Create S3 key: oversize/{bill_id}.json.gz
    s3_key = f"oversize/{bill_id}.json.gz"
    
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
    
    log_print(f"      💾 Stored oversized bill {bill_id} to S3: {s3_key} ({len(compressed_data):,} bytes compressed, {len(json_bytes):,} bytes uncompressed)")
    return s3_key


def extract_gsi_fields_only(full_item: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract only GSI fields and essential metadata for DynamoDB.
    Used when item exceeds 400KB limit.
    
    GSI fields to preserve:
    - Primary key: bill_id
    - GSI hash keys: sponsor_party, sponsor_full_name, congress, bill_type, bill_title, bill_number, bipartisan
    - GSI range keys: introduced_date
    - Essential metadata: congress, bill_type, bill_number, bipartisan, policy_area
    """
    gsi_fields = {
        # Primary key (required)
        'bill_id': full_item.get('bill_id'),
        'search_index_sk': full_item.get('search_index_sk', full_item.get('bill_id')),  # Range key (required)
        
        # GSI hash keys
        'sponsor_party': full_item.get('sponsor_party'),
        'sponsor_full_name': full_item.get('sponsor_full_name'),
        'congress': full_item.get('congress'),
        'bill_type': full_item.get('bill_type'),
        'bill_title': full_item.get('bill_title'),
        'bill_number': full_item.get('bill_number'),
        'bipartisan': full_item.get('bipartisan'),
        'policy_area': full_item.get('policy_area'),
        
        # GSI range keys
        'introduced_date': full_item.get('introduced_date'),
        'latest_action_date': full_item.get('latest_action_date'),
        
        # Essential metadata
        'is_oversized': True,
        'oversize_s3_key': f"oversize/{full_item.get('bill_id')}.json.gz",
    }
    
    # Remove None values and empty strings (GSI keys cannot be empty strings)
    # Keep False/0 values as they are valid
    cleaned = {}
    for k, v in gsi_fields.items():
        if v is not None and v != "":
            cleaned[k] = v
    
    return cleaned


def clean_empty_gsi_keys(record: Dict) -> Dict:
    """
    Remove empty string and None values from GSI key fields.
    DynamoDB does not allow empty strings or NULL values for GSI keys.
    """
    # List of GSI key fields that cannot be empty strings or None
    gsi_key_fields = [
        'sponsor_party', 'sponsor_full_name', 'sponsor_state',
        'introduced_date', 'latest_action_date',
        'bill_title', 'policy_area'
    ]
    
    cleaned = record.copy()
    for field in gsi_key_fields:
        if field in cleaned and (cleaned[field] == "" or cleaned[field] is None):
            # Remove empty string or None GSI keys (must be omitted entirely)
            del cleaned[field]
    
    return cleaned


def store_bill_to_dynamodb(record: Dict):
    """Store bill record to DynamoDB. Handles oversized items by storing to S3. Also creates cosponsor search index items."""
    if not bills_table:
        log_print("⚠️ DynamoDB table not configured, skipping storage")
        return
    
    bill_id = record.get('bill_id', 'unknown')
    introduced_date = record.get('introduced_date', '')
    
    # Ensure search_index_sk is set (for regular bills, it equals bill_id)
    if 'search_index_sk' not in record or not record.get('search_index_sk'):
        record['search_index_sk'] = bill_id
    
    # Validate that we have the essential fields
    if not bill_id or bill_id == 'unknown':
        log_print(f"      ⚠️ Skipping bill with invalid bill_id: {bill_id}")
        return
    
    # Log record size and key fields for debugging
    record_size = len(str(record))
    key_fields = {
        'bill_id': record.get('bill_id'),
        'search_index_sk': record.get('search_index_sk'),
        'bill_title': record.get('bill_title', '')[:50] if record.get('bill_title') else '',
        'sponsor_full_name': record.get('sponsor_full_name', '')[:50] if record.get('sponsor_full_name') else '',
        'congress': record.get('congress'),
        'bill_type': record.get('bill_type'),
        'bill_number': record.get('bill_number'),
    }
    log_print(f"      📝 Storing {bill_id}: {len(record)} fields, {record_size:,} bytes. Key fields: {key_fields}")
    
    # Clean up empty string GSI keys before storing
    record = clean_empty_gsi_keys(record)
    
    max_put_retries = 3
    
    # Remove temporary _text_versions field before storing (store it separately for SQS)
    text_versions = record.pop('_text_versions', [])
    
    # Preserve attributes written by backfill/Lambda that the fetcher does not set. Fetcher does a full put_item;
    # without this we would wipe roll_call_votes (backfill) and bill_texts (Lambda).
    try:
        existing = bills_table.get_item(
            Key={"bill_id": bill_id, "search_index_sk": record.get("search_index_sk", bill_id)},
            ProjectionExpression="roll_call_votes, roll_call_number, roll_call_votes_oversize_s3_key, bill_texts",
        )
        item = existing.get("Item") or {}
        for key in ("roll_call_votes", "roll_call_number", "roll_call_votes_oversize_s3_key", "bill_texts"):
            if key in item and item[key] is not None:
                record[key] = item[key]
    except Exception as e:
        log_print(f"      ⚠️ Could not preserve roll-call/bill_texts for {bill_id}: {str(e)[:120]}")
    
    # Full overwrite: bulk gives us full bill data, no merge with existing item (bill_texts stay [] until Lambda/backfill run)
    for put_attempt in range(max_put_retries):
        try:
            bills_table.put_item(Item=record)
            log_print(f"      ✅ Stored {bill_id} to DynamoDB with {len(record)} fields")
            
            # Create cosponsor search index items
            cosponsors_json = record.get('cosponsors_json', '')
            if cosponsors_json:
                try:
                    cosponsors = json.loads(cosponsors_json) if isinstance(cosponsors_json, str) else cosponsors_json
                    if isinstance(cosponsors, list):
                        for cosponsor in cosponsors:
                            # Extract cosponsor name (try fullName first, then name)
                            cosponsor_name = cosponsor.get('fullName') or cosponsor.get('name', '')
                            if cosponsor_name:
                                try:
                                    save_cosponsor_search_index_item(
                                        cosponsor_name=cosponsor_name,
                                        bill_id=bill_id,
                                        introduced_date=introduced_date
                                    )
                                except Exception as idx_error:
                                    log_print(f"      ⚠️ Failed to create cosponsor index for {cosponsor_name} on {bill_id}: {str(idx_error)[:200]}")
                except (json.JSONDecodeError, TypeError) as e:
                    log_print(f"      ⚠️ Failed to parse cosponsors_json for {bill_id}: {str(e)[:200]}")
            
            # Send bill text download request to SQS with full text_versions (Lambda downloads all, no API call)
            if text_versions and BILL_TEXT_SQS_URL:
                try:
                    search_index_sk = record.get('search_index_sk') or bill_id
                    message_body = {'bill_id': bill_id, 'search_index_sk': search_index_sk, 'text_versions': text_versions}
                    message_json = json.dumps(message_body, default=str)
                    sqs_client.send_message(
                        QueueUrl=BILL_TEXT_SQS_URL,
                        MessageBody=message_json
                    )
                    log_print(f"      📤 Sent bill text download request for {bill_id} to SQS ({len(text_versions)} version(s))")
                except Exception as e:
                    log_print(f"      ⚠️ Error sending bill text download to SQS: {str(e)[:200]}")
            
            return
        except Exception as put_error:
            error_str = str(put_error)
            
            # Handle oversized items (ValidationException)
            if 'ValidationException' in error_str and 'Item size has exceeded' in error_str:
                log_print(f"      ⚠️ Bill {bill_id} exceeds DynamoDB size limit, storing to S3...")
                
                oversize_s3_key = store_oversized_item_to_s3(bill_id, record)
                gsi_only_item = extract_gsi_fields_only(record)
                gsi_only_item['oversize_s3_key'] = oversize_s3_key
                # Ensure search_index_sk is set for GSI-only item too
                if 'search_index_sk' not in gsi_only_item or not gsi_only_item.get('search_index_sk'):
                    gsi_only_item['search_index_sk'] = bill_id
                
                try:
                    bills_table.put_item(Item=gsi_only_item)
                    log_print(f"      ✅ Stored GSI fields for oversized bill {bill_id} to DynamoDB, full data in S3")
                    
                    # Still create cosponsor search index items even for oversized bills
                    cosponsors_json = record.get('cosponsors_json', '')
                    if cosponsors_json:
                        try:
                            cosponsors = json.loads(cosponsors_json) if isinstance(cosponsors_json, str) else cosponsors_json
                            if isinstance(cosponsors, list):
                                for cosponsor in cosponsors:
                                    cosponsor_name = cosponsor.get('fullName') or cosponsor.get('name', '')
                                    if cosponsor_name:
                                        try:
                                            save_cosponsor_search_index_item(
                                                cosponsor_name=cosponsor_name,
                                                bill_id=bill_id,
                                                introduced_date=introduced_date
                                            )
                                        except Exception as idx_error:
                                            log_print(f"      ⚠️ Failed to create cosponsor index for {cosponsor_name} on {bill_id}: {str(idx_error)[:200]}")
                        except (json.JSONDecodeError, TypeError) as e:
                            log_print(f"      ⚠️ Failed to parse cosponsors_json for {bill_id}: {str(e)[:200]}")
                    
                    # Send bill text download request to SQS with full text_versions
                    if text_versions and BILL_TEXT_SQS_URL:
                        try:
                            search_index_sk = record.get('search_index_sk') or bill_id
                            message_body = {'bill_id': bill_id, 'search_index_sk': search_index_sk, 'text_versions': text_versions}
                            message_json = json.dumps(message_body, default=str)
                            sqs_client.send_message(
                                QueueUrl=BILL_TEXT_SQS_URL,
                                MessageBody=message_json
                            )
                            log_print(f"      📤 Sent bill text download request for {bill_id} to SQS ({len(text_versions)} version(s))")
                        except Exception as e:
                            log_print(f"      ⚠️ Error sending bill text download to SQS: {str(e)[:200]}")
                    
                    return
                except Exception as gsi_error:
                    log_print(f"      ❌ Even GSI-only item too large for {bill_id}: {str(gsi_error)}")
                    raise
            
            # Handle throttling
            elif 'ThrottlingException' in error_str or 'ProvisionedThroughputExceededException' in error_str:
                if put_attempt < max_put_retries - 1:
                    wait_time = (put_attempt + 1) * 2
                    log_print(f"      ⚠️ DynamoDB throttled for bill {bill_id}, waiting {wait_time}s before retry {put_attempt + 1}/{max_put_retries}")
                    time.sleep(wait_time)
                    continue
            
            # Re-raise if not throttling or out of retries
            raise
    
    log_print(f"      ❌ Failed to store {bill_id} after {max_put_retries} attempts")
    raise Exception(f"Failed to store bill {bill_id} to DynamoDB after retries")


def list_bulk_xml_files(congress: int, bill_type: str) -> List[str]:
    """
    List XML files available in bulk data repository for a Congress and bill type.
    Since we can't easily list directory contents via HTTP, we'll try to download
    a known XML file pattern or use the sitemap.
    
    Args:
        congress: Congress number
        bill_type: Bill type (lowercase, e.g., "hr")
    
    Returns:
        List of XML file URLs (or empty list if unable to determine)
    """
    # For now, return empty list - we'll handle this by trying to download
    # the ZIP file first, and if that fails, we'll need to use a different approach
    # The bulk data repository structure may require checking the sitemap
    return []


def save_zip_to_s3(zip_content: bytes, start_date: str, end_date: str, congress: int, bill_type: str) -> str:
    """Save downloaded ZIP file to S3 in downloads/{startdate-enddate}/ format"""
    date_range_path = format_date_range_path(start_date, end_date)
    bill_type_lower = bill_type.lower()
    s3_key = f"downloads/{date_range_path}/BILLSTATUS-{congress}-{bill_type_lower}.zip"
    
    log_print(f"💾 Saving ZIP file to S3: s3://{S3_BUCKET_NAME}/{s3_key}")
    
    s3_client.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=s3_key,
        Body=zip_content,
        ContentType='application/zip'
    )
    
    log_print(f"✅ ZIP file saved to S3: s3://{S3_BUCKET_NAME}/{s3_key} ({len(zip_content):,} bytes)")
    return s3_key


def check_s3_zip_exists(start_date: str, end_date: str, congress: int, bill_type: str) -> Optional[str]:
    """
    Check if a ZIP file exists in S3 for the given date range, Congress, and bill type.
    
    Returns:
        S3 key of ZIP file if exists, None otherwise
    """
    date_range_path = format_date_range_path(start_date, end_date)
    bill_type_lower = bill_type.lower()
    zip_s3_key = f"downloads/{date_range_path}/BILLSTATUS-{congress}-{bill_type_lower}.zip"
    
    try:
        s3_client.head_object(Bucket=S3_BUCKET_NAME, Key=zip_s3_key)
        log_print(f"✅ Found existing ZIP file in S3: {zip_s3_key}")
        return zip_s3_key
    except s3_client.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            return None
        else:
            log_print(f"⚠️ Error checking S3 for ZIP file {zip_s3_key}: {str(e)[:200]}")
            return None


def extract_zip_from_s3(zip_s3_key: str) -> Dict[str, bytes]:
    """
    Extract ZIP file from S3 and return dict of XML filename -> XML content.
    
    Returns:
        Dict mapping XML filename -> XML content as bytes
    """
    log_print(f"📦 Extracting ZIP file from S3: {zip_s3_key}")
    
    # Download ZIP from S3
    zip_obj = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=zip_s3_key)
    zip_content = zip_obj['Body'].read()
    
    # Extract XML files
    xml_files = {}
    zip_file = BytesIO(zip_content)
    
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        file_list = zip_ref.namelist()
        log_print(f"📋 ZIP contains {len(file_list)} file(s)")
        
        # Find XML files
        xml_file_list = [f for f in file_list if f.lower().endswith('.xml')]
        log_print(f"📄 Found {len(xml_file_list)} XML file(s)")
        
        for xml_file in xml_file_list:
            try:
                xml_content = zip_ref.read(xml_file)
                # Store with just the filename (not full path)
                filename = xml_file.split('/')[-1]
                xml_files[filename] = xml_content
            except Exception as e:
                log_print(f"   ⚠️ Failed to extract {xml_file}: {str(e)[:200]}")
    
    log_print(f"✅ Extracted {len(xml_files)} XML file(s) from ZIP")
    return xml_files


def _elem_text(parent, tag: str) -> str:
    """Get trimmed text of first child element with given tag, or empty string."""
    if parent is None:
        return ""
    child = parent.find(tag)
    if child is None or child.text is None:
        return ""
    return child.text.strip() if isinstance(child.text, str) else str(child.text)


def parse_bill_xml(xml_content: bytes, politicians: List[Dict[str, Any]]) -> Optional[Dict]:
    """
    Parse BILLSTATUS XML file and convert to DynamoDB record format.
    Maintains existing table schema and field names.
    
    Args:
        xml_content: XML file content as bytes
        politicians: List of politician records for name matching
    
    Returns:
        Bill record dict matching existing DynamoDB schema, or None if parsing fails
    """
    try:
        # Parse XML (defusedxml prevents XXE)
        root = defusedxml.ElementTree.fromstring(xml_content)
        
        # Find <bill> element
        bill_elem = root.find('bill')
        if bill_elem is None:
            return None
        
        # Extract basic bill info
        # XML uses <number> and <type> (not <billNumber> and <billType>)
        congress_elem = bill_elem.find('congress')
        congress = int(congress_elem.text) if congress_elem is not None and congress_elem.text else None
        
        # Try both <type> and <billType> for compatibility
        bill_type_elem = bill_elem.find('type')
        if bill_type_elem is None:
            bill_type_elem = bill_elem.find('billType')
        bill_type = bill_type_elem.text.upper() if bill_type_elem is not None and bill_type_elem.text else None
        
        # Try both <number> and <billNumber> for compatibility
        bill_number_elem = bill_elem.find('number')
        if bill_number_elem is None:
            bill_number_elem = bill_elem.find('billNumber')
        bill_number = bill_number_elem.text if bill_number_elem is not None and bill_number_elem.text else None
        
        if not congress or not bill_type or not bill_number:
            return None
        
        bill_id_str = f"{congress}-{bill_type}-{bill_number}"
        
        # Extract title (prefer Display Title, fallback to Official Title as Introduced)
        bill_title = ""
        titles_elem = bill_elem.find('titles')
        if titles_elem is not None:
            title_items = titles_elem.findall('item')
            for title_item in title_items:
                title_type_elem = title_item.find('titleType')
                title_elem = title_item.find('title')
                if title_type_elem is not None and title_elem is not None:
                    title_type = title_type_elem.text if title_type_elem.text else ""
                    title_text = title_elem.text if title_elem.text else ""
                    if title_type == "Display Title" and title_text:
                        bill_title = title_text
                        break
                    elif title_type == "Official Title as Introduced" and title_text and not bill_title:
                        bill_title = title_text
        # Fallback: try direct title element (legacy format)
        if not bill_title:
            title_elem = bill_elem.find('title')
            if title_elem is not None and title_elem.text:
                bill_title = title_elem.text
        
        # Full titles list (all title types) for indexing/display
        titles_full = []
        if titles_elem is not None:
            for title_item in titles_elem.findall('item') or []:
                t = {}
                for tag in ('chamberCode', 'chamberName', 'parentTitleType', 'titleType', 'title'):
                    child = title_item.find(tag)
                    if child is not None and child.text is not None:
                        t[tag] = child.text.strip() if isinstance(child.text, str) else str(child.text)
                if t:
                    titles_full.append(t)
        
        # Scalar bill-level fields from XML
        xml_create_date = _elem_text(bill_elem, 'createDate')
        xml_update_date = _elem_text(bill_elem, 'updateDate')
        origin_chamber = _elem_text(bill_elem, 'originChamber')
        is_by_request = _elem_text(bill_elem, 'isByRequest')
        xml_version = _elem_text(bill_elem, 'version')
        if not xml_version and root is not None:
            xml_version = _elem_text(root, 'version')
        xml_update_date_including_text = _elem_text(bill_elem, 'updateDateIncludingText')
        
        # lastAction / latestAction (dedicated snapshot: actionDate, text, actionTime, links)
        last_action_json = ""
        last_action_elem = bill_elem.find('lastAction') or bill_elem.find('latestAction')
        if last_action_elem is not None:
            la = {}
            for tag in ('actionDate', 'text', 'actionTime'):
                child = last_action_elem.find(tag)
                if child is not None and child.text is not None:
                    la[tag] = child.text.strip() if isinstance(child.text, str) else str(child.text)
            links_la = last_action_elem.find('links')
            if links_la is not None:
                la_links = []
                for le in links_la.findall('link') or []:
                    name_e, url_e = le.find('name'), le.find('url')
                    n = name_e.text if name_e is not None and name_e.text else ""
                    u = url_e.text if url_e is not None and url_e.text else ""
                    if n or u:
                        la_links.append({"name": n, "url": u})
                if la_links:
                    la['links'] = la_links
            if la:
                last_action_json = json.dumps(la)
        
        # Extract introduced date
        introduced_date_elem = bill_elem.find('introducedDate')
        introduced_date = introduced_date_elem.text if introduced_date_elem is not None and introduced_date_elem.text else None
        introduced_date = introduced_date if introduced_date else None
        
        # Find actions element (used for both latest action date and actions extraction)
        actions_elem = bill_elem.find('actions')
        
        # Actions container-level counts (facets: actionByCounts, actionTypeCounts)
        actions_action_by_counts = {}
        actions_action_type_counts = {}
        if actions_elem is not None:
            abc_elem = actions_elem.find('actionByCounts')
            if abc_elem is not None:
                for child in abc_elem or []:
                    if child.tag is not None and child.text is not None:
                        key = child.tag.split('}')[-1] if '}' in str(child.tag) else child.tag
                        actions_action_by_counts[key] = child.text.strip()
            atc_elem = actions_elem.find('actionTypeCounts')
            if atc_elem is not None:
                for child in atc_elem or []:
                    if child.tag is not None and child.text is not None:
                        key = child.tag.split('}')[-1] if '}' in str(child.tag) else child.tag
                        actions_action_type_counts[key] = child.text.strip()
        
        # Extract latest action date (from <latestAction> or first action)
        latest_action_date = None
        latest_action_elem = bill_elem.find('latestAction')
        if latest_action_elem is not None:
            action_date_elem = latest_action_elem.find('actionDate')
            if action_date_elem is not None and action_date_elem.text:
                latest_action_date = action_date_elem.text
        
        # Fallback to first action if latestAction not found
        if not latest_action_date and actions_elem is not None:
            action_items = actions_elem.findall('item')
            if action_items:
                # Get first action (most recent)
                first_action = action_items[0]
                action_date_elem = first_action.find('actionDate')
                if action_date_elem is not None and action_date_elem.text:
                    latest_action_date = action_date_elem.text
        
        # Extract primary sponsor
        primary_sponsor = {}
        sponsors_elem = bill_elem.find('sponsors')
        if sponsors_elem is not None:
            sponsor_item = sponsors_elem.find('item')
            if sponsor_item is not None:
                # Extract identifiers (current format)
                identifiers_elem = sponsor_item.find('identifiers')
                if identifiers_elem is not None:
                    bioguide_id_elem = identifiers_elem.find('bioguideId')
                    if bioguide_id_elem is not None and bioguide_id_elem.text:
                        primary_sponsor['bioguideId'] = bioguide_id_elem.text
                    for id_tag in ('gpoId', 'lisID'):
                        e = identifiers_elem.find(id_tag)
                        if e is not None and e.text:
                            primary_sponsor[id_tag] = e.text.strip()
                else:
                    # Legacy format: bioguideId might be direct child
                    bioguide_id_elem = sponsor_item.find('bioguideId')
                    if bioguide_id_elem is not None and bioguide_id_elem.text:
                        primary_sponsor['bioguideId'] = bioguide_id_elem.text
                
                middle_name_elem = sponsor_item.find('middleName')
                if middle_name_elem is not None and middle_name_elem.text:
                    primary_sponsor['middleName'] = middle_name_elem.text.strip()
                
                # Extract name (current format has firstName/lastName/fullName)
                first_name_elem = sponsor_item.find('firstName')
                last_name_elem = sponsor_item.find('lastName')
                full_name_elem = sponsor_item.find('fullName')
                
                if first_name_elem is not None and last_name_elem is not None:
                    primary_sponsor['firstName'] = first_name_elem.text if first_name_elem.text else ""
                    primary_sponsor['lastName'] = last_name_elem.text if last_name_elem.text else ""
                    if full_name_elem is not None and full_name_elem.text:
                        primary_sponsor['fullName'] = full_name_elem.text
                    else:
                        primary_sponsor['fullName'] = f"{primary_sponsor.get('firstName', '')} {primary_sponsor.get('lastName', '')}".strip()
                elif full_name_elem is not None and full_name_elem.text:
                    # Legacy format: might only have fullName
                    primary_sponsor['fullName'] = full_name_elem.text
                    # Try to parse first/last from fullName
                    name_parts = full_name_elem.text.split()
                    if len(name_parts) >= 2:
                        primary_sponsor['firstName'] = name_parts[0]
                        primary_sponsor['lastName'] = " ".join(name_parts[1:])
                
                # Extract party and state
                party_elem = sponsor_item.find('party')
                if party_elem is not None and party_elem.text:
                    primary_sponsor['party'] = party_elem.text
                
                state_elem = sponsor_item.find('state')
                if state_elem is not None and state_elem.text:
                    primary_sponsor['state'] = state_elem.text
                
                district_elem = sponsor_item.find('district')
                if district_elem is not None and district_elem.text:
                    primary_sponsor['district'] = district_elem.text
        
        # Match sponsor with politician CSV data (prefer bioguide_id, then name matching)
        matched_politician = None
        if primary_sponsor.get('bioguideId'):
            # Try bioguide_id first (most reliable)
            matched_politician = find_politician_by_bioguide_id(
                primary_sponsor.get('bioguideId', ''),
                politicians
            )
        
        if not matched_politician and primary_sponsor.get('fullName'):
            # Fall back to name matching
            matched_politician = find_matching_politician(
                primary_sponsor.get('fullName', ''),
                politicians,
                first_name=primary_sponsor.get('firstName', ''),
                last_name=primary_sponsor.get('lastName', ''),
                party=primary_sponsor.get('party', ''),
                state=primary_sponsor.get('state', '')
            )
        
        if matched_politician:
            # Merge matched data - use CSV standardized name for sponsor_full_name
            primary_sponsor.update(matched_politician)
            # Override fullName with CSV standardized name for search compatibility
            if matched_politician.get('name'):
                primary_sponsor['fullName'] = matched_politician['name']
                primary_sponsor['firstName'] = matched_politician.get('first_name', primary_sponsor.get('firstName', ''))
                primary_sponsor['lastName'] = matched_politician.get('last_name', primary_sponsor.get('lastName', ''))
                # Use CSV party/state if available (more reliable)
                if matched_politician.get('party'):
                    primary_sponsor['party'] = matched_politician['party']
                if matched_politician.get('state'):
                    primary_sponsor['state'] = matched_politician['state']
        
        # Extract cosponsors
        cosponsors = []
        cosponsors_elem = bill_elem.find('cosponsors')
        if cosponsors_elem is not None:
            cosponsor_items = cosponsors_elem.findall('item')
            for cosponsor_item in cosponsor_items:
                cosponsor = {}
                
                # Extract name (handle both current and legacy formats)
                first_name_elem = cosponsor_item.find('firstName')
                last_name_elem = cosponsor_item.find('lastName')
                full_name_elem = cosponsor_item.find('fullName')
                name_elem = cosponsor_item.find('name')  # Legacy format
                middle_name_elem = cosponsor_item.find('middleName')
                if middle_name_elem is not None and middle_name_elem.text:
                    cosponsor['middleName'] = middle_name_elem.text.strip()
                for tag in ('isOriginalCosponsor', 'sponsorshipDate', 'sponsorshipWithdrawnDate'):
                    e = cosponsor_item.find(tag)
                    if e is not None and e.text is not None:
                        cosponsor[tag] = e.text.strip() if isinstance(e.text, str) else str(e.text)
                
                if first_name_elem is not None and last_name_elem is not None:
                    cosponsor['firstName'] = first_name_elem.text if first_name_elem.text else ""
                    cosponsor['lastName'] = last_name_elem.text if last_name_elem.text else ""
                    if full_name_elem is not None and full_name_elem.text:
                        cosponsor['fullName'] = full_name_elem.text
                    else:
                        cosponsor['fullName'] = f"{cosponsor.get('firstName', '')} {cosponsor.get('lastName', '')}".strip()
                elif full_name_elem is not None and full_name_elem.text:
                    cosponsor['fullName'] = full_name_elem.text
                    name_parts = full_name_elem.text.split()
                    if len(name_parts) >= 2:
                        cosponsor['firstName'] = name_parts[0]
                        cosponsor['lastName'] = " ".join(name_parts[1:])
                elif name_elem is not None and name_elem.text:
                    # Legacy format: use 'name' field
                    cosponsor['fullName'] = name_elem.text
                    name_parts = name_elem.text.split(',')
                    if len(name_parts) >= 2:
                        cosponsor['lastName'] = name_parts[0].strip()
                        cosponsor['firstName'] = name_parts[1].strip().split()[0] if name_parts[1].strip() else ""
                
                # Extract identifiers (handle both current and legacy formats)
                identifiers_elem = cosponsor_item.find('identifiers')
                if identifiers_elem is not None:
                    bioguide_id_elem = identifiers_elem.find('bioguideId')
                    if bioguide_id_elem is not None and bioguide_id_elem.text:
                        cosponsor['bioguideId'] = bioguide_id_elem.text
                    for id_tag in ('gpoId', 'lisID'):
                        e = identifiers_elem.find(id_tag)
                        if e is not None and e.text:
                            cosponsor[id_tag] = e.text.strip()
                else:
                    # Legacy format: bioguideId might be direct child
                    bioguide_id_elem = cosponsor_item.find('bioguideId')
                    if bioguide_id_elem is not None and bioguide_id_elem.text:
                        cosponsor['bioguideId'] = bioguide_id_elem.text
                
                # Extract party and state (may be in fullName for legacy format, e.g., "Rep. Gabbard, Tulsi [D-HI-2]")
                party_elem = cosponsor_item.find('party')
                if party_elem is not None and party_elem.text:
                    cosponsor['party'] = party_elem.text
                elif cosponsor.get('fullName'):
                    # Try to extract from fullName format: "[D-HI-2]"
                    import re
                    match = re.search(r'\[([DRIL])-([A-Z]{2})(?:-(\d+))?\]', cosponsor['fullName'])
                    if match:
                        cosponsor['party'] = match.group(1)
                        cosponsor['state'] = match.group(2)
                        if match.group(3):
                            cosponsor['district'] = match.group(3)
                
                state_elem = cosponsor_item.find('state')
                if state_elem is not None and state_elem.text:
                    cosponsor['state'] = state_elem.text
                
                district_elem = cosponsor_item.find('district')
                if district_elem is not None and district_elem.text:
                    cosponsor['district'] = district_elem.text
                
                # Match with politician CSV data (prefer bioguide_id, then name matching)
                matched_politician = None
                if cosponsor.get('bioguideId'):
                    # Try bioguide_id first (most reliable)
                    matched_politician = find_politician_by_bioguide_id(
                        cosponsor.get('bioguideId', ''),
                        politicians
                    )
                
                if not matched_politician and cosponsor.get('fullName'):
                    # Fall back to name matching
                    matched_politician = find_matching_politician(
                        cosponsor.get('fullName', ''),
                        politicians,
                        first_name=cosponsor.get('firstName', ''),
                        last_name=cosponsor.get('lastName', ''),
                        party=cosponsor.get('party', ''),
                        state=cosponsor.get('state', '')
                    )
                
                if matched_politician:
                    # Merge matched data - use CSV standardized name for search compatibility
                    cosponsor.update(matched_politician)
                    # Override fullName with CSV standardized name
                    if matched_politician.get('name'):
                        cosponsor['fullName'] = matched_politician['name']
                        cosponsor['name'] = matched_politician['name']  # Also set 'name' field for consistency
                        cosponsor['firstName'] = matched_politician.get('first_name', cosponsor.get('firstName', ''))
                        cosponsor['lastName'] = matched_politician.get('last_name', cosponsor.get('lastName', ''))
                        # Use CSV party/state if available (more reliable)
                        if matched_politician.get('party'):
                            cosponsor['party'] = matched_politician['party']
                        if matched_politician.get('state'):
                            cosponsor['state'] = matched_politician['state']
                
                cosponsors.append(cosponsor)
        
        # Extract actions
        actions = []
        if actions_elem is not None:
            action_items = actions_elem.findall('item')
            for action_item in action_items:
                action = {}
                
                action_date_elem = action_item.find('actionDate')
                if action_date_elem is not None and action_date_elem.text:
                    action['actionDate'] = action_date_elem.text
                
                action_text_elem = action_item.find('text')
                if action_text_elem is not None and action_text_elem.text:
                    action['text'] = action_text_elem.text
                
                action_type_elem = action_item.find('type')
                if action_type_elem is not None and action_type_elem.text:
                    action['type'] = action_type_elem.text
                
                action_code_elem = action_item.find('actionCode')
                if action_code_elem is not None and action_code_elem.text:
                    action['actionCode'] = action_code_elem.text
                
                action_time_elem = action_item.find('actionTime')
                if action_time_elem is not None and action_time_elem.text:
                    action['actionTime'] = action_time_elem.text.strip() if isinstance(action_time_elem.text, str) else str(action_time_elem.text)
                
                committee_elem = action_item.find('committee')
                if committee_elem is not None:
                    c_name = _elem_text(committee_elem, 'name')
                    c_code = _elem_text(committee_elem, 'systemCode')
                    if c_name or c_code:
                        action['committee'] = {'name': c_name, 'systemCode': c_code}
                
                source_elem = action_item.find('sourceSystem')
                if source_elem is not None:
                    s_code = _elem_text(source_elem, 'code')
                    s_name = _elem_text(source_elem, 'name')
                    if s_code or s_name:
                        action['sourceSystem'] = {'code': s_code, 'name': s_name}
                
                # Links (e.g. roll call: name "Roll no. 60", url to clerk.house.gov/evs/... or senate)
                links_elem = action_item.find('links')
                if links_elem is not None:
                    link_elems = links_elem.findall('link')
                    action_links = []
                    for le in link_elems or []:
                        name_e = le.find('name') if le is not None else None
                        url_e = le.find('url') if le is not None else None
                        name = name_e.text if name_e is not None and name_e.text else ""
                        url = url_e.text if url_e is not None and url_e.text else ""
                        if name or url:
                            action_links.append({"name": name, "url": url})
                    if action_links:
                        action['links'] = action_links
                
                # Per-action recordedVotes (format as of late 2022: votes live under actions/item)
                rv_action_elem = action_item.find('recordedVotes')
                if rv_action_elem is not None:
                    for rv_item in rv_action_elem.findall('recordedVote') or []:
                        rv = {}
                        for tag in ('chamber', 'congress', 'date', 'fullActionName', 'rollNumber', 'sessionNumber', 'url'):
                            child = rv_item.find(tag)
                            if child is not None and child.text is not None:
                                rv[tag] = child.text.strip() if isinstance(child.text, str) else str(child.text)
                        if rv:
                            action.setdefault('recordedVotes', []).append(rv)
                
                actions.append(action)
        
        # Deduplicate actions so we match Congress.gov (one row per logical event; XML has same event from multiple sources e.g. House floor + LoC)
        seen_action_key = set()
        deduped_actions = []
        for a in actions:
            text = (a.get("text") or "").strip()
            key = (a.get("actionDate") or "", text)
            if key in seen_action_key:
                # Merge recordedVotes into the copy we already kept (recorded_votes dedupe below handles chamber+roll+session)
                for d in deduped_actions:
                    if (d.get("actionDate") or "", (d.get("text") or "").strip()) == key:
                        d.setdefault("recordedVotes", []).extend(a.get("recordedVotes") or [])
                        break
                continue
            seen_action_key.add(key)
            deduped_actions.append(a)
        actions = deduped_actions
        
        # Recorded votes: bill-level (legacy) + from actions (current format); dedupe by chamber+roll+session
        def _norm_rv(r):
            return (r.get('chamber') or '', r.get('rollNumber') or '', r.get('sessionNumber') or '')
        recorded_votes = []
        seen_rv = set()
        rv_elem = bill_elem.find('recordedVotes')
        if rv_elem is not None:
            for rv_item in rv_elem.findall('recordedVote') or []:
                rv = {}
                for tag in ('chamber', 'congress', 'date', 'fullActionName', 'rollNumber', 'sessionNumber', 'url'):
                    child = rv_item.find(tag)
                    if child is not None and child.text is not None:
                        rv[tag] = child.text.strip() if isinstance(child.text, str) else str(child.text)
                if rv and _norm_rv(rv) not in seen_rv:
                    seen_rv.add(_norm_rv(rv))
                    recorded_votes.append(rv)
        for a in actions:
            for rv in a.get('recordedVotes') or []:
                if _norm_rv(rv) not in seen_rv:
                    seen_rv.add(_norm_rv(rv))
                    recorded_votes.append(rv)
        
        # Extract summaries (support both billSummaries/item and summary elements)
        summaries = []
        summaries_elem = bill_elem.find('summaries')
        if summaries_elem is not None:
            bill_summaries_elem = summaries_elem.find('billSummaries')
            if bill_summaries_elem is not None:
                summary_items = bill_summaries_elem.findall('item')
            else:
                summary_items = summaries_elem.findall('summary')
            for summary_item in summary_items or []:
                summary = {}
                # Try to get text from <cdata><text> structure (current format)
                cdata_elem = summary_item.find('cdata')
                if cdata_elem is not None:
                    text_in_cdata = cdata_elem.find('text')
                    if text_in_cdata is not None and text_in_cdata.text:
                        summary['text'] = text_in_cdata.text
                if not summary.get('text'):
                    text_elem = summary_item.find('text')
                    if text_elem is not None and text_elem.text:
                        summary['text'] = text_elem.text
                for tag in ('actionDate', 'actionDesc', 'versionCode', 'name', 'updateDate', 'lastSummaryUpdateDate'):
                    val = _elem_text(summary_item, tag)
                    if val:
                        summary[tag] = val
                if not summary.get('versionCode'):
                    version_elem = summary_item.find('versionCode')
                    if version_elem is not None and version_elem.text:
                        summary['versionCode'] = version_elem.text.strip()
                if not summary.get('actionDesc'):
                    action_desc_elem = summary_item.find('actionDesc')
                    if action_desc_elem is not None and action_desc_elem.text:
                        summary['actionDesc'] = action_desc_elem.text.strip()
                if summary.get('text'):
                    summaries.append(summary)
        
        # Extract subjects/policy area
        # Policy area can be:
        # 1. Directly under <bill> as <policyArea><name>
        # 2. Under <subjects> as <policyArea><name>
        policy_area = ""
        
        # Try direct <policyArea> under <bill> first
        policy_area_elem = bill_elem.find('policyArea')
        if policy_area_elem is not None:
            name_elem = policy_area_elem.find('name')
            if name_elem is not None and name_elem.text:
                policy_area = name_elem.text
        
        # Fallback to <subjects><policyArea><name>
        if not policy_area:
            subjects_elem = bill_elem.find('subjects')
            if subjects_elem is not None:
                policy_area_elem = subjects_elem.find('policyArea')
                if policy_area_elem is not None:
                    name_elem = policy_area_elem.find('name')
                    if name_elem is not None and name_elem.text:
                        policy_area = name_elem.text
        
        # Full subjects (billSubjects/otherSubjects + primarySubjects, or legislativeSubjects/item + policyArea)
        subjects_full = []
        subjects_elem = bill_elem.find('subjects')
        if subjects_elem is not None:
            bill_subjects_elem = subjects_elem.find('billSubjects')
            if bill_subjects_elem is not None:
                for os_elem in bill_subjects_elem.findall('otherSubjects') or []:
                    for item in os_elem.findall('item') or []:
                        s = {}
                        name_el = item.find('name')
                        if name_el is not None and name_el.text:
                            s['name'] = name_el.text.strip()
                        parent = item.find('parentSubject')
                        if parent is not None:
                            pname = parent.find('name')
                            if pname is not None and pname.text:
                                s['parentSubject'] = pname.text.strip()
                        if s:
                            subjects_full.append(s)
                for ps_elem in bill_subjects_elem.findall('primarySubjects') or []:
                    name_el = ps_elem.find('name')
                    if name_el is not None and name_el.text:
                        s = {'name': name_el.text.strip(), 'primary': True}
                        parent = ps_elem.find('parentSubject')
                        if parent is not None:
                            pname = parent.find('name')
                            if pname is not None and pname.text:
                                s['parentSubject'] = pname.text.strip()
                        subjects_full.append(s)
            # Current format: subjects/legislativeSubjects/item (name, updateDate) and subjects/policyArea (name)
            if not subjects_full:
                for item in (subjects_elem.find('legislativeSubjects') or subjects_elem).findall('item') or []:
                    s = {}
                    v = _elem_text(item, 'name')
                    if v:
                        s['name'] = v
                    v = _elem_text(item, 'updateDate')
                    if v:
                        s['updateDate'] = v
                    if s:
                        subjects_full.append(s)
                pa = subjects_elem.find('policyArea')
                if pa is not None:
                    v = _elem_text(pa, 'name')
                    if v and not any(x.get('name') == v and x.get('primary') for x in subjects_full):
                        subjects_full.append({'name': v, 'primary': True})
        
        # Extract amendments (full: number, description, purpose, type, latestAction, amendedBill, sponsors, actions, Congress.gov URL)
        amendments = []
        amendments_elem = bill_elem.find('amendments')
        if amendments_elem is not None:
            amendment_items = amendments_elem.findall('amendment')
            for amendment_item in amendment_items or []:
                amendment = {}
                for tag in ('number', 'description', 'purpose', 'type', 'submittedDate', 'chamber', 'updateDate'):
                    val = _elem_text(amendment_item, tag)
                    if val:
                        amendment[tag] = val
                # Amendment congress (for URL); fallback to bill congress
                amdt_congress = congress
                c_el = amendment_item.find('congress')
                if c_el is not None and c_el.text:
                    try:
                        amdt_congress = int(c_el.text.strip())
                    except ValueError:
                        pass
                la_am = amendment_item.find('latestAction')
                if la_am is not None:
                    la = {}
                    for t in ('actionDate', 'text'):
                        v = _elem_text(la_am, t)
                        if v:
                            la[t] = v
                    if la:
                        amendment['latestAction'] = la
                ab_elem = amendment_item.find('amendedBill')
                if ab_elem is not None:
                    ab = {}
                    for t in ('congress', 'number', 'originChamber', 'originChamberCode', 'title', 'type', 'updateDateIncludingText'):
                        v = _elem_text(ab_elem, t)
                        if v:
                            ab[t] = v
                    if ab:
                        amendment['amendedBill'] = ab
                # Sponsors (e.g. Rules Committee)
                sponsors_am = amendment_item.find('sponsors')
                if sponsors_am is not None:
                    sponsor_items = sponsors_am.findall('item')
                    if not sponsor_items:
                        name_el = sponsors_am.find('name')
                        if name_el is not None and name_el.text:
                            amendment['sponsors'] = [{'name': name_el.text.strip()}]
                    else:
                        amendment['sponsors'] = []
                        for si in sponsor_items:
                            n = _elem_text(si, 'name')
                            if n:
                                amendment['sponsors'].append({'name': n})
                # Actions (count + list of action items)
                actions_am_elem = amendment_item.find('actions')
                if actions_am_elem is not None:
                    count_el = actions_am_elem.find('count')
                    if count_el is not None and count_el.text is not None:
                        try:
                            amendment['actionsCount'] = int(count_el.text.strip())
                        except ValueError:
                            pass
                    actions_container = actions_am_elem.find('actions')
                    if actions_container is not None:
                        action_items = actions_container.findall('item')
                        amdt_actions = []
                        for act_item in action_items or []:
                            act = {}
                            for t in ('actionDate', 'actionTime', 'text', 'type', 'actionCode'):
                                v = _elem_text(act_item, t)
                                if v:
                                    act[t] = v
                            if act:
                                amdt_actions.append(act)
                        if amdt_actions:
                            amendment['actions'] = amdt_actions
                # Congress.gov amendment URL (house-amendment / senate-amendment + number)
                amdt_type = (amendment.get('type') or '').upper()
                amdt_number = amendment.get('number') or ''
                if amdt_number and amdt_congress:
                    path_part = 'house-amendment' if amdt_type == 'HAMDT' else 'senate-amendment' if amdt_type == 'SAMDT' else 'house-amendment'
                    amendment['congress_gov_url'] = f"https://www.congress.gov/amendment/{amdt_congress}th-congress/{path_part}/{amdt_number}"
                if amendment:
                    amendments.append(amendment)
        
        # Extract text versions (for SQS processing; store all formats per version)
        text_versions = []
        text_versions_elem = bill_elem.find('textVersions')
        if text_versions_elem is not None:
            text_version_items = text_versions_elem.findall('item')
            for text_version_item in text_version_items:
                text_version = {}
                
                type_elem = text_version_item.find('type')
                if type_elem is not None and type_elem.text:
                    text_version['type'] = type_elem.text
                
                formats_elem = text_version_item.find('formats')
                if formats_elem is not None:
                    format_items = formats_elem.findall('item')
                    formats_list = []
                    for format_item in format_items:
                        f = {}
                        url_elem = format_item.find('url')
                        if url_elem is not None and url_elem.text:
                            f['url'] = url_elem.text.strip()
                        type_f = _elem_text(format_item, 'type')
                        if type_f:
                            f['type'] = type_f
                        if f:
                            formats_list.append(f)
                    if formats_list:
                        text_version['formats'] = formats_list
                    # Keep first url at top level for backward compatibility / SQS
                    if formats_list and formats_list[0].get('url'):
                        text_version['url'] = formats_list[0]['url']
                
                date_elem = text_version_item.find('date')
                if date_elem is not None and date_elem.text:
                    text_version['date'] = date_elem.text.strip()
                if text_version:
                    text_versions.append(text_version)
        
        # Calendar numbers
        calendar_numbers = []
        cal_elem = bill_elem.find('calendarNumbers')
        if cal_elem is not None:
            for item in cal_elem.findall('item') or []:
                c = {}
                for tag in ('calendar', 'number'):
                    v = _elem_text(item, tag)
                    if v:
                        c[tag] = v
                if c:
                    calendar_numbers.append(c)
        
        # CBO cost estimates
        cbo_cost_estimates = []
        cbo_elem = bill_elem.find('cboCostEstimates')
        if cbo_elem is not None:
            for item in cbo_elem.findall('item') or []:
                c = {}
                for tag in ('rptPubDate', 'rptTitle', 'rptUrl'):
                    v = _elem_text(item, tag)
                    if v:
                        c[tag] = v
                if c:
                    cbo_cost_estimates.append(c)
        
        # Constitutional authority statement (direct or inside <cdata>)
        constitutional_authority_statement_text = ""
        cas_elem = bill_elem.find('constitutionalAuthorityStatementText')
        if cas_elem is not None and cas_elem.text:
            constitutional_authority_statement_text = (cas_elem.text or "").strip()[:50000]
        if not constitutional_authority_statement_text:
            cdata_elem = bill_elem.find('cdata')
            if cdata_elem is not None:
                cas_elem = cdata_elem.find('constitutionalAuthorityStatementText')
                if cas_elem is not None and cas_elem.text:
                    constitutional_authority_statement_text = (cas_elem.text or "").strip()[:50000]
        
        # Committee reports
        committee_reports = []
        cr_elem = bill_elem.find('committeeReports')
        if cr_elem is not None:
            for report in cr_elem.findall('committeeReport') or []:
                cit = _elem_text(report, 'citation')
                if cit:
                    committee_reports.append({"citation": cit})
        
        # Committees (billCommittees/item or committees/item: chamber, name, systemCode, type, activities, subcommittees)
        committees_list = []
        comm_elem = bill_elem.find('committees')
        if comm_elem is not None:
            items_src = comm_elem.find('billCommittees')
            if items_src is not None:
                committee_items = items_src.findall('item') or []
            else:
                committee_items = comm_elem.findall('item') or []
            if committee_items:
                for item in committee_items:
                    c = {}
                    for tag in ('chamber', 'name', 'systemCode', 'type'):
                        v = _elem_text(item, tag)
                        if v:
                            c[tag] = v
                    activities = []
                    act_container = item.find('activities')
                    for act_item in (act_container.findall('item') if act_container is not None else []) or []:
                        a = {}
                        for t in ('date', 'name'):
                            v = _elem_text(act_item, t)
                            if v:
                                a[t] = v
                        if a:
                            activities.append(a)
                    if activities:
                        c['activities'] = activities
                    subcoms = []
                    sub_container = item.find('subcommittees')
                    for sc in (sub_container.findall('item') if sub_container is not None else []) or []:
                        sc_d = {}
                        for t in ('name', 'systemCode'):
                            v = _elem_text(sc, t)
                            if v:
                                sc_d[t] = v
                        if sc_d:
                            subcoms.append(sc_d)
                    if subcoms:
                        c['subcommittees'] = subcoms
                    if c:
                        committees_list.append(c)
        
        # Laws (public/private law citations)
        laws_list = []
        laws_elem = bill_elem.find('laws')
        if laws_elem is not None:
            for item in laws_elem.findall('item') or []:
                l = {}
                for tag in ('number', 'type'):
                    v = _elem_text(item, tag)
                    if v:
                        l[tag] = v
                if l:
                    laws_list.append(l)
        
        # Notes (links + text CDATA)
        notes_list = []
        notes_elem = bill_elem.find('notes')
        if notes_elem is not None:
            for item in notes_elem.findall('Item') or notes_elem.findall('item') or []:
                n = {}
                text_el = item.find('text')
                if text_el is not None and text_el.text:
                    n['text'] = (text_el.text or "")[:30000]
                links_el = item.find('links')
                if links_el is not None:
                    link_list = []
                    for le in links_el.findall('link') or []:
                        na, u = _elem_text(le, 'name'), _elem_text(le, 'url')
                        if na or u:
                            link_list.append({"name": na, "url": u})
                    if link_list:
                        n['links'] = link_list
                if n:
                    notes_list.append(n)
        
        # Related bills
        related_bills = []
        rb_elem = bill_elem.find('relatedBills')
        if rb_elem is not None:
            for item in rb_elem.findall('item') or []:
                r = {}
                for tag in ('congress', 'number', 'type', 'latestTitle', 'title'):
                    v = _elem_text(item, tag)
                    if v:
                        r[tag] = v
                if r.get('title') and not r.get('latestTitle'):
                    r['latestTitle'] = r['title']
                la_r = item.find('latestAction')
                if la_r is not None:
                    r_la = {}
                    for t in ('actionDate', 'text'):
                        v = _elem_text(la_r, t)
                        if v:
                            r_la[t] = v
                    if r_la:
                        r['latestAction'] = r_la
                rd_el = item.find('relationshipDetails')
                if rd_el is not None:
                    rds = []
                    for rd_item in rd_el.findall('item') or []:
                        rd = {}
                        for t in ('identifiedBy', 'type'):
                            v = _elem_text(rd_item, t)
                            if v:
                                rd[t] = v
                        if rd:
                            rds.append(rd)
                    if rds:
                        r['relationshipDetails'] = rds
                if r:
                    related_bills.append(r)
        
        # Dublin Core metadata (under billStatus root; may use dc: namespace)
        dublin_core = {}
        dc_elem = root.find('dublinCore')
        if dc_elem is not None:
            for child in dc_elem:
                if child.text is not None:
                    key = child.tag.split('}')[-1] if '}' in str(child.tag) else child.tag
                    dublin_core[key] = child.text.strip()
        
        # Build record matching existing DynamoDB schema
        record = {
            # Bill Basic Info
            "bill_id": bill_id_str,
            "search_index_sk": bill_id_str,
            "congress": congress,
            "bill_type": bill_type,
            "bill_number": int(bill_number) if bill_number.isdigit() else 0,
            "bill_title": bill_title,
            "bill_url": f"https://www.congress.gov/bill/{congress}th-congress/{bill_type.lower()}/{bill_number}",
            
            # Dates
            "introduced_date": introduced_date,
            "latest_action_date": latest_action_date,
            "update_date": xml_update_date if xml_update_date else "",
            "update_date_including_text": xml_update_date_including_text if xml_update_date_including_text else "",
            
            # Primary Sponsor
            "sponsor_bioguide_id": primary_sponsor.get("bioguideId", ""),
            "sponsor_full_name": primary_sponsor.get("fullName", ""),
            "sponsor_first_name": primary_sponsor.get("firstName", ""),
            "sponsor_last_name": primary_sponsor.get("lastName", ""),
            "sponsor_party": primary_sponsor.get("party", ""),
            "sponsor_state": primary_sponsor.get("state", ""),
            "sponsor_district": primary_sponsor.get("district", ""),
            "sponsor_url": primary_sponsor.get("url", ""),
            
            # Cosponsors
            "cosponsor_count": len(cosponsors),
            "cosponsors": "",
            "cosponsor_parties": "|".join([c.get("party", "") for c in cosponsors if c.get("party")]),
            "cosponsors_json": json.dumps(cosponsors) if cosponsors else "",
            
            # Actions (including container-level counts)
            "action_count": len(actions),
            "actions_json": json.dumps(actions) if actions else "",
            "actions_summary": " | ".join([f"{a.get('actionDate', '')}: {a.get('text', '')[:100]}" for a in actions[:10]]) if actions else "",
            "actions_action_by_counts_json": json.dumps(actions_action_by_counts) if actions_action_by_counts else "",
            "actions_action_type_counts_json": json.dumps(actions_action_type_counts) if actions_action_type_counts else "",
            
            # Latest action
            "latest_action_text": actions[0].get('text', '') if actions else "",
            "latest_action_type": actions[0].get('type', '') if actions else "",
            
            # Recorded votes (from bulk XML; has_roll_call drives HasRollCallIndex; member-level data from backfill)
            "has_roll_call": 1 if recorded_votes else 0,
            "recorded_votes_json": json.dumps(recorded_votes) if recorded_votes else "",
            
            # Summaries
            "summary_count": len(summaries),
            "summaries_json": json.dumps(summaries) if summaries else "",
            "summary_text": " | ".join([s.get('text', '')[:200] for s in summaries[:3]]) if summaries else "",
            
            # Subjects/Policy Area (full subject terms from XML)
            "policy_area": policy_area if policy_area else "Other",
            "legislative_subjects": "|".join([s.get("name", "") for s in subjects_full if s.get("name")])[:4000] if subjects_full else "",
            "subjects_json": json.dumps(subjects_full) if subjects_full else "",
            
            # Amendments
            "amendment_count": len(amendments),
            "amendments_json": json.dumps(amendments) if amendments else "",
            
            # Full titles, lastAction, XML metadata
            "titles_json": json.dumps(titles_full) if titles_full else "",
            "last_action_json": last_action_json,
            "xml_create_date": xml_create_date,
            "xml_update_date": xml_update_date,
            "xml_version": xml_version,
            "origin_chamber": origin_chamber,
            "is_by_request": is_by_request,
            
            # Calendar, CBO, constitutional statement, committees, laws, notes, related bills
            "calendar_numbers_json": json.dumps(calendar_numbers) if calendar_numbers else "",
            "cbo_cost_estimates_json": json.dumps(cbo_cost_estimates) if cbo_cost_estimates else "",
            "constitutional_authority_statement_text": constitutional_authority_statement_text[:50000] if constitutional_authority_statement_text else "",
            "committee_reports_json": json.dumps(committee_reports) if committee_reports else "",
            "committees_json": json.dumps(committees_list) if committees_list else "",
            "laws_json": json.dumps(laws_list) if laws_list else "",
            "notes_json": json.dumps(notes_list) if notes_list else "",
            "related_bills_json": json.dumps(related_bills) if related_bills else "",
            
            # Text versions (persisted for indexing; also used for SQS)
            "text_versions_json": json.dumps(text_versions) if text_versions else "",
            # Bill texts: array of {name, s3_key, type} filled by backfill; fetcher sets [] (we own files in S3)
            "bill_texts": [],
            
            # Dublin Core (root-level metadata)
            "dublin_core_json": json.dumps(dublin_core) if dublin_core else "",
            
            # Metadata
            "indexed_at": datetime.now(timezone.utc).isoformat(),
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "data_source": "congress_gov_bulk_data",
        }
        
        # Calculate bipartisan (1 if any cosponsor party differs from sponsor OR cosponsor parties are mixed)
        # Use matched party values from CSV (more reliable)
        sponsor_party = primary_sponsor.get("party", "")
        if sponsor_party:
            # Normalize party to single character (R, D, I)
            sponsor_party = sponsor_party.strip().upper()[0] if sponsor_party.strip() else ""
        
        if sponsor_party and cosponsors:
            cosponsor_parties = []
            for c in cosponsors:
                cosp_party = c.get("party", "")
                if cosp_party:
                    # Normalize to single character
                    cosp_party = cosp_party.strip().upper()[0] if cosp_party.strip() else ""
                    if cosp_party:
                        cosponsor_parties.append(cosp_party)
            
            if len(cosponsor_parties) > 0:
                # Check if cosponsor parties are all the same
                unique_cosponsor_parties = set(cosponsor_parties)
                
                # Bipartisan if:
                # 1. Cosponsor parties are not all the same (mixed parties), OR
                # 2. At least one cosponsor party is different from sponsor party
                if len(unique_cosponsor_parties) > 1:
                    # Mixed cosponsor parties = bipartisan
                    record["bipartisan"] = 1
                elif sponsor_party not in unique_cosponsor_parties:
                    # All cosponsors are same party, but different from sponsor = bipartisan
                    record["bipartisan"] = 1
                else:
                    # All cosponsors same party as sponsor = not bipartisan
                    record["bipartisan"] = 0
            else:
                # No cosponsors with parties = not bipartisan
                record["bipartisan"] = 0
        else:
            # No sponsor party or no cosponsors = not bipartisan
            record["bipartisan"] = 0
        
        # Store text versions in record for later SQS processing (after bill is stored)
        record["_text_versions"] = text_versions  # Temporary field, will be removed before storage
        
        return record
        
    except ET.ParseError as e:
        log_print(f"      ⚠️ XML parsing error: {str(e)[:200]}")
        return None
    except Exception as e:
        log_print(f"      ⚠️ Error parsing bill XML: {str(e)[:200]}")
        return None


def process_bulk_zip_file(congress: int, bill_type: str, start_date: str, end_date: str,
                          politicians: List[Dict[str, Any]], zip_content: Optional[bytes] = None,
                          zip_s3_key: Optional[str] = None, start_date_dt: Optional[datetime] = None,
                          end_date_dt: Optional[datetime] = None,
                          politicians_by_bioguide: Optional[Dict[str, Dict[str, Any]]] = None,
                          rolls_written_from_bills: Optional[set] = None,
                          roll_write_lock: Optional[threading.Lock] = None) -> Tuple[int, int]:
    """
    Process a bulk ZIP file: extract XML files, parse them, and store each batch immediately.
    Follows govt_contracts pattern: download, parse, store, clear memory.
    When politicians_by_bioguide and rolls_written_from_bills are provided, runs roll call delta
    after each stored bill (SEARCH#ROLL + SEARCH#VOTE updates).

    Args:
        congress: Congress number
        bill_type: Bill type (e.g., "HR", "S")
        start_date: Start date (YYYY-MM-DD) for filtering
        end_date: End date (YYYY-MM-DD) for filtering
        politicians: List of politician records for name matching
        zip_content: ZIP file content as bytes (if already downloaded)
        zip_s3_key: S3 key of ZIP file (if exists in S3)
        start_date_dt: Start date as datetime object for filtering
        end_date_dt: End date as datetime object for filtering
        politicians_by_bioguide: Optional bioguide_id -> politician map for roll call indexing
        rolls_written_from_bills: Optional set of (congress_str, session, roll) written this run
        roll_write_lock: Optional lock when updating rolls_written_from_bills from multiple threads

    Returns:
        (number of XML files processed, number of bills stored)
    """
    bill_type_lower = bill_type.lower()
    log_print(f"📦 Processing bulk ZIP for Congress {congress}, Bill Type {bill_type}")
    
    # Get ZIP content once (needed for all batches)
    if zip_s3_key:
        # Download ZIP from S3 to memory
        zip_obj = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=zip_s3_key)
        zip_content = zip_obj['Body'].read()
    elif zip_content:
        pass  # Already have content
    else:
        log_print(f"   ❌ No ZIP content or S3 key provided")
        return 0, 0
    
    # Extract file list first (ZIP is not thread-safe for concurrent reads)
    zip_file = BytesIO(zip_content)
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        file_list = zip_ref.namelist()
        xml_file_list = [f for f in file_list if f.lower().endswith('.xml')]
        total_files = len(xml_file_list)
        log_print(f"📄 Found {total_files} XML file(s) to process")
    
    # Process in batches: extract batch sequentially, then parse batch in parallel
    batch_size = 100
    parse_workers = min(20, batch_size)
    log_print(f"🔄 Starting parallel parsing with {parse_workers} worker(s), processing in batches of {batch_size}...")
    
    total_stored = 0
    
    def parse_xml_file(filename: str, content: bytes):
        try:
            bill_record = parse_bill_xml(content, politicians)
            if bill_record:
                return bill_record.get('bill_id'), bill_record
            return None, None
        except Exception as e:
            log_print(f"   ⚠️ Error parsing {filename}: {str(e)[:200]}")
            return None, None
    
    # Process batches: extract sequentially, parse in parallel
    for batch_start in range(0, total_files, batch_size):
        batch_end = min(batch_start + batch_size, total_files)
        batch_files = xml_file_list[batch_start:batch_end]
        batch_num = (batch_start // batch_size) + 1
        total_batches = (total_files + batch_size - 1) // batch_size
        
        log_print(f"   📦 Processing batch {batch_num}/{total_batches} (files {batch_start+1}-{batch_end} of {total_files})...")
        
        # Initialize bills dict for this batch
        bills = {}
        
        # Extract batch from ZIP sequentially (ZIP not thread-safe)
        log_print(f"      📥 Extracting {len(batch_files)} files from ZIP...")
        batch_xml_data = {}
        zip_file_batch = BytesIO(zip_content)
        with zipfile.ZipFile(zip_file_batch, 'r') as zip_ref:
            for idx, filename in enumerate(batch_files):
                try:
                    xml_content = zip_ref.read(filename)
                    batch_xml_data[filename] = xml_content
                    if (idx + 1) % 20 == 0:
                        log_print(f"      📥 Extracted {idx + 1}/{len(batch_files)} files...")
                except Exception as e:
                    log_print(f"   ⚠️ Failed to extract {filename}: {str(e)[:200]}")
        
        log_print(f"      ✅ Extracted {len(batch_xml_data)} files, starting parallel parsing...")
        
        # Parse batch in parallel (now we have the data in memory)
        with ThreadPoolExecutor(max_workers=parse_workers) as executor:
            future_to_file = {
                executor.submit(parse_xml_file, filename, content): filename
                for filename, content in batch_xml_data.items()
            }
            
            processed_in_batch = 0
            for future in as_completed(future_to_file):
                filename = future_to_file[future]
                processed_in_batch += 1
                
                try:
                    bill_id, bill_record = future.result()
                    if bill_id and bill_record:
                        bills[bill_id] = bill_record
                except Exception as e:
                    log_print(f"   ⚠️ Error processing {filename}: {str(e)[:200]}")
                
                # Log progress every 10 files
                if processed_in_batch % 10 == 0:
                    log_print(f"      🔄 Parsed {processed_in_batch}/{len(batch_xml_data)} files in batch {batch_num}...")
        
        log_print(f"   ✅ Batch {batch_num} complete: {processed_in_batch} files processed, {len(bills)} bills parsed")
        
        # Filter batch by date if needed
        # For daily runs, filter by latest_action_date to catch bills with new actions
        # For historical runs, filter by introduced_date
        if start_date_dt and end_date_dt:
            filtered_batch = {}
            for bill_id, bill_record in bills.items():
                # Prefer latest_action_date for daily updates (catches bills with new actions)
                # Fall back to introduced_date if latest_action_date not available
                filter_date_str = bill_record.get('latest_action_date') or bill_record.get('introduced_date')
                if filter_date_str:
                    try:
                        # Parse date (handle both YYYY-MM-DD and ISO format)
                        if 'T' in filter_date_str:
                            bill_date = datetime.fromisoformat(filter_date_str.replace('Z', '+00:00'))
                        else:
                            bill_date = datetime.strptime(filter_date_str, '%Y-%m-%d')
                            bill_date = bill_date.replace(tzinfo=timezone.utc)
                        
                        if start_date_dt <= bill_date <= end_date_dt:
                            filtered_batch[bill_id] = bill_record
                    except (ValueError, AttributeError):
                        # If date parsing fails, include bill if it has no date filter
                        # (for backwards compatibility)
                        pass
                else:
                    # No date available - include in batch (for backwards compatibility)
                    filtered_batch[bill_id] = bill_record
            bills = filtered_batch
        
        # Store batch to DynamoDB immediately
        if bills and len(bills) > 0:
            log_print(f"   💾 Storing {len(bills)} bill(s) from batch {batch_num} to DynamoDB...")
            store_workers = min(20, len(bills))
            
            def store_bill(bill_id: str, bill_record: Dict):
                try:
                    store_bill_to_dynamodb(bill_record)
                    if roll_call_indexing and bill_record.get("recorded_votes_json") and politicians_by_bioguide is not None and rolls_written_from_bills is not None:
                        try:
                            roll_call_indexing.run_roll_call_delta_for_bill(
                                bill_id, bill_record, politicians, politicians_by_bioguide,
                                rolls_written_from_bills, roll_write_lock
                            )
                        except roll_call_indexing.RollCallIndexError as rc_err:
                            log_print(f"      [FAIL] Roll call vote index not created for {bill_id}: {rc_err}")
                            raise
                        except Exception as rc_err:
                            log_print(f"      ⚠️ Roll call delta for {bill_id}: {str(rc_err)[:200]}")
                    return True, None
                except Exception as e:
                    error_msg = f"Error storing {bill_id}: {str(e)[:200]}"
                    return False, error_msg
            
            stored_count = 0
            with ThreadPoolExecutor(max_workers=store_workers) as executor:
                future_to_bill = {
                    executor.submit(store_bill, bill_id, bill_record): bill_id
                    for bill_id, bill_record in bills.items()
                }
                
                for future in as_completed(future_to_bill):
                    bill_id = future_to_bill[future]
                    try:
                        success, error_msg = future.result()
                        if success:
                            stored_count += 1
                        else:
                            if error_msg:
                                log_print(f"      ❌ {error_msg}")
                    except Exception as e:
                        if roll_call_indexing and isinstance(e, roll_call_indexing.RollCallIndexError):
                            log_print(f"      ❌ [FAIL] Roll call vote index not created: {e}")
                            raise
                        log_print(f"      ❌ Exception storing {bill_id}: {str(e)[:200]}")
            
            log_print(f"   ✅ Stored {stored_count}/{len(bills)} bill(s) from batch {batch_num} to DynamoDB")
            total_stored += stored_count
        
        # Clear batch data from memory before next batch
        del bills
        del batch_xml_data
        del zip_file_batch
        import gc
        gc.collect()
        log_print(f"   🧹 Memory cleared after batch {batch_num}")
    
    log_print(f"✅ Completed processing {total_files} XML file(s), stored {total_stored} bill(s)")
    
    return total_files, total_stored


# ============================================================================
# Main Execution
# ============================================================================

def main():
    log_print("=" * 80)
    log_print("Congress.gov Bill Fetcher Glue Job - Starting")
    log_print("=" * 80)
    
    if not BILLS_TABLE_NAME:
        raise ValueError("BILLS_TABLE_NAME job parameter not set")
    
    # API keys not required for bulk downloads (public data)
    # But keep initialization optional for backward compatibility
    api_key = None
    try:
        rotator = get_congress_api_keys()
        api_key = rotator.get_key()  # Get one key for backward compatibility
        log_print(f"✅ Initialized API key rotator with {rotator.get_key_count()} key(s) (optional for bulk downloads)")
    except Exception as e:
        log_print(f"ℹ️ API keys not available (not required for bulk downloads): {str(e)}")
        # Don't raise - bulk downloads don't need API keys
    
    # Parse date parameters
    congress = args.get("CONGRESS")
    start_date_str = args.get("START_DATE")
    end_date_str = args.get("END_DATE")
    source = args.get("SOURCE")
    
    # Handle null/empty values (step function may pass "null" as string or empty string)
    if start_date_str in [None, "", "null", "None"]:
        start_date_str = None
    if end_date_str in [None, "", "null", "None"]:
        end_date_str = None
    
    # Calculate date range based on source
    scheduler_mode = False
    if source == "scheduler":
        # If source is "scheduler", calculate previous day 11:00 AM UTC to current day 11:00 AM UTC
        now = datetime.now(timezone.utc)

        # Calculate previous day 11:00 AM UTC
        previous_day_11am = now.replace(hour=11, minute=0, second=0, microsecond=0) - timedelta(days=1)

        # Calculate current day 11:00 AM UTC
        current_day_11am = now.replace(hour=11, minute=0, second=0, microsecond=0)

        # If current time is before 11:00 AM UTC, use previous day 11:00 AM to previous day 11:00 AM (same day)
        # Otherwise use previous day 11:00 AM to current day 11:00 AM
        if now.hour < 11:
            # Before 11:00 AM UTC, fetch previous day 11:00 AM to previous day 11:00 AM (same day)
            start_date = previous_day_11am
            end_date = previous_day_11am
        else:
            # At or after 11:00 AM UTC, fetch previous day 11:00 AM to current day 11:00 AM
            start_date = previous_day_11am
            end_date = current_day_11am

        # Format directly as ISO (skip mm/dd/yyyy parsing)
        start_date_str = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_date_str = end_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        scheduler_mode = True
        log_print(f"📅 Source is scheduler, calculating date range: {start_date_str} to {end_date_str} (previous day 11:00 AM UTC to current day 11:00 AM UTC)")

    # Parse and normalize dates (handle both mm/dd/yyyy and ISO formats)
    if start_date_str and end_date_str and not scheduler_mode:
        # Try parsing as mm/dd/yyyy first (router Lambda format)
        try:
            start_date = datetime.strptime(start_date_str, "%m/%d/%Y")
            end_date = datetime.strptime(end_date_str, "%m/%d/%Y")
            # Set timezone to UTC
            start_date = start_date.replace(tzinfo=timezone.utc)
            end_date = end_date.replace(tzinfo=timezone.utc)
            # Set start_date to beginning of day (00:00:00)
            start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
            # Set end_date to end of day (23:59:59)
            end_date = end_date.replace(hour=23, minute=59, second=59, microsecond=999999)
            # Convert to ISO format for API
            start_date_str = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
            end_date_str = end_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        except ValueError:
            # If mm/dd/yyyy parsing fails, assume it's already in ISO format
            try:
                # Validate ISO format (handle both with and without Z)
                if start_date_str.endswith('Z'):
                    start_date_parsed = datetime.fromisoformat(start_date_str.replace('Z', '+00:00'))
                else:
                    start_date_parsed = datetime.fromisoformat(start_date_str)
                if end_date_str.endswith('Z'):
                    end_date_parsed = datetime.fromisoformat(end_date_str.replace('Z', '+00:00'))
                else:
                    end_date_parsed = datetime.fromisoformat(end_date_str)
                # Already in ISO format, use as-is
                pass
            except ValueError:
                raise ValueError(f"Invalid date format. Expected mm/dd/yyyy or ISO format, got: {start_date_str} or {end_date_str}")
    elif not scheduler_mode:
        # Calculate date range if not provided (default to last 7 days for Glue)
        # Skip this if we're in scheduler mode (dates already set)
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=7)
        start_date_str = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_date_str = end_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        log_print(f"⚠️ Date range not provided, using last 7 days: {start_date_str} to {end_date_str}")
    
    log_print(f"📅 Date Range: {start_date_str} to {end_date_str}")
    
    # Parse dates to datetime objects for congress calculation
    start_date_dt = None
    end_date_dt = None
    if start_date_str and end_date_str:
        try:
            if start_date_str.endswith('Z'):
                start_date_dt = datetime.fromisoformat(start_date_str.replace('Z', '+00:00'))
            else:
                start_date_dt = datetime.fromisoformat(start_date_str)
            
            if end_date_str.endswith('Z'):
                end_date_dt = datetime.fromisoformat(end_date_str.replace('Z', '+00:00'))
            else:
                end_date_dt = datetime.fromisoformat(end_date_str)
        except (ValueError, AttributeError):
            log_print(f"⚠️ Could not parse dates for congress calculation, using provided congress or current")
    
    # Get Congress number(s) - auto-detect from date range if not provided
    congresses_to_query = []
    if congress:
        # Use provided congress
        congresses_to_query = [int(congress)]
        log_print(f"✅ Using provided Congress: {congress}")
    elif start_date_dt and end_date_dt:
        # Auto-detect congresses from date range
        congresses_to_query = get_congresses_from_date_range(start_date_dt, end_date_dt)
        log_print(f"✅ Auto-detected Congress(es) from date range: {congresses_to_query}")
    else:
        # Fallback to current congress (calculate from current date)
        current_year = datetime.now().year
        current_congress = ((current_year - 1789) // 2) + 1
        congresses_to_query = [current_congress]
        log_print(f"✅ Using current Congress: {current_congress} (calculated from current year)")
    
    log_print("")  # Empty line for readability
    
    # Load politician CSV data for name matching
    log_print("📋 Loading politician CSV data for name matching...")
    politicians = load_legislators_csv()
    log_print(f"✅ Loaded {len(politicians)} politician records")
    politicians_by_bioguide = {}
    for p in politicians or []:
        bid = (p.get("bioguide_id") or "").strip()
        if bid:
            politicians_by_bioguide[bid.upper()] = p
    log_print("")  # Empty line for readability

    # API keys for roll call indexing (house-vote members API)
    api_key_rotator = None
    try:
        api_key_rotator = get_congress_api_keys()
        log_print(f"✅ Initialized API key rotator with {api_key_rotator.get_key_count()} key(s) (for roll call indexing)")
    except Exception as e:
        log_print(f"ℹ️ API keys not available (roll call indexing will be skipped): {str(e)}")

    if roll_call_indexing and bills_table and api_key_rotator:
        roll_call_indexing.set_context({
            "table": bills_table,
            "s3_client": s3_client,
            "bucket_name": S3_BUCKET_NAME or "",
            "api_base_url": API_BASE_URL or "https://api.congress.gov/v3",
            "get_api_key": lambda: (api_key_rotator.get_key(), 0),
            "log_print": log_print,
            "request_timeout": REQUEST_TIMEOUT,
        })
        roll_call_indexing.ensure_search_vote_items_for_legislators(politicians)
        log_print("✅ Roll call indexing context set and SEARCH#VOTE items ensured")
    else:
        if not roll_call_indexing:
            log_print("ℹ️ roll_call_indexing module not available; skipping roll call indexing")
        elif not api_key_rotator:
            log_print("ℹ️ API keys not available; skipping roll call indexing")

    rolls_written_from_bills = set()
    roll_write_lock = threading.Lock()

    # Convert date strings to YYYY-MM-DD format for S3 path
    start_date_simple = start_date_str.split('T')[0] if start_date_str else None
    end_date_simple = end_date_str.split('T')[0] if end_date_str else None
    
    if not start_date_simple or not end_date_simple:
        raise ValueError("Start date and end date are required for bulk downloads")
    
    # Process bills using bulk downloads (follow govt_contracts pattern: download, parse, clear, repeat)
    log_print("📦 Processing Bills via Bulk Downloads...")
    log_print("-" * 80)
    log_print(f"   📅 Date Range: {start_date_simple} to {end_date_simple}")
    log_print(f"   📋 Congress(es): {congresses_to_query}")
    log_print(f"   📋 Bill Types: {BILL_TYPES}")
    log_print("")  # Empty line for readability
    
    processed_count = 0
    error_count = 0
    total_bills_processed = 0
    
    # Process each Congress and bill type separately (clear memory between each)
    for congress_num in congresses_to_query:
        log_print(f"\n{'=' * 80}")
        log_print(f"📋 Processing Congress {congress_num}")
        log_print(f"{'=' * 80}")
        
        for bill_type in BILL_TYPES:
            log_print(f"\n{'─' * 80}")
            log_print(f"📦 Processing {bill_type} bills for Congress {congress_num}")
            log_print(f"{'─' * 80}")
            
            try:
                # Check if ZIP exists in S3
                zip_s3_key = check_s3_zip_exists(start_date_simple, end_date_simple, congress_num, bill_type)
                
                zip_content = None
                if zip_s3_key:
                    log_print(f"✅ Using existing ZIP file from S3: {zip_s3_key}")
                else:
                    # Download ZIP from bulk data repository
                    log_print(f"📥 Downloading ZIP file from bulk data repository...")
                    zip_content = download_bulk_zip(congress_num, bill_type)
                    
                    if zip_content:
                        # Save ZIP to S3
                        zip_s3_key = save_zip_to_s3(zip_content, start_date_simple, end_date_simple, congress_num, bill_type)
                        log_print(f"✅ Downloaded and saved ZIP file ({len(zip_content):,} bytes)")
                    else:
                        log_print(f"⚠️ No ZIP file available for Congress {congress_num}, Bill Type {bill_type}")
                        continue
                
                # Process ZIP file: extract, parse, store each batch immediately
                log_print(f"📄 Processing ZIP file...")
                try:
                    files_processed, bills_stored = process_bulk_zip_file(
                        congress_num, bill_type, start_date_simple, end_date_simple,
                        politicians, zip_content=zip_content, zip_s3_key=zip_s3_key,
                        start_date_dt=start_date_dt, end_date_dt=end_date_dt,
                        politicians_by_bioguide=politicians_by_bioguide,
                        rolls_written_from_bills=rolls_written_from_bills,
                        roll_write_lock=roll_write_lock,
                    )
                    log_print(f"📊 Processing complete: {files_processed} XML file(s) processed, {bills_stored} bill(s) stored for {bill_type} in Congress {congress_num}")
                    processed_count += bills_stored
                    total_bills_processed += bills_stored
                except Exception as e:
                    error_msg = f"❌ Error processing ZIP file for {bill_type} in Congress {congress_num}: {str(e)[:300]}"
                    log_print(error_msg)
                    logger.error(error_msg, exc_info=True)
                    error_count += 1
                    continue
                
                if zip_content:
                    del zip_content
                gc.collect()
                log_print(f"🧹 Memory cleared after processing {bill_type}")
                
            except Exception as e:
                error_count += 1
                error_msg = f"Error processing {bill_type} for Congress {congress_num}: {str(e)[:300]}"
                log_print(f"❌ {error_msg}")
                logger.error(error_msg, exc_info=True)
                # Continue with next bill type
                continue
    
    if roll_call_indexing and politicians_by_bioguide and set(congresses_to_query):
        log_print("")
        log_print("🔄 Running house-vote second pass (SEARCH#ROLL for rolls not tied to bills)...")
        try:
            roll_call_indexing.run_house_vote_second_pass(
                set(congresses_to_query), politicians, politicians_by_bioguide, rolls_written_from_bills
            )
            log_print("✅ House-vote second pass complete")
        except roll_call_indexing.RollCallIndexError as e:
            log_print(f"❌ [FAIL] Roll call vote index not created (second pass): {e}")
            logger.error(f"Roll call vote index not created: {e}", exc_info=True)
            raise
        except Exception as e:
            log_print(f"⚠️ House-vote second pass error: {str(e)[:300]}")
            logger.exception("House-vote second pass failed")

    log_print(f"\n{'=' * 80}")
    log_print(f"✅ Bulk Download Processing Complete")
    log_print(f"{'=' * 80}")
    log_print(f"   📊 Total Bills Processed: {total_bills_processed}")
    log_print(f"   ✅ Successfully Stored: {processed_count}")
    if error_count > 0:
        log_print(f"   ⚠️ Errors: {error_count}")
    
    log_print("")  # Empty line for readability
    log_print("=" * 80)
    log_print("✅ Glue job completed successfully!")
    log_print("=" * 80)
    
    job.commit()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        error_msg = f"CRITICAL ERROR in Congress.gov bill fetcher job: {str(e)}"
        error_traceback = traceback.format_exc()
        log_print(f"❌ {error_msg}")
        log_print(f"❌ Traceback:\n{error_traceback}")
        logger.error(f"❌ {error_msg}", exc_info=True)
        logger.error(f"❌ Traceback:\n{error_traceback}")
        # Re-raise to trigger Glue job failure
        raise


