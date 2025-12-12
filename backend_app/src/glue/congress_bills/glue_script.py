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
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional, Tuple
from io import StringIO
from difflib import SequenceMatcher
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

# Get optional date parameters (parse manually to avoid errors if not provided)
# getResolvedOptions requires all arguments, so we parse manually for optional ones
optional_params = ['CONGRESS', 'START_DATE', 'END_DATE', 'POLITICIAN_TRADES_S3_BUCKET', 'SOURCE']
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
    Load congress-legislators CSV from S3
    
    Returns:
        List of politician dicts with name, party, state, district, position, and alternativeNames
    """
    try:
        if not POLITICIAN_TRADES_S3_BUCKET:
            log_print("⚠️ POLITICIAN_TRADES_S3_BUCKET not set, skipping CSV load")
            return []
        
        # Download congress-legislators.csv from S3
        response = s3_client.get_object(
            Bucket=POLITICIAN_TRADES_S3_BUCKET,
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
POLITICIAN_TRADES_S3_BUCKET = args.get('POLITICIAN_TRADES_S3_BUCKET')
REQUEST_TIMEOUT = int(args.get('REQUEST_TIMEOUT', '30'))
MAX_RETRIES = int(args.get('MAX_RETRIES', '5'))
RETRY_DELAY = int(args.get('RETRY_DELAY', '2'))

# Name matching threshold (0.0 to 1.0)
NAME_MATCH_THRESHOLD = 0.85  # 85% similarity

# Bill types to fetch
BILL_TYPES = ["HR", "S", "HJRES", "SJRES", "HCONRES", "SCONRES", "HRES", "SRES"]

# AWS clients
dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')
secrets_client = boto3.client('secretsmanager')
bills_table = dynamodb.Table(BILLS_TABLE_NAME) if BILLS_TABLE_NAME else None

# ============================================================================
# Helper Functions (same as Lambda)
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

def get_current_congress(api_key: str) -> int:
    """Get the current Congress number."""
    url = f"{API_BASE_URL}/congress"
    params = {"api_key": api_key, "format": "json"}
    
    try:
        response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        
        # Handle different response structures
        congresses = None
        if isinstance(data, list):
            congresses = data
        elif "congresses" in data:
            congresses_data = data["congresses"]
            if isinstance(congresses_data, dict):
                congresses = congresses_data.get("item", [])
            elif isinstance(congresses_data, list):
                congresses = congresses_data
        
        if not congresses:
            current_year = datetime.now().year
            congress = ((current_year - 1789) // 2) + 1
            log_print(f"⚠️ Could not fetch Congress list, using calculated value: {congress}")
            return congress
        
        sorted_congresses = sorted(congresses, key=lambda x: x.get("number", 0) if isinstance(x, dict) else (x if isinstance(x, (int, str)) else 0), reverse=True)
        if not sorted_congresses:
            raise ValueError("No congresses found")
        
        first_congress = sorted_congresses[0]
        if isinstance(first_congress, dict):
            current_congress = first_congress.get("number")
        elif isinstance(first_congress, (int, str)):
            current_congress = int(first_congress)
        else:
            raise ValueError(f"Unexpected congress format: {first_congress}")
        
        if current_congress is None:
            raise ValueError("Congress number is None")
        
        log_print(f"✅ Current Congress: {current_congress}")
        return int(current_congress)
    except Exception as e:
        log_print(f"⚠️ Error fetching current Congress: {e}. Using calculated value.")
        current_year = datetime.now().year
        congress = ((current_year - 1789) // 2) + 1
        return congress


def make_api_request(url: str, params: Dict[str, Any], api_key: str, retries: int = MAX_RETRIES) -> Optional[Dict]:
    """Make API request with retry logic."""
    # Ensure API key is in params
    if "api_key" not in params:
        params["api_key"] = api_key
    """Make API request with retry logic."""
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


def fetch_bills_list(congress: int, bill_type: str, from_date: str, to_date: str, api_key: str) -> List[Dict]:
    """Fetch list of bills for a specific type and date range."""
    bills = []
    offset = 0
    limit = 250
    
    while True:
        url = f"{API_BASE_URL}/bill/{congress}/{bill_type.lower()}"
        params = {
            "format": "json",
            "offset": offset,
            "limit": limit,
            "fromDateTime": from_date,
            "toDateTime": to_date
        }
        
        data = make_api_request(url, params, api_key)
        if not data:
            break
        
        # Handle different response structures
        bill_list = []
        if isinstance(data, list):
            bill_list = data
        elif "bills" in data:
            bills_data = data["bills"]
            if isinstance(bills_data, dict):
                bill_list = bills_data.get("bill", [])
            elif isinstance(bills_data, list):
                bill_list = bills_data
        
        if not bill_list:
            break
        
        bills.extend(bill_list)
        
        # Check pagination
        pagination = {}
        if isinstance(data, dict):
            if "bills" in data and isinstance(data["bills"], dict):
                pagination = data["bills"].get("pagination", {})
            elif "pagination" in data:
                pagination = data["pagination"]
        
        count = pagination.get("count", 0) if pagination else 0
        if count > 0 and offset + limit >= count:
            break
        
        offset += limit
        time.sleep(0.3)  # Rate limiting
    
    return bills


def fetch_bill_details(congress: int, bill_type: str, bill_number: int, api_key: str) -> Optional[Dict]:
    """Fetch full bill details."""
    url = f"{API_BASE_URL}/bill/{congress}/{bill_type.lower()}/{bill_number}"
    params = {"format": "json"}
    
    data = make_api_request(url, params, api_key)
    if data and isinstance(data, dict):
        return data.get("bill", data)
    return data


def fetch_bill_actions(congress: int, bill_type: str, bill_number: int, api_key: str) -> List[Dict]:
    """Fetch all actions for a bill."""
    actions = []
    offset = 0
    limit = 250
    
    while True:
        url = f"{API_BASE_URL}/bill/{congress}/{bill_type.lower()}/{bill_number}/actions"
        params = {
            "format": "json",
            "offset": offset,
            "limit": limit
        }
        
        data = make_api_request(url, params, api_key)
        if not data:
            break
        
        action_list = []
        if isinstance(data, list):
            action_list = data
        elif "actions" in data:
            actions_data = data["actions"]
            if isinstance(actions_data, dict):
                action_list = actions_data.get("item", [])
            elif isinstance(actions_data, list):
                action_list = actions_data
        
        if not action_list:
            break
        
        actions.extend(action_list)
        
        # Check pagination
        pagination = {}
        if isinstance(data, dict):
            if "actions" in data and isinstance(data["actions"], dict):
                pagination = data["actions"].get("pagination", {})
            elif "pagination" in data:
                pagination = data["pagination"]
        
        count = pagination.get("count", 0) if pagination else 0
        if count > 0 and offset + limit >= count:
            break
        
        offset += limit
        time.sleep(0.2)
    
    return actions


def fetch_bill_amendments(congress: int, bill_type: str, bill_number: int, api_key: str) -> List[Dict]:
    """Fetch all amendments for a bill."""
    amendments = []
    offset = 0
    limit = 250
    
    while True:
        url = f"{API_BASE_URL}/bill/{congress}/{bill_type.lower()}/{bill_number}/amendments"
        params = {
            "format": "json",
            "offset": offset,
            "limit": limit
        }
        
        data = make_api_request(url, params, api_key)
        if not data:
            break
        
        amendment_list = []
        if isinstance(data, list):
            amendment_list = data
        elif "amendments" in data:
            amendments_data = data["amendments"]
            if isinstance(amendments_data, dict):
                amendment_list = amendments_data.get("amendment", [])
            elif isinstance(amendments_data, list):
                amendment_list = amendments_data
        
        if not amendment_list:
            break
        
        amendments.extend(amendment_list)
        
        # Check pagination
        pagination = {}
        if isinstance(data, dict):
            if "amendments" in data and isinstance(data["amendments"], dict):
                pagination = data["amendments"].get("pagination", {})
            elif "pagination" in data:
                pagination = data["pagination"]
        
        count = pagination.get("count", 0) if pagination else 0
        if count > 0 and offset + limit >= count:
            break
        
        offset += limit
        time.sleep(0.2)
    
    return amendments


def fetch_bill_cosponsors(congress: int, bill_type: str, bill_number: int, api_key: str) -> List[Dict]:
    """Fetch all cosponsors for a bill."""
    cosponsors = []
    offset = 0
    limit = 250
    
    while True:
        url = f"{API_BASE_URL}/bill/{congress}/{bill_type.lower()}/{bill_number}/cosponsors"
        params = {
            "format": "json",
            "offset": offset,
            "limit": limit
        }
        
        data = make_api_request(url, params, api_key)
        if not data:
            break
        
        cosponsor_list = []
        if isinstance(data, list):
            cosponsor_list = data
        elif "cosponsors" in data:
            cosponsors_data = data["cosponsors"]
            if isinstance(cosponsors_data, dict):
                cosponsor_list = cosponsors_data.get("item", [])
            elif isinstance(cosponsors_data, list):
                cosponsor_list = cosponsors_data
        
        if not cosponsor_list:
            break
        
        cosponsors.extend(cosponsor_list)
        
        # Check pagination
        pagination = {}
        if isinstance(data, dict):
            if "cosponsors" in data and isinstance(data["cosponsors"], dict):
                pagination = data["cosponsors"].get("pagination", {})
            elif "pagination" in data:
                pagination = data["pagination"]
        
        count = pagination.get("count", 0) if pagination else 0
        if count > 0 and offset + limit >= count:
            break
        
        offset += limit
        time.sleep(0.2)
    
    return cosponsors


def fetch_bill_summaries(congress: int, bill_type: str, bill_number: int, api_key: str) -> List[Dict]:
    """Fetch all summaries for a bill."""
    summaries = []
    offset = 0
    limit = 250
    
    while True:
        url = f"{API_BASE_URL}/bill/{congress}/{bill_type.lower()}/{bill_number}/summaries"
        params = {
            "format": "json",
            "offset": offset,
            "limit": limit
        }
        
        data = make_api_request(url, params, api_key)
        if not data:
            break
        
        summary_list = []
        if isinstance(data, list):
            summary_list = data
        elif "summaries" in data:
            summaries_data = data["summaries"]
            if isinstance(summaries_data, dict):
                summary_list = summaries_data.get("summary", [])
            elif isinstance(summaries_data, list):
                summary_list = summaries_data
        
        if not summary_list:
            break
        
        summaries.extend(summary_list)
        
        # Check pagination
        pagination = {}
        if isinstance(data, dict):
            if "summaries" in data and isinstance(data["summaries"], dict):
                pagination = data["summaries"].get("pagination", {})
            elif "pagination" in data:
                pagination = data["pagination"]
        
        count = pagination.get("count", 0) if pagination else 0
        if count > 0 and offset + limit >= count:
            break
        
        offset += limit
        time.sleep(0.2)
    
    return summaries


def fetch_bill_subjects(congress: int, bill_type: str, bill_number: int, api_key: str) -> Dict:
    """Fetch subjects for a bill."""
    url = f"{API_BASE_URL}/bill/{congress}/{bill_type.lower()}/{bill_number}/subjects"
    params = {"format": "json"}
    
    data = make_api_request(url, params, api_key)
    if data and isinstance(data, dict):
        return data.get("subjects", {})
    return {}

def fetch_bill_titles(congress: int, bill_type: str, bill_number: int, api_key: str) -> List[Dict]:
    """Fetch all titles for a bill, including official titles."""
    titles = []
    offset = 0
    limit = 250
    
    while True:
        url = f"{API_BASE_URL}/bill/{congress}/{bill_type.lower()}/{bill_number}/titles"
        params = {
            "format": "json",
            "offset": offset,
            "limit": limit
        }
        
        data = make_api_request(url, params, api_key)
        if not data:
            break
        
        title_list = []
        if isinstance(data, list):
            title_list = data
        elif "titles" in data:
            titles_data = data["titles"]
            if isinstance(titles_data, dict):
                title_list = titles_data.get("item", [])
            elif isinstance(titles_data, list):
                title_list = titles_data
        
        if not title_list:
            break
        
        titles.extend(title_list)
        
        # Check pagination
        pagination = {}
        if isinstance(data, dict):
            if "titles" in data and isinstance(data["titles"], dict):
                pagination = data["titles"].get("pagination", {})
            elif "pagination" in data:
                pagination = data["pagination"]
        
        count = pagination.get("count", 0) if pagination else 0
        if count > 0 and offset + limit >= count:
            break
        
        # If we got fewer items than limit, we're done
        if len(title_list) < limit:
            break
        
        offset += limit
    
    return titles


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
    """
    Download bill text file (XML/HTML) from Congress.gov.
    
    Args:
        text_url: URL to the bill text file (e.g., https://www.congress.gov/119/bills/hr303/BILLS-119hr303ih.xml)
        retries: Number of retry attempts
        
    Returns:
        File content as bytes, or None if download failed
    """
    for attempt in range(retries):
        try:
            response = requests.get(text_url, timeout=REQUEST_TIMEOUT * 2)  # Longer timeout for file downloads
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
    """
    Store bill text file to S3 in bill_text/ folder.
    
    Args:
        bill_id: Bill ID (e.g., "119-HR-303")
        text_content: Bill text content as bytes
        text_version_type: Type of text version (e.g., "introduced", "enrolled")
        
    Returns:
        S3 key where the file was stored
    """
    # Determine file extension based on content or default to .xml
    # Most bill text files are XML, but some might be HTML
    file_ext = ".xml"
    if text_content.startswith(b'<!DOCTYPE html') or text_content.startswith(b'<html'):
        file_ext = ".html"
    
    # Create S3 key: bill_text/{bill_id}_{text_version_type}{ext}
    # Clean text_version_type to be filesystem-safe
    safe_version_type = re.sub(r'[^a-zA-Z0-9_-]', '_', text_version_type.lower())
    s3_key = f"bill_text/{bill_id}_{safe_version_type}{file_ext}"
    
    # Upload to S3 (S3 bucket encryption is handled at bucket level via Terraform)
    s3_client.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=s3_key,
        Body=text_content,
        ContentType='application/xml' if file_ext == '.xml' else 'text/html'
    )
    
    log_print(f"      💾 Stored bill text for {bill_id} to S3: {s3_key} ({len(text_content):,} bytes)")
    return s3_key


def build_comprehensive_bill_record(bill: Dict, congress: int, bill_type: str, api_key: str) -> Optional[Dict]:
    """
    Build a comprehensive bill record with all related data.
    One row per bill with all information.
    Uses parallel API calls to speed up processing.
    """
    bill_number = bill.get("number")
    if not bill_number:
        return None
    
    import threading
    thread_id = threading.current_thread().name
    log_print(f"      📋 [{thread_id}] Processing {bill_type} {bill_number}...")
    
    # Fetch all related data in parallel (8 API calls including text versions)
    log_print(f"      🔄 [{thread_id}] Starting 8 parallel API calls for {bill_type} {bill_number}...")
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {
            executor.submit(fetch_bill_details, congress, bill_type, bill_number, api_key): 'details',
            executor.submit(fetch_bill_actions, congress, bill_type, bill_number, api_key): 'actions',
            executor.submit(fetch_bill_amendments, congress, bill_type, bill_number, api_key): 'amendments',
            executor.submit(fetch_bill_cosponsors, congress, bill_type, bill_number, api_key): 'cosponsors',
            executor.submit(fetch_bill_summaries, congress, bill_type, bill_number, api_key): 'summaries',
            executor.submit(fetch_bill_subjects, congress, bill_type, bill_number, api_key): 'subjects',
            executor.submit(fetch_bill_titles, congress, bill_type, bill_number, api_key): 'titles',
            executor.submit(fetch_bill_text_versions, congress, bill_type, bill_number, api_key): 'text_versions',
        }
        
        results = {}
        for future in as_completed(futures):
            key = futures[future]
            try:
                results[key] = future.result()
            except Exception as e:
                log_print(f"      ⚠️ Error fetching {key} for {bill_type} {bill_number}: {str(e)}")
                results[key] = None
        
        details = results.get('details')
        actions = results.get('actions', [])
        amendments = results.get('amendments', [])
        cosponsors = results.get('cosponsors', [])
        summaries = results.get('summaries', [])
        subjects = results.get('subjects', [])
        titles = results.get('titles', [])
        text_versions = results.get('text_versions', [])
    
    # Extract primary sponsor (first sponsor from details)
    primary_sponsor = {}
    if details:
        sponsors = details.get("sponsors", {})
        if isinstance(sponsors, dict):
            sponsor_items = sponsors.get("item", [])
            if isinstance(sponsor_items, list) and len(sponsor_items) > 0:
                primary_sponsor = sponsor_items[0]
            elif isinstance(sponsor_items, dict):
                primary_sponsor = sponsor_items
        elif isinstance(sponsors, list) and len(sponsors) > 0:
            primary_sponsor = sponsors[0]
    
    # Fetch and store bill text (prefer "Introduced in House/Senate" version)
    bill_text_s3_key = ""
    if text_versions:
        # Find the "Introduced" version first, fallback to first available
        introduced_version = None
        for version in text_versions:
            version_type = version.get("type", "").lower()
            if "introduced" in version_type:
                introduced_version = version
                break
        
        # Use introduced version if found, otherwise use first version
        selected_version = introduced_version if introduced_version else text_versions[0]
        
        # Get the Formatted XML URL (preferred format)
        formats = selected_version.get("formats", {})
        if isinstance(formats, dict):
            format_items = formats.get("item", [])
            if isinstance(format_items, list):
                for fmt_item in format_items:
                    if fmt_item.get("type") == "Formatted XML":
                        text_url = fmt_item.get("url")
                        if text_url:
                            log_print(f"      📄 [{thread_id}] Downloading bill text from {text_url}...")
                            text_content = download_bill_text_file(text_url)
                            if text_content:
                                version_type_name = selected_version.get("type", "introduced")
                                bill_text_s3_key = store_bill_text_to_s3(
                                    f"{congress}-{bill_type}-{bill_number}",
                                    text_content,
                                    version_type_name
                                )
                                log_print(f"      ✅ [{thread_id}] Stored bill text to S3: {bill_text_s3_key}")
                            break
    
    # Build comprehensive record
    record = {
        # Bill Basic Info
        "bill_id": f"{congress}-{bill_type}-{bill_number}",
        "congress": congress,
        "bill_type": bill_type,
        "bill_number": int(bill_number) if bill_number else 0,  # Store as integer for GSI
        "bill_title": bill.get("title") or (details.get("title") if details else ""),
        "bill_url": bill.get("url") or (details.get("url") if details else ""),
        
        # Bill Text S3 Key
        "bill_text_s3_key": bill_text_s3_key,
        
        # Dates (for sorting)
        "introduced_date": details.get("introducedDate") if details else bill.get("introducedDate", ""),
        "latest_action_date": bill.get("latestAction", {}).get("actionDate") if isinstance(bill.get("latestAction"), dict) else (details.get("latestAction", {}).get("actionDate") if details and isinstance(details.get("latestAction"), dict) else ""),
        "update_date": bill.get("updateDate", ""),
        "update_date_including_text": bill.get("updateDateIncludingText", ""),
        
        # Primary Sponsor (for sorting by proposer/party)
        "sponsor_bioguide_id": primary_sponsor.get("bioguideId", ""),
        "sponsor_full_name": primary_sponsor.get("fullName", ""),
        "sponsor_first_name": primary_sponsor.get("firstName", ""),
        "sponsor_last_name": primary_sponsor.get("lastName", ""),
        "sponsor_party": primary_sponsor.get("party", ""),  # For party sorting
        "sponsor_state": primary_sponsor.get("state", ""),
        "sponsor_district": primary_sponsor.get("district", ""),
        "sponsor_url": primary_sponsor.get("url", ""),
        
        # Cosponsors (will be populated with matched CSV data later)
        "cosponsor_count": len(cosponsors),
        "cosponsors": "",  # Will be populated as JSON array with matched CSV data
        "cosponsor_parties": "|".join([c.get("party", "") for c in cosponsors if c.get("party")]),  # Temporary, will be updated
        "cosponsors_json": json.dumps(cosponsors) if cosponsors else "",
        
        # Actions (full history - JSON for detailed access)
        "action_count": len(actions),
        "actions_json": json.dumps(actions) if actions else "",
        "actions_summary": " | ".join([f"{a.get('actionDate', '')}: {a.get('text', '')[:100]}" for a in actions[:10]]) if actions else "",
        
        # Amendments (with sponsors - who's amending it)
        "amendment_count": len(amendments),
        "amendment_numbers": "|".join([str(a.get("number", "")) for a in amendments if a.get("number")]),
        "amendment_sponsors": "|".join([
            a.get("sponsors", {}).get("item", [{}])[0].get("fullName", "") if isinstance(a.get("sponsors"), dict) and a.get("sponsors", {}).get("item") else ""
            for a in amendments
        ]),
        "amendments_json": json.dumps(amendments) if amendments else "",
        
        # Summaries (for text search)
        "summary_count": len(summaries),
        "summary_text": "",  # Will be populated below with fallback logic
        "summaries_json": json.dumps(summaries) if summaries else "",
        
        # Subjects
        "subjects_json": json.dumps(subjects) if subjects else "",
        "policy_area": "",
        "legislative_subjects": "",
        
        # Additional bill info
        "origin_chamber": bill.get("originChamber") or (details.get("originChamber") if details else ""),
        "origin_chamber_code": bill.get("originChamberCode") or (details.get("originChamberCode") if details else ""),
        "latest_action_text": bill.get("latestAction", {}).get("text", "") if isinstance(bill.get("latestAction"), dict) else (details.get("latestAction", {}).get("text", "") if details and isinstance(details.get("latestAction"), dict) else ""),
        
        # Metadata
        "indexed_at": datetime.now(timezone.utc).isoformat(),
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "data_source": "congress_gov_api",
        "api_version": "v3",
    }
    
    # Parse subjects (handle different response structures)
    if isinstance(subjects, dict):
        # Parse policy area
        policy_area_data = subjects.get("policyArea", {})
        if isinstance(policy_area_data, dict):
            record["policy_area"] = policy_area_data.get("name", "")
        elif isinstance(policy_area_data, str):
            record["policy_area"] = policy_area_data
        
        # Parse legislative subjects
        legislative_subjects_data = subjects.get("legislativeSubjects", {})
        subject_names = []
        if isinstance(legislative_subjects_data, dict):
            items = legislative_subjects_data.get("item", [])
            if isinstance(items, list):
                subject_names = [s.get("name", "") for s in items if isinstance(s, dict) and s.get("name")]
            elif isinstance(items, dict):
                subject_names = [items.get("name", "")] if items.get("name") else []
        elif isinstance(legislative_subjects_data, list):
            subject_names = [s.get("name", "") for s in legislative_subjects_data if isinstance(s, dict) and s.get("name")]
        
        record["legislative_subjects"] = "|".join(subject_names)
    
    # Ensure policy_area is not empty (DynamoDB GSI hash key cannot be empty string)
    if not record.get("policy_area") or record.get("policy_area").strip() == "":
        record["policy_area"] = "Other"
    
    # Build summary_text with fallback logic
    # Priority: 1) Summaries, 2) Official Title as Introduced, 3) Display Title
    summary_text = ""
    if summaries and len(summaries) > 0:
        # Use summaries if available
        summary_text = " ".join([
            s.get("text", "").replace("<p>", " ").replace("</p>", " ").replace("<strong>", "").replace("</strong>", "")
            for s in summaries if s.get("text")
        ])
    elif titles:
        # Fallback to Official Title as Introduced (titleTypeCode 6)
        official_title = None
        for title_item in titles:
            if isinstance(title_item, dict):
                title_type_code = title_item.get("titleTypeCode")
                if title_type_code == 6 or (isinstance(title_type_code, str) and title_type_code == "6"):
                    official_title = title_item.get("title", "")
                    break
        
        if official_title:
            summary_text = official_title
        else:
            # Use first available title as last resort
            for title_item in titles:
                if isinstance(title_item, dict):
                    title_text = title_item.get("title", "")
                    if title_text:
                        summary_text = title_text
                        break
    
    # If still no summary text, use display title
    if not summary_text:
        summary_text = record.get("bill_title", "")
    
    # Clean and limit summary_text
    summary_text = summary_text.strip()[:5000]  # Limit to 5000 chars for DynamoDB
    record["summary_text"] = summary_text
    
    # Load legislators CSV and match sponsor/cosponsors (cache at module level)
    if not hasattr(load_legislators_csv, '_cache'):
        load_legislators_csv._cache = load_legislators_csv()
        log_print(f"✅ Loaded {len(load_legislators_csv._cache)} legislators from CSV")
    
    politicians = load_legislators_csv._cache
    
    # Match sponsor to CSV using multiple criteria
    sponsor_name = record.get("sponsor_full_name", "")
    sponsor_first = record.get("sponsor_first_name", "")
    sponsor_last = record.get("sponsor_last_name", "")
    sponsor_party_api = record.get("sponsor_party", "")  # From API (might be "R" or full name)
    sponsor_state_api = record.get("sponsor_state", "")
    
    # Extract first character of party if it's a full name
    if sponsor_party_api and len(sponsor_party_api) > 1:
        sponsor_party_char = sponsor_party_api[0].upper()
    else:
        sponsor_party_char = sponsor_party_api.upper() if sponsor_party_api else ""
    
    if sponsor_name and politicians:
        matched_sponsor = find_matching_politician(
            sponsor_name, 
            politicians,
            first_name=sponsor_first,
            last_name=sponsor_last,
            party=sponsor_party_char,
            state=sponsor_state_api
        )
        if matched_sponsor:
            # Update all sponsor fields from CSV match
            csv_name = matched_sponsor.get("name", "")
            if csv_name:
                record["sponsor_full_name"] = csv_name
            
            # Use first_name and last_name from CSV if available
            if matched_sponsor.get("first_name"):
                record["sponsor_first_name"] = matched_sponsor.get("first_name")
            if matched_sponsor.get("last_name"):
                record["sponsor_last_name"] = matched_sponsor.get("last_name")
            
            # Use full party name from CSV (e.g., "Republican" not just "R")
            record["sponsor_party"] = matched_sponsor.get("party_full", record.get("sponsor_party", ""))
            
            # Format state/district same as politician trades matchers (MA01 for House, MA for Senate)
            state_district_formatted = format_state_district(matched_sponsor)
            if state_district_formatted:
                record["sponsor_state"] = state_district_formatted
            
            # Update district from CSV
            if matched_sponsor.get("district"):
                record["sponsor_district"] = matched_sponsor.get("district")
            
            # Update bioguide_id from CSV
            if matched_sponsor.get("bioguide_id"):
                record["sponsor_bioguide_id"] = matched_sponsor["bioguide_id"]
    
    # Match cosponsors from cosponsors_json and build structured cosponsors array
    cosponsors_list = []
    cosponsor_parties_list = []
    
    # Parse cosponsors from JSON string
    cosponsors_json_str = record.get("cosponsors_json", "")
    if cosponsors_json_str and politicians:
        try:
            cosponsors_data = json.loads(cosponsors_json_str) if isinstance(cosponsors_json_str, str) else cosponsors_json_str
            if isinstance(cosponsors_data, list):
                for cosponsor in cosponsors_data:
                    cosponsor_name = cosponsor.get("fullName", "")
                    cosponsor_first = cosponsor.get("firstName", "")
                    cosponsor_last = cosponsor.get("lastName", "")
                    cosponsor_party_api = cosponsor.get("party", "")
                    cosponsor_state_api = cosponsor.get("state", "")
                    
                    # Extract first character of party
                    if cosponsor_party_api and len(cosponsor_party_api) > 1:
                        cosponsor_party_char = cosponsor_party_api[0].upper()
                    else:
                        cosponsor_party_char = cosponsor_party_api.upper() if cosponsor_party_api else ""
                    
                    if cosponsor_name:
                        matched_cosponsor = find_matching_politician(
                            cosponsor_name,
                            politicians,
                            first_name=cosponsor_first,
                            last_name=cosponsor_last,
                            party=cosponsor_party_char,
                            state=cosponsor_state_api
                        )
                        
                        # Build cosponsor object with matched CSV data
                        cosponsor_obj = {}
                        if matched_cosponsor:
                            # Use matched CSV name
                            cosponsor_obj["name"] = matched_cosponsor.get("name", cosponsor_name)
                            # Format state/district same as politician trades matchers
                            state_district_formatted = format_state_district(matched_cosponsor)
                            cosponsor_obj["state"] = state_district_formatted if state_district_formatted else cosponsor_state_api
                            # Use full party name from CSV
                            cosponsor_obj["party"] = matched_cosponsor.get("party_full", cosponsor_party_api)
                            # Add position (House/Senate) from CSV
                            cosponsor_obj["position"] = matched_cosponsor.get("position", "")
                            cosponsor_parties_list.append(matched_cosponsor.get("party_full", cosponsor_party_api))
                        else:
                            # Fallback to API data if no match - try to infer position from district
                            cosponsor_obj["name"] = cosponsor_name
                            cosponsor_obj["state"] = cosponsor_state_api
                            cosponsor_obj["party"] = cosponsor_party_api if cosponsor_party_api else ""
                            # Infer position: if district is 0 or empty, likely Senate; otherwise House
                            district_val = cosponsor.get("district", "")
                            if district_val == 0 or district_val == "" or district_val is None:
                                cosponsor_obj["position"] = "Senate"
                            else:
                                cosponsor_obj["position"] = "House"
                            cosponsor_parties_list.append(cosponsor_party_api if cosponsor_party_api else "")
                        
                        cosponsors_list.append(cosponsor_obj)
        except (json.JSONDecodeError, TypeError) as e:
            log_print(f"⚠️ Error parsing cosponsors_json: {e}")
    
    # Update cosponsors (JSON array) and cosponsor_parties (pipe-separated for bipartisan calculation)
    if cosponsors_list:
        record["cosponsors"] = json.dumps(cosponsors_list)
    if cosponsor_parties_list:
        record["cosponsor_parties"] = "|".join(cosponsor_parties_list)
    
    # Calculate bipartisan: TRUE if sponsor party is different from cosponsor parties
    # OR if both R and D are present in sponsor + cosponsors combined
    # This means the bill has support from both parties (sponsor + cosponsors)
    # Examples:
    # - sponsor R, cosponsor_parties: "D|D" → TRUE (sponsor R different from cosponsors D)
    # - sponsor R, cosponsor_parties: "R|D|R" → TRUE (has both R and D present)
    # - sponsor R, cosponsor_parties: "R|R|R" → FALSE (sponsor R same as all cosponsors R, only one party)
    # - sponsor D, cosponsor_parties: "R" → TRUE (sponsor D different from cosponsor R)
    # - cosponsor_parties: "" (empty) → FALSE (no cosponsors to compare)
    bipartisan = False
    # Get first character of sponsor party (full name like "Republican" -> "R")
    sponsor_party_full = record.get("sponsor_party", "").strip()
    sponsor_party = sponsor_party_full[0].upper() if sponsor_party_full else ""
    cosponsor_parties_str = record.get("cosponsor_parties", "").strip()
    
    if not cosponsor_parties_str:
        # Empty cosponsor parties = not bipartisan (no cosponsors to compare)
        bipartisan = False
    elif sponsor_party:
        # Get unique cosponsor parties (use first character for comparison)
        cosponsor_parties_set = set()
        for party in cosponsor_parties_str.split("|"):
            party_clean = party.strip().upper()
            if party_clean:
                # Get first character if it's a full party name
                party_char = party_clean[0] if party_clean else ""
                if party_char:
                    cosponsor_parties_set.add(party_char)
        
        if cosponsor_parties_set:
            # Check if sponsor party is different from cosponsor parties
            # (i.e., sponsor party is not in the set of cosponsor parties)
            sponsor_different_from_cosponsors = sponsor_party not in cosponsor_parties_set
            
            # Also check if both R and D are present in the combined set (sponsor + cosponsors)
            all_parties = {sponsor_party} | cosponsor_parties_set
            has_both_parties = "R" in all_parties and "D" in all_parties
            
            # Bipartisan if sponsor is different from cosponsors OR both parties are present
            bipartisan = sponsor_different_from_cosponsors or has_both_parties
        else:
            # No valid cosponsor parties = not bipartisan
            bipartisan = False
    else:
        # No sponsor party = not bipartisan
        bipartisan = False
    
    # Store bipartisan as number for DynamoDB (0 = false, 1 = true)
    record["bipartisan"] = 1 if bipartisan else 0
    
    return record


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
    
    # Remove None values (but keep False/0 values)
    cleaned = {}
    for k, v in gsi_fields.items():
        if v is not None:
            cleaned[k] = v
    
    return cleaned


def store_bill_to_dynamodb(record: Dict):
    """Store bill record to DynamoDB. Handles oversized items by storing to S3."""
    if not bills_table:
        log_print("⚠️ DynamoDB table not configured, skipping storage")
        return
    
    bill_id = record.get('bill_id', 'unknown')
    max_put_retries = 3
    
    for put_attempt in range(max_put_retries):
        try:
            bills_table.put_item(Item=record)
            log_print(f"      ✅ Stored {bill_id} to DynamoDB")
            return
        except Exception as put_error:
            error_str = str(put_error)
            
            # Handle oversized items (ValidationException)
            if 'ValidationException' in error_str and 'Item size has exceeded' in error_str:
                log_print(f"      ⚠️ Bill {bill_id} exceeds DynamoDB size limit, storing to S3...")
                
                oversize_s3_key = store_oversized_item_to_s3(bill_id, record)
                gsi_only_item = extract_gsi_fields_only(record)
                gsi_only_item['oversize_s3_key'] = oversize_s3_key
                
                try:
                    bills_table.put_item(Item=gsi_only_item)
                    log_print(f"      ✅ Stored GSI fields for oversized bill {bill_id} to DynamoDB, full data in S3")
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


# ============================================================================
# Main Execution
# ============================================================================

def main():
    log_print("=" * 80)
    log_print("Congress.gov Bill Fetcher Glue Job - Starting")
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
    
    # Calculate date range based on source (same logic as router Lambda)
    if source == "scheduler":
        # If source is "scheduler", calculate yesterday's date
        now = datetime.now(timezone.utc)
        yesterday = now - timedelta(days=1)
        # Format as mm/dd/yyyy (matching router Lambda format)
        start_date_str = yesterday.strftime("%m/%d/%Y")
        end_date_str = yesterday.strftime("%m/%d/%Y")
        log_print(f"📅 Source is scheduler, calculating yesterday's date: {start_date_str}")
    
    # Parse and normalize dates (handle both mm/dd/yyyy and ISO formats)
    if start_date_str and end_date_str:
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
    else:
        # Calculate date range if not provided (default to last 7 days for Glue)
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=7)
        start_date_str = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_date_str = end_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        log_print(f"⚠️ Date range not provided, using last 7 days: {start_date_str} to {end_date_str}")
    
    log_print(f"📅 Date Range: {start_date_str} to {end_date_str}")
    
    # Get Congress number if not provided
    if not congress:
        congress = get_current_congress(api_key)
    else:
        congress = int(congress)
        log_print(f"✅ Using provided Congress: {congress}")
    
    log_print("")  # Empty line for readability
    
    # Fetch all bills
    log_print("📋 Fetching Bills...")
    log_print("-" * 80)
    
    all_bills = []
    for bill_type in BILL_TYPES:
        log_print(f"   📋 Fetching {bill_type} bills...")
        bills = fetch_bills_list(congress, bill_type, start_date_str, end_date_str, api_key)
        all_bills.extend(bills)
        log_print(f"      ✅ Found {len(bills)} {bill_type} bills")
    
    log_print(f"\n✅ Total bills found: {len(all_bills)}")
    log_print("")  # Empty line for readability
    
    # Build comprehensive records and store to DynamoDB (parallelized)
    log_print("🔍 Building comprehensive bill records and storing to DynamoDB...")
    log_print("-" * 80)
    log_print(f"   🚀 Using parallel processing with up to 20 concurrent workers...")
    
    processed_count = 0
    error_count = 0
    
    def process_bill(bill: Dict) -> Tuple[bool, Optional[str]]:
        """Process a single bill and return (success, error_message)"""
        import threading
        thread_id = threading.current_thread().name
        bill_type = bill.get("type", "")
        bill_number = bill.get("number", "unknown")
        
        if not bill_type:
            return False, "No bill type"
        
        try:
            log_print(f"      🧵 [{thread_id}] Starting {bill_type} {bill_number}...")
            record = build_comprehensive_bill_record(bill, congress, bill_type, api_key)
            if record:
                store_bill_to_dynamodb(record)
                log_print(f"      ✅ [{thread_id}] Completed {bill_type} {bill_number}")
                return True, None
            else:
                return False, "No record built"
        except Exception as e:
            error_msg = f"Error processing {bill_type} {bill_number}: {str(e)}"
            log_print(f"      ❌ [{thread_id}] {error_msg}")
            return False, error_msg
    
    # Process bills in parallel (max 20 concurrent for Glue - more resources available)
    log_print(f"   📤 Submitting {len(all_bills)} bills for parallel processing...")
    with ThreadPoolExecutor(max_workers=20) as executor:
        # Submit all tasks at once
        future_to_bill = {}
        for bill in all_bills:
            future = executor.submit(process_bill, bill)
            future_to_bill[future] = bill
        
        log_print(f"   ✅ All {len(future_to_bill)} tasks submitted, processing in parallel...")
        
        completed = 0
        for future in as_completed(future_to_bill):
            completed += 1
            try:
                success, error_msg = future.result()
                
                if success:
                    processed_count += 1
                else:
                    error_count += 1
                    if error_msg:
                        log_print(f"      ❌ {error_msg}")
            except Exception as e:
                error_count += 1
                bill = future_to_bill.get(future, {})
                log_print(f"      ❌ Exception processing bill: {str(e)}")
            
            if completed % 10 == 0:
                log_print(f"      ✅ Processed {completed}/{len(all_bills)} bills...")
            
            # Memory cleanup for large batches
            if completed % 100 == 0:
                gc.collect()
    
    log_print(f"\n✅ Processed {processed_count} bills successfully")
    if error_count > 0:
        log_print(f"⚠️ {error_count} bills had errors")
    
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


