"""
Lambda function to fetch comprehensive Congress.gov bill data.
Fetches all bills, actions, amendments, cosponsors, summaries, and subjects.
Stores everything in DynamoDB with one row per bill.

This function is designed for date ranges ≤ 2 days (15 min timeout).
For longer ranges, use the Glue job instead.
"""

import json
import os
import logging
import requests
import boto3
import csv
import re
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional
from io import StringIO
from difflib import SequenceMatcher
import time

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS clients
dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')
secrets_client = boto3.client('secretsmanager')

# Environment variables
PROJECT_NAME = os.environ.get('PROJECT_NAME', 'cosine')
ENVIRONMENT = os.environ.get('ENVIRONMENT', 'staging')
API_BASE_URL = os.environ.get('CONGRESS_API_BASE_URL', 'https://api.congress.gov/v3')
DYNAMODB_TABLE_NAME = os.environ.get('BILLS_TABLE_NAME')
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME')
POLITICIAN_TRADES_S3_BUCKET = os.environ.get('POLITICIAN_TRADES_S3_BUCKET')
REQUEST_TIMEOUT = int(os.environ.get('REQUEST_TIMEOUT', '30'))
MAX_RETRIES = int(os.environ.get('MAX_RETRIES', '3'))
RETRY_DELAY = int(os.environ.get('RETRY_DELAY', '2'))

# Name matching threshold (0.0 to 1.0)
NAME_MATCH_THRESHOLD = 0.85  # 85% similarity

# Bill types to fetch
BILL_TYPES = ["HR", "S", "HJRES", "SJRES", "HCONRES", "SCONRES", "HRES", "SRES"]

# DynamoDB table
bills_table = dynamodb.Table(DYNAMODB_TABLE_NAME) if DYNAMODB_TABLE_NAME else None

# Cache for legislators CSV (loaded once per Lambda execution)
_legislators_cache = None

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
        logger.error(f"Error retrieving Congress API key from Secrets Manager: {str(e)}")
        raise ValueError(f"Failed to retrieve Congress API key from Secrets Manager: {str(e)}")

def log_print(message: str):
    """Print to both logger and stdout for maximum visibility"""
    logger.info(message)
    print(message, flush=True)


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
            
            politicians.append({
                'name': primary_name,
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
        logger.error(f"❌ Error loading congress-legislators list: {e}")
        return []


def fuzzy_match_name(name: str, politician: Dict[str, Any]) -> float:
    """
    Fuzzy match a name to a politician using Levenshtein distance
    Handles various name formats
    """
    # Normalize names (lowercase, strip)
    name_normalized = name.lower().strip()
    politician_normalized = politician['name'].lower().strip()
    
    # Remove position markers like "(Senator)", "(Representative)" from name
    name_clean = re.sub(r'\s*\([^)]*(?:senator|representative)[^)]*\)', '', name_normalized, flags=re.IGNORECASE)
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


def find_matching_politician(name: str, politicians: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Find best matching politician for a name
    """
    if not name or not politicians:
        return None
    
    best_match = None
    best_score = 0.0
    
    for politician in politicians:
        score = fuzzy_match_name(name, politician)
        if score > best_score:
            best_score = score
            best_match = politician
    
    # Return match if above threshold
    if best_score >= NAME_MATCH_THRESHOLD:
        return {
            **best_match,
            'matchScore': best_score
        }
    
    return None


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


def build_comprehensive_bill_record(bill: Dict, congress: int, bill_type: str, api_key: str) -> Optional[Dict]:
    """
    Build a comprehensive bill record with all related data.
    One row per bill with all information.
    """
    bill_number = bill.get("number")
    if not bill_number:
        return None
    
    log_print(f"      📋 Processing {bill_type} {bill_number}...")
    
    # Fetch all related data
    details = fetch_bill_details(congress, bill_type, bill_number, api_key)
    actions = fetch_bill_actions(congress, bill_type, bill_number, api_key)
    amendments = fetch_bill_amendments(congress, bill_type, bill_number, api_key)
    cosponsors = fetch_bill_cosponsors(congress, bill_type, bill_number, api_key)
    summaries = fetch_bill_summaries(congress, bill_type, bill_number, api_key)
    subjects = fetch_bill_subjects(congress, bill_type, bill_number, api_key)
    titles = fetch_bill_titles(congress, bill_type, bill_number, api_key)
    
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
    
    # Build comprehensive record
    record = {
        # Bill Basic Info
        "bill_id": f"{congress}-{bill_type}-{bill_number}",
        "congress": congress,
        "bill_type": bill_type,
        "bill_number": int(bill_number) if bill_number else 0,  # Store as integer for GSI
        "bill_title": bill.get("title") or (details.get("title") if details else ""),
        "bill_url": bill.get("url") or (details.get("url") if details else ""),
        
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
        
        # Cosponsors (comma-separated for easy viewing)
        "cosponsor_count": len(cosponsors),
        "cosponsor_names": "|".join([c.get("fullName", "") for c in cosponsors if c.get("fullName")]),
        "cosponsor_parties": "|".join([c.get("party", "") for c in cosponsors if c.get("party")]),
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
    
    # Load legislators CSV and match sponsor/cosponsors
    global _legislators_cache
    if _legislators_cache is None:
        _legislators_cache = load_legislators_csv()
        log_print(f"✅ Loaded {len(_legislators_cache)} legislators from CSV")
    
    politicians = _legislators_cache
    
    # Match sponsor to CSV
    sponsor_name = record.get("sponsor_full_name", "")
    if sponsor_name and politicians:
        matched_sponsor = find_matching_politician(sponsor_name, politicians)
        if matched_sponsor:
            # Update from CSV - use full name from CSV (cleaner than API format)
            record["sponsor_full_name"] = matched_sponsor.get("name", record.get("sponsor_full_name", ""))
            # Use full party name from CSV (e.g., "Republican" not just "R")
            record["sponsor_party"] = matched_sponsor.get("party_full", record.get("sponsor_party", ""))
            # Format state/district same as politician trades matchers (MA01 for House, MA for Senate)
            state_district_formatted = format_state_district(matched_sponsor)
            if state_district_formatted:
                record["sponsor_state"] = state_district_formatted
            if matched_sponsor.get("bioguide_id"):
                record["sponsor_bioguide_id"] = matched_sponsor["bioguide_id"]
    
    # Match cosponsors to CSV and update parties (use full party names)
    cosponsor_parties_list = []
    if cosponsors and politicians:
        for cosponsor in cosponsors:
            cosponsor_name = cosponsor.get("fullName", "")
            if cosponsor_name:
                matched_cosponsor = find_matching_politician(cosponsor_name, politicians)
                if matched_cosponsor:
                    # Use full party name from CSV (e.g., "Republican" not just "R")
                    cosponsor_parties_list.append(matched_cosponsor.get("party_full", cosponsor.get("party", "")))
                else:
                    # Fallback to API party (use as-is, might be full name or abbreviation)
                    api_party = cosponsor.get("party", "")
                    cosponsor_parties_list.append(api_party if api_party else "")
    
    # Update cosponsor_parties with matched values
    if cosponsor_parties_list:
        record["cosponsor_parties"] = "|".join(cosponsor_parties_list)
    
    # Calculate bipartisan: TRUE if sponsor party AND cosponsor parties contain both R and D
    # Examples:
    # - cosponsor_parties: "R|D|R" with sponsor R → TRUE (has both R and D)
    # - cosponsor_parties: "R|R|R" with sponsor R → FALSE (only R)
    # - cosponsor_parties: "" (empty) → FALSE
    bipartisan = False
    # Get first character of sponsor party (full name like "Republican" -> "R")
    sponsor_party_full = record.get("sponsor_party", "").strip()
    sponsor_party = sponsor_party_full[0].upper() if sponsor_party_full else ""
    cosponsor_parties_str = record.get("cosponsor_parties", "").strip()
    
    if not cosponsor_parties_str:
        # Empty cosponsor parties = not bipartisan
        bipartisan = False
    elif sponsor_party:
        # Get unique parties from sponsor + all cosponsors (use first character for comparison)
        all_parties = set([sponsor_party])
        for party in cosponsor_parties_str.split("|"):
            party_clean = party.strip().upper()
            if party_clean:
                # Get first character if it's a full party name
                party_char = party_clean[0] if party_clean else ""
                if party_char:
                    all_parties.add(party_char)
        
        # Bipartisan if both R and D are present
        bipartisan = "R" in all_parties and "D" in all_parties
    else:
        # No sponsor party = not bipartisan
        bipartisan = False
    
    # Store bipartisan as number for DynamoDB (0 = false, 1 = true)
    record["bipartisan"] = 1 if bipartisan else 0
    
    return record


def store_bill_to_dynamodb(record: Dict):
    """Store bill record to DynamoDB."""
    if not bills_table:
        log_print("⚠️ DynamoDB table not configured, skipping storage")
        return
    
    try:
        # Convert to DynamoDB format (handle any Decimal conversions if needed)
        bills_table.put_item(Item=record)
        log_print(f"      ✅ Stored {record['bill_id']} to DynamoDB")
    except Exception as e:
        log_print(f"      ❌ Error storing {record.get('bill_id', 'unknown')} to DynamoDB: {str(e)}")
        raise


# ============================================================================
# Lambda Handler
# ============================================================================

def lambda_handler(event: Dict, context: Any) -> Dict:
    """
    Lambda handler for fetching Congress.gov bill data.
    
    Expected event format:
    {
        "congress": 119,  # Optional, will auto-detect if not provided
        "start_date": "2025-12-08T00:00:00Z",  # ISO format
        "end_date": "2025-12-09T23:59:59Z",    # ISO format
        "bill_types": ["HR", "S", ...]  # Optional, defaults to all types
    }
    """
    log_print("=" * 80)
    log_print("Congress.gov Bill Fetcher Lambda - Starting")
    log_print("=" * 80)
    
    if not DYNAMODB_TABLE_NAME:
        raise ValueError("BILLS_TABLE_NAME environment variable not set")
    
    # Get API key from Secrets Manager
    try:
        api_key = get_congress_api_key()
        log_print("✅ Retrieved Congress API key from Secrets Manager")
    except Exception as e:
        log_print(f"❌ Failed to retrieve API key: {str(e)}")
        raise
    
    # Parse event parameters
    congress = event.get("congress")
    start_date_str = event.get("start_date")
    end_date_str = event.get("end_date")
    bill_types = event.get("bill_types", BILL_TYPES)
    
    # Calculate date range if not provided
    if not start_date_str or not end_date_str:
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=1)
        start_date_str = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        end_date_str = end_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        log_print(f"⚠️ Date range not provided, using last 1 day: {start_date_str} to {end_date_str}")
    
    log_print(f"📅 Date Range: {start_date_str} to {end_date_str}")
    
    # Get Congress number if not provided
    if not congress:
        congress = get_current_congress(api_key)
    else:
        log_print(f"✅ Using provided Congress: {congress}")
    
    log_print()
    
    # Fetch all bills
    log_print("📋 Fetching Bills...")
    log_print("-" * 80)
    
    all_bills = []
    for bill_type in bill_types:
        log_print(f"   📋 Fetching {bill_type} bills...")
        bills = fetch_bills_list(congress, bill_type, start_date_str, end_date_str, api_key)
        all_bills.extend(bills)
        log_print(f"      ✅ Found {len(bills)} {bill_type} bills")
    
    log_print(f"\n✅ Total bills found: {len(all_bills)}")
    log_print()
    
    # Build comprehensive records and store to DynamoDB
    log_print("🔍 Building comprehensive bill records and storing to DynamoDB...")
    log_print("-" * 80)
    
    processed_count = 0
    error_count = 0
    
    for i, bill in enumerate(all_bills):
        bill_type = bill.get("type", "")
        if not bill_type:
            continue
        
        try:
            record = build_comprehensive_bill_record(bill, congress, bill_type, api_key)
            if record:
                store_bill_to_dynamodb(record)
                processed_count += 1
        except Exception as e:
            error_count += 1
            log_print(f"      ❌ Error processing {bill_type} {bill.get('number', 'unknown')}: {str(e)}")
        
        if (i + 1) % 10 == 0:
            log_print(f"      ✅ Processed {i + 1}/{len(all_bills)} bills...")
        
        time.sleep(0.3)  # Rate limiting
    
    log_print(f"\n✅ Processed {processed_count} bills successfully")
    if error_count > 0:
        log_print(f"⚠️ {error_count} bills had errors")
    
    log_print()
    log_print("=" * 80)
    log_print("✅ Lambda completed successfully!")
    log_print("=" * 80)
    
    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Congress bills fetched and stored successfully",
            "congress": congress,
            "start_date": start_date_str,
            "end_date": end_date_str,
            "total_bills_found": len(all_bills),
            "processed_count": processed_count,
            "error_count": error_count
        })
    }


