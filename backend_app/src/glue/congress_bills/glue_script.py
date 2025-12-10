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
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional

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

# Get optional date parameters
try:
    optional_args = getResolvedOptions(sys.argv, ['CONGRESS', 'START_DATE', 'END_DATE'])
    args.update(optional_args)
    print(f"✅ Successfully parsed optional arguments: CONGRESS={optional_args.get('CONGRESS')}, START_DATE={optional_args.get('START_DATE')}, END_DATE={optional_args.get('END_DATE')}", flush=True)
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
        "bill_number": bill_number,
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
        "summary_text": " ".join([
            s.get("text", "").replace("<p>", " ").replace("</p>", " ").replace("<strong>", "").replace("</strong>", "")
            for s in summaries if s.get("text")
        ])[:5000],  # Limit to 5000 chars for DynamoDB
        "summaries_json": json.dumps(summaries) if summaries else "",
        
        # Short description (for quick search/display - extracted from first summary or title)
        "short_description": "",
        
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
    
    # Short description: Use bill title (primary) + first sentence of summary (if available)
    # This makes bill_title the primary searchable field, with summary context as backup
    bill_title = record.get("bill_title", "")
    short_desc = bill_title  # Start with bill title
    
    if summaries and len(summaries) > 0:
        first_summary = summaries[0].get("text", "")
        if first_summary:
            # Clean HTML and get first sentence
            cleaned = first_summary.replace("<p>", " ").replace("</p>", " ").replace("<strong>", "").replace("</strong>", "").replace("<ul>", " ").replace("</ul>", " ").replace("<li>", " ").replace("</li>", " ").strip()
            first_sentence = cleaned.split(".")[0] if "." in cleaned else cleaned[:200]
            # Combine: "Bill Title. First sentence of summary"
            if first_sentence and len(first_sentence) > 10:  # Only add if meaningful
                short_desc = f"{bill_title}. {first_sentence[:200]}".strip()
    
    record["short_description"] = short_desc[:500]  # Limit to 500 chars total
    
    return record


def store_bill_to_dynamodb(record: Dict):
    """Store bill record to DynamoDB with retry logic."""
    if not bills_table:
        log_print("⚠️ DynamoDB table not configured, skipping storage")
        return
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            bills_table.put_item(Item=record)
            log_print(f"      ✅ Stored {record['bill_id']} to DynamoDB")
            return
        except Exception as e:
            error_str = str(e)
            if 'ThrottlingException' in error_str or 'ProvisionedThroughputExceededException' in error_str:
                if attempt < max_retries - 1:
                    wait_time = (attempt + 1) * 2
                    log_print(f"      ⚠️ Throttling for {record['bill_id']}, retrying in {wait_time}s...")
                    time.sleep(wait_time)
                    continue
            log_print(f"      ❌ Error storing {record.get('bill_id', 'unknown')} to DynamoDB: {error_str}")
            raise


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
    
    # Calculate date range if not provided (default to last 7 days for Glue)
    if not start_date_str or not end_date_str:
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
    
    log_print()
    
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
        
        # Memory cleanup for large batches
        if (i + 1) % 100 == 0:
            gc.collect()
        
        time.sleep(0.3)  # Rate limiting
    
    log_print(f"\n✅ Processed {processed_count} bills successfully")
    if error_count > 0:
        log_print(f"⚠️ {error_count} bills had errors")
    
    log_print()
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
