"""
AWS Glue Job: Congress Bills Roll Call Maintenance
Daily job to refresh roll call (voter) data for all bills.

Roll call maintenance (all bills, excluding search indices):
   - Scans the table for all bill items (bill_id does NOT start with "SEARCH#")
   - For each bill, fetches bill actions from Congress.gov API and extracts recordedVotes
   - For each House roll call, fetches member-level vote data (house-vote/{congress}/{session}/{voteNumber}/members)
   - Updates each item with:
     - roll_call_number (N): first House roll call number, or omitted if none
     - roll_call_votes (S): JSON string of roll call(s) with member votes
     - has_roll_call (N): 1 if bill has at least one roll call, 0 otherwise (GSI key for HasRollCallIndex)
   - Runs with a thread pool (ROLL_CALL_MAX_WORKERS, default 8) and per-key rate limiters
     (CONGRESS_API_MAX_REQUESTS_PER_HOUR, default 4800, per key) so total capacity = 4800 * n keys.

Bill text is filled by the fetcher pipeline (bulk XML + SQS -> Lambda); no separate backfill.
"""


import sys
import json
import logging
import time
import threading
import requests
import re
import zipfile
from io import BytesIO
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Tuple, Set

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
log_print("✅ Congress Bills Roll Call Maintenance Glue Job - Script Loaded Successfully")
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

# Congress.gov API: 5,000 requests/hour per key (https://github.com/LibraryOfCongress/api.congress.gov)
CONGRESS_API_MAX_REQUESTS_PER_HOUR = int(args.get('CONGRESS_API_MAX_REQUESTS_PER_HOUR', '4800'))
ROLL_CALL_MAX_WORKERS = int(args.get('ROLL_CALL_MAX_WORKERS', '8'))

# Optional: for consolidated run (after fetcher). If set, only process bills from these ZIPs; use delta logic.
def _get_optional_arg(name: str) -> Optional[str]:
    for i in range(len(sys.argv) - 1):
        if sys.argv[i] == f"--{name}":
            return sys.argv[i + 1]
    return None

START_DATE = _get_optional_arg("START_DATE")
END_DATE = _get_optional_arg("END_DATE")
ZIP_S3_KEYS_JSON = _get_optional_arg("ZIP_S3_KEYS")

# AWS clients
dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')
secrets_client = boto3.client('secretsmanager')
bills_table = dynamodb.Table(BILLS_TABLE_NAME) if BILLS_TABLE_NAME else None

# ============================================================================
# Rate limiter (Congress.gov: 5,000 req/hour per key; one limiter per key)
# ============================================================================

class CongressApiRateLimiter:
    """Thread-safe rate limiter: at most N requests per rolling hour (per API key)."""

    def __init__(self, max_per_hour: int = 4800, window_seconds: int = 3600):
        self.max_per_hour = max_per_hour
        self.window_seconds = window_seconds
        self._timestamps = deque()
        self._lock = threading.Lock()

    def acquire(self) -> None:
        with self._lock:
            now = time.time()
            # Drop timestamps outside the window
            while self._timestamps and self._timestamps[0] < now - self.window_seconds:
                self._timestamps.popleft()
            while len(self._timestamps) >= self.max_per_hour:
                # Wait until oldest request exits the window
                wait_until = self._timestamps[0] + self.window_seconds
                sleep_time = wait_until - time.time()
                if sleep_time > 0:
                    time.sleep(sleep_time)
                now = time.time()
                while self._timestamps and self._timestamps[0] < now - self.window_seconds:
                    self._timestamps.popleft()
            self._timestamps.append(time.time())


# One rate limiter per API key (4800 req/hour each). Initialized when keys are loaded.
_congress_rate_limiters: Optional[List['CongressApiRateLimiter']] = None

# Fallback single limiter when key_index not provided
_congress_rate_limiter = None

def _get_rate_limiter() -> CongressApiRateLimiter:
    global _congress_rate_limiter
    if _congress_rate_limiter is None:
        _congress_rate_limiter = CongressApiRateLimiter(
            max_per_hour=CONGRESS_API_MAX_REQUESTS_PER_HOUR,
            window_seconds=3600,
        )
    return _congress_rate_limiter

def _get_rate_limiter_for_key(key_index: int) -> CongressApiRateLimiter:
    """Per-key rate limiter: 4800 req/hour per key. Use when rotating keys."""
    global _congress_rate_limiters
    if _congress_rate_limiters is None or key_index < 0 or key_index >= len(_congress_rate_limiters):
        return _get_rate_limiter()
    return _congress_rate_limiters[key_index]


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
        
        # One rate limiter per key (4800 req/hour each) so total capacity = 4800 * n keys
        global _congress_rate_limiters
        _congress_rate_limiters = [
            CongressApiRateLimiter(max_per_hour=CONGRESS_API_MAX_REQUESTS_PER_HOUR, window_seconds=3600)
            for _ in api_keys
        ]
        
        # Thread-safe round-robin rotator for use with multithreaded roll call phase
        class SimpleApiKeyRotator:
            def __init__(self, keys):
                self.keys = keys
                self.current_index = 0
                self._lock = threading.Lock()
                log_print(f"✅ Retrieved {len(keys)} Congress API key(s) from Secrets Manager")
                log_print(f"   Rate limit: {CONGRESS_API_MAX_REQUESTS_PER_HOUR} req/hour per key ({len(keys) * CONGRESS_API_MAX_REQUESTS_PER_HOUR} total/hour)")

            def get_key(self):
                with self._lock:
                    key = self.keys[self.current_index]
                    self.current_index = (self.current_index + 1) % len(self.keys)
                    return key

            def get_key_and_index(self):
                """Return (key, index) for per-key rate limiting. Index is used to acquire from the correct limiter."""
                with self._lock:
                    idx = self.current_index
                    key = self.keys[idx]
                    self.current_index = (self.current_index + 1) % len(self.keys)
                    return key, idx

            def get_key_count(self):
                return len(self.keys)
        
        _api_key_rotator = SimpleApiKeyRotator(api_keys)
        return _api_key_rotator
        
    except Exception as e:
        log_print(f"❌ Error retrieving Congress API keys from Secrets Manager: {str(e)}")
        raise ValueError(f"Failed to retrieve Congress API keys from Secrets Manager: {str(e)}")


def make_api_request(
    url: str,
    params: Dict[str, Any],
    api_key: str,
    retries: int = MAX_RETRIES,
    key_index: Optional[int] = None,
) -> Optional[Dict]:
    """Make API request with retry logic, rate limit (per-key when key_index set), and exponential backoff for 429."""
    if key_index is not None:
        _get_rate_limiter_for_key(key_index).acquire()
    else:
        _get_rate_limiter().acquire()
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


# BILLSTATUS-119hr123.xml or BILLSTATUS-119-hr-123.xml (ZIP may use either)
_BILLSTATUS_FILENAME_RE = re.compile(
    r"BILLSTATUS-(\d+)(hr|s|hjres|sjres|hconres|sconres|hres|sres)(\d+)\.xml",
    re.IGNORECASE
)

def _bill_id_from_zip_filename(filename: str) -> Optional[str]:
    """Derive bill_id from ZIP entry name (e.g. BILLSTATUS-119hr123.xml -> 119-HR-123)."""
    base = filename.split("/")[-1]
    m = _BILLSTATUS_FILENAME_RE.match(base)
    if not m:
        return None
    congress, bill_type, number = m.group(1), m.group(2).upper(), m.group(3)
    return f"{congress}-{bill_type}-{number}"


def get_bill_ids_from_zip_s3_key(bucket: str, zip_s3_key: str) -> List[str]:
    """Download ZIP from S3, list XML entries, return list of bill_ids (regular bills only)."""
    zip_obj = s3_client.get_object(Bucket=bucket, Key=zip_s3_key)
    zip_content = zip_obj["Body"].read()
    bill_ids = []
    with zipfile.ZipFile(BytesIO(zip_content), "r") as zf:
        for name in zf.namelist():
            if name.lower().endswith(".xml"):
                bid = _bill_id_from_zip_filename(name)
                if bid and not bid.startswith("SEARCH#"):
                    bill_ids.append(bid)
    return bill_ids


def resolve_zip_s3_keys() -> Optional[List[str]]:
    """Get list of ZIP S3 keys: from --ZIP_S3_KEYS, or run-outputs/<start>_<end>/zip_s3_keys.json, or latest run-outputs folder."""
    if ZIP_S3_KEYS_JSON:
        try:
            keys = json.loads(ZIP_S3_KEYS_JSON)
            if isinstance(keys, list) and keys:
                return keys
        except (json.JSONDecodeError, TypeError):
            pass
    if S3_BUCKET_NAME:
        run_prefix = "run-outputs/"
        start_ok = START_DATE and str(START_DATE).strip().lower() not in ("", "null")
        end_ok = END_DATE and str(END_DATE).strip().lower() not in ("", "null")
        if start_ok and end_ok:
            start_simple = START_DATE.split("T")[0]
            end_simple = END_DATE.split("T")[0]
            run_key = f"{run_prefix}{start_simple}_{end_simple}/zip_s3_keys.json"
            try:
                obj = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=run_key)
                data = json.loads(obj["Body"].read().decode())
                keys = data.get("zip_s3_keys") if isinstance(data, dict) else None
                if isinstance(keys, list) and keys:
                    return keys
            except s3_client.exceptions.ClientError as e:
                if e.response["Error"]["Code"] != "404":
                    log_print(f"   ⚠️ Failed to read run-outputs: {e}")
            except (json.JSONDecodeError, TypeError):
                pass
        # Fallback: list run-outputs/ and use latest folder (e.g. scheduler passed null dates)
        try:
            paginator = s3_client.get_paginator("list_objects_v2")
            prefixes = []
            for page in paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix=run_prefix, Delimiter="/"):
                for p in page.get("CommonPrefixes") or []:
                    pre = p.get("Prefix", "")
                    if pre.endswith("/"):
                        prefixes.append(pre)
            if prefixes:
                prefixes.sort(reverse=True)  # lexicographic: 2024-02-05_2024-02-05/ > 2024-02-01_...
                run_key = f"{prefixes[0]}zip_s3_keys.json"
                obj = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=run_key)
                data = json.loads(obj["Body"].read().decode())
                keys = data.get("zip_s3_keys") if isinstance(data, dict) else None
                if isinstance(keys, list) and keys:
                    log_print(f"   📂 Using latest run-outputs folder: {prefixes[0]}")
                    return keys
        except (s3_client.exceptions.ClientError, json.JSONDecodeError, TypeError) as e:
            log_print(f"   ⚠️ No run-outputs found or read failed: {e}")
    return None


def _fetch_bill_actions(
    congress: str,
    bill_type: str,
    bill_number: str,
    api_key: str,
    key_index: Optional[int] = None,
) -> List[Dict]:
    """Fetch all bill actions (paginated) from /bill/{congress}/{billType}/{billNumber}/actions."""
    actions = []
    offset = 0
    limit = 250
    while True:
        url = f"{API_BASE_URL}/bill/{congress}/{bill_type.lower()}/{bill_number}/actions"
        params = {"format": "json", "offset": offset, "limit": limit}
        data = make_api_request(url, params, api_key, key_index=key_index)
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


def _fetch_house_vote_members(
    congress: str,
    session: int,
    roll_number: int,
    api_key: str,
    key_index: Optional[int] = None,
) -> List[Dict]:
    """
    Fetch house roll call member votes (paginated). Returns list of member vote dicts.
    Mirrors scripts/Bills/test_bill_roll_call.py fetch_house_vote_members so we handle
    all Congress.gov API response shapes (houseRollCallVoteMemberVotes, results, members, etc.).
    """
    all_members = []
    offset = 0
    limit = 250
    while True:
        url = f"{API_BASE_URL}/house-vote/{congress}/{session}/{roll_number}/members"
        params = {"format": "json", "offset": offset, "limit": limit}
        data = make_api_request(url, params, api_key, key_index=key_index)
        if not data:
            break
        results = None
        if isinstance(data, dict):
            raw = data.get("houseRollCallVoteMemberVotes")
            if isinstance(raw, list):
                results = raw
            elif isinstance(raw, dict):
                for key in ("item", "memberVote", "memberVotes", "houseRollCallVoteMemberVote"):
                    val = raw.get(key)
                    if isinstance(val, list):
                        results = val
                        break
                    if isinstance(val, dict) and (val.get("voteCast") or val.get("bioguideID")):
                        results = [val]
                        break
                if results is None and raw:
                    for _k, val in raw.items():
                        if isinstance(val, list) and val and isinstance(val[0], dict):
                            if "voteCast" in val[0] or "bioguideID" in val[0]:
                                results = val
                                break
            if not isinstance(results, list):
                results = data.get("results") or data.get("members") or data.get("memberVotes")
            if not isinstance(results, list) and isinstance(data.get("houseVote"), dict):
                h = data["houseVote"]
                results = h.get("results") or h.get("members")
            if not isinstance(results, list):
                for key in ("voteMembers", "items", "votes"):
                    cand = data.get(key)
                    if isinstance(cand, list) and cand and isinstance(cand[0], dict):
                        if "voteCast" in cand[0] or "bioguideID" in cand[0]:
                            results = cand
                            break
        elif isinstance(data, list) and data and isinstance(data[0], dict):
            if "voteCast" in data[0] or "bioguideID" in data[0]:
                results = data
        if isinstance(results, list):
            all_members.extend(results)
        pagination = data.get("pagination") if isinstance(data, dict) else {}
        count = pagination.get("count", 0) if isinstance(pagination, dict) else 0
        if isinstance(data, dict) and count and offset + limit >= count:
            break
        if not (isinstance(results, list) and len(results) == limit):
            break
        offset += limit
    return all_members


def _build_roll_call_data_for_bill(
    bill_id: str,
    api_key: str,
    key_index: Optional[int] = None,
) -> Dict[str, Any]:
    """
    For a bill: fetch actions -> recordedVotes -> for each House vote fetch members.
    Returns dict: roll_call_number (int or None), roll_call_votes (list of {roll, session, members}), has_roll_call (1 or 0).
    """
    parsed = _parse_bill_id(bill_id)
    if not parsed:
        return {"roll_call_number": None, "roll_call_votes": [], "has_roll_call": 0}
    congress, bill_type, bill_number = parsed
    actions = _fetch_bill_actions(congress, bill_type, bill_number, api_key, key_index=key_index)
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
        members = _fetch_house_vote_members(congress, session, roll, api_key, key_index=key_index)
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
    key_index: Optional[int] = None,
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
        data = _build_roll_call_data_for_bill(bill_id, api_key, key_index=key_index)
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


def _recorded_votes_house_rolls(recorded_votes_json: Any) -> Set[Tuple[str, str]]:
    """Parse recorded_votes_json (from table/XML); return set of (sessionNumber, rollNumber) for House only."""
    out = set()
    if not recorded_votes_json:
        return out
    try:
        data = json.loads(recorded_votes_json) if isinstance(recorded_votes_json, str) else recorded_votes_json
    except (TypeError, json.JSONDecodeError):
        return out
    if not isinstance(data, list):
        return out
    for r in data:
        if not isinstance(r, dict):
            continue
        if (r.get("chamber") or "").strip().upper() != "HOUSE":
            continue
        s, roll = r.get("sessionNumber"), r.get("rollNumber")
        if s is not None and roll is not None:
            out.add((str(s), str(roll)))
    return out


def _existing_rolls_from_roll_call_votes(roll_call_votes_json: Any) -> Set[Tuple[str, str]]:
    """Parse roll_call_votes (from table); return set of (session, roll)."""
    out = set()
    if not roll_call_votes_json:
        return out
    try:
        data = json.loads(roll_call_votes_json) if isinstance(roll_call_votes_json, str) else roll_call_votes_json
    except (TypeError, json.JSONDecodeError):
        return out
    if not isinstance(data, list):
        return out
    for r in data:
        if not isinstance(r, dict):
            continue
        s, roll = r.get("session"), r.get("roll")
        if s is not None and roll is not None:
            out.add((str(s), str(roll)))
    return out


def process_bill_roll_call_delta(
    item: Dict[str, Any],
    api_key: str,
    key_index: Optional[int] = None,
) -> Tuple[bool, Optional[str], Optional[Dict]]:
    """
    Delta roll call: compare recorded_votes_json (XML) vs roll_call_votes (table).
    If same -> skip. If new roll(s) -> fetch only those, merge, return update payload.
    Returns (success, error_message, update_dict or None to skip).
    """
    bill_id = (item.get("bill_id") or "").strip()
    if not bill_id or bill_id.startswith("SEARCH#"):
        return True, None, None
    search_index_sk = str(item.get("search_index_sk") or bill_id)

    xml_rolls = _recorded_votes_house_rolls(item.get("recorded_votes_json"))
    existing_rolls = _existing_rolls_from_roll_call_votes(item.get("roll_call_votes"))

    if xml_rolls == existing_rolls:
        return True, None, None  # skip

    if not xml_rolls:
        return True, None, {
            "bill_id": bill_id,
            "search_index_sk": search_index_sk,
            "UpdateExpression": "SET has_roll_call = :h REMOVE roll_call_number, roll_call_votes",
            "ExpressionAttributeValues": {":h": 0},
        }

    to_fetch = xml_rolls - existing_rolls
    parsed = _parse_bill_id(bill_id)
    if not parsed:
        return True, None, None
    congress, bill_type, bill_number = parsed

    existing_list = []
    try:
        rcv = item.get("roll_call_votes")
        existing_list = json.loads(rcv) if isinstance(rcv, str) else (rcv or [])
    except (TypeError, json.JSONDecodeError):
        pass
    if not isinstance(existing_list, list):
        existing_list = []

    new_entries = []
    for (session, roll) in sorted(to_fetch):
        try:
            members = _fetch_house_vote_members(congress, session, roll, api_key, key_index=key_index)
            new_entries.append({"roll": roll, "session": session, "members": members})
        except Exception as e:
            log_print(f"      ⚠️ Failed to fetch roll {session}/{roll} for {bill_id}: {e}")
            continue

    merged = existing_list + new_entries
    try:
        roll_call_number = int(merged[0]["roll"]) if merged else None
    except (TypeError, ValueError, KeyError):
        roll_call_number = merged[0].get("roll") if merged else None

    return True, None, {
        "bill_id": bill_id,
        "search_index_sk": search_index_sk,
        "UpdateExpression": "SET has_roll_call = :h, roll_call_number = :n, roll_call_votes = :v",
        "ExpressionAttributeValues": {
            ":h": 1,
            ":n": roll_call_number,
            ":v": json.dumps(merged),
        },
    }


# ============================================================================
# Main Execution
# ============================================================================

def main():
    log_print("=" * 80)
    log_print("Congress Bills Roll Call Maintenance Glue Job - Starting")
    log_print("=" * 80)

    if not BILLS_TABLE_NAME:
        raise ValueError("BILLS_TABLE_NAME job parameter not set")

    try:
        api_key_rotator = get_congress_api_keys()
        log_print(f"✅ Retrieved {api_key_rotator.get_key_count()} Congress API key(s) from Secrets Manager")
    except Exception as e:
        log_print(f"❌ Failed to retrieve API keys: {str(e)}")
        raise

    zip_s3_keys = resolve_zip_s3_keys()
    if zip_s3_keys:
        log_print("🔍 MODE: Roll call for this run's bills only (from ZIP S3 keys), with delta (skip unchanged)")
        log_print(f"   ZIP S3 keys: {len(zip_s3_keys)}")
        log_print("-" * 80)
        bill_ids_set = set()
        for zk in zip_s3_keys:
            try:
                bids = get_bill_ids_from_zip_s3_key(S3_BUCKET_NAME, zk)
                bill_ids_set.update(bids)
                log_print(f"   📦 {zk}: {len(bids)} bill(s)")
            except Exception as e:
                log_print(f"   ⚠️ Failed to read ZIP {zk}: {e}")
        roll_call_items = []
        for bill_id in bill_ids_set:
            try:
                item = bills_table.get_item(
                    Key={"bill_id": bill_id, "search_index_sk": bill_id}
                ).get("Item")
                if item:
                    roll_call_items.append(item)
            except Exception as e:
                log_print(f"   ⚠️ get_item {bill_id}: {e}")
        total_roll_call = len(roll_call_items)
        log_print(f"   Found {total_roll_call} bill item(s) in table for roll call delta.")
        use_delta = True
    else:
        log_print("🔍 MODE: Full table scan (all bills), full API fetch per bill")
        log_print("-" * 80)
        roll_call_items = []
        last_key = None
        while True:
            scan_params = {"FilterExpression": Attr("bill_id").exists() & ~Attr("bill_id").begins_with("SEARCH#")}
            if last_key:
                scan_params["ExclusiveStartKey"] = last_key
            response = bills_table.scan(**scan_params)
            roll_call_items.extend(response.get("Items", []))
            last_key = response.get("LastEvaluatedKey")
            if not last_key:
                break
        total_roll_call = len(roll_call_items)
        log_print(f"   Found {total_roll_call} bill items for roll call maintenance.")
        use_delta = False

    num_keys = api_key_rotator.get_key_count()
    log_print(f"   Using {min(ROLL_CALL_MAX_WORKERS, total_roll_call or 1)} workers.")
    roll_ok = 0
    roll_err = 0
    roll_skip = 0
    progress_lock = threading.Lock()
    last_logged = 0

    def _process_one(bill_item: Dict[str, Any]) -> Tuple[str, Optional[str], Optional[Dict]]:
        try:
            api_key, key_index = api_key_rotator.get_key_and_index()
            if use_delta:
                success, err, payload = process_bill_roll_call_delta(
                    bill_item, api_key, key_index=key_index
                )
            else:
                success, err, payload = process_bill_roll_call(
                    bill_item, BILLS_TABLE_NAME, api_key, key_index=key_index
                )
            if payload is None:
                return ("skip", None, None)
            if not success:
                return ("err", err, None)
            bills_table.update_item(
                Key={"bill_id": payload["bill_id"], "search_index_sk": payload["search_index_sk"]},
                UpdateExpression=payload["UpdateExpression"],
                ExpressionAttributeValues=payload["ExpressionAttributeValues"],
            )
            return ("ok", None, payload)
        except Exception as e:
            return ("err", str(e), None)

    workers = min(ROLL_CALL_MAX_WORKERS, total_roll_call or 1)
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_process_one, item): item for item in roll_call_items}
        for future in as_completed(futures):
            bill_item = futures[future]
            try:
                status, err_msg, _ = future.result()
            except Exception as e:
                status, err_msg = "err", str(e)
            with progress_lock:
                if status == "ok":
                    roll_ok += 1
                elif status == "err":
                    roll_err += 1
                    if err_msg:
                        log_print(f"      ❌ Roll call {bill_item.get('bill_id', '?')}: {err_msg}")
                else:
                    roll_skip += 1
                done = roll_ok + roll_err + roll_skip
                if done >= last_logged + 50:
                    last_logged = (done // 50) * 50
                    log_print(f"      Roll call: {done}/{total_roll_call} (ok: {roll_ok}, err: {roll_err}, skip: {roll_skip})")
    log_print(f"✅ Roll call maintenance done. Updated {roll_ok}, errors {roll_err}, skipped {roll_skip}.")
    log_print("")
    log_print("=" * 80)
    log_print("✅ Roll call maintenance job completed successfully!")
    log_print("=" * 80)

    job.commit()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        error_msg = f"CRITICAL ERROR in Congress bills roll call maintenance job: {str(e)}"
        error_traceback = traceback.format_exc()
        log_print(f"❌ {error_msg}")
        log_print(f"❌ Traceback:\n{error_traceback}")
        logger.error(f"❌ {error_msg}", exc_info=True)
        logger.error(f"❌ Traceback:\n{error_traceback}")
        raise



