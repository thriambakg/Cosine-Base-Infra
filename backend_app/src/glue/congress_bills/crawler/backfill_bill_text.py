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
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
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
# Use slightly under to avoid 429s when multiple keys share the same account or limits.
CONGRESS_API_MAX_REQUESTS_PER_HOUR = int(args.get('CONGRESS_API_MAX_REQUESTS_PER_HOUR', '4800'))
ROLL_CALL_MAX_WORKERS = int(args.get('ROLL_CALL_MAX_WORKERS', '8'))

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


# ============================================================================
# Main Execution
# ============================================================================

def main():
    log_print("=" * 80)
    log_print("Congress Bills Roll Call Maintenance Glue Job - Starting")
    log_print("🔍 MODE: Roll call maintenance (all bills)")
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
    # Roll call maintenance (all bill rows, exclude search indices)
    # -------------------------------------------------------------------------
    log_print("")
    log_print("📋 Roll call maintenance - scanning all bills (excluding SEARCH#)...")
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
    total_roll_call = len(roll_call_items)
    num_keys = api_key_rotator.get_key_count()
    log_print(f"   Found {total_roll_call} bill items for roll call maintenance.")
    log_print(f"   Using {min(ROLL_CALL_MAX_WORKERS, total_roll_call)} workers, max {CONGRESS_API_MAX_REQUESTS_PER_HOUR} req/hour per key ({num_keys} key(s) = {num_keys * CONGRESS_API_MAX_REQUESTS_PER_HOUR} total/hour).")
    roll_ok = 0
    roll_err = 0
    progress_lock = threading.Lock()
    last_logged = 0

    def _process_one_roll_call(bill_item: Dict[str, Any]) -> Tuple[str, Optional[str], Optional[Dict]]:
        """Returns ('ok'|'err'|'skip', error_message_or_none, payload_or_none)."""
        try:
            api_key, key_index = api_key_rotator.get_key_and_index()
            success, err, payload = process_bill_roll_call(
                bill_item, BILLS_TABLE_NAME, api_key, key_index=key_index
            )
            if payload is None:
                return ('skip', None, None)
            if not success:
                return ('err', err, None)
            bills_table.update_item(
                Key={"bill_id": payload["bill_id"], "search_index_sk": payload["search_index_sk"]},
                UpdateExpression=payload["UpdateExpression"],
                ExpressionAttributeValues=payload["ExpressionAttributeValues"],
            )
            return ('ok', None, payload)
        except Exception as e:
            return ('err', str(e), None)

    workers = min(ROLL_CALL_MAX_WORKERS, total_roll_call or 1)
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_process_one_roll_call, item): item for item in roll_call_items}
        for future in as_completed(futures):
            bill_item = futures[future]
            try:
                status, err_msg, _ = future.result()
            except Exception as e:
                status, err_msg = 'err', str(e)
            with progress_lock:
                if status == 'ok':
                    roll_ok += 1
                elif status == 'err':
                    roll_err += 1
                    if err_msg:
                        log_print(f"      ❌ Roll call {bill_item.get('bill_id', '?')}: {err_msg}")
                done = roll_ok + roll_err
                if done >= last_logged + 50:
                    last_logged = (done // 50) * 50
                    log_print(f"      Roll call: {done}/{total_roll_call} (ok: {roll_ok}, err: {roll_err})")
    log_print(f"✅ Roll call maintenance done. Updated {roll_ok} bills with roll call data, {roll_err} errors.")
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



