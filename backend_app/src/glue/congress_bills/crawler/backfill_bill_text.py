"""
AWS Glue Job: Congress Bills Roll Call Maintenance
Daily job to refresh roll call (voter) data for bills in the fetcher's date range (no full table scan).

Behavior:
   - Accepts START_DATE, END_DATE, and optional SOURCE. Mirrors fetcher glue_script for date range and S3 path:
     when source is "scheduler" or both dates are null/empty, uses previous day 11:00 AM UTC to current day
     11:00 AM UTC (same window as fetcher) so the job finds the same downloads/{YYYYMMDD-YYYYMMDD}/ folder.
   - Builds S3 prefix downloads/{YYYYMMDD-YYYYMMDD}/ (format_date_range_path) and lists ZIP files there.
   - Derives bill_ids from ZIP contents (BILLSTATUS-*xml filenames); only these bills are processed.
   - For each bill: get_item from DynamoDB; compare recorded_votes_json (XML rolls) with roll_call_votes (existing).
   - If roll sets match -> skip (no API call). If different -> fetch only new rolls via API, merge into
     roll_call_votes, then update table (roll_call_number, roll_call_votes, has_roll_call).
   - SEARCH#VOTE index: for each bill updated, updates per-politician vote index items (PK SEARCH#VOTE#<politician_id>,
     SK VOTE). Attributes: display_name, bill_yea, bill_nea, bill_present, bill_not_voting (lists of bill_ids),
     roll_yea, roll_nea, roll_present, roll_not_voting (lists of roll ids "{congress}#{session}#{roll}").
     Voters matched via congress-legislators.csv. Congress.gov "Present" -> bill_present/roll_present,
     "Not Voting" -> bill_not_voting/roll_not_voting.
   - SEARCH#ROLL index: one item per roll call (PK SEARCH#ROLL, SK {congress}#{session}#{latest_action_date}#{roll}
     or legacy {congress}#{session}#{roll}) so roll calls can be listed and sorted by latest action date. Filled from
     (1) bill pass (recordedVotes on bills) and (2) house-vote
     list backfill: GET /house-vote/{congress}/{session} for any (congress, session, roll) not already written.
     Attributes: congress, session, roll, bill_id_associated, roll_display, members or members_oversize_s3_key.
   - Oversize: when roll_call_votes or SEARCH#VOTE/SEARCH#ROLL data would exceed DynamoDB item size, store in S3 oversize/
     and set roll_call_votes_oversize_s3_key (bill item) or vote_data_oversize_s3_key (SEARCH#VOTE item). Search/API
     should resolve these keys when present (gzip JSON in same bucket).
   - Uses same thread pool and per-key rate limiters as before.

Bill text is filled by the fetcher pipeline (bulk XML + SQS -> Lambda); no separate backfill.
"""


import sys
import json
import logging
import time
import threading
import re
import csv
import gzip
import zipfile
import xml.etree.ElementTree as ET
from io import BytesIO, StringIO
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
from typing import Dict, List, Any, Optional, Tuple, Set

import requests

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

import boto3
from boto3.dynamodb.conditions import Attr, Key

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
# Optional: START_DATE, END_DATE, SOURCE (passed from Step Function; mirror fetcher glue_script for S3 path)
def _get_opt_arg(name: str) -> str:
    for i, a in enumerate(sys.argv):
        if a == f"--{name}" and i + 1 < len(sys.argv):
            v = sys.argv[i + 1] or ""
            return "" if v in ("null", "None") else str(v).strip()
    return ""
START_DATE_ARG = _get_opt_arg("START_DATE")
END_DATE_ARG = _get_opt_arg("END_DATE")
SOURCE_ARG = _get_opt_arg("SOURCE")

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


def _normalize_date_to_yyyy_mm_dd(date_str: str) -> str:
    """Parse date in YYYY-MM-DD or MM/DD/YYYY and return YYYY-MM-DD."""
    s = (date_str or "").strip().split("T")[0]
    if not s:
        return ""
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
                    continue
    return s


def _format_date_range_path(start_date: str, end_date: str) -> str:
    """Format date range as YYYYMMDD-YYYYMMDD for S3 path. Accepts YYYY-MM-DD or MM/DD/YYYY."""
    start_norm = _normalize_date_to_yyyy_mm_dd(start_date)
    end_norm = _normalize_date_to_yyyy_mm_dd(end_date)
    start_dt = datetime.strptime(start_norm, "%Y-%m-%d")
    end_dt = datetime.strptime(end_norm, "%Y-%m-%d")
    start_formatted = start_dt.strftime("%Y%m%d")
    end_formatted = end_dt.strftime("%Y%m%d")
    return f"{start_formatted}-{end_formatted}"


def _get_start_end_dates() -> Tuple[str, str]:
    """
    Return (start_date, end_date) in YYYY-MM-DD for S3 path.
    Mirrors fetcher glue_script: when source is 'scheduler' or both dates null/empty,
    use previous day 11:00 AM UTC to current day 11:00 AM UTC so we point at the same
    downloads/{YYYYMMDD-YYYYMMDD}/ folder the fetcher wrote to.
    """
    start_raw = (START_DATE_ARG or "").strip()
    end_raw = (END_DATE_ARG or "").strip()
    source = (SOURCE_ARG or "").strip().lower()
    scheduler_mode = source == "scheduler" or (not start_raw and not end_raw)

    if scheduler_mode:
        # Same logic as fetcher: previous day 11:00 AM UTC to current day 11:00 AM UTC
        now = datetime.now(timezone.utc)
        previous_day_11am = now.replace(hour=11, minute=0, second=0, microsecond=0) - timedelta(days=1)
        current_day_11am = now.replace(hour=11, minute=0, second=0, microsecond=0)
        if now.hour < 11:
            start_date = previous_day_11am
            end_date = previous_day_11am
        else:
            start_date = previous_day_11am
            end_date = current_day_11am
        return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")

    # Explicit dates: normalize to YYYY-MM-DD (support ISO, YYYY-MM-DD, or MM/DD/YYYY)
    start_simple = start_raw.split("T")[0] if start_raw else ""
    end_simple = end_raw.split("T")[0] if end_raw else ""
    if not start_simple or not end_simple:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        return (start_simple or today), (end_simple or today)
    return _normalize_date_to_yyyy_mm_dd(start_simple), _normalize_date_to_yyyy_mm_dd(end_simple)


def _list_zip_s3_keys(prefix: str) -> List[str]:
    """List S3 keys under prefix that end with .zip."""
    keys = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET_NAME, Prefix=prefix):
        for obj in page.get("Contents") or []:
            k = obj.get("Key") or ""
            if k.endswith(".zip"):
                keys.append(k)
    return keys


def _bill_id_from_zip_filename(filename: str) -> Optional[str]:
    """Derive bill_id from ZIP member filename, e.g. BILLSTATUS-119hr123.xml -> 119-HR-123."""
    base = filename.split("/")[-1] if "/" in filename else filename
    m = re.match(r"BILLSTATUS-(\d+)([a-zA-Z]+)(\d+)\.xml", base, re.IGNORECASE)
    if not m:
        return None
    congress, bill_type, number = m.group(1), m.group(2).upper(), m.group(3)
    return f"{congress}-{bill_type}-{number}"


def _elem_text(elem: Optional[Any], tag: str) -> Optional[str]:
    """Get text of first child element with given tag (strip namespace from tag)."""
    if elem is None:
        return None
    for child in elem:
        local = child.tag.split("}")[-1] if "}" in str(child.tag) else child.tag
        if local == tag and child.text is not None:
            return child.text.strip() if isinstance(child.text, str) else str(child.text)
    return None


def _parse_bill_filter_date_from_xml(xml_bytes: bytes) -> Optional[datetime]:
    """
    Parse bill XML and return the date used for date-range filtering (same logic as fetcher):
    latest_action_date or introduced_date. Only bills with this date within the job's date range
    are processed. Returns datetime at start of day UTC, or None if no date or parse error.
    """
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        return None
    # GovInfo BILLSTATUS: root is billStatus, bill is under it
    bill_elem = root.find(".//{*}bill") if root is not None else None
    if bill_elem is None:
        bill_elem = root
    introduced_date = _elem_text(bill_elem, "introducedDate")
    latest_action_date = None
    latest_elem = bill_elem.find(".//{*}latestAction") if bill_elem is not None else None
    if latest_elem is not None:
        latest_action_date = _elem_text(latest_elem, "actionDate")
    if not latest_action_date and bill_elem is not None:
        actions_elem = bill_elem.find("{*}actions")
        if actions_elem is not None:
            items = actions_elem.findall("{*}item")
            if items:
                latest_action_date = _elem_text(items[0], "actionDate")
    filter_date_str = latest_action_date or introduced_date
    if not filter_date_str:
        return None
    try:
        if "T" in filter_date_str:
            bill_date = datetime.fromisoformat(filter_date_str.replace("Z", "+00:00"))
        else:
            bill_date = datetime.strptime(filter_date_str[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return bill_date.replace(hour=0, minute=0, second=0, microsecond=0)
    except (ValueError, AttributeError):
        return None


def _get_bill_ids_from_zip_content(
    zip_content: bytes,
    start_date_dt: Optional[datetime] = None,
    end_date_dt: Optional[datetime] = None,
) -> Set[str]:
    """
    Extract set of bill_ids from ZIP contents. If start_date_dt and end_date_dt are provided,
    only include bills whose filter date (latest_action_date or introduced_date from XML)
    falls within [start_date_dt, end_date_dt], matching the fetcher's date filtering so we
    only check roll calls for bills the fetcher would have written for this run.
    """
    bill_ids = set()
    with zipfile.ZipFile(BytesIO(zip_content), "r") as zf:
        for name in zf.namelist():
            if not name.lower().endswith(".xml"):
                continue
            bid = _bill_id_from_zip_filename(name)
            if not bid:
                continue
            if start_date_dt is not None and end_date_dt is not None:
                try:
                    with zf.open(name) as f:
                        xml_bytes = f.read()
                except Exception:
                    continue
                filter_date = _parse_bill_filter_date_from_xml(xml_bytes)
                if filter_date is None:
                    continue
                if not (start_date_dt <= filter_date <= end_date_dt):
                    continue
            bill_ids.add(bid)
    return bill_ids


def _house_rolls_from_recorded_votes_json(recorded_votes_json: Any) -> Set[Tuple[str, str]]:
    """From table's recorded_votes_json (from bulk XML), return set of (sessionNumber, rollNumber) for House only. Normalized to (str, str)."""
    out = set()
    if not recorded_votes_json:
        return out
    raw = recorded_votes_json
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            return out
    if not isinstance(raw, list):
        return out
    for v in raw:
        if not isinstance(v, dict):
            continue
        chamber = (v.get("chamber") or "").strip().upper()
        if chamber != "HOUSE":
            continue
        s = v.get("sessionNumber")
        r = v.get("rollNumber")
        if s is None or r is None:
            continue
        out.add((str(s), str(r)))
    return out


def _existing_rolls_from_roll_call_votes(roll_call_votes: Any) -> Set[Tuple[str, str]]:
    """From table's roll_call_votes, return set of (session, roll). Normalized to (str, str)."""
    out = set()
    if not roll_call_votes:
        return out
    raw = roll_call_votes
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            return out
    if not isinstance(raw, list):
        return out
    for entry in raw:
        if not isinstance(entry, dict):
            continue
        s = entry.get("session")
        r = entry.get("roll")
        if s is None or r is None:
            continue
        out.add((str(s), str(r)))
    return out


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


def _parse_vote_date(data: Dict[str, Any]) -> Optional[str]:
    """Extract vote date from API response: prefer updateDate, then startDate. Return YYYY-MM-DD or None."""
    if not isinstance(data, dict):
        return None
    for key in ("updateDate", "startDate", "date", "actionDate", "voteDate"):
        val = data.get(key)
        if not val:
            continue
        s = str(val).strip()
        if len(s) >= 10 and s[:10].replace("-", "").isdigit():
            return s[:10]
        if "T" in s:
            return s.split("T")[0][:10]
    return None


def _extract_members_from_vote_response(data: Any) -> List[Dict]:
    """Parse member list from house-vote API response (same shapes as _fetch_house_vote_members)."""
    results = None
    if isinstance(data, dict):
        results = data.get("results") or data.get("members") or data.get("memberVotes")
        if not isinstance(results, list):
            raw = data.get("houseRollCallVoteMemberVotes")
            if isinstance(raw, list):
                results = raw
            elif isinstance(raw, dict):
                for k in ("item", "memberVote", "memberVotes", "houseRollCallVoteMemberVote"):
                    val = raw.get(k)
                    if isinstance(val, list):
                        results = val
                        break
                    if isinstance(val, dict) and (val.get("voteCast") or val.get("bioguideID")):
                        results = [val]
                        break
            if results is None and raw:
                for _k, val in (raw or {}).items():
                    if isinstance(val, list) and val and isinstance(val[0], dict) and ("voteCast" in val[0] or "bioguideID" in val[0]):
                        results = val
                        break
        if not isinstance(results, list) and isinstance(data.get("houseVote"), dict):
            h = data["houseVote"]
            results = h.get("results") or h.get("members")
        if not isinstance(results, list):
            for key in ("voteMembers", "items", "votes"):
                cand = data.get(key)
                if isinstance(cand, list) and cand and isinstance(cand[0], dict) and ("voteCast" in cand[0] or "bioguideID" in cand[0]):
                    results = cand
                    break
    elif isinstance(data, list) and data and isinstance(data[0], dict) and ("voteCast" in data[0] or "bioguideID" in data[0]):
        results = data
    return results if isinstance(results, list) else []


def _fetch_house_vote_members(
    congress: str,
    session: int,
    roll_number: int,
    api_key: str,
    key_index: Optional[int] = None,
) -> Tuple[List[Dict], Optional[str]]:
    """
    Fetch house roll call member votes (paginated). Returns (members, vote_date_yyyy_mm_dd).
    Vote date is parsed from the same API response we use for members (updateDate/startDate on first page).
    No extra API call - we get the date from the existing /members response when the API includes it.
    """
    all_members = []
    vote_date = None
    offset = 0
    limit = 250
    while True:
        url = f"{API_BASE_URL}/house-vote/{congress}/{session}/{roll_number}/members"
        params = {"format": "json", "offset": offset, "limit": limit}
        data = make_api_request(url, params, api_key, key_index=key_index)
        if not data:
            break
        if offset == 0 and isinstance(data, dict):
            vote_date = _parse_vote_date(data)
        results = _extract_members_from_vote_response(data)
        if results:
            all_members.extend(results)
        pagination = data.get("pagination") if isinstance(data, dict) else {}
        count = pagination.get("count", 0) if isinstance(pagination, dict) else 0
        if isinstance(data, dict) and count and offset + limit >= count:
            break
        if not (isinstance(results, list) and len(results) == limit):
            break
        offset += limit
    return all_members, vote_date


def _fetch_house_vote_list(
    congress: int,
    session: int,
    api_key: str,
    key_index: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    GET /house-vote/{congress}/{session} - list all roll call votes for that Congress and session.
    Returns list of vote summary dicts (rollCallNumber, sessionNumber, congress, legislationUrl, etc.). Paginated (limit 250).
    Congress.gov v3 may wrap the list in 'votes', 'houseVotes', 'results', or 'items'.
    """
    out: List[Dict[str, Any]] = []
    offset = 0
    limit = 250
    _logged_keys = False
    while True:
        url = f"{API_BASE_URL}/house-vote/{congress}/{session}"
        params = {"format": "json", "offset": offset, "limit": limit}
        data = make_api_request(url, params, api_key, key_index=key_index)
        if not data:
            break
        items = None
        if isinstance(data, dict):
            items = data.get("votes") or data.get("houseVotes") or data.get("results") or data.get("items")
            if isinstance(items, dict):
                items = items.get("item", items.get("vote", items.get("votes", [])))
            if not isinstance(items, list) and isinstance(data.get("houseVote"), dict):
                items = data["houseVote"].get("votes") or data["houseVote"].get("item")
            if not isinstance(items, list):
                for key, val in data.items():
                    if key in ("pagination", "request"):
                        continue
                    if isinstance(val, list) and val and isinstance(val[0], dict):
                        if "rollCallNumber" in val[0] or "legislationNumber" in val[0] or "rollNumber" in val[0]:
                            items = val
                            break
            if not isinstance(items, list) and offset == 0 and not _logged_keys:
                log_print(f"      House-vote list response keys (Congress {congress} Session {session}): {list(data.keys())}")
                _logged_keys = True
            if not isinstance(items, list) and isinstance(data, dict) and "rollCallNumber" in data:
                items = [data]
        if isinstance(items, list):
            out.extend(items)
        elif isinstance(data, list):
            out.extend(data)
        elif isinstance(data, dict) and "rollCallNumber" in data:
            out.append(data)
        if not isinstance(data, dict):
            break
        num_items = len(items) if isinstance(items, list) else 0
        if num_items < limit:
            break
        offset += limit
    return out


def _bill_id_from_legislation(
    congress: int,
    legislation_type: Optional[Any],
    legislation_number: Optional[Any],
    legislation_url: Optional[str],
) -> Optional[str]:
    """Derive bill_id (e.g. 119-HR-30) from API vote list item. Uses legislationType+legislationNumber or parses legislationUrl."""
    if legislation_type and legislation_number is not None:
        lt = str(legislation_type).strip().upper()
        if lt in ("HR", "S", "HJRES", "SJRES", "HCONRES", "SCONRES", "HRES", "SRES"):
            return f"{congress}-{lt}-{legislation_number}"
    if legislation_url:
        m = re.search(r"/bill/(\d+)/([^/]+)/(\d+)", str(legislation_url))
        if m:
            c, kind, num = m.group(1), m.group(2), m.group(3)
            type_map = {"house-bill": "HR", "senate-bill": "S", "house-joint-resolution": "HJRES", "senate-joint-resolution": "SJRES"}
            bt = type_map.get(kind.lower(), kind.upper()[:2] if len(kind) >= 2 else "HR")
            return f"{c}-{bt}-{num}"
    return None


def _fetch_new_rolls_only(
    bill_id: str,
    to_fetch: Set[Tuple[str, str]],
    api_key: str,
    key_index: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Fetch member data for only the (session, roll) pairs in to_fetch. Returns list of {roll, session, members}."""
    parsed = _parse_bill_id(bill_id)
    if not parsed or not to_fetch:
        return []
    congress, _bt, _bn = parsed
    result = []
    for session_s, roll_s in sorted(to_fetch):
        session = int(session_s) if session_s.isdigit() else 0
        roll = int(roll_s) if roll_s.isdigit() else 0
        if session <= 0 or roll <= 0:
            continue
        members, vote_date = _fetch_house_vote_members(congress, session, roll, api_key, key_index=key_index)
        entry = {"roll": roll, "session": session, "members": members}
        if vote_date:
            entry["vote_date"] = vote_date
        result.append(entry)
    return result


def _merge_roll_call_votes(
    existing_votes: List[Dict],
    xml_rolls: Set[Tuple[str, str]],
    new_rolls_data: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Keep existing entries whose (session, roll) is in xml_rolls; drop others; append new_rolls_data. Returns merged list."""
    kept = []
    for entry in existing_votes or []:
        if not isinstance(entry, dict):
            continue
        s, r = entry.get("session"), entry.get("roll")
        if s is None or r is None:
            continue
        if (str(s), str(r)) in xml_rolls:
            kept.append(entry)
    # Append new rolls (no duplicates; new_rolls_data is for to_fetch only)
    seen = {(str(e.get("session")), str(e.get("roll"))) for e in kept}
    for e in new_rolls_data:
        s, r = e.get("session"), e.get("roll")
        if (str(s), str(r)) in seen:
            continue
        seen.add((str(s), str(r)))
        kept.append(e)
    return kept


# ---------------------------------------------------------------------------
# Legislators CSV and SEARCH#VOTE index (match voters to politicians; Congress.gov Present/Not Voting -> separate columns)
# ---------------------------------------------------------------------------

def load_legislators_csv() -> List[Dict[str, Any]]:
    """
    Load congress-legislators CSV from S3 (bills data bucket root).
    Returns list of politician dicts with name, bioguide_id, first_name, last_name, party, state, alternativeNames.
    """
    try:
        if not S3_BUCKET_NAME:
            log_print("⚠️ S3_BUCKET_NAME not set, skipping legislators CSV load")
            return []
        response = s3_client.get_object(
            Bucket=S3_BUCKET_NAME,
            Key="congress-legislators.csv",
        )
        csv_content = response["Body"].read().decode("utf-8")
        reader = csv.DictReader(StringIO(csv_content))
        politicians = []
        for row in reader:
            name_parts = []
            if row.get("first_name"):
                name_parts.append(row["first_name"])
            if row.get("middle_name"):
                name_parts.append(row["middle_name"])
            if row.get("last_name"):
                name_parts.append(row["last_name"])
            if row.get("suffix"):
                name_parts.append(row["suffix"])
            primary_name = (row.get("full_name") or "").strip() or (" ".join(name_parts) if name_parts else "")
            alt_names = []
            if row.get("nickname"):
                alt_names.append(row["nickname"])
            if row.get("full_name") and row.get("full_name").strip() != primary_name:
                alt_names.append(row["full_name"].strip())
            leg_type = (row.get("type") or "").lower().strip()
            position = "Senate" if leg_type == "sen" else ("House" if leg_type == "rep" else leg_type)
            party_full = (row.get("party") or "").strip()
            party = party_full[0].upper() if party_full else ""
            politicians.append({
                "name": primary_name,
                "first_name": (row.get("first_name") or "").strip(),
                "last_name": (row.get("last_name") or "").strip(),
                "party": party,
                "state": (row.get("state") or "").strip(),
                "position": position,
                "alternativeNames": alt_names,
                "bioguide_id": (row.get("bioguide_id") or "").strip() or None,
            })
        return politicians
    except Exception as e:
        log_print(f"❌ Error loading congress-legislators CSV: {e}")
        return []


def _fuzzy_match_name(name: str, politician: Dict[str, Any]) -> float:
    """Score 0..1 for name vs politician (handles Rep. Last, First [R-ST] style)."""
    name_norm = re.sub(r"^(?:rep\.|sen\.|representative|senator)\s+", "", name.lower().strip(), flags=re.IGNORECASE)
    name_norm = re.sub(r"\s*\[[^\]]+\]", "", name_norm).strip()
    pol_norm = (politician.get("name") or "").lower().strip()
    if name_norm == pol_norm:
        return 1.0
    for alt in politician.get("alternativeNames") or []:
        if name_norm == (alt or "").lower().strip():
            return 1.0
    if "," in name_norm:
        parts = [p.strip() for p in name_norm.split(",", 1)]
        if len(parts) == 2:
            rev = f"{parts[1]} {parts[0]}".strip()
            if rev == pol_norm:
                return 1.0
            return max(SequenceMatcher(None, name_norm, pol_norm).ratio(), SequenceMatcher(None, rev, pol_norm).ratio())
    return SequenceMatcher(None, name_norm, pol_norm).ratio()


def _find_politician_by_bioguide(bioguide_id: str, politicians: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not bioguide_id or not politicians:
        return None
    bid = str(bioguide_id).strip().upper()
    for p in politicians:
        pb = p.get("bioguide_id")
        if pb and str(pb).strip().upper() == bid:
            return p
    return None


def _build_politicians_by_bioguide(politicians: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """O(1) lookup by bioguide_id; built once so per-member resolution is not a bottleneck."""
    out: Dict[str, Dict[str, Any]] = {}
    for p in politicians or []:
        bid = (p.get("bioguide_id") or "").strip()
        if bid:
            out[bid.upper()] = p
    return out


def _find_matching_politician(
    name: str,
    politicians: List[Dict[str, Any]],
    first_name: str = "",
    last_name: str = "",
    party: str = "",
    state: str = "",
) -> Optional[Dict[str, Any]]:
    if not name or not politicians:
        return None
    first_n = first_name.strip().lower() if first_name else ""
    last_n = last_name.strip().lower() if last_name else ""
    party_n = (party.strip().upper() or " ")[0] if party else ""
    state_n = state.strip().upper() if state else ""
    best = None
    best_score = 0.0
    for p in politicians:
        score = 0.0
        n_match = _fuzzy_match_name(name, p)
        if n_match > 0.7:
            score += n_match * 0.5
        if first_n:
            pf = (p.get("first_name") or "").strip().lower()
            if pf == first_n:
                score += 0.2
            elif pf and (first_n in pf or pf in first_n):
                score += 0.15
        if last_n:
            pl = (p.get("last_name") or "").strip().lower()
            if pl == last_n:
                score += 0.2
            elif pl and (last_n in pl or pl in last_n):
                score += 0.15
        if party_n and party_n != " ":
            if (p.get("party") or "").strip().upper() and (p.get("party") or "").strip().upper()[0] == party_n:
                score += 0.1
        if state_n and (p.get("state") or "").strip().upper() == state_n:
            score += 0.1
        if score > best_score:
            best_score = score
            best = p
    threshold = 0.6 if (first_n or last_n or party_n or state_n) else 0.7
    return best if best and best_score >= threshold else None


def _resolve_politician_id_and_name(
    member: Dict[str, Any],
    politicians: List[Dict[str, Any]],
    politicians_by_bioguide: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Tuple[Optional[str], Optional[str]]:
    """
    Resolve API member to (politician_id, display_name) for SEARCH#VOTE PK and name.
    Uses O(1) bioguide lookup when politicians_by_bioguide is provided; else list scan (fallback).
    """
    bioguide = (member.get("bioguideID") or member.get("bioguide_id") or "").strip()
    first = (member.get("firstName") or member.get("first_name") or "").strip()
    last = (member.get("lastName") or member.get("last_name") or "").strip()
    name = (member.get("name") or "").strip() or f"{first} {last}".strip()
    party = (member.get("voteParty") or member.get("party") or "").strip()
    state = (member.get("voteState") or member.get("state") or "").strip()
    if bioguide:
        pol = None
        if politicians_by_bioguide:
            pol = politicians_by_bioguide.get(bioguide.upper())
        if pol is None and politicians:
            pol = _find_politician_by_bioguide(bioguide, politicians)
        if pol:
            return (pol.get("bioguide_id") or bioguide, pol.get("name") or name or bioguide)
        return (bioguide, name or bioguide)
    if name or first or last:
        pol = _find_matching_politician(name or f"{first} {last}", politicians, first_name=first, last_name=last, party=party, state=state)
        if pol:
            pid = pol.get("bioguide_id") or ("NAME#" + re.sub(r"[^A-Za-z0-9]", "_", (pol.get("name") or name).strip()))
            return (pid, pol.get("name") or name)
        fallback_id = "NAME#" + re.sub(r"[^A-Za-z0-9]", "_", (name or f"{first}_{last}").strip()) if (name or first or last) else None
        return (fallback_id, name or f"{first} {last}".strip() or None)
    return (None, None)


def _vote_cast_to_bucket(vote_cast: Any) -> Optional[str]:
    """
    Map Congress.gov voteCast to index bucket. Yea/Yes/Aye -> yea; Nay/No -> nea;
    Present -> present; Not Voting -> not_voting. House uses 'Aye'/'No'; Senate may use 'Yea'/'Nay'.
    Unknown values are mapped to not_voting so no member is dropped; first occurrence is logged.
    """
    global _logged_unknown_vote_cast
    if vote_cast is None:
        return None
    v = str(vote_cast).strip()
    if not v:
        return None
    v_lower = v.lower()
    if v_lower in ("yea", "yes", "aye"):
        return "yea"
    if v_lower in ("nay", "no"):
        return "nea"
    if v_lower == "present":
        return "present"
    # Any variant containing "not voting" -> not_voting (do not map to present)
    if v_lower in ("not voting", "not voting (present)", "not voting, present", "present (not voting)", "present, not voting"):
        return "not_voting"
    # Fallback: treat unknown voteCast as not_voting so we still index the member; log once per run
    if v_lower not in _logged_unknown_vote_cast:
        _logged_unknown_vote_cast.add(v_lower)
        log_print(f"      [SEARCH#VOTE] Unmapped voteCast (treated as not_voting): {repr(vote_cast)}")
    return "not_voting"


# DynamoDB item size limit 400KB; use oversize/ folder for large roll_call_votes or SEARCH#VOTE data (mirror fetcher)
_DYNAMODB_ITEM_SIZE_LIMIT = 400 * 1024
_OVERSIZE_SAFE_SIZE = int(_DYNAMODB_ITEM_SIZE_LIMIT * 0.85)  # stay under limit with margin


def _store_roll_call_votes_to_s3(bill_id: str, roll_call_votes: List[Dict[str, Any]]) -> str:
    """Store roll_call_votes to S3 oversize/ folder. Returns S3 key (e.g. oversize/119-HR-1-roll_call_votes.json.gz)."""
    s3_key = f"oversize/{bill_id}-roll_call_votes.json.gz"
    json_bytes = json.dumps(roll_call_votes, default=str).encode("utf-8")
    compressed = gzip.compress(json_bytes)
    s3_client.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=s3_key,
        Body=compressed,
        ContentType="application/json",
        ContentEncoding="gzip",
    )
    log_print(f"      💾 Stored oversized roll_call_votes for {bill_id} to S3: {s3_key} ({len(compressed):,} bytes compressed)")
    return s3_key


def _store_vote_data_to_s3(
    pk: str,
    bill_yea: List[str],
    bill_nea: List[str],
    bill_present: List[str],
    bill_not_voting: List[str],
    roll_yea: List[str],
    roll_nea: List[str],
    roll_present: List[str],
    roll_not_voting: List[str],
) -> str:
    """Store SEARCH#VOTE bill_/roll_ lists to S3 oversize/. Returns S3 key. pk is SEARCH#VOTE#<pid>."""
    safe_key = pk.replace("#", "-") + ".json.gz"
    s3_key = f"oversize/{safe_key}"
    payload = {
        "bill_yea": bill_yea,
        "bill_nea": bill_nea,
        "bill_present": bill_present,
        "bill_not_voting": bill_not_voting,
        "roll_yea": roll_yea,
        "roll_nea": roll_nea,
        "roll_present": roll_present,
        "roll_not_voting": roll_not_voting,
    }
    json_bytes = json.dumps(payload).encode("utf-8")
    compressed = gzip.compress(json_bytes)
    s3_client.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=s3_key,
        Body=compressed,
        ContentType="application/json",
        ContentEncoding="gzip",
    )
    return s3_key


def _load_vote_data_from_item(
    existing_item: Dict[str, Any],
) -> Tuple[List[str], List[str], List[str], List[str], List[str], List[str], List[str], List[str]]:
    """Get bill_yea, bill_nea, bill_present, bill_not_voting, roll_yea, roll_nea, roll_present, roll_not_voting."""
    def _list(key: str, legacy: Optional[str] = None) -> List[str]:
        val = existing_item.get(key) or (existing_item.get(legacy) if legacy else [])
        return list(val or [])

    bill_yea = _list("bill_yea", "yea")
    bill_nea = _list("bill_nea", "nea")
    bill_present = _list("bill_present")
    bill_not_voting = _list("bill_not_voting")
    roll_yea = _list("roll_yea")
    roll_nea = _list("roll_nea")
    roll_present = _list("roll_present")
    roll_not_voting = _list("roll_not_voting")

    s3_key = (existing_item.get("vote_data_oversize_s3_key") or "").strip()
    if s3_key:
        try:
            resp = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=s3_key)
            body = resp.get("Body")
            raw = gzip.decompress(body.read()) if body else b""
            data = json.loads(raw.decode("utf-8"))
            bill_yea = list(data.get("bill_yea") or data.get("yea") or [])
            bill_nea = list(data.get("bill_nea") or data.get("nea") or [])
            bill_present = list(data.get("bill_present") or [])
            bill_not_voting = list(data.get("bill_not_voting") or [])
            roll_yea = list(data.get("roll_yea") or [])
            roll_nea = list(data.get("roll_nea") or [])
            roll_present = list(data.get("roll_present") or [])
            roll_not_voting = list(data.get("roll_not_voting") or [])
        except Exception as e:
            log_print(f"      ⚠️ Failed to load vote data from {s3_key}: {str(e)[:120]}")
            # Do not return in-item lists (empty for oversize items): re-raise so caller does not overwrite with partial data.
            raise
    return bill_yea, bill_nea, bill_present, bill_not_voting, roll_yea, roll_nea, roll_present, roll_not_voting


def _store_roll_members_to_s3(congress: str, session: int, roll: int, members: List[Dict[str, Any]]) -> str:
    """Store roll call members list to S3 oversize/. Returns S3 key."""
    s3_key = f"oversize/SEARCH-ROLL-{congress}-{session}-{roll}.json.gz"
    json_bytes = json.dumps(members, default=str).encode("utf-8")
    compressed = gzip.compress(json_bytes)
    s3_client.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=s3_key,
        Body=compressed,
        ContentType="application/json",
        ContentEncoding="gzip",
    )
    return s3_key


def _load_existing_roll_keys_for_congress(table: Any, congress: int) -> Set[Tuple[str, int, int]]:
    """
    Query SEARCH#ROLL for all items with PK=SEARCH#ROLL and SK begins_with "{congress}#".
    Returns set of (str(congress), session, roll) so the second pass can skip rolls we already have.
    """
    out: Set[Tuple[str, int, int]] = set()
    pk = "SEARCH#ROLL"
    sk_prefix = f"{congress}#"
    try:
        pagination_kwargs = {}
        while True:
            resp = table.query(
                KeyConditionExpression=Key("bill_id").eq(pk) & Key("search_index_sk").begins_with(sk_prefix),
                ProjectionExpression="search_index_sk, #s, #r",
                ExpressionAttributeNames={"#s": "session", "#r": "roll"},
                **pagination_kwargs,
            )
            for item in resp.get("Items") or []:
                sk = item.get("search_index_sk") or ""
                session_val = item.get("session")
                roll_val = item.get("roll")
                if session_val is not None and roll_val is not None:
                    sess_int = int(session_val) if not isinstance(session_val, int) else session_val
                    roll_int = int(roll_val) if not isinstance(roll_val, int) else roll_val
                    out.add((str(congress), sess_int, roll_int))
            next_key = resp.get("LastEvaluatedKey")
            if not next_key:
                break
            pagination_kwargs = {"ExclusiveStartKey": next_key}
    except Exception as e:
        log_print(f"      ⚠️ Query existing SEARCH#ROLL for congress {congress}: {str(e)[:120]}")
    return out


def _write_one_roll_item(
    congress: str,
    session_int: int,
    roll_int: int,
    members: List[Dict[str, Any]],
    bill_id_associated: str,
    table: Any,
    latest_action_date: Optional[str] = None,
) -> None:
    """Write a single SEARCH#ROLL item. Used by bill pass and by house-vote-list backfill.
    SK is {congress}#{date}#{session}#{roll} so begins_with("119#") + ScanIndexForward=False
    returns items in true date-descending order (newest first). Assumes empty table / fresh backfill.
    """
    date_part = "0000-00-00"
    if latest_action_date and str(latest_action_date).strip():
        d = str(latest_action_date).strip()[:10]
        if len(d) >= 10 and d.replace("-", "").isdigit():
            date_part = d
    sk = f"{congress}#{date_part}#{session_int}#{roll_int}"
    date_attr = date_part if date_part != "0000-00-00" else None
    roll_display = f"Roll no. {roll_int}"
    item = {
        "bill_id": "SEARCH#ROLL",
        "search_index_sk": sk,
        "search_type": "ROLL",
        "search_value": sk,
        "congress": int(congress),
        "session": session_int,
        "roll": roll_int,
        "bill_id_associated": bill_id_associated,
        "roll_display": roll_display,
        "members": members,
        "is_search_index": True,
    }
    if date_attr:
        item["latest_action_date"] = date_attr
    approx = len(json.dumps(item, default=str))
    if approx > _OVERSIZE_SAFE_SIZE:
        s3_key = _store_roll_members_to_s3(congress, session_int, roll_int, members)
        item = {
            "bill_id": "SEARCH#ROLL",
            "search_index_sk": sk,
            "search_type": "ROLL",
            "search_value": sk,
            "congress": int(congress),
            "session": session_int,
            "roll": roll_int,
            "bill_id_associated": bill_id_associated,
            "roll_display": roll_display,
            "members_oversize_s3_key": s3_key,
            "is_search_index": True,
        }
        if date_attr:
            item["latest_action_date"] = date_attr
    try:
        table.put_item(Item=item)
    except Exception as e:
        log_print(f"      ⚠️ SEARCH#ROLL put failed for {sk}: {str(e)[:150]}")


def update_search_roll_index_for_bill(
    bill_id: str,
    roll_call_votes: List[Dict[str, Any]],
    table: Any,
    rolls_written: Optional[Set[Tuple[str, int, int]]] = None,
    roll_write_lock: Optional[threading.Lock] = None,
) -> None:
    """
    Write one SEARCH#ROLL item per roll call. SK = {congress}#{date}#{session}#{roll}
    so begins_with("119#") + ScanIndexForward=False returns newest first.
    If rolls_written is provided, skips (congress, session, roll) already in the set to avoid
    duplicate SEARCH#ROLL items when the same roll appears on multiple bills. Use roll_write_lock
    when called from multiple threads so only one thread writes each (c, s, r).
    """
    parsed = _parse_bill_id(bill_id)
    if not parsed:
        return
    congress, _bt, _bn = parsed
    bill_latest_date = None
    try:
        resp = table.get_item(Key={"bill_id": bill_id, "search_index_sk": bill_id})
        item = resp.get("Item")
        if item and not (str(item.get("bill_id") or "").startswith("SEARCH#")):
            d = item.get("latest_action_date")
            if d is not None:
                bill_latest_date = str(d).strip()[:10] if hasattr(d, "strip") else str(d)[:10]
    except Exception:
        pass
    for entry in roll_call_votes or []:
        if not isinstance(entry, dict):
            continue
        session = entry.get("session")
        roll = entry.get("roll")
        members = entry.get("members") or []
        if session is None or roll is None:
            continue
        session_int = int(session) if isinstance(session, (int, float)) else (int(session) if str(session).isdigit() else None)
        roll_int = int(roll) if isinstance(roll, (int, float)) else (int(roll) if str(roll).isdigit() else None)
        if session_int is None or roll_int is None:
            continue
        key = (congress, session_int, roll_int)
        if rolls_written is not None:
            if roll_write_lock is not None:
                with roll_write_lock:
                    if key in rolls_written:
                        continue
                    rolls_written.add(key)
            else:
                if key in rolls_written:
                    continue
                rolls_written.add(key)
        vote_date = entry.get("vote_date") or bill_latest_date
        _write_one_roll_item(congress, session_int, roll_int, members, bill_id, table, latest_action_date=vote_date)


# DynamoDB batch limits so SEARCH#VOTE updates don't become a bottleneck (only API should be).
_BATCH_GET_MAX = 100
_BATCH_WRITE_MAX = 25

# Track unknown voteCast values logged once per run (avoid log spam).
_logged_unknown_vote_cast: Set[str] = set()


def ensure_search_vote_items_for_legislators(politicians: List[Dict[str, Any]], table: Any) -> None:
    """
    Ensure every legislator in the CSV has a SEARCH#VOTE item (PK = SEARCH#VOTE#<pid>, SK = VOTE).
    Creates missing items with empty bill_yea, bill_nea, bill_present, bill_not_voting, roll_yea, roll_nea, roll_present, roll_not_voting.
    Idempotent; does not overwrite existing items.
    """
    if not politicians:
        return
    pids_with_names: List[Tuple[str, str]] = []
    for p in politicians:
        pid = (p.get("bioguide_id") or "").strip()
        if not pid:
            pid = "NAME#" + re.sub(r"[^A-Za-z0-9]", "_", (p.get("name") or "unknown").strip())
        display_name = (p.get("name") or "").strip() or pid
        pids_with_names.append((pid, display_name))
    client = table.meta.client
    table_name = table.name
    keys = [{"bill_id": f"SEARCH#VOTE#{pid}", "search_index_sk": "VOTE"} for pid, _ in pids_with_names]
    existing_pids: Set[str] = set()
    for i in range(0, len(keys), _BATCH_GET_MAX):
        chunk = keys[i : i + _BATCH_GET_MAX]
        try:
            resp = client.batch_get_item(RequestItems={table_name: {"Keys": chunk}})
            for item in resp.get("Responses", {}).get(table_name, []):
                pk = (item.get("bill_id") or "").strip()
                if pk.startswith("SEARCH#VOTE#"):
                    existing_pids.add(pk.replace("SEARCH#VOTE#", "", 1))
            unprocessed = resp.get("UnprocessedKeys", {}).get(table_name, {}).get("Keys", [])
            while unprocessed:
                time.sleep(0.2)
                resp = client.batch_get_item(RequestItems={table_name: {"Keys": unprocessed}})
                for item in resp.get("Responses", {}).get(table_name, []):
                    pk = (item.get("bill_id") or "").strip()
                    if pk.startswith("SEARCH#VOTE#"):
                        existing_pids.add(pk.replace("SEARCH#VOTE#", "", 1))
                unprocessed = resp.get("UnprocessedKeys", {}).get(table_name, {}).get("Keys", [])
        except Exception as e:
            log_print(f"      ⚠️ SEARCH#VOTE seed batch get failed: {str(e)[:150]}")
            return
    to_create = [(pid, name) for pid, name in pids_with_names if pid not in existing_pids]
    if not to_create:
        log_print(f"   All {len(politicians)} legislators already have SEARCH#VOTE items.")
        return
    put_items: List[Dict[str, Any]] = []
    for pid, display_name in to_create:
        put_items.append({
            "bill_id": f"SEARCH#VOTE#{pid}",
            "search_index_sk": "VOTE",
            "search_type": "VOTE",
            "search_value": display_name,
            "display_name": display_name,
            "bill_yea": [],
            "bill_nea": [],
            "bill_present": [],
            "bill_not_voting": [],
            "roll_yea": [],
            "roll_nea": [],
            "roll_present": [],
            "roll_not_voting": [],
            "is_search_index": True,
        })
    for i in range(0, len(put_items), _BATCH_WRITE_MAX):
        chunk = put_items[i : i + _BATCH_WRITE_MAX]
        write_reqs = [{"PutRequest": {"Item": item}} for item in chunk]
        try:
            resp = client.batch_write_item(RequestItems={table_name: write_reqs})
            unprocessed = resp.get("UnprocessedItems", {}).get(table_name, [])
            while unprocessed:
                time.sleep(0.2)
                resp = client.batch_write_item(RequestItems={table_name: unprocessed})
                unprocessed = resp.get("UnprocessedItems", {}).get(table_name, [])
        except Exception as e:
            log_print(f"      ⚠️ SEARCH#VOTE seed batch write failed: {str(e)[:150]}")
            return
    log_print(f"   Seeded {len(to_create)} SEARCH#VOTE item(s) for legislators (total legislators: {len(politicians)}).")


def update_search_vote_index_for_bill(
    bill_id: str,
    roll_call_votes: List[Dict[str, Any]],
    politicians: List[Dict[str, Any]],
    table: Any,
    politicians_by_bioguide: Optional[Dict[str, Dict[str, Any]]] = None,
) -> None:
    """
    For a bill's roll_call_votes, update each voter's SEARCH#VOTE item: PK = SEARCH#VOTE#<politician_id>, SK = VOTE;
    attributes bill_yea, bill_nea, bill_present, bill_not_voting and roll_yea, roll_nea, roll_present, roll_not_voting
    (lists of bill/roll ids). Merges with existing; idempotent. Uses BatchGetItem + BatchWriteItem.
    """
    parsed = _parse_bill_id(bill_id)
    congress = int(parsed[0]) if parsed and parsed[0].isdigit() else None
    if congress is None and (roll_call_votes or []):
        log_print(f"      [SEARCH#VOTE] bill_id unparseable (roll_ids will be missing): {repr(bill_id)}")
    updates: Dict[str, Dict[str, Any]] = {}
    n_members_total = 0
    n_skipped_no_bucket = 0
    n_skipped_no_pid = 0
    for entry in roll_call_votes or []:
        if not isinstance(entry, dict):
            continue
        session_raw = entry.get("session")
        roll_raw = entry.get("roll")
        sess_int = int(session_raw) if session_raw is not None and str(session_raw).isdigit() else (int(session_raw) if isinstance(session_raw, (int, float)) else None)
        roll_int = int(roll_raw) if roll_raw is not None and str(roll_raw).isdigit() else (int(roll_raw) if isinstance(roll_raw, (int, float)) else None)
        if congress is None or sess_int is None or roll_int is None:
            roll_id = None
        else:
            roll_id = f"{congress}#{sess_int}#{roll_int}"
        for member in entry.get("members") or []:
            if not isinstance(member, dict):
                continue
            n_members_total += 1
            bucket = _vote_cast_to_bucket(member.get("voteCast"))
            if not bucket:
                n_skipped_no_bucket += 1
                continue
            pid, display_name = _resolve_politician_id_and_name(
                member, politicians, politicians_by_bioguide=politicians_by_bioguide
            )
            if not pid:
                n_skipped_no_pid += 1
                continue
            if pid not in updates:
                updates[pid] = {
                    "bill_yea": set(), "bill_nea": set(), "bill_present": set(), "bill_not_voting": set(),
                    "roll_yea": set(), "roll_nea": set(), "roll_present": set(), "roll_not_voting": set(),
                    "display_name": display_name or "",
                }
            updates[pid][f"bill_{bucket}"].add(bill_id)
            if roll_id:
                updates[pid][f"roll_{bucket}"].add(roll_id)
            if display_name and not updates[pid]["display_name"]:
                updates[pid]["display_name"] = display_name
    if n_skipped_no_bucket or n_skipped_no_pid:
        log_print(f"      [SEARCH#VOTE] bill {bill_id}: members={n_members_total}, indexed={len(updates)}, skipped_no_vote_bucket={n_skipped_no_bucket}, skipped_no_pid={n_skipped_no_pid}")
    if not updates:
        return
    client = table.meta.client
    table_name = table.name
    keys = [{"bill_id": f"SEARCH#VOTE#{pid}", "search_index_sk": "VOTE"} for pid in updates]
    existing: Dict[str, Dict[str, Any]] = {}
    for i in range(0, len(keys), _BATCH_GET_MAX):
        chunk = keys[i : i + _BATCH_GET_MAX]
        try:
            resp = client.batch_get_item(RequestItems={table_name: {"Keys": chunk}})
            items = resp.get("Responses", {}).get(table_name, [])
            for item in items:
                pk = item.get("bill_id") or ""
                if pk.startswith("SEARCH#VOTE#"):
                    pid = pk.replace("SEARCH#VOTE#", "", 1)
                    existing[pid] = item
            unprocessed = resp.get("UnprocessedKeys", {}).get(table_name, {}).get("Keys", [])
            if unprocessed:
                time.sleep(0.2)
                resp2 = client.batch_get_item(RequestItems={table_name: {"Keys": unprocessed}})
                for item in resp2.get("Responses", {}).get(table_name, []):
                    pk = item.get("bill_id") or ""
                    if pk.startswith("SEARCH#VOTE#"):
                        pid = pk.replace("SEARCH#VOTE#", "", 1)
                        existing[pid] = item
        except Exception as e:
            log_print(f"      ⚠️ SEARCH#VOTE batch get failed: {str(e)[:150]}")
            return
    put_items: List[Dict[str, Any]] = []
    for pid, data in updates.items():
        pk, sk = f"SEARCH#VOTE#{pid}", "VOTE"
        item = existing.get(pid) or {}
        (existing_bill_yea, existing_bill_nea, existing_bill_present, existing_bill_not_voting,
         existing_roll_yea, existing_roll_nea, existing_roll_present, existing_roll_not_voting) = _load_vote_data_from_item(item)
        bill_yea = list(set(existing_bill_yea) | data["bill_yea"])
        bill_nea = list(set(existing_bill_nea) | data["bill_nea"])
        bill_present = list(set(existing_bill_present) | data.get("bill_present", set()))
        bill_not_voting = list(set(existing_bill_not_voting) | data.get("bill_not_voting", set()))
        roll_yea = list(set(existing_roll_yea) | data.get("roll_yea", set()))
        roll_nea = list(set(existing_roll_nea) | data.get("roll_nea", set()))
        roll_present = list(set(existing_roll_present) | data.get("roll_present", set()))
        roll_not_voting = list(set(existing_roll_not_voting) | data.get("roll_not_voting", set()))
        display_name = (data.get("display_name") or item.get("display_name") or item.get("search_value") or "").strip() or pid
        full_item = {
            "bill_id": pk,
            "search_index_sk": sk,
            "search_type": "VOTE",
            "search_value": display_name,
            "display_name": display_name,
            "bill_yea": bill_yea,
            "bill_nea": bill_nea,
            "bill_present": bill_present,
            "bill_not_voting": bill_not_voting,
            "roll_yea": roll_yea,
            "roll_nea": roll_nea,
            "roll_present": roll_present,
            "roll_not_voting": roll_not_voting,
            "is_search_index": True,
        }
        approx_size = len(json.dumps(full_item))
        if approx_size > _OVERSIZE_SAFE_SIZE:
            s3_key = _store_vote_data_to_s3(pk, bill_yea, bill_nea, bill_present, bill_not_voting, roll_yea, roll_nea, roll_present, roll_not_voting)
            full_item = {
                "bill_id": pk,
                "search_index_sk": sk,
                "search_type": "VOTE",
                "search_value": display_name,
                "display_name": display_name,
                "vote_data_oversize_s3_key": s3_key,
                "is_search_index": True,
            }
        put_items.append(full_item)
    for i in range(0, len(put_items), _BATCH_WRITE_MAX):
        chunk = put_items[i : i + _BATCH_WRITE_MAX]
        write_reqs = [{"PutRequest": {"Item": item}} for item in chunk]
        try:
            resp = client.batch_write_item(RequestItems={table_name: write_reqs})
            unprocessed = resp.get("UnprocessedItems", {}).get(table_name, [])
            while unprocessed:
                time.sleep(0.2)
                resp = client.batch_write_item(RequestItems={table_name: unprocessed})
                unprocessed = resp.get("UnprocessedItems", {}).get(table_name, [])
        except Exception as e:
            log_print(f"      ⚠️ SEARCH#VOTE batch write failed: {str(e)[:150]}")


def update_search_vote_index_for_roll(
    congress: int,
    session: int,
    roll: int,
    members: List[Dict[str, Any]],
    politicians: List[Dict[str, Any]],
    table: Any,
    politicians_by_bioguide: Optional[Dict[str, Dict[str, Any]]] = None,
) -> None:
    """
    For a standalone roll call (no bill), update each voter's SEARCH#VOTE item: add roll id to roll_yea, roll_nea, roll_present, or roll_not_voting.
    Roll id format: "{congress}#{session}#{roll}". Read existing, merge in memory, overwrite.
    """
    roll_id = f"{congress}#{session}#{roll}"
    updates: Dict[str, Dict[str, Any]] = {}
    n_members_total = 0
    n_skipped_no_bucket = 0
    n_skipped_no_pid = 0
    for member in members or []:
        if not isinstance(member, dict):
            continue
        n_members_total += 1
        bucket = _vote_cast_to_bucket(member.get("voteCast"))
        if not bucket:
            n_skipped_no_bucket += 1
            continue
        pid, display_name = _resolve_politician_id_and_name(
            member, politicians, politicians_by_bioguide=politicians_by_bioguide
        )
        if not pid:
            n_skipped_no_pid += 1
            continue
        if pid not in updates:
            updates[pid] = {"roll_yea": set(), "roll_nea": set(), "roll_present": set(), "roll_not_voting": set(), "display_name": display_name or ""}
        updates[pid][f"roll_{bucket}"].add(roll_id)
        if display_name and not updates[pid]["display_name"]:
            updates[pid]["display_name"] = display_name
    if n_skipped_no_bucket or n_skipped_no_pid:
        log_print(f"      [SEARCH#VOTE] roll {roll_id}: members={n_members_total}, indexed={len(updates)}, skipped_no_vote_bucket={n_skipped_no_bucket}, skipped_no_pid={n_skipped_no_pid}")
    if not updates:
        return
    client = table.meta.client
    table_name = table.name
    keys = [{"bill_id": f"SEARCH#VOTE#{pid}", "search_index_sk": "VOTE"} for pid in updates]
    existing: Dict[str, Dict[str, Any]] = {}
    for i in range(0, len(keys), _BATCH_GET_MAX):
        chunk = keys[i : i + _BATCH_GET_MAX]
        try:
            resp = client.batch_get_item(RequestItems={table_name: {"Keys": chunk}})
            items = resp.get("Responses", {}).get(table_name, [])
            for item in items:
                pk = item.get("bill_id") or ""
                if pk.startswith("SEARCH#VOTE#"):
                    pid = pk.replace("SEARCH#VOTE#", "", 1)
                    existing[pid] = item
            unprocessed = resp.get("UnprocessedKeys", {}).get(table_name, {}).get("Keys", [])
            if unprocessed:
                time.sleep(0.2)
                resp2 = client.batch_get_item(RequestItems={table_name: {"Keys": unprocessed}})
                for item in resp2.get("Responses", {}).get(table_name, []):
                    pk = item.get("bill_id") or ""
                    if pk.startswith("SEARCH#VOTE#"):
                        pid = pk.replace("SEARCH#VOTE#", "", 1)
                        existing[pid] = item
        except Exception as e:
            log_print(f"      ⚠️ SEARCH#VOTE batch get failed (roll): {str(e)[:150]}")
            return
    put_items = []
    for pid, data in updates.items():
        pk, sk = f"SEARCH#VOTE#{pid}", "VOTE"
        item = existing.get(pid) or {}
        (existing_bill_yea, existing_bill_nea, existing_bill_present, existing_bill_not_voting,
         existing_roll_yea, existing_roll_nea, existing_roll_present, existing_roll_not_voting) = _load_vote_data_from_item(item)
        bill_yea = list(existing_bill_yea)
        bill_nea = list(existing_bill_nea)
        bill_present = list(existing_bill_present)
        bill_not_voting = list(existing_bill_not_voting)
        roll_yea = list(set(existing_roll_yea) | data["roll_yea"])
        roll_nea = list(set(existing_roll_nea) | data["roll_nea"])
        roll_present = list(set(existing_roll_present) | data.get("roll_present", set()))
        roll_not_voting = list(set(existing_roll_not_voting) | data.get("roll_not_voting", set()))
        display_name = (data.get("display_name") or item.get("display_name") or item.get("search_value") or "").strip() or pid
        full_item = {
            "bill_id": pk,
            "search_index_sk": sk,
            "search_type": "VOTE",
            "search_value": display_name,
            "display_name": display_name,
            "bill_yea": bill_yea,
            "bill_nea": bill_nea,
            "bill_present": bill_present,
            "bill_not_voting": bill_not_voting,
            "roll_yea": roll_yea,
            "roll_nea": roll_nea,
            "roll_present": roll_present,
            "roll_not_voting": roll_not_voting,
            "is_search_index": True,
        }
        approx_size = len(json.dumps(full_item))
        if approx_size > _OVERSIZE_SAFE_SIZE:
            s3_key = _store_vote_data_to_s3(pk, bill_yea, bill_nea, bill_present, bill_not_voting, roll_yea, roll_nea, roll_present, roll_not_voting)
            full_item = {
                "bill_id": pk,
                "search_index_sk": sk,
                "search_type": "VOTE",
                "search_value": display_name,
                "display_name": display_name,
                "vote_data_oversize_s3_key": s3_key,
                "is_search_index": True,
            }
        put_items.append(full_item)
    for i in range(0, len(put_items), _BATCH_WRITE_MAX):
        chunk = put_items[i : i + _BATCH_WRITE_MAX]
        write_reqs = [{"PutRequest": {"Item": item}} for item in chunk]
        try:
            resp = client.batch_write_item(RequestItems={table_name: write_reqs})
            unprocessed = resp.get("UnprocessedItems", {}).get(table_name, [])
            while unprocessed:
                time.sleep(0.2)
                resp = client.batch_write_item(RequestItems={table_name: unprocessed})
                unprocessed = resp.get("UnprocessedItems", {}).get(table_name, [])
        except Exception as e:
            log_print(f"      ⚠️ SEARCH#VOTE batch write failed (roll): {str(e)[:150]}")


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
        members, vote_date = _fetch_house_vote_members(congress, session, roll, api_key, key_index=key_index)
        entry = {"roll": roll, "session": session, "members": members}
        if vote_date:
            entry["vote_date"] = vote_date
        roll_call_votes.append(entry)
        if roll_call_number is None:
            roll_call_number = roll
    return {
        "roll_call_number": roll_call_number,
        "roll_call_votes": roll_call_votes,
        "has_roll_call": 1 if roll_call_votes else 0,
    }


def process_bill_roll_call_delta(
    bill_id: str,
    table_name: str,
    api_key: str,
    key_index: Optional[int] = None,
) -> Tuple[bool, Optional[str], Optional[str], Optional[Dict]]:
    """
    Get bill item from table; compare XML roll set vs existing roll_call_votes.
    If same -> return (True, 'skipped', None, None). If different -> fetch only new rolls, merge, return (True, None, None, update_payload) or (False, None, err, None).
    Returns (success, 'skipped'|None, error_message, update_dict).
    """
    table = dynamodb.Table(table_name)
    try:
        resp = table.get_item(
            Key={"bill_id": bill_id, "search_index_sk": bill_id},
            ProjectionExpression="recorded_votes_json, roll_call_votes",
        )
    except Exception as e:
        return False, None, str(e), None
    item = resp.get("Item")
    if not item:
        return True, "skipped", None, None  # bill not in table, skip
    recorded_votes_json = item.get("recorded_votes_json")
    roll_call_votes_raw = item.get("roll_call_votes")
    xml_rolls = _house_rolls_from_recorded_votes_json(recorded_votes_json)
    existing_rolls = _existing_rolls_from_roll_call_votes(roll_call_votes_raw)
    # Never overwrite or clear roll_call_votes when the bill has no XML roll list (e.g. fetcher
    # wrote the item without recorded_votes_json). Otherwise we would merge with empty xml_rolls,
    # drop all existing entries, and write REMOVE roll_call_votes, wiping data that was there.
    if not xml_rolls:
        return True, "skipped", None, None
    if xml_rolls == existing_rolls:
        return True, "skipped", None, None
    to_fetch = xml_rolls - existing_rolls
    existing_list = roll_call_votes_raw
    if isinstance(existing_list, str):
        try:
            existing_list = json.loads(existing_list)
        except json.JSONDecodeError:
            existing_list = []
    if not isinstance(existing_list, list):
        existing_list = []
    try:
        new_rolls_data = _fetch_new_rolls_only(bill_id, to_fetch, api_key, key_index=key_index)
    except Exception as e:
        return False, None, str(e), None
    merged = _merge_roll_call_votes(existing_list, xml_rolls, new_rolls_data)
    roll_call_number = None
    if merged:
        first_ent = merged[0]
        roll_call_number = first_ent.get("roll")
    has_roll_call = 1 if merged else 0
    if has_roll_call:
        update_expr = "SET has_roll_call = :h, roll_call_number = :n, roll_call_votes = :v REMOVE roll_call_votes_oversize_s3_key"
        attr_vals = {
            ":h": 1,
            ":n": roll_call_number,
            ":v": json.dumps(merged),
        }
    else:
        update_expr = "SET has_roll_call = :h REMOVE roll_call_number, roll_call_votes, roll_call_votes_oversize_s3_key"
        attr_vals = {":h": 0}
    return True, None, None, {
        "bill_id": bill_id,
        "search_index_sk": bill_id,
        "UpdateExpression": update_expr,
        "ExpressionAttributeValues": attr_vals,
        "roll_call_votes": merged,
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
    log_print("=" * 80)

    if not BILLS_TABLE_NAME:
        raise ValueError("BILLS_TABLE_NAME job parameter not set")

    start_date, end_date = _get_start_end_dates()
    date_range_path = _format_date_range_path(start_date, end_date)
    s3_prefix = f"downloads/{date_range_path}/"
    log_print(f"📅 Date range: {start_date} to {end_date} (prefix: {s3_prefix})")

    # Parse to datetime for XML date filtering: only bills updated within this range (same as fetcher)
    start_date_dt = None
    end_date_dt = None
    try:
        start_date_dt = datetime.strptime(start_date[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
        end_date_dt = (
            datetime.strptime(end_date[:10], "%Y-%m-%d")
            .replace(hour=23, minute=59, second=59, microsecond=999999)
            .replace(tzinfo=timezone.utc)
        )
    except (ValueError, AttributeError):
        pass

    # List ZIP keys in that folder
    zip_keys = _list_zip_s3_keys(s3_prefix)
    log_print(f"📦 Found {len(zip_keys)} ZIP(s) under {s3_prefix}")

    all_bill_ids = set()
    for zk in zip_keys:
        try:
            resp = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=zk)
            body = resp.get("Body")
            zip_content = body.read() if body else b""
            bill_ids = _get_bill_ids_from_zip_content(zip_content, start_date_dt, end_date_dt)
            all_bill_ids.update(bill_ids)
        except Exception as e:
            log_print(f"   ⚠ Failed to read ZIP {zk}: {e}")
    bill_id_list = sorted(all_bill_ids)
    total_bills = len(bill_id_list)
    log_print(f"📋 Bills to consider (from ZIPs, within date range): {total_bills}")

    if total_bills == 0:
        log_print("   No bills in date range; nothing to do.")
        log_print("=" * 80)
        job.commit()
        return

    try:
        api_key_rotator = get_congress_api_keys()
        log_print(f"✅ Retrieved {api_key_rotator.get_key_count()} Congress API key(s)")
    except Exception as e:
        log_print(f"❌ Failed to retrieve API keys: {str(e)}")
        raise

    bills_table = dynamodb.Table(BILLS_TABLE_NAME)
    politicians = load_legislators_csv()
    politicians_by_bioguide = _build_politicians_by_bioguide(politicians)
    log_print(f"   Loaded {len(politicians)} legislators for SEARCH#VOTE index (O(1) lookup by bioguide).")
    ensure_search_vote_items_for_legislators(politicians, bills_table)

    roll_ok = 0
    roll_skip = 0
    roll_err = 0
    progress_lock = threading.Lock()
    roll_write_lock = threading.Lock()
    last_logged = 0
    rolls_written_from_bills: Set[Tuple[str, int, int]] = set()

    def _process_one_bill(bid: str) -> Tuple[str, Optional[str], Optional[Dict]]:
        """Returns ('ok'|'skip'|'err', error_message_or_none, payload_or_none)."""
        try:
            api_key, key_index = api_key_rotator.get_key_and_index()
            success, skip, err, payload = process_bill_roll_call_delta(
                bid, BILLS_TABLE_NAME, api_key, key_index=key_index
            )
            if skip == "skipped":
                return ("skip", None, None)
            if not success:
                return ("err", err, None)
            if payload:
                try:
                    bills_table.update_item(
                        Key={"bill_id": payload["bill_id"], "search_index_sk": payload["search_index_sk"]},
                        UpdateExpression=payload["UpdateExpression"],
                        ExpressionAttributeValues=payload["ExpressionAttributeValues"],
                    )
                except Exception as update_err:
                    err_str = str(update_err)
                    if "ValidationException" in err_str and "exceeded" in err_str.lower() and payload.get("roll_call_votes"):
                        # Item size exceeded: store roll_call_votes in oversize/ and set key on item
                        s3_key = _store_roll_call_votes_to_s3(bid, payload["roll_call_votes"])
                        bills_table.update_item(
                            Key={"bill_id": payload["bill_id"], "search_index_sk": payload["search_index_sk"]},
                            UpdateExpression="SET has_roll_call = :h, roll_call_number = :n, roll_call_votes_oversize_s3_key = :k REMOVE roll_call_votes",
                            ExpressionAttributeValues={
                                ":h": 1,
                                ":n": payload["ExpressionAttributeValues"][":n"],
                                ":k": s3_key,
                            },
                        )
                    else:
                        raise
                roll_votes = payload.get("roll_call_votes")
                if roll_votes and politicians:
                    update_search_vote_index_for_bill(
                        bid, roll_votes, politicians, bills_table,
                        politicians_by_bioguide=politicians_by_bioguide,
                    )
                if roll_votes:
                    update_search_roll_index_for_bill(bid, roll_votes, bills_table, rolls_written=rolls_written_from_bills, roll_write_lock=roll_write_lock)
                return ("ok", None, payload)
            return ("skip", None, None)
        except Exception as e:
            return ("err", str(e), None)

    workers = min(ROLL_CALL_MAX_WORKERS, total_bills or 1)
    log_print(f"   Using {workers} workers; delta logic (no API call when roll set unchanged).")
    log_print("-" * 80)
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(_process_one_bill, bid): bid for bid in bill_id_list}
        for future in as_completed(futures):
            bid = futures[future]
            try:
                status, err_msg, _ = future.result()
            except Exception as e:
                status, err_msg = "err", str(e)
            with progress_lock:
                if status == "ok":
                    roll_ok += 1
                elif status == "skip":
                    roll_skip += 1
                else:
                    roll_err += 1
                    if err_msg:
                        log_print(f"      ❌ {bid}: {err_msg}")
                done = roll_ok + roll_skip + roll_err
                if done >= last_logged + 50:
                    last_logged = (done // 50) * 50
                    log_print(f"      Roll call: {done}/{total_bills} (updated: {roll_ok}, skipped: {roll_skip}, err: {roll_err})")
    log_print(f"✅ Roll call maintenance done. Updated {roll_ok}, skipped (no change) {roll_skip}, errors {roll_err}.")
    log_print(f"   Rolls written from bills (this run): {len(rolls_written_from_bills)}")

    # Second pass: GET /house-vote/{congress}/{session} for any votes not already in SEARCH#ROLL from the bill pass
    log_print("-" * 80)
    log_print("Second pass: house-vote list API — fetch additional roll calls not already written from bills.")
    congresses_from_bills = set()
    for bid in bill_id_list:
        parsed = _parse_bill_id(bid)
        if parsed:
            congresses_from_bills.add(int(parsed[0]))
    if not congresses_from_bills:
        current_year = datetime.now(timezone.utc).year
        congresses_from_bills = {((current_year - 1789) // 2) + 1}
        log_print(f"   No congress from bills; using current congress {list(congresses_from_bills)[0]} for house-vote list.")
    if congresses_from_bills:
        roll_backfill_added = 0
        roll_skipped_already_from_bills = 0
        roll_skipped_already_in_table = 0
        house_vote_list_api_calls = 0
        house_vote_members_api_calls = 0
        for congress in sorted(congresses_from_bills):
            existing_roll_keys = _load_existing_roll_keys_for_congress(bills_table, congress)
            log_print(f"   Congress {congress}: {len(existing_roll_keys)} roll(s) already in SEARCH#ROLL (will skip API call).")
            for session in (1, 2):
                api_key, key_index = api_key_rotator.get_key_and_index()
                vote_list = _fetch_house_vote_list(congress, session, api_key, key_index=key_index)
                house_vote_list_api_calls += 1
                log_print(f"   Congress {congress} Session {session}: house-vote list API returned {len(vote_list)} roll(s).")
                added_this_session = 0
                skipped_this_session = 0
                skipped_already_in_table_this_session = 0
                for v in vote_list:
                    if not isinstance(v, dict):
                        continue
                    roll_num = v.get("rollCallNumber") or v.get("rollNumber")
                    sess_num = v.get("sessionNumber") or session
                    if roll_num is None:
                        continue
                    roll_int = int(roll_num) if isinstance(roll_num, (int, float)) else (int(roll_num) if str(roll_num).isdigit() else None)
                    sess_int = int(sess_num) if isinstance(sess_num, (int, float)) else (int(sess_num) if str(sess_num).isdigit() else None)
                    if roll_int is None or sess_int is None:
                        continue
                    key = (str(congress), sess_int, roll_int)
                    if key in rolls_written_from_bills:
                        skipped_this_session += 1
                        continue
                    if key in existing_roll_keys:
                        skipped_already_in_table_this_session += 1
                        continue
                    api_key, key_index = api_key_rotator.get_key_and_index()
                    members = _fetch_house_vote_members(str(congress), sess_int, roll_int, api_key, key_index=key_index)
                    house_vote_members_api_calls += 1
                    if added_this_session < 5:
                        log_print(f"      Fetching members for roll {congress}#{session}#{roll_int} (not from bill pass) — API call.")
                    bill_id_associated = _bill_id_from_legislation(
                        congress,
                        v.get("legislationType"),
                        v.get("legislationNumber"),
                        v.get("legislationUrl"),
                    ) or ""
                    vote_date = v.get("updateDate") or v.get("startDate") or v.get("date") or v.get("actionDate") or v.get("voteDate")
                    if vote_date and len(str(vote_date).strip()) >= 10:
                        vote_date = str(vote_date).strip()[:10]
                        if "T" in vote_date:
                            vote_date = vote_date.split("T")[0][:10]
                    else:
                        vote_date = "0000-00-00"
                    _write_one_roll_item(str(congress), sess_int, roll_int, members, bill_id_associated, bills_table, latest_action_date=vote_date)
                    rolls_written_from_bills.add(key)
                    existing_roll_keys.add(key)
                    roll_backfill_added += 1
                    added_this_session += 1
                    if politicians:
                        if bill_id_associated:
                            single_roll = [{"roll": roll_int, "session": sess_int, "members": members}]
                            update_search_vote_index_for_bill(
                                bill_id_associated,
                                single_roll,
                                politicians,
                                bills_table,
                                politicians_by_bioguide=politicians_by_bioguide,
                            )
                        else:
                            update_search_vote_index_for_roll(
                                int(congress),
                                sess_int,
                                roll_int,
                                members,
                                politicians,
                                bills_table,
                                politicians_by_bioguide=politicians_by_bioguide,
                            )
            roll_skipped_already_from_bills += skipped_this_session
            roll_skipped_already_in_table += skipped_already_in_table_this_session
            if skipped_this_session or skipped_already_in_table_this_session or added_this_session:
                log_print(
                    f"   Congress {congress} Session {session}: {skipped_this_session} already from bills (skipped), "
                    f"{skipped_already_in_table_this_session} already in table (skipped), {added_this_session} new (fetched members, wrote SEARCH#ROLL + SEARCH#VOTE)."
                )
        log_print(
            f"   House-vote list summary: {roll_skipped_already_from_bills} from bills (skipped), "
            f"{roll_skipped_already_in_table} already in table (skipped), {roll_backfill_added} new (API calls made)."
        )
        log_print(f"   House-vote list API calls: {house_vote_list_api_calls} list call(s), {house_vote_members_api_calls} member call(s) for new rolls.")
        if roll_backfill_added:
            log_print(f"✅ House-vote list backfill: added {roll_backfill_added} roll call(s) not from bill pass.")
        else:
            log_print(f"   House-vote list backfill: no additional rolls (all rolls in list were already written from bill pass).")
    
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


