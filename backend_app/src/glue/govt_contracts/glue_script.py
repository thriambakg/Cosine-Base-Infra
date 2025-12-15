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
    'REQUEST_TIMEOUT',
    'ORPHAN_SUBAWARD_SQS_URL',
    'DLQ_SQS_URL'
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
sqs_client = boto3.client('sqs')

# SQS queues
ORPHAN_SUBAWARD_SQS_URL = args.get('ORPHAN_SUBAWARD_SQS_URL', '')
DLQ_SQS_URL = args.get('DLQ_SQS_URL', '')
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

def fetch_award_from_api(award_id: str) -> Optional[Dict[str, Any]]:
    """
    Fetch award details from USAspending API and convert to our award record format.
    
    Args:
        award_id: The award ID to fetch (e.g., ASST_NON_251VA307N1199_012)
    
    Returns:
        Award record dict in our format, or None if fetch fails
    """
    try:
        log_print(f"📡 Fetching award {award_id} from USAspending API...")
        api_response = call_usaspending_api(f'/api/v2/awards/{award_id}/', method='GET')
        
        if not api_response:
            log_print(f"⚠️ No data returned from API for award {award_id}")
        return None

        # Convert API response to our award record format
        # Store ALL fields from API response to match bulk file granularity
        award_record = {}
        
        # First, copy all top-level fields from API response (preserve everything)
        for key, value in api_response.items():
            if value is None:
                continue
            
            # Skip nested objects - we'll flatten them separately
            if isinstance(value, (dict, list)):
                continue
            
            # Convert numeric values to Decimal for consistency
            # Handle None values - API can return None for nullable fields
            if value is None:
                continue
            if isinstance(value, (int, float)):
                try:
                    award_record[key] = Decimal(str(value))
                except (ValueError, TypeError, Exception):
                    # If conversion fails, store as string or skip
                    award_record[key] = normalize_string(value)
            else:
                award_record[key] = normalize_string(value) if isinstance(value, str) else value
        
        # Map API fields to our format (override with our field names where needed)
        award_record['award_id'] = api_response.get('generated_unique_award_id') or award_id
        award_record['data_source'] = 'usaspending_api_direct'
        award_record['api_version'] = 'api_v2_direct_fetch'
        
        # Map total_obligation to total_obligated_amount (our standard field name)
        # Handle null values - API can return None for financial fields
        if 'total_obligation' in api_response:
            total_obligation_val = api_response['total_obligation']
            if total_obligation_val is not None and total_obligation_val != '':
                try:
                    # Handle both numeric and string values
                    if isinstance(total_obligation_val, (int, float)):
                        award_record['total_obligated_amount'] = Decimal(str(total_obligation_val))
                    elif isinstance(total_obligation_val, str) and total_obligation_val.strip():
                        award_record['total_obligated_amount'] = Decimal(total_obligation_val.strip())
                except (ValueError, TypeError, Exception) as e:
                    # If conversion fails, skip this field
                    log_print(f"⚠️ Could not convert total_obligation for {award_id}: {str(e)[:100]}")
                    pass
        if 'total_obligation' in award_record and 'total_obligated_amount' not in award_record:
            total_obligation_val = award_record.get('total_obligation')
            if total_obligation_val is not None and total_obligation_val != '':
                try:
                    if isinstance(total_obligation_val, (int, float)):
                        award_record['total_obligated_amount'] = Decimal(str(total_obligation_val))
                    elif isinstance(total_obligation_val, str) and total_obligation_val.strip():
                        award_record['total_obligated_amount'] = Decimal(total_obligation_val.strip())
                except (ValueError, TypeError, Exception):
                    pass
        
        # Map base_exercised_options to base_and_exercised_options_value
        # API can return None for this field (especially for assistance awards)
        if 'base_exercised_options' in api_response:
            base_exercised_val = api_response['base_exercised_options']
            if base_exercised_val is not None and base_exercised_val != '':
                try:
                    # Handle both numeric and string values
                    if isinstance(base_exercised_val, (int, float)):
                        award_record['base_and_exercised_options_value'] = Decimal(str(base_exercised_val))
                    elif isinstance(base_exercised_val, str) and base_exercised_val.strip():
                        award_record['base_and_exercised_options_value'] = Decimal(base_exercised_val.strip())
                except (ValueError, TypeError, Exception) as e:
                    # If conversion fails (e.g., None, empty string, invalid format), skip this field
                    log_print(f"⚠️ Could not convert base_exercised_options for {award_id}: {str(e)[:100]}")
                    pass
        
        # Map base_and_all_options to base_and_all_options_value
        # API can return None for this field
        if 'base_and_all_options' in api_response:
            base_all_val = api_response['base_and_all_options']
            if base_all_val is not None and base_all_val != '':
                try:
                    # Handle both numeric and string values
                    if isinstance(base_all_val, (int, float)):
                        award_record['base_and_all_options_value'] = Decimal(str(base_all_val))
                    elif isinstance(base_all_val, str) and base_all_val.strip():
                        award_record['base_and_all_options_value'] = Decimal(base_all_val.strip())
                except (ValueError, TypeError, Exception) as e:
                    # If conversion fails, skip this field
                    log_print(f"⚠️ Could not convert base_and_all_options for {award_id}: {str(e)[:100]}")
                    pass
        
        # Flatten nested objects to match bulk CSV structure
        # Agency information (flatten awarding_agency)
        if 'awarding_agency' in api_response:
            agency = api_response['awarding_agency']
            if isinstance(agency, dict):
                for agency_key, agency_value in agency.items():
                    if agency_value is None:
                        continue
                    # Flatten nested agency fields
                    if agency_key == 'name':
                        award_record['awarding_agency_name'] = normalize_string(agency_value)
                    elif agency_key == 'id':
                        award_record['awarding_agency_code'] = str(agency_value)
                    elif agency_key == 'toptier_agency':
                        if isinstance(agency_value, dict):
                            if 'name' in agency_value:
                                award_record['awarding_agency_name'] = normalize_string(agency_value['name'])
                            if 'toptier_code' in agency_value:
                                award_record['awarding_agency_code'] = normalize_string(agency_value['toptier_code'])
                    elif agency_key == 'subtier_agency':
                        if isinstance(agency_value, dict):
                            if 'name' in agency_value:
                                award_record['awarding_sub_agency_name'] = normalize_string(agency_value['name'])
                            if 'subtier_code' in agency_value:
                                award_record['awarding_sub_agency_code'] = normalize_string(agency_value['subtier_code'])
                    else:
                        # Store other agency fields with prefix
                        award_record[f'awarding_agency_{agency_key}'] = normalize_string(agency_value) if isinstance(agency_value, str) else agency_value
        
        # Flatten funding_agency
        if 'funding_agency' in api_response:
            agency = api_response['funding_agency']
            if isinstance(agency, dict):
                for agency_key, agency_value in agency.items():
                    if agency_value is None:
                        continue
                    if agency_key == 'name':
                        award_record['funding_agency_name'] = normalize_string(agency_value)
                    elif agency_key == 'id':
                        award_record['funding_agency_code'] = str(agency_value)
                    elif agency_key == 'toptier_agency':
                        if isinstance(agency_value, dict):
                            if 'name' in agency_value:
                                award_record['funding_agency_name'] = normalize_string(agency_value['name'])
                            if 'toptier_code' in agency_value:
                                award_record['funding_agency_code'] = normalize_string(agency_value['toptier_code'])
                    elif agency_key == 'subtier_agency':
                        if isinstance(agency_value, dict):
                            if 'name' in agency_value:
                                award_record['funding_sub_agency_name'] = normalize_string(agency_value['name'])
                            if 'subtier_code' in agency_value:
                                award_record['funding_sub_agency_code'] = normalize_string(agency_value['subtier_code'])
                    else:
                        award_record[f'funding_agency_{agency_key}'] = normalize_string(agency_value) if isinstance(agency_value, str) else agency_value
        
        # Flatten recipient information (matching Lambda approach)
        if 'recipient' in api_response:
            recipient = api_response['recipient']
            if isinstance(recipient, dict):
                # Extract recipient name from recipient_recipient_name field in API response
                # Store in uppercase to match USAspending standard and autocomplete behavior
                recipient_name = recipient.get('recipient_name', '')
                if recipient_name:
                    # Store raw recipient_name in uppercase (matches USAspending standard)
                    recipient_name_upper = recipient_name.upper() if isinstance(recipient_name, str) else recipient_name
                    award_record['recipient_name'] = recipient_name_upper
                    award_record['recipient_name_normalized'] = recipient_name_upper.lower()
                
                # Process other recipient fields
                for recipient_key, recipient_value in recipient.items():
                    if recipient_value is None or recipient_key == 'recipient_name':
                        continue
                    if recipient_key == 'location':
                        if isinstance(recipient_value, dict):
                            for loc_key, loc_value in recipient_value.items():
                                if loc_value is None:
                                    continue
                                # Map location fields
                                if loc_key == 'state_code':
                                    award_record['recipient_location_state'] = normalize_string(loc_value)
                                elif loc_key == 'state_name':
                                    award_record['recipient_state_name'] = normalize_string(loc_value)
                                elif loc_key == 'country_code':
                                    award_record['recipient_location_country'] = normalize_string(loc_value)
                                elif loc_key == 'country_name':
                                    award_record['recipient_country_name'] = normalize_string(loc_value)
                                elif loc_key == 'city_name':
                                    award_record['recipient_city_name'] = normalize_string(loc_value)
                                elif loc_key == 'county_name':
                                    award_record['recipient_county_name'] = normalize_string(loc_value)
                                elif loc_key == 'address_line1':
                                    award_record['recipient_address_line_1'] = normalize_string(loc_value)
                                elif loc_key == 'address_line2':
                                    award_record['recipient_address_line_2'] = normalize_string(loc_value)
                                elif loc_key == 'zip5' or loc_key == 'zip':
                                    award_record['recipient_zip_code'] = normalize_string(loc_value)
                                else:
                                    award_record[f'recipient_location_{loc_key}'] = normalize_string(loc_value) if isinstance(loc_value, str) else loc_value
                    elif recipient_key == 'uei':
                        award_record['recipient_uei'] = normalize_string(recipient_value)
                    elif recipient_key == 'duns':
                        award_record['recipient_duns'] = normalize_string(recipient_value)
                    else:
                        award_record[f'recipient_{recipient_key}'] = normalize_string(recipient_value) if isinstance(recipient_value, str) else recipient_value
        
        # Flatten period_of_performance
        if 'period_of_performance' in api_response:
            pop = api_response['period_of_performance']
            if isinstance(pop, dict):
                for pop_key, pop_value in pop.items():
                    if pop_value is None:
                        continue
                    if pop_key == 'start_date':
                        award_record['period_of_performance_start_date'] = normalize_string(pop_value)
                    elif pop_key == 'end_date' or pop_key == 'current_end_date':
                        award_record['period_of_performance_end_date'] = normalize_string(pop_value)
                    elif pop_key == 'potential_end_date':
                        award_record['period_of_performance_potential_end_date'] = normalize_string(pop_value)
                    else:
                        award_record[f'period_of_performance_{pop_key}'] = normalize_string(pop_value) if isinstance(pop_value, str) else pop_value
        
        # Flatten place_of_performance
        if 'place_of_performance' in api_response:
            pop_loc = api_response['place_of_performance']
            if isinstance(pop_loc, dict):
                for pop_key, pop_value in pop_loc.items():
                    if pop_value is None:
                        continue
                    award_record[f'place_of_performance_{pop_key}'] = normalize_string(pop_value) if isinstance(pop_value, str) else pop_value
        
        # Flatten parent_award
        if 'parent_award' in api_response:
            parent = api_response['parent_award']
            if isinstance(parent, dict):
                for parent_key, parent_value in parent.items():
                    if parent_value is None:
                        continue
                    if parent_key == 'piid':
                        award_record['parent_award_piid'] = normalize_string(parent_value)
                    elif parent_key == 'agency_id':
                        award_record['parent_award_agency_id'] = str(parent_value)
                    else:
                        award_record[f'parent_award_{parent_key}'] = normalize_string(parent_value) if isinstance(parent_value, str) else parent_value
        
        # Flatten NAICS hierarchy
        if 'naics_hierarchy' in api_response:
            naics = api_response['naics_hierarchy']
            if isinstance(naics, dict):
                for naics_key, naics_value in naics.items():
                    if naics_value is None:
                        continue
                    if naics_key == 'naics_code':
                        award_record['naics_code'] = normalize_string(naics_value)
                    elif naics_key == 'naics_description':
                        award_record['naics_description'] = normalize_string(naics_value)
                    else:
                        award_record[f'naics_{naics_key}'] = normalize_string(naics_value) if isinstance(naics_value, str) else naics_value
        
        # Flatten PSC hierarchy
        if 'psc_hierarchy' in api_response:
            psc = api_response['psc_hierarchy']
            if isinstance(psc, dict):
                for psc_key, psc_value in psc.items():
                    if psc_value is None:
                        continue
                    if psc_key == 'psc_code':
                        award_record['psc_code'] = normalize_string(psc_value)
                    elif psc_key == 'psc_description':
                        award_record['psc_description'] = normalize_string(psc_value)
                    else:
                        award_record[f'psc_{psc_key}'] = normalize_string(psc_value) if isinstance(psc_value, str) else psc_value
        
        # Flatten CFDA info (for assistance awards)
        if 'cfda_info' in api_response:
            cfda_list = api_response['cfda_info']
            if isinstance(cfda_list, list) and len(cfda_list) > 0:
                # Take first CFDA entry
                cfda = cfda_list[0]
                if isinstance(cfda, dict):
                    if 'cfda_number' in cfda:
                        award_record['cfda_number'] = normalize_string(cfda['cfda_number'])
                    if 'cfda_title' in cfda:
                        award_record['cfda_title'] = normalize_string(cfda['cfda_title'])
                    # Store full CFDA info array
                    award_record['cfda_info'] = cfda_list
        
        # Contract-specific field mappings
        if 'piid' in api_response:
            award_record['award_id_piid'] = normalize_string(api_response['piid'])
        
        # Assistance-specific field mappings
        if 'fain' in api_response:
            award_record['award_id_fain'] = normalize_string(api_response['fain'])
        if 'uri' in api_response:
            award_record['award_id_uri'] = normalize_string(api_response['uri'])
        
        # Store latest_transaction_contract_data as nested object (preserve structure)
        if 'latest_transaction_contract_data' in api_response:
            award_record['latest_transaction_contract_data'] = convert_floats_to_decimal(api_response['latest_transaction_contract_data'])
        
        # Store executive_details as nested object
        if 'executive_details' in api_response:
            award_record['executive_details'] = convert_floats_to_decimal(api_response['executive_details'])
        
        # Store account obligations/outlays arrays
        if 'account_obligations_by_defc' in api_response:
            award_record['account_obligations_by_defc'] = convert_floats_to_decimal(api_response['account_obligations_by_defc'])
        if 'account_outlays_by_defc' in api_response:
            award_record['account_outlays_by_defc'] = convert_floats_to_decimal(api_response['account_outlays_by_defc'])
        
        # Extract fiscal year
        period_start = award_record.get('period_of_performance_start_date')
        fiscal_year = extract_fiscal_year(period_start)
        if fiscal_year:
            award_record['fiscal_year'] = fiscal_year
        else:
            # Use current fiscal year as default
            now = datetime.now(timezone.utc)
            award_record['fiscal_year'] = now.year + 1 if now.month >= 10 else now.year
        
        # Map period_of_performance fields to period_start_date/period_end_date for GSI compatibility
        if 'period_of_performance_start_date' in award_record and 'period_start_date' not in award_record:
            award_record['period_start_date'] = award_record['period_of_performance_start_date']
        if 'period_of_performance_end_date' in award_record and 'period_end_date' not in award_record:
            award_record['period_end_date'] = award_record['period_of_performance_end_date']
        
        # Ensure total_obligated_amount is set (required for GSI)
        if 'total_obligated_amount' not in award_record or not award_record.get('total_obligated_amount'):
            award_record['total_obligated_amount'] = Decimal('0')
        
        # Initialize arrays and counts
        award_record['transactions'] = []
        award_record['subawards'] = []
        award_record['transaction_count'] = 0
        award_record['subaward_count'] = api_response.get('subaward_count', 0)
        
        # Metadata
        award_record['indexed_at'] = datetime.now(timezone.utc).isoformat()
        award_record['last_updated'] = datetime.now(timezone.utc).isoformat()
        award_record['award_details_indexed'] = True
        award_record['full_indexing_complete'] = False  # Will be set to True after subawards are added
        award_record['ttl'] = int((datetime.now(timezone.utc).timestamp() + (90 * 24 * 60 * 60)))
        
        # Set is_assistance based on category (0 = contract, 1 = assistance)
        category = api_response.get('category', '').lower()
        if category == 'assistance':
            award_record['is_assistance'] = 1
        else:
            award_record['is_assistance'] = 0
        
        log_print(f"✅ Successfully fetched award {award_id} from API")
        return award_record
        
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            log_print(f"⚠️ Award {award_id} not found in USAspending API (404)")
        else:
            log_print(f"⚠️ HTTP error fetching award {award_id} from API: {e.response.status_code}")
        return None
    except Exception as e:
        log_print(f"⚠️ Error fetching award {award_id} from API: {str(e)[:200]}")
        logger.error(f"⚠️ Error fetching award {award_id} from API", exc_info=True)
        return None

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
    if obj is None:
        return None
    if isinstance(obj, float):
        try:
            return Decimal(str(obj))
        except (ValueError, TypeError, Exception):
            return obj
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
        'recipient_name_normalized': full_item.get('recipient_name_normalized') or 'unknown',
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
    """Recursively convert Decimal, Binary, and bytes to JSON-serializable types"""
    # Handle DynamoDB Binary type (from boto3.dynamodb.types)
    try:
        from boto3.dynamodb.types import Binary as DynamoDBBinary
        if isinstance(obj, DynamoDBBinary):
            # Convert DynamoDB Binary to bytes, then handle as bytes
            obj = obj.value
    except ImportError:
        pass
    
    if isinstance(obj, Decimal):
        # Convert Decimal to float for JSON (preserves precision for most cases)
        try:
            return float(obj)
        except (OverflowError, ValueError):
            # If float conversion fails, use string representation
            return str(obj)
    elif isinstance(obj, bytes):
        # Convert binary bytes to integer (for is_assistance field: b'\x00' -> 0, b'\x01' -> 1)
        if len(obj) == 1:
            return int(obj[0])
        else:
            # For other bytes, convert to list of integers
            return [int(b) for b in obj]
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
    
    logger.info(f"Stored oversized award {award_id} to S3: {s3_key} ({len(compressed_data):,} bytes compressed, {len(json_bytes):,} bytes uncompressed)")
    return s3_key


def store_failed_award_to_s3(award_id: str, award_record: Dict[str, Any]) -> str:
    """
    Store failed award to S3 in failed/ folder for DLQ processing.
    Returns the S3 key.
    """
    # Create S3 key: failed/{award_id}.json.gz
    s3_key = f"failed/{award_id}.json.gz"
    
    # Convert Decimal values to JSON-serializable types
    json_ready_item = convert_decimal_for_json(award_record)
    
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
    
    logger.info(f"Stored failed award {award_id} to S3: {s3_key} ({len(compressed_data):,} bytes compressed, {len(json_bytes):,} bytes uncompressed)")
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
    last_gc_row = 0
    gc_interval = 50000  # Run garbage collection every 50k rows
    
    try:
        reader = csv.DictReader(csv_file_obj)
        
        for row in reader:
            try:
                row_count += 1
                
                # Log progress for large files
                if row_count % 100000 == 0:
                    log_print(f"   📊 Processing row {row_count:,} of {csv_filename}...")
                
                # Periodic memory cleanup for very large files
                if row_count - last_gc_row >= gc_interval:
                    gc.collect()
                    last_gc_row = row_count
                
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
                    
                    # Detect IDV child awards (awards with parent_award_piid and award types A/B/C/D, not IDV_ types)
                    parent_award_piid = row.get('parent_award_piid') or row.get('parent_award_id_piid')
                    parent_award_agency_id = row.get('parent_award_agency_id') or row.get('parent_award_agency_code')
                    award_type = row.get('award_type') or row.get('contract_award_type') or ''
                    
                    # Check if this is an IDV child award (has parent_award_piid and award type is A/B/C/D)
                    is_idv_child = False
                    if parent_award_piid and parent_award_agency_id:
                        # Award types A, B, C, D are child awards (delivery orders/calls against IDVs)
                        # Award types starting with IDV_ are parent IDVs themselves
                        if award_type in ['A', 'B', 'C', 'D'] or (isinstance(award_type, str) and award_type.upper() in ['A', 'B', 'C', 'D']):
                            is_idv_child = True
                            # Build parent IDV ID: CONT_IDV_{piid}_{agency_id}
                            # This matches the format used by USAspending for IDV awards
                            parent_idv_id = f"CONT_IDV_{parent_award_piid}_{parent_award_agency_id}"
                            award_record['parent_idv_id'] = parent_idv_id
                            award_record['parent_award_piid'] = parent_award_piid
                            award_record['parent_award_agency_id'] = parent_award_agency_id
                            award_record['is_idv_child'] = True
                    
                    # Detect if this is a parent IDV (award type starts with IDV_)
                    if award_type and isinstance(award_type, str) and award_type.startswith('IDV_'):
                        award_record['is_idv_parent'] = True
                        award_record['child_awards'] = []
                        award_record['child_award_count'] = 0
                    
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
            except Exception as row_error:
                # Log row-level errors but continue processing
                error_msg = f"Error processing row {row_count} in {csv_filename}: {str(row_error)[:200]}"
                logger.warning(error_msg)
                # Continue processing next row
                continue
        
        log_print(f"✅ Parsed {csv_filename}: {len(awards)} unique awards, {sum(a['transaction_count'] for a in awards.values())} total transactions, {row_count:,} rows processed")
        return awards
    
    except MemoryError as mem_error:
        error_msg = f"Memory error parsing {csv_filename} at row {row_count}: {str(mem_error)}"
        log_print(f"❌ {error_msg}")
        logger.error(error_msg, exc_info=True)
        # Force garbage collection before re-raising
        gc.collect()
        raise Exception(f"Out of memory while parsing {csv_filename} at row {row_count:,}. File may be too large.")
    except Exception as e:
        error_msg = f"Error parsing {csv_filename} at row {row_count}: {str(e)}"
        log_print(f"❌ {error_msg}")
        logger.error(error_msg, exc_info=True)
        raise

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
    last_gc_row = 0
    gc_interval = 50000  # Run garbage collection every 50k rows
    
    try:
        reader = csv.DictReader(csv_file_obj)
        
        for row in reader:
            try:
                row_count += 1
                
                # Log progress for large files
                if row_count % 100000 == 0:
                    log_print(f"   📊 Processing row {row_count:,} of {csv_filename}...")
                
                # Periodic memory cleanup for very large files
                if row_count - last_gc_row >= gc_interval:
                    gc.collect()
                    last_gc_row = row_count
                
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
            except Exception as row_error:
                # Log row-level errors but continue processing
                error_msg = f"Error processing row {row_count} in {csv_filename}: {str(row_error)[:200]}"
                logger.warning(error_msg)
                # Continue processing next row
                continue
        
        total_subawards = sum(len(subs) for subs in subawards_by_parent.values())
        log_print(f"✅ Parsed {csv_filename}: {len(subawards_by_parent)} parent awards, {total_subawards} total sub-awards, {row_count:,} rows processed")
        return subawards_by_parent
    
    except MemoryError as mem_error:
        error_msg = f"Memory error parsing {csv_filename} at row {row_count}: {str(mem_error)}"
        log_print(f"❌ {error_msg}")
        logger.error(error_msg, exc_info=True)
        # Force garbage collection before re-raising
        gc.collect()
        raise Exception(f"Out of memory while parsing {csv_filename} at row {row_count:,}. File may be too large.")
    except Exception as e:
        error_msg = f"Error parsing {csv_filename} at row {row_count}: {str(e)}"
        log_print(f"❌ {error_msg}")
        logger.error(error_msg, exc_info=True)
        raise

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
                
                # Parse CSV directly from stream with improved error handling
                try:
                    return parse_prime_award_csv_streaming(csv_file_obj, file_name)
                except MemoryError as mem_error:
                    error_msg = f"Memory error parsing {file_name}: {str(mem_error)}"
                    log_print(f"❌ {agency_prefix}{error_msg}")
                    logger.error(f"❌ {agency_prefix}{error_msg}", exc_info=True)
                    # Force garbage collection
                    gc.collect()
                    raise Exception(f"Out of memory while parsing {file_name}. File may be too large for available memory.")
                except Exception as parse_error:
                    error_msg = f"Error parsing {file_name}: {str(parse_error)}"
                    log_print(f"❌ {agency_prefix}{error_msg}")
                    logger.error(f"❌ {agency_prefix}{error_msg}", exc_info=True)
                    raise
            except Exception as e:
                error_msg = f"Error processing {file_name}: {str(e)[:300]}"
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
                
                # Parse CSV directly from stream with improved error handling
                try:
                    return parse_subaward_csv_streaming(csv_file_obj, file_name)
                except MemoryError as mem_error:
                    error_msg = f"Memory error parsing {file_name}: {str(mem_error)}"
                    log_print(f"❌ {agency_prefix}{error_msg}")
                    logger.error(f"❌ {agency_prefix}{error_msg}", exc_info=True)
                    # Force garbage collection
                    gc.collect()
                    raise Exception(f"Out of memory while parsing {file_name}. File may be too large for available memory.")
                except Exception as parse_error:
                    error_msg = f"Error parsing {file_name}: {str(parse_error)}"
                    log_print(f"❌ {agency_prefix}{error_msg}")
                    logger.error(f"❌ {agency_prefix}{error_msg}", exc_info=True)
                    raise
            except Exception as e:
                error_msg = f"Error processing {file_name}: {str(e)[:300]}"
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
                    # Parent not in bulk CSV files or DynamoDB - send to SQS for Lambda processing
                    log_print(f"🔍 {agency_prefix}Parent award {parent_id} not in bulk CSV files or DynamoDB, sending to SQS for Lambda processing...")
                    
                    if ORPHAN_SUBAWARD_SQS_URL:
                        try:
                            # Prepare message body
                            message_body = {
                                'parent_id': parent_id,
                                'subawards': subawards
                            }
                            
                            # Check message size (SQS limit is 256 KB)
                            message_json = json.dumps(message_body, default=str)
                            message_size_bytes = len(message_json.encode('utf-8'))
                            max_message_size = 200 * 1024  # 200 KB (leave room for SQS overhead)
                            
                            if message_size_bytes > max_message_size:
                                # Message too large - store subawards to S3 and send S3 key
                                log_print(f"⚠️ {agency_prefix}Message for parent {parent_id} too large ({message_size_bytes:,} bytes), storing subawards to S3...")
                                s3_key = f"orphan-subawards/{parent_id}.json.gz"
                                
                                # Compress and upload to S3
                                json_bytes = message_json.encode('utf-8')
                                compressed_data = gzip.compress(json_bytes)
                                
                                s3_client.put_object(
                                    Bucket=S3_BUCKET_NAME,
                                    Key=s3_key,
                                    Body=compressed_data,
                                    ContentType='application/json',
                                    ContentEncoding='gzip'
                                )
                                
                                # Send message with S3 key instead
                                message_body = {
                                    'parent_id': parent_id,
                                    'subawards_s3_key': s3_key
                                }
                                message_json = json.dumps(message_body, default=str)
                                log_print(f"💾 {agency_prefix}Stored {len(subawards)} subawards to S3: {s3_key}")
                            
                            # Send message to SQS queue
                            response = sqs_client.send_message(
                                QueueUrl=ORPHAN_SUBAWARD_SQS_URL,
                                MessageBody=message_json
                            )
                            
                            log_print(f"✅ {agency_prefix}Sent orphan subaward for parent {parent_id} ({len(subawards)} subawards) to SQS queue (MessageId: {response.get('MessageId')})")
                            dynamodb_linked += 1
                        except Exception as e:
                            error_msg = f"Error sending orphan subaward to SQS for parent {parent_id}: {str(e)[:200]}"
                            log_print(f"⚠️ {agency_prefix}{error_msg}")
                            logger.error(f"⚠️ {agency_prefix}{error_msg}", exc_info=True)
                            # Continue processing other parents even if one fails
                    else:
                        log_print(f"⚠️ {agency_prefix}ORPHAN_SUBAWARD_SQS_URL not configured, skipping orphan subaward for parent {parent_id}")
            except Exception as e:
                error_msg = f"Error checking DynamoDB for parent {parent_id}: {str(e)}"
                log_print(f"⚠️ {agency_prefix}{error_msg}")
                logger.error(f"⚠️ {agency_prefix}{error_msg}", exc_info=True)
                # Continue processing other parents even if one fails
        
        if dynamodb_linked > 0:
            log_print(f"✅ {agency_prefix}Found {dynamodb_linked} parent awards in DynamoDB")
    
    log_print(f"✅ {agency_prefix}Linked {linked_count} parent awards with sub-awards from bulk file")
    
    # Link IDV child awards to parent IDVs
    log_print(f"🔗 {agency_prefix}Linking IDV child awards to parent IDVs...")
    idv_child_linked = 0
    idv_child_unlinked = {}
    
    for award_id, award_data in all_prime_awards.items():
        parent_idv_id = award_data.get('parent_idv_id')
        if parent_idv_id:
            # This is an IDV child award
            if parent_idv_id in all_prime_awards:
                # Parent IDV is in current bulk file
                parent_idv = all_prime_awards[parent_idv_id]
                if 'child_awards' not in parent_idv:
                    parent_idv['child_awards'] = []
                if award_id not in parent_idv['child_awards']:
                    parent_idv['child_awards'].append(award_id)
                parent_idv['child_award_count'] = len(parent_idv['child_awards'])
                idv_child_linked += 1
            else:
                # Parent IDV not in current bulk file - will check DynamoDB during indexing
                if parent_idv_id not in idv_child_unlinked:
                    idv_child_unlinked[parent_idv_id] = []
                idv_child_unlinked[parent_idv_id].append(award_id)
    
    # Check DynamoDB for missing parent IDVs
    if idv_child_unlinked:
        log_print(f"🔍 {agency_prefix}Checking DynamoDB for {len(idv_child_unlinked)} missing parent IDVs...")
        dynamodb_idv_linked = 0
        
        for parent_idv_id, child_award_ids in idv_child_unlinked.items():
            try:
                # Try to get parent IDV from DynamoDB
                response = awards_table.get_item(Key={'award_id': parent_idv_id})
                if 'Item' in response:
                    # Parent IDV exists in DynamoDB - will update during indexing
                    log_print(f"✅ {agency_prefix}Found parent IDV {parent_idv_id} in DynamoDB (will update during indexing)")
                    # Store child awards to be added during indexing
                    # Create a minimal parent IDV record that will trigger an update
                    if parent_idv_id not in all_prime_awards:
                        all_prime_awards[parent_idv_id] = {
                            'award_id': parent_idv_id,
                            'child_awards': child_award_ids,
                            'child_award_count': len(child_award_ids),
                            'update_from_dynamodb': True,
                            'existing_item': response['Item'],
                            'is_idv_parent': True
                        }
                    else:
                        # Parent IDV already in awards (shouldn't happen, but handle it)
                        if 'child_awards' not in all_prime_awards[parent_idv_id]:
                            all_prime_awards[parent_idv_id]['child_awards'] = []
                        all_prime_awards[parent_idv_id]['child_awards'].extend(child_award_ids)
                        all_prime_awards[parent_idv_id]['child_award_count'] = len(all_prime_awards[parent_idv_id]['child_awards'])
                    dynamodb_idv_linked += 1
                else:
                    log_print(f"⚠️ {agency_prefix}Parent IDV {parent_idv_id} not found in bulk file or DynamoDB for {len(child_award_ids)} child award(s)")
            except Exception as e:
                error_msg = f"Error checking DynamoDB for parent IDV {parent_idv_id}: {str(e)}"
                log_print(f"⚠️ {agency_prefix}{error_msg}")
                logger.error(f"⚠️ {agency_prefix}{error_msg}", exc_info=True)
                # Continue processing other parents even if one fails
        
        if dynamodb_idv_linked > 0:
            log_print(f"✅ {agency_prefix}Found {dynamodb_idv_linked} parent IDVs in DynamoDB")
    
    if idv_child_linked > 0:
        log_print(f"✅ {agency_prefix}Linked {idv_child_linked} IDV child awards to parent IDVs from bulk file")
    
        parse_duration = time.time() - parse_start_time
        log_print(f"✅ {agency_prefix}CSV Parsing Completed:")
        log_print(f"   📊 Prime Awards: {len(all_prime_awards):,}")
        log_print(f"   📊 Sub-Awards: {sum(len(subs) for subs in all_subawards_by_parent.values()):,}")
        log_print(f"   📊 IDV Child Awards: {sum(1 for a in all_prime_awards.values() if a.get('is_idv_child'))}")
        log_print(f"   ⏱️ Parse Time: {int(parse_duration // 60)}m {int(parse_duration % 60)}s")
    
    return {
        'prime_awards': all_prime_awards,
        'subawards_by_parent': all_subawards_by_parent,
        'csv_s3_keys': csv_s3_keys  # Include CSV S3 keys for DLQ processing
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
        
        # Normalize recipient_name for GSI (required for RecipientNameFiscalYearIndex)
        # Store raw recipient_name in uppercase to match USAspending standard and autocomplete behavior
        recipient_name = db_item.get('recipient_name') or db_item.get('prime_awardee_name')
        if recipient_name:
            # Ensure recipient_name is stored in uppercase (matches USAspending standard)
            if isinstance(recipient_name, str):
                recipient_name_upper = recipient_name.upper()
                db_item['recipient_name'] = recipient_name_upper
                db_item['recipient_name_normalized'] = recipient_name_upper.lower()
            else:
                db_item['recipient_name_normalized'] = str(recipient_name).lower()
        else:
            # If recipient_name is missing, set a default value for GSI (required field)
            # Use "unknown" as normalized value to ensure GSI can be queried
            db_item['recipient_name_normalized'] = "unknown"
        
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
        
        # Set is_assistance number field (0 = contract, 1 = assistance)
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
        
        # Store as number: 0 for contract, 1 for assistance
        db_item['is_assistance'] = 1 if is_assistance else 0
        
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
            # Update existing item - MERGE all fields from award_record (NEW CSV data) into existing_item
            # This preserves existing fields and adds/updates new ones from the CSV
            # This handles cases where:
            # 1. Orphan sub-award: parent not in bulk download but exists in DynamoDB
            # 2. Transaction adjustments: parent appears in bulk download with updated transactions
            # 3. API-fetched awards: merge API fields with existing DynamoDB fields
            # IMPORTANT: When an existing award is updated, ALL fields from the NEW CSV are applied,
            # not just transactions and obligated_amount. This ensures the award reflects the latest CSV data.
            db_item = existing_item.copy()
            
            # Merge ALL fields from award_record (CSV) into db_item (preserve existing, update with new)
            # This updates ALL fields from the CSV: agency info, recipient info, dates, amounts, etc.
            # Skip internal fields that are handled separately (transactions/subawards merged later)
            skip_fields = {'transactions', 'subawards', 'child_awards', 'update_from_dynamodb', 'existing_item'}
            fields_updated_count = 0
            for key, value in award_record.items():
                if key in skip_fields:
                    continue
                if value is None:
                    continue
                
                # Convert value to appropriate type and update db_item
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
                            converted_list.append(normalize_string(item) if isinstance(item, str) else item)
                    db_item[key] = converted_list
                elif isinstance(value, dict):
                    db_item[key] = convert_floats_to_decimal(value)
                else:
                    db_item[key] = normalize_string(value) if isinstance(value, str) else value
                
                fields_updated_count += 1
            
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
                else:
                    # No existing transactions, use new ones
                    db_item['transactions'] = convert_floats_to_decimal(transactions)
                    db_item['transaction_count'] = transaction_count
                    # Use total_obligated_amount from CSV (don't calculate)
                    if 'total_obligated_amount' in award_record and award_record.get('total_obligated_amount'):
                        db_item['total_obligated_amount'] = award_record['total_obligated_amount']
                    elif 'total_dollars_obligated' in award_record and award_record.get('total_dollars_obligated'):
                        db_item['total_obligated_amount'] = award_record['total_dollars_obligated']
            
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
            
            # Merge child awards for IDV parents (append to existing if any)
            child_awards = award_record.get('child_awards', [])
            if db_item.get('is_idv_parent') or award_record.get('is_idv_parent'):
                existing_child_awards = db_item.get('child_awards', [])
                if existing_child_awards and child_awards:
                    # Combine and deduplicate child awards
                    existing_child_ids = set(existing_child_awards)
                    new_child_awards = [child_id for child_id in child_awards if child_id not in existing_child_ids]
                    
                    if new_child_awards:
                        db_item['child_awards'] = existing_child_awards + new_child_awards
                    else:
                        db_item['child_awards'] = existing_child_awards
                elif child_awards:
                    db_item['child_awards'] = child_awards
                
                # Update child award count
                db_item['child_award_count'] = len(db_item.get('child_awards', []))
            
            # Update counts from CSV data (authoritative source)
            # The CSV's transaction_count and subaward_count are calculated from the CSV's arrays
            # Use the CSV's calculated counts (transaction_count, subaward_count variables from CSV parsing)
            # These represent the authoritative counts from the NEW CSV data
            db_item['transaction_count'] = transaction_count  # From CSV (line 2166)
            db_item['subaward_count'] = subaward_count  # From CSV (line 2167)
            
            # Note: The merge logic above (lines 2189-2222) already updates ALL other fields from award_record (CSV)
            # This ensures that when an existing award is updated, ALL fields from the NEW CSV are applied,
            # not just transactions and obligated_amount. This includes:
            # - All agency fields (awarding_agency_code, awarding_agency_name, etc.)
            # - All recipient fields (recipient_name, recipient_location_state, etc.)
            # - All date fields (period_start_date, period_end_date, etc.)
            # - All other metadata fields from the CSV
            
            # Update timestamp
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
                        # Check if existing item already has an oversize_s3_key (from previous oversized state)
                        existing_oversize_key = db_item.get('oversize_s3_key')
                        
                        try:
                            # Store full merged item to S3 (will overwrite existing file if present)
                            # This includes all merged transactions, subawards, and other fields from the new CSV
                            oversize_s3_key = store_oversized_item_to_s3(award_id, db_item)
                            
                            # Extract only GSI fields for DynamoDB
                            gsi_only_item = extract_gsi_fields_only(db_item)
                            # Replace/update oversize_s3_key with the new one (contains merged data)
                            gsi_only_item['oversize_s3_key'] = oversize_s3_key
                            
                            # Try to store GSI-only item (replaces existing row with updated GSI fields + new S3 key)
                            awards_table.put_item(Item=gsi_only_item)
                            if existing_oversize_key:
                                logger.info(f"Replaced oversized award {award_id} in DynamoDB with merged data, updated S3 file")
                            else:
                                logger.info(f"Stored GSI fields for oversized award {award_id} to DynamoDB, full data in S3")
                            break
                        except Exception as gsi_error:
                            error_msg = f"Even GSI-only item too large for {award_id}: {str(gsi_error)[:200]}"
                            logger.error(error_msg)
                            raise Exception(error_msg)
                    
                    # Handle throttling with simple exponential backoff
                    elif 'ThrottlingException' in error_str or 'ProvisionedThroughputExceededException' in error_str:
                        if put_attempt < max_put_retries - 1:
                            # Simple exponential backoff: 2s, 4s, 8s
                            wait_time = 2 ** (put_attempt + 1)
                            wait_time = min(wait_time, 30)  # Cap at 30 seconds
                            # Only log throttling on first attempt to reduce noise
                            if put_attempt == 0:
                                logger.warning(f"DynamoDB throttled for award {award_id}, waiting {wait_time}s before retry")
                            time.sleep(wait_time)
                            continue
                        else:
                            # Out of retries - log error and re-raise
                            logger.error(f"DynamoDB throttling persisted after {max_put_retries} retries for award {award_id}")
                            raise
                    
                    # Handle other DynamoDB errors
                    elif 'ClientError' in error_str or 'Boto3Error' in error_str:
                        logger.error(f"DynamoDB client error for award {award_id}: {error_str[:200]}")
                        raise
                    
                    # Re-raise if not a known error type or out of retries
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
        
        # Add child awards for IDV parents
        child_awards = award_record.get('child_awards', [])
        if award_record.get('is_idv_parent'):
            db_item['child_awards'] = child_awards
            db_item['child_award_count'] = len(child_awards)
            db_item['is_idv_parent'] = True
        
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
                    logger.warning(f"Award {award_id} exceeds DynamoDB size limit, storing to S3")
                    
                    # Store full item to S3
                    try:
                        oversize_s3_key = store_oversized_item_to_s3(award_id, db_item)
                        
                        # Extract only GSI fields for DynamoDB
                        gsi_only_item = extract_gsi_fields_only(db_item)
                        gsi_only_item['oversize_s3_key'] = oversize_s3_key
                        
                        # Try to store GSI-only item
                        awards_table.put_item(Item=gsi_only_item)
                        logger.info(f"Stored GSI fields for oversized award {award_id} to DynamoDB, full data in S3")
                        break
                    except Exception as gsi_error:
                        error_msg = f"Even GSI-only item too large for {award_id}: {str(gsi_error)[:200]}"
                        logger.error(error_msg)
                        raise Exception(error_msg)
                
                # Handle throttling with simple exponential backoff
                elif 'ThrottlingException' in error_str or 'ProvisionedThroughputExceededException' in error_str:
                    if put_attempt < max_put_retries - 1:
                        # Simple exponential backoff: 2s, 4s, 8s
                        wait_time = 2 ** (put_attempt + 1)
                        wait_time = min(wait_time, 30)  # Cap at 30 seconds
                        # Only log throttling on first attempt to reduce noise
                        if put_attempt == 0:
                            logger.warning(f"DynamoDB throttled for award {award_id}, waiting {wait_time}s before retry")
                        time.sleep(wait_time)
                        continue
                    else:
                        # Out of retries - log error and re-raise
                        error_msg = f"DynamoDB throttling persisted after {max_put_retries} retries for award {award_id}"
                        logger.error(error_msg)
                        raise Exception(error_msg)
                
                # Handle other DynamoDB errors
                elif 'ClientError' in error_str or 'Boto3Error' in error_str or 'botocore' in error_str.lower():
                    error_msg = f"DynamoDB client error for award {award_id}: {error_str[:200]}"
                    logger.error(error_msg)
                    raise Exception(error_msg)
                
                # Re-raise if not a known error type or out of retries
                raise
    
        return {
            'success': True,
            'award_id': award_id,
            'transaction_count': transaction_count,
            'subaward_count': subaward_count
        }
    
    except Exception as e:
        award_id = award_record.get('award_id', 'unknown')
        error_type = type(e).__name__
        error_msg = str(e)[:500]  # Limit error message length
        
        # Log error with context
        logger.error(f"Error indexing award {award_id}: {error_type}: {error_msg}", exc_info=True)
        
        # Re-raise to be caught by the calling code which will send to DLQ
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
                # Add csv_s3_keys to parse_results for DLQ processing
                parse_results['csv_s3_keys'] = csv_s3_keys
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
            # Track prime CSV S3 keys for this agency (for DLQ processing)
            csv_s3_keys_dict = parse_results.get('csv_s3_keys', {})
            prime_csv_s3_keys = [s3_key for filename, s3_key in csv_s3_keys_dict.items() if 'subaward' not in filename.lower()]
            
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
            
            # Use fixed worker count regardless of agency size
            # Size-based throttling removed - use consistent parallelism for all agencies
            base_workers = 10
            max_workers = min(base_workers, len(award_list))
            
            log_print(f"⚙️ Parallel Processing: {max_workers} workers")
            
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
                    executor.submit(index_award_complete, award_record): award_record
                    for award_record in award_list
                }
                
                for future in as_completed(future_to_award):
                    award_record = future_to_award[future]
                    award_id = award_record.get('award_id', 'unknown')
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
                        error_type = type(e).__name__
                        error_msg = str(e)[:300]  # Limit error message length
                        full_error = f"{error_type}: {error_msg}"
                        
                        # Track error for summary
                        agency_errors.append(f"Award {award_id}: {full_error}")
                        
                        # Log error (use logger instead of log_print to reduce noise)
                        logger.error(f"Failed to index award {award_id}: {full_error}", exc_info=False)
                        
                        # Store failed award to S3 and send to DLQ for individual processing
                        if DLQ_SQS_URL and award_id != 'unknown':
                            try:
                                # Store failed award as zipped JSON in S3
                                failed_award_s3_key = store_failed_award_to_s3(award_id, award_record)
                                
                                # Send S3 key to DLQ
                                message_body = {
                                    'award_id': award_id,
                                    'failed_award_s3_key': failed_award_s3_key,
                                    'error_type': error_type,
                                    'error_message': error_msg
                                }
                                message_json = json.dumps(message_body, default=str)
                                
                                sqs_client.send_message(
                                    QueueUrl=DLQ_SQS_URL,
                                    MessageBody=message_json
                                )
                                logger.info(f"Sent failed award {award_id} to DLQ (stored at {failed_award_s3_key})")
                            except Exception as dlq_error:
                                logger.warning(f"Failed to send award {award_id} to DLQ: {str(dlq_error)[:200]}")
                        elif not DLQ_SQS_URL:
                            logger.debug(f"DLQ_SQS_URL not configured, skipping DLQ for failed award {award_id}")
                        elif award_id == 'unknown':
                            logger.warning(f"Cannot send award to DLQ: award_id is missing from award_record")
                        
                        # Update progress counter even on error
                        with _progress_lock:
                            _progress_counter['processed'] += 1
            
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
