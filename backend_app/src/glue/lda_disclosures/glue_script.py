"""
AWS Glue Job: LDA Senate Lobbying Disclosures Indexing
Fetches, indexes, and downloads all lobbying disclosure filings and contributions from LDA Senate API.

This job:
1. Fetches all filings (LD-1, LD-2) and contributions (LD-203) from LDA API
2. Indexes all data to DynamoDB with GSIs for search
3. Downloads associated documents (PDF/HTML) to S3
4. Handles pagination and rate limiting
"""

import sys
import json
import logging
import time
import requests
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
from decimal import Decimal
from pathlib import Path
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

import boto3
from botocore.exceptions import ClientError

# ============================================================================
# Configuration
# ============================================================================

# Get required job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'LDA_API_BASE_URL',
    'LDA_SECRET_NAME',
    'FILINGS_TABLE_NAME',
    'S3_BUCKET_NAME',
    'REQUEST_TIMEOUT',
    'RATE_LIMIT_DELAY',
    'PAC_QUEUE_URL'
])

# Get date parameters (optional - can be null to fetch all records)
# Parse each argument individually since they're optional
start_date = None
end_date = None

# Print sys.argv for debugging
print(f"🔍 DEBUG: sys.argv = {sys.argv}", flush=True)

# Check if --START_DATE is in sys.argv
for i, arg in enumerate(sys.argv):
    if arg == '--START_DATE' and i + 1 < len(sys.argv):
        start_date_value = sys.argv[i + 1]
        if start_date_value and start_date_value.strip() and start_date_value.strip().lower() != 'null':
            start_date = start_date_value.strip()
        print(f"🔍 DEBUG: Found --START_DATE in sys.argv at index {i}, raw_value='{start_date_value}', parsed='{start_date}'", flush=True)
        break

# Check if --END_DATE is in sys.argv
for i, arg in enumerate(sys.argv):
    if arg == '--END_DATE' and i + 1 < len(sys.argv):
        end_date_value = sys.argv[i + 1]
        if end_date_value and end_date_value.strip() and end_date_value.strip().lower() != 'null':
            end_date = end_date_value.strip()
        print(f"🔍 DEBUG: Found --END_DATE in sys.argv at index {i}, raw_value='{end_date_value}', parsed='{end_date}'", flush=True)
        break

# Also try getResolvedOptions as a fallback for each individually
if start_date is None:
    try:
        date_args = getResolvedOptions(sys.argv, ['START_DATE'])
        start_date = date_args.get('START_DATE')
        if start_date == '':
            start_date = None
        print(f"🔍 DEBUG: getResolvedOptions found START_DATE={start_date}", flush=True)
    except Exception:
        pass  # START_DATE not provided, which is fine

if end_date is None:
    try:
        date_args = getResolvedOptions(sys.argv, ['END_DATE'])
        end_date = date_args.get('END_DATE')
        if end_date == '':
            end_date = None
        print(f"🔍 DEBUG: getResolvedOptions found END_DATE={end_date}", flush=True)
    except Exception:
        pass  # END_DATE not provided, which is fine

args['START_DATE'] = start_date
args['END_DATE'] = end_date
print(f"✅ Parsed date arguments: START_DATE={start_date}, END_DATE={end_date}", flush=True)

# Get optional testing parameter (passed as string from Step Functions)
# Check sys.argv directly since TESTING is optional and getResolvedOptions requires all args
# TESTING now accepts a number representing the limit per endpoint (filings endpoint and contributions endpoint)
testing_limit = None
testing_value = None

# Check if --TESTING is in sys.argv
for i, arg in enumerate(sys.argv):
    if arg == '--TESTING' and i + 1 < len(sys.argv):
        testing_value = sys.argv[i + 1]
        break

if testing_value is not None:
    print(f"🔍 DEBUG: Found TESTING in args: '{testing_value}' (type: {type(testing_value).__name__})", flush=True)
    try:
        # Try to parse as integer (number of records per type)
        testing_limit = int(testing_value)
        if testing_limit < 1:
            print(f"⚠️ TESTING value must be >= 1, ignoring: {testing_value}", flush=True)
            testing_limit = None
        else:
            print(f"🔍 DEBUG: Parsed TESTING value: '{testing_value}' -> {testing_limit} records per type", flush=True)
    except (ValueError, TypeError):
        # If not a number, check if it's a boolean for backward compatibility
        if isinstance(testing_value, str):
            testing_str = testing_value.strip().lower()
            if testing_str in ['true', '1', 'yes', 't']:
                testing_limit = 10  # Default to 10 for backward compatibility
                print(f"🔍 DEBUG: Parsed TESTING as boolean 'true', defaulting to {testing_limit} records per type", flush=True)
            else:
                print(f"⚠️ TESTING value must be a number or boolean, ignoring: {testing_value}", flush=True)
        else:
            print(f"⚠️ TESTING value must be a number, ignoring: {testing_value}", flush=True)
else:
    print(f"ℹ️ Testing parameter not provided (optional), processing all records", flush=True)

args['TESTING'] = testing_limit
if testing_limit:
    print(f"🧪 Testing mode enabled - will limit to {testing_limit} records per endpoint (filings and contributions)", flush=True)
else:
    print(f"ℹ️ Testing mode disabled - processing all records", flush=True)

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
log_print("✅ LDA Senate Lobbying Disclosures Glue Job - Script Loaded Successfully")
log_print("=" * 80)

# Environment variables
LDA_API_BASE_URL = args.get('LDA_API_BASE_URL', 'https://lda.senate.gov/api/v1')
LDA_SECRET_NAME = args.get('LDA_SECRET_NAME')
FILINGS_TABLE_NAME = args.get('FILINGS_TABLE_NAME')
S3_BUCKET_NAME = args.get('S3_BUCKET_NAME')
REQUEST_TIMEOUT = int(args.get('REQUEST_TIMEOUT', '30'))
RATE_LIMIT_DELAY = float(args.get('RATE_LIMIT_DELAY', '0.5'))
PAC_QUEUE_URL = args.get('PAC_QUEUE_URL', '')  # Optional - only send if queue URL is provided

# AWS clients
dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')
secrets_client = boto3.client('secretsmanager')
sqs_client = boto3.client('sqs')

# DynamoDB table (both filings and contributions use the same table)
filings_table = dynamodb.Table(FILINGS_TABLE_NAME)

log_print(f"ℹ️ Configuration: Table={FILINGS_TABLE_NAME} (filings and contributions), S3 Bucket={S3_BUCKET_NAME}")
if PAC_QUEUE_URL:
    log_print(f"✅ PAC Queue URL configured: {PAC_QUEUE_URL}")

# ============================================================================
# Rate Limiting
# ============================================================================

class RateLimiter:
    """Thread-safe rate limiter for API calls"""
    def __init__(self, calls_per_minute: int = 120):
        self.calls_per_minute = calls_per_minute
        self.min_interval = 60.0 / calls_per_minute  # Minimum seconds between calls
        self.last_call_time = 0.0
        self.lock = Lock()
    
    def wait(self):
        """Wait if necessary to respect rate limit"""
        with self.lock:
            current_time = time.time()
            time_since_last_call = current_time - self.last_call_time
            
            if time_since_last_call < self.min_interval:
                sleep_time = self.min_interval - time_since_last_call
                time.sleep(sleep_time)
            
            self.last_call_time = time.time()

# Global rate limiter (120 calls per minute = 0.5 seconds between calls)
rate_limiter = RateLimiter(calls_per_minute=120)

# ============================================================================
# Helper Functions
# ============================================================================

def get_api_key() -> str:
    """Retrieve LDA API key from Secrets Manager"""
    try:
        response = secrets_client.get_secret_value(SecretId=LDA_SECRET_NAME)
        secret_data = json.loads(response['SecretString'])
        return secret_data.get('api_key') or secret_data.get('API_KEY') or secret_data.get('LDA_API_KEY') or secret_data.get('lda_api_key')
    except ClientError as e:
        log_print(f"❌ Error retrieving API key from Secrets Manager: {str(e)}")
        raise

def create_session(api_key: str) -> requests.Session:
    """Create a requests session with Authorization header"""
    session = requests.Session()
    session.headers.update({
        'Authorization': f'Token {api_key}',
        'Accept': 'application/json',
    })
    return session

def call_api(session: requests.Session, endpoint: str, params: Optional[Dict] = None) -> Dict:
    """Call LDA API endpoint with thread-safe rate limiting"""
    url = f"{LDA_API_BASE_URL}{endpoint}"
    
    rate_limiter.wait()  # Thread-safe rate limiting
    response = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
    
    if response.status_code >= 400:
        log_print(f"   🔍 Error Details:")
        log_print(f"      Status: {response.status_code}")
        log_print(f"      URL: {response.url}")
        try:
            error_data = response.json()
            log_print(f"      Error response: {json.dumps(error_data, indent=2)}")
        except:
            log_print(f"      Error text: {response.text[:500]}")
    
    response.raise_for_status()
    return response.json()

def download_document(session: requests.Session, url: str, s3_key: str) -> bool:
    """Download a document from URL and upload to S3"""
    try:
        rate_limiter.wait()  # Thread-safe rate limiting
        response = session.get(url, timeout=REQUEST_TIMEOUT, stream=True)
        response.raise_for_status()
        
        # Upload directly to S3
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=s3_key,
            Body=response.content,
            ContentType=response.headers.get('Content-Type', 'application/pdf')
        )
        
        return True
    except Exception as e:
        log_print(f"   ❌ Failed to download {url}: {str(e)[:200]}")
        return False

def merge_address_fields(item: Dict, registrant: Optional[Dict] = None) -> Dict:
    """
    Merge address fields from top-level and registrant into unified address structure.
    Priority: top-level address fields > registrant address fields > registrant object address fields
    """
    unified_address = {}
    
    # Check top-level address fields first
    if item.get('address_1'):
        unified_address['address_1'] = item.get('address_1')
    if item.get('address_2'):
        unified_address['address_2'] = item.get('address_2')
    if item.get('city'):
        unified_address['city'] = item.get('city')
    if item.get('state'):
        unified_address['state'] = item.get('state')
    if item.get('zip'):
        unified_address['zip'] = item.get('zip')
    if item.get('country'):
        unified_address['country'] = item.get('country')
    
    # Fall back to registrant-specific address fields if top-level is missing
    if not unified_address.get('address_1') and item.get('registrant_address_1'):
        unified_address['address_1'] = item.get('registrant_address_1')
    if not unified_address.get('address_2') and item.get('registrant_address_2'):
        unified_address['address_2'] = item.get('registrant_address_2')
    if not unified_address.get('city') and item.get('registrant_city'):
        unified_address['city'] = item.get('registrant_city')
    if not unified_address.get('state') and item.get('registrant_state'):
        unified_address['state'] = item.get('registrant_state')
    if not unified_address.get('zip') and item.get('registrant_zip'):
        unified_address['zip'] = item.get('registrant_zip')
    if not unified_address.get('country') and item.get('registrant_country'):
        unified_address['country'] = item.get('registrant_country')
    
    # Finally, fall back to registrant object address fields if still missing
    if registrant:
        if not unified_address.get('address_1') and registrant.get('address_1'):
            unified_address['address_1'] = registrant.get('address_1')
        if not unified_address.get('address_2') and registrant.get('address_2'):
            unified_address['address_2'] = registrant.get('address_2')
        if not unified_address.get('city') and registrant.get('city'):
            unified_address['city'] = registrant.get('city')
        if not unified_address.get('state') and registrant.get('state'):
            unified_address['state'] = registrant.get('state')
        if not unified_address.get('zip') and registrant.get('zip'):
            unified_address['zip'] = registrant.get('zip')
        if not unified_address.get('country') and registrant.get('country'):
            unified_address['country'] = registrant.get('country')
    
    return unified_address if unified_address else None

def create_unified_entity(client: Optional[Dict], lobbyist: Optional[Dict]) -> Optional[Dict]:
    """
    Create a unified entity field from either client or lobbyist (mutually exclusive).
    Returns a dict with 'entity_type' ('client' or 'lobbyist') and 'entity' (the object).
    """
    if client:
        return {
            'entity_type': 'client',
            'entity': client
        }
    elif lobbyist:
        return {
            'entity_type': 'lobbyist',
            'entity': lobbyist
        }
    return None

def send_autocomplete_value(field_type: str, value: str):
    """
    Send an autocomplete value to SQS queue for CSV generation.
    Cleans double quotes and commas from values before sending (would confuse CSV structure).
    
    Args:
        field_type: One of 'pac_name', 'client_name', 'lobbyist_name', 'registrant_name'
        value: The string value to add to autocomplete CSV
    """
    if not PAC_QUEUE_URL or not value:
        return
    
    # Clean value: remove double quotes, commas, and strip whitespace
    cleaned_value = value.strip()
    
    # Remove surrounding double quotes if present
    if cleaned_value.startswith('"') and cleaned_value.endswith('"'):
        cleaned_value = cleaned_value[1:-1]
    
    # Remove any remaining double quotes (shouldn't happen, but be safe)
    cleaned_value = cleaned_value.replace('"', '')
    
    # Remove commas (would confuse CSV structure and search)
    cleaned_value = cleaned_value.replace(',', '')
    
    # Normalize whitespace (multiple spaces to single space)
    cleaned_value = ' '.join(cleaned_value.split())
    
    if not cleaned_value:
        return
    
    try:
        sqs_client.send_message(
            QueueUrl=PAC_QUEUE_URL,
            MessageBody=json.dumps({
                'field_type': field_type,
                'value': cleaned_value
            })
        )
    except Exception as e:
        log_print(f"⚠️ Failed to send {field_type} '{cleaned_value}' to SQS: {str(e)[:200]}")

def extract_indexed_fields_filing(filing: Dict) -> Dict:
    """Extract indexed fields for a filing (LD-1 or LD-2)"""
    indexed = {}
    
    # Primary Key
    indexed['filing_uuid'] = filing.get('filing_uuid')
    
    # Registrant
    registrant = filing.get('registrant', {})
    if registrant:
        indexed['registrant_id'] = registrant.get('id')
        indexed['registrant_name'] = registrant.get('name')
        indexed['registrant_house_registrant_id'] = registrant.get('house_registrant_id')
    
    # Client
    client = filing.get('client', {})
    if client:
        indexed['client_id'] = client.get('id')
        indexed['client_name'] = client.get('name')
        indexed['client_client_id'] = client.get('client_id')
    
    # Lobbyists, General Issue Code, and Government Entity ID (get first activity for GSI - can expand later)
    lobbying_activities = filing.get('lobbying_activities', [])
    all_general_issue_codes = []
    all_government_entity_ids = []
    
    if lobbying_activities:
        for activity in lobbying_activities:
            # Extract general issue code from first activity (for GSI)
            general_issue_code = activity.get('general_issue_code')
            if general_issue_code:
                if not indexed.get('general_issue_code'):  # Only set first one for GSI
                    indexed['general_issue_code'] = general_issue_code
                    indexed['general_issue_code_display'] = activity.get('general_issue_code_display')
                # Collect all codes for autocomplete
                if general_issue_code not in all_general_issue_codes:
                    all_general_issue_codes.append(general_issue_code)
            
            # Extract first government entity ID from first activity (for GSI)
            government_entities = activity.get('government_entities', [])
            if government_entities:
                first_entity = government_entities[0]
                entity_id = first_entity.get('id')
                if entity_id:
                    if 'government_entity_id' not in indexed:  # Only set first one for GSI
                        try:
                            indexed['government_entity_id'] = int(entity_id)
                        except (ValueError, TypeError):
                            pass
                    # Collect all entity IDs for autocomplete
                    try:
                        entity_id_int = int(entity_id)
                        if entity_id_int not in all_government_entity_ids:
                            all_government_entity_ids.append(entity_id_int)
                    except (ValueError, TypeError):
                        pass
                # Collect all entity IDs from all entities in this activity
                for entity in government_entities:
                    entity_id = entity.get('id')
                    if entity_id:
                        try:
                            entity_id_int = int(entity_id)
                            if entity_id_int not in all_government_entity_ids:
                                all_government_entity_ids.append(entity_id_int)
                        except (ValueError, TypeError):
                            pass
            
            # Extract first lobbyist
            lobbyists = activity.get('lobbyists', [])
            if lobbyists:
                lobbyist = lobbyists[0].get('lobbyist', {})
                if lobbyist:
                    if 'lobbyist_name' not in indexed:  # Only set first one for GSI
                        name_parts = [
                            lobbyist.get('prefix_display', ''),
                            lobbyist.get('first_name', ''),
                            lobbyist.get('middle_name', ''),
                            lobbyist.get('last_name', ''),
                            lobbyist.get('suffix_display', '')
                        ]
                        indexed['lobbyist_name'] = ' '.join(filter(None, name_parts))
                        indexed['lobbyist_id'] = lobbyist.get('id')
                    break  # Use first lobbyist and first issue code for GSI
    
    # Store all codes as lists for autocomplete (preserves all activities)
    if all_general_issue_codes:
        indexed['all_general_issue_codes'] = all_general_issue_codes
    if all_government_entity_ids:
        indexed['all_government_entity_ids'] = all_government_entity_ids
    
    # Foreign Entities - extract country codes from foreign_entities list
    foreign_entities = filing.get('foreign_entities', [])
    foreign_countries = set()  # Use set for automatic deduplication
    
    if foreign_entities:
        for entity in foreign_entities:
            # Check both 'country' and 'ppb_country' fields
            country_code = entity.get('country') or entity.get('ppb_country')
            if country_code and country_code != 'US':  # Exclude US (domestic)
                foreign_countries.add(country_code)
    
    # Set is_foreign as number for GSI (1 if any foreign countries found, 0 otherwise)
    indexed['is_foreign'] = 1 if len(foreign_countries) > 0 else 0
    
    # Store all foreign countries as a list for autocomplete (deduplicated)
    if foreign_countries:
        indexed['foreign_countries'] = sorted(list(foreign_countries))
    
    # Report type
    indexed['report_type'] = filing.get('filing_type')
    indexed['report_type_display'] = filing.get('filing_type_display')
    
    # Filing period and year
    indexed['filing_period'] = filing.get('filing_period')
    indexed['filing_period_display'] = filing.get('filing_period_display')
    # filing_year must be a number (N type) for GSI
    filing_year = filing.get('filing_year')
    if filing_year:
        try:
            indexed['filing_year'] = int(filing_year)
        except (ValueError, TypeError):
            indexed['filing_year'] = filing_year
    
    # Posted date
    indexed['dt_posted'] = filing.get('dt_posted')
    
    # Amount fields
    income = filing.get('income')
    if income is not None:
        try:
            indexed['amount_reported'] = Decimal(str(income))
        except (ValueError, TypeError):
            pass
    
    # Convert expenses to Decimal if present
    expenses = filing.get('expenses')
    if expenses is not None:
        try:
            indexed['expenses'] = Decimal(str(expenses))
        except (ValueError, TypeError):
            indexed['expenses'] = expenses  # Keep original if conversion fails
    else:
        indexed['expenses'] = None
    
    indexed['expenses_method'] = filing.get('expenses_method')
    
    # Extract state from address or registrant
    state = None
    if filing.get('address'):
        state = filing['address'].get('state')
    if not state and registrant:
        if registrant.get('address'):
            state = registrant['address'].get('state')
        if not state:
            state = registrant.get('state')
    indexed['state'] = state
    
    return indexed

def extract_indexed_fields_contribution(contribution: Dict) -> Dict:
    """Extract indexed fields for a contribution report (LD-203)"""
    indexed = {}
    
    # Primary Key
    indexed['filing_uuid'] = contribution.get('filing_uuid')
    
    # Registrant
    registrant = contribution.get('registrant', {})
    if registrant:
        indexed['registrant_id'] = registrant.get('id')
        indexed['registrant_name'] = registrant.get('name')
        indexed['registrant_house_registrant_id'] = registrant.get('house_registrant_id')
    
    # Client (contributions may have a client field - check for it)
    client = contribution.get('client', {})
    if client:
        indexed['client_id'] = client.get('id')
        indexed['client_name'] = client.get('name')
        indexed['client_client_id'] = client.get('client_id')
    
    # Lobbyist
    lobbyist = contribution.get('lobbyist', {})
    if lobbyist:
        name_parts = [
            lobbyist.get('prefix_display', ''),
            lobbyist.get('first_name', ''),
            lobbyist.get('middle_name', ''),
            lobbyist.get('last_name', ''),
            lobbyist.get('suffix_display', '')
        ]
        indexed['lobbyist_name'] = ' '.join(filter(None, name_parts))
        indexed['lobbyist_id'] = lobbyist.get('id')
    
    # Report type
    indexed['report_type'] = contribution.get('filing_type')
    indexed['report_type_display'] = contribution.get('filing_type_display')
    
    # Filing period and year
    indexed['filing_period'] = contribution.get('filing_period')
    indexed['filing_period_display'] = contribution.get('filing_period_display')
    # filing_year must be a number (N type) for GSI
    filing_year = contribution.get('filing_year')
    if filing_year:
        try:
            indexed['filing_year'] = int(filing_year)
        except (ValueError, TypeError):
            indexed['filing_year'] = filing_year
    
    # Posted date
    indexed['dt_posted'] = contribution.get('dt_posted')
    
    # Filer type (only for contributions - indicates who filed: registrant, lobbyist, etc.)
    indexed['filer_type'] = contribution.get('filer_type')
    indexed['filer_type_display'] = contribution.get('filer_type_display')
    
    # Calculate total contribution amount from contribution_items
    # Also use this as amount_reported for contributions
    # Extract first contribution_item_type for GSI
    contribution_items = contribution.get('contribution_items', [])
    total_amount = Decimal('0')
    all_contribution_item_types = []
    
    if contribution_items:
        for item in contribution_items:
            # Extract contribution type (for GSI and autocomplete)
            contribution_type = item.get('contribution_type')
            if contribution_type:
                if 'contribution_item_type' not in indexed:  # Only set first one for GSI
                    indexed['contribution_item_type'] = contribution_type
                # Collect all types for autocomplete
                if contribution_type not in all_contribution_item_types:
                    all_contribution_item_types.append(contribution_type)
            
            # Calculate amount
            amount_str = item.get('amount')
            if amount_str:
                try:
                    total_amount += Decimal(str(amount_str))
                except (ValueError, TypeError):
                    pass  # Skip invalid amounts
    
    # For contributions, amount_reported comes from contribution_items (used for GSI)
    indexed['amount_reported'] = total_amount if total_amount > 0 else None
    
    # Store all contribution item types as list for autocomplete
    if all_contribution_item_types:
        indexed['all_contribution_item_types'] = all_contribution_item_types
    
    # Extract state from address or registrant
    state = None
    if contribution.get('address'):
        state = contribution['address'].get('state')
    if not state and registrant:
        if registrant.get('address'):
            state = registrant['address'].get('state')
        if not state:
            state = registrant.get('state')
    indexed['state'] = state
    
    return indexed

def save_filing_to_dynamodb(filing: Dict, indexed_fields: Dict, s3_key: Optional[str] = None):
    """Save filing to DynamoDB with all fields and indexed GSI fields"""
    try:
        # Prepare item with all filing data
        item = json.loads(json.dumps(filing), parse_float=Decimal)  # Convert floats to Decimal
        
        # Add indexed fields for GSIs (this includes amount_reported and expenses as Decimal)
        item.update(indexed_fields)
        
        # Ensure expenses is stored as Decimal if it exists in indexed_fields
        if 'expenses' in indexed_fields and indexed_fields['expenses'] is not None:
            item['expenses'] = indexed_fields['expenses']
        
        # Add S3 key if provided
        if s3_key:
            item['s3_key'] = s3_key
        
        # Merge address fields into unified structure
        registrant = item.get('registrant')
        unified_address = merge_address_fields(item, registrant)
        if unified_address:
            item['address'] = unified_address
        
        # Remove redundant address fields - clean up top-level address fields
        item.pop('address_1', None)
        item.pop('address_2', None)
        item.pop('zip', None)
        item.pop('registrant_address_1', None)
        item.pop('registrant_address_2', None)
        item.pop('registrant_city', None)
        item.pop('registrant_state', None)
        item.pop('registrant_zip', None)
        item.pop('registrant_country', None)
        
        # Create unified entity field (client takes priority over lobbyist for filings)
        client = item.get('client')
        # Extract first lobbyist from lobbying_activities if present
        lobbyist = None
        lobbying_activities = item.get('lobbying_activities', [])
        if lobbying_activities:
            for activity in lobbying_activities:
                lobbyists = activity.get('lobbyists', [])
                if lobbyists:
                    lobbyist = lobbyists[0].get('lobbyist', {})
                    break
        unified_entity = create_unified_entity(client, lobbyist)
        if unified_entity:
            item['entity'] = unified_entity
        
        # Set null values for contribution-specific fields (not applicable to filings)
        item['contribution_items'] = None
        item['no_contributions'] = None
        # filer_type - not applicable to filings, ensure it's removed (omit, don't set to None)
        item.pop('filer_type', None)
        item.pop('filer_type_display', None)
        # pac - not applicable to filings, ensure it's removed
        item.pop('pac', None)
        
        # Set primary key
        item['PK'] = f"FILING#{item['filing_uuid']}"
        item['SK'] = f"FILING#{item['filing_uuid']}"
        
        # Set GSI fields using existing fields (matching government contracts pattern)
        # GSI fields are set directly from indexed_fields - no special GSI field names
        # DynamoDB will automatically index these fields based on the GSI definitions
        
        # filing_year and dt_posted are already in indexed_fields and will be in item
        # filing_period and dt_posted are already in indexed_fields
        # report_type and dt_posted are already in indexed_fields
        # registrant_name and dt_posted are already in indexed_fields
        # Send registrant_name to autocomplete queue
        if indexed_fields.get('registrant_name'):
            send_autocomplete_value('registrant_name', indexed_fields['registrant_name'])
        
        # client_name and dt_posted - only set if client_name exists (omit if missing)
        if not indexed_fields.get('client_name'):
            item.pop('client_name', None)
        else:
            # Send client_name to autocomplete queue
            send_autocomplete_value('client_name', indexed_fields['client_name'])
        
        # lobbyist_name and dt_posted are already in indexed_fields
        if not indexed_fields.get('lobbyist_name'):
            item.pop('lobbyist_name', None)
        else:
            # Send lobbyist_name to autocomplete queue
            send_autocomplete_value('lobbyist_name', indexed_fields['lobbyist_name'])
        
        # amount_bucket and amount_reported - only set if amount_reported exists
        if indexed_fields.get('amount_reported'):
            amount = indexed_fields['amount_reported']
            # Round to nearest 10k for partition key (amount_bucket)
            amount_bucket = int(float(amount) / 10000) * 10000
            item['amount_bucket'] = Decimal(str(amount_bucket))
            # amount_reported is already in indexed_fields
        else:
            item.pop('amount_reported', None)
            item.pop('amount_bucket', None)
        
        # state and dt_posted - only set if state exists (omit if missing)
        if not indexed_fields.get('state'):
            item.pop('state', None)
        
        # general_issue_code and dt_posted - only set if general_issue_code exists (omit if missing)
        # Note: Only filings have lobbying_activities, so this will be None for contributions
        if not indexed_fields.get('general_issue_code'):
            item.pop('general_issue_code', None)
            item.pop('general_issue_code_display', None)
        
        # government_entity_id and dt_posted - only set if government_entity_id exists (omit if missing)
        if not indexed_fields.get('government_entity_id'):
            item.pop('government_entity_id', None)
        else:
            # Ensure it's stored as integer (N type)
            item['government_entity_id'] = int(indexed_fields['government_entity_id'])
        
        # contribution_item_type - not applicable to filings, ensure it's removed
        item.pop('contribution_item_type', None)
        item.pop('all_contribution_item_types', None)
        
        # is_foreign and dt_posted - only set if is_foreign is 1 (omit if 0)
        if indexed_fields.get('is_foreign') != 1:
            item.pop('is_foreign', None)
        else:
            # Ensure it's stored as integer (N type)
            item['is_foreign'] = 1
        
        # Save to DynamoDB
        filings_table.put_item(Item=item)
        
    except Exception as e:
        log_print(f"❌ Error saving filing to DynamoDB: {str(e)[:200]}")
        raise

def save_contribution_to_dynamodb(contribution: Dict, indexed_fields: Dict, s3_key: Optional[str] = None):
    """Save contribution to DynamoDB with all fields and indexed GSI fields (same table as filings)"""
    try:
        # Prepare item with all contribution data
        item = json.loads(json.dumps(contribution), parse_float=Decimal)
        
        # Add indexed fields for GSIs (this includes total_contribution_amount as Decimal)
        item.update(indexed_fields)
        
        # Add S3 key if provided
        if s3_key:
            item['s3_key'] = s3_key
        
        # Merge address fields into unified structure
        registrant = item.get('registrant')
        unified_address = merge_address_fields(item, registrant)
        if unified_address:
            item['address'] = unified_address
        
        # Remove redundant address fields - clean up top-level address fields
        item.pop('address_1', None)
        item.pop('address_2', None)
        item.pop('zip', None)
        item.pop('registrant_address_1', None)
        item.pop('registrant_address_2', None)
        item.pop('registrant_city', None)
        item.pop('registrant_state', None)
        item.pop('registrant_zip', None)
        item.pop('registrant_country', None)
        
        # Create unified entity field (client or lobbyist - mutually exclusive for contributions)
        client = item.get('client')
        lobbyist = item.get('lobbyist')
        unified_entity = create_unified_entity(client, lobbyist)
        if unified_entity:
            item['entity'] = unified_entity
        
        # Check for PACs (Political Action Committees)
        pacs = contribution.get('pacs', [])
        # Store as number (1 if PACs exist, 0 otherwise) for GSI
        indexed_fields['pac'] = 1 if (pacs and len(pacs) > 0) else 0
        item['pac'] = indexed_fields['pac']
        
        # Send PAC names to SQS for autocomplete (if queue URL is configured)
        # Note: send_autocomplete_value() will clean double quotes from PAC names
        if PAC_QUEUE_URL and pacs:
            log_print(f"📤 Found {len(pacs)} PAC(s) in contribution, sending to SQS...")
            for pac in pacs:
                # Handle both formats: object with 'name' field or string
                if isinstance(pac, dict):
                    pac_name = pac.get('name') or pac.get('S')  # Handle DynamoDB native format {"S": "name"}
                elif isinstance(pac, str):
                    pac_name = pac
                else:
                    pac_name = str(pac)
                
                if pac_name:
                    # send_autocomplete_value() will clean double quotes before sending to SQS
                    send_autocomplete_value('pac_name', pac_name)
                else:
                    log_print(f"⚠️ PAC object missing 'name' field: {pac}")
        elif pacs and not PAC_QUEUE_URL:
            log_print(f"⚠️ Found {len(pacs)} PAC(s) but PAC_QUEUE_URL not configured, skipping SQS send")
        
        # Set null values for filing-specific fields (not applicable to contributions)
        # Note: client and lobbyist fields are preserved from the original contribution if they exist
        # The unified 'entity' field is the primary way to access this data
        original_client = contribution.get('client')
        if not original_client:
            # No client in original contribution, set to None
            item['client'] = None
            item['client_id'] = None
            item['client_name'] = None
            item['client_client_id'] = None
        # If original_client exists, it's already in item from json.loads, so keep it
        item['income'] = None
        item['expenses'] = None
        item['expenses_method'] = None
        item['expenses_method_display'] = None
        item['lobbying_activities'] = None
        item['termination_date'] = None
        # Contributions don't have general_issue_code (only filings have lobbying_activities)
        item.pop('general_issue_code', None)
        item.pop('general_issue_code_display', None)
        # amount_reported is now set from contribution_items in extract_indexed_fields_contribution
        
        # Set primary key
        item['PK'] = f"CONTRIBUTION#{item['filing_uuid']}"
        item['SK'] = f"CONTRIBUTION#{item['filing_uuid']}"
        
        # Set GSI fields using existing fields (matching government contracts pattern)
        # GSI fields are set directly from indexed_fields - no special GSI field names
        # DynamoDB will automatically index these fields based on the GSI definitions
        
        # filing_year and dt_posted are already in indexed_fields and will be in item
        # filing_period and dt_posted are already in indexed_fields
        # report_type and dt_posted are already in indexed_fields
        # registrant_name and dt_posted are already in indexed_fields
        # Send registrant_name to autocomplete queue
        if indexed_fields.get('registrant_name'):
            send_autocomplete_value('registrant_name', indexed_fields['registrant_name'])
        
        # client_name and dt_posted - only set if client_name exists (omit if missing)
        if not indexed_fields.get('client_name'):
            item.pop('client_name', None)
        else:
            # Send client_name to autocomplete queue
            send_autocomplete_value('client_name', indexed_fields['client_name'])
        
        # lobbyist_name and dt_posted are already in indexed_fields
        if not indexed_fields.get('lobbyist_name'):
            item.pop('lobbyist_name', None)
        else:
            # Send lobbyist_name to autocomplete queue
            send_autocomplete_value('lobbyist_name', indexed_fields['lobbyist_name'])
        
        # amount_bucket and amount_reported - contributions can have amount_reported from contribution_items
        if indexed_fields.get('amount_reported'):
            amount = indexed_fields['amount_reported']
            # Round to nearest 10k for partition key (amount_bucket)
            amount_bucket = int(float(amount) / 10000) * 10000
            item['amount_bucket'] = Decimal(str(amount_bucket))
            # amount_reported is already in indexed_fields
        else:
            item.pop('amount_reported', None)
            item.pop('amount_bucket', None)
        
        # state and dt_posted - only set if state exists (omit if missing)
        if not indexed_fields.get('state'):
            item.pop('state', None)
        
        # filer_type and dt_posted - only set if filer_type exists (omit if missing)
        if not indexed_fields.get('filer_type'):
            item.pop('filer_type', None)
            item.pop('filer_type_display', None)
        
        # contribution_item_type and dt_posted - only set if contribution_item_type exists (omit if missing)
        if not indexed_fields.get('contribution_item_type'):
            item.pop('contribution_item_type', None)
        
        # government_entity_id - not applicable to contributions, ensure it's removed
        item.pop('government_entity_id', None)
        item.pop('all_government_entity_ids', None)
        item.pop('all_general_issue_codes', None)
        item.pop('general_issue_code', None)
        item.pop('general_issue_code_display', None)
        
        # is_foreign - not applicable to contributions, ensure it's removed
        item.pop('is_foreign', None)
        item.pop('foreign_countries', None)
        
        # Save to DynamoDB (same table as filings)
        filings_table.put_item(Item=item)
        
    except Exception as e:
        log_print(f"❌ Error saving contribution to DynamoDB: {str(e)[:200]}")
        raise

def process_single_filing(session: requests.Session, filing: Dict) -> Optional[str]:
    """Process a single filing (download document, save to DynamoDB)
    Uses data directly from listing endpoint - no additional API call needed
    Returns the filing_type if successful, None otherwise"""
    filing_uuid = filing.get('filing_uuid')
    if not filing_uuid:
        return None
    
    try:
        # Get filing type for tracking
        filing_type = filing.get('filing_type', '')
        
        # Extract indexed fields (uses data directly from listing API response)
        # The listing endpoint already includes all nested objects (registrant, client, lobbying_activities, etc.)
        indexed_fields = extract_indexed_fields_filing(filing)
        
        # Download document if available and get S3 key
        s3_key = None
        doc_url = filing.get('filing_document_url')
        if doc_url:
            content_type = filing.get('filing_document_content_type', 'pdf')
            ext = 'pdf' if 'pdf' in content_type.lower() else 'html'
            s3_key = f"filings/{filing_type}/{filing_uuid}.{ext}"
            download_document(session, doc_url, s3_key)  # Download silently, errors will be logged
        
        # Save to DynamoDB (with S3 key)
        save_filing_to_dynamodb(filing, indexed_fields, s3_key=s3_key)
        return filing_type
        
    except Exception as e:
        log_print(f"   ❌ Error processing filing {filing_uuid}: {str(e)[:200]}")
        return None

def fetch_filings_page(session: requests.Session, base_params: Dict, page: int) -> Optional[Dict]:
    """Fetch a single page of filings - used for parallel page fetching"""
    try:
        page_params = base_params.copy()
        page_params['page'] = page
        response = call_api(session, '/filings/', params=page_params)
        return response
    except Exception as e:
        log_print(f"   ❌ Error fetching filings page {page}: {str(e)[:200]}")
        return None

def process_all_filings(session: requests.Session, start_date: Optional[str] = None, end_date: Optional[str] = None, testing_limit: Optional[int] = None):
    """Fetch and process all filings with parallel API calls and parallel processing"""
    log_print("\n" + "="*80)
    log_print("📋 Processing Filings")
    log_print("="*80)
    
    if testing_limit:
        log_print(f"🧪 TESTING MODE: Limiting to {testing_limit} filings total")
    
    # Build base params - API requires at least one query parameter for pagination
    # If both dates are null, use a very early default date to effectively fetch all records
    base_params = {'page_size': 25}  # LDA API appears to return max 25 items per page
    
    if not start_date and not end_date:
        # API requires at least one filter parameter, so use a very early date to fetch all records
        # LDA data goes back to around 2000, so use 2000-01-01 as default
        base_params['filing_dt_posted_after'] = '2000-01-01'
        log_print(f"📅 No date filters provided - using default start date 2000-01-01 to fetch all records")
    else:
        if start_date:
            base_params['filing_dt_posted_after'] = start_date
        if end_date:
            base_params['filing_dt_posted_before'] = end_date
        
        # Log date range being used
        if start_date and end_date:
            log_print(f"📅 Date range: {start_date} to {end_date}")
        elif start_date:
            log_print(f"📅 Start date: {start_date} (no end date - fetching all records from start date forward)")
        elif end_date:
            log_print(f"📅 End date: {end_date} (no start date - fetching all records up to end date)")
    
    # Configuration for parallel fetching and processing
    # Reduced to 25 to avoid connection errors and stay within API limits
    max_api_workers = 25  # Number of pages to fetch in parallel
    max_processing_workers = 25  # Number of items to process in parallel
    
    # First, fetch page 1 to get total count and determine pagination
    log_print(f"\n📄 Fetching initial page to determine pagination...")
    first_response = fetch_filings_page(session, base_params, 1)
    if not first_response:
        log_print(f"❌ Failed to fetch initial page")
        return
    
    total_count = first_response.get('count', 0)
    first_results = first_response.get('results', [])
    has_next = first_response.get('next') is not None
    
    if not first_results:
        log_print(f"✅ No filings to process")
        return
    
    log_print(f"   Found {len(first_results)} filings on page 1 (total: {total_count})")
    
    # Calculate total pages (assuming 25 items per page)
    page_size = 25
    total_pages = (total_count + page_size - 1) // page_size if total_count > 0 else 1
    log_print(f"   Estimated total pages: {total_pages}")
    
    total_processed = 0
    current_page = 1
    batch_number = 0
    
    # Process first page results
    log_print(f"\n📊 Processing batch 1 (page 1) with {max_processing_workers} workers...")
    with ThreadPoolExecutor(max_workers=max_processing_workers) as processing_executor:
        future_to_filing = {
            processing_executor.submit(process_single_filing, session, filing): filing
            for filing in first_results
        }
        
        batch_processed = 0
        for future in as_completed(future_to_filing):
            filing = future_to_filing[future]
            try:
                filing_type = future.result()
                if filing_type:
                    batch_processed += 1
                    total_processed += 1
            except Exception as e:
                filing_uuid = filing.get('filing_uuid', 'unknown')
                log_print(f"   ❌ Exception processing filing {filing_uuid}: {str(e)[:200]}")
    
    log_print(f"✅ Batch 1 complete: Processed and indexed {batch_processed} filings from page 1 (Total: {total_processed})")
    
    # Check if we've reached testing limit after first page
    if testing_limit and total_processed >= testing_limit:
        log_print(f"✅ Reached testing limit: {total_processed}/{testing_limit}")
        return
    
    # Fetch remaining pages in parallel batches and process each page as it's fetched
    while has_next and current_page < total_pages:
        # Determine how many pages to fetch in this batch
        pages_to_fetch = min(max_api_workers, total_pages - current_page)
        if testing_limit and total_processed >= testing_limit:
            # We have enough results already, don't fetch more
            break
        
        batch_number += 1
        batch_start_page = current_page + 1
        batch_end_page = current_page + pages_to_fetch
        log_print(f"\n📄 Batch {batch_number + 1}: Fetching pages {batch_start_page} to {batch_end_page} in parallel (fanning out to {max_processing_workers} workers per page)...")
        
        # Fetch multiple pages in parallel, and process each page's results immediately as it's fetched
        pages_to_process = list(range(batch_start_page, batch_end_page + 1))
        
        # Use a shared processing executor for all pages in this batch
        # This allows up to 25 pages * 25 workers = 625 concurrent processing tasks
        with ThreadPoolExecutor(max_workers=max_api_workers * max_processing_workers) as processing_executor:
            # Track futures for both API calls and processing
            page_processing_futures = []
            batch_total_processed = 0
            
            with ThreadPoolExecutor(max_workers=max_api_workers) as api_executor:
                # Submit all page fetch requests
                future_to_page = {
                    api_executor.submit(fetch_filings_page, session, base_params, page): page
                    for page in pages_to_process
                }
                
                # As each page is fetched, immediately start processing it with 25 workers
                for future in as_completed(future_to_page):
                    page_num = future_to_page[future]
                    try:
                        response = future.result()
                        if response:
                            page_results = response.get('results', [])
                            if page_results:
                                # Check testing limit before processing
                                if testing_limit:
                                    remaining = testing_limit - total_processed
                                    if remaining <= 0:
                                        has_next = False
                                        break
                                    page_results = page_results[:remaining]
                                
                                # Immediately start processing this page's results with 25 workers
                                for filing in page_results:
                                    processing_future = processing_executor.submit(process_single_filing, session, filing)
                                    page_processing_futures.append((processing_future, filing, page_num))
                                
                                # Update total_processed count for testing limit check
                                total_processed += len(page_results)
                                if testing_limit and total_processed >= testing_limit:
                                    has_next = False
                                    break
                            else:
                                has_next = False
                                break
                        else:
                            log_print(f"   ⚠️ Page {page_num} returned None")
                    except Exception as e:
                        log_print(f"   ❌ Exception fetching page {page_num}: {str(e)[:200]}")
            
            # Wait for all processing to complete for this batch
            for processing_future, filing, page_num in page_processing_futures:
                try:
                    filing_type = processing_future.result()
                    if filing_type:
                        batch_total_processed += 1
                except Exception as e:
                    filing_uuid = filing.get('filing_uuid', 'unknown')
                    log_print(f"   ❌ Exception processing filing {filing_uuid} from page {page_num}: {str(e)[:200]}")
        
        log_print(f"✅ Batch {batch_number + 1} complete: Processed and indexed {batch_total_processed} filings from pages {batch_start_page}-{batch_end_page} (Total: {total_processed})")
        
        current_page = batch_end_page
        
        # Check if we've reached testing limit
        if testing_limit and total_processed >= testing_limit:
            log_print(f"✅ Reached testing limit: {total_processed}/{testing_limit}")
            break
        
        # Check if we should continue
        if not has_next:
            break
    
    log_print(f"\n✅ Finished processing all filings. Total processed and indexed: {total_processed}")

def fetch_contributions_page(session: requests.Session, base_params: Dict, page: int) -> Optional[Dict]:
    """Fetch a single page of contributions - used for parallel page fetching"""
    try:
        page_params = base_params.copy()
        page_params['page'] = page
        response = call_api(session, '/contributions/', params=page_params)
        return response
    except Exception as e:
        log_print(f"   ❌ Error fetching contributions page {page}: {str(e)[:200]}")
        return None

def process_single_contribution(session: requests.Session, contribution: Dict) -> bool:
    """Process a single contribution (download document, save to DynamoDB)
    Uses data directly from listing endpoint - no additional API call needed"""
    filing_uuid = contribution.get('filing_uuid')
    if not filing_uuid:
        return False
    
    try:
        # Extract indexed fields (uses data directly from listing API response)
        # The listing endpoint already includes all nested objects (registrant, client, lobbyist, contribution_items, etc.)
        indexed_fields = extract_indexed_fields_contribution(contribution)
        
        # Download document if available and get S3 key
        s3_key = None
        doc_url = contribution.get('filing_document_url')
        if doc_url:
            filing_type = contribution.get('filing_type', 'unknown')
            content_type = contribution.get('filing_document_content_type', 'pdf')
            ext = 'pdf' if 'pdf' in content_type.lower() else 'html'
            s3_key = f"contributions/{filing_type}/{filing_uuid}.{ext}"
            download_document(session, doc_url, s3_key)  # Download silently, errors will be logged
        
        # Save to DynamoDB (same table as filings, with S3 key)
        save_contribution_to_dynamodb(contribution, indexed_fields, s3_key=s3_key)
        return True
        
    except Exception as e:
        log_print(f"   ❌ Error processing contribution {filing_uuid}: {str(e)[:200]}")
        return False

def process_all_contributions(session: requests.Session, start_date: Optional[str] = None, end_date: Optional[str] = None, testing_limit: Optional[int] = None):
    """Fetch and process all contributions with parallel API calls and parallel processing"""
    log_print("\n" + "="*80)
    log_print("📋 Processing Contributions (LD-203)")
    log_print("="*80)
    
    if testing_limit:
        log_print(f"🧪 TESTING MODE: Limiting to {testing_limit} contributions total")
    
    # Build base params - API requires at least one query parameter for pagination
    # If both dates are null, use a very early default date to effectively fetch all records
    base_params = {'page_size': 25}  # LDA API appears to return max 25 items per page
    
    if not start_date and not end_date:
        # API requires at least one filter parameter, so use a very early date to fetch all records
        # LDA data goes back to around 2000, so use 2000-01-01 as default
        base_params['filing_dt_posted_after'] = '2000-01-01'
        log_print(f"📅 No date filters provided - using default start date 2000-01-01 to fetch all records")
    else:
        if start_date:
            base_params['filing_dt_posted_after'] = start_date
        if end_date:
            base_params['filing_dt_posted_before'] = end_date
        
        # Log date range being used
        if start_date and end_date:
            log_print(f"📅 Date range: {start_date} to {end_date}")
        elif start_date:
            log_print(f"📅 Start date: {start_date} (no end date - fetching all records from start date forward)")
        elif end_date:
            log_print(f"📅 End date: {end_date} (no start date - fetching all records up to end date)")
    
    # Configuration for parallel fetching and processing
    # Reduced to 25 to avoid connection errors and stay within API limits
    max_api_workers = 25  # Number of pages to fetch in parallel
    max_processing_workers = 25  # Number of items to process in parallel
    
    # First, fetch page 1 to get total count and determine pagination
    log_print(f"\n📄 Fetching initial page to determine pagination...")
    first_response = fetch_contributions_page(session, base_params, 1)
    if not first_response:
        log_print(f"❌ Failed to fetch initial page")
        return
    
    total_count = first_response.get('count', 0)
    first_results = first_response.get('results', [])
    has_next = first_response.get('next') is not None
    
    if not first_results:
        log_print(f"✅ No contributions to process")
        return
    
    log_print(f"   Found {len(first_results)} contributions on page 1 (total: {total_count})")
    
    # Calculate total pages (assuming 25 items per page)
    page_size = 25
    total_pages = (total_count + page_size - 1) // page_size if total_count > 0 else 1
    log_print(f"   Estimated total pages: {total_pages}")
    
    total_processed = 0
    current_page = 1
    batch_number = 0
    
    # Process first page results
    log_print(f"\n📊 Processing batch 1 (page 1) with {max_processing_workers} workers...")
    with ThreadPoolExecutor(max_workers=max_processing_workers) as processing_executor:
        future_to_contribution = {
            processing_executor.submit(process_single_contribution, session, contribution): contribution
            for contribution in first_results
        }
        
        batch_processed = 0
        for future in as_completed(future_to_contribution):
            contribution = future_to_contribution[future]
            try:
                success = future.result()
                if success:
                    batch_processed += 1
                    total_processed += 1
            except Exception as e:
                contribution_uuid = contribution.get('filing_uuid', 'unknown')
                log_print(f"   ❌ Exception processing contribution {contribution_uuid}: {str(e)[:200]}")
    
    log_print(f"✅ Batch 1 complete: Processed and indexed {batch_processed} contributions from page 1 (Total: {total_processed})")
    
    # Check if we've reached testing limit after first page
    if testing_limit and total_processed >= testing_limit:
        log_print(f"✅ Reached testing limit: {total_processed}/{testing_limit}")
        return
    
    # Fetch remaining pages in parallel batches and process each page as it's fetched
    while has_next and current_page < total_pages:
        # Determine how many pages to fetch in this batch
        pages_to_fetch = min(max_api_workers, total_pages - current_page)
        if testing_limit and total_processed >= testing_limit:
            # We have enough results already, don't fetch more
            break
        
        batch_number += 1
        batch_start_page = current_page + 1
        batch_end_page = current_page + pages_to_fetch
        log_print(f"\n📄 Batch {batch_number + 1}: Fetching pages {batch_start_page} to {batch_end_page} in parallel (fanning out to {max_processing_workers} workers per page)...")
        
        # Fetch multiple pages in parallel, and process each page's results immediately as it's fetched
        pages_to_process = list(range(batch_start_page, batch_end_page + 1))
        
        # Use a shared processing executor for all pages in this batch
        # This allows up to 25 pages * 25 workers = 625 concurrent processing tasks
        with ThreadPoolExecutor(max_workers=max_api_workers * max_processing_workers) as processing_executor:
            # Track futures for both API calls and processing
            page_processing_futures = []
            batch_total_processed = 0
            
            with ThreadPoolExecutor(max_workers=max_api_workers) as api_executor:
                # Submit all page fetch requests
                future_to_page = {
                    api_executor.submit(fetch_contributions_page, session, base_params, page): page
                    for page in pages_to_process
                }
                
                # As each page is fetched, immediately start processing it with 25 workers
                for future in as_completed(future_to_page):
                    page_num = future_to_page[future]
                    try:
                        response = future.result()
                        if response:
                            page_results = response.get('results', [])
                            if page_results:
                                # Check testing limit before processing
                                if testing_limit:
                                    remaining = testing_limit - total_processed
                                    if remaining <= 0:
                                        has_next = False
                                        break
                                    page_results = page_results[:remaining]
                                
                                # Immediately start processing this page's results with 25 workers
                                for contribution in page_results:
                                    processing_future = processing_executor.submit(process_single_contribution, session, contribution)
                                    page_processing_futures.append((processing_future, contribution, page_num))
                                
                                # Update total_processed count for testing limit check
                                total_processed += len(page_results)
                                if testing_limit and total_processed >= testing_limit:
                                    has_next = False
                                    break
                            else:
                                has_next = False
                                break
                        else:
                            log_print(f"   ⚠️ Page {page_num} returned None")
                    except Exception as e:
                        log_print(f"   ❌ Exception fetching page {page_num}: {str(e)[:200]}")
            
            # Wait for all processing to complete for this batch
            for processing_future, contribution, page_num in page_processing_futures:
                try:
                    success = processing_future.result()
                    if success:
                        batch_total_processed += 1
                except Exception as e:
                    contribution_uuid = contribution.get('filing_uuid', 'unknown')
                    log_print(f"   ❌ Exception processing contribution {contribution_uuid} from page {page_num}: {str(e)[:200]}")
        
        log_print(f"✅ Batch {batch_number + 1} complete: Processed and indexed {batch_total_processed} contributions from pages {batch_start_page}-{batch_end_page} (Total: {total_processed})")
        
        current_page = batch_end_page
        
        # Check if we've reached testing limit
        if testing_limit and total_processed >= testing_limit:
            log_print(f"✅ Reached testing limit: {total_processed}/{testing_limit}")
            break
        
        # Check if we should continue
        if not has_next:
            break
    
    log_print(f"\n✅ Finished processing all contributions. Total processed and indexed: {total_processed}")

# ============================================================================
# Main Execution
# ============================================================================

def main():
    """Main execution"""
    log_print("\n" + "="*80)
    log_print("🚀 Starting LDA Disclosures Indexing Job")
    log_print("="*80)
    
    # Get API key
    api_key = get_api_key()
    log_print("✅ Retrieved API key from Secrets Manager")
    
    # Create session
    session = create_session(api_key)
    
    # Get date range (optional - can be null to fetch all records)
    start_date = args.get('START_DATE')
    end_date = args.get('END_DATE')
    
    # Log date range configuration
    if start_date and end_date:
        log_print(f"📅 Date range: {start_date} to {end_date}")
    elif start_date:
        log_print(f"📅 Start date: {start_date} (no end date - will fetch all records from start date forward)")
    elif end_date:
        log_print(f"📅 End date: {end_date} (no start date - will fetch all records up to end date)")
    else:
        log_print(f"📅 No date filters provided - will use default start date 2000-01-01 to fetch all records (API requires at least one filter parameter)")
    
    # Get testing limit (number of records per type, or None for all records)
    testing_limit = args.get('TESTING')
    
    # Process filings (LD-1 and LD-2)
    process_all_filings(session, start_date, end_date, testing_limit=testing_limit)
    
    # Process contributions (LD-203)
    process_all_contributions(session, start_date, end_date, testing_limit=testing_limit)
    
    log_print("\n" + "="*80)
    log_print("✅ LDA Disclosures Indexing Job Completed Successfully")
    log_print("="*80)
    
    job.commit()

if __name__ == "__main__":
    main()

