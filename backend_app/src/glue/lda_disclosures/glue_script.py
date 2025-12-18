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
    'CONTRIBUTIONS_TABLE_NAME',
    'S3_BUCKET_NAME',
    'REQUEST_TIMEOUT',
    'RATE_LIMIT_DELAY'
])

# Get date parameters (required for API pagination)
# Try to get them - they should be provided by Step Function
try:
    date_args = getResolvedOptions(sys.argv, ['START_DATE', 'END_DATE'])
    # Convert empty strings to None
    start_date = date_args.get('START_DATE')
    end_date = date_args.get('END_DATE')
    if start_date == '':
        start_date = None
    if end_date == '':
        end_date = None
    args['START_DATE'] = start_date
    args['END_DATE'] = end_date
    print(f"✅ Successfully parsed date arguments: START_DATE={start_date}, END_DATE={end_date}", flush=True)
except Exception as e:
    print(f"⚠️ Date arguments not provided or failed to parse: {str(e)[:200]}", flush=True)
    print(f"⚠️ LDA API requires at least one filter parameter for pagination. START_DATE and/or END_DATE are required.", flush=True)
    # Set to None - will be handled in main() to require dates
    args['START_DATE'] = None
    args['END_DATE'] = None

# Get optional testing parameter (passed as string from Step Functions)
# Check sys.argv directly since TESTING is optional and getResolvedOptions requires all args
testing = False
testing_value = None

# Check if --TESTING is in sys.argv
for i, arg in enumerate(sys.argv):
    if arg == '--TESTING' and i + 1 < len(sys.argv):
        testing_value = sys.argv[i + 1]
        break

if testing_value is not None:
    print(f"🔍 DEBUG: Found TESTING in args: '{testing_value}' (type: {type(testing_value).__name__})", flush=True)
    # Glue job arguments are always strings, parse as string
    if isinstance(testing_value, str):
        testing_str = testing_value.strip().lower()
        testing = testing_str in ['true', '1', 'yes', 't']
    elif isinstance(testing_value, bool):
        testing = testing_value
    else:
        # Convert to string first, then parse
        testing_str = str(testing_value).strip().lower()
        testing = testing_str in ['true', '1', 'yes', 't']
    print(f"🔍 DEBUG: Parsed TESTING value: '{testing_value}' -> {testing}", flush=True)
else:
    print(f"ℹ️ Testing parameter not provided (optional), defaulting to False", flush=True)

args['TESTING'] = testing
if testing:
    print(f"🧪 Testing mode enabled - will limit to 10 records per type", flush=True)
else:
    print(f"ℹ️ Testing mode disabled", flush=True)

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
CONTRIBUTIONS_TABLE_NAME = args.get('CONTRIBUTIONS_TABLE_NAME')
S3_BUCKET_NAME = args.get('S3_BUCKET_NAME')
REQUEST_TIMEOUT = int(args.get('REQUEST_TIMEOUT', '30'))
RATE_LIMIT_DELAY = float(args.get('RATE_LIMIT_DELAY', '0.5'))

# AWS clients
dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')
secrets_client = boto3.client('secretsmanager')

# DynamoDB tables
filings_table = dynamodb.Table(FILINGS_TABLE_NAME)
contributions_table = dynamodb.Table(CONTRIBUTIONS_TABLE_NAME)

log_print(f"ℹ️ Configuration: Filings Table={FILINGS_TABLE_NAME}, Contributions Table={CONTRIBUTIONS_TABLE_NAME}, S3 Bucket={S3_BUCKET_NAME}")

# ============================================================================
# Helper Functions
# ============================================================================

def get_api_key() -> str:
    """Retrieve LDA API key from Secrets Manager"""
    try:
        response = secrets_client.get_secret_value(SecretId=LDA_SECRET_NAME)
        secret_data = json.loads(response['SecretString'])
        return secret_data.get('api_key') or secret_data.get('API_KEY') or secret_data.get('lda_api_key')
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
    """Call LDA API endpoint with rate limiting"""
    url = f"{LDA_API_BASE_URL}{endpoint}"
    
    time.sleep(RATE_LIMIT_DELAY)  # Rate limiting
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
        time.sleep(RATE_LIMIT_DELAY)
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
    
    # Lobbyists (get first lobbyist for GSI - can expand later)
    lobbying_activities = filing.get('lobbying_activities', [])
    if lobbying_activities:
        for activity in lobbying_activities:
            lobbyists = activity.get('lobbyists', [])
            if lobbyists:
                lobbyist = lobbyists[0].get('lobbyist', {})
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
                    break  # Use first lobbyist for GSI
    
    # Report type
    indexed['report_type'] = filing.get('filing_type')
    indexed['report_type_display'] = filing.get('filing_type_display')
    
    # Filing period and year
    indexed['filing_period'] = filing.get('filing_period')
    indexed['filing_period_display'] = filing.get('filing_period_display')
    indexed['filing_year'] = filing.get('filing_year')
    
    # Posted date
    indexed['dt_posted'] = filing.get('dt_posted')
    
    # Amount fields
    income = filing.get('income')
    if income is not None:
        try:
            indexed['amount_reported'] = Decimal(str(income))
        except (ValueError, TypeError):
            pass
    
    indexed['expenses'] = filing.get('expenses')
    indexed['expenses_method'] = filing.get('expenses_method')
    
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
    indexed['filing_year'] = contribution.get('filing_year')
    
    # Posted date
    indexed['dt_posted'] = contribution.get('dt_posted')
    
    # Filer type
    indexed['filer_type'] = contribution.get('filer_type')
    
    return indexed

def save_filing_to_dynamodb(filing: Dict, indexed_fields: Dict):
    """Save filing to DynamoDB with all fields and indexed GSI fields"""
    try:
        # Prepare item with all filing data
        item = json.loads(json.dumps(filing), parse_float=Decimal)  # Convert floats to Decimal
        
        # Add indexed fields for GSIs
        item.update(indexed_fields)
        
        # Set primary key
        item['PK'] = f"FILING#{item['filing_uuid']}"
        item['SK'] = f"FILING#{item['filing_uuid']}"
        
        # Set GSI keys based on indexed fields
        if indexed_fields.get('filing_year'):
            item['GSI1PK'] = f"YEAR#{indexed_fields['filing_year']}"
            item['GSI1SK'] = indexed_fields.get('dt_posted', '')
        
        if indexed_fields.get('filing_period'):
            item['GSI2PK'] = f"PERIOD#{indexed_fields['filing_period']}"
            item['GSI2SK'] = indexed_fields.get('dt_posted', '')
        
        if indexed_fields.get('report_type'):
            item['GSI3PK'] = f"TYPE#{indexed_fields['report_type']}"
            item['GSI3SK'] = indexed_fields.get('dt_posted', '')
        
        if indexed_fields.get('registrant_name'):
            item['GSI4PK'] = f"REGISTRANT#{indexed_fields['registrant_name']}"
            item['GSI4SK'] = indexed_fields.get('dt_posted', '')
        
        if indexed_fields.get('client_name'):
            item['GSI5PK'] = f"CLIENT#{indexed_fields['client_name']}"
            item['GSI5SK'] = indexed_fields.get('dt_posted', '')
        
        if indexed_fields.get('lobbyist_name'):
            item['GSI6PK'] = f"LOBBYIST#{indexed_fields['lobbyist_name']}"
            item['GSI6SK'] = indexed_fields.get('dt_posted', '')
        
        if indexed_fields.get('amount_reported'):
            # For numeric range queries, use a partition key format
            amount = indexed_fields['amount_reported']
            # Round to nearest 10k for partition key
            amount_bucket = int(float(amount) / 10000) * 10000
            item['GSI7PK'] = f"AMOUNT#{amount_bucket}"
            # Keep as Decimal (Number type) for DynamoDB - don't convert to string
            item['GSI7SK'] = Decimal(str(amount)) if not isinstance(amount, Decimal) else amount
        
        # Save to DynamoDB
        filings_table.put_item(Item=item)
        
    except Exception as e:
        log_print(f"❌ Error saving filing to DynamoDB: {str(e)[:200]}")
        raise

def save_contribution_to_dynamodb(contribution: Dict, indexed_fields: Dict):
    """Save contribution to DynamoDB with all fields and indexed GSI fields"""
    try:
        # Prepare item with all contribution data
        item = json.loads(json.dumps(contribution), parse_float=Decimal)
        
        # Add indexed fields for GSIs
        item.update(indexed_fields)
        
        # Set primary key
        item['PK'] = f"CONTRIBUTION#{item['filing_uuid']}"
        item['SK'] = f"CONTRIBUTION#{item['filing_uuid']}"
        
        # Set GSI keys (shared with filings)
        if indexed_fields.get('filing_year'):
            item['GSI1PK'] = f"YEAR#{indexed_fields['filing_year']}"
            item['GSI1SK'] = indexed_fields.get('dt_posted', '')
        
        if indexed_fields.get('filing_period'):
            item['GSI2PK'] = f"PERIOD#{indexed_fields['filing_period']}"
            item['GSI2SK'] = indexed_fields.get('dt_posted', '')
        
        if indexed_fields.get('report_type'):
            item['GSI3PK'] = f"TYPE#{indexed_fields['report_type']}"
            item['GSI3SK'] = indexed_fields.get('dt_posted', '')
        
        if indexed_fields.get('registrant_name'):
            item['GSI4PK'] = f"REGISTRANT#{indexed_fields['registrant_name']}"
            item['GSI4SK'] = indexed_fields.get('dt_posted', '')
        
        if indexed_fields.get('lobbyist_name'):
            item['GSI6PK'] = f"LOBBYIST#{indexed_fields['lobbyist_name']}"
            item['GSI6SK'] = indexed_fields.get('dt_posted', '')
        
        # Save to DynamoDB
        contributions_table.put_item(Item=item)
        
    except Exception as e:
        log_print(f"❌ Error saving contribution to DynamoDB: {str(e)[:200]}")
        raise

def process_all_filings(session: requests.Session, start_date: Optional[str] = None, end_date: Optional[str] = None, testing: bool = False):
    """Fetch and process all filings with pagination"""
    log_print("\n" + "="*80)
    log_print("📋 Processing Filings")
    log_print("="*80)
    
    if testing:
        log_print("🧪 TESTING MODE: Limiting to 10 filings")
    
    # LDA API requires at least one query parameter for pagination
    if not start_date and not end_date:
        raise ValueError("START_DATE and/or END_DATE must be provided. LDA API requires at least one filter parameter for pagination.")
    
    params = {'page_size': 100}  # Max page size
    if start_date:
        params['filing_dt_posted_after'] = start_date
    if end_date:
        params['filing_dt_posted_before'] = end_date
    
    page = 1
    total_processed = 0
    max_records = 10 if testing else None
    
    while True:
        params['page'] = page
        log_print(f"\n📄 Fetching filings page {page}...")
        
        try:
            response = call_api(session, '/filings/', params=params)
            results = response.get('results', [])
            count = response.get('count', 0)
            
            if not results:
                log_print(f"✅ No more filings to process. Total processed: {total_processed}")
                break
            
            log_print(f"   Found {len(results)} filings on page {page} (total: {count})")
            
            for filing in results:
                filing_uuid = filing.get('filing_uuid')
                if not filing_uuid:
                    continue
                
                try:
                    # Fetch full details
                    full_filing = call_api(session, f'/filings/{filing_uuid}/')
                    
                    # Extract indexed fields
                    indexed_fields = extract_indexed_fields_filing(full_filing)
                    
                    # Save to DynamoDB
                    save_filing_to_dynamodb(full_filing, indexed_fields)
                    
                    # Download document if available
                    doc_url = full_filing.get('filing_document_url')
                    if doc_url:
                        filing_type = full_filing.get('filing_type', 'unknown')
                        content_type = full_filing.get('filing_document_content_type', 'pdf')
                        ext = 'pdf' if 'pdf' in content_type.lower() else 'html'
                        s3_key = f"filings/{filing_type}/{filing_uuid}.{ext}"
                        download_document(session, doc_url, s3_key)
                        log_print(f"   ✅ Downloaded document for {filing_uuid}")
                    
                    total_processed += 1
                    if total_processed % 100 == 0:
                        log_print(f"   📊 Processed {total_processed} filings...")
                    
                    # Stop if testing mode and reached limit
                    if testing and total_processed >= max_records:
                        log_print(f"🧪 Testing mode: Reached limit of {max_records} filings. Stopping.")
                        break
                    
                except Exception as e:
                    log_print(f"   ❌ Error processing filing {filing_uuid}: {str(e)[:200]}")
                    continue
            
            # Stop if testing mode and reached limit
            if testing and total_processed >= max_records:
                log_print(f"✅ Finished processing filings (testing mode limit: {total_processed})")
                break
            
            # Check if there's a next page
            if response.get('next'):
                page += 1
            else:
                log_print(f"✅ Finished processing all filings. Total: {total_processed}")
                break
                
        except Exception as e:
            log_print(f"❌ Error fetching filings page {page}: {str(e)[:200]}")
            break

def process_all_contributions(session: requests.Session, start_date: Optional[str] = None, end_date: Optional[str] = None, testing: bool = False):
    """Fetch and process all contributions with pagination"""
    log_print("\n" + "="*80)
    log_print("📋 Processing Contributions")
    log_print("="*80)
    
    if testing:
        log_print("🧪 TESTING MODE: Limiting to 10 contributions")
    
    # LDA API requires at least one query parameter for pagination
    if not start_date and not end_date:
        raise ValueError("START_DATE and/or END_DATE must be provided. LDA API requires at least one filter parameter for pagination.")
    
    params = {'page_size': 100}
    if start_date:
        params['filing_dt_posted_after'] = start_date
    if end_date:
        params['filing_dt_posted_before'] = end_date
    
    page = 1
    total_processed = 0
    max_records = 10 if testing else None
    
    while True:
        params['page'] = page
        log_print(f"\n📄 Fetching contributions page {page}...")
        
        try:
            response = call_api(session, '/contributions/', params=params)
            results = response.get('results', [])
            count = response.get('count', 0)
            
            if not results:
                log_print(f"✅ No more contributions to process. Total processed: {total_processed}")
                break
            
            log_print(f"   Found {len(results)} contributions on page {page} (total: {count})")
            
            for contribution in results:
                filing_uuid = contribution.get('filing_uuid')
                if not filing_uuid:
                    continue
                
                try:
                    # Fetch full details
                    full_contribution = call_api(session, f'/contributions/{filing_uuid}/')
                    
                    # Extract indexed fields
                    indexed_fields = extract_indexed_fields_contribution(full_contribution)
                    
                    # Save to DynamoDB
                    save_contribution_to_dynamodb(full_contribution, indexed_fields)
                    
                    # Download document if available
                    doc_url = full_contribution.get('filing_document_url')
                    if doc_url:
                        filing_type = full_contribution.get('filing_type', 'unknown')
                        content_type = full_contribution.get('filing_document_content_type', 'pdf')
                        ext = 'pdf' if 'pdf' in content_type.lower() else 'html'
                        s3_key = f"contributions/{filing_type}/{filing_uuid}.{ext}"
                        download_document(session, doc_url, s3_key)
                        log_print(f"   ✅ Downloaded document for {filing_uuid}")
                    
                    total_processed += 1
                    if total_processed % 100 == 0:
                        log_print(f"   📊 Processed {total_processed} contributions...")
                    
                    # Stop if testing mode and reached limit
                    if testing and total_processed >= max_records:
                        log_print(f"🧪 Testing mode: Reached limit of {max_records} contributions. Stopping.")
                        break
                    
                except Exception as e:
                    log_print(f"   ❌ Error processing contribution {filing_uuid}: {str(e)[:200]}")
                    continue
            
            # Stop if testing mode and reached limit
            if testing and total_processed >= max_records:
                log_print(f"✅ Finished processing contributions (testing mode limit: {total_processed})")
                break
            
            # Check if there's a next page
            if response.get('next'):
                page += 1
            else:
                log_print(f"✅ Finished processing all contributions. Total: {total_processed}")
                break
                
        except Exception as e:
            log_print(f"❌ Error fetching contributions page {page}: {str(e)[:200]}")
            break

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
    
    # Get date range (required - LDA API requires at least one filter parameter for pagination)
    start_date = args.get('START_DATE')
    end_date = args.get('END_DATE')
    
    # Validate that at least one date is provided
    if not start_date and not end_date:
        error_msg = "ERROR: START_DATE and/or END_DATE must be provided. LDA API requires at least one query parameter for pagination."
        log_print(f"❌ {error_msg}")
        raise ValueError(error_msg)
    
    if start_date and end_date:
        log_print(f"📅 Date range: {start_date} to {end_date}")
    elif start_date:
        log_print(f"📅 Start date: {start_date} (no end date - will fetch all records from start date)")
    elif end_date:
        log_print(f"📅 End date: {end_date} (no start date - will fetch all records up to end date)")
    
    # Get testing mode
    testing = args.get('TESTING', False)
    
    # Process filings
    process_all_filings(session, start_date, end_date, testing=testing)
    
    # Process contributions
    process_all_contributions(session, start_date, end_date, testing=testing)
    
    log_print("\n" + "="*80)
    log_print("✅ LDA Disclosures Indexing Job Completed Successfully")
    log_print("="*80)
    
    job.commit()

if __name__ == "__main__":
    main()

