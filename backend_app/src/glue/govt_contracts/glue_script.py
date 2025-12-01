"""
AWS Glue Job: USAspending Daily Bulk Indexing
Fetches all new contract awards from the previous day, downloads bulk data,
and indexes awards, transactions, and subawards to DynamoDB and S3.

This job runs daily via Step Functions to:
1. Get yesterday's date range
2. Initiate bulk download for all contracts from that day
3. Download and parse the CSV file
4. For each award, fetch full details, transactions, and subawards
5. Store award metadata in DynamoDB and transaction/subaward details in S3
"""

import sys
import json
import logging
import time
import csv
import gzip
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional
from decimal import Decimal
from io import StringIO, BytesIO

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

import boto3
import requests

# ============================================================================
# Configuration
# ============================================================================

# Get job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'USASPENDING_BASE_URL',
    'USASPENDING_USER_AGENT',
    'AWARDS_TABLE_NAME',
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

# Environment variables
USASPENDING_BASE_URL = args.get('USASPENDING_BASE_URL', 'https://api.usaspending.gov')
USASPENDING_USER_AGENT = args.get('USASPENDING_USER_AGENT', 'Cosine Financial Platform (contact@cosine.financial)')
AWARDS_TABLE_NAME = args.get('AWARDS_TABLE_NAME', 'usaspending-awards-index')
S3_BUCKET_NAME = args.get('S3_BUCKET_NAME', 'cosine-usaspending-data-production')
REQUEST_TIMEOUT = int(args.get('REQUEST_TIMEOUT', '30'))

# AWS clients
dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')
awards_table = dynamodb.Table(AWARDS_TABLE_NAME)

# ============================================================================
# Helper Functions (ported from Lambda)
# ============================================================================

def create_session():
    """Create a requests session with proper headers"""
    session = requests.Session()
    session.headers.update({
        'User-Agent': USASPENDING_USER_AGENT,
        'Accept': 'application/json',
        'Content-Type': 'application/json',
    })
    return session


def call_usaspending_api(endpoint: str, method: str = 'GET', body: Optional[Dict] = None, params: Optional[Dict] = None) -> Dict[str, Any]:
    """Call USAspending API endpoint - failures will stop execution"""
    url = f"{USASPENDING_BASE_URL}{endpoint}"
    session = create_session()
    
    if method.upper() == 'POST':
        response = session.post(url, json=body, timeout=REQUEST_TIMEOUT)
    else:
        response = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
    
    response.raise_for_status()
    return response.json()


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
    if isinstance(obj, float):
        return Decimal(str(obj))
    elif isinstance(obj, dict):
        return {key: convert_floats_to_decimal(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_floats_to_decimal(item) for item in obj]
    else:
        return obj


def flatten_award_data(award_data: Dict[str, Any]) -> Dict[str, Any]:
    """Flatten award data for DynamoDB storage - ported from Lambda"""
    award_id = award_data.get('generated_unique_award_id') or award_data.get('id')
    if not award_id:
        raise ValueError("Award ID not found in award data")
    
    awarding_agency = award_data.get('awarding_agency', {})
    funding_agency = award_data.get('funding_agency', {})
    recipient = award_data.get('recipient', {})
    recipient_location = recipient.get('location', {}) if recipient else {}
    period_of_performance = award_data.get('period_of_performance', {})
    period_start_date = period_of_performance.get('start_date') if period_of_performance else None
    period_end_date = period_of_performance.get('end_date') if period_of_performance else None
    
    naics_hierarchy = award_data.get('naics_hierarchy', {})
    naics_code = None
    if naics_hierarchy:
        base_code = naics_hierarchy.get('base_code', {})
        naics_code = base_code.get('code') if base_code else None
    
    psc_hierarchy = award_data.get('psc_hierarchy', {})
    psc_code = None
    if psc_hierarchy:
        base_code = psc_hierarchy.get('base_code', {})
        psc_code = base_code.get('code') if base_code else None
    
    cfda_number = None
    if award_data.get('category') == 'financial_assistance':
        cfda_info = award_data.get('cfda_info', [])
        if cfda_info and len(cfda_info) > 0:
            cfda_number = cfda_info[0].get('number')
    
    def_codes = []
    if 'account_obligations_by_defc' in award_data:
        account_obligations = award_data['account_obligations_by_defc']
        if isinstance(account_obligations, dict):
            def_codes = list(account_obligations.keys())
        elif isinstance(account_obligations, list):
            def_codes = [item.get('code') for item in account_obligations if isinstance(item, dict) and item.get('code')]
    
    category = award_data.get('category', 'contract')
    award_type = category
    fiscal_year = extract_fiscal_year(period_start_date)
    
    recipient_name = recipient.get('recipient_name') if recipient else None
    recipient_name_normalized = recipient_name.lower().strip() if recipient_name else None
    
    flattened = {
        'award_id': award_id,
        'award_type': award_type,
        'total_obligation': Decimal(str(award_data.get('total_obligation', 0))),
        'period_start_date': period_start_date,
        'period_end_date': period_end_date,
        'fiscal_year': fiscal_year,
        'description': award_data.get('description', ''),
        'awarding_agency_id': awarding_agency.get('id') if awarding_agency else None,
        'awarding_agency_name': awarding_agency.get('toptier_agency', {}).get('name') if awarding_agency else None,
        'funding_agency_id': funding_agency.get('id') if funding_agency else None,
        'funding_agency_name': funding_agency.get('toptier_agency', {}).get('name') if funding_agency else None,
        'recipient_name': recipient_name,
        'recipient_name_normalized': recipient_name_normalized,
        'recipient_unique_id': recipient.get('recipient_unique_id') if recipient else None,
        'recipient_location_country': recipient_location.get('country_code') if recipient_location else None,
        'naics_description': naics_hierarchy.get('base_code', {}).get('description') if naics_hierarchy else None,
        'psc_description': psc_hierarchy.get('base_code', {}).get('description') if psc_hierarchy else None,
        'def_codes': def_codes,
        'full_response': convert_floats_to_decimal(award_data),
        'indexed_at': datetime.now(timezone.utc).isoformat(),
        'last_updated': datetime.now(timezone.utc).isoformat(),
        'data_source': 'usaspending_api',
        'api_version': 'v2',
        'award_details_indexed': False,
        'full_indexing_complete': False,
        'ttl': int((datetime.now(timezone.utc).timestamp() + (90 * 24 * 60 * 60)))
    }
    
    # Conditionally add GSI attributes (sparse GSIs)
    if awarding_agency and awarding_agency.get('toptier_agency', {}).get('toptier_code'):
        flattened['awarding_agency_code'] = awarding_agency.get('toptier_agency', {}).get('toptier_code')
    if funding_agency and funding_agency.get('toptier_agency', {}).get('toptier_code'):
        flattened['funding_agency_code'] = funding_agency.get('toptier_agency', {}).get('toptier_code')
    if recipient and recipient.get('recipient_id'):
        flattened['recipient_id'] = recipient.get('recipient_id')
    if recipient_location and recipient_location.get('state_code'):
        flattened['recipient_location_state'] = recipient_location.get('state_code')
    if naics_code:
        flattened['naics_code'] = naics_code
    if psc_code:
        flattened['psc_code'] = psc_code
    if cfda_number is not None:
        flattened['cfda_number'] = cfda_number
    
    return convert_floats_to_decimal(flattened)


def fetch_all_transactions(award_id: str) -> List[Dict[str, Any]]:
    """Fetch all transactions for an award (paginated)"""
    all_transactions = []
    page = 1
    limit = 100
    
    while True:
        response = call_usaspending_api(
            '/api/v2/transactions/',
            method='POST',
            body={
                'award_id': award_id,
                'page': page,
                'limit': limit,
                'sort': 'action_date',
                'order': 'desc'
            }
        )
        
        if not response:
            break
        
        transactions = response.get('results', [])
        if not transactions:
            break
        
        all_transactions.extend(transactions)
        
        page_metadata = response.get('page_metadata', {})
        if not page_metadata.get('hasNext', False):
            break
        
        page += 1
    
    return all_transactions


def fetch_all_subawards(award_id: str) -> List[Dict[str, Any]]:
    """Fetch all subawards for an award (paginated)"""
    all_subawards = []
    page = 1
    limit = 100
    
    while True:
        response = call_usaspending_api(
            '/api/v2/subawards/',
            method='POST',
            body={
                'award_id': award_id,
                'page': page,
                'limit': limit,
                'sort': 'amount',
                'order': 'desc'
            }
        )
        
        if not response:
            break
        
        subawards = response.get('results', [])
        if not subawards:
            break
        
        all_subawards.extend(subawards)
        
        page_metadata = response.get('page_metadata', {})
        if not page_metadata.get('hasNext', False):
            break
        
        page += 1
    
    return all_subawards


def upload_award_details_to_s3(award_id: str, transactions: List[Dict], subawards: List[Dict]) -> str:
    """Upload combined transactions and subawards to S3"""
    combined_data = {
        'transactions': transactions,
        'subawards': subawards,
        'indexed_at': datetime.now(timezone.utc).isoformat(),
        'transaction_count': len(transactions),
        'subaward_count': len(subawards)
    }
    combined_json = json.dumps(combined_data, default=str)
    combined_gzipped = gzip.compress(combined_json.encode('utf-8'))
    s3_key = f"award-details/{award_id}/details.json.gz"
    
    s3_client.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=s3_key,
        Body=combined_gzipped,
        ContentType='application/json',
        ContentEncoding='gzip',
        ServerSideEncryption='aws:kms'
    )
    
    return s3_key


def index_award(award_id: str) -> Dict[str, Any]:
    """Index a single award: fetch details, transactions, subawards, and store"""
    try:
        # Check if already indexed
        existing_award = awards_table.get_item(Key={'award_id': award_id})
        item = existing_award.get('Item')
        if item and (item.get('full_indexing_complete') or item.get('award_details_s3_key')):
            logger.info(f"Award {award_id} already indexed, skipping")
            return {'success': True, 'award_id': award_id, 'skipped': True}
        
        # Fetch award details
        award_data = call_usaspending_api(f'/api/v2/awards/{award_id}/', method='GET')
        if not award_data:
            raise Exception(f'Award not found: {award_id}')
        
        # Flatten award data
        flattened_award = flatten_award_data(award_data)
        
        # Store award in DynamoDB
        awards_table.put_item(Item=flattened_award)
        
        # Fetch transactions and subawards
        transactions = fetch_all_transactions(award_id)
        subawards = fetch_all_subawards(award_id)
        
        # Upload to S3
        s3_key = upload_award_details_to_s3(award_id, transactions, subawards)
        
        # Update DynamoDB with S3 key and completion flags
        existing_item = flattened_award.copy()
        existing_item['award_details_s3_key'] = s3_key
        existing_item['award_details_indexed'] = True
        existing_item['transaction_count'] = len(transactions)
        existing_item['subaward_count'] = len(subawards)
        existing_item['full_indexing_complete'] = True
        existing_item['last_updated'] = datetime.now(timezone.utc).isoformat()
        awards_table.put_item(Item=existing_item)
        
        logger.info(f"✅ Indexed award {award_id}: {len(transactions)} transactions, {len(subawards)} subawards")
        return {'success': True, 'award_id': award_id, 'transaction_count': len(transactions), 'subaward_count': len(subawards)}
    
    except Exception as e:
        logger.error(f"❌ Error indexing award {award_id}: {str(e)}", exc_info=True)
        raise  # Re-raise to stop execution


# ============================================================================
# Bulk Download Functions
# ============================================================================

def initiate_bulk_download(start_date: str, end_date: str) -> Dict[str, Any]:
    """Initiate bulk download for all contracts in date range"""
    logger.info(f"Initiating bulk download for date range: {start_date} to {end_date}")
    
    bulk_filters = {
        "date_range": {
            "start_date": start_date,
            "end_date": end_date
        },
        "date_type": "action_date",
        "prime_award_types": ["A", "B", "C", "D"]  # Contract types
    }
    
    response = call_usaspending_api(
        "/api/v2/bulk_download/awards/",
        method='POST',
        body={
            "filters": bulk_filters,
            "file_format": "csv"
        }
    )
    
    file_name = response.get("file_name")
    if not file_name:
        raise Exception("No file_name in bulk download response")
    
    logger.info(f"Bulk download initiated: {file_name}")
    return {'file_name': file_name, 'response': response}


def poll_download_status(file_name: str, max_wait: int = 3600, poll_interval: int = 10) -> Dict[str, Any]:
    """Poll bulk download status until ready"""
    logger.info(f"Polling download status for {file_name}")
    start_time = time.time()
    
    while time.time() - start_time < max_wait:
        response = call_usaspending_api(
            "/api/v2/bulk_download/status/",
            method='POST',
            body={"file_name": file_name}
        )
        
        status = response.get("status")
        logger.info(f"Download status: {status}")
        
        if status == "ready":
            return response
        elif status == "failed":
            raise Exception(f"Bulk download failed: {response.get('message', 'Unknown error')}")
        
        time.sleep(poll_interval)
    
    raise Exception(f"Bulk download timeout after {max_wait} seconds")


def download_and_parse_csv(file_url: str) -> List[Dict[str, Any]]:
    """Download CSV file and parse to extract award IDs"""
    logger.info(f"Downloading CSV from {file_url}")
    
    response = requests.get(file_url, timeout=300)
    response.raise_for_status()
    
    # Parse CSV
    csv_content = response.text
    reader = csv.DictReader(StringIO(csv_content))
    
    award_ids = []
    for row in reader:
        # Extract award ID from CSV row (field name may vary)
        award_id = row.get('generated_unique_award_id') or row.get('award_id') or row.get('Award ID')
        if award_id and award_id not in award_ids:
            award_ids.append(award_id)
    
    logger.info(f"Extracted {len(award_ids)} unique award IDs from CSV")
    return award_ids


# ============================================================================
# Main Job Logic
# ============================================================================

def main():
    """Main Glue job execution"""
    try:
        # Get yesterday's date range
        yesterday = datetime.now(timezone.utc) - timedelta(days=1)
        start_date = yesterday.strftime('%Y-%m-%d')
        end_date = yesterday.strftime('%Y-%m-%d')
        
        logger.info(f"Starting daily bulk indexing for {start_date}")
        
        # Step 1: Initiate bulk download
        download_info = initiate_bulk_download(start_date, end_date)
        file_name = download_info['file_name']
        
        # Step 2: Poll for download completion
        status_info = poll_download_status(file_name, max_wait=3600, poll_interval=30)
        file_url = status_info.get('file_url')
        
        if not file_url:
            raise Exception("No file_url in download status response")
        
        # Step 3: Download and parse CSV
        award_ids = download_and_parse_csv(file_url)
        
        if not award_ids:
            logger.warning(f"No award IDs found in bulk download for {start_date}")
            return
        
        # Step 4: Index all awards
        logger.info(f"Indexing {len(award_ids)} awards...")
        indexed_count = 0
        skipped_count = 0
        
        for i, award_id in enumerate(award_ids, 1):
            try:
                result = index_award(award_id)
                if result.get('skipped'):
                    skipped_count += 1
                else:
                    indexed_count += 1
                
                if i % 100 == 0:
                    logger.info(f"Progress: {i}/{len(award_ids)} awards processed ({indexed_count} indexed, {skipped_count} skipped)")
            
            except Exception as e:
                logger.error(f"Failed to index award {award_id}: {str(e)}")
                raise  # Stop on error
        
        logger.info(f"✅ Bulk indexing complete: {indexed_count} indexed, {skipped_count} skipped out of {len(award_ids)} total")
        
        # Job success
        job.commit()
        
    except Exception as e:
        logger.error(f"❌ CRITICAL ERROR in bulk indexing job: {str(e)}", exc_info=True)
        raise  # Re-raise to mark job as failed


if __name__ == "__main__":
    main()

