"""
LDA Disclosures Fetcher Lambda
Determines total pages for filings and contributions endpoints and creates batches for processing.
"""

import json
import os
import requests
import boto3
from botocore.exceptions import ClientError
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

# Environment variables
LDA_API_BASE_URL = os.environ.get('LDA_API_BASE_URL', 'https://lda.senate.gov/api/v1')
LDA_SECRET_NAME = os.environ.get('LDA_SECRET_NAME')
REQUEST_TIMEOUT = int(os.environ.get('REQUEST_TIMEOUT', '30'))
BATCH_QUEUE_URL = os.environ.get('BATCH_QUEUE_URL')  # SQS standard queue for batches

# AWS clients
secrets_client = boto3.client('secretsmanager')

# Configure SQS client with larger connection pool to avoid warnings
# Default pool size is 10, increase to 25 to match our parallelism
from botocore.config import Config
sqs_config = Config(
    max_pool_connections=25  # Match our max_workers to avoid connection pool warnings
)
sqs_client = boto3.client('sqs', config=sqs_config)

def get_api_key() -> str:
    """Retrieve LDA API key from Secrets Manager"""
    try:
        response = secrets_client.get_secret_value(SecretId=LDA_SECRET_NAME)
        secret_data = json.loads(response['SecretString'])
        return secret_data.get('api_key') or secret_data.get('API_KEY') or secret_data.get('LDA_API_KEY') or secret_data.get('lda_api_key')
    except ClientError as e:
        print(f"❌ Error retrieving API key from Secrets Manager: {str(e)}")
        raise

def create_session(api_key: str) -> requests.Session:
    """Create a requests session with Authorization header"""
    session = requests.Session()
    session.headers.update({
        'Authorization': f'Token {api_key}',
        'Accept': 'application/json',
    })
    return session

def fetch_page_count(session: requests.Session, endpoint: str, start_date: Optional[str], end_date: Optional[str]) -> Dict:
    """Fetch first page to get total count"""
    base_params = {'page_size': 25}
    
    if not start_date and not end_date:
        base_params['filing_dt_posted_after'] = '2000-01-01'
    else:
        if start_date:
            base_params['filing_dt_posted_after'] = start_date
        if end_date:
            base_params['filing_dt_posted_before'] = end_date
    
    api_url = f"{LDA_API_BASE_URL}/{endpoint}/"
    
    response = session.get(api_url, params={**base_params, 'page': 1}, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    data = response.json()
    
    return data

def create_batches(total_pages: int, batch_size: int = 25) -> List[Dict]:
    """Create batches of pages"""
    batches = []
    for batch_id in range(1, (total_pages + batch_size - 1) // batch_size + 1):
        start_page = (batch_id - 1) * batch_size + 1
        end_page = min(batch_id * batch_size, total_pages)
        pages = list(range(start_page, end_page + 1))
        
        batches.append({
            'batch_id': batch_id,
            'pages': pages,
            'start_page': start_page,
            'end_page': end_page,
            'page_count': len(pages)
        })
    
    return batches

def send_page_to_queue(page: int, endpoint: str, start_date: Optional[str], end_date: Optional[str], testing_limit: Optional[int]):
    """Send a single page to SQS standard queue"""
    if not BATCH_QUEUE_URL:
        print(f"⚠️  BATCH_QUEUE_URL not configured, skipping SQS send")
        return False
    
    message_body = {
        'page': page,
        'endpoint': endpoint,
        'start_date': start_date,
        'end_date': end_date,
        'testing_limit': testing_limit
    }
    
    # Standard queue - no MessageGroupId or MessageDeduplicationId needed
    # Standard queues allow full concurrency up to the Lambda's reserved_concurrent_executions limit (25)
    
    try:
        sqs_client.send_message(
            QueueUrl=BATCH_QUEUE_URL,
            MessageBody=json.dumps(message_body)
        )
        return True
    except Exception as e:
        print(f"   ❌ Error sending page {page} to queue: {str(e)}")
        return False

def send_pages_parallel(pages: List[int], endpoint: str, start_date: Optional[str], end_date: Optional[str], testing_limit: Optional[int], max_workers: int = 10):
    """Send multiple pages to SQS queue in parallel (reduced to 10 to avoid connection pool issues)"""
    if not pages:
        return 0
    
    successful = 0
    failed = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all send tasks
        future_to_page = {
            executor.submit(send_page_to_queue, page, endpoint, start_date, end_date, testing_limit): page
            for page in pages
        }
        
        # Process completed tasks
        for future in as_completed(future_to_page):
            page = future_to_page[future]
            try:
                if future.result():
                    successful += 1
                else:
                    failed += 1
            except Exception as e:
                print(f"   ❌ Exception sending page {page}: {str(e)}")
                failed += 1
    
    return successful

def lambda_handler(event, context):
    """
    Fetcher Lambda: Determines total pages and outputs batches for processing
    
    Input:
    {
        "START_DATE": "2025-01-01" (optional),
        "END_DATE": "2025-01-31" (optional),
        "TESTING": 10 (optional, number of records per endpoint)
    }
    
    Output:
    {
        "filingbatches": {
            "batches": [...],
            "total_pages": 200,
            "total_count": 5000,
            "endpoint": "filings",
            "start_date": "2025-01-01",
            "end_date": "2025-01-31"
        },
        "contributionbatches": {
            "batches": [...],
            "total_pages": 150,
            "total_count": 3750,
            "endpoint": "contributions",
            "start_date": "2025-01-01",
            "end_date": "2025-01-31"
        }
    }
    """
    print("=" * 80)
    print("🚀 LDA Disclosures Fetcher Lambda - Starting")
    print("=" * 80)
    
    # Get input parameters
    start_date = event.get('START_DATE')
    end_date = event.get('END_DATE')
    testing_limit = event.get('TESTING')  # Optional: number of records per endpoint
    
    print(f"📅 Date range: START_DATE={start_date}, END_DATE={end_date}")
    if testing_limit:
        print(f"🧪 Testing mode: {testing_limit} records per endpoint")
    
    # Get API key
    api_key = get_api_key()
    print("✅ Retrieved API key from Secrets Manager")
    
    # Create session
    session = create_session(api_key)
    
    total_pages_sent = 0
    
    # Process filings endpoint
    print("\n📋 Fetching filings endpoint count...")
    try:
        filings_data = fetch_page_count(session, 'filings', start_date, end_date)
        filings_count = filings_data.get('count', 0)
        filings_total_pages = (filings_count + 24) // 25 if filings_count > 0 else 1
        
        # Apply testing limit if provided
        if testing_limit:
            pages_needed = (testing_limit + 24) // 25
            filings_total_pages = min(filings_total_pages, pages_needed)
            filings_count = min(filings_count, testing_limit)
        
        print(f"✅ Filings: {filings_count} total records, {filings_total_pages} pages")
        print(f"📤 Sending {filings_total_pages} pages to SQS queue in parallel...")
        
        # Send all pages to SQS in parallel
        filings_pages = list(range(1, filings_total_pages + 1))
        successful = send_pages_parallel(filings_pages, 'filings', start_date, end_date, testing_limit)
        total_pages_sent += successful
        
        print(f"✅ Sent {successful}/{filings_total_pages} filings pages to queue")
    except Exception as e:
        print(f"❌ Error fetching filings count: {str(e)}")
    
    # Process contributions endpoint
    print("\n📋 Fetching contributions endpoint count...")
    try:
        contributions_data = fetch_page_count(session, 'contributions', start_date, end_date)
        contributions_count = contributions_data.get('count', 0)
        contributions_total_pages = (contributions_count + 24) // 25 if contributions_count > 0 else 1
        
        # Apply testing limit if provided
        if testing_limit:
            pages_needed = (testing_limit + 24) // 25
            contributions_total_pages = min(contributions_total_pages, pages_needed)
            contributions_count = min(contributions_count, testing_limit)
        
        print(f"✅ Contributions: {contributions_count} total records, {contributions_total_pages} pages")
        print(f"📤 Sending {contributions_total_pages} pages to SQS queue in parallel...")
        
        # Send all pages to SQS in parallel
        contributions_pages = list(range(1, contributions_total_pages + 1))
        successful = send_pages_parallel(contributions_pages, 'contributions', start_date, end_date, testing_limit)
        total_pages_sent += successful
        
        print(f"✅ Sent {successful}/{contributions_total_pages} contributions pages to queue")
    except Exception as e:
        print(f"❌ Error fetching contributions count: {str(e)}")
    
    print("\n" + "=" * 80)
    print(f"✅ Fetcher Lambda Complete - Sent {total_pages_sent} pages to queue")
    print("=" * 80)
    
    return {
        'statusCode': 200,
        'total_pages_sent': total_pages_sent,
        'start_date': start_date,
        'end_date': end_date,
        'testing_limit': testing_limit
    }

