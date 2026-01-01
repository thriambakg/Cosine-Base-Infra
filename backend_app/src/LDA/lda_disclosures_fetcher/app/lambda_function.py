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
from datetime import datetime, timedelta

# Environment variables
LDA_API_BASE_URL = os.environ.get('LDA_API_BASE_URL', 'https://lda.senate.gov/api/v1')
LDA_SECRET_NAME = os.environ.get('LDA_SECRET_NAME')
REQUEST_TIMEOUT = int(os.environ.get('REQUEST_TIMEOUT', '30'))
BATCH_QUEUE_URL = os.environ.get('BATCH_QUEUE_URL')  # SQS standard queue for batches

# Environment variables
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME')

# AWS clients
secrets_client = boto3.client('secretsmanager')
s3_client = boto3.client('s3') if S3_BUCKET_NAME else None

# Configure SQS client with larger connection pool for high parallelism
from botocore.config import Config
sqs_config = Config(
    max_pool_connections=150  # Increased to support 100 parallel workers with batch sends
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

def send_message_batch_to_queue(pages_batch: List[int], endpoint: str, start_date: Optional[str], end_date: Optional[str], testing_limit: Optional[int]):
    """Send a batch of pages to SQS using send_message_batch (up to 10 messages per call)"""
    if not BATCH_QUEUE_URL:
        print(f"⚠️  BATCH_QUEUE_URL not configured, skipping SQS send")
        return 0
    
    if not pages_batch:
        return 0
    
    # Prepare batch entries (SQS allows up to 10 messages per batch)
    entries = []
    for idx, page in enumerate(pages_batch):
        message_body = {
            'page': page,
            'endpoint': endpoint,
            'start_date': start_date,
            'end_date': end_date,
            'testing_limit': testing_limit
        }
        entries.append({
            'Id': str(page),  # Unique ID for this message in the batch
            'MessageBody': json.dumps(message_body)
        })
    
    try:
        response = sqs_client.send_message_batch(
            QueueUrl=BATCH_QUEUE_URL,
            Entries=entries
        )
        # Count successful and failed
        successful = len(response.get('Successful', []))
        failed = len(response.get('Failed', []))
        if failed > 0:
            print(f"   ⚠️  Batch send: {successful} successful, {failed} failed")
        return successful
    except Exception as e:
        print(f"   ❌ Error sending batch to queue: {str(e)}")
        return 0

def send_pages_parallel(pages: List[int], endpoint: str, start_date: Optional[str], end_date: Optional[str], testing_limit: Optional[int], max_workers: int = 100, batch_size: int = 10):
    """Send multiple pages to SQS queue in parallel using batch sends
    
    Uses send_message_batch to send up to 10 messages per API call, significantly reducing
    the number of API calls and improving throughput.
    
    Increased max_workers to 100 for maximum parallelism within Lambda timeout limits.
    """
    if not pages:
        return 0
    
    # Group pages into batches of 10 (SQS batch limit)
    page_batches = []
    for i in range(0, len(pages), batch_size):
        page_batches.append(pages[i:i + batch_size])
    
    successful = 0
    failed = 0
    
    # Use ThreadPoolExecutor to send batches in parallel
    # Increased to 100 workers for maximum throughput
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all batch send tasks
        future_to_batch = {
            executor.submit(send_message_batch_to_queue, batch, endpoint, start_date, end_date, testing_limit): batch
            for batch in page_batches
        }
        
        # Process completed tasks
        for future in as_completed(future_to_batch):
            batch = future_to_batch[future]
            try:
                batch_successful = future.result()
                successful += batch_successful
                failed += (len(batch) - batch_successful)
            except Exception as e:
                print(f"   ❌ Exception sending batch: {str(e)}")
                failed += len(batch)
    
    return successful

def json_to_txt_names_only(data: List[Dict], name_column: str) -> str:
    """Convert JSON data to TXT file with names only (one per line, preserves commas and special characters)"""
    if not data:
        return ''
    
    lines = []
    
    for item in data:
        name = item.get(name_column, '')
        if name and str(name).strip():
            # Preserve commas and special characters as they come from the API
            lines.append(str(name).strip())
    
    return '\n'.join(lines) + '\n'

def fetch_and_store_constants(session: requests.Session):
    """Fetch constants from LDA API, convert to TXT file (names only for autocomplete, one per line), and store in S3"""
    if not S3_BUCKET_NAME or not s3_client:
        print("⚠️  S3_BUCKET_NAME not configured, skipping constants storage")
        return
    
    # Constants to fetch and store (TXT file with names for autocomplete, one per line)
    # Note: We store names (not codes/IDs) because autocomplete needs human-readable names
    # TXT format preserves commas and special characters as they come from the API
    constants_config = {
        "general_issues": {
            "endpoint": f"{LDA_API_BASE_URL}/constants/filing/lobbyingactivityissues/",
            "name_column": "name"  # Store names for autocomplete, not codes
        },
        "government_entities": {
            "endpoint": f"{LDA_API_BASE_URL}/constants/filing/governmententities/",
            "name_column": "name"  # Store names for autocomplete, not IDs
        },
        "countries": {
            "endpoint": f"{LDA_API_BASE_URL}/constants/general/countries/",
            "name_column": "name"  # Store names for autocomplete, not codes
        }
    }
    
    print("\n📋 Fetching and storing constants as TXT files to S3...")
    
    for constant_type, config in constants_config.items():
        try:
            print(f"   📡 Fetching {constant_type}...")
            response = session.get(config["endpoint"], timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            constants = response.json()
            
            # Convert to TXT with names only (one per line) for autocomplete
            # Preserves commas and special characters as they come from the API
            txt_content = json_to_txt_names_only(
                constants,
                config["name_column"]
            )
            
            # Store TXT file in S3
            s3_key = f"lists/{constant_type}.txt"
            s3_client.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=s3_key,
                Body=txt_content.encode('utf-8'),
                ContentType='text/plain'
            )
            
            print(f"   ✅ Stored {len(constants)} {constant_type} values to s3://{S3_BUCKET_NAME}/{s3_key}")
            
        except Exception as e:
            print(f"   ⚠️  Error fetching/storing {constant_type}: {str(e)[:200]}")

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
    
    # Check if this is a scheduled invocation
    source = event.get('source', '')
    is_scheduled = source == 'Scheduler' or source == 'scheduler'
    
    # Get input parameters
    start_date = event.get('START_DATE') or event.get('start_date')
    end_date = event.get('END_DATE') or event.get('end_date')
    testing_limit = event.get('TESTING') or event.get('testing')  # Optional: number of records per endpoint
    
    # If triggered by scheduler, set start_date to previous day and end_date to empty
    if is_scheduled:
        print("📅 Scheduled invocation detected - setting date range to previous day")
        # Calculate previous day in YYYY-MM-DD format
        previous_day = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        start_date = previous_day
        end_date = ""  # Empty string for end_date means "no end date" (fetch all from start_date onwards)
        print(f"📅 Scheduler date range: START_DATE={start_date}, END_DATE={end_date} (empty)")
    else:
        print(f"📅 Manual invocation - Date range: START_DATE={start_date}, END_DATE={end_date}")
    
    # Convert testing_limit to int if it's provided (might come as string from event)
    if testing_limit is not None:
        try:
            testing_limit = int(testing_limit)
        except (ValueError, TypeError):
            print(f"⚠️  Invalid TESTING value: {testing_limit}, ignoring")
            testing_limit = None
    
    if testing_limit:
        print(f"🧪 Testing mode: {testing_limit} records per endpoint")
    
    # Get API key
    api_key = get_api_key()
    print("✅ Retrieved API key from Secrets Manager")
    
    # Create session
    session = create_session(api_key)
    
    # Fetch and store constants to S3 at the start
    fetch_and_store_constants(session)
    
    total_pages_sent = 0
    
    # Process filings endpoint
    print("\n📋 Fetching filings endpoint count...")
    try:
        filings_data = fetch_page_count(session, 'filings', start_date, end_date)
        # Convert count to int (API might return it as string)
        filings_count_raw = filings_data.get('count', 0)
        try:
            filings_count = int(filings_count_raw) if filings_count_raw else 0
        except (ValueError, TypeError):
            filings_count = 0
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
        # Convert count to int (API might return it as string)
        contributions_count_raw = contributions_data.get('count', 0)
        try:
            contributions_count = int(contributions_count_raw) if contributions_count_raw else 0
        except (ValueError, TypeError):
            contributions_count = 0
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

