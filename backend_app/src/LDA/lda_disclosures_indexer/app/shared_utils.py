"""
Shared utilities for LDA indexer Lambda functions
Contains common helper functions used by both filings.py and contributions.py
"""

import json
import os
import time
import requests
import boto3
from typing import Dict, Optional
from decimal import Decimal
from threading import Lock
from botocore.exceptions import ClientError

# Environment variables
LDA_API_BASE_URL = os.environ.get('LDA_API_BASE_URL', 'https://lda.senate.gov/api/v1')
LDA_SECRET_NAME = os.environ.get('LDA_SECRET_NAME')
FILINGS_TABLE_NAME = os.environ.get('FILINGS_TABLE_NAME')
PARAMETER_FILINGS_TABLE_NAME = os.environ.get('PARAMETER_FILINGS_TABLE_NAME')
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME')
REQUEST_TIMEOUT = int(os.environ.get('REQUEST_TIMEOUT', '30'))
RATE_LIMIT_DELAY = float(os.environ.get('RATE_LIMIT_DELAY', '0.5'))
PAC_QUEUE_URL = os.environ.get('PAC_QUEUE_URL', '')

# AWS clients
dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')
secrets_client = boto3.client('secretsmanager')
sqs_client = boto3.client('sqs')

# DynamoDB tables
filings_table = dynamodb.Table(FILINGS_TABLE_NAME) if FILINGS_TABLE_NAME else None
parameter_filings_table = dynamodb.Table(PARAMETER_FILINGS_TABLE_NAME) if PARAMETER_FILINGS_TABLE_NAME else None

# Rate Limiter
class RateLimiter:
    """Thread-safe rate limiter for API calls"""
    def __init__(self, calls_per_minute: int = 120):
        self.calls_per_minute = calls_per_minute
        self.min_interval = 60.0 / calls_per_minute
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

rate_limiter = RateLimiter(calls_per_minute=120)

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

def call_api(session: requests.Session, endpoint: str, params: Optional[Dict] = None) -> Dict:
    """Call LDA API endpoint with thread-safe rate limiting"""
    url = f"{LDA_API_BASE_URL}/{endpoint}/"
    
    rate_limiter.wait()
    response = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
    
    # Check for rate limiting (429)
    if response.status_code == 429:
        error_msg = f"Rate limited (429) for {endpoint}"
        print(f"   ⚠️  {error_msg}")
        try:
            error_data = response.json()
            print(f"      Error response: {json.dumps(error_data, indent=2)}")
        except:
            print(f"      Error text: {response.text[:500]}")
        # Raise a custom exception that can be caught for retry
        raise requests.exceptions.HTTPError(error_msg, response=response)
    
    if response.status_code >= 400:
        print(f"   🔍 Error Details:")
        print(f"      Status: {response.status_code}")
        print(f"      URL: {response.url}")
        try:
            error_data = response.json()
            print(f"      Error response: {json.dumps(error_data, indent=2)}")
        except:
            print(f"      Error text: {response.text[:500]}")
    
    response.raise_for_status()
    return response.json()

def download_document(session: requests.Session, url: str, s3_key: str) -> bool:
    """Download a document from URL and upload to S3"""
    try:
        rate_limiter.wait()
        response = session.get(url, timeout=REQUEST_TIMEOUT, stream=True)
        response.raise_for_status()
        
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=s3_key,
            Body=response.content,
            ContentType=response.headers.get('Content-Type', 'application/pdf')
        )
        
        return True
    except Exception as e:
        print(f"   ❌ Failed to download {url}: {str(e)[:200]}")
        return False

def merge_address_fields(item: Dict, registrant: Optional[Dict] = None) -> Dict:
    """Merge address fields from top-level and registrant into unified address structure"""
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
    
    # Fall back to registrant-specific address fields
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
    
    # Finally, fall back to registrant object address fields
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
    """Create a unified entity field from either client or lobbyist"""
    if client:
        return {'entity_type': 'client', 'entity': client}
    elif lobbyist:
        return {'entity_type': 'lobbyist', 'entity': lobbyist}
    return None

def send_autocomplete_value(field_type: str, value: str):
    """Send an autocomplete value to SQS queue for CSV generation"""
    if not PAC_QUEUE_URL or not value:
        return
    
    cleaned_value = value.strip()
    if cleaned_value.startswith('"') and cleaned_value.endswith('"'):
        cleaned_value = cleaned_value[1:-1]
    cleaned_value = cleaned_value.replace('"', '').replace(',', '')
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
        print(f"⚠️ Failed to send {field_type} to SQS: {str(e)[:200]}")


def save_parameter_filing_mapping(
    parameter_type: str,
    parameter_value: str,
    filing_uuid: str,
    filing_type: str,
    dt_posted: Optional[str] = None,
    filing_year: Optional[int] = None
):
    """
    Save a parameter-filing mapping to the parameter-filings table
    
    Args:
        parameter_type: Type of parameter (e.g., "PAC", "GENERAL_ISSUE", "GOVERNMENT_ENTITY", "FOREIGN_COUNTRY", "LOBBYIST")
        parameter_value: The actual value (e.g., "Dave Kamp 2008", "TAX", "12345", "FR", "John Doe")
        filing_uuid: UUID of the filing or contribution
        filing_type: "FILING" or "CONTRIBUTION"
        dt_posted: Posted date (optional)
        filing_year: Filing year (optional)
    """
    if not parameter_filings_table or not parameter_value or not filing_uuid:
        return
    
    try:
        # Create parameter key: "PARAMETER_TYPE#VALUE"
        parameter_key = f"{parameter_type}#{parameter_value}"
        
        item = {
            'parameter_key': parameter_key,
            'filing_uuid': filing_uuid,
            'parameter_type': parameter_type,
            'parameter_value': parameter_value,
            'filing_type': filing_type
        }
        
        if dt_posted:
            item['dt_posted'] = dt_posted
        
        if filing_year is not None:
            item['filing_year'] = int(filing_year)
        
        parameter_filings_table.put_item(Item=item)
        
    except Exception as e:
        print(f"⚠️ Failed to save parameter mapping {parameter_type}#{parameter_value} for {filing_uuid}: {str(e)[:200]}")

