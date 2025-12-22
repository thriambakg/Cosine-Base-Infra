"""
Shared utilities for LDA indexer Lambda functions
Contains common helper functions used by both filings.py and contributions.py
"""

import json
import os
import csv
import time
import requests
import boto3
from typing import Dict, Optional
from decimal import Decimal
from threading import Lock
from botocore.exceptions import ClientError
from io import StringIO

# Environment variables
LDA_API_BASE_URL = os.environ.get('LDA_API_BASE_URL', 'https://lda.senate.gov/api/v1')
LDA_SECRET_NAME = os.environ.get('LDA_SECRET_NAME')
FILINGS_TABLE_NAME = os.environ.get('FILINGS_TABLE_NAME')
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

# Government entities mapping (ID -> name)
_government_entities_map = None
_government_entities_map_lock = Lock()

def sync_file_from_s3_to_efs(s3_key: str, efs_path: str) -> bool:
    """Sync a file from S3 to EFS if EFS is available and file doesn't exist in EFS"""
    try:
        # Check if EFS is available
        efs_base = '/mnt/efs'
        if not os.path.exists(efs_base):
            return False
        
        # Check if file already exists in EFS
        if os.path.exists(efs_path):
            return True  # Already synced
        
        # Create directory if needed
        efs_dir = os.path.dirname(efs_path)
        os.makedirs(efs_dir, exist_ok=True)
        
        # Download from S3
        if not S3_BUCKET_NAME:
            return False
        
        response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=s3_key)
        content = response['Body'].read().decode('utf-8')
        
        # Write to EFS
        with open(efs_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        print(f"✅ Synced {s3_key} from S3 to EFS: {efs_path}")
        return True
        
    except Exception as e:
        print(f"⚠️  Could not sync {s3_key} to EFS: {str(e)[:200]}")
        return False

def load_government_entities_map() -> Dict[int, str]:
    """Load government entities mapping from local CSV file, EFS, or S3"""
    global _government_entities_map
    
    if _government_entities_map is not None:
        return _government_entities_map
    
    with _government_entities_map_lock:
        if _government_entities_map is not None:
            return _government_entities_map
        
        try:
            # Try to load from local file first (in Lambda package)
            local_path = os.path.join(os.path.dirname(__file__), 'government_entities.csv')
            if os.path.exists(local_path):
                with open(local_path, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    _government_entities_map = {int(row['id']): row['name'] for row in reader if row.get('id') and row.get('name')}
                    print(f"✅ Loaded {len(_government_entities_map)} government entities from local file")
                    return _government_entities_map
            
            # Try EFS mount point (if available)
            efs_path = '/mnt/efs/lists/government_entities.csv'
            if os.path.exists(efs_path):
                with open(efs_path, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    _government_entities_map = {int(row['id']): row['name'] for row in reader if row.get('id') and row.get('name')}
                    print(f"✅ Loaded {len(_government_entities_map)} government entities from EFS")
                    return _government_entities_map
            
            # Try to sync from S3 to EFS (lazy loading)
            if os.path.exists('/mnt/efs'):
                sync_file_from_s3_to_efs('lists/government_entities.csv', efs_path)
                # Try loading from EFS again after sync
                if os.path.exists(efs_path):
                    with open(efs_path, 'r', encoding='utf-8') as f:
                        reader = csv.DictReader(f)
                        _government_entities_map = {int(row['id']): row['name'] for row in reader if row.get('id') and row.get('name')}
                        print(f"✅ Loaded {len(_government_entities_map)} government entities from EFS (synced from S3)")
                        return _government_entities_map
            
            # Fallback: try to load directly from S3
            if S3_BUCKET_NAME:
                try:
                    response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key='lists/government_entities.csv')
                    csv_content = response['Body'].read().decode('utf-8')
                    reader = csv.DictReader(StringIO(csv_content))
                    _government_entities_map = {int(row['id']): row['name'] for row in reader if row.get('id') and row.get('name')}
                    print(f"✅ Loaded {len(_government_entities_map)} government entities from S3")
                    return _government_entities_map
                except Exception as e:
                    print(f"⚠️  Could not load government entities from S3: {str(e)[:200]}")
            
            # If all fail, return empty map
            print("⚠️  Could not load government entities mapping, using empty map")
            _government_entities_map = {}
            return _government_entities_map
            
        except Exception as e:
            print(f"❌ Error loading government entities mapping: {str(e)[:200]}")
            _government_entities_map = {}
            return _government_entities_map

def get_government_entity_name(entity_id: int) -> Optional[str]:
    """Get government entity name by ID"""
    entities_map = load_government_entities_map()
    return entities_map.get(entity_id)

# General issue codes mapping (code -> name)
_general_issues_map = None
_general_issues_map_lock = Lock()

def load_general_issues_map() -> Dict[str, str]:
    """Load general issue codes mapping from local CSV file, EFS, or S3"""
    global _general_issues_map
    
    if _general_issues_map is not None:
        return _general_issues_map
    
    with _general_issues_map_lock:
        if _general_issues_map is not None:
            return _general_issues_map
        
        try:
            # Try to load from local file first (in Lambda package)
            local_path = os.path.join(os.path.dirname(__file__), 'general_issues.csv')
            if os.path.exists(local_path):
                with open(local_path, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    _general_issues_map = {row['value']: row['name'] for row in reader if row.get('value') and row.get('name')}
                    print(f"✅ Loaded {len(_general_issues_map)} general issue codes from local file")
                    return _general_issues_map
            
            # Try EFS mount point (if available)
            efs_path = '/mnt/efs/lists/general_issues.csv'
            if os.path.exists(efs_path):
                with open(efs_path, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    _general_issues_map = {row['value']: row['name'] for row in reader if row.get('value') and row.get('name')}
                    print(f"✅ Loaded {len(_general_issues_map)} general issue codes from EFS")
                    return _general_issues_map
            
            # Try to sync from S3 to EFS (lazy loading)
            if os.path.exists('/mnt/efs'):
                sync_file_from_s3_to_efs('lists/general_issues.csv', efs_path)
                # Try loading from EFS again after sync
                if os.path.exists(efs_path):
                    with open(efs_path, 'r', encoding='utf-8') as f:
                        reader = csv.DictReader(f)
                        _general_issues_map = {row['value']: row['name'] for row in reader if row.get('value') and row.get('name')}
                        print(f"✅ Loaded {len(_general_issues_map)} general issue codes from EFS (synced from S3)")
                        return _general_issues_map
            
            # Fallback: try to load directly from S3
            if S3_BUCKET_NAME:
                try:
                    response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key='lists/general_issues.csv')
                    csv_content = response['Body'].read().decode('utf-8')
                    reader = csv.DictReader(StringIO(csv_content))
                    _general_issues_map = {row['value']: row['name'] for row in reader if row.get('value') and row.get('name')}
                    print(f"✅ Loaded {len(_general_issues_map)} general issue codes from S3")
                    return _general_issues_map
                except Exception as e:
                    print(f"⚠️  Could not load general issue codes from S3: {str(e)[:200]}")
            
            # If all fail, return empty map
            print("⚠️  Could not load general issue codes mapping, using empty map")
            _general_issues_map = {}
            return _general_issues_map
            
        except Exception as e:
            print(f"❌ Error loading general issue codes mapping: {str(e)[:200]}")
            _general_issues_map = {}
            return _general_issues_map

def get_general_issue_name(issue_code: str) -> Optional[str]:
    """Get general issue name by code"""
    issues_map = load_general_issues_map()
    return issues_map.get(issue_code)

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
    dt_posted: Optional[str] = None
):
    """
    Save a parameter-filing mapping to the main filings table using a different PK/SK structure
    
    Structure: PK = "PARAMETER_TYPE#VALUE", SK = "FILING#{uuid}" or "CONTRIBUTION#{uuid}"
    This allows querying by parameter to get all associated filings/contributions
    
    Args:
        parameter_type: Type of parameter (e.g., "PAC", "GENERAL_ISSUE", "GOVERNMENT_ENTITY", "FOREIGN_COUNTRY", "LOBBYIST")
        parameter_value: The actual value (e.g., "Dave Kamp 2008", "TAX", "12345", "FR", "John Doe")
        filing_uuid: UUID of the filing or contribution
        filing_type: "FILING" or "CONTRIBUTION"
        dt_posted: Posted date (optional)
    """
    if not filings_table or not parameter_value or not filing_uuid:
        return
    
    try:
        # Create parameter key: "PARAMETER_TYPE#VALUE" as PK
        parameter_key = f"{parameter_type}#{parameter_value}"
        
        # SK is the filing/contribution identifier
        sk = f"{filing_type}#{filing_uuid}"
        
        item = {
            'PK': parameter_key,
            'SK': sk,
            'parameter_type': parameter_type,
            'parameter_value': parameter_value,
            'filing_uuid': filing_uuid,
            'filing_type': filing_type
        }
        
        if dt_posted:
            item['dt_posted'] = dt_posted
        
        filings_table.put_item(Item=item)
        
    except Exception as e:
        print(f"⚠️ Failed to save parameter mapping {parameter_type}#{parameter_value} for {filing_uuid}: {str(e)[:200]}")

