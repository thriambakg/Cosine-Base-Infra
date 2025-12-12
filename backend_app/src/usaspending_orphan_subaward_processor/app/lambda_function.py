"""
USAspending Orphan Subaward Processor Lambda
Processes orphan subawards by fetching parent awards from API and storing them in DynamoDB
"""

import json
import os
import logging
import boto3
import gzip
import time
from typing import Dict, Any, Optional
from decimal import Decimal
from datetime import datetime, timezone

# Configure logging
logger = logging.getLogger()
logger.setLevel(os.environ.get('LOG_LEVEL', 'INFO').upper())

# AWS clients
dynamodb = boto3.resource('dynamodb')
sqs_client = boto3.client('sqs')
s3_client = boto3.client('s3')

# Environment variables
AWARDS_TABLE_NAME = os.environ.get('AWARDS_TABLE_NAME', 'usaspending-awards-index')
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', 'cosine-usaspending-data-production')
USASPENDING_BASE_URL = os.environ.get('USASPENDING_BASE_URL', 'https://api.usaspending.gov')
ORPHAN_SUBAWARD_QUEUE_URL = os.environ.get('ORPHAN_SUBAWARD_QUEUE_URL', '')
MAX_RETRIES = 3
RETRY_DELAY = 1  # seconds
MAX_MESSAGES_PER_INVOCATION = 50  # Memory considerations:
# - Message payloads: up to 50 × 200KB = ~10MB (if not in S3)
# - Processing is sequential, so peak memory is per-message, not all 50
# - S3 downloads (decompressed) can be large per message
# - Lambda memory set to 1024MB to handle worst-case scenarios
POLL_WAIT_SECONDS = 3  # Wait time before polling for additional messages

# Get DynamoDB table
awards_table = dynamodb.Table(AWARDS_TABLE_NAME) if AWARDS_TABLE_NAME else None


def normalize_string(value: Any) -> str:
    """Normalize string values"""
    if value is None:
        return ''
    return str(value).strip()


def call_usaspending_api(endpoint: str, method: str = 'GET', params: Optional[Dict] = None) -> Optional[Dict[str, Any]]:
    """
    Call USAspending API with retry logic
    
    Args:
        endpoint: API endpoint (e.g., '/api/v2/awards/ASST_NON_...')
        method: HTTP method (default: 'GET')
        params: Optional query parameters
    
    Returns:
        API response as dict, or None if failed
    """
    import requests
    import time
    
    url = f"{USASPENDING_BASE_URL}{endpoint}"
    last_exception = None
    
    for attempt in range(MAX_RETRIES):
        try:
            if method.upper() == 'GET':
                response = requests.get(url, params=params, timeout=30)
            else:
                response = requests.request(method, url, json=params, timeout=30)
            
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.warning(f"Award not found in API (404): {endpoint}")
                return None
            last_exception = e
            if attempt < MAX_RETRIES - 1:
                wait_time = RETRY_DELAY * (2 ** attempt)
                logger.warning(f"HTTP error calling API (attempt {attempt + 1}/{MAX_RETRIES}): {e.response.status_code}, retrying in {wait_time}s...")
                time.sleep(wait_time)
                continue
        except Exception as e:
            last_exception = e
            if attempt < MAX_RETRIES - 1:
                wait_time = RETRY_DELAY * (2 ** attempt)
                logger.warning(f"Error calling API (attempt {attempt + 1}/{MAX_RETRIES}): {str(e)[:200]}, retrying in {wait_time}s...")
                time.sleep(wait_time)
                continue
    
    if last_exception:
        logger.error(f"Failed to call API after {MAX_RETRIES} retries: {str(last_exception)}")
        raise last_exception
    raise Exception(f"Failed to call API after {MAX_RETRIES} retries")


def fetch_award_from_api(award_id: str) -> Optional[Dict[str, Any]]:
    """
    Fetch award details from USAspending API and convert to our award record format.
    This matches the logic from the Glue job's fetch_award_from_api function.
    
    Args:
        award_id: The award ID to fetch (e.g., ASST_NON_251VA307N1199_012)
    
    Returns:
        Award record dict in our format, or None if fetch fails
    """
    try:
        logger.info(f"📡 Fetching award {award_id} from USAspending API...")
        api_response = call_usaspending_api(f'/api/v2/awards/{award_id}/', method='GET')
        
        if not api_response:
            logger.warning(f"⚠️ No data returned from API for award {award_id}")
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
        if 'total_obligation' in api_response:
            total_obligation_val = api_response['total_obligation']
            if total_obligation_val is not None and total_obligation_val != '':
                try:
                    if isinstance(total_obligation_val, (int, float)):
                        award_record['total_obligated_amount'] = Decimal(str(total_obligation_val))
                    elif isinstance(total_obligation_val, str) and total_obligation_val.strip():
                        award_record['total_obligated_amount'] = Decimal(total_obligation_val.strip())
                except (ValueError, TypeError, Exception):
                    pass
        
        # Flatten awarding_agency (matching Glue job logic)
        if 'awarding_agency' in api_response:
            agency = api_response['awarding_agency']
            if isinstance(agency, dict):
                for agency_key, agency_value in agency.items():
                    if agency_value is None:
                        continue
                    if agency_key == 'toptier_agency':
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
        
        # Flatten funding_agency (matching Glue job logic)
        if 'funding_agency' in api_response:
            agency = api_response['funding_agency']
            if isinstance(agency, dict):
                for agency_key, agency_value in agency.items():
                    if agency_value is None:
                        continue
                    if agency_key == 'toptier_agency':
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
        
        # Flatten recipient information (matching Glue job logic - store in uppercase)
        if 'recipient' in api_response:
            recipient = api_response['recipient']
            if isinstance(recipient, dict):
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
                                if loc_key == 'state_code':
                                    award_record['recipient_location_state'] = normalize_string(loc_value)
                                elif loc_key == 'country_code':
                                    award_record['recipient_location_country'] = normalize_string(loc_value)
        
        # Ensure recipient_name_normalized is set (required for GSI)
        if not award_record.get('recipient_name_normalized'):
            award_record['recipient_name_normalized'] = 'unknown'
        
        # Flatten period_of_performance
        if 'period_of_performance' in api_response:
            pop = api_response['period_of_performance']
            if isinstance(pop, dict):
                if 'start_date' in pop:
                    award_record['period_start_date'] = normalize_string(pop['start_date'])
                    award_record['period_of_performance_start_date'] = normalize_string(pop['start_date'])
                if 'end_date' in pop or 'current_end_date' in pop:
                    end_date = pop.get('end_date') or pop.get('current_end_date')
                    award_record['period_end_date'] = normalize_string(end_date)
                    award_record['period_of_performance_current_end_date'] = normalize_string(end_date)
        
        # Fallback to top-level date fields if period_of_performance not available
        if 'period_start_date' not in award_record:
            award_record['period_start_date'] = normalize_string(api_response.get('period_of_performance_start_date', ''))
        if 'period_end_date' not in award_record:
            award_record['period_end_date'] = normalize_string(api_response.get('period_of_performance_current_end_date', ''))
        
        # Calculate fiscal year from start date
        if award_record.get('period_start_date'):
            try:
                date_obj = datetime.strptime(award_record['period_start_date'].split('T')[0], '%Y-%m-%d')
                year = date_obj.year
                month = date_obj.month
                # US fiscal year: Oct 1 - Sep 30
                award_record['fiscal_year'] = year + 1 if month >= 10 else year
            except (ValueError, TypeError):
                pass
        
        # Handle award type
        award_record['award_type'] = normalize_string(api_response.get('type', ''))
        award_record['award_type_description'] = normalize_string(api_response.get('type_description', ''))
        
        # Handle amounts
        if api_response.get('total_obligated_amount') is not None:
            try:
                award_record['total_obligated_amount'] = Decimal(str(api_response['total_obligated_amount']))
            except (ValueError, TypeError):
                pass
        
        # Handle IDV information
        award_record['is_idv_parent'] = api_response.get('is_idv_parent', False)
        award_record['is_idv_child'] = api_response.get('is_idv_child', False)
        award_record['parent_idv_id'] = normalize_string(api_response.get('parent_idv_id', ''))
        
        # Handle is_assistance (number field: 0 = contract, 1 = assistance)
        award_record['is_assistance'] = 1 if api_response.get('assistance_type', {}).get('name') else 0
        
        # Set category based on award type
        if award_record.get('award_type') in ['A', 'B', 'C', 'D', 'IDV']:
            award_record['category'] = 'contract'
        elif award_record.get('award_type') in ['02', '03', '04', '05', '06', '07', '08', '09', '10', '11']:
            award_record['category'] = 'assistance'
        else:
            award_record['category'] = 'other'
        
        # Initialize arrays for transactions and subawards (will be populated from message)
        award_record['transactions'] = []
        award_record['transaction_count'] = 0
        award_record['subawards'] = []
        award_record['subaward_count'] = 0
        
        logger.info(f"✅ Successfully fetched award {award_id} from API")
        return award_record
        
    except Exception as e:
        logger.error(f"❌ Error fetching award {award_id} from API: {str(e)}", exc_info=True)
        return None


def convert_floats_to_decimal(obj: Any) -> Any:
    """Recursively convert float values to Decimal for DynamoDB compatibility"""
    if isinstance(obj, float):
        return Decimal(str(obj))
    elif isinstance(obj, dict):
        return {k: convert_floats_to_decimal(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_floats_to_decimal(item) for item in obj]
    else:
        return obj


def extract_gsi_fields_only(full_item: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract only GSI fields and essential metadata for DynamoDB.
    Used when item exceeds 400KB limit.
    """
    gsi_fields = {
        'award_id': full_item.get('award_id'),
        'awarding_agency_code': full_item.get('awarding_agency_code'),
        'awarding_agency_name': full_item.get('awarding_agency_name'),
        'recipient_name_normalized': full_item.get('recipient_name_normalized') or 'unknown',
        'recipient_location_state': full_item.get('recipient_location_state'),
        'award_type': full_item.get('award_type'),
        'fiscal_year': full_item.get('fiscal_year'),
        'total_obligated_amount': full_item.get('total_obligated_amount'),
        'period_start_date': full_item.get('period_start_date'),
        'period_end_date': full_item.get('period_end_date'),
        'transaction_count': full_item.get('transaction_count', 0),
        'subaward_count': full_item.get('subaward_count', 0),
        'full_indexing_complete': full_item.get('full_indexing_complete', True),
        'last_updated': full_item.get('last_updated'),
        'data_source': full_item.get('data_source', 'usaspending_api_direct'),
        'api_version': full_item.get('api_version', 'api_v2_direct_fetch'),
        'is_oversized': True,
    }
    
    # Remove None values (but keep False/0 values)
    cleaned = {}
    for k, v in gsi_fields.items():
        if v is not None:
            cleaned[k] = v
    
    return cleaned


def store_oversized_item_to_s3(award_id: str, full_item: Dict[str, Any]) -> str:
    """Store oversized item to S3 in oversize/ folder. Returns the S3 key."""
    s3_key = f"oversize/{award_id}.json.gz"
    
    # Convert Decimal values to JSON-serializable types
    def convert_for_json(obj):
        if isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, bytes):
            return int(obj[0]) if len(obj) == 1 else [int(b) for b in obj]
        elif isinstance(obj, dict):
            return {k: convert_for_json(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [convert_for_json(item) for item in obj]
        else:
            return obj
    
    json_ready_item = convert_for_json(full_item)
    json_data = json.dumps(json_ready_item, ensure_ascii=False, indent=2)
    json_bytes = json_data.encode('utf-8')
    compressed_data = gzip.compress(json_bytes)
    
    s3_client.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=s3_key,
        Body=compressed_data,
        ContentType='application/json',
        ContentEncoding='gzip'
    )
    
    logger.info(f"💾 Stored oversized award {award_id} to S3: {s3_key}")
    return s3_key


def store_award_to_dynamodb(award_record: Dict[str, Any], subawards: list) -> bool:
    """
    Store award record to DynamoDB with subawards.
    Matches the logic from Glue job's store_award_to_dynamodb function.
    
    Args:
        award_record: Award record dict
        subawards: List of subaward records
    
    Returns:
        True if successful, False otherwise
    """
    try:
        award_id = award_record.get('award_id')
        if not award_id:
            logger.error("Missing award_id in award_record")
            return False
        
        # Add subawards to award record
        award_record['subawards'] = convert_floats_to_decimal(subawards)
        award_record['subaward_count'] = len(subawards)
        award_record['full_indexing_complete'] = True
        award_record['last_updated'] = datetime.now(timezone.utc).isoformat()
        
        # Ensure fiscal_year is set (required for GSIs)
        if 'fiscal_year' not in award_record or not award_record.get('fiscal_year'):
            if award_record.get('period_start_date'):
                try:
                    date_obj = datetime.strptime(award_record['period_start_date'].split('T')[0], '%Y-%m-%d')
                    year = date_obj.year
                    month = date_obj.month
                    award_record['fiscal_year'] = year + 1 if month >= 10 else year
                except (ValueError, TypeError):
                    # Use current fiscal year as default
                    now = datetime.now(timezone.utc)
                    award_record['fiscal_year'] = now.year + 1 if now.month >= 10 else now.year
            else:
                # Use current fiscal year as default
                now = datetime.now(timezone.utc)
                award_record['fiscal_year'] = now.year + 1 if now.month >= 10 else now.year
        
        # Ensure recipient_name_normalized is set (required for GSI)
        if not award_record.get('recipient_name_normalized'):
            award_record['recipient_name_normalized'] = 'unknown'
        
        # Convert all floats to Decimal
        db_item = convert_floats_to_decimal(award_record)
        
        # Store to DynamoDB with retry logic for throttling and oversized items
        max_put_retries = 3
        for put_attempt in range(max_put_retries):
            try:
                awards_table.put_item(Item=db_item)
                logger.info(f"✅ Successfully stored award {award_id} to DynamoDB with {len(subawards)} subawards")
                return True
            except Exception as put_error:
                error_str = str(put_error)
                
                # Handle oversized items (ValidationException)
                if 'ValidationException' in error_str and 'Item size has exceeded' in error_str:
                    logger.warning(f"⚠️ Award {award_id} exceeds DynamoDB size limit, storing to S3...")
                    
                    # Store full item to S3
                    oversize_s3_key = store_oversized_item_to_s3(award_id, db_item)
                    
                    # Extract only GSI fields for DynamoDB
                    gsi_only_item = extract_gsi_fields_only(db_item)
                    gsi_only_item['oversize_s3_key'] = oversize_s3_key
                    
                    # Try to store GSI-only item
                    try:
                        awards_table.put_item(Item=gsi_only_item)
                        logger.info(f"✅ Stored GSI fields for oversized award {award_id} to DynamoDB, full data in S3")
                        return True
                    except Exception as gsi_error:
                        logger.error(f"❌ Even GSI-only item too large for {award_id}: {str(gsi_error)}")
                        return False
                
                # Handle throttling
                elif 'ThrottlingException' in error_str or 'ProvisionedThroughputExceededException' in error_str:
                    if put_attempt < max_put_retries - 1:
                        wait_time = (put_attempt + 1) * 2  # 2s, 4s, 6s
                        logger.warning(f"⚠️ DynamoDB throttled for award {award_id}, waiting {wait_time}s before retry {put_attempt + 1}/{max_put_retries}")
                        time.sleep(wait_time)
                        continue
                
                # Re-raise if not throttling or out of retries
                raise
        
        return False
        
    except Exception as e:
        logger.error(f"❌ Error storing award to DynamoDB: {str(e)}", exc_info=True)
        return False


def process_message(record: Dict[str, Any]) -> tuple[bool, Optional[str]]:
    """
    Process a single SQS message record.
    
    Returns:
        Tuple of (success: bool, parent_id: Optional[str])
    """
    try:
        # Parse SQS message body
        if isinstance(record.get('body'), str):
            message_body = json.loads(record['body'])
        else:
            message_body = record.get('body', {})
        
        parent_id = message_body.get('parent_id')
        
        if not parent_id:
            logger.error("Missing parent_id in message")
            return (False, None)
        
        # Get subawards from message or S3
        subawards = []
        if 'subawards' in message_body:
            # Subawards directly in message
            subawards = message_body.get('subawards', [])
        elif 'subawards_s3_key' in message_body:
            # Subawards stored in S3 (message was too large)
            s3_key = message_body.get('subawards_s3_key')
            logger.info(f"Fetching subawards from S3: {s3_key}")
            
            try:
                # Download and decompress from S3
                s3_response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=s3_key)
                compressed_data = s3_response['Body'].read()
                json_data = gzip.decompress(compressed_data).decode('utf-8')
                s3_message = json.loads(json_data)
                subawards = s3_message.get('subawards', [])
                logger.info(f"✅ Loaded {len(subawards)} subawards from S3")
            except Exception as e:
                logger.error(f"❌ Error loading subawards from S3 {s3_key}: {str(e)}", exc_info=True)
                return (False, parent_id)
        else:
            logger.warning(f"No subawards or subawards_s3_key in message for parent {parent_id}")
            return (False, parent_id)
        
        if not subawards:
            logger.warning(f"No subawards found for parent {parent_id}")
            return (False, parent_id)
        
        logger.info(f"Processing orphan subaward for parent {parent_id} with {len(subawards)} subaward(s)")
        
        # Fetch parent award from API
        award_record = fetch_award_from_api(parent_id)
        
        if not award_record:
            logger.error(f"Failed to fetch parent award {parent_id} from API")
            return (False, parent_id)
        
        # Store to DynamoDB
        if store_award_to_dynamodb(award_record, subawards):
            logger.info(f"✅ Successfully processed orphan subaward for parent {parent_id}")
            return (True, parent_id)
        else:
            logger.error(f"❌ Failed to store award {parent_id} to DynamoDB")
            return (False, parent_id)
            
    except Exception as e:
        logger.error(f"❌ Error processing SQS record: {str(e)}", exc_info=True)
        return (False, None)


def lambda_handler(event, context):
    """
    Lambda handler for processing orphan subaward messages from SQS.
    Processes up to 50 messages per invocation. If initial batch has fewer than 50,
    waits 3 seconds and polls for additional messages.
    
    Expected message format:
    {
        "parent_id": "ASST_NON_...",
        "subawards": [...]  // Direct subawards in message
        OR
        "subawards_s3_key": "orphan-subawards/ASST_NON_....json.gz"  // S3 key if message too large
    }
    """
    # Collect all messages to process
    all_records = list(event.get('Records', []))
    initial_count = len(all_records)
    
    logger.info(f"Initial batch: {initial_count} message(s)")
    
    # If we have fewer than 50 messages, wait and poll for more
    if initial_count < MAX_MESSAGES_PER_INVOCATION and ORPHAN_SUBAWARD_QUEUE_URL:
        logger.info(f"Initial batch has {initial_count} messages (target: {MAX_MESSAGES_PER_INVOCATION}), waiting {POLL_WAIT_SECONDS} seconds before polling for additional messages...")
        time.sleep(POLL_WAIT_SECONDS)
        
        # Poll for additional messages
        messages_needed = MAX_MESSAGES_PER_INVOCATION - initial_count
        try:
            # Receive messages from SQS (max 10 per call, but we'll call multiple times if needed)
            while len(all_records) < MAX_MESSAGES_PER_INVOCATION:
                messages_to_fetch = min(10, MAX_MESSAGES_PER_INVOCATION - len(all_records))
                response = sqs_client.receive_message(
                    QueueUrl=ORPHAN_SUBAWARD_QUEUE_URL,
                    MaxNumberOfMessages=messages_to_fetch,
                    WaitTimeSeconds=0  # Short polling
                )
                
                messages = response.get('Messages', [])
                if not messages:
                    logger.info("No additional messages available in queue")
                    break
                
                logger.info(f"Received {len(messages)} additional message(s) from SQS")
                
                # Convert SQS message format to event record format
                for msg in messages:
                    if len(all_records) >= MAX_MESSAGES_PER_INVOCATION:
                        break
                    
                    record = {
                        'body': msg.get('Body', '{}'),
                        'receiptHandle': msg.get('ReceiptHandle'),
                        'messageId': msg.get('MessageId')
                    }
                    all_records.append(record)
                
                # If we got fewer than requested, no more messages available
                if len(messages) < messages_to_fetch:
                    break
                    
        except Exception as e:
            logger.error(f"Error polling SQS for additional messages: {str(e)}", exc_info=True)
    
    total_to_process = len(all_records)
    logger.info(f"Processing {total_to_process} message(s) total")
    
    success_count = 0
    failure_count = 0
    processed_receipt_handles = []  # Track manually polled messages for deletion
    
    # Process all collected messages
    for i, record in enumerate(all_records):
        # Track if this was a manually polled message (has receiptHandle but not from event)
        is_manually_polled = i >= initial_count and 'receiptHandle' in record
        
        success, parent_id = process_message(record)
        
        if success:
            success_count += 1
            # Delete manually polled messages from queue after successful processing
            if is_manually_polled and record.get('receiptHandle'):
                processed_receipt_handles.append(record['receiptHandle'])
        else:
            failure_count += 1
            # Don't delete failed messages - let them go to DLQ after max retries
    
    # Delete successfully processed messages that were manually polled
    if processed_receipt_handles:
        try:
            # Delete in batches of 10 (SQS limit)
            for i in range(0, len(processed_receipt_handles), 10):
                batch = processed_receipt_handles[i:i+10]
                entries = [
                    {'Id': str(j), 'ReceiptHandle': handle}
                    for j, handle in enumerate(batch)
                ]
                sqs_client.delete_message_batch(
                    QueueUrl=ORPHAN_SUBAWARD_QUEUE_URL,
                    Entries=entries
                )
            logger.info(f"Deleted {len(processed_receipt_handles)} successfully processed message(s) from queue")
        except Exception as e:
            logger.warning(f"Failed to delete some messages from queue: {str(e)}")
    
    logger.info(f"Processing complete: {total_to_process} total processed, {success_count} successful, {failure_count} failed")
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'success': True,
            'processed': total_to_process,
            'successful': success_count,
            'failed': failure_count
        })
    }




