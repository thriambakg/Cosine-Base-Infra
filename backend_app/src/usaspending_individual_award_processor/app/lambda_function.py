"""
USAspending Individual Award Processor Lambda
Processes individual awards from DLQ with full functionality including oversize support.
Reads award from CSV file(s) in S3 and processes it.
"""

import json
import os
import logging
import boto3
import time
import gzip
from typing import Dict, Any, Optional, List
from decimal import Decimal
from datetime import datetime, timezone

# Configure logging
logger = logging.getLogger()
logger.setLevel(os.environ.get('LOG_LEVEL', 'INFO').upper())

# AWS clients
dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')

# Environment variables
AWARDS_TABLE_NAME = os.environ.get('AWARDS_TABLE_NAME', 'usaspending-awards-index')
S3_BUCKET_NAME = os.environ.get('S3_BUCKET_NAME', 'cosine-usaspending-data-production')
USASPENDING_BASE_URL = os.environ.get('USASPENDING_BASE_URL', 'https://api.usaspending.gov')
MAX_RETRIES = 3
RETRY_DELAY = 1  # seconds

# Get DynamoDB table
awards_table = dynamodb.Table(AWARDS_TABLE_NAME) if AWARDS_TABLE_NAME else None


def convert_floats_to_decimal(obj: Any) -> Any:
    """Recursively convert floats to Decimal"""
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


def load_failed_award_from_s3(failed_award_s3_key: str) -> Optional[Dict[str, Any]]:
    """Load a failed award from S3 (stored as gzipped JSON)"""
    try:
        logger.info(f"Loading failed award from S3: {failed_award_s3_key}")
        s3_obj = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=failed_award_s3_key)
        
        # Decompress gzipped JSON
        compressed_data = s3_obj['Body'].read()
        json_bytes = gzip.decompress(compressed_data)
        json_data = json_bytes.decode('utf-8')
        
        # Parse JSON
        award_record = json.loads(json_data)
        
        # Convert numeric strings back to Decimal for DynamoDB
        award_record = convert_floats_to_decimal(award_record)
        
        logger.info(f"✅ Loaded failed award {award_record.get('award_id', 'unknown')} from S3")
        return award_record
        
    except Exception as e:
        logger.error(f"❌ Error loading failed award from S3 {failed_award_s3_key}: {str(e)}", exc_info=True)
        return None


def extract_gsi_fields_only(full_item: Dict[str, Any]) -> Dict[str, Any]:
    """Extract only GSI fields for DynamoDB when item is oversized"""
    gsi_fields = {
        'award_id': full_item.get('award_id'),
        'awarding_agency_code': full_item.get('awarding_agency_code'),
        'awarding_agency_name': full_item.get('awarding_agency_name'),
        'recipient_name_normalized': full_item.get('recipient_name_normalized') or 'unknown',
        'recipient_location_state': full_item.get('recipient_location_state'),
        'award_type': full_item.get('award_type'),
        'fiscal_year': full_item.get('fiscal_year'),
        'total_obligated_amount': (full_item.get('total_obligated_amount') or 
                                  full_item.get('total_dollars_obligated')),
        'period_start_date': (full_item.get('period_start_date') or 
                             full_item.get('period_of_performance_start_date')),
        'period_end_date': (full_item.get('period_end_date') or 
                           full_item.get('period_of_performance_end_date')),
        'transaction_count': full_item.get('transaction_count', 0),
        'subaward_count': full_item.get('subaward_count', 0),
        'full_indexing_complete': full_item.get('full_indexing_complete', True),
        'last_updated': full_item.get('last_updated'),
        'indexed_at': full_item.get('indexed_at'),
        'data_source': full_item.get('data_source', 'usaspending_bulk_download'),
        'api_version': full_item.get('api_version', 'bulk_csv_v2'),
        'ttl': full_item.get('ttl'),
        'is_oversized': True,
    }
    
    # Remove None values (but keep False/0 values)
    cleaned = {}
    for key, value in gsi_fields.items():
        if value is not None:
            cleaned[key] = value
    
    return cleaned


def convert_decimal_for_json(obj: Any) -> Any:
    """Recursively convert Decimal, Binary, and bytes to JSON-serializable types"""
    # Handle DynamoDB Binary type
    try:
        from boto3.dynamodb.types import Binary as DynamoDBBinary
        if isinstance(obj, DynamoDBBinary):
            obj = obj.value
    except ImportError:
        pass
    
    if isinstance(obj, Decimal):
        try:
            return float(obj)
        except (OverflowError, ValueError):
            return str(obj)
    elif isinstance(obj, bytes):
        if len(obj) == 1:
            return int(obj[0])
        else:
            return [int(b) for b in obj]
    elif isinstance(obj, dict):
        return {key: convert_decimal_for_json(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_decimal_for_json(item) for item in obj]
    else:
        return obj


def store_oversized_item_to_s3(award_id: str, full_item: Dict[str, Any]) -> str:
    """Store oversized item to S3"""
    s3_key = f"oversize/{award_id}.json.gz"
    
    json_ready_item = convert_decimal_for_json(full_item)
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
    
    logger.info(f"💾 Stored oversized award {award_id} to S3: {s3_key} ({len(compressed_data):,} bytes compressed)")
    return s3_key


def process_award(award_record: Dict[str, Any]) -> Dict[str, Any]:
    """Process and store an award to DynamoDB with oversize support"""
    award_id = award_record['award_id']
    
    # Convert all floats to Decimal
    db_item = convert_floats_to_decimal(award_record.copy())
    
    # Update timestamp
    db_item['last_updated'] = datetime.now(timezone.utc).isoformat()
    db_item['full_indexing_complete'] = True
    
    # Store to DynamoDB with retry logic for throttling and oversized items
    max_put_retries = 3
    for put_attempt in range(max_put_retries):
        try:
            awards_table.put_item(Item=db_item)
            logger.info(f"✅ Successfully stored award {award_id} to DynamoDB")
            break
        except Exception as put_error:
            error_str = str(put_error)
            
            # Handle oversized items
            if 'ValidationException' in error_str and 'Item size has exceeded' in error_str:
                logger.info(f"⚠️ Award {award_id} exceeds DynamoDB size limit, storing to S3...")
                
                # Store full item to S3
                oversize_s3_key = store_oversized_item_to_s3(award_id, db_item)
                
                # Extract only GSI fields for DynamoDB
                gsi_only_item = extract_gsi_fields_only(db_item)
                gsi_only_item['oversize_s3_key'] = oversize_s3_key
                
                # Try to store GSI-only item
                try:
                    awards_table.put_item(Item=gsi_only_item)
                    logger.info(f"✅ Stored GSI fields for oversized award {award_id} to DynamoDB, full data in S3")
                    break
                except Exception as gsi_error:
                    logger.error(f"❌ Even GSI-only item too large for {award_id}: {str(gsi_error)}")
                    raise
            
            # Handle throttling
            elif 'ThrottlingException' in error_str or 'ProvisionedThroughputExceededException' in error_str:
                if put_attempt < max_put_retries - 1:
                    wait_time = (put_attempt + 1) * 2
                    logger.info(f"⚠️ DynamoDB throttled for award {award_id}, waiting {wait_time}s before retry {put_attempt + 1}/{max_put_retries}")
                    time.sleep(wait_time)
                    continue
            
            # Re-raise if not throttling or out of retries
            raise
    
    return {
        'success': True,
        'award_id': award_id,
        'transaction_count': award_record.get('transaction_count', 0),
        'subaward_count': award_record.get('subaward_count', 0)
    }


def lambda_handler(event, context):
    """Process individual award from DLQ"""
    success_count = 0
    failure_count = 0
    
    # Handle SQS event
    records = event.get('Records', [])
    logger.info(f"Processing {len(records)} message(s) from DLQ")
    
    for record in records:
        try:
            # Parse SQS message body
            if isinstance(record.get('body'), str):
                message_body = json.loads(record['body'])
            else:
                message_body = record.get('body', {})
            
            award_id = message_body.get('award_id')
            failed_award_s3_key = message_body.get('failed_award_s3_key')
            
            if not award_id:
                logger.error("Missing award_id in message")
                failure_count += 1
                continue
            
            if not failed_award_s3_key:
                logger.error(f"Missing failed_award_s3_key in message for award {award_id}")
                failure_count += 1
                continue
            
            # Load failed award directly from S3
            award_record = load_failed_award_from_s3(failed_award_s3_key)
            
            if not award_record:
                logger.error(f"❌ Failed to load award {award_id} from S3 key {failed_award_s3_key}")
                failure_count += 1
                continue
            
            # Verify award_id matches
            if award_record.get('award_id') != award_id:
                logger.warning(f"⚠️ Award ID mismatch: expected {award_id}, got {award_record.get('award_id')}")
                # Use the award_id from the record if it exists, otherwise use the one from message
                if award_record.get('award_id'):
                    award_id = award_record['award_id']
                else:
                    award_record['award_id'] = award_id
            
            # Process the award
            result = process_award(award_record)
            if result.get('success'):
                success_count += 1
                logger.info(f"✅ Successfully processed award {award_id}")
            else:
                failure_count += 1
                logger.error(f"❌ Failed to process award {award_id}")
                
        except Exception as e:
            logger.error(f"❌ Error processing message: {str(e)}", exc_info=True)
            failure_count += 1
    
    logger.info(f"Processing complete: {success_count} successful, {failure_count} failed")
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'success': True,
            'processed': len(records),
            'successful': success_count,
            'failed': failure_count
        })
    }



