"""
AWS Glue Job: Update IDV Parent Award Obligations
Updates total_obligated_amount from combined_obligated_amount for IDV parent awards.
This makes IDVs queryable by obligation amount in GSIs.

This is a one-time maintenance job that can be run and then destroyed.
"""

import sys
import json
import logging
from decimal import Decimal
from typing import Dict, Any, Optional, Tuple

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, isnan, isnull

import boto3

# Get required job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'AWARDS_TABLE_NAME'
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

def log_print(message):
    """Print to both logger and stdout for maximum visibility in Glue"""
    logger.info(message)
    print(message, file=sys.stdout, flush=True)

log_print("=" * 80)
log_print("✅ IDV Obligation Update Glue Job - Starting")
log_print("=" * 80)

# Environment variables
AWARDS_TABLE_NAME = args.get('AWARDS_TABLE_NAME', 'cosine-usaspending-awards-index-production')

# AWS clients
dynamodb = boto3.client('dynamodb')
dynamodb_resource = boto3.resource('dynamodb')
table = dynamodb_resource.Table(AWARDS_TABLE_NAME)

log_print(f"Table: {AWARDS_TABLE_NAME}")

# Statistics
stats = {
    'total_scanned': 0,
    'total_updated': 0,
    'total_skipped': 0,
    'total_errors': 0
}

def should_update_idv(item: Dict[str, Any]) -> Tuple[bool, Optional[Decimal]]:
    """
    Check if an IDV needs to be updated.
    Returns (needs_update, combined_obligated_amount)
    """
    award_id = item.get('award_id', {}).get('S', '') if isinstance(item.get('award_id'), dict) else item.get('award_id', '')
    
    # Only process IDV parent awards
    if not award_id.startswith('CONT_IDV'):
        return False, None
    
    # Check if it's a parent IDV
    is_idv_parent = (
        item.get('is_idv_parent', {}).get('BOOL', False) if isinstance(item.get('is_idv_parent'), dict) else item.get('is_idv_parent', False) or
        'child_awards' in item or
        (item.get('category', {}).get('S', '') if isinstance(item.get('category'), dict) else item.get('category', '')) == 'idv' or
        (item.get('award_or_idv_flag', {}).get('S', '') if isinstance(item.get('award_or_idv_flag'), dict) else item.get('award_or_idv_flag', '')) == 'IDV'
    )
    
    # Skip child awards
    if 'parent_idv_id' in item:
        return False, None
    
    if not is_idv_parent:
        return False, None
    
    # Get current total_obligated_amount (handle DynamoDB format)
    total_obligated_raw = item.get('total_obligated_amount')
    if isinstance(total_obligated_raw, dict):
        total_obligated = Decimal(str(total_obligated_raw.get('N', '0')))
    elif total_obligated_raw is None:
        total_obligated = None
    else:
        try:
            total_obligated = Decimal(str(total_obligated_raw))
        except (ValueError, TypeError):
            total_obligated = None
    
    # Get combined_obligated_amount (handle DynamoDB format)
    combined_obligated_raw = item.get('combined_obligated_amount')
    if isinstance(combined_obligated_raw, dict):
        combined_obligated = Decimal(str(combined_obligated_raw.get('N', '0')))
    elif combined_obligated_raw is None:
        combined_obligated = None
    else:
        try:
            combined_obligated = Decimal(str(combined_obligated_raw))
        except (ValueError, TypeError):
            combined_obligated = None
    
    # Need to update if:
    # 1. total_obligated_amount is missing, None, or 0
    # 2. combined_obligated_amount exists and is > 0
    needs_update = False
    
    if combined_obligated and combined_obligated > 0:
        if total_obligated is None or total_obligated == 0:
            needs_update = True
    
    return needs_update, combined_obligated

def update_idv_item(award_id: str, combined_obligated: Decimal) -> bool:
    """
    Update an IDV item with total_obligated_amount.
    Returns True if successful, False otherwise.
    """
    try:
        table.update_item(
            Key={'award_id': award_id},
            UpdateExpression='SET total_obligated_amount = :val',
            ExpressionAttributeValues={
                ':val': combined_obligated
            }
        )
        return True
    except Exception as e:
        log_print(f"  ERROR updating {award_id}: {e}")
        return False

def process_batch(items: list):
    """Process a batch of items"""
    for item in items:
        stats['total_scanned'] += 1
        award_id = item.get('award_id', {}).get('S', '') if isinstance(item.get('award_id'), dict) else item.get('award_id', 'Unknown')
        
        needs_update, combined_obligated = should_update_idv(item)
        
        if needs_update and combined_obligated:
            log_print(f"  Updating {award_id}: total_obligated_amount = {combined_obligated}")
            if update_idv_item(award_id, combined_obligated):
                stats['total_updated'] += 1
            else:
                stats['total_errors'] += 1
        else:
            stats['total_skipped'] += 1
        
        # Progress update every 100 items
        if stats['total_scanned'] % 100 == 0:
            log_print(f"  Progress: Scanned {stats['total_scanned']}, Updated {stats['total_updated']}, Skipped {stats['total_skipped']}, Errors {stats['total_errors']}")

# Scan DynamoDB table for IDV awards
log_print(f"\nScanning table for IDV parent awards...")
log_print(f"Filter: award_id starts with 'CONT_IDV'\n")

scan_kwargs = {
    'TableName': AWARDS_TABLE_NAME,
    'FilterExpression': 'begins_with(award_id, :prefix)',
    'ExpressionAttributeValues': {
        ':prefix': {'S': 'CONT_IDV'}
    },
    'Limit': 100  # Process in batches
}

try:
    while True:
        response = dynamodb.scan(**scan_kwargs)
        items = response.get('Items', [])
        
        if items:
            process_batch(items)
        
        # Check if there are more items to scan
        if 'LastEvaluatedKey' not in response:
            break
        
        scan_kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']
        
        # Progress update every 1000 items
        if stats['total_scanned'] % 1000 == 0:
            log_print(f"\nProgress Update:")
            log_print(f"  Scanned: {stats['total_scanned']}")
            log_print(f"  Updated: {stats['total_updated']}")
            log_print(f"  Skipped: {stats['total_skipped']}")
            log_print(f"  Errors: {stats['total_errors']}\n")

except Exception as e:
    log_print(f"\n\nERROR during scan: {e}")
    import traceback
    traceback.print_exc()
    raise

log_print(f"\n{'='*80}")
log_print(f"✅ Job Complete:")
log_print(f"  Total scanned: {stats['total_scanned']}")
log_print(f"  Total updated: {stats['total_updated']}")
log_print(f"  Total skipped: {stats['total_skipped']}")
log_print(f"  Total errors: {stats['total_errors']}")
log_print(f"{'='*80}")

job.commit()

