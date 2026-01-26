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
from datetime import datetime
from typing import Dict, Any, Optional, Tuple, Set

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, isnan, isnull

import boto3
from boto3.dynamodb.conditions import Key

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

def query_gsi_for_zero_obligation(fiscal_year: int) -> Set[str]:
    """
    Query FiscalYearObligationIndex GSI for awards with total_obligated_amount = 0.
    Returns set of award_ids that start with CONT_IDV.
    """
    award_ids = set()
    
    try:
        # Query GSI with total_obligated_amount = 0
        # GSI structure: hash_key=fiscal_year, range_key=total_obligated_amount
        key_condition = Key('fiscal_year').eq(fiscal_year) & Key('total_obligated_amount').eq(Decimal('0'))
        
        query_kwargs = {
            'IndexName': 'FiscalYearObligationIndex',
            'KeyConditionExpression': key_condition,
            'ProjectionExpression': 'award_id',  # Only need award_id from GSI (KEYS_ONLY projection)
        }
        
        while True:
            response = table.query(**query_kwargs)
            items = response.get('Items', [])
            
            for item in items:
                # Handle both DynamoDB format and resource format
                if isinstance(item.get('award_id'), dict):
                    award_id = item.get('award_id', {}).get('S', '')
                else:
                    award_id = item.get('award_id', '')
                
                # Filter to only IDV parent awards
                if award_id and award_id.startswith('CONT_IDV'):
                    award_ids.add(award_id)
            
            # Check if there are more items
            if 'LastEvaluatedKey' not in response:
                break
            
            query_kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']
            
    except Exception as e:
        log_print(f"  ERROR querying GSI for fiscal_year {fiscal_year}: {e}")
        import traceback
        traceback.print_exc()
    
    return award_ids

def process_batch(award_ids: Set[str]):
    """Process a batch of award_ids by fetching full items and updating"""
    if not award_ids:
        return
    
    # Fetch full items in batches of 100 (DynamoDB BatchGetItem limit)
    award_ids_list = list(award_ids)
    batch_size = 100
    
    for i in range(0, len(award_ids_list), batch_size):
        batch_ids = award_ids_list[i:i + batch_size]
        
        try:
            # Batch get items
            response = dynamodb.batch_get_item(
                RequestItems={
                    AWARDS_TABLE_NAME: {
                        'Keys': [{'award_id': {'S': aid}} for aid in batch_ids]
                    }
                }
            )
            
            items = response.get('Responses', {}).get(AWARDS_TABLE_NAME, [])
            
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
        
        except Exception as e:
            log_print(f"  ERROR processing batch: {e}")
            stats['total_errors'] += len(batch_ids)

# Query GSI for awards with total_obligated_amount = 0
log_print(f"\nQuerying FiscalYearObligationIndex GSI for awards with total_obligated_amount = 0")
log_print(f"Filtering to awards starting with 'CONT_IDV'\n")

# Query multiple fiscal years (last 20 years should cover most data)
current_year = datetime.now().year
fiscal_years = list(range(current_year, current_year - 20, -1))

all_idv_award_ids = set()

for fiscal_year in fiscal_years:
    log_print(f"Querying fiscal year {fiscal_year}...")
    year_award_ids = query_gsi_for_zero_obligation(fiscal_year)
    all_idv_award_ids.update(year_award_ids)
    log_print(f"  Found {len(year_award_ids)} IDV awards with obligation = 0 (total so far: {len(all_idv_award_ids)})")

log_print(f"\nTotal unique IDV awards with obligation = 0: {len(all_idv_award_ids)}")
log_print(f"Processing awards...\n")

try:
    # Process all collected award_ids
    process_batch(all_idv_award_ids)

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

