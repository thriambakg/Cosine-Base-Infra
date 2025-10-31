"""
Politician Trades Saver Lambda
Batch writes matched politician trades to DynamoDB with idempotency

This is Step 3 of the 3-step politician trades aggregation workflow.
"""

import json
import os
import logging
import boto3
from typing import List, Dict, Any
from datetime import datetime
from decimal import Decimal

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS clients
dynamodb = boto3.resource('dynamodb')

# Environment variables
DYNAMODB_TABLE_NAME = os.environ.get('DYNAMODB_TABLE_NAME')

def convert_to_dynamodb_format(item: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert Python types to DynamoDB-compatible types
    
    Args:
        item: Trade dict with Python types
        
    Returns:
        Dict with DynamoDB-compatible types (Decimal for numbers)
    """
    dynamodb_item = {}
    
    for key, value in item.items():
        if value is None:
            continue
        elif isinstance(value, (int, float)):
            dynamodb_item[key] = Decimal(str(value))
        elif isinstance(value, bool):
            dynamodb_item[key] = value
        elif isinstance(value, (str, list, dict)):
            dynamodb_item[key] = value
        else:
            dynamodb_item[key] = str(value)
    
    return dynamodb_item

def batch_write_trades(table, trades: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Batch write trades to DynamoDB with idempotency
    
    Args:
        table: DynamoDB table resource
        trades: List of trade dicts
        
    Returns:
        Dict with counts: {'saved': 45, 'skipped': 2, 'errors': 0}
    """
    saved = 0
    skipped = 0
    errors = 0
    
    # Process in batches of 25 (DynamoDB batch_write limit)
    batch_size = 25
    
    for i in range(0, len(trades), batch_size):
        batch = trades[i:i + batch_size]
        write_requests = []
        
        for trade in batch:
            # Convert to DynamoDB format
            dynamodb_item = convert_to_dynamodb_format(trade)
            
            # Add processing timestamp
            dynamodb_item['processingDate'] = Decimal(str(int(datetime.now().timestamp())))
            
            # Prepare put request with condition expression for idempotency
            write_requests.append({
                'PutRequest': {
                    'Item': dynamodb_item,
                    # ConditionExpression would go here, but batch_write doesn't support it
                    # We'll use individual put_item calls with ConditionExpression instead
                }
            })
        
        # Note: batch_write_item doesn't support ConditionExpression
        # So we'll use individual put_item with ConditionExpression for idempotency
        # For now, use batch_write and handle duplicates separately
        
        try:
            # Write batch
            response = table.batch_writer()
            for trade in batch:
                dynamodb_item = convert_to_dynamodb_format(trade)
                dynamodb_item['processingDate'] = Decimal(str(int(datetime.now().timestamp())))
                
                try:
                    # Use put_item with ConditionExpression for idempotency
                    table.put_item(
                        Item=dynamodb_item,
                        ConditionExpression='attribute_not_exists(tradeId)'
                    )
                    saved += 1
                except table.meta.client.exceptions.ConditionalCheckFailedException:
                    # Trade already exists - skip (idempotency)
                    skipped += 1
                    logger.debug(f"⏭️ Skipped duplicate trade: {trade.get('tradeId')}")
                except Exception as e:
                    logger.error(f"❌ Error saving trade {trade.get('tradeId')}: {e}")
                    errors += 1
            
            response.__exit__(None, None, None)
            
        except Exception as e:
            logger.error(f"❌ Error in batch write: {e}")
            errors += len(batch)
    
    return {
        'saved': saved,
        'skipped': skipped,
        'errors': errors
    }

def lambda_handler(event, context):
    """
    Lambda handler for saving matched trades to DynamoDB
    
    Expected input from Step 2:
    {
        "date": "2024-01-15",
        "matchedTrades": [...],
        "totalMatched": 45
    }
    
    Returns:
    {
        "date": "2024-01-15",
        "tradesSaved": 45,
        "tradesSkipped": 2,
        "errors": 0
    }
    """
    logger.info("🚀 Politician Trades Saver Lambda started")
    
    if not DYNAMODB_TABLE_NAME:
        raise ValueError("DYNAMODB_TABLE_NAME environment variable not set")
    
    # Get DynamoDB table
    table = dynamodb.Table(DYNAMODB_TABLE_NAME)
    
    # Get input from previous step
    match_results = event.get('matchResults') or event
    matched_trades = match_results.get('matchedTrades', [])
    date = match_results.get('date')
    
    logger.info(f"📅 Processing date: {date}")
    logger.info(f"💾 Saving {len(matched_trades)} matched trades to DynamoDB")
    
    if not matched_trades:
        logger.warning("⚠️ No matched trades to save")
        return {
            'statusCode': 200,
            'body': json.dumps({
                'date': date,
                'tradesSaved': 0,
                'tradesSkipped': 0,
                'errors': 0
            })
        }
    
    try:
        # Batch write trades with idempotency
        results = batch_write_trades(table, matched_trades)
        
        logger.info(f"✅ Saved {results['saved']} trades")
        logger.info(f"⏭️ Skipped {results['skipped']} duplicates")
        if results['errors'] > 0:
            logger.warning(f"⚠️ {results['errors']} errors during save")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'date': date,
                'tradesSaved': results['saved'],
                'tradesSkipped': results['skipped'],
                'errors': results['errors']
            })
        }
        
    except Exception as e:
        logger.error(f"❌ Fatal error in saver Lambda: {e}")
        raise

