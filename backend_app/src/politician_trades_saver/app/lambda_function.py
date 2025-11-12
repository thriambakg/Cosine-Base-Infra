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
    
    GSIs:
    - PoliticianTradeDateIndex: hash_key=politicianName, range_key=transactionDate
    - PositionTradeDateIndex: hash_key=position, range_key=transactionDate
    - PartyTradeDateIndex: hash_key=party, range_key=transactionDate
    - SecurityTradeDateIndex: hash_key=securitySymbol, range_key=transactionDate
    - FormTypeTradeDateIndex: hash_key=formType, range_key=transactionDate
    - TransactionTypeTradeDateIndex: hash_key=transactionType, range_key=transactionDate
    - AmountRangeTradeDateIndex: hash_key=amountMin, range_key=transactionDate
    - StateDistrictTradeDateIndex: hash_key=stateDistrict, range_key=transactionDate
    
    Note: GSI hash keys cannot be null. If securitySymbol or amountMin is null, we exclude it
    so the item won't appear in those GSIs.
    
    Args:
        item: Trade dict with Python types (includes websiteUrl as a string attribute, not GSI)
        
    Returns:
        Dict with DynamoDB-compatible types (Decimal for numbers, strings preserved)
    """
    dynamodb_item = {}
    
    # GSI hash keys that cannot be null
    # Note: amountMin is numeric, others are strings
    gsi_hash_keys_string = ['politicianName', 'party', 'position', 'securitySymbol', 'formType', 'transactionType', 'stateDistrict']
    gsi_hash_keys_numeric = ['amountMin']
    
    # Extract amountMin from amountRange if present
    # amountRange is a list [min, max], we need amountMin as a number for the GSI
    if 'amountRange' in item and isinstance(item.get('amountRange'), list) and len(item.get('amountRange', [])) > 0:
        amount_range = item.get('amountRange')
        if amount_range[0] is not None:
            # Validate the value is numeric before using it
            try:
                # Try to convert to float to validate it's a number
                min_val = float(amount_range[0])
                # Check for NaN or Inf
                if min_val != min_val or min_val == float('inf') or min_val == float('-inf'):
                    logger.warning(f"⚠️ Invalid amountRange[0] value: {amount_range[0]} (NaN or Inf)")
                else:
                    # Ensure amountMin is set from amountRange if not already present
                    if 'amountMin' not in item or item.get('amountMin') is None:
                        item['amountMin'] = amount_range[0]
            except (ValueError, TypeError) as e:
                logger.warning(f"⚠️ Invalid amountRange[0] value: {amount_range[0]} - {e}")
    
    for key, value in item.items():
        if value is None:
            # For GSI hash keys, skip null values (item won't appear in that GSI)
            # For other fields, just skip them
            if key in gsi_hash_keys_string or key in gsi_hash_keys_numeric:
                logger.debug(f"⚠️ Skipping null GSI hash key '{key}' - item won't appear in {key} GSI")
            continue
        elif isinstance(value, bool):
            dynamodb_item[key] = value
        elif isinstance(value, (int, float)):
            # Handle special float values (NaN, Inf) that can't be converted to Decimal
            if isinstance(value, float) and (value != value or value == float('inf') or value == float('-inf')):
                logger.warning(f"⚠️ Skipping invalid numeric value for '{key}': {value} (NaN or Inf)")
                continue
            try:
                # Convert to string first, then to Decimal
                value_str = str(value)
                # Check for invalid string representations
                if value_str.lower() in ['none', 'null', 'nan', 'inf', '-inf', '']:
                    logger.warning(f"⚠️ Skipping invalid numeric value for '{key}': '{value_str}'")
                    continue
                dynamodb_item[key] = Decimal(value_str)
            except (ValueError, TypeError, Exception) as e:
                logger.error(f"❌ Error converting '{key}' to Decimal: value={value} (type={type(value).__name__}), error={type(e).__name__}: {e}")
                # Skip this field rather than failing the entire trade
                continue
        elif isinstance(value, str):
            # Check if string represents a number (for edge cases)
            if value.strip().lower() in ['none', 'null', 'nan', 'inf', '-inf', '']:
                # Skip invalid string representations
                if key in gsi_hash_keys_string or key in gsi_hash_keys_numeric:
                    logger.debug(f"⚠️ Skipping invalid string value for GSI hash key '{key}': '{value}'")
                continue
            # Ensure GSI hash keys are non-empty strings
            if key in gsi_hash_keys_string and not value.strip():
                logger.debug(f"⚠️ Skipping empty GSI hash key '{key}' - item won't appear in {key} GSI")
                continue
            dynamodb_item[key] = value
        elif isinstance(value, list):
            # Convert list elements to DynamoDB-compatible types (e.g., amountRange)
            # DynamoDB lists can contain Decimal values, so convert numeric elements
            converted_list = []
            for item in value:
                if item is None:
                    converted_list.append(None)
                elif isinstance(item, (int, float)):
                    # Handle special float values (NaN, Inf)
                    if isinstance(item, float) and (item != item or item == float('inf') or item == float('-inf')):
                        logger.warning(f"⚠️ Skipping invalid numeric value in list: {item} (NaN or Inf)")
                        continue
                    try:
                        converted_list.append(Decimal(str(item)))
                    except (ValueError, TypeError) as e:
                        logger.warning(f"⚠️ Error converting list element to Decimal: {item} - {e}")
                        converted_list.append(item)  # Keep original if conversion fails
                else:
                    converted_list.append(item)
            dynamodb_item[key] = converted_list
        elif isinstance(value, dict):
            dynamodb_item[key] = value
        else:
            # Try to convert to string, but skip if it's None or invalid
            try:
                dynamodb_item[key] = str(value)
            except Exception as e:
                logger.warning(f"⚠️ Error converting '{key}' to string: {type(value).__name__} - {e}")
                continue
    
    # Validate required GSI fields are present (securitySymbol and amountMin are optional)
    required_gsi_fields = ['politicianName', 'party', 'position', 'formType', 'transactionType', 'transactionDate']
    missing_fields = [field for field in required_gsi_fields if field not in dynamodb_item]
    if missing_fields:
        logger.warning(f"⚠️ Missing required GSI fields: {missing_fields}")
    
    return dynamodb_item

def batch_write_trades(table, trades: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Batch write trades to DynamoDB
    
    All transactions are saved, even if they appear in multiple sources (SEC vs Senate).
    The source field distinguishes filings, and table indexing handles any duplicates.
    
    Args:
        table: DynamoDB table resource
        trades: List of trade dicts
        
    Returns:
        Dict with counts: {'saved': 45, 'errors': 0}
    """
    saved = 0
    errors = 0
    
    # Process in batches of 25 (DynamoDB batch_write limit)
    batch_size = 25
    
    for i in range(0, len(trades), batch_size):
        batch = trades[i:i + batch_size]
        
        # Process each trade individually for better error handling
        for trade in batch:
            try:
                # Convert to DynamoDB format
                dynamodb_item = convert_to_dynamodb_format(trade)
                
                # Add processing timestamp
                dynamodb_item['processingDate'] = Decimal(str(int(datetime.now().timestamp())))
                
                # Save all transactions - each transaction should be saved even if it appears in multiple sources
                # The source field distinguishes SEC vs Senate filings, and table indexing handles duplicates
                table.put_item(Item=dynamodb_item)
                saved += 1
            except Exception as e:
                trade_id = trade.get('tradeId', 'unknown')
                logger.error(f"❌ Error saving trade {trade_id}: {type(e).__name__}: {e}")
                logger.error(f"   Trade data: {json.dumps(trade, default=str)[:500]}")  # Log first 500 chars of trade
                errors += 1
    
    return {
        'saved': saved,
        'errors': errors
    }

def lambda_handler(event, context):
    """
    Lambda handler for saving matched trades to DynamoDB
    
    All transactions are saved, even if they appear in multiple sources (SEC vs Senate).
    The source field distinguishes filings, and table indexing handles any duplicates.
    
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
        "errors": 0
    }
    """
    logger.info("🚀 Politician Trades Saver Lambda started")
    
    if not DYNAMODB_TABLE_NAME:
        raise ValueError("DYNAMODB_TABLE_NAME environment variable not set")
    
    # Get DynamoDB table
    table = dynamodb.Table(DYNAMODB_TABLE_NAME)
    
    # Get input from previous step
    # Support both old format (matchResults) and new format (aggregateResults)
    match_results = event.get('aggregateResults') or event.get('matchResults') or event
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
                'errors': 0
            })
        }
    
    try:
        # Batch write all trades - each transaction is saved even if it appears in multiple sources
        results = batch_write_trades(table, matched_trades)
        
        logger.info(f"✅ Saved {results['saved']} trades")
        if results['errors'] > 0:
            logger.warning(f"⚠️ {results['errors']} errors during save")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'date': date,
                'tradesSaved': results['saved'],
                'errors': results['errors']
            })
        }
        
    except Exception as e:
        logger.error(f"❌ Fatal error in saver Lambda: {e}")
        raise

