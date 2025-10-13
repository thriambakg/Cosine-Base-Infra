"""
EOD Batch Generator Lambda
Generates batches of stock symbols from S3 for the EOD aggregator to process.
This runs once per day after market close to prepare batches for parallel processing.
"""

import json
import os
import logging
import boto3
from typing import List, Dict, Any
import pandas_market_calendars as mcal
import pytz
from datetime import datetime

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS clients
s3_client = boto3.client('s3')

# Environment variables
S3_BUCKET = os.environ.get('S3_BUCKET')
BATCH_SIZE = int(os.environ.get('BATCH_SIZE', '200'))


def is_trading_day() -> bool:
    """
    Check if today is a valid NYSE trading day (excludes weekends and holidays).
    
    Returns:
        True if market is open, False if closed (holiday or weekend)
    """
    try:
        nyse = mcal.get_calendar('NYSE')
        et_tz = pytz.timezone('America/New_York')
        today = datetime.now(et_tz).date()
        
        # Check if today is in the NYSE trading schedule
        schedule = nyse.schedule(start_date=today, end_date=today)
        
        is_open = not schedule.empty
        
        if is_open:
            logger.info(f"✅ Today ({today}) is a trading day")
        else:
            logger.info(f"🎄 Today ({today}) is NOT a trading day (market holiday or weekend)")
        
        return is_open
        
    except Exception as e:
        # If check fails, assume it's a trading day (fail-safe to avoid missing updates)
        logger.warning(f"Could not verify trading day status: {e}, assuming market is open")
        return True


def list_all_stocks_from_s3() -> Dict[str, List[str]]:
    """
    List all stock JSON files from S3 bucket organized by priority.
    
    Returns:
        Dict with priority tiers as keys and lists of symbols as values
    """
    stocks_by_priority = {
        'high': [],
        'medium': [],
        'low': []
    }
    
    try:
        for priority in ['high', 'medium', 'low']:
            prefix = f"stock-data/{priority}/"
            logger.info(f"Listing stocks from s3://{S3_BUCKET}/{prefix}")
            
            paginator = s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix)
            
            for page in pages:
                if 'Contents' not in page:
                    continue
                
                for obj in page['Contents']:
                    key = obj['Key']
                    # Extract symbol from filename (e.g., "stock-data/high/AAPL.json" -> "AAPL")
                    if key.endswith('.json'):
                        symbol = key.split('/')[-1].replace('.json', '')
                        stocks_by_priority[priority].append(symbol)
            
            logger.info(f"✅ Found {len(stocks_by_priority[priority])} {priority}-priority stocks")
    
    except Exception as e:
        logger.error(f"Error listing S3 objects: {e}")
        raise
    
    return stocks_by_priority


def generate_batches(stocks_by_priority: Dict[str, List[str]], batch_size: int = 200) -> List[Dict[str, Any]]:
    """
    Generate batches of stocks for parallel processing.
    
    Args:
        stocks_by_priority: Dict of priority -> list of symbols
        batch_size: Number of stocks per batch
        
    Returns:
        List of batch configurations
    """
    batches = []
    batch_number = 1
    
    # Process each priority tier
    for priority in ['high', 'medium', 'low']:
        symbols = stocks_by_priority[priority]
        
        # Split into batches
        for i in range(0, len(symbols), batch_size):
            batch_symbols = symbols[i:i + batch_size]
            
            batches.append({
                'symbols': batch_symbols,
                'priority': priority,
                'batch_number': batch_number,
                'total_in_batch': len(batch_symbols)
            })
            
            batch_number += 1
    
    logger.info(f"✅ Generated {len(batches)} batches from {sum(len(s) for s in stocks_by_priority.values())} total stocks")
    
    return batches


def lambda_handler(event, context):
    """
    Main Lambda handler - generates batches for EOD aggregator Step Functions.
    
    Returns:
        Batch configuration for Step Functions Map state
    """
    try:
        logger.info("=== EOD Batch Generator Started ===")
        logger.info(f"Event: {json.dumps(event, default=str)}")
        
        # List all stocks from S3
        stocks_by_priority = list_all_stocks_from_s3()
        
        total_stocks = sum(len(symbols) for symbols in stocks_by_priority.values())
        logger.info(f"Found {total_stocks} total stocks across all priority tiers")
        
        # Generate batches
        batches = generate_batches(stocks_by_priority, BATCH_SIZE)
        
        # Return batch configuration for Step Functions
        result = {
            'batches': batches,
            'total_batches': len(batches),
            'total_stocks': total_stocks,
            'batch_size': BATCH_SIZE,
            'generated_at': event.get('time', 'unknown')
        }
        
        logger.info(f"=== Batch Generation Complete ===")
        logger.info(f"Total batches: {len(batches)}, Total stocks: {total_stocks}")
        
        return result
        
    except Exception as e:
        logger.error(f"Error in EOD batch generator: {str(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        
        raise Exception(f"EOD batch generation failed: {str(e)}")

