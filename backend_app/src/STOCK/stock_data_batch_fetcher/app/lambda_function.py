"""
Stock Data Batch Fetcher Lambda
Loads stock symbols from CSVs, creates batches based on priority tier, and sends to SQS for processing
"""

import json
import os
import logging
import boto3
import csv
from typing import Dict, List, Any
from datetime import datetime
import pandas_market_calendars as mcal
import pytz

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS clients
sqs = boto3.client('sqs')

# Configuration from environment variables
BATCH_SIZE_HIGH = int(os.environ.get('BATCH_SIZE_HIGH', '100'))
BATCH_SIZE_MEDIUM = int(os.environ.get('BATCH_SIZE_MEDIUM', '75'))
BATCH_SIZE_LOW = int(os.environ.get('BATCH_SIZE_LOW', '50'))
SQS_QUEUE_URL = os.environ.get('SQS_QUEUE_URL')

# Module-level cache for stock symbols (persists across Lambda invocations)
_fortune500_symbols = None
_midcap_symbols = None
_all_symbols = None


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


def load_highcap_symbols() -> List[str]:
    """Load high-cap stock symbols from CSV (HIGH priority)"""
    global _fortune500_symbols
    
    if _fortune500_symbols is not None:
        return _fortune500_symbols
    
    try:
        symbols = []
        csv_path = os.path.join(os.path.dirname(__file__), 'highcap.csv')
        
        if not os.path.exists(csv_path):
            logger.warning(f"High-cap CSV not found at {csv_path}")
            return []
        
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                symbol = row.get('Symbol', '').strip().upper()
                if symbol and symbol.replace('.', '').isalnum():
                    symbols.append(symbol)
        
        _fortune500_symbols = symbols
        logger.info(f"✅ Loaded {len(symbols)} high-cap symbols")
        return symbols
        
    except Exception as e:
        logger.error(f"Error loading high-cap symbols: {str(e)}")
        return []

def load_midcap_symbols() -> List[str]:
    """Load Mid-cap stock symbols from CSV (MEDIUM priority)"""
    global _midcap_symbols
    
    if _midcap_symbols is not None:
        return _midcap_symbols
    
    try:
        symbols = []
        csv_path = os.path.join(os.path.dirname(__file__), 'midcap.csv')
        
        if not os.path.exists(csv_path):
            logger.warning(f"Mid-cap CSV not found at {csv_path}")
            return []
        
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                symbol = row.get('Symbol', '').strip().upper()
                if symbol and symbol.replace('.', '').isalnum():
                    symbols.append(symbol)
        
        _midcap_symbols = symbols
        logger.info(f"✅ Loaded {len(symbols)} mid-cap symbols")
        return symbols
        
    except Exception as e:
        logger.error(f"Error loading mid-cap symbols: {str(e)}")
        return []

def load_all_symbols() -> List[str]:
    """
    Load all available stock symbols from lowcap.csv (LOW priority).
    This file already excludes Fortune 500 and Mid-cap stocks and is deduplicated.
    """
    global _all_symbols
    
    if _all_symbols is not None:
        return _all_symbols
    
    try:
        symbols = []
        csv_path = os.path.join(os.path.dirname(__file__), 'lowcap.csv')
        
        if not os.path.exists(csv_path):
            logger.warning(f"Low-cap CSV not found at {csv_path}")
            return []
        
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                symbol = row.get('Symbol', '').strip().upper()
                if symbol and symbol.replace('.', '').isalnum():
                    symbols.append(symbol)
        
        _all_symbols = symbols
        logger.info(f"✅ Loaded {len(symbols)} low-cap symbols")
        return symbols
        
    except Exception as e:
        logger.error(f"Error loading low-cap symbols: {str(e)}")
        return []

def create_batches(symbols: List[str], priority_tier: str, timeframe: str) -> List[Dict[str, Any]]:
    """
    Create SQS message batches from symbol list.
    
    Args:
        symbols: List of stock symbols
        priority_tier: Priority tier ('high', 'medium', 'low')
        timeframe: Time period ('1d', '7d', '30d', '1y')
    
    Returns:
        List of batch dictionaries ready for SQS
    """
    try:
        # Get batch size based on priority
        batch_sizes = {
            'high': BATCH_SIZE_HIGH,
            'medium': BATCH_SIZE_MEDIUM,
            'low': BATCH_SIZE_LOW
        }
        batch_size = batch_sizes.get(priority_tier, BATCH_SIZE_MEDIUM)
        
        batches = []
        total_batches = (len(symbols) + batch_size - 1) // batch_size
        
        for i in range(0, len(symbols), batch_size):
            batch_symbols = symbols[i:i + batch_size]
            
            batch = {
                'symbols': batch_symbols,
                'priority': priority_tier,
                'timeframe': timeframe,
                'batch_number': i // batch_size,
                'total_batches': total_batches,
                'batch_size': len(batch_symbols),
                'timestamp': datetime.utcnow().isoformat()
            }
            batches.append(batch)
        
        logger.info(f"✅ Created {len(batches)} batches (size: {batch_size}) for {priority_tier} priority")
        return batches
    except Exception as e:
        logger.error(f"Error creating batches: {str(e)}")
        return []

def send_batches_to_sqs(batches: List[Dict[str, Any]], queue_url: str) -> int:
    """
    Send batches to SQS queue.
    
    Args:
        batches: List of batch dictionaries
        queue_url: SQS queue URL
    
    Returns:
        Number of successfully sent batches
    """
    try:
        sent_count = 0
        
        for i, batch in enumerate(batches):
            try:
                # Send message to SQS
        response = sqs.send_message(
            QueueUrl=queue_url,
                    MessageBody=json.dumps(batch),
            MessageAttributes={
                        'priority': {
                            'StringValue': batch['priority'],
                    'DataType': 'String'
                },
                        'timeframe': {
                            'StringValue': batch['timeframe'],
                    'DataType': 'String'
                }
            }
        )
        
                sent_count += 1
                
                if (i + 1) % 10 == 0:  # Log every 10 batches
                    logger.info(f"📤 Sent {i + 1}/{len(batches)} batches to SQS")
        
    except Exception as e:
                logger.error(f"Error sending batch {i} to SQS: {str(e)}")
                continue
        
        logger.info(f"✅ Successfully sent {sent_count}/{len(batches)} batches to SQS")
        return sent_count
        
    except Exception as e:
        logger.error(f"Error sending batches to SQS: {str(e)}")
        return 0

def lambda_handler(event, context):
    """
    Main Lambda handler triggered by EventBridge Scheduler.
    
    Expected event format:
    {
        "priority_tier": "high" | "medium" | "low",
        "timeframe": "1d" | "7d" | "30d" | "1y"
    }
    
    This function will process ALL stocks for the given priority tier by creating
    multiple SQS batches internally (no pagination needed).
    """
    try:
        logger.info(f"=== Stock Data Batch Fetcher Started ===")
        logger.info(f"Event: {json.dumps(event, default=str)}")
        logger.info(f"Environment: BATCH_SIZE_HIGH={BATCH_SIZE_HIGH}, MEDIUM={BATCH_SIZE_MEDIUM}, LOW={BATCH_SIZE_LOW}")
        
        # Check if today is a trading day (skip on holidays/weekends)
        # Allow bypass for manual testing
        bypass_holiday_check = event.get('bypass_holiday_check', False)
        
        if not bypass_holiday_check and not is_trading_day():
            logger.info("Market is closed today (holiday or weekend), skipping batch generation")
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Market closed - no batches sent',
                    'reason': 'Not a trading day'
                })
            }
        
        if bypass_holiday_check:
            logger.info("⚠️ Bypass flag set - running batch generation even though market may be closed")
        
        # Extract parameters from event
        priority_tier = event.get('priority_tier', 'medium')
        timeframe = event.get('timeframe', '1d')
        
        logger.info(f"Parameters: priority={priority_tier}, timeframe={timeframe}")
        
        # Validate SQS queue URL
        if not SQS_QUEUE_URL:
            raise ValueError("SQS_QUEUE_URL environment variable not set")
        
        # Load ALL symbols for this priority tier (no pagination)
        if priority_tier == 'high':
            symbols = load_highcap_symbols()
        elif priority_tier == 'medium':
            symbols = load_midcap_symbols()
        elif priority_tier == 'low':
            symbols = load_all_symbols()
        else:
            logger.error(f"Unknown priority tier: {priority_tier}")
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': f'Invalid priority tier: {priority_tier}',
                    'message': 'Priority tier must be high, medium, or low'
                })
            }
        
        if not symbols:
            logger.warning(f"No symbols found for priority {priority_tier}")
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'No symbols to process',
                    'priority': priority_tier,
                    'timeframe': timeframe
                })
            }
        
        logger.info(f"📊 Processing {len(symbols)} symbols for {priority_tier} priority")
        
        # Create batches for SQS (this will create multiple batches to cover all symbols)
        batches = create_batches(symbols, priority_tier, timeframe)
        
        if not batches:
            logger.warning("No batches created")
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'No batches created',
                    'symbols_count': len(symbols)
                })
            }
        
        # Send all batches to SQS
        sent_count = send_batches_to_sqs(batches, SQS_QUEUE_URL)
        
        logger.info(f"=== Stock Data Batch Fetcher Completed ===")
        logger.info(f"Summary: {len(symbols)} symbols, {len(batches)} batches, {sent_count} sent to SQS")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Stock data batch fetcher completed successfully',
                'priority': priority_tier,
                'timeframe': timeframe,
                'symbols_count': len(symbols),
                'batches_created': len(batches),
                'batches_sent': sent_count,
                'timestamp': datetime.utcnow().isoformat()
            })
        }
        
    except Exception as e:
        logger.error(f"Error in stock data batch fetcher: {str(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'message': 'Stock data batch fetcher failed'
            })
        }
