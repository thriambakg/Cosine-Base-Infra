"""
Stock Data Processor Lambda
Processes stock data from SQS and stores in DynamoDB
"""

import json
import os
import logging
import boto3
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import time

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS clients
dynamodb = boto3.resource('dynamodb')
sqs = boto3.client('sqs')

# Configuration
MAX_BATCH_SIZE = 25  # DynamoDB batch write limit
RETRY_ATTEMPTS = 3
RETRY_DELAY = 1.0

def get_dynamodb_table():
    """Get DynamoDB table reference"""
    table_name = os.environ.get('DYNAMODB_TABLE_NAME')
    if not table_name:
        raise ValueError("DYNAMODB_TABLE_NAME environment variable not set")
    return dynamodb.Table(table_name)

def calculate_historical_metrics(stock_data: Dict[str, Any]) -> Dict[str, Any]:
    """Calculate historical metrics for different timeframes"""
    
    # For now, we'll use the current volatility for all timeframes
    # In a full implementation, you would calculate these from historical data
    current_volatility = stock_data.get('volatility', 0)
    
    # Simulate different timeframes with slight variations
    # In production, these would be calculated from actual historical data
    historical_metrics = {
        'volatility_1d': current_volatility * 0.8,  # Daily volatility typically lower
        'volatility_1w': current_volatility * 0.9,  # Weekly volatility
        'volatility_1mo': current_volatility,       # Monthly volatility (current)
        'volatility_3mo': current_volatility * 1.1, # Quarterly volatility
        'volatility_6mo': current_volatility * 1.2, # Semi-annual volatility
        'volatility_1y': current_volatility * 1.3,  # Annual volatility
        'volatility_2y': current_volatility * 1.4,  # 2-year volatility
        'volatility_5y': current_volatility * 1.5   # 5-year volatility
    }
    
    return historical_metrics

def create_dynamodb_item(stock_data: Dict[str, Any]) -> Dict[str, Any]:
    """Create DynamoDB item from stock data"""
    
    symbol = stock_data.get('symbol', '')
    timestamp = datetime.utcnow().isoformat()
    
    # Calculate historical metrics
    historical_metrics = calculate_historical_metrics(stock_data)
    
    # Create the main item
    item = {
        'PK': f'STOCK#{symbol}',
        'SK': 'CURRENT',
        'symbol': symbol,
        'current_price': stock_data.get('current_price', 0),
        'price_change': stock_data.get('price_change', 0),
        'price_change_percent': stock_data.get('price_change_percent', 0),
        'volatility': stock_data.get('volatility', 0),
        'market_cap': stock_data.get('market_cap', 0),
        'industry': stock_data.get('industry', 'Unknown'),
        'sector': stock_data.get('sector', 'Unknown'),
        'volume': stock_data.get('volume', 0),
        'pe_ratio': stock_data.get('pe_ratio', 0),
        'eps': stock_data.get('eps', 0),
        'dividend_yield': stock_data.get('dividend_yield', 0),
        'beta': stock_data.get('beta', 0),
        'last_updated': timestamp,
        'data_source': stock_data.get('data_source', 'unknown'),
        
        # GSI Keys for fast filtering
        'GSI1PK': f'INDUSTRY#{stock_data.get("industry", "Unknown")}',
        'GSI1SK': 'CURRENT',
        'GSI2PK': f'VOLATILITY#{stock_data.get("volatility_category", "MEDIUM")}',
        'GSI2SK': 'CURRENT',
        'GSI3PK': f'PRICE_CHANGE#{stock_data.get("price_change_category", "MEDIUM")}',
        'GSI3SK': 'CURRENT',
        'GSI4PK': f'MARKET_CAP#{stock_data.get("market_cap_category", "MEDIUM")}',
        'GSI4SK': 'CURRENT',
        'GSI5PK': f'PRICE#{stock_data.get("price_category", "MEDIUM")}',
        'GSI5SK': 'CURRENT',
        
        # Historical metrics
        **historical_metrics,
        
        # TTL for automatic cleanup (optional - set to 30 days from now)
        'expires_at': int((datetime.utcnow() + timedelta(days=30)).timestamp())
    }
    
    return item

def batch_write_to_dynamodb(table, items: List[Dict[str, Any]]) -> bool:
    """Write items to DynamoDB in batches"""
    try:
        # DynamoDB batch_write_item can handle up to 25 items
        with table.batch_writer() as batch:
            for item in items:
                batch.put_item(Item=item)
        
        logger.info(f"Successfully wrote {len(items)} items to DynamoDB")
        return True
        
    except Exception as e:
        logger.error(f"Error writing to DynamoDB: {str(e)}")
        return False

def process_sqs_messages(queue_url: str) -> int:
    """Process messages from SQS queue"""
    processed_count = 0
    
    try:
        # Receive messages from SQS
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,  # Maximum messages per batch
            WaitTimeSeconds=20,      # Long polling
            MessageAttributeNames=['All']
        )
        
        messages = response.get('Messages', [])
        if not messages:
            logger.info("No messages to process")
            return 0
        
        logger.info(f"Processing {len(messages)} messages from SQS")
        
        # Get DynamoDB table
        table = get_dynamodb_table()
        
        # Process messages in batches
        items_to_write = []
        
        for message in messages:
            try:
                # Parse message body
                message_body = json.loads(message['Body'])
                
                # Create DynamoDB item
                dynamodb_item = create_dynamodb_item(message_body)
                items_to_write.append(dynamodb_item)
                
                # Delete message from SQS after successful processing
                sqs.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=message['ReceiptHandle']
                )
                
                processed_count += 1
                
            except Exception as e:
                logger.error(f"Error processing message: {str(e)}")
                continue
        
        # Write items to DynamoDB in batches
        if items_to_write:
            # Split into batches of 25 (DynamoDB limit)
            for i in range(0, len(items_to_write), MAX_BATCH_SIZE):
                batch = items_to_write[i:i + MAX_BATCH_SIZE]
                if batch_write_to_dynamodb(table, batch):
                    logger.info(f"Successfully processed batch of {len(batch)} items")
                else:
                    logger.error(f"Failed to process batch of {len(batch)} items")
        
        return processed_count
        
    except Exception as e:
        logger.error(f"Error processing SQS messages: {str(e)}")
        return 0

def lambda_handler(event, context):
    """Main Lambda handler"""
    try:
        logger.info(f"Stock Data Processor started: {json.dumps(event)}")
        
        # Get SQS queue URL from environment
        queue_url = os.environ.get('SQS_QUEUE_URL')
        if not queue_url:
            raise ValueError("SQS_QUEUE_URL environment variable not set")
        
        # Process messages from SQS
        processed_count = process_sqs_messages(queue_url)
        
        logger.info(f"Successfully processed {processed_count} stock data records")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Stock data processing completed',
                'processed_count': processed_count
            })
        }
        
    except Exception as e:
        logger.error(f"Error in stock data processor: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'message': 'Stock data processing failed'
            })
        }
