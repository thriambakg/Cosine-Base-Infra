"""
EOD (End of Day) Aggregator Lambda
Reads historical stock data from S3, calculates metrics for multiple timeframes,
and writes aggregated data to DynamoDB for fast querying.

Runs daily after market close via Step Functions for parallel batch processing.
"""

import json
import os
import logging
import boto3
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from decimal import Decimal
import numpy as np

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS clients
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

# Environment variables
S3_BUCKET = os.environ.get('S3_BUCKET')
DYNAMODB_TABLE_NAME = os.environ.get('DYNAMODB_TABLE_NAME')


def get_dynamodb_table():
    """Get DynamoDB table reference"""
    if not DYNAMODB_TABLE_NAME:
        raise ValueError("DYNAMODB_TABLE_NAME environment variable not set")
    return dynamodb.Table(DYNAMODB_TABLE_NAME)


def read_stock_from_s3(symbol: str, priority: str) -> Optional[Dict[str, Any]]:
    """
    Read stock historical data from S3.
    
    Args:
        symbol: Stock ticker symbol
        priority: Priority tier ('high', 'medium', 'low')
        
    Returns:
        Stock data dict or None if not found
    """
    try:
        s3_key = f"historical/{priority}/{symbol}.json"
        
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
        stock_data = json.loads(response['Body'].read().decode('utf-8'))
        
        return stock_data
        
    except s3_client.exceptions.NoSuchKey:
        logger.warning(f"Stock file not found: {symbol} (priority: {priority})")
        return None
    except Exception as e:
        logger.error(f"Error reading S3 file for {symbol}: {e}")
        return None


def calculate_metrics_for_timeframe(history: List[Dict[str, Any]], timeframe: str) -> Dict[str, Any]:
    """
    Calculate metrics for a specific timeframe from historical data.
    Handles mixed-frequency data (daily + intraday) by normalizing volatility.
    
    Args:
        history: List of historical data points (sorted oldest to newest)
        timeframe: '1d', '7d', '30d', or '1y'
        
    Returns:
        Dict with calculated metrics
    """
    try:
        if not history or len(history) < 2:
            return get_default_metrics()
        
        # Map timeframe to number of calendar days
        days_map = {
            '1d': 1,
            '7d': 7,
            '30d': 30,
            '1y': 365  # Calendar days (not trading days)
        }
        
        lookback_days = days_map.get(timeframe, 1)
        
        # Get the most recent data point
        current_point = history[-1]
        current_price = current_point.get('close', 0)
        current_volume = current_point.get('volume', 0)
        current_market_cap = current_point.get('market_cap', 0)
        
        # Filter history by DATE (not by count) to get the correct timeframe
        from datetime import datetime, timedelta
        
        try:
            current_date = datetime.fromisoformat(current_point.get('date', '').replace('Z', '+00:00'))
        except:
            current_date = datetime.utcnow()
        
        cutoff_date = current_date - timedelta(days=lookback_days)
        
        # Get all data points within the timeframe (by date, not count)
        relevant_history = [p for p in history if datetime.fromisoformat(p.get('date', '').replace('Z', '+00:00')) >= cutoff_date]
        
        # Debug logging for 1d timeframe
        if timeframe == '1d':
            logger.info(f"📊 1d timeframe debug: current_date={current_date}, cutoff_date={cutoff_date}")
            logger.info(f"📊 Total history points: {len(history)}, Relevant points: {len(relevant_history)}")
            if len(history) > 0:
                first_date = history[0].get('date', 'N/A')
                last_date = history[-1].get('date', 'N/A')
                logger.info(f"📊 History date range: {first_date} to {last_date}")
        
        if len(relevant_history) < 2:
            logger.warning(f"⚠️ Not enough data for {timeframe}: found {len(relevant_history)} points (need 2+)")
            return get_default_metrics()
        
        # Get the oldest point in this timeframe for price change calculation
        previous_point = relevant_history[0]
        previous_close = previous_point.get('close', current_price)
        
        # Calculate price change
        price_change = current_price - previous_close
        price_change_percent = ((current_price - previous_close) / previous_close * 100) if previous_close > 0 else 0
        
        # Extract prices for volatility calculation
        prices = [point.get('close', 0) for point in relevant_history if point.get('close', 0) > 0]
        timestamps = [point.get('timestamp', 0) for point in relevant_history if point.get('close', 0) > 0]
        
        # Calculate volatility with frequency normalization
        volatility = 0.0
        if len(prices) > 1 and len(timestamps) > 1:
            log_returns = []
            time_deltas = []
            
            for i in range(1, len(prices)):
                if prices[i-1] > 0:
                    log_return = np.log(prices[i] / prices[i-1])
                    log_returns.append(log_return)
                    
                    # Calculate time between observations in days
                    if timestamps[i] > timestamps[i-1]:
                        time_delta_days = (timestamps[i] - timestamps[i-1]) / 86400  # seconds to days
                        time_deltas.append(time_delta_days)
            
            if log_returns and time_deltas:
                # Calculate average sampling frequency (observations per day)
                avg_time_delta = np.mean(time_deltas)
                observations_per_day = 1.0 / avg_time_delta if avg_time_delta > 0 else 1.0
                
                # Standard deviation of log returns
                std_log_returns = np.std(log_returns)
                
                # Annualize: std * sqrt(observations_per_day * 252 trading days)
                # This normalizes regardless of whether we have hourly, daily, or mixed data
                volatility = float(std_log_returns * np.sqrt(observations_per_day * 252))
                
                logger.info(f"Volatility calc: {len(prices)} points over {lookback_days} days, avg_delta={avg_time_delta:.2f} days, obs/day={observations_per_day:.2f}")
        
        # Calculate week return (7 days back, regardless of data frequency)
        week_return = 0.0
        week_cutoff = current_date - timedelta(days=7)
        week_history = [p for p in history if datetime.fromisoformat(p.get('date', '').replace('Z', '+00:00')) >= week_cutoff]
        
        if len(week_history) >= 2:
            week_ago_price = week_history[0].get('close', 0)
            if week_ago_price > 0:
                week_total_return = (current_price - week_ago_price) / week_ago_price
                week_return = week_total_return * (252 / 7) * 100  # Annualized percentage
        else:
            # For short timeframes, approximate from price change
            total_return = ((current_price - previous_close) / previous_close) if previous_close > 0 else 0
            week_return = total_return * (252 / max(lookback_days, 1)) * 100 / 52
        
        # Calculate average volume (across all points in timeframe)
        volumes = [point.get('volume', 0) for point in relevant_history if point.get('volume', 0) > 0]
        avg_volume = int(np.mean(volumes)) if volumes else current_volume
        
        # Get day high/low (max/min from last 24 hours of data)
        day_cutoff = current_date - timedelta(days=1)
        day_points = [p for p in history if datetime.fromisoformat(p.get('date', '').replace('Z', '+00:00')) >= day_cutoff]
        day_high = max((p.get('high', 0) for p in day_points), default=current_price) if day_points else current_price
        day_low = min((p.get('low', current_price) for p in day_points if p.get('low', 0) > 0), default=current_price) if day_points else current_price
        
        # Get year high/low (from last 365 calendar days)
        year_cutoff = current_date - timedelta(days=365)
        year_history = [p for p in history if datetime.fromisoformat(p.get('date', '').replace('Z', '+00:00')) >= year_cutoff]
        year_high = max((p.get('high', 0) for p in year_history), default=current_price) if year_history else current_price
        year_low = min((p.get('low', current_price) for p in year_history if p.get('low', 0) > 0), default=current_price) if year_history else current_price
        
        return {
            'current_price': current_price,
            'previous_close': previous_close,
            'price_change': price_change,
            'price_change_percent': price_change_percent,
            'week_return': week_return,
            'volatility': volatility,
            'volume': current_volume,
            'avg_volume': avg_volume,
            'market_cap': current_market_cap,
            'day_high': day_high,
            'day_low': day_low,
            'year_high': year_high,
            'year_low': year_low
        }
        
    except Exception as e:
        logger.error(f"Error calculating metrics for timeframe {timeframe}: {e}")
        return get_default_metrics()


def get_default_metrics() -> Dict[str, Any]:
    """Return default metrics when calculation fails"""
    return {
        'current_price': 0,
        'previous_close': 0,
        'price_change': 0,
        'price_change_percent': 0,
        'week_return': 0,
        'annual_return': 0,
        'volatility': 0,
        'volume': 0,
        'avg_volume': 0,
        'market_cap': 0,
        'day_high': 0,
        'day_low': 0,
        'year_high': 0,
        'year_low': 0
    }


def create_dynamodb_item(symbol: str, stock_data: Dict[str, Any], metrics: Dict[str, Any], timeframe: str) -> Dict[str, Any]:
    """
    Create DynamoDB item with GSI structure for efficient querying.
    
    Args:
        symbol: Stock ticker symbol
        stock_data: Full stock data from S3
        metrics: Calculated metrics for this timeframe
        timeframe: Time period ('1d', '7d', '30d', '1y')
        
    Returns:
        DynamoDB item dict
    """
    # Extract metadata from S3 data
    industry = stock_data.get('industry', 'Unknown')
    sector = stock_data.get('sector', 'Unknown')
    company_name = stock_data.get('company_name', symbol)
    shares_outstanding = stock_data.get('shares_outstanding', 0)
    pe_ratio = stock_data.get('pe_ratio', 0)
    dividend_yield = stock_data.get('dividend_yield', 0)
    
    # Convert to Decimal for DynamoDB
    current_price = Decimal(str(metrics['current_price']))
    volatility = Decimal(str(metrics['volatility']))
    market_cap = Decimal(str(metrics['market_cap']))
    price_change_percent = Decimal(str(metrics['price_change_percent']))
    pe_ratio_decimal = Decimal(str(pe_ratio)) if pe_ratio else Decimal('0')
    dividend_yield_decimal = Decimal(str(dividend_yield)) if dividend_yield else Decimal('0')
    
    item = {
        # Primary Key
        'PK': f'STOCK#{symbol}',
        'SK': f'{timeframe}#CURRENT',
        
        # Stock identification
        'symbol': symbol,
        'company_name': company_name,
        'timeframe': timeframe,
        
        # Price data
        'current_price': current_price,
        'previous_close': Decimal(str(metrics['previous_close'])),
        'price_change': Decimal(str(metrics['price_change'])),
        'price_change_percent': price_change_percent,
        'day_high': Decimal(str(metrics['day_high'])),
        'day_low': Decimal(str(metrics['day_low'])),
        'year_high': Decimal(str(metrics['year_high'])),
        'year_low': Decimal(str(metrics['year_low'])),
        
        # Returns and volatility
        'week_return': Decimal(str(metrics['week_return'])),
        'volatility': volatility,
        
        # Volume
        'volume': int(metrics['volume']),
        'avg_volume': int(metrics['avg_volume']),
        
        # Market data
        'market_cap': market_cap,
        'shares_outstanding': int(shares_outstanding),
        'pe_ratio': pe_ratio_decimal,
        'dividend_yield': dividend_yield_decimal,
        
        # Company metadata
        'industry': industry,
        'sector': sector,
        
        # Metadata
        'data_source': 'S3-Historical-EOD',
        'last_updated': datetime.utcnow().isoformat(),
        
        # GSI1: Sector-based queries sorted by volatility (using sector for broader filtering)
        'GSI1PK': f'SECTOR#{sector}#{timeframe}',
        'GSI1SK': volatility,  # Numeric (Decimal) for range queries
        
        # GSI2: Volatility range queries
        'GSI2PK': f'VOLATILITY#{timeframe}',
        'GSI2SK': volatility,  # Numeric (Decimal) for range queries
        
        # GSI3: Price change range queries
        'GSI3PK': f'PRICE_CHANGE#{timeframe}',
        'GSI3SK': price_change_percent,  # Numeric (Decimal) for range queries
        
        # GSI4: Market cap range queries
        'GSI4PK': f'MARKET_CAP#{timeframe}',
        'GSI4SK': market_cap,  # Numeric (Decimal) for range queries
        
        # GSI5: Price range queries
        'GSI5PK': f'PRICE#{timeframe}',
        'GSI5SK': current_price,  # Numeric (Decimal) for range queries
        
        # GSI6: P/E ratio range queries
        'GSI6PK': f'PE_RATIO#{timeframe}',
        'GSI6SK': pe_ratio_decimal,  # Numeric (Decimal) for range queries
        
        # GSI7: Dividend yield range queries
        'GSI7PK': f'DIVIDEND_YIELD#{timeframe}',
        'GSI7SK': dividend_yield_decimal,  # Numeric (Decimal) for range queries
        
        # TTL for automatic cleanup (7 days - refreshed daily)
        'expires_at': int((datetime.utcnow() + timedelta(days=7)).timestamp())
    }
    
    return item


def process_batch(symbols: List[str], priority: str) -> List[Dict[str, Any]]:
    """
    Process a batch of stocks and generate DynamoDB items for all timeframes.
    
    Args:
        symbols: List of stock symbols to process
        priority: Priority tier
        
    Returns:
        List of DynamoDB items
    """
    items = []
    timeframes = ['1d', '7d', '30d', '1y']
    
    for symbol in symbols:
        try:
            # Read stock data from S3
            stock_data = read_stock_from_s3(symbol, priority)
            
            if not stock_data:
                logger.warning(f"Skipping {symbol} - no S3 data")
                continue
            
            history = stock_data.get('history', [])
            if not history or len(history) < 2:
                logger.warning(f"Skipping {symbol} - insufficient history")
                continue
            
            # Calculate metrics for each timeframe
            for timeframe in timeframes:
                metrics = calculate_metrics_for_timeframe(history, timeframe)
                item = create_dynamodb_item(symbol, stock_data, metrics, timeframe)
                items.append(item)
            
            logger.info(f"✅ Processed {symbol}: {len(timeframes)} timeframes, market_cap=${metrics.get('market_cap', 0):,}")
            
        except Exception as e:
            logger.error(f"Error processing {symbol}: {e}")
            continue
    
    logger.info(f"Batch complete: generated {len(items)} DynamoDB items from {len(symbols)} stocks")
    return items


def batch_write_to_dynamodb(table, items: List[Dict[str, Any]]) -> Dict[str, int]:
    """
    Write items to DynamoDB in batches.
    
    Args:
        table: DynamoDB table resource
        items: List of items to write
        
    Returns:
        Dict with success/failure counts
    """
    if not items:
        return {'success': 0, 'failed': 0}
    
    try:
        MAX_BATCH_SIZE = 25  # DynamoDB limit
        success_count = 0
        failed_count = 0
        
        for i in range(0, len(items), MAX_BATCH_SIZE):
            batch = items[i:i + MAX_BATCH_SIZE]
            
            try:
                with table.batch_writer() as writer:
                    for item in batch:
                        writer.put_item(Item=item)
                
                success_count += len(batch)
                logger.info(f"Wrote batch of {len(batch)} items ({success_count}/{len(items)} total)")
                
            except Exception as batch_error:
                logger.error(f"Error writing batch: {batch_error}")
                failed_count += len(batch)
        
        logger.info(f"DynamoDB write complete: {success_count} successful, {failed_count} failed")
        
        return {'success': success_count, 'failed': failed_count}
        
    except Exception as e:
        logger.error(f"Error in batch write: {e}")
        return {'success': 0, 'failed': len(items)}


def lambda_handler(event, context):
    """
    Main Lambda handler - processes a batch of stocks for EOD aggregation.
    
    Expected input (from Step Functions):
    {
        "symbols": ["AAPL", "MSFT", "GOOGL", ...],
        "priority": "high",
        "batch_number": 1,
        "total_in_batch": 200
    }
    
    Returns:
        Summary of processing results
    """
    try:
        logger.info("=== EOD Aggregator Started ===")
        logger.info(f"Event: {json.dumps(event, default=str)}")
        
        # Extract batch info
        symbols = event.get('symbols', [])
        priority = event.get('priority', 'unknown')
        batch_number = event.get('batch_number', 0)
        
        if not symbols:
            logger.error("No symbols provided in event")
            return {
                'statusCode': 400,
                'batch_number': batch_number,
                'error': 'No symbols provided'
            }
        
        logger.info(f"Processing batch #{batch_number}: {len(symbols)} stocks (priority: {priority})")
        
        # Get DynamoDB table
        table = get_dynamodb_table()
        
        # Process batch and generate DynamoDB items
        items = process_batch(symbols, priority)
        
        if not items:
            logger.warning(f"No items generated for batch {batch_number}")
            return {
                'statusCode': 200,
                'batch_number': batch_number,
                'symbols_processed': len(symbols),
                'items_written': 0,
                'items_failed': 0
            }
        
        # Write to DynamoDB
        write_result = batch_write_to_dynamodb(table, items)
        
        logger.info(f"=== Batch {batch_number} Complete ===")
        logger.info(f"Symbols: {len(symbols)}, Items: {len(items)}, Success: {write_result['success']}, Failed: {write_result['failed']}")
        
        return {
            'statusCode': 200,
            'batch_number': batch_number,
            'priority': priority,
            'symbols_processed': len(symbols),
            'items_written': write_result['success'],
            'items_failed': write_result['failed']
        }
        
    except Exception as e:
        logger.error(f"Error in EOD aggregator: {str(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        
        return {
            'statusCode': 500,
            'batch_number': event.get('batch_number', 0),
            'error': str(e),
            'message': 'EOD aggregation failed'
        }

