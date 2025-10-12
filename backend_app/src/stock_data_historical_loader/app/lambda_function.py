"""
Stock Data Historical Loader Lambda

This Lambda function loads historical stock data (up to 5 years) from Yahoo Finance
and stores it in S3. It's designed to process batches of symbols within the 15-minute
Lambda timeout limit.

Designed to be orchestrated by AWS Step Functions for parallel batch processing.
"""

import json
import os
import time
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import csv
import io
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
import urllib.request
import urllib.error
from decimal import Decimal

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Environment variables
S3_BUCKET = os.environ.get('S3_BUCKET')
RATE_LIMIT = float(os.environ.get('RATE_LIMIT', '2.0'))  # Requests per second
MAX_WORKERS = int(os.environ.get('MAX_WORKERS', '5'))
YAHOO_FINANCE_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=5y"

# Import boto3 (available in Lambda runtime)
import boto3
s3_client = boto3.client('s3')


def load_symbols_from_event(event: Dict[str, Any]) -> tuple[List[str], str]:
    """
    Load symbols from the event payload.
    
    If no payload is provided, this function will load all symbols from CSVs in S3.
    
    Args:
        event: Lambda event object
        
    Returns:
        Tuple of (list of symbols, priority tier)
    """
    # Check if symbols are provided in the event
    if 'symbols' in event:
        symbols = event['symbols']
        priority = event.get('priority', 'unknown')
        logger.info(f"Loaded {len(symbols)} symbols from event payload (priority: {priority})")
        return symbols, priority
    
    # If no symbols in event, this is the initial invocation
    # We'll return empty to signal that batch generation is needed
    return [], 'none'


def load_all_symbols_from_s3() -> List[Dict[str, Any]]:
    """
    Load all symbols from CSV files stored in S3 bucket.
    This is used for the initial Step Functions invocation to generate batches.
    
    Returns:
        List of dicts with 'symbol' and 'priority' keys
    """
    all_symbols = []
    csv_files = {
        'highcap.csv': 'high',
        'midcap.csv': 'medium',
        'lowcap.csv': 'low'
    }
    
    for filename, priority in csv_files.items():
        try:
            # Try to read from S3 first (if uploaded there)
            try:
                response = s3_client.get_object(Bucket=S3_BUCKET, Key=f"stock-lists/{filename}")
                content = response['Body'].read().decode('utf-8')
                logger.info(f"Loaded {filename} from S3")
            except:
                # Fallback: read from Lambda package (if bundled)
                logger.info(f"Could not load {filename} from S3, will be provided by Step Functions")
                continue
            
            # Parse CSV
            csv_reader = csv.DictReader(io.StringIO(content))
            symbols = [{'symbol': row['Symbol'], 'priority': priority} for row in csv_reader]
            all_symbols.extend(symbols)
            logger.info(f"Loaded {len(symbols)} symbols from {filename} ({priority} priority)")
            
        except Exception as e:
            logger.error(f"Error loading {filename}: {str(e)}")
            continue
    
    logger.info(f"Total symbols loaded: {len(all_symbols)}")
    return all_symbols


def generate_batches(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Generate batches for Step Functions Map state.
    This is called when the Lambda is invoked without a 'symbols' key in the event.
    
    Returns:
        Dict with 'batches' key containing list of batch configs
    """
    logger.info("=== Generating Batches for Step Functions ===")
    
    # Load CSV files from Lambda package (bundled with deployment)
    all_symbols = []
    csv_files = {
        'highcap.csv': 'high',
        'midcap.csv': 'medium',
        'lowcap.csv': 'low'
    }
    
    for filename, priority in csv_files.items():
        try:
            # Read from Lambda package directory
            file_path = os.path.join(os.path.dirname(__file__), filename)
            with open(file_path, 'r') as f:
                csv_reader = csv.DictReader(f)
                symbols = [row['Symbol'] for row in csv_reader]
                all_symbols.append({'symbols': symbols, 'priority': priority, 'filename': filename})
                logger.info(f"Loaded {len(symbols)} symbols from {filename} ({priority} priority)")
        except Exception as e:
            logger.error(f"Error loading {filename}: {str(e)}")
            logger.error(traceback.format_exc())
    
    # Split into batches of 150 symbols each (safe for 15-min timeout)
    batches = []
    batch_size = 150
    
    for symbol_group in all_symbols:
        symbols = symbol_group['symbols']
        priority = symbol_group['priority']
        
        for i in range(0, len(symbols), batch_size):
            batch_symbols = symbols[i:i + batch_size]
            batches.append({
                'symbols': batch_symbols,
                'priority': priority,
                'batch_number': len(batches) + 1,
                'total_in_batch': len(batch_symbols)
            })
    
    logger.info(f"Generated {len(batches)} batches from {sum(len(g['symbols']) for g in all_symbols)} total symbols")
    
    return {
        'batches': batches,
        'total_batches': len(batches),
        'total_symbols': sum(len(g['symbols']) for g in all_symbols)
    }


def fetch_stock_metadata(symbol: str) -> Dict[str, Any]:
    """
    Fetch company metadata (industry, sector, company name, market cap) from Yahoo Finance.
    
    Args:
        symbol: Stock ticker symbol
        
    Returns:
        Dict with metadata fields
    """
    try:
        # Use Yahoo Finance quoteSummary endpoint for detailed metadata
        url = f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{symbol}"
        params = "modules=assetProfile,price,summaryDetail"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        req = urllib.request.Request(f"{url}?{params}", headers=headers)
        
        with urllib.request.urlopen(req, timeout=10) as response:
            data = json.loads(response.read().decode())
        
        result = data.get('quoteSummary', {}).get('result', [{}])[0]
        
        # Extract metadata from different modules
        asset_profile = result.get('assetProfile', {})
        price = result.get('price', {})
        summary_detail = result.get('summaryDetail', {})
        
        return {
            'company_name': price.get('longName', price.get('shortName', symbol)),
            'industry': asset_profile.get('industry', 'Unknown'),
            'sector': asset_profile.get('sector', 'Unknown'),
            'market_cap': summary_detail.get('marketCap', {}).get('raw', 0) if isinstance(summary_detail.get('marketCap'), dict) else summary_detail.get('marketCap', 0),
            'country': asset_profile.get('country', 'US'),
            'website': asset_profile.get('website', ''),
            'description': asset_profile.get('longBusinessSummary', '')[:500] if asset_profile.get('longBusinessSummary') else ''
        }
        
    except Exception as e:
        logger.warning(f"Could not fetch metadata for {symbol}: {str(e)}")
        # Return defaults if metadata fetch fails
        return {
            'company_name': symbol,
            'industry': 'Unknown',
            'sector': 'Unknown',
            'market_cap': 0,
            'country': 'US',
            'website': '',
            'description': ''
        }


def fetch_historical_data(symbol: str, years: int = 5) -> Optional[Dict[str, Any]]:
    """
    Fetch historical data for a single symbol from Yahoo Finance.
    
    Args:
        symbol: Stock ticker symbol
        years: Number of years of historical data to fetch
        
    Returns:
        Dict with historical data or None if failed
    """
    url = YAHOO_FINANCE_URL.format(symbol=symbol)
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        req = urllib.request.Request(url, headers=headers)
        
        with urllib.request.urlopen(req, timeout=10) as response:
            data = json.loads(response.read().decode())
        
        # Parse Yahoo Finance response
        chart = data.get('chart', {})
        result = chart.get('result', [])
        
        if not result or len(result) == 0:
            logger.warning(f"No data returned for {symbol}")
            return None
        
        quote = result[0]
        timestamps = quote.get('timestamp', [])
        indicators = quote.get('indicators', {}).get('quote', [{}])[0]
        
        # Extract OHLCV data
        opens = indicators.get('open', [])
        highs = indicators.get('high', [])
        lows = indicators.get('low', [])
        closes = indicators.get('close', [])
        volumes = indicators.get('volume', [])
        
        # Build historical data points
        history = []
        for i in range(len(timestamps)):
            # Skip if any required field is None
            if any(x is None or (i < len(x) and x[i] is None) for x in [closes, opens, highs, lows]):
                continue
                
            history.append({
                'timestamp': timestamps[i],
                'date': datetime.fromtimestamp(timestamps[i]).isoformat(),
                'open': float(opens[i]) if i < len(opens) and opens[i] is not None else None,
                'high': float(highs[i]) if i < len(highs) and highs[i] is not None else None,
                'low': float(lows[i]) if i < len(lows) and lows[i] is not None else None,
                'close': float(closes[i]) if i < len(closes) and closes[i] is not None else None,
                'volume': int(volumes[i]) if i < len(volumes) and volumes[i] is not None else 0
            })
        
        if not history:
            logger.warning(f"No valid data points for {symbol}")
            return None
        
        # Get basic metadata from chart response
        meta = quote.get('meta', {})
        
        # Fetch detailed metadata (industry, sector, etc.)
        logger.info(f"Fetching metadata for {symbol}")
        detailed_metadata = fetch_stock_metadata(symbol)
        
        return {
            'symbol': symbol,
            'currency': meta.get('currency', 'USD'),
            'exchange': meta.get('exchangeName', 'UNKNOWN'),
            'instrument_type': meta.get('instrumentType', 'EQUITY'),
            
            # Company metadata
            'company_name': detailed_metadata['company_name'],
            'industry': detailed_metadata['industry'],
            'sector': detailed_metadata['sector'],
            'market_cap': detailed_metadata['market_cap'],
            'country': detailed_metadata['country'],
            'website': detailed_metadata['website'],
            'description': detailed_metadata['description'],
            
            'data_points': len(history),
            'first_date': history[0]['date'] if history else None,
            'last_date': history[-1]['date'] if history else None,
            'history': history
        }
        
    except urllib.error.HTTPError as e:
        if e.code == 404:
            logger.warning(f"Symbol not found: {symbol}")
        else:
            logger.error(f"HTTP error fetching {symbol}: {e.code}")
        return None
    except Exception as e:
        logger.error(f"Error fetching {symbol}: {str(e)}")
        return None


def store_to_s3(symbol: str, data: Dict[str, Any], priority: str) -> bool:
    """
    Store historical data to S3.
    
    Args:
        symbol: Stock ticker symbol
        data: Historical data dict
        priority: Priority tier (high, medium, low)
        
    Returns:
        True if successful, False otherwise
    """
    try:
        s3_key = f"historical/{priority}/{symbol}.json"
        
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=json.dumps(data),
            ContentType='application/json',
            Metadata={
                'symbol': symbol,
                'priority': priority,
                'loaded_at': datetime.utcnow().isoformat(),
                'data_points': str(data.get('data_points', 0))
            }
        )
        
        logger.info(f"✅ Stored {symbol} to S3: {data.get('data_points', 0)} data points")
        return True
        
    except Exception as e:
        logger.error(f"Error storing {symbol} to S3: {str(e)}")
        return False


def process_symbol(symbol: str, priority: str) -> Dict[str, Any]:
    """
    Process a single symbol: fetch data and store to S3.
    
    Args:
        symbol: Stock ticker symbol
        priority: Priority tier
        
    Returns:
        Dict with processing results
    """
    start_time = time.time()
    
    try:
        # Fetch historical data
        data = fetch_historical_data(symbol)
        
        if data is None:
            return {
                'symbol': symbol,
                'status': 'failed',
                'error': 'No data returned from Yahoo Finance',
                'duration': time.time() - start_time
            }
        
        # Store to S3
        success = store_to_s3(symbol, data, priority)
        
        if success:
            return {
                'symbol': symbol,
                'status': 'success',
                'data_points': data.get('data_points', 0),
                'duration': time.time() - start_time
            }
        else:
            return {
                'symbol': symbol,
                'status': 'failed',
                'error': 'Failed to store to S3',
                'duration': time.time() - start_time
            }
            
    except Exception as e:
        logger.error(f"Error processing {symbol}: {str(e)}")
        return {
            'symbol': symbol,
            'status': 'failed',
            'error': str(e),
            'duration': time.time() - start_time
        }


def lambda_handler(event, context):
    """
    Main Lambda handler.
    
    Can be invoked in two modes:
    1. Batch Generation Mode (no 'symbols' in event): Generates batches for Step Functions
    2. Batch Processing Mode ('symbols' in event): Processes a batch of symbols
    """
    try:
        logger.info("=== Stock Data Historical Loader Started ===")
        logger.info(f"Event: {json.dumps(event, default=str)}")
        
        # Check if this is batch generation or batch processing
        if 'symbols' not in event:
            logger.info("Mode: Batch Generation")
            return generate_batches(event)
        
        # Batch Processing Mode
        logger.info("Mode: Batch Processing")
        
        symbols = event['symbols']
        priority = event.get('priority', 'unknown')
        batch_number = event.get('batch_number', 'unknown')
        
        logger.info(f"Processing batch {batch_number}: {len(symbols)} symbols ({priority} priority)")
        
        # Process symbols with rate limiting
        results = []
        delay_between_requests = 1.0 / RATE_LIMIT  # Convert requests/sec to delay
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Submit all tasks
            future_to_symbol = {
                executor.submit(process_symbol, symbol, priority): symbol
                for symbol in symbols
            }
            
            # Process completed tasks
            for future in as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                try:
                    result = future.result()
                    results.append(result)
                    
                    # Rate limiting
                    time.sleep(delay_between_requests)
                    
                except Exception as e:
                    logger.error(f"Exception processing {symbol}: {str(e)}")
                    results.append({
                        'symbol': symbol,
                        'status': 'failed',
                        'error': str(e)
                    })
        
        # Calculate summary statistics
        successful = [r for r in results if r['status'] == 'success']
        failed = [r for r in results if r['status'] == 'failed']
        total_data_points = sum(r.get('data_points', 0) for r in successful)
        avg_duration = sum(r.get('duration', 0) for r in results) / len(results) if results else 0
        
        logger.info("=== Batch Processing Complete ===")
        logger.info(f"Batch {batch_number} Summary:")
        logger.info(f"  Total: {len(results)} | Success: {len(successful)} | Failed: {len(failed)}")
        logger.info(f"  Data points: {total_data_points} | Avg duration: {avg_duration:.2f}s")
        
        return {
            'statusCode': 200,
            'batch_number': batch_number,
            'priority': priority,
            'total': len(results),
            'successful': len(successful),
            'failed': len(failed),
            'total_data_points': total_data_points,
            'avg_duration': avg_duration,
            'failed_symbols': [r['symbol'] for r in failed] if failed else []
        }
        
    except Exception as e:
        logger.error(f"Fatal error in lambda_handler: {str(e)}")
        logger.error(traceback.format_exc())
        
        return {
            'statusCode': 500,
            'error': str(e),
            'message': 'Historical loader failed'
        }

