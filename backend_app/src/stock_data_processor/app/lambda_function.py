"""
Stock Data Processor Lambda
Processes stock data from SQS, fetches from Yahoo Finance, and stores in DynamoDB
"""

import json
import os
import logging
import boto3
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from decimal import Decimal
import time
import random
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np
import urllib.request

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS clients
dynamodb = boto3.resource('dynamodb')
sqs = boto3.client('sqs')

# Configuration from environment variables
MAX_PARALLEL_THREADS = int(os.environ.get('MAX_PARALLEL_THREADS', '10'))
REQUEST_RATE_LIMIT = float(os.environ.get('REQUEST_RATE_LIMIT', '1.0'))
BATCH_TIMEOUT = int(os.environ.get('BATCH_TIMEOUT', '50'))

# Rate limiting state
_last_request_time = {}
_request_lock = None

# Global cache for SEC data (persists across invocations in same Lambda container)
_sec_company_tickers_cache = None
_shares_outstanding_cache = {}  # Cache shares outstanding by symbol

def get_rate_limit_lock():
    """Get or create threading lock for rate limiting"""
    global _request_lock
    if _request_lock is None:
        import threading
        _request_lock = threading.Lock()
    return _request_lock

def enforce_rate_limit(symbol: str):
    """Enforce rate limiting between requests"""
    global _last_request_time
    
    lock = get_rate_limit_lock()
    with lock:
        current_time = time.time()
        last_time = _last_request_time.get(symbol, 0)
        time_since_last = current_time - last_time
        
        min_interval = 1.0 / REQUEST_RATE_LIMIT  # e.g., 1.0 req/sec = 1.0 second interval
        
        if time_since_last < min_interval:
            sleep_time = min_interval - time_since_last
            # Add small jitter to avoid synchronized requests
            sleep_time += random.uniform(0.1, 0.3)
            time.sleep(sleep_time)
        
        _last_request_time[symbol] = time.time()

def get_dynamodb_table():
    """Get DynamoDB table reference"""
    table_name = os.environ.get('DYNAMODB_TABLE_NAME')
    if not table_name:
        raise ValueError("DYNAMODB_TABLE_NAME environment variable not set")
    return dynamodb.Table(table_name)

def load_sec_company_tickers() -> Dict[str, Any]:
    """
    Load SEC company tickers JSON (cached globally per Lambda execution).
    """
    global _sec_company_tickers_cache
    if _sec_company_tickers_cache is not None:
        return _sec_company_tickers_cache
    
    try:
        url = "https://www.sec.gov/files/company_tickers.json"
        headers = {
            'User-Agent': 'Cosine-AI stock-data-processor contact@cosine-ai.com',
            'Accept-Encoding': 'gzip, deflate'
        }
        req = urllib.request.Request(url, headers=headers)
        
        with urllib.request.urlopen(req, timeout=10) as response:
            _sec_company_tickers_cache = json.loads(response.read().decode())
            logger.info(f"✅ Loaded SEC company tickers: {len(_sec_company_tickers_cache)} companies")
            return _sec_company_tickers_cache
    
    except Exception as e:
        logger.error(f"Failed to load SEC company tickers: {e}")
        return {}

def fetch_shares_outstanding_sec(symbol: str) -> int:
    """
    Fetch shares outstanding from SEC EDGAR API.
    Results are cached globally per Lambda execution.
    
    Returns:
        Shares outstanding as integer, or 0 if not found
    """
    global _shares_outstanding_cache
    
    # Check cache first
    if symbol in _shares_outstanding_cache:
        return _shares_outstanding_cache[symbol]
    
    try:
        # Use SEC's company tickers to get CIK
        sec_tickers = load_sec_company_tickers()
        company_info = None
        for key, company in sec_tickers.items():
            if company.get('ticker', '').upper() == symbol.upper():
                company_info = company
                break
        
        if not company_info:
            logger.warning(f"Symbol {symbol} not found in SEC tickers")
            _shares_outstanding_cache[symbol] = 0
            return 0
        
        cik = str(company_info.get('cik_str', '')).zfill(10)
        
        # Get company facts (includes shares outstanding)
        time.sleep(0.2)  # SEC rate limiting: 5 req/sec
        facts_url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
        facts_headers = {
            'User-Agent': 'Cosine-AI stock-data-processor contact@cosine-ai.com',
            'Accept-Encoding': 'gzip, deflate'
        }
        facts_req = urllib.request.Request(facts_url, headers=facts_headers)
        
        with urllib.request.urlopen(facts_req, timeout=10) as facts_response:
            facts_data = json.loads(facts_response.read().decode())
            
            us_gaap = facts_data.get('facts', {}).get('us-gaap', {})
            
            # Try multiple fields for shares outstanding
            share_fields = [
                'EntityCommonStockSharesOutstanding',
                'CommonStockSharesOutstanding',
                'CommonStockSharesIssued',
                'WeightedAverageNumberOfSharesOutstandingBasic'
            ]
            
            for field in share_fields:
                if field in us_gaap:
                    units = us_gaap[field].get('units', {}).get('shares', [])
                    if units:
                        # Get most recent value
                        most_recent = sorted(units, key=lambda x: x.get('end', ''), reverse=True)[0]
                        shares = most_recent.get('val', 0)
                        if shares > 0:
                            logger.info(f"✅ Found shares outstanding for {symbol}: {shares:,} (field: {field})")
                            _shares_outstanding_cache[symbol] = shares
                            return shares
        
        # Not found
        logger.warning(f"⚠️ No shares outstanding found in SEC data for {symbol}")
        _shares_outstanding_cache[symbol] = 0
        return 0
        
    except Exception as e:
        logger.error(f"Error fetching shares outstanding from SEC for {symbol}: {e}")
        _shares_outstanding_cache[symbol] = 0
        return 0

def fetch_stock_data_yahoo(symbol: str, timeframe: str = '1d') -> Optional[Dict[str, Any]]:
    """
    Fetch stock data from Yahoo Finance using direct HTTP calls.
    Based on the working stock_data lambda implementation.
    
    Args:
        symbol: Stock ticker symbol
        timeframe: Time period ('1d', '7d', '30d', '1y')
        
    Returns:
        Dictionary with stock data or None if failed
    """
    try:
        logger.info(f"Fetching data for {symbol} (timeframe: {timeframe})")
        
        # Enforce rate limiting
        enforce_rate_limit(symbol)
        
        # Step 1: Get current price and basic data
        current_data = fetch_current_price_yahoo(symbol)
        if not current_data or 'error' in current_data:
            logger.error(f"Failed to get current price for {symbol}")
            return None
        
        # Step 2: Get current price first (needed for market cap calculation)
        current_price = current_data.get('current_price', 0)
        
        # Step 3: Get historical data for volatility and returns calculation
        chart_data = fetch_historical_data_yahoo(symbol, timeframe)
        if not chart_data:
            logger.warning(f"No historical data for {symbol}, using current data only")
        
        # Step 4: Get additional metrics (industry, sector, market cap, etc.)
        # Pass current_price for market cap calculation from SEC shares outstanding
        additional_data = fetch_additional_metrics_yahoo(symbol, current_price)
        
        # Step 5: Calculate statistics from chart data
        week_return, annual_return, volatility = calculate_stats_from_chart_data(
            chart_data, current_price, timeframe
        )
        
        # Combine all data
        result = {
            'symbol': symbol,
            'timeframe': timeframe,
            'current_price': current_price,
            'previous_close': current_data.get('previous_close', current_price),
            'price_change': current_price - current_data.get('previous_close', current_price),
            'price_change_percent': current_data.get('price_change_24h', 0),
            'week_return': week_return,
            'annual_return': annual_return,
            'volatility': volatility,
            'volume': additional_data.get('volume', 0),
            'avg_volume': additional_data.get('avg_volume', 0),
            'market_cap': additional_data.get('market_cap', 0),
            'pe_ratio': additional_data.get('pe_ratio', 0),
            'beta': additional_data.get('beta', 1.0),
            'dividend_yield': additional_data.get('dividend_yield', 0),
            'eps': additional_data.get('eps', 0),
            'industry': additional_data.get('industry', 'Unknown'),
            'sector': additional_data.get('sector', 'Unknown'),
            'day_high': additional_data.get('day_high', current_price),
            'day_low': additional_data.get('day_low', current_price),
            'year_high': additional_data.get('year_high', current_price),
            'year_low': additional_data.get('year_low', current_price),
            'data_source': 'Yahoo Finance HTTP',
            'last_updated': datetime.utcnow().isoformat()
        }
        
        logger.info(f"Successfully fetched data for {symbol}: ${current_price:.2f}, vol={volatility:.4f}")
        return result
        
    except Exception as e:
        logger.error(f"Error fetching stock data for {symbol}: {str(e)}")
        return None

def fetch_current_price_yahoo(symbol: str) -> Optional[Dict[str, Any]]:
    """Fetch current price using direct HTTP call to Yahoo Finance"""
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive'
        }
        
        response = requests.get(url, headers=headers, timeout=15)
        
        if response.status_code != 200:
            return {'error': f"HTTP {response.status_code}"}
        
        data = response.json()
        
        if 'chart' not in data or not data['chart']['result']:
            return {'error': 'No data'}
        
        result = data['chart']['result'][0]
        meta = result.get('meta', {})
        
        current_price = meta.get('regularMarketPrice', 0)
        previous_close = meta.get('previousClose', current_price)
        price_change_24h = ((current_price - previous_close) / previous_close * 100) if previous_close > 0 else 0
        
        return {
            'current_price': current_price,
            'previous_close': previous_close,
            'price_change_24h': price_change_24h
        }
        
    except Exception as e:
        logger.error(f"Error fetching current price for {symbol}: {str(e)}")
        return {'error': str(e)}

def fetch_historical_data_yahoo(symbol: str, timeframe: str = '1d') -> List[Dict[str, Any]]:
    """Fetch historical data for chart using direct HTTP call"""
    try:
        # Map timeframe to Yahoo Finance parameters
        timeframe_map = {
            '1d': {'range': '1d', 'interval': '1m'},
            '7d': {'range': '7d', 'interval': '1h'},
            '30d': {'range': '1mo', 'interval': '1d'},
            '1y': {'range': '1y', 'interval': '1d'}
        }
        
        config = timeframe_map.get(timeframe, timeframe_map['1y'])
        
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
        params = {
            'range': config['range'],
            'interval': config['interval'],
            'includePrePost': 'true'
        }
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json',
            'Accept-Language': 'en-US,en;q=0.9'
        }
        
        # Add small delay to avoid overwhelming the API
        time.sleep(random.uniform(0.5, 1.5))
        
        response = requests.get(url, params=params, headers=headers, timeout=15)
        
        if response.status_code != 200:
            logger.error(f"Historical data request failed: {response.status_code}")
            return []
        
        data = response.json()
        
        if 'chart' not in data or not data['chart']['result']:
            return []
        
        result = data['chart']['result'][0]
        timestamps = result.get('timestamp', [])
        quotes = result.get('indicators', {}).get('quote', [{}])[0]
        closes = quotes.get('close', [])
        
        # Format data for calculations
        chart_data = []
        for i, timestamp in enumerate(timestamps):
            if i < len(closes) and closes[i] is not None:
                chart_data.append({
                    'time': timestamp,
                    'close': closes[i]
                })
        
        logger.info(f"Retrieved {len(chart_data)} historical data points for {symbol}")
        return chart_data
        
    except Exception as e:
        logger.error(f"Error fetching historical data for {symbol}: {str(e)}")
        return []

def fetch_additional_metrics_yahoo(symbol: str, current_price: float = 0) -> Dict[str, Any]:
    """
    Fetch additional metrics (industry, sector, market cap, etc.).
    Market cap is calculated from SEC shares outstanding × current price.
    Other metrics attempted from Yahoo Finance quoteSummary (often fails with auth).
    """
    metrics = {
        'volume': 0,
        'avg_volume': 0,
        'market_cap': 0,
        'pe_ratio': 0,
        'beta': 1.0,
        'dividend_yield': 0,
        'eps': 0,
        'industry': 'Unknown',
        'sector': 'Unknown',
        'day_high': 0,
        'day_low': 0,
        'year_high': 0,
        'year_low': 0
    }
    
    # Calculate market cap from SEC shares outstanding
    if current_price > 0:
        shares_outstanding = fetch_shares_outstanding_sec(symbol)
        if shares_outstanding > 0:
            metrics['market_cap'] = int(shares_outstanding * current_price)
            logger.info(f"✅ Calculated market cap for {symbol}: ${metrics['market_cap']:,} ({shares_outstanding:,} shares @ ${current_price:.2f})")
    
    # Try to get other metrics from Yahoo (often fails, so we have defaults)
    try:
        url = f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{symbol}"
        params = {
            'modules': 'price,summaryDetail,assetProfile,defaultKeyStatistics'
        }
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json'
        }
        
        # Add delay to avoid rate limiting
        time.sleep(random.uniform(0.5, 1.5))
        
        response = requests.get(url, params=params, headers=headers, timeout=15)
        
        if response.status_code == 200:
            data = response.json()
            
            if 'quoteSummary' in data and data['quoteSummary']['result']:
                result = data['quoteSummary']['result'][0]
                
                # Extract metrics from different modules
                price_info = result.get('price', {})
                summary_detail = result.get('summaryDetail', {})
                asset_profile = result.get('assetProfile', {})
                key_stats = result.get('defaultKeyStatistics', {})
                
                # Helper function to extract raw value from Yahoo's nested structure
                def get_raw(obj, key, default=0):
                    val = obj.get(key, {})
                    if isinstance(val, dict):
                        return val.get('raw', default)
                    return val if val is not None else default
                
                # Update metrics with Yahoo data (but keep SEC market cap)
                metrics.update({
                    'volume': get_raw(price_info, 'regularMarketVolume', metrics['volume']),
                    'avg_volume': get_raw(summary_detail, 'averageVolume', metrics['avg_volume']),
                    # 'market_cap': keep SEC calculation
                    'pe_ratio': get_raw(summary_detail, 'trailingPE', metrics['pe_ratio']),
                    'beta': get_raw(key_stats, 'beta', metrics['beta']),
                    'dividend_yield': get_raw(summary_detail, 'dividendYield', metrics['dividend_yield']),
                    'eps': get_raw(key_stats, 'trailingEps', metrics['eps']),
                    'industry': asset_profile.get('industry', metrics['industry']),
                    'sector': asset_profile.get('sector', metrics['sector']),
                    'day_high': get_raw(summary_detail, 'dayHigh', metrics['day_high']),
                    'day_low': get_raw(summary_detail, 'dayLow', metrics['day_low']),
                    'year_high': get_raw(summary_detail, 'fiftyTwoWeekHigh', metrics['year_high']),
                    'year_low': get_raw(summary_detail, 'fiftyTwoWeekLow', metrics['year_low'])
                })
                logger.info(f"✅ Fetched additional Yahoo metrics for {symbol}")
        else:
            logger.warning(f"Yahoo quoteSummary failed for {symbol}: {response.status_code} (using defaults)")
        
    except Exception as e:
        logger.warning(f"Could not fetch Yahoo quoteSummary for {symbol}: {e} (using defaults)")
    
    return metrics

def calculate_stats_from_chart_data(
    chart_data: List[Dict[str, Any]], 
    current_price: float, 
    timeframe: str
) -> tuple:
    """
    Calculate week_return, annual_return, and volatility from chart data.
    Based on portfolio risk formulas.
    
    Args:
        chart_data: List of {'time': timestamp, 'close': price} dictionaries
        current_price: Current price
        timeframe: Time period ('1d', '7d', '30d', '1y')
        
    Returns:
        tuple: (week_return, annual_return, volatility)
    """
    try:
        if not chart_data or not current_price or len(chart_data) < 2:
            return 0.0, 0.0, 0.0
        
        # Extract prices
        prices = [point['close'] for point in chart_data if point.get('close')]
        if len(prices) < 2:
            return 0.0, 0.0, 0.0
        
        # Calculate total return
        start_price = prices[0]
        total_return = (current_price / start_price) - 1 if start_price > 0 else 0.0
        
        # Calculate annual return using portfolio risk formula: total_return * (252 / len(df))
        annual_return = total_return * (252 / len(prices)) * 100.0  # Percentage
        
        # Calculate week return (last 7 data points if available)
        week_return = 0.0
        if len(prices) >= 7:
            week_ago_price = prices[-7]
            week_total_return = (current_price / week_ago_price) - 1 if week_ago_price > 0 else 0.0
            week_return = week_total_return * (252 / 7) * 100.0  # Percentage
        else:
            # If less than 7 points, scale annual return to week
            week_return = annual_return / 52
        
        # Calculate volatility using log returns with sqrt(252) annualization
        volatility = 0.0
        if len(prices) > 1:
            log_returns = []
            for i in range(1, len(prices)):
                if prices[i-1] > 0:
                    log_return = np.log(prices[i] / prices[i-1])
                    log_returns.append(log_return)
            
            if log_returns:
                # Annualize volatility: std * sqrt(252)
                volatility = np.std(log_returns) * np.sqrt(252)
        
        return round(week_return, 2), round(annual_return, 2), round(volatility, 4)
        
    except Exception as e:
        logger.error(f"Error calculating stats: {str(e)}")
        return 0.0, 0.0, 0.0

def create_dynamodb_item(stock_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create DynamoDB item from stock data with new GSI structure.
    Uses numeric sort keys for efficient range queries.
    
    Args:
        stock_data: Stock data dictionary
        
    Returns:
        DynamoDB item dictionary
    """
    symbol = stock_data.get('symbol', '')
    timeframe = stock_data.get('timeframe', '1d')
    
    # Extract numeric values (use Decimal for DynamoDB)
    current_price = Decimal(str(stock_data.get('current_price', 0)))
    volatility = Decimal(str(stock_data.get('volatility', 0)))
    market_cap = Decimal(str(stock_data.get('market_cap', 0)))
    price_change_percent = Decimal(str(stock_data.get('price_change_percent', 0)))
    
    industry = stock_data.get('industry', 'Unknown')
    sector = stock_data.get('sector', 'Unknown')
    
    # Create the DynamoDB item with new GSI structure
    item = {
        # Primary Key
        'PK': f'STOCK#{symbol}',
        'SK': f'{timeframe}#CURRENT',
        
        # Stock data (all numeric fields as Decimal)
        'symbol': symbol,
        'timeframe': timeframe,
        'current_price': current_price,
        'previous_close': Decimal(str(stock_data.get('previous_close', 0))),
        'price_change': Decimal(str(stock_data.get('price_change', 0))),
        'price_change_percent': price_change_percent,
        'week_return': Decimal(str(stock_data.get('week_return', 0))),
        'annual_return': Decimal(str(stock_data.get('annual_return', 0))),
        'volatility': volatility,
        'volume': int(stock_data.get('volume', 0)),
        'avg_volume': int(stock_data.get('avg_volume', 0)),
        'market_cap': market_cap,
        'pe_ratio': Decimal(str(stock_data.get('pe_ratio', 0))),
        'beta': Decimal(str(stock_data.get('beta', 1.0))),
        'dividend_yield': Decimal(str(stock_data.get('dividend_yield', 0))),
        'eps': Decimal(str(stock_data.get('eps', 0))),
        'industry': industry,
        'sector': sector,
        'day_high': Decimal(str(stock_data.get('day_high', 0))),
        'day_low': Decimal(str(stock_data.get('day_low', 0))),
        'year_high': Decimal(str(stock_data.get('year_high', 0))),
        'year_low': Decimal(str(stock_data.get('year_low', 0))),
        'data_source': stock_data.get('data_source', 'Yahoo Finance'),
        'last_updated': stock_data.get('last_updated', datetime.utcnow().isoformat()),
        
        # GSI1: Industry-based queries sorted by volatility
        'GSI1PK': f'INDUSTRY#{industry}#{timeframe}',
        'GSI1SK': volatility,  # Numeric sort key
        
        # GSI2: Volatility range queries
        'GSI2PK': f'VOLATILITY#{timeframe}',
        'GSI2SK': volatility,  # Numeric sort key
        
        # GSI3: Price change range queries
        'GSI3PK': f'PRICE_CHANGE#{timeframe}',
        'GSI3SK': price_change_percent,  # Numeric sort key
        
        # GSI4: Market cap range queries
        'GSI4PK': f'MARKET_CAP#{timeframe}',
        'GSI4SK': market_cap,  # Numeric sort key
        
        # GSI5: Price range queries
        'GSI5PK': f'PRICE#{timeframe}',
        'GSI5SK': current_price,  # Numeric sort key
        
        # TTL for automatic cleanup (30 days)
        'expires_at': int((datetime.utcnow() + timedelta(days=30)).timestamp())
    }
    
    return item

def process_stock_batch(symbols: List[str], timeframe: str, priority: str) -> List[Dict[str, Any]]:
    """
    Process a batch of stock symbols in parallel using ThreadPoolExecutor.
    
    Args:
        symbols: List of stock symbols
        timeframe: Time period ('1d', '7d', '30d', '1y')
        priority: Priority tier ('high', 'medium', 'low')
        
    Returns:
        List of DynamoDB items ready to write
    """
    items = []
    
    logger.info(f"Processing batch of {len(symbols)} stocks (timeframe: {timeframe}, priority: {priority})")
    
    # Process stocks in parallel
    with ThreadPoolExecutor(max_workers=MAX_PARALLEL_THREADS) as executor:
        # Submit all tasks
        future_to_symbol = {
            executor.submit(fetch_stock_data_yahoo, symbol, timeframe): symbol 
            for symbol in symbols
        }
        
        # Collect results as they complete
        for future in as_completed(future_to_symbol):
            symbol = future_to_symbol[future]
            try:
                stock_data = future.result(timeout=30)
                if stock_data:
                    # Create DynamoDB item
                    item = create_dynamodb_item(stock_data)
                    items.append(item)
                else:
                    logger.warning(f"No data returned for {symbol}")
            except Exception as e:
                logger.error(f"Error processing {symbol}: {str(e)}")
    
    logger.info(f"Successfully processed {len(items)}/{len(symbols)} stocks")
    return items

def batch_write_to_dynamodb(table, items: List[Dict[str, Any]]) -> bool:
    """Write items to DynamoDB in batches"""
    try:
        if not items:
            return True
        
        # DynamoDB batch_write_item can handle up to 25 items
        MAX_BATCH_SIZE = 25
        
        success_count = 0
        for i in range(0, len(items), MAX_BATCH_SIZE):
            batch = items[i:i + MAX_BATCH_SIZE]
            
            with table.batch_writer() as writer:
                for item in batch:
                    writer.put_item(Item=item)
            
            success_count += len(batch)
            logger.info(f"Wrote batch of {len(batch)} items ({success_count}/{len(items)} total)")
        
        logger.info(f"Successfully wrote {len(items)} items to DynamoDB")
        return True
        
    except Exception as e:
        logger.error(f"Error writing to DynamoDB: {str(e)}")
        return False

def lambda_handler(event, context):
    """
    Main Lambda handler - processes stock data from SQS.
    
    Expected SQS message format:
    {
        "symbols": ["AAPL", "MSFT", "GOOGL"],
        "timeframe": "1d",
        "priority": "high",
        "batch_number": 0
    }
    """
    try:
        logger.info(f"Stock Data Processor started")
        logger.info(f"Event: {json.dumps(event, default=str)}")
        
        # Check if invoked by SQS
        if 'Records' not in event:
            logger.error("Not invoked by SQS - missing Records")
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Expected SQS event'})
            }
        
        # Get DynamoDB table
        table = get_dynamodb_table()
        
        # Process each SQS record
        total_processed = 0
        all_items = []
        
        for record in event['Records']:
            try:
                # Parse SQS message
                message_body = json.loads(record['body'])
                logger.info(f"Processing SQS message: {message_body}")
                
                symbols = message_body.get('symbols', [])
                timeframe = message_body.get('timeframe', '1d')
                priority = message_body.get('priority', 'medium')
                batch_number = message_body.get('batch_number', 0)
                
                if not symbols:
                    logger.warning("No symbols in message")
                    continue
                
                logger.info(f"Processing batch #{batch_number}: {len(symbols)} symbols, timeframe={timeframe}, priority={priority}")
                
                # Fetch and process stock data
                items = process_stock_batch(symbols, timeframe, priority)
                all_items.extend(items)
                
                total_processed += len(symbols)
                
            except Exception as e:
                logger.error(f"Error processing SQS record: {str(e)}")
                continue
        
        # Write all items to DynamoDB
        if all_items:
            success = batch_write_to_dynamodb(table, all_items)
            if success:
                logger.info(f"Successfully wrote {len(all_items)} items to DynamoDB")
            else:
                logger.error("Failed to write some items to DynamoDB")
        
        logger.info(f"Stock Data Processor completed: processed {total_processed} symbols, wrote {len(all_items)} items")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Stock data processing completed',
                'symbols_processed': total_processed,
                'items_written': len(all_items)
            })
        }
        
    except Exception as e:
        logger.error(f"Error in stock data processor: {str(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'message': 'Stock data processing failed'
            })
        }
