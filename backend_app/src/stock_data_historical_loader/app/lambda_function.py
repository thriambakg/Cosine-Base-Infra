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
import gzip

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

# Global cache for SEC company tickers (loaded once per Lambda execution)
_sec_company_tickers_cache = None


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


def load_sec_company_tickers() -> Dict[str, Any]:
    """
    Load SEC company tickers JSON (cached globally per Lambda execution).
    This file is updated daily by the SEC and contains ~13,000 companies.
    
    Returns:
        Dict of company ticker data
    """
    global _sec_company_tickers_cache
    
    if _sec_company_tickers_cache is not None:
        return _sec_company_tickers_cache
    
    try:
        sec_url = "https://www.sec.gov/files/company_tickers.json"
        headers = {
            'User-Agent': 'CosineApp admin@cosine.com'  # SEC requires user agent
        }
        
        logger.info("📥 Loading SEC company tickers (once per batch)...")
        req = urllib.request.Request(sec_url, headers=headers)
        with urllib.request.urlopen(req, timeout=15) as response:
            _sec_company_tickers_cache = json.loads(response.read().decode())
        
        logger.info(f"✅ Loaded {len(_sec_company_tickers_cache)} companies from SEC")
        return _sec_company_tickers_cache
        
    except Exception as e:
        logger.error(f"Failed to load SEC company tickers: {e}")
        return {}


def fetch_stock_metadata(symbol: str) -> Dict[str, Any]:
    """
    Fetch company metadata from multiple sources with fallback chain:
    1. SEC EDGAR API (official, free, reliable)
    2. Yahoo Finance direct API
    3. Default values
    
    Args:
        symbol: Stock ticker symbol
        
    Returns:
        Dict with metadata fields
    """
    # Try SEC EDGAR first
    try:
        # Load cached company tickers
        sec_data = load_sec_company_tickers()
        
        # Find company by ticker symbol
        company_info = None
        for key, company in sec_data.items():
            if company.get('ticker', '').upper() == symbol.upper():
                company_info = company
                break
        
        if company_info:
            cik = str(company_info.get('cik_str', '')).zfill(10)
            company_name = company_info.get('title', symbol)
            
            # Fetch SIC code for industry classification
            # Rate limit: SEC allows 10 requests/second, we'll do 5/second to be safe
            time.sleep(0.2)  # 200ms delay = 5 requests/second
            
            try:
                submissions_url = f"https://data.sec.gov/submissions/CIK{cik}.json"
                headers_sec = {
                    'User-Agent': 'CosineApp admin@cosine.com'
                }
                req = urllib.request.Request(submissions_url, headers=headers_sec)
                with urllib.request.urlopen(req, timeout=10) as response:
                    submissions = json.loads(response.read().decode())
                
                sic_code = submissions.get('sic', '')
                sic_description = submissions.get('sicDescription', 'Unknown')
                
                # Map SIC to GICS Sector
                gics_sector = map_sic_to_gics(sic_code, sic_description)
                
                logger.info(f"✅ Found SEC data for {symbol}: {company_name}, Sector: {gics_sector}")
                
                return {
                    'company_name': company_name,
                    'industry': sic_description,
                    'sector': gics_sector,
                    'market_cap': 0,  # SEC doesn't provide market cap (fetched from Yahoo chart meta)
                    'pe_ratio': 0,  # SEC doesn't provide P/E ratio
                    'dividend_yield': 0,  # SEC doesn't provide dividend yield
                    'country': 'US'
                }
            except Exception as e:
                logger.warning(f"Could not fetch SIC for {symbol}: {e}")
        
    except Exception as e:
        logger.warning(f"SEC EDGAR API failed for {symbol}: {e}")
    
    # Fallback to Yahoo Finance direct API
    try:
        url = f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={symbol}"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=10) as response:
            data = json.loads(response.read().decode())
        
        result = data.get('quoteResponse', {}).get('result', [])
        if result and len(result) > 0:
            quote = result[0]
            
            # Map Yahoo sectors to GICS
            yahoo_sector = quote.get('sector', 'Unknown')
            sector_mapping = {
                'Technology': 'Information Technology',
                'Healthcare': 'Health Care',
                'Financial Services': 'Financials',
                'Consumer Cyclical': 'Consumer Discretionary',
                'Consumer Defensive': 'Consumer Staples',
                'Communication Services': 'Communication Services',
                'Energy': 'Energy',
                'Industrials': 'Industrials',
                'Basic Materials': 'Materials',
                'Real Estate': 'Real Estate',
                'Utilities': 'Utilities',
                'Financial': 'Financials'
            }
            gics_sector = sector_mapping.get(yahoo_sector, yahoo_sector)
            
            return {
                'company_name': quote.get('longName', quote.get('shortName', symbol)),
                'industry': quote.get('industry', 'Unknown'),
                'sector': gics_sector,
                'market_cap': quote.get('marketCap', 0),
                'pe_ratio': quote.get('trailingPE', 0),
                'dividend_yield': quote.get('dividendYield', 0),
                'country': 'US'
            }
    except Exception as e:
        logger.warning(f"Yahoo Finance API failed for {symbol}: {e}")
    
    # Final fallback - return defaults
    logger.warning(f"All metadata sources failed for {symbol}, using defaults")
    return {
        'company_name': symbol,
        'industry': 'Unknown',
        'sector': 'Unknown',
        'market_cap': 0,
        'pe_ratio': 0,
        'dividend_yield': 0,
        'country': 'US'
    }


def map_sic_to_gics(sic_code: str, sic_description: str) -> str:
    """
    Map SEC SIC code to GICS Sector.
    
    Args:
        sic_code: Standard Industrial Classification code
        sic_description: SIC description text
        
    Returns:
        GICS Sector name
    """
    try:
        sic_int = int(sic_code)
    except:
        return 'Unknown'
    
    # SIC to GICS mapping based on standard classifications
    if 100 <= sic_int <= 999:
        return 'Materials'  # Agriculture, Mining
    elif 1000 <= sic_int <= 1499:
        return 'Energy'  # Mining, Oil & Gas
    elif 1500 <= sic_int <= 1799:
        return 'Industrials'  # Construction
    elif 2000 <= sic_int <= 3999:
        if 2800 <= sic_int <= 2899:
            return 'Materials'  # Chemicals
        elif 2830 <= sic_int <= 2836:
            return 'Health Care'  # Pharmaceuticals
        elif 3570 <= sic_int <= 3579:
            return 'Information Technology'  # Computers
        elif 3600 <= sic_int <= 3699:
            return 'Information Technology'  # Electronics
        elif 3700 <= sic_int <= 3799:
            return 'Consumer Discretionary'  # Automobiles
        else:
            return 'Industrials'  # Manufacturing
    elif 4000 <= sic_int <= 4999:
        if 4800 <= sic_int <= 4899:
            return 'Communication Services'  # Communications
        else:
            return 'Utilities'  # Transportation, Utilities
    elif 5000 <= sic_int <= 5999:
        if 5200 <= sic_int <= 5399:
            return 'Consumer Staples'  # Retail - Food & Staples
        else:
            return 'Consumer Discretionary'  # Retail - General
    elif 6000 <= sic_int <= 6999:
        return 'Financials'  # Finance, Insurance, Real Estate
    elif 7000 <= sic_int <= 7999:
        if 7370 <= sic_int <= 7379:
            return 'Information Technology'  # Software & Services
        else:
            return 'Industrials'  # Services
    elif 8000 <= sic_int <= 8999:
        if 8000 <= sic_int <= 8099:
            return 'Health Care'  # Health Services
        else:
            return 'Industrials'  # Services
    else:
        # Check description keywords as fallback
        desc_lower = sic_description.lower()
        if any(word in desc_lower for word in ['software', 'computer', 'electronic']):
            return 'Information Technology'
        elif any(word in desc_lower for word in ['pharmaceutical', 'drug', 'medical', 'health']):
            return 'Health Care'
        elif any(word in desc_lower for word in ['bank', 'financial', 'insurance']):
            return 'Financials'
        else:
            return 'Unknown'


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
        
        # Build historical data points (market cap will be added after fetching shares outstanding)
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
        
        # Fetch shares outstanding BEFORE building history (needed for market cap in each data point)
        shares_outstanding = None
        shares_outstanding_history = []  # Will store all historical shares outstanding filings
        try:
            # Use SEC's company tickers to get CIK
            sec_tickers = load_sec_company_tickers()
            company_info = None
            for key, company in sec_tickers.items():
                if company.get('ticker', '').upper() == symbol.upper():
                    company_info = company
                    break
            
            if company_info:
                cik = str(company_info.get('cik_str', '')).zfill(10)
                
                # Get company facts (includes shares outstanding)
                time.sleep(0.2)  # SEC rate limiting
                facts_url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
                facts_headers = {
                    'User-Agent': 'Cosine-AI stock-data-loader contact@cosine-ai.com',
                    'Accept-Encoding': 'gzip, deflate'
                }
                facts_req = urllib.request.Request(facts_url, headers=facts_headers)
                
                with urllib.request.urlopen(facts_req, timeout=10) as facts_response:
                    # Handle gzip-compressed response
                    response_data = facts_response.read()
                    
                    # Check if response is gzipped (starts with 0x1f8b magic bytes)
                    if response_data[:2] == b'\x1f\x8b':
                        response_data = gzip.decompress(response_data)
                    
                    facts_data = json.loads(response_data.decode('utf-8'))
                    
                    us_gaap = facts_data.get('facts', {}).get('us-gaap', {})
                    
                    # Try multiple fields for shares outstanding (in priority order)
                    share_fields = [
                        'WeightedAverageNumberOfSharesOutstandingBasic',  # Most reliable
                        'CommonStockSharesOutstanding',
                        'EntityCommonStockSharesOutstanding',
                        'CommonStockSharesIssued'
                    ]
                    
                    # Get ALL historical shares outstanding (not just most recent)
                    shares_outstanding_history = []
                    
                    for field in share_fields:
                        if field in us_gaap:
                            units = us_gaap[field].get('units', {}).get('shares', [])
                            if units:
                                # Get all non-zero values with their dates
                                non_zero_units = [u for u in units if u.get('val', 0) > 0]
                                if non_zero_units:
                                    # Convert to list of (date, shares) tuples
                                    for unit in non_zero_units:
                                        end_date = unit.get('end', '')
                                        shares = unit.get('val', 0)
                                        if end_date and shares > 0:
                                            shares_outstanding_history.append({
                                                'date': end_date,
                                                'shares': shares,
                                                'field': field
                                            })
                                    
                                    # If we found any data, use this field
                                    if shares_outstanding_history:
                                        # Sort by date (oldest first)
                                        shares_outstanding_history.sort(key=lambda x: x['date'])
                                        logger.info(f"✅ Found {len(shares_outstanding_history)} shares outstanding records for {symbol} (field: {field}, range: {shares_outstanding_history[0]['date']} to {shares_outstanding_history[-1]['date']})")
                                        
                                        # Set the most recent as current shares outstanding
                                        shares_outstanding = shares_outstanding_history[-1]['shares']
                                        break
                
        except Exception as e:
            logger.warning(f"Could not fetch shares outstanding from SEC for {symbol}: {e}")
        
        # Fallback: Try Financial Modeling Prep free API if SEC failed
        if not shares_outstanding or shares_outstanding == 0:
            try:
                fmp_url = f"https://financialmodelingprep.com/api/v3/profile/{symbol}?apikey=demo"
                fmp_headers = {'User-Agent': 'Mozilla/5.0'}
                fmp_req = urllib.request.Request(fmp_url, headers=fmp_headers)
                
                with urllib.request.urlopen(fmp_req, timeout=5) as fmp_response:
                    fmp_data = json.loads(fmp_response.read().decode())
                    if fmp_data and len(fmp_data) > 0:
                        profile = fmp_data[0]
                        shares_outstanding = profile.get('sharesOutstanding', 0)
                        if shares_outstanding > 0:
                            logger.info(f"✅ Found shares outstanding for {symbol} from FMP: {shares_outstanding:,}")
            except Exception as fmp_error:
                logger.warning(f"Could not fetch shares from FMP for {symbol}: {fmp_error}")
        
        # Add market cap to each historical data point using historically accurate shares outstanding
        if shares_outstanding_history:
            # We have historical shares outstanding data - use the correct value for each period
            for point in history:
                close_price = point.get('close', 0)
                point_date = point.get('date', '')  # ISO format date string
                
                if close_price and close_price > 0 and point_date:
                    # Find the most recent shares outstanding filing BEFORE or AT this date
                    applicable_shares = None
                    for shares_record in shares_outstanding_history:
                        if shares_record['date'] <= point_date:
                            applicable_shares = shares_record['shares']
                        else:
                            # We've gone past the point date, stop searching
                            break
                    
                    if applicable_shares:
                        point['market_cap'] = int(applicable_shares * close_price)
                    else:
                        # No filing before this date, use the earliest available
                        point['market_cap'] = int(shares_outstanding_history[0]['shares'] * close_price)
                else:
                    point['market_cap'] = 0
            
            logger.info(f"✅ Added historically accurate market cap to {len(history)} data points for {symbol} using {len(shares_outstanding_history)} SEC filings")
        
        elif shares_outstanding and shares_outstanding > 0:
            # Fallback: Only have current shares outstanding (from FMP), apply to all periods
            # This is less accurate but better than nothing
            for point in history:
                close_price = point.get('close', 0)
                if close_price and close_price > 0:
                    point['market_cap'] = int(shares_outstanding * close_price)
                else:
                    point['market_cap'] = 0
            logger.warning(f"⚠️ Using current shares outstanding for all historical periods for {symbol} (historical data not available)")
        
        else:
            # No shares outstanding available at all, set market cap to 0 for all points
            for point in history:
                point['market_cap'] = 0
            logger.warning(f"⚠️ No shares outstanding found for {symbol}, market cap set to 0")
        
        # Get current market cap (from last data point - already calculated above)
        market_cap = history[-1].get('market_cap', 0) if history else 0
        
        return {
            'symbol': symbol,
            'currency': meta.get('currency', 'USD'),
            'exchange': meta.get('exchangeName', 'UNKNOWN'),
            'instrument_type': meta.get('instrumentType', 'EQUITY'),
            
            # Company metadata
            'company_name': detailed_metadata['company_name'],
            'industry': detailed_metadata['industry'],
            'sector': detailed_metadata['sector'],
            'market_cap': market_cap,  # Current market cap (from last data point)
            'shares_outstanding': shares_outstanding if shares_outstanding else 0,  # Shares outstanding (relatively static)
            'pe_ratio': detailed_metadata.get('pe_ratio', 0),  # P/E ratio
            'dividend_yield': detailed_metadata.get('dividend_yield', 0),  # Dividend yield
            'country': detailed_metadata['country'],
            
            'data_points': len(history),
            'first_date': history[0]['date'] if history else None,
            'last_date': history[-1]['date'] if history else None,
            'history': history  # Each point now includes market_cap calculated from price × shares
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
    
    Can be invoked in three modes:
    1. Single Stock Mode ('symbol' in event): Fetches data for one symbol and returns it
    2. Batch Processing Mode ('symbols' in event): Processes a batch of symbols
    3. Batch Generation Mode (neither): Generates batches for Step Functions
    """
    try:
        logger.info("=== Stock Data Historical Loader Started ===")
        logger.info(f"Event: {json.dumps(event, default=str)}")
        
        # Check for single stock mode (for manual testing/API Gateway)
        if 'symbol' in event and 'symbols' not in event:
            logger.info("Mode: Single Stock")
            symbol = event['symbol']
            years = event.get('years', 5)
            
            logger.info(f"Fetching historical data for {symbol} ({years} years)")
            
            # Fetch the data using the same function as batch processing
            data = fetch_historical_data(symbol, years)
            
            if data is None:
                return {
                    'statusCode': 400,
                    'body': json.dumps({
                        'error': 'Failed to fetch data from Yahoo Finance',
                        'symbol': symbol
                    })
                }
            
            # Return the data directly (for testing/debugging)
            return {
                'statusCode': 200,
                'body': json.dumps(data, default=str)
            }
        
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

