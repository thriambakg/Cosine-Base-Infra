"""
Stock Data Batch Fetcher Lambda
Fetches stock data from external APIs and sends to SQS for processing
"""

import json
import os
import time
import logging
import boto3
import yfinance as yf
import requests
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import signal
import sys

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS clients
sqs = boto3.client('sqs')
secrets_manager = boto3.client('secretsmanager')

# Configuration
MAX_BATCH_SIZE = 100  # Maximum stocks per batch
MAX_RETRIES = 3
RETRY_DELAY = 1.0

# Stock priority tiers
HIGH_PRIORITY_STOCKS = [
    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'BRK-B', 'UNH', 'JNJ',
    'V', 'PG', 'JPM', 'XOM', 'HD', 'CVX', 'MA', 'PFE', 'ABBV', 'BAC',
    'KO', 'AVGO', 'PEP', 'TMO', 'COST', 'WMT', 'DHR', 'VZ', 'ADBE', 'ACN',
    'NFLX', 'CRM', 'TXN', 'QCOM', 'NKE', 'LIN', 'ABT', 'NEE', 'PM', 'RTX',
    'HON', 'UNP', 'LOW', 'SPGI', 'INTU', 'IBM', 'CAT', 'GE', 'AMD', 'PYPL'
]

MEDIUM_PRIORITY_STOCKS = [
    'DIS', 'AMGN', 'T', 'GS', 'AXP', 'BKNG', 'SYK', 'BLK', 'EL', 'GILD',
    'MDT', 'ISRG', 'TJX', 'LMT', 'VRTX', 'ZTS', 'ADP', 'MMM', 'CVS', 'CI',
    'SO', 'DUK', 'AON', 'ITW', 'PNC', 'USB', 'TGT', 'CME', 'ICE', 'SPG',
    'PLD', 'EQIX', 'CCI', 'AMT', 'SBAC', 'EXR', 'PSA', 'AVB', 'EQR', 'MAA',
    'UDR', 'ESS', 'CPT', 'AIV', 'BXP', 'KIM', 'REG', 'SLG', 'VTR', 'WELL'
]

def get_alpha_vantage_api_key() -> Optional[str]:
    """Get Alpha Vantage API key from Secrets Manager"""
    try:
        secret_name = f"cosine-alpha-vantage-api-{os.environ.get('ENVIRONMENT', 'production')}"
        response = secrets_manager.get_secret_value(SecretId=secret_name)
        return response['SecretString']
    except Exception as e:
        logger.error(f"Failed to retrieve Alpha Vantage API key: {str(e)}")
        return None

def categorize_metrics(data: Dict[str, Any]) -> Dict[str, str]:
    """Categorize metrics into High/Medium/Low for GSI filtering"""
    
    def categorize_volatility(volatility: float) -> str:
        """Categorize volatility: 0-30% = LOW, 30-60% = MEDIUM, 60%+ = HIGH"""
        if volatility < 30:
            return "LOW"
        elif volatility < 60:
            return "MEDIUM"
        else:
            return "HIGH"
    
    def categorize_price_change(price_change: float) -> str:
        """Categorize price change: -100% to +100% range"""
        if price_change < -20:
            return "VERY_LOW"
        elif price_change < -5:
            return "LOW"
        elif price_change < 5:
            return "MEDIUM"
        elif price_change < 20:
            return "HIGH"
        else:
            return "VERY_HIGH"
    
    def categorize_market_cap(market_cap: float) -> str:
        """Categorize market cap: <1B = MICRO, 1-10B = SMALL, 10-100B = MEDIUM, 100B+ = LARGE"""
        if market_cap < 1_000_000_000:
            return "MICRO"
        elif market_cap < 10_000_000_000:
            return "SMALL"
        elif market_cap < 100_000_000_000:
            return "MEDIUM"
        else:
            return "LARGE"
    
    def categorize_price(price: float) -> str:
        """Categorize price: <$10 = LOW, $10-100 = MEDIUM, $100+ = HIGH"""
        if price < 10:
            return "LOW"
        elif price < 100:
            return "MEDIUM"
        else:
            return "HIGH"
    
    return {
        "volatility_category": categorize_volatility(data.get('volatility', 0)),
        "price_change_category": categorize_price_change(data.get('price_change_percent', 0)),
        "market_cap_category": categorize_market_cap(data.get('market_cap', 0)),
        "price_category": categorize_price(data.get('current_price', 0))
    }

def fetch_stock_data_yfinance(symbols: List[str], timeframe: str = "1d") -> Dict[str, Any]:
    """Fetch stock data using yfinance batch API"""
    try:
        logger.info(f"Fetching data for {len(symbols)} symbols using yfinance")
        
        # Create Tickers object for batch processing
        tickers = yf.Tickers(' '.join(symbols))
        
        results = {}
        for symbol in symbols:
            try:
                ticker = tickers.tickers[symbol]
                
                # Get current info
                info = ticker.info
                
                # Get historical data for volatility calculation
                hist = ticker.history(period=timeframe, interval="1d")
                
                if hist.empty or info is None:
                    logger.warning(f"No data available for {symbol}")
                    continue
                
                # Calculate volatility (standard deviation of daily returns)
                if len(hist) > 1:
                    returns = hist['Close'].pct_change().dropna()
                    volatility = returns.std() * 100  # Convert to percentage
                else:
                    volatility = 0
                
                # Get current price and change
                current_price = info.get('currentPrice', hist['Close'].iloc[-1] if not hist.empty else 0)
                previous_close = info.get('previousClose', hist['Close'].iloc[-2] if len(hist) > 1 else current_price)
                price_change = current_price - previous_close
                price_change_percent = (price_change / previous_close * 100) if previous_close != 0 else 0
                
                results[symbol] = {
                    'symbol': symbol,
                    'current_price': current_price,
                    'price_change': price_change,
                    'price_change_percent': price_change_percent,
                    'volatility': volatility,
                    'market_cap': info.get('marketCap', 0),
                    'industry': info.get('industry', 'Unknown'),
                    'sector': info.get('sector', 'Unknown'),
                    'volume': info.get('volume', 0),
                    'pe_ratio': info.get('trailingPE', 0),
                    'eps': info.get('trailingEps', 0),
                    'dividend_yield': info.get('dividendYield', 0),
                    'beta': info.get('beta', 0),
                    'timestamp': datetime.utcnow().isoformat(),
                    'data_source': 'yfinance'
                }
                
                # Add categorization
                categories = categorize_metrics(results[symbol])
                results[symbol].update(categories)
                
            except Exception as e:
                logger.error(f"Error fetching data for {symbol}: {str(e)}")
                continue
        
        logger.info(f"Successfully fetched data for {len(results)} symbols")
        return results
        
    except Exception as e:
        logger.error(f"Error in yfinance batch fetch: {str(e)}")
        return {}

def fetch_stock_data_alpha_vantage(symbols: List[str]) -> Dict[str, Any]:
    """Fetch stock data using Alpha Vantage API as fallback"""
    api_key = get_alpha_vantage_api_key()
    if not api_key:
        logger.error("Alpha Vantage API key not available")
        return {}
    
    try:
        logger.info(f"Fetching data for {len(symbols)} symbols using Alpha Vantage")
        
        results = {}
        for symbol in symbols:
            try:
                # Alpha Vantage Global Quote API
                url = f"https://www.alphavantage.co/query"
                params = {
                    'function': 'GLOBAL_QUOTE',
                    'symbol': symbol,
                    'apikey': api_key
                }
                
                response = requests.get(url, params=params, timeout=10)
                response.raise_for_status()
                data = response.json()
                
                if 'Global Quote' not in data:
                    logger.warning(f"No data available for {symbol} from Alpha Vantage")
                    continue
                
                quote = data['Global Quote']
                
                current_price = float(quote.get('05. price', 0))
                price_change = float(quote.get('09. change', 0))
                price_change_percent = float(quote.get('10. change percent', '0%').replace('%', ''))
                volume = int(quote.get('06. volume', 0))
                
                results[symbol] = {
                    'symbol': symbol,
                    'current_price': current_price,
                    'price_change': price_change,
                    'price_change_percent': price_change_percent,
                    'volatility': 0,  # Alpha Vantage doesn't provide volatility in global quote
                    'market_cap': 0,  # Would need separate API call
                    'industry': 'Unknown',
                    'sector': 'Unknown',
                    'volume': volume,
                    'pe_ratio': 0,
                    'eps': 0,
                    'dividend_yield': 0,
                    'beta': 0,
                    'timestamp': datetime.utcnow().isoformat(),
                    'data_source': 'alpha_vantage'
                }
                
                # Add categorization
                categories = categorize_metrics(results[symbol])
                results[symbol].update(categories)
                
                # Rate limiting - Alpha Vantage has strict limits
                time.sleep(0.2)  # 5 calls per second max
                
            except Exception as e:
                logger.error(f"Error fetching Alpha Vantage data for {symbol}: {str(e)}")
                continue
        
        logger.info(f"Successfully fetched Alpha Vantage data for {len(results)} symbols")
        return results
        
    except Exception as e:
        logger.error(f"Error in Alpha Vantage batch fetch: {str(e)}")
        return {}

def send_to_sqs(queue_url: str, data: Dict[str, Any]) -> bool:
    """Send stock data to SQS queue for processing"""
    try:
        message_body = json.dumps(data)
        
        response = sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=message_body,
            MessageAttributes={
                'symbol': {
                    'StringValue': data.get('symbol', ''),
                    'DataType': 'String'
                },
                'data_source': {
                    'StringValue': data.get('data_source', ''),
                    'DataType': 'String'
                },
                'timestamp': {
                    'StringValue': data.get('timestamp', ''),
                    'DataType': 'String'
                }
            }
        )
        
        logger.info(f"Sent message to SQS for {data.get('symbol')}: {response['MessageId']}")
        return True
        
    except Exception as e:
        logger.error(f"Error sending message to SQS: {str(e)}")
        return False

def get_stock_list_by_priority(priority_tier: str) -> List[str]:
    """Get stock list based on priority tier"""
    if priority_tier == "high":
        return HIGH_PRIORITY_STOCKS
    elif priority_tier == "medium":
        return MEDIUM_PRIORITY_STOCKS
    else:
        # For low priority, combine both lists
        return HIGH_PRIORITY_STOCKS + MEDIUM_PRIORITY_STOCKS

def lambda_handler(event, context):
    """Main Lambda handler"""
    try:
        logger.info(f"Stock Data Batch Fetcher started: {json.dumps(event)}")
        
        # Get configuration from event or environment
        priority_tier = event.get('priority_tier', 'high')
        timeframe = event.get('timeframe', '1d')
        queue_url = os.environ.get('SQS_QUEUE_URL')
        
        if not queue_url:
            raise ValueError("SQS_QUEUE_URL environment variable not set")
        
        # Get stock list based on priority
        stock_symbols = get_stock_list_by_priority(priority_tier)
        
        # Limit batch size
        if len(stock_symbols) > MAX_BATCH_SIZE:
            stock_symbols = stock_symbols[:MAX_BATCH_SIZE]
        
        logger.info(f"Processing {len(stock_symbols)} stocks for {priority_tier} priority tier")
        
        # Try yfinance first
        stock_data = fetch_stock_data_yfinance(stock_symbols, timeframe)
        
        # If yfinance fails or returns limited data, try Alpha Vantage
        if len(stock_data) < len(stock_symbols) * 0.5:  # Less than 50% success rate
            logger.warning("yfinance returned limited data, trying Alpha Vantage fallback")
            alpha_vantage_data = fetch_stock_data_alpha_vantage(stock_symbols)
            
            # Merge results, preferring yfinance data
            for symbol, data in alpha_vantage_data.items():
                if symbol not in stock_data:
                    stock_data[symbol] = data
        
        # Send each stock's data to SQS
        success_count = 0
        for symbol, data in stock_data.items():
            if send_to_sqs(queue_url, data):
                success_count += 1
        
        logger.info(f"Successfully processed {success_count}/{len(stock_data)} stocks")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Stock data batch fetch completed',
                'processed_stocks': success_count,
                'total_stocks': len(stock_symbols),
                'priority_tier': priority_tier,
                'timeframe': timeframe
            })
        }
        
    except Exception as e:
        logger.error(f"Error in stock data batch fetcher: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'message': 'Stock data batch fetch failed'
            })
        }
