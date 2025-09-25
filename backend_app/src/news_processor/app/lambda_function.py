import json
import boto3
import os
from datetime import datetime, timedelta

# Placeholder for logger
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """Process news articles from SQS and store in DynamoDB"""
    
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(os.environ['NEWS_TABLE_NAME'])
    
    processed_count = 0
    
    for record in event['Records']:
        try:
            article = json.loads(record['body'])
            store_article(table, article)
            processed_count += 1
            
        except Exception as e:
            logger.error(f"Error processing article: {str(e)}")
            continue
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': f'Successfully processed {processed_count} articles'
        })
    }

def store_article(table, article):
    """Store article in DynamoDB with proper GSI structure"""
    
    # Calculate TTL (30 days from now)
    ttl = int((datetime.utcnow() + timedelta(days=30)).timestamp())
    
    # Extract date for partition key
    # Ensure pubDate is in a format compatible with fromisoformat
    published_date_str = article['pubDate'].replace('Z', '+00:00') if 'Z' in article['pubDate'] else article['pubDate']
    published_date = datetime.fromisoformat(published_date_str)
    date_str = published_date.strftime('%Y-%m-%d')
    
    # Main table item
    item = {
        'PK': f"NEWS#{date_str}",
        'SK': article['article_id'],
        'title': article['title'],
        'description': article.get('description', ''),
        'source_url': article['link'],
        'source_name': article.get('source_priority', article.get('source_id', 'Unknown')),
        'published_date': article['pubDate'],
        'keywords': ','.join(extract_keywords(article)),
        'sector': classify_sector(article),
        'sentiment': analyze_sentiment(article),
        'image_url': article.get('image_url'),
        'creator': ','.join(article.get('creator', [])),
        'ttl': ttl,
        
        # GSI1: Title-based search
        'GSI1PK': 'TITLE',
        'GSI1SK': article['title'],
        
        # GSI2: Date-based chronological
        'GSI2PK': 'DATE',
        'GSI2SK': article['pubDate'],
        
        # GSI3: Source-based
        'GSI3PK': f"SOURCE#{article.get('source_priority', article.get('source_id', 'Unknown'))}",
        'GSI3SK': article['pubDate']
    }
    
    table.put_item(Item=item)

def extract_keywords(article):
    """Extract keywords from article title and description"""
    text = f"{article.get('title', '')} {article.get('description', '')}"
    
    # Common stock symbols to look for (example, expand as needed)
    stock_symbols = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 'META', 'NVDA', 'NFLX']
    found_symbols = []
    
    for symbol in stock_symbols:
        if symbol in text.upper():
            found_symbols.append(symbol)
    
    # Also consider using article.get('keywords', []) if NewsData.io provides them
    return found_symbols

def classify_sector(article):
    """Classify article into business sectors"""
    text = f"{article.get('title', '')} {article.get('description', '')}".lower()
    
    if any(word in text for word in ['technology', 'tech', 'software', 'ai', 'artificial intelligence']):
        return 'Technology'
    elif any(word in text for word in ['bank', 'financial', 'finance', 'banking']):
        return 'Financial'
    elif any(word in text for word in ['healthcare', 'medical', 'pharmaceutical']):
        return 'Healthcare'
    elif any(word in text for word in ['energy', 'oil', 'gas', 'renewable']):
        return 'Energy'
    else:
        return 'General'

def analyze_sentiment(article):
    """Basic sentiment analysis"""
    text = f"{article.get('title', '')} {article.get('description', '')}".lower()
    
    positive_words = ['surge', 'rise', 'gain', 'up', 'positive', 'growth', 'profit']
    negative_words = ['fall', 'drop', 'decline', 'down', 'negative', 'loss', 'crash']
    
    positive_count = sum(1 for word in positive_words if word in text)
    negative_count = sum(1 for word in negative_words if word in text)
    
    if positive_count > negative_count:
        return 'positive'
    elif negative_count > positive_count:
        return 'negative'
    else:
        return 'neutral'
