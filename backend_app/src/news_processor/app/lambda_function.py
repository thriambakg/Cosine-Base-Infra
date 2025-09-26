import json
import boto3
import os
import re
from datetime import datetime, timedelta
from financial_keywords import find_keywords_in_text

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

def extract_title_keywords(title):
    """Extract meaningful keywords from article title using comprehensive keyword database"""
    if not title:
        return []
    
    # Use the comprehensive keyword database for title analysis
    title_keywords = find_keywords_in_text(title, use_binary_search=True)
    
    # Also extract company/ticker symbols (uppercase words, 1-5 chars)
    ticker_pattern = r'\b[A-Z]{1,5}\b'
    tickers = re.findall(ticker_pattern, title)
    
    # Combine and deduplicate
    all_keywords = title_keywords + tickers
    unique_keywords = list(dict.fromkeys(all_keywords))  # Preserves order
    
    # Return top 10 most relevant keywords (increased from 5 for better coverage)
    return unique_keywords[:10]

def store_article(table, article):
    """Store article in DynamoDB with proper GSI structure and duplicate detection"""
    
    # Check for duplicate before storing
    if is_duplicate_article(table, article):
        logger.info(f"Skipping duplicate article: {article.get('title', 'Unknown')}")
        return
    
    # Calculate TTL (30 days from now)
    ttl = int((datetime.utcnow() + timedelta(days=30)).timestamp())
    
    # Extract date for partition key
    # Ensure pubDate is in a format compatible with fromisoformat
    published_date_str = article['pubDate'].replace('Z', '+00:00') if 'Z' in article['pubDate'] else article['pubDate']
    published_date = datetime.fromisoformat(published_date_str)
    date_str = published_date.strftime('%Y-%m-%d')
    
    # Extract meaningful keywords from title and description
    title_keywords = extract_title_keywords(article['title'])
    description_keywords = find_keywords_in_text(article.get('description', ''), use_binary_search=True)
    
    # Combine all keyword sources
    api_keywords = article.get('keywords', []) or []
    combined_keywords = title_keywords + description_keywords + api_keywords
    unique_keywords = list(dict.fromkeys(combined_keywords))  # Remove duplicates, preserve order
    
    # Main table item
    item = {
        'PK': f"NEWS#{date_str}",
        'SK': article['article_id'],
        'title': article['title'],
        'description': article.get('description', ''),
        'source_url': article['link'],
        'source_name': article.get('source_name', article.get('source_id', 'Unknown')),
        'published_date': article['pubDate'],
        'keywords': ','.join(unique_keywords),  # Use improved keywords
        'category': ','.join(article.get('category', []) or []),
        'sentiment': article.get('sentiment', 'neutral'),
        'ai_tag': ','.join(article.get('ai_tag', []) or []),
        'image_url': article.get('image_url'),
        'creator': ','.join(article.get('creator', []) or []),
        'country': ','.join(article.get('country', []) or []),
        'language': article.get('language', 'english'),
        'ttl': ttl,
        
        # GSI1: Category-based search
        'GSI1PK': f"CATEGORY#{(article.get('category', []) or ['general'])[0]}",  # Use first category
        'GSI1SK': article['pubDate'],
        
        # GSI2: Sentiment-based search
        'GSI2PK': f"SENTIMENT#{article.get('sentiment', 'neutral')}",
        'GSI2SK': article['pubDate'],
        
        # GSI3: AI Tag-based search
        'GSI3PK': f"TAG#{(article.get('ai_tag', []) or ['general'])[0]}",  # Use first AI tag
        'GSI3SK': article['pubDate'],
        
        # GSI4: Keywords-based search (use first meaningful keyword from title)
        'GSI4PK': f"KEYWORD#{(unique_keywords or ['general'])[0]}",  # Use first improved keyword
        'GSI4SK': article['pubDate'],
        
        # GSI5: Source-based search
        'GSI5PK': f"SOURCE#{article.get('source_name', article.get('source_id', 'Unknown'))}",
        'GSI5SK': article['pubDate']
    }
    
    table.put_item(Item=item)
    logger.info(f"Stored new article: {article.get('title', 'Unknown')}")

def is_duplicate_article(table, article):
    """Check if article already exists in database"""
    try:
        # Check by article_id (primary key)
        response = table.get_item(
            Key={
                'PK': f"NEWS#{datetime.fromisoformat(article['pubDate'].replace('Z', '+00:00')).strftime('%Y-%m-%d')}",
                'SK': article['article_id']
            }
        )
        
        if 'Item' in response:
            return True
        
        # Also check by URL to catch duplicates with different article_ids
        response = table.scan(
            FilterExpression='source_url = :url',
            ExpressionAttributeValues={
                ':url': article['link']
            },
            Limit=1
        )
        
        return len(response['Items']) > 0
        
    except Exception as e:
        logger.error(f"Error checking for duplicate article: {str(e)}")
        # If we can't check, assume it's not a duplicate to avoid missing articles
        return False

# Removed helper functions - now using NewsData.io's built-in fields:
# - keywords: article.get('keywords', [])
# - category: article.get('category', [])  
# - sentiment: article.get('sentiment', 'neutral')
# - ai_tag: article.get('ai_tag', [])
