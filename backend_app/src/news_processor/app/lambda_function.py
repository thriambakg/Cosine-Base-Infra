import json
import boto3
import os
import re
from datetime import datetime, timedelta
from deterministic_tokenizer import tokenizer

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

def extract_title_keywords(title, description=""):
    """Extract meaningful keywords from article using deterministic tokenization"""
    if not title:
        return []
    
    try:
        # Use deterministic tokenizer for consistent results
        tokenization_result = tokenizer.tokenize_article(title, description, max_tokens=100)
        
        # Combine tokens and phrases for comprehensive indexing
        all_keywords = tokenization_result['tokens'] + tokenization_result['phrases']
        
        logger.info(f"Extracted {len(all_keywords)} keywords using deterministic tokenization")
        logger.info(f"Sample keywords: {all_keywords[:10]}")
        
        return all_keywords
        
    except Exception as e:
        logger.error(f"Error extracting keywords with deterministic tokenizer: {str(e)}")
        # Fallback to simple regex extraction
        ticker_pattern = r'\b[A-Z]{1,5}\b'
        tickers = re.findall(ticker_pattern, title)
        return tickers[:5]

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
    
    # Extract meaningful keywords using Amazon Comprehend
    title_keywords = extract_title_keywords(article['title'], article.get('description', ''))
    
    # Combine with API-provided keywords
    api_keywords = article.get('keywords', []) or []
    combined_keywords = title_keywords + api_keywords
    unique_keywords = list(dict.fromkeys(combined_keywords))  # Remove duplicates, preserve order
    
    # Store the main article record with all tokens for scan-based search
    main_item = {
        'PK': f"NEWS#{date_str}",
        'SK': article['article_id'],
        'title': article['title'],
        'description': article.get('description', ''),
        'source_url': article['link'],
        'source_name': article.get('source_name', article.get('source_id', 'Unknown')),
        'published_date': article['pubDate'],
        
        # Store all tokens as comma-separated string for scan filtering
        'tokens': ','.join(unique_keywords),  # All deterministic tokens
        'keywords': ','.join(api_keywords),   # Original API keywords for reference
        'category': ','.join(article.get('category', []) or []),
        'sentiment': article.get('sentiment', 'neutral'),
        'ai_tag': ','.join(article.get('ai_tag', []) or []),
        'image_url': article.get('image_url'),
        'creator': ','.join(article.get('creator', []) or []),
        'country': ','.join(article.get('country', []) or []),
        'language': article.get('language', 'english'),
        'ttl': ttl,
        
        # Keep some GSIs for non-keyword searches (source, category, sentiment)
        'GSI1PK': f"CATEGORY#{(article.get('category', []) or ['general'])[0]}",
        'GSI1SK': article['pubDate'],
        
        'GSI2PK': f"SENTIMENT#{article.get('sentiment', 'neutral')}",
        'GSI2SK': article['pubDate'],
        
        'GSI3PK': f"TAG#{(article.get('ai_tag', []) or ['general'])[0]}",
        'GSI3SK': article['pubDate'],
        
        'GSI4PK': f"SOURCE#{article.get('source_name', article.get('source_id', 'Unknown'))}",
        'GSI4SK': article['pubDate']
    }
    
    # Store only the main article record - no separate keyword entries
    table.put_item(Item=main_item)
    
    logger.info(f"Stored article with {len(unique_keywords)} deterministic tokens: {article.get('title', 'Unknown')}")
    logger.info(f"Sample tokens: {unique_keywords[:10]}")

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
