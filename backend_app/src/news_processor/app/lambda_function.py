import json
import boto3
import os
from datetime import datetime, timedelta
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
    """Store article in DynamoDB with title-based GSI5 for searching"""
    
    # Check for duplicate before storing
    if is_duplicate_article(table, article):
        logger.info(f"Skipping duplicate article: {article.get('title', 'Unknown')}")
        return
    
    # Calculate TTL (30 days from now)
    ttl = int((datetime.utcnow() + timedelta(days=30)).timestamp())
    
    # Extract date for partition key
    published_date_str = article['pubDate'].replace('Z', '+00:00') if 'Z' in article['pubDate'] else article['pubDate']
    published_date = datetime.fromisoformat(published_date_str)
    date_str = published_date.strftime('%Y-%m-%d')
    
    # Get API-provided keywords (if any)
    api_keywords = article.get('keywords', []) or []
    keywords_str = ','.join(api_keywords) if api_keywords else ''
    
    # Store the main article record
    main_item = {
        'PK': f"NEWS#{date_str}",
        'SK': article['article_id'],
        'title': article['title'],
        'description': article.get('description', ''),
        'source_url': article['link'],
        'source_name': article.get('source_name', article.get('source_id', 'Unknown')),
        'published_date': article['pubDate'],
        
        # Store API keywords for reference (optional)
        'keywords': keywords_str,
        'category': ','.join(article.get('category', []) or []),
        'sentiment': article.get('sentiment', 'neutral'),
        'ai_tag': ','.join(article.get('ai_tag', []) or []),
        'image_url': article.get('image_url'),
        'creator': ','.join(article.get('creator', []) or []),
        'country': ','.join(article.get('country', []) or []),
        'language': article.get('language', 'english'),
        'ttl': ttl,
        
        # GSI1: Category-based search
        'GSI1PK': f"CATEGORY#{(article.get('category', []) or ['general'])[0]}",
        'GSI1SK': article['pubDate'],
        
        # GSI2: Sentiment-based search
        'GSI2PK': f"SENTIMENT#{article.get('sentiment', 'neutral')}",
        'GSI2SK': article['pubDate'],
        
        # GSI3: AI Tag-based search
        'GSI3PK': f"TAG#{(article.get('ai_tag', []) or ['general'])[0]}",
        'GSI3SK': article['pubDate'],
        
        # GSI4: Source-based search
        'GSI4PK': f"SOURCE#{article.get('source_name', article.get('source_id', 'Unknown'))}",
        'GSI4SK': article['pubDate'],
        
        # GSI5: Title-based search using contains() filter
        'GSI5PK': "TITLE_SEARCH",  # Constant for all articles
        'GSI5SK': article['title'].lower()  # Lowercase title for case-insensitive search
    }
    
    # Store the article
    table.put_item(Item=main_item)
    
    logger.info(f"Stored article: {article.get('title', 'Unknown')}")

def is_duplicate_article(table, article):
    """Check if article already exists in database"""
    try:
        # Extract date for partition key
        published_date_str = article['pubDate'].replace('Z', '+00:00') if 'Z' in article['pubDate'] else article['pubDate']
        published_date = datetime.fromisoformat(published_date_str)
        date_str = published_date.strftime('%Y-%m-%d')
        
        # Check by article_id (primary key)
        response = table.get_item(
            Key={
                'PK': f"NEWS#{date_str}",
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
