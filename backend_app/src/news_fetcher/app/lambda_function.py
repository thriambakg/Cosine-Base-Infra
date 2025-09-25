import os
import json
import boto3
from datetime import datetime
from newsdataapi import NewsDataApiClient
import time

# Placeholder for logger, assuming it's configured elsewhere or will be added
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """News fetcher Lambda using NewsData.io Python client"""
    
    # Initialize NewsData.io client
    api_key = get_secret_value('newsdata-api-key')
    api = NewsDataApiClient(apikey=api_key)
    
    # Fetch financial news
    articles = fetch_financial_news(api)
    
    # Send to SQS for processing
    send_to_sqs(articles)
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': f'Successfully queued {len(articles)} articles',
            'articles_count': len(articles)
        })
    }

def fetch_financial_news(api):
    """Fetch financial news using NewsData.io Python client with timeframe filtering"""
    
    import time
    
    # Use latest endpoint to get the most recent articles
    # This avoids timezone overlap issues and ensures fresh content
    # The removeduplicate=1 parameter handles duplicates within the query
    logger.info("Fetching latest articles (no timeframe to avoid overlap issues)")
    
    # Rotate queries to get diverse content
    query_sets = [
        'stock market OR trading OR "market analysis"',
        'earnings OR "quarterly results" OR "financial results"',
        'IPO OR merger OR acquisition OR "stock split"',
        '"Federal Reserve" OR "interest rates" OR inflation',
        '"market rally" OR "market crash" OR "bull market" OR "bear market"',
        'technology OR "tech stocks" OR "AI stocks"',
        'energy OR "oil prices" OR "renewable energy"',
        'volatility OR "market volatility" OR "VIX"'
    ]
    
    # Select query based on current time to rotate through different topics
    current_time_unix = int(time.time())
    query_index = (current_time_unix // 480) % len(query_sets)  # Rotate every 8 minutes
    selected_query = query_sets[query_index]
    
    logger.info(f"Using query: {selected_query}")
    
    all_articles = []
    seen_articles = set()  # Track article IDs to avoid duplicates within this fetch
    
    try:
        # Use the official Python client with latest endpoint (no timeframe)
        response = api.news_api(
            qInMeta=selected_query,
            country="us",
            language="en",
            category="business,science,technology,politics",  # Multiple categories for broader coverage
            # No timeframe parameter - gets latest articles to avoid overlap issues
            size=10,  # Free tier limit: 10 articles per request
            removeduplicate=1,  # Remove duplicates at API level
            prioritydomain="top",  # Get articles from top 10% news domains
            image=1,  # Only articles with featured images
            full_content=0  # Don't fetch full content to save bandwidth
        )
        
        articles = response.get('results', [])
        
        # Filter out duplicates within this fetch
        for article in articles:
            article_id = article.get('article_id')
            if article_id and article_id not in seen_articles:
                seen_articles.add(article_id)
                all_articles.append(article)
        
        logger.info(f"Fetched {len(all_articles)} unique latest articles")
        
    except Exception as e:
        logger.error(f"Error fetching news for query '{selected_query}': {str(e)}")
    
    return all_articles

def get_secret_value(secret_name):
    """Get API key from AWS Secrets Manager"""
    try:
        client = boto3.client('secretsmanager')
        # Assuming PROJECT_NAME and ENVIRONMENT are set as environment variables in Lambda
        secret_full_name = f"{os.environ['PROJECT_NAME']}-{secret_name}-{os.environ['ENVIRONMENT']}"
        response = client.get_secret_value(
            SecretId=secret_full_name
        )
        secret_data = json.loads(response['SecretString'])
        return secret_data['api_key']
    except Exception as e:
        logger.error(f"Error retrieving secret {secret_name}: {str(e)}")
        raise

def send_to_sqs(articles):
    """Send articles to SQS for processing"""
    sqs = boto3.client('sqs')
    queue_url = os.environ['SQS_QUEUE_URL']
    
    for article in articles:
        try:
            sqs.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps(article)
            )
        except Exception as e:
            logger.error(f"Error sending article to SQS: {str(e)}")
            continue
