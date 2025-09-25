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
    """Fetch financial news using NewsData.io Python client"""
    
    queries = [
        'stock market OR trading OR "market analysis"',
        'earnings OR "quarterly results" OR "financial results"',
        'IPO OR merger OR acquisition OR "stock split"',
        '"Federal Reserve" OR "interest rates" OR inflation',
        '"market rally" OR "market crash" OR "bull market" OR "bear market"'
    ]
    
    all_articles = []
    
    for query in queries:
        try:
            # Use the official Python client with qInMeta
            response = api.news_api(
                qInMeta=query,
                country="us",
                language="en",
                category="business"
            )
            
            articles = response.get('results', [])
            all_articles.extend(articles)
            
            # Rate limiting
            time.sleep(1)
            
        except Exception as e:
            logger.error(f"Error fetching news for query '{query}': {str(e)}")
            continue
    
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
