"""
LDA Disclosures Indexer Lambda Handler
Processes individual pages from SQS queue.
Each page is processed with 25 parallel workers.
Implements rate limiting with exponential backoff.
"""

import json
import os
import time
from typing import Dict, Optional
from botocore.exceptions import ClientError
import requests

# Import processing modules
from filings import process_filings_page
from contributions import process_contributions_page

def lambda_handler(event, context):
    """
    Indexer Lambda: Processes a single page from SQS
    
    SQS Event Format:
    {
        "Records": [
            {
                "body": "{\"page\": 1, \"endpoint\": \"filings\", \"start_date\": \"2025-01-01\", \"end_date\": null, \"testing_limit\": null}"
            }
        ]
    }
    
    Message Body:
    {
        "page": 1,
        "endpoint": "filings" or "contributions",
        "start_date": "2025-01-01" (optional),
        "end_date": "2025-01-31" (optional),
        "testing_limit": 10 (optional)
    }
    """
    # Handle SQS event
    if 'Records' in event:
        # Process all records (should be 1 per invocation due to batch_size=1)
        total_processed = 0
        for record in event['Records']:
            try:
                message_body = json.loads(record['body'])
                result = process_single_page_with_retry(message_body)
                total_processed += result.get('processed_count', 0)
            except Exception as e:
                print(f"❌ Error processing SQS record: {str(e)}")
                raise
        
        return {
            'statusCode': 200,
            'total_processed': total_processed
        }
    else:
        # Direct invocation (for testing)
        return process_single_page_with_retry(event)

def process_single_page_with_retry(message_body: Dict, max_retries: int = 5) -> Dict:
    """Process a single page with exponential backoff retry on rate limiting"""
    page = message_body.get('page')
    endpoint = message_body.get('endpoint')
    start_date = message_body.get('start_date')
    end_date = message_body.get('end_date')
    testing_limit = message_body.get('testing_limit')
    
    # Normalize empty strings to None
    if start_date == "" or start_date is None:
        start_date = None
    if end_date == "" or end_date is None:
        end_date = None
    if testing_limit == "" or testing_limit is None:
        testing_limit = None
    
    if not page:
        raise ValueError('Missing required parameter: page')
    
    if not endpoint or endpoint not in ['filings', 'contributions']:
        raise ValueError(f'Invalid endpoint: {endpoint} (must be "filings" or "contributions")')
    
    # Retry logic with exponential backoff
    delay = 1  # Start with 1 second
    last_exception = None
    
    for attempt in range(max_retries + 1):
        try:
            print(f"📄 Processing {endpoint} page {page} (attempt {attempt + 1}/{max_retries + 1})...")
            
            if endpoint == 'filings':
                result = process_filings_page(page, start_date, end_date)
            else:  # contributions
                result = process_contributions_page(page, start_date, end_date)
            
            print(f"✅ Page {page} complete: {result.get('processed_count', 0)} items processed")
            return result
            
        except requests.exceptions.HTTPError as e:
            # Check if it's an HTTP 429 (rate limit)
            if e.response and e.response.status_code == 429:
                if attempt < max_retries:
                    print(f"⚠️  Rate limited (429) on page {page}, retrying in {delay}s...")
                    time.sleep(delay)
                    delay = min(delay * 2, 60)  # Exponential backoff, max 60 seconds
                    last_exception = e
                    continue
                else:
                    print(f"❌ Rate limited on page {page} after {max_retries} retries")
                    raise
            else:
                # Not a rate limit error, re-raise immediately
                raise
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            # Check if it's a rate limiting error (429 or throttling)
            if error_code in ['ThrottlingException', 'TooManyRequestsException'] or \
               e.response.get('ResponseMetadata', {}).get('HTTPStatusCode') == 429:
                if attempt < max_retries:
                    print(f"⚠️  Rate limited on page {page}, retrying in {delay}s...")
                    time.sleep(delay)
                    delay = min(delay * 2, 60)  # Exponential backoff, max 60 seconds
                    last_exception = e
                    continue
                else:
                    print(f"❌ Rate limited on page {page} after {max_retries} retries")
                    raise
            else:
                # Not a rate limit error, re-raise immediately
                raise
        except Exception as e:
            # Check if it's an HTTP 429 or rate limit related
            error_str = str(e).lower()
            if '429' in error_str or 'rate limit' in error_str or 'too many requests' in error_str:
                if attempt < max_retries:
                    print(f"⚠️  Rate limited on page {page}, retrying in {delay}s...")
                    time.sleep(delay)
                    delay = min(delay * 2, 60)  # Exponential backoff, max 60 seconds
                    last_exception = e
                    continue
                else:
                    print(f"❌ Rate limited on page {page} after {max_retries} retries")
                    raise
            else:
                # Not a rate limit error, re-raise immediately
                raise
    
    # If we exhausted retries, raise the last exception
    if last_exception:
        raise last_exception
    
    return {'processed_count': 0, 'success': False}

