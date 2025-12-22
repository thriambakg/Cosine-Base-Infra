"""
LDA Disclosures Indexer Lambda Handler
Processes individual pages from SQS queue.
Each page is processed with 25 parallel workers.
Implements rate limiting with exponential backoff.
Failed messages are automatically sent to DLQ by SQS after max_receive_count (3) failed attempts.
"""

import json
import os
import time
import re
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
                # Log error - SQS will automatically retry and move to DLQ after max_receive_count (3) failures
                error_msg = str(e)[:500]
                print(f"❌ Error processing SQS record: {error_msg}")
                # Re-raise exception so SQS can track the failure and retry
                # After max_receive_count failures, SQS will automatically move message to DLQ
                raise
        
        return {
            'statusCode': 200,
            'total_processed': total_processed
        }
    else:
        # Direct invocation (for testing)
        return process_single_page_with_retry(event)

def process_single_page_with_retry(message_body: Dict, max_retries: int = 6) -> Dict:
    """Process a single page with exponential backoff retry on rate limiting
    
    For 429 errors: Retries with exponential backoff, using "Expected available in X seconds" if provided
    For other errors: Sends to DLQ for future redriving
    """
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
    base_delay = 1  # Start with 1 second
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
                    # Try to parse "Expected available in X seconds" from response
                    retry_after = None
                    try:
                        error_data = e.response.json()
                        detail = error_data.get('detail', '')
                        if 'Expected available in' in detail:
                            # Extract number from "Expected available in X second(s)"
                            match = re.search(r'(\d+)', detail)
                            if match:
                                retry_after = int(match.group(1)) + 1  # Add 1 second buffer
                    except:
                        pass
                    
                    # Use retry_after if available, otherwise exponential backoff
                    if retry_after:
                        delay = retry_after
                    else:
                        delay = min(base_delay * (2 ** attempt), 60)  # Exponential backoff, max 60 seconds
                    
                    print(f"⚠️  Rate limited (429) on page {page}, retrying in {delay}s...")
                    time.sleep(delay)
                    last_exception = e
                    continue
                else:
                    error_msg = f'Rate limited on page {page} after {max_retries} retries'
                    print(f"❌ {error_msg}")
                    # SQS will automatically move to DLQ after max_receive_count failures
                    raise Exception(error_msg)
            else:
                # Not a rate limit error - SQS will automatically move to DLQ after max_receive_count failures
                error_msg = f'HTTP {e.response.status_code if e.response else "unknown"} on page {page}: {str(e)[:200]}'
                print(f"❌ {error_msg}")
                raise Exception(error_msg)
                
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            # Check if it's a rate limiting error (429 or throttling)
            if error_code in ['ThrottlingException', 'TooManyRequestsException'] or \
               e.response.get('ResponseMetadata', {}).get('HTTPStatusCode') == 429:
                if attempt < max_retries:
                    delay = min(base_delay * (2 ** attempt), 60)  # Exponential backoff, max 60 seconds
                    print(f"⚠️  AWS throttling on page {page}, retrying in {delay}s...")
                    time.sleep(delay)
                    last_exception = e
                    continue
                else:
                    error_msg = f'AWS throttling on page {page} after {max_retries} retries'
                    print(f"❌ {error_msg}")
                    # SQS will automatically move to DLQ after max_receive_count failures
                    raise Exception(error_msg)
            else:
                # Not a rate limit error - SQS will automatically move to DLQ after max_receive_count failures
                error_msg = f'AWS error {error_code} on page {page}: {str(e)[:200]}'
                print(f"❌ {error_msg}")
                raise Exception(error_msg)
                
        except Exception as e:
            # Check if it's an HTTP 429 or rate limit related
            error_str = str(e).lower()
            if '429' in error_str or 'rate limit' in error_str or 'too many requests' in error_str:
                if attempt < max_retries:
                    delay = min(base_delay * (2 ** attempt), 60)  # Exponential backoff, max 60 seconds
                    print(f"⚠️  Rate limited on page {page}, retrying in {delay}s...")
                    time.sleep(delay)
                    last_exception = e
                    continue
                else:
                    error_msg = f'Rate limited on page {page} after {max_retries} retries'
                    print(f"❌ {error_msg}")
                    # SQS will automatically move to DLQ after max_receive_count failures
                    raise Exception(error_msg)
            else:
                # Other errors - SQS will automatically move to DLQ after max_receive_count failures
                error_msg = f'Error processing page {page}: {str(e)[:200]}'
                print(f"❌ {error_msg}")
                raise Exception(error_msg)
    
    # If we exhausted retries, SQS will automatically move to DLQ after max_receive_count failures
    if last_exception:
        error_msg = f'Exhausted retries for page {page}'
        print(f"❌ {error_msg}")
        raise Exception(error_msg)
    
    # Should not reach here
    error_msg = f'Unknown error processing page {page}'
    raise Exception(error_msg)

