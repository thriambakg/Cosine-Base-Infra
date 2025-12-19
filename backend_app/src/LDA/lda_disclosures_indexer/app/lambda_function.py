"""
LDA Disclosures Indexer Lambda Handler
Routes to filings.py or contributions.py based on endpoint.
Processes a single page with 25 parallel workers.
"""

import json
import os
from typing import Dict

# Import processing modules
from filings import process_filings_page
from contributions import process_contributions_page

def lambda_handler(event, context):
    """
    Indexer Lambda: Processes a single page of filings or contributions
    
    Input:
    {
        "page": 2,
        "start_date": "2025-01-01",
        "end_date": "2025-01-31",
        "endpoint": "filings"  # or "contributions"
    }
    
    Output:
    {
        "statusCode": 200,
        "page": 2,
        "endpoint": "filings",
        "processed_count": 25,
        "success": true
    }
    """
    page = event.get('page')
    start_date = event.get('start_date')
    end_date = event.get('end_date')
    endpoint = event.get('endpoint')
    
    # Normalize empty strings to None (Step Functions might pass "" for null)
    if start_date == "" or start_date is None:
        start_date = None
    if end_date == "" or end_date is None:
        end_date = None
    
    if not page:
        return {
            'statusCode': 400,
            'error': 'Missing required parameter: page'
        }
    
    if not endpoint or endpoint not in ['filings', 'contributions']:
        return {
            'statusCode': 400,
            'error': 'Missing or invalid parameter: endpoint (must be "filings" or "contributions")'
        }
    
    try:
        if endpoint == 'filings':
            result = process_filings_page(page, start_date, end_date)
        else:  # contributions
            result = process_contributions_page(page, start_date, end_date)
        
        return {
            'statusCode': 200,
            'page': page,
            'endpoint': endpoint,
            'processed_count': result.get('processed_count', 0),
            'success': True
        }
    except Exception as e:
        print(f"❌ Error processing page {page} for {endpoint}: {str(e)}")
        return {
            'statusCode': 500,
            'page': page,
            'endpoint': endpoint,
            'error': str(e),
            'success': False
        }

