"""
Utility Lambda to split date ranges into 5-day batches for Step Functions
This prevents payload size limits when processing large date ranges.
"""

import json
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def split_date_range(start_date_str: str, end_date_str: str, batch_size: int = 5) -> List[Dict[str, str]]:
    """
    Split a date range into batches of specified size (default 5 days)
    
    Args:
        start_date_str: Start date in YYYY-MM-DD format
        end_date_str: End date in YYYY-MM-DD format
        batch_size: Number of days per batch (default 5)
        
    Returns:
        List of batch dicts with startDate and endDate keys
    """
    try:
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
        
        if start_date > end_date:
            raise ValueError(f"startDate ({start_date_str}) must be <= endDate ({end_date_str})")
        
        batches = []
        current_start = start_date
        
        while current_start <= end_date:
            # Calculate batch end (batch_size - 1 days later, or end_date if sooner)
            batch_end = min(
                current_start + timedelta(days=batch_size - 1),
                end_date
            )
            
            batches.append({
                'startDate': current_start.strftime('%Y-%m-%d'),
                'endDate': batch_end.strftime('%Y-%m-%d')
            })
            
            # Move to next batch start (1 day after current batch end)
            current_start = batch_end + timedelta(days=1)
        
        logger.info(f"📅 Split {start_date_str} to {end_date_str} into {len(batches)} batches of up to {batch_size} days")
        return batches
        
    except ValueError as e:
        logger.error(f"❌ Invalid date format: {e}")
        raise


def lambda_handler(event, context):
    """
    Lambda handler for splitting date ranges into batches
    
    Expected input:
    {
        "startDate": "2025-10-01",
        "endDate": "2025-10-31"
    }
    
    Returns:
    {
        "batches": [
            {"startDate": "2025-10-01", "endDate": "2025-10-05"},
            {"startDate": "2025-10-06", "endDate": "2025-10-10"},
            ...
        ]
    }
    """
    logger.info("🚀 Date Range Splitter Lambda started")
    
    try:
        start_date = event.get('startDate')
        end_date = event.get('endDate')
        
        if not start_date or not end_date:
            raise ValueError("Both startDate and endDate must be provided")
        
        batches = split_date_range(start_date, end_date, batch_size=5)
        
        # Return batches directly (Step Functions will handle the response)
        return {
            'batches': batches,
            'totalBatches': len(batches),
            'startDate': start_date,
            'endDate': end_date
        }
        
    except Exception as e:
        logger.error(f"❌ Error splitting date range: {e}")
        raise

