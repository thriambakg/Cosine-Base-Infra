"""
Lambda function to calculate date range and route to appropriate executor.
Returns routing decision for Step Functions.
"""

import json
from datetime import datetime, timezone
from typing import Dict, Any

def lambda_handler(event: Dict, context: Any) -> Dict:
    """
    Calculate days between start_date and end_date, return routing decision.
    Congress number is auto-detected by the fetcher scripts, so we don't need to pass it.
    
    Expected event:
    {
        "start_date": "12/08/2025",  # mm/dd/yyyy format
        "end_date": "12/09/2025"     # mm/dd/yyyy format
    }
    
    Returns:
    {
        "use_glue": true/false,  # true if > 2 days
        "days_diff": 2,
        "start_date": "2025-12-08T00:00:00Z",  # ISO format for API
        "end_date": "2025-12-09T23:59:59Z"      # ISO format for API
    }
    """
    start_date_str = event.get("start_date")
    end_date_str = event.get("end_date")
    
    if not start_date_str or not end_date_str:
        raise ValueError("start_date and end_date are required")
    
    # Parse mm/dd/yyyy format
    try:
        start_date = datetime.strptime(start_date_str, "%m/%d/%Y")
        end_date = datetime.strptime(end_date_str, "%m/%d/%Y")
    except ValueError as e:
        raise ValueError(f"Invalid date format. Expected mm/dd/yyyy, got: {start_date_str} or {end_date_str}. Error: {str(e)}")
    
    # Set timezone to UTC
    start_date = start_date.replace(tzinfo=timezone.utc)
    end_date = end_date.replace(tzinfo=timezone.utc)
    
    # Set start_date to beginning of day (00:00:00)
    start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
    # Set end_date to end of day (23:59:59)
    end_date = end_date.replace(hour=23, minute=59, second=59, microsecond=999999)
    
    # Calculate days difference
    days_diff = (end_date - start_date).total_seconds() / (24 * 60 * 60)
    
    # Route to Glue if > 2 days, otherwise Lambda
    use_glue = days_diff > 2.0
    
    # Convert to ISO format strings for API calls
    start_date_iso = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_date_iso = end_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    return {
        "use_glue": use_glue,
        "days_diff": days_diff,
        "start_date": start_date_iso,
        "end_date": end_date_iso
    }


