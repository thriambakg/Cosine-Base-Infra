"""
Lambda function to calculate date range and route to appropriate executor.
Returns routing decision for Step Functions.
"""

import json
from datetime import datetime, timezone, timedelta
from typing import Dict, Any

def lambda_handler(event: Dict, context: Any) -> Dict:
    """
    Calculate days between start_date and end_date, return routing decision.
    Congress number is auto-detected by the fetcher scripts, so we don't need to pass it.
    
    Expected event (manual):
    {
        "start_date": "12/08/2025",  # mm/dd/yyyy format
        "end_date": "12/09/2025",    # mm/dd/yyyy format
        "source": null                # null for manual runs
    }
    
    Expected event (scheduler):
    {
        "start_date": null,
        "end_date": null,
        "source": "scheduler"          # "scheduler" for automated runs
    }
    
    When source is "scheduler", calculates yesterday's date (if triggered on 12/11/2025 12:00 AM, uses 12/10/2025).
    
    Returns:
    {
        "use_glue": true/false,  # true if > 2 days
        "days_diff": 1,
        "start_date": "2025-12-10T00:00:00Z",  # ISO format for API
        "end_date": "2025-12-10T23:59:59Z"      # ISO format for API
    }
    """
    source = event.get("source")
    start_date_str = event.get("start_date")
    end_date_str = event.get("end_date")
    
    # If source is "scheduler", calculate yesterday's date
    if source == "scheduler":
        # Get current UTC time
        now = datetime.now(timezone.utc)
        # Calculate yesterday (previous day)
        yesterday = now - timedelta(days=1)
        # Format as mm/dd/yyyy
        start_date_str = yesterday.strftime("%m/%d/%Y")
        end_date_str = yesterday.strftime("%m/%d/%Y")
    
    # Validate that we have dates
    if not start_date_str or not end_date_str:
        raise ValueError("start_date and end_date are required. If source is 'scheduler', dates will be calculated automatically.")
    
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
    
    # Calculate days difference (inclusive) - use date difference, not time difference
    # For same day: days_diff = 1
    # For consecutive days: days_diff = 2, etc.
    start_date_only = start_date.date()
    end_date_only = end_date.date()
    days_diff = (end_date_only - start_date_only).days + 1
    
    # Route to Glue if > 2 days, otherwise Lambda
    use_glue = days_diff > 2.0
    
    # Convert to ISO format strings for API calls
    start_date_iso = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_date_iso = end_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    return {
        "use_glue": use_glue,
        "days_diff": int(days_diff),
        "start_date": start_date_iso,
        "end_date": end_date_iso
    }


