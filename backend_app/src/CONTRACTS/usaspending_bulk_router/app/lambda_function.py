"""
Lambda function to calculate date range and route to appropriate executor for USAspending.
Returns routing decision for Step Functions.
"""

import json
from datetime import datetime, timedelta
from typing import Dict, Any
from zoneinfo import ZoneInfo

def lambda_handler(event: Dict, context: Any) -> Dict:
    """
    Calculate days between START_DATE and END_DATE, return routing decision.
    
    Expected event:
    {
        "JobName": "cosine-usaspending-bulk-indexing-production",
        "AWARDS_TABLE_NAME": "cosine-usaspending-awards-index-production",
        "S3_BUCKET_NAME": "cosine-usaspending-data-production",
        "START_DATE": "2025-12-08",  # Optional, YYYY-MM-DD format
        "END_DATE": "2025-12-09",     # Optional, YYYY-MM-DD format
        "source": "scheduler"         # Optional, if present and dates not provided, uses scheduled mode
    }
    
    Scheduled mode (when source is "scheduler" or "scheduler-daily" and dates not provided):
    - start_date = previous day (yesterday)
    - end_date = previous day (yesterday) - single day range to avoid overlap
    
    Returns:
    {
        "use_glue": true/false,  # true if > 2 days
        "days_diff": 2,
        "JobName": "...",
        "AWARDS_TABLE_NAME": "...",
        "S3_BUCKET_NAME": "...",
        "START_DATE": "2025-12-08",  # YYYY-MM-DD format
        "END_DATE": "2025-12-09"     # YYYY-MM-DD format
    }
    """
    # Extract all input parameters
    job_name = event.get("JobName")
    awards_table_name = event.get("AWARDS_TABLE_NAME")
    s3_bucket_name = event.get("S3_BUCKET_NAME")
    source = event.get("source", "")
    start_date_str = event.get("START_DATE")
    end_date_str = event.get("END_DATE")
    
    # Check if this is a scheduled execution
    is_scheduled = source in ["scheduler", "scheduler-daily"]
    
    # Get current EST/EDT date for calculations (USAspending updates are based on EST)
    # zoneinfo handles EST/EDT automatically (US/Eastern)
    est_tz = ZoneInfo('US/Eastern')
    now_est = datetime.now(est_tz)
    today_est = now_est.replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday_est = today_est - timedelta(days=1)
    
    # If dates not provided, use scheduled mode defaults or fallback to yesterday
    if not start_date_str:
        if is_scheduled:
            # Scheduled mode: start_date = previous day (yesterday in EST)
            start_date_str = yesterday_est.strftime('%Y-%m-%d')
        else:
            # Manual/default mode: default to yesterday
            start_date_str = yesterday_est.strftime('%Y-%m-%d')
    
    if not end_date_str:
        if is_scheduled:
            # Scheduled mode: end_date = previous day (same as start_date) - single day range to avoid overlap
            # This ensures each daily run processes exactly one day without overlapping with the next day's run
            end_date_str = yesterday_est.strftime('%Y-%m-%d')
        else:
            # Manual/default mode: default to start_date
            end_date_str = start_date_str
    
    # Parse YYYY-MM-DD format
    try:
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
    except ValueError as e:
        raise ValueError(f"Invalid date format. Expected YYYY-MM-DD, got: {start_date_str} or {end_date_str}. Error: {str(e)}")
    
    # Validate date range
    if end_date < start_date:
        raise ValueError(f"END_DATE ({end_date_str}) must be >= START_DATE ({start_date_str})")
    
    # Calculate days difference (inclusive)
    days_diff = (end_date - start_date).days + 1
    
    # Route to Glue if > 2 days, otherwise Lambda
    use_glue = days_diff > 2
    
    return {
        "use_glue": use_glue,
        "days_diff": days_diff,
        "JobName": job_name,
        "AWARDS_TABLE_NAME": awards_table_name,
        "S3_BUCKET_NAME": s3_bucket_name,
        "START_DATE": start_date_str,
        "END_DATE": end_date_str
    }









