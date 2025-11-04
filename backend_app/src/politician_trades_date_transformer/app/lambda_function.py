"""
Lambda function to transform date range or single date into array of dates for Step Functions.

Input formats:
1. Date range:
   {
     "startDate": "2025-10-25",
     "endDate": "2025-10-31"
   }

2. Single date:
   {
     "date": "2025-10-30"
   }

Output:
   {
     "dates": ["2025-10-25", "2025-10-26", ..., "2025-10-31"]
   }
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event: Dict[str, Any], context) -> Dict[str, Any]:
    """
    Transform date range or single date into array of dates
    
    Args:
        event: Input with either:
            - {"startDate": "YYYY-MM-DD", "endDate": "YYYY-MM-DD"} (date range)
            - {"date": "YYYY-MM-DD"} (single date)
    
    Returns:
        {"dates": ["YYYY-MM-DD", ...]} - Array of dates
    """
    logger.info("🚀 Date Transformer Lambda started")
    logger.info(f"📥 Input: {json.dumps(event)}")
    
    dates: List[str] = []
    
    try:
        # Check if it's a date range
        if "startDate" in event and "endDate" in event:
            start_date_str = event["startDate"]
            end_date_str = event["endDate"]
            
            try:
                start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
                end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()
                
                if start_date > end_date:
                    raise ValueError("startDate must be <= endDate")
                
                # Generate list of dates from start to end (inclusive)
                current_date = start_date
                while current_date <= end_date:
                    dates.append(current_date.strftime("%Y-%m-%d"))
                    current_date += timedelta(days=1)
                
                logger.info(f"📅 Date range mode: {start_date_str} to {end_date_str} ({len(dates)} dates)")
            except ValueError as e:
                logger.error(f"❌ Invalid date format or range: {e}")
                raise ValueError(f"Invalid date range: startDate='{start_date_str}', endDate='{end_date_str}'. Expected YYYY-MM-DD format.")
        
        # Check if it's a single date
        elif "date" in event:
            date_str = event["date"]
            
            try:
                # Validate date format
                datetime.strptime(date_str, "%Y-%m-%d")
                dates = [date_str]
                logger.info(f"📅 Single date mode: {date_str}")
            except ValueError as e:
                logger.error(f"❌ Invalid date format: {e}")
                raise ValueError(f"Invalid date format: '{date_str}'. Expected YYYY-MM-DD format.")
        
        else:
            raise ValueError("Input must contain either 'startDate'/'endDate' (date range) or 'date' (single date)")
        
        if not dates:
            raise ValueError("No dates generated from input")
        
        result = {
            "dates": dates
        }
        
        logger.info(f"✅ Generated {len(dates)} date(s): {dates[0] if len(dates) == 1 else f'{dates[0]} to {dates[-1]}'}")
        
        return result
        
    except Exception as e:
        logger.error(f"❌ Error in date transformer: {e}")
        raise

