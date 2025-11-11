"""
Politician Trades Fetcher Lambda
Fetches Congressional PTRs (House/Senate) metadata

This is Step 1 of the politician trades aggregation workflow.
Note: SEC forms are now handled by a separate Glue job.
"""

import json
import os
import logging
from typing import List, Dict, Any
from datetime import datetime, timedelta
import time

# Configure logging (must be before imports that use logger)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Import web scraper helper
try:
    from webscraper import CongressionalPTRScraper
except ImportError as e:
    logger.error(f"❌ Failed to import webscraper: {e}")
    raise

# Environment variables
S3_BUCKET = os.environ.get('S3_BUCKET')

def get_today_date() -> str:
    """Get today's date in YYYY-MM-DD format"""
    today = datetime.now().date()
    return today.strftime('%Y-%m-%d')

def lambda_handler(event, context):
    """
    Lambda handler for fetching Congressional PTRs
    
    Supports three input modes:
    
    1. Single date (backwards compatible):
    {
        "date": "2025-10-30"
    }
    
    2. Date range (for backfilling historical data):
    {
        "startDate": "2025-01-01",
        "endDate": "2025-12-31"
    }
    
    3. Default (from EventBridge daily scheduler):
    {
        "source": "scheduler-daily",
        "backdate": null,
        "date": null
    }
    # Defaults to today's date (matches Glue job behavior)
    
    Returns:
    {
        "date": "2025-10-30",  # First date (for backwards compatibility)
        "dateRange": {  # Present only if date range was provided
            "startDate": "2025-01-01",
            "endDate": "2025-12-31",
            "totalDays": 365
        },
        "housePTRsFetched": 0,
        "senatePTRsFetched": 8,
        "housePTRs": [...],
        "senatePTRs": [...]
    }
    
    Note: For large date ranges, consider breaking into smaller chunks to avoid Lambda timeout.
    Recommended: Process 30-90 days at a time for optimal performance.
    """
    logger.info("🚀 Politician Trades Fetcher Lambda started")
    
    if not S3_BUCKET:
        raise ValueError("S3_BUCKET environment variable not set")
    
    # Parse date input - support backdate, single date, or default
    # Options:
    # 1. Backdate: {"backdate": "2025-11-05"} - fetches from today back to backdate
    # 2. Single date: {"date": "2025-10-30"}
    # 3. Default: today's date (for scheduled runs - matches Glue job behavior)
    target_dates = []
    
    if isinstance(event, dict):
        # Handle backdate (check for None, empty string, or string "null")
        backdate_str = event.get('backdate')
        if backdate_str and backdate_str.strip() != '' and backdate_str.lower() != 'null':
            # Backdate mode - fetch from today back to backdate
            try:
                backdate_obj = datetime.strptime(backdate_str, '%Y-%m-%d').date()
                today = datetime.now().date()
                
                if backdate_obj > today:
                    raise ValueError("backdate must be <= today")
                
                # Generate list of dates from today back to backdate (inclusive)
                current_date = today
                while current_date >= backdate_obj:
                    target_dates.append(current_date.strftime('%Y-%m-%d'))
                    current_date -= timedelta(days=1)
                
                logger.info(f"📅 Backdate mode: {backdate_str} to {today} ({len(target_dates)} days)")
            except ValueError as e:
                logger.error(f"❌ Invalid backdate format: {e}")
                raise ValueError(f"Invalid backdate: backdate='{backdate_str}'. Expected YYYY-MM-DD format and must be <= today.")
        elif event.get('date'):
            # Single date mode (backwards compatible)
            target_dates = [event.get('date')]
            logger.info(f"📅 Single date mode: {target_dates[0]}")
        else:
            # Default: today for scheduled runs (matches Glue job behavior)
            target_dates = [get_today_date()]
            logger.info(f"📅 Default date mode (today): {target_dates[0]}")
    else:
        # Default: today for scheduled runs (matches Glue job behavior)
        target_dates = [get_today_date()]
        logger.info(f"📅 Default date mode (today): {target_dates[0]}")
    
    logger.info(f"📅 Processing {len(target_dates)} date(s): {target_dates[0] if len(target_dates) == 1 else f'{target_dates[0]} to {target_dates[-1]}'}")
    
    # Initialize aggregate results
    aggregate_results = {
        'dateRange': {
            'startDate': target_dates[0],
            'endDate': target_dates[-1],
            'totalDays': len(target_dates)
        } if len(target_dates) > 1 else {'singleDate': target_dates[0]},
        'housePTRsFetched': 0,
        'senatePTRsFetched': 0,
        'housePTRs': [],
        'senatePTRs': []
    }
    
    try:
        # Process each date in the range
        for date_index, target_date in enumerate(target_dates, 1):
            logger.info(f"📅 Processing date {date_index}/{len(target_dates)}: {target_date}")
            
            # Initialize scraper (reused for both House and Senate)
            scraper = CongressionalPTRScraper()
            
            # Step 1: Fetch House PTRs
            # House PTRs are searched by filing year (not date)
            # Extract year from target_date
            target_year = target_date.split('-')[0] if '-' in target_date else str(datetime.now().year)
            logger.info(f"🏛️ Fetching House PTRs for filing year: {target_year}...")
            house_ptrs = scraper.fetch_house_ptrs(filing_year=target_year)
            
            # Return metadata for downloader Lambda
            # The downloader will download them and use Textract to filter by actual filing date
            for ptr_data in house_ptrs:
                # Add source field if not present
                if 'source' not in ptr_data:
                    ptr_data['source'] = 'house'
                # Ensure formType is set
                if 'formType' not in ptr_data and 'form_type' not in ptr_data:
                    ptr_data['formType'] = 'house_ptr'
                    ptr_data['form_type'] = 'house_ptr'
                
                aggregate_results['housePTRs'].append(ptr_data)
                aggregate_results['housePTRsFetched'] += 1
            
            logger.info(f"✅ Found {len(house_ptrs)} House PTRs for {target_year} (total so far: {aggregate_results['housePTRsFetched']})")
            
            # Step 2: Fetch Senate PTRs (metadata only - downloader will download them)
            logger.info(f"🏛️ Fetching Senate PTRs for {target_date}...")
            senate_ptrs = scraper.fetch_senate_ptrs(target_date)
            
            # Return metadata for downloader Lambda
            # The downloader will download them and use Textract to filter by actual filing date
            for ptr_data in senate_ptrs:
                # Add source field if not present
                if 'source' not in ptr_data:
                    ptr_data['source'] = 'senate'
                # Ensure formType is set
                if 'formType' not in ptr_data and 'form_type' not in ptr_data:
                    ptr_data['formType'] = 'senate_ptr'
                    ptr_data['form_type'] = 'senate_ptr'
                
                aggregate_results['senatePTRs'].append(ptr_data)
                aggregate_results['senatePTRsFetched'] += 1
            
            logger.info(f"✅ Found {len(senate_ptrs)} Senate PTRs for {target_date} (total so far: {aggregate_results['senatePTRsFetched']})")
            
            # Add a small delay between dates to avoid rate limiting (for date ranges)
            if len(target_dates) > 1 and date_index < len(target_dates):
                time.sleep(1)  # 1 second delay between dates
        
        # Summary
        total_fetched = (
            aggregate_results['housePTRsFetched'] + 
            aggregate_results['senatePTRsFetched']
        )
        
        logger.info(f"✅ Total forms fetched across {len(target_dates)} date(s): {total_fetched}")
        logger.info(f"   - House PTRs: {aggregate_results['housePTRsFetched']}")
        logger.info(f"   - Senate PTRs: {aggregate_results['senatePTRsFetched']}")
        
        # For backwards compatibility with Step Functions, also include 'date' field
        # Use the first date if range, or the single date
        aggregate_results['date'] = target_dates[0]
        
        # Log the return value for debugging
        logger.info(f"📤 Returning results: {json.dumps({k: v if k != 'senatePTRs' else f'[{len(v)} items]' for k, v in aggregate_results.items()}, default=str)}")
        
        # Return dict directly for Step Functions (not wrapped in statusCode/body)
        # Step Functions expects a JSON-serializable dict
        return aggregate_results
        
    except Exception as e:
        logger.error(f"❌ Fatal error in fetcher Lambda: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise
