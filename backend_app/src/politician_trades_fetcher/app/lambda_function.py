"""
Politician Trades Fetcher Lambda
Fetches SEC forms (3, 4, 5) and Congressional PTRs (House/Senate) and stores in S3

This is Step 1 of the 3-step politician trades aggregation workflow.
"""

import json
import os
import logging
import boto3
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import requests
import time

# Import web scraper helper
from webscraper import CongressionalPTRScraper

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS clients
s3_client = boto3.client('s3')

# Environment variables
S3_BUCKET = os.environ.get('S3_BUCKET')

# SEC EDGAR API configuration
SEC_EDGAR_BASE_URL = "https://data.sec.gov"
SEC_USER_AGENT = "Cosine Financial Platform contact@cosine.financial"  # SEC requires contact info

def get_yesterday_date() -> str:
    """Get yesterday's date in YYYY-MM-DD format"""
    yesterday = datetime.now() - timedelta(days=1)
    return yesterday.strftime('%Y-%m-%d')

def fetch_sec_forms(target_date: str) -> List[Dict[str, Any]]:
    """
    Fetch SEC Forms 3, 4, 5 from EDGAR API for a specific date
    
    Args:
        target_date: Date in YYYY-MM-DD format
        
    Returns:
        List of form metadata dicts with keys: form_type, cik, accession_number, filename, filing_date
    """
    logger.info(f"📋 Fetching SEC forms for date: {target_date}")
    
    forms = []
    form_types = ['3', '4', '5']
    
    # Create requests session with required headers
    session = requests.Session()
    session.headers.update({
        'User-Agent': SEC_USER_AGENT,
        'Accept': 'application/json'
    })
    
    try:
        # Note: SEC EDGAR API doesn't have a simple "get all forms by date" endpoint
        # For production, you would need to:
        # 1. Use RSS feeds to get recent filings
        # 2. Parse RSS XML to extract CIK, accession numbers, filing dates
        # 3. Filter by target_date
        # 4. Construct download URLs from accession numbers
        
        # For Forms 3, 4, 5, use RSS feed:
        # https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&company=&count=100
        
        for form_type in form_types:
            # RSS feed for recent filings (last 100)
            rss_url = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type={form_type}&company=&count=100"
            
            try:
                response = session.get(rss_url, timeout=30)
                response.raise_for_status()
                
                # Parse RSS feed (XML)
                # TODO: Full implementation would:
                # 1. Parse RSS XML using xml.etree.ElementTree
                # 2. Extract <item> elements
                # 3. Parse <pubDate> to match target_date
                # 4. Extract CIK from <guid> or <link>
                # 5. Extract accession number from URL
                # 6. Determine filename (usually ends in .txt or .xml for primary document)
                
                logger.info(f"📥 Fetched RSS feed for Form {form_type} - parsing not yet implemented")
                
                # Placeholder: In production, parse RSS and filter by date
                # For now, this function returns empty list as a placeholder
                # Full implementation would populate forms list with:
                # forms.append({
                #     'form_type': form_type,
                #     'cik': extracted_cik,
                #     'accession_number': extracted_accession,
                #     'filename': extracted_filename,
                #     'filing_date': target_date
                # })
                
            except Exception as e:
                logger.error(f"❌ Error fetching Form {form_type} RSS feed: {e}")
                continue
            
            # Rate limiting: SEC requires 10 requests/second max
            time.sleep(0.2)  # 200ms = 5 requests/second (safe margin)
    
    except Exception as e:
        logger.error(f"❌ Error in SEC forms fetch: {e}")
    
    logger.info(f"📋 Found {len(forms)} SEC forms for {target_date}")
    return forms

def download_and_store_sec_form(form_data: Dict[str, Any], target_date: str) -> Optional[str]:
    """
    Download SEC form file and store in S3
    
    Args:
        form_data: Form metadata (CIK, accession number, form type)
        target_date: Date string for S3 key structure
        
    Returns:
        S3 key if successful, None otherwise
    """
    try:
        # Construct SEC filing URL
        # Format: https://www.sec.gov/Archives/edgar/data/{CIK}/{accession_no}/{filename}
        
        cik = form_data.get('cik')
        accession = form_data.get('accession_number')
        form_type = form_data.get('form_type')
        filename = form_data.get('filename')
        
        if not all([cik, accession, filename]):
            logger.warning(f"⚠️ Missing required fields for form download")
            return None
        
        # Download form file
        file_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession}/{filename}"
        
        session = requests.Session()
        session.headers.update({'User-Agent': SEC_USER_AGENT})
        
        response = session.get(file_url, timeout=30)
        response.raise_for_status()
        
        # Determine file type and S3 key
        file_ext = 'xml' if filename.endswith('.xml') else 'pdf'
        s3_key = f"trades/{target_date}/sec/{form_type}-{cik}-{target_date}.{file_ext}"
        
        # Upload to S3
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=response.content,
            ContentType='application/xml' if file_ext == 'xml' else 'application/pdf'
        )
        
        logger.info(f"✅ Stored SEC form to S3: {s3_key}")
        return s3_key
        
    except Exception as e:
        logger.error(f"❌ Error downloading/storing SEC form: {e}")
        return None

def lambda_handler(event, context):
    """
    Lambda handler for fetching SEC forms and Congressional PTRs
    
    Expected input:
    {
        "date": "2024-01-15"  // Optional, defaults to yesterday
    }
    
    Returns:
    {
        "date": "2024-01-15",
        "secFormsFetched": 1250,
        "housePTRsFetched": 15,
        "senatePTRsFetched": 8,
        "secForms": [...],
        "housePTRs": [...],
        "senatePTRs": [...]
    }
    """
    logger.info("🚀 Politician Trades Fetcher Lambda started")
    
    # Get target date from event
    # Step Functions from EventBridge passes: {"source": "scheduler-daily", "timestamp": "..."}
    # For daily runs, we always want yesterday's date (previous trading day)
    # Direct invocations can pass explicit date
    if isinstance(event, dict) and event.get('date'):
        target_date = event.get('date')
    else:
        # Default to yesterday for scheduled runs (previous trading day)
        target_date = get_yesterday_date()
    
    logger.info(f"📅 Processing date: {target_date}")
    
    if not S3_BUCKET:
        raise ValueError("S3_BUCKET environment variable not set")
    
    # Initialize results
    results = {
        'date': target_date,
        'secFormsFetched': 0,
        'housePTRsFetched': 0,
        'senatePTRsFetched': 0,
        'secForms': [],
        'housePTRs': [],
        'senatePTRs': []
    }
    
    try:
        # Step 1: Fetch SEC Forms (3, 4, 5)
        logger.info("📋 Fetching SEC Forms 3, 4, 5...")
        sec_forms = fetch_sec_forms(target_date)
        
        # Download and store each form
        for form_data in sec_forms:
            s3_key = download_and_store_sec_form(form_data, target_date)
            if s3_key:
                results['secForms'].append({
                    'formType': form_data.get('form_type'),
                    'cik': form_data.get('cik'),
                    'filingDate': target_date,
                    's3Key': s3_key
                })
                results['secFormsFetched'] += 1
        
        logger.info(f"✅ Fetched {results['secFormsFetched']} SEC forms")
        
        # Step 2: Fetch House PTRs
        logger.info("🏛️ Fetching House PTRs...")
        scraper = CongressionalPTRScraper()
        house_ptrs = scraper.fetch_house_ptrs(target_date)
        
        # Download and store each House PTR
        for ptr_data in house_ptrs:
            if scraper.download_ptr_file(
                ptr_data.get('url'),
                ptr_data.get('s3_key'),
                S3_BUCKET,
                s3_client
            ):
                results['housePTRs'].append(ptr_data)
                results['housePTRsFetched'] += 1
        
        logger.info(f"✅ Fetched {results['housePTRsFetched']} House PTRs")
        
        # Step 3: Fetch Senate PTRs
        logger.info("🏛️ Fetching Senate PTRs...")
        senate_ptrs = scraper.fetch_senate_ptrs(target_date)
        
        # Download and store each Senate PTR
        for ptr_data in senate_ptrs:
            if scraper.download_ptr_file(
                ptr_data.get('url'),
                ptr_data.get('s3_key'),
                S3_BUCKET,
                s3_client
            ):
                results['senatePTRs'].append(ptr_data)
                results['senatePTRsFetched'] += 1
        
        logger.info(f"✅ Fetched {results['senatePTRsFetched']} Senate PTRs")
        
        # Summary
        total_fetched = (
            results['secFormsFetched'] + 
            results['housePTRsFetched'] + 
            results['senatePTRsFetched']
        )
        
        logger.info(f"✅ Total forms fetched: {total_fetched}")
        
        # Return dict directly for Step Functions (not wrapped in statusCode/body)
        return results
        
    except Exception as e:
        logger.error(f"❌ Fatal error in fetcher Lambda: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise

