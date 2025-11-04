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
import xml.etree.ElementTree as ET
from urllib.parse import urlparse, parse_qs
import re

# Configure logging (must be before imports that use logger)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Import web scraper helper
try:
    from webscraper import CongressionalPTRScraper
except ImportError as e:
    logger.error(f"❌ Failed to import webscraper: {e}")
    raise

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
    Fetch SEC Forms 3, 4, 5 from EDGAR daily index files for a specific date
    
    Uses SEC's daily index files to get ALL forms for the target date, not just recent ones.
    Daily index format: https://www.sec.gov/Archives/edgar/daily-index/{YEAR}/{DATE}/master.idx
    
    Args:
        target_date: Date in YYYY-MM-DD format
        
    Returns:
        List of form metadata dicts with keys: form_type, cik, accession_number, filename, filing_date
    """
    logger.info(f"📋 Fetching SEC forms for date: {target_date}")
    
    forms = []
    form_types = ['3', '4', '5']  # Only Forms 3, 4, 5
    
    # Parse target date to get year, quarter, and date string
    try:
        date_obj = datetime.strptime(target_date, '%Y-%m-%d').date()
        year = date_obj.year
        month = date_obj.month
        quarter = (month - 1) // 3 + 1  # Q1=1, Q2=2, Q3=3, Q4=4
        date_str = date_obj.strftime('%Y%m%d')  # YYYYMMDD format
    except ValueError as e:
        logger.error(f"❌ Invalid date format: {target_date}, expected YYYY-MM-DD")
        return []
    
    # Create requests session with required headers
    session = requests.Session()
    session.headers.update({
        'User-Agent': SEC_USER_AGENT,
        'Accept': 'text/plain'
    })
    
    try:
        # SEC daily index files are available in two formats:
        # 1. New format: https://www.sec.gov/Archives/edgar/daily-index/{YEAR}/{DATE}/master.idx
        # 2. Old format: https://www.sec.gov/Archives/edgar/daily-index/{YEAR}/QTR{QUARTER}/master.{DATE}.idx
        
        index_urls = [
            f"https://www.sec.gov/Archives/edgar/daily-index/{year}/{date_str}/master.idx",  # New format (preferred)
            f"https://www.sec.gov/Archives/edgar/daily-index/{year}/QTR{quarter}/master.{date_str}.idx",  # Old format (fallback)
        ]
        
        index_content = None
        for index_url in index_urls:
            try:
                logger.info(f"📥 Attempting to fetch daily index: {index_url}")
                response = session.get(index_url, timeout=30)
                response.raise_for_status()
                
                # Check if we got valid content (not HTML error page)
                if response.text and not response.text.strip().startswith('<!'):
                    index_content = response.text
                    logger.info(f"✅ Successfully fetched daily index from: {index_url}")
                    break
                else:
                    logger.debug(f"⚠️ {index_url} returned HTML (likely error page), trying next format...")
                    
            except requests.exceptions.RequestException as e:
                logger.debug(f"⚠️ Error fetching {index_url}: {e}, trying next format...")
                continue
        
        if not index_content:
            logger.warning(f"⚠️ Could not fetch daily index file for {target_date} from any URL format")
            return []
        
        # Parse the daily index file
        # Format is fixed-width with header lines, then data lines
        # Header: Description... CIK... Company Name... Form Type... Date Filed... File Name
        # Data lines are fixed-width: CIK (12), Company Name (~60), Form Type (~10), Date Filed (8), File Name (variable)
        
        lines = index_content.split('\n')
        
        # Find the header line (usually starts with "Description" or "CIK")
        header_line_index = None
        for i, line in enumerate(lines):
            if line.strip() and ('CIK' in line.upper() or 'Description' in line.upper()):
                header_line_index = i
                break
        
        if header_line_index is None:
            logger.warning(f"⚠️ Could not find header line in daily index file")
            return []
        
        # Skip header lines and separator lines
        data_start_index = header_line_index + 1
        # Skip separator line (usually dashes or empty)
        if data_start_index < len(lines) and (not lines[data_start_index].strip() or lines[data_start_index].strip().startswith('-')):
            data_start_index += 1
        
        # Parse data lines
        forms_found = 0
        for line_num, line in enumerate(lines[data_start_index:], start=data_start_index):
            line = line.rstrip('\n\r')
            if not line.strip():
                continue  # Skip empty lines
            
            try:
                # Parse fixed-width format
                # CIK is first 12 characters (padded with spaces)
                # Company Name is next ~60 characters
                # Form Type is next ~10 characters
                # Date Filed is next 8 characters (YYYYMMDD)
                # File Name is the rest
                
                # Try to parse - CIK is typically first 12 chars, but may have padding
                parts = line.split()
                if len(parts) < 4:
                    continue  # Invalid line
                
                # CIK is typically the first numeric field
                cik = None
                form_type = None
                date_filed = None
                file_name = None
                
                # More robust parsing: look for CIK (10 digits, may be padded), Form Type (3, 4, or 5), Date (YYYYMMDD)
                for i, part in enumerate(parts):
                    # Check if this looks like a CIK (10 digits, possibly with leading zeros)
                    if part.isdigit() and len(part) == 10 and not cik:
                        cik = part.lstrip('0') or '0'  # Remove leading zeros, but keep '0' if all zeros
                    # Check if this is a Form Type we want (3, 4, or 5)
                    elif part in form_types and not form_type:
                        form_type = part
                    # Check if this looks like a date (8 digits: YYYYMMDD)
                    elif part.isdigit() and len(part) == 8 and not date_filed:
                        try:
                            parsed_date = datetime.strptime(part, '%Y%m%d').date()
                            # Verify it matches our target date
                            if parsed_date == date_obj:
                                date_filed = part
                        except ValueError:
                            pass
                
                # If we didn't find all required fields, try alternative parsing
                if not all([cik, form_type, date_filed]):
                    # Try regex-based parsing for more complex formats
                    # Look for pattern: CIK (10 digits), Form Type (3/4/5), Date (YYYYMMDD), File Name
                    cik_match = re.search(r'\b(\d{10})\b', line)
                    form_match = re.search(r'\b([345])\b', line)
                    date_match = re.search(r'(\d{8})', line)
                    
                    if cik_match:
                        cik = cik_match.group(1).lstrip('0') or '0'
                    if form_match and form_match.group(1) in form_types:
                        form_type = form_match.group(1)
                    if date_match:
                        date_str_check = date_match.group(1)
                        try:
                            parsed_date = datetime.strptime(date_str_check, '%Y%m%d').date()
                            if parsed_date == date_obj:
                                date_filed = date_str_check
                        except ValueError:
                            pass
                
                # Only process if we have all required fields and form type matches
                if cik and form_type in form_types and date_filed:
                    # Extract file name - it's usually at the end of the line
                    # Format is typically: edgar/data/{CIK}/{ACCESSION}/{FILENAME}
                    file_match = re.search(r'edgar/data/\d+/([\d-]+)/([^\s]+)', line)
                    if file_match:
                        accession_number = file_match.group(1).replace('-', '')
                        file_name = file_match.group(2)
                    else:
                        # Try to extract accession number from file path
                        # Accession numbers are 18 digits: 10-2-6 format
                        acc_match = re.search(r'(\d{10}-\d{2}-\d{6})', line)
                        if acc_match:
                            accession_number = acc_match.group(1).replace('-', '')
                            file_name = None  # Will be determined by downloader
                        else:
                            # Skip if we can't extract accession
                            continue
                    
                    forms.append({
                        'form_type': f'form{form_type}',
                        'cik': cik,
                        'accession_number': accession_number,
                        'filename': file_name,
                        'filing_date': target_date
                    })
                    
                    forms_found += 1
                    if forms_found <= 5:  # Log first few
                        logger.debug(f"✅ Extracted Form {form_type}: CIK={cik}, Accession={accession_number[:10]}...")
                
            except Exception as line_error:
                logger.debug(f"⚠️ Error parsing line {line_num}: {line_error}")
                continue
        
        logger.info(f"📊 Found {forms_found} Forms 3/4/5 in daily index for {target_date}")
    
    except Exception as e:
        logger.error(f"❌ Error fetching SEC daily index for {target_date}: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
    
    logger.info(f"📋 Found {len(forms)} SEC forms (Forms 3/4/5) for {target_date}")
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
        
        if not all([cik, accession]):
            logger.warning(f"⚠️ Missing required fields (CIK/accession) for form download")
            return None
        
        # Construct accession number with dashes (format: 0001234567-12-345678)
        # Accession numbers are 18 digits, formatted as 10-2-6
        if len(accession) == 18:
            accession_dashed = f"{accession[:10]}-{accession[10:12]}-{accession[12:]}"
        else:
            accession_dashed = accession
        
        # Determine filename and file type
        # Try .txt first (most common), then .xml if not found
        filename = form_data.get('filename', f'{accession_dashed}.txt')
        
        # Build base URL
        base_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_dashed}"
        
        session = requests.Session()
        session.headers.update({'User-Agent': SEC_USER_AGENT})
        
        # Try to download the file - try .txt first, then .xml
        file_content = None
        file_ext = None
        content_type = None
        
        file_extensions = [
            ('.txt', 'application/xml'),  # .txt files are usually XML content
            ('.xml', 'application/xml'),
            ('.pdf', 'application/pdf')
        ]
        
        # If filename already has extension, try that first
        if filename.endswith(('.txt', '.xml', '.pdf')):
            ext = filename[filename.rfind('.'):]
            for fe, ct in file_extensions:
                if ext == fe:
                    file_extensions.insert(0, (fe, ct))
                    break
        
        for ext, ct in file_extensions:
            try:
                if filename.endswith(ext):
                    file_url = f"{base_url}/{filename}"
                else:
                    file_url = f"{base_url}/{accession_dashed}{ext}"
                
                logger.debug(f"📥 Attempting to download: {file_url}")
                response = session.get(file_url, timeout=30)
                
                if response.status_code == 200:
                    file_content = response.content
                    file_ext = ext[1:]  # Remove the dot
                    content_type = ct
                    
                    # For .txt files, check if it's actually XML (common for SEC forms)
                    if ext == '.txt' and file_content.startswith(b'<?xml'):
                        file_ext = 'xml'
                        content_type = 'application/xml'
                    
                    logger.info(f"✅ Successfully downloaded: {file_url}")
                    break
                else:
                    logger.debug(f"⚠️ HTTP {response.status_code} for {file_url}, trying next extension...")
                    
            except requests.exceptions.RequestException as e:
                logger.debug(f"⚠️ Could not download {file_url}: {e}")
                continue
        
        if not file_content:
            # Log more details about what we tried
            attempted_urls = []
            for ext, ct in file_extensions:
                if filename.endswith(ext):
                    attempted_urls.append(f"{base_url}/{filename}")
                else:
                    attempted_urls.append(f"{base_url}/{accession_dashed}{ext}")
            logger.error(f"❌ Could not download form for CIK {cik}, accession {accession_dashed}")
            logger.error(f"   Attempted URLs: {attempted_urls}")
            return None
        
        # Generate S3 key
        s3_key = f"trades/{target_date}/sec/{form_type}-{cik}-{target_date}.{file_ext}"
        
        # Upload to S3
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=file_content,
            ContentType=content_type
        )
        
        logger.info(f"✅ Stored SEC form to S3: {s3_key}")
        return s3_key
        
    except Exception as e:
        logger.error(f"❌ Error downloading/storing SEC form: {e}")
        return None

def lambda_handler(event, context):
    """
    Lambda handler for fetching SEC forms and Congressional PTRs
    
    Supports four input modes:
    
    1. Generate dates list (for Step Functions date range processing):
    {
        "action": "generateDates",
        "startDate": "2025-01-01",
        "endDate": "2025-01-31"
    }
    Returns: {"dates": ["2025-01-01", "2025-01-02", ...]}
    
    2. Single date (backwards compatible):
    {
        "date": "2025-10-30"
    }
    
    3. Date range (DEPRECATED - use Step Functions Map state instead):
    {
        "startDate": "2025-01-01",
        "endDate": "2025-12-31"
    }
    
    4. Default (from EventBridge daily scheduler):
    {
        "source": "scheduler-daily",
        "timestamp": "2025-10-31T00:00:00Z"
    }
    # Defaults to yesterday's date
    
    Returns:
    {
        "date": "2025-10-30",  # First date (for backwards compatibility)
        "dateRange": {  # Present only if date range was provided
            "startDate": "2025-01-01",
            "endDate": "2025-12-31",
            "totalDays": 365
        },
        "secFormsFetched": 52,
        "housePTRsFetched": 0,
        "senatePTRsFetched": 8,
        "secForms": [...],  # All forms across all dates in range
        "housePTRs": [...],
        "senatePTRs": [...]
    }
    
    Note: For large date ranges, consider breaking into smaller chunks to avoid Lambda timeout.
    Recommended: Process 30-90 days at a time for optimal performance.
    """
    logger.info("🚀 Politician Trades Fetcher Lambda started")
    
    if not S3_BUCKET:
        raise ValueError("S3_BUCKET environment variable not set")
    
    # Handle special "generateDates" action for Step Functions date range processing
    if isinstance(event, dict) and event.get('action') == 'generateDates':
        start_date_str = event.get('startDate')
        end_date_str = event.get('endDate')
        
        if not start_date_str or not end_date_str:
            raise ValueError("startDate and endDate required for generateDates action")
        
        try:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
            
            if start_date > end_date:
                raise ValueError("startDate must be <= endDate")
            
            # Generate list of dates
            dates = []
            current_date = start_date
            while current_date <= end_date:
                dates.append(current_date.strftime('%Y-%m-%d'))
                current_date += timedelta(days=1)
            
            logger.info(f"📅 Generated {len(dates)} dates from {start_date_str} to {end_date_str}")
            return {
                "dates": dates,
                "startDate": start_date_str,
                "endDate": end_date_str,
                "totalDays": len(dates)
            }
        except ValueError as e:
            logger.error(f"❌ Invalid date format in generateDates: {e}")
            raise ValueError(f"Invalid date range: startDate='{start_date_str}', endDate='{end_date_str}'. Expected YYYY-MM-DD format.")
    
    # Parse date input - support both single date and date range
    # Options:
    # 1. Single date: {"date": "2025-10-30"}
    # 2. Date range: {"startDate": "2025-01-01", "endDate": "2025-12-31"} (DEPRECATED - use Step Functions Map instead)
    # 3. Default: yesterday's date (for scheduled runs)
    target_dates = []
    
    if isinstance(event, dict):
        if event.get('startDate') and event.get('endDate') and not event.get('action'):
            # Date range mode - backfill historical data
            start_date_str = event.get('startDate')
            end_date_str = event.get('endDate')
            
            try:
                start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
                end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date()
                
                if start_date > end_date:
                    raise ValueError("startDate must be <= endDate")
                
                # Generate list of dates from start to end (inclusive)
                current_date = start_date
                while current_date <= end_date:
                    target_dates.append(current_date.strftime('%Y-%m-%d'))
                    current_date += timedelta(days=1)
                
                logger.info(f"📅 Date range mode: {start_date_str} to {end_date_str} ({len(target_dates)} days)")
            except ValueError as e:
                logger.error(f"❌ Invalid date format or range: {e}")
                raise ValueError(f"Invalid date range: startDate='{start_date_str}', endDate='{end_date_str}'. Expected YYYY-MM-DD format.")
        elif event.get('date'):
            # Single date mode (backwards compatible)
            target_dates = [event.get('date')]
            logger.info(f"📅 Single date mode: {target_dates[0]}")
        else:
            # Default: yesterday for scheduled runs
            target_dates = [get_yesterday_date()]
            logger.info(f"📅 Default date mode (yesterday): {target_dates[0]}")
    else:
        # Default: yesterday for scheduled runs
        target_dates = [get_yesterday_date()]
        logger.info(f"📅 Default date mode (yesterday): {target_dates[0]}")
    
    logger.info(f"📅 Processing {len(target_dates)} date(s): {target_dates[0] if len(target_dates) == 1 else f'{target_dates[0]} to {target_dates[-1]}'}")
    
    # Initialize aggregate results
    aggregate_results = {
        'dateRange': {
            'startDate': target_dates[0],
            'endDate': target_dates[-1],
            'totalDays': len(target_dates)
        } if len(target_dates) > 1 else {'singleDate': target_dates[0]},
        'secFormsFetched': 0,
        'housePTRsFetched': 0,
        'senatePTRsFetched': 0,
        'secForms': [],
        'housePTRs': [],
        'senatePTRs': []
    }
    
    try:
        # Process each date in the range
        for date_index, target_date in enumerate(target_dates, 1):
            logger.info(f"📅 Processing date {date_index}/{len(target_dates)}: {target_date}")
            
            # Step 1: Fetch SEC Forms (3, 4, 5) - only metadata, no downloads
            logger.info(f"📋 Fetching SEC Forms 3, 4, 5 for {target_date}...")
            sec_forms = fetch_sec_forms(target_date)
            
            # Return metadata for parallel downloading (done by separate Lambda)
            for form_data in sec_forms:
                aggregate_results['secForms'].append({
                    'formType': form_data.get('form_type'),
                    'cik': form_data.get('cik'),
                    'accessionNumber': form_data.get('accession_number'),
                    'filename': form_data.get('filename'),
                    'filingDate': target_date
                })
                aggregate_results['secFormsFetched'] += 1
            
            logger.info(f"✅ Found {len(sec_forms)} SEC forms for {target_date} (total so far: {aggregate_results['secFormsFetched']})")
            
            # Step 2: Fetch House PTRs
            # NOTE: House PTRs are in XML format (annual filings), not individual PTR PDFs
            # Skipping House PTRs for now - focus on Senate PTRs which have better filing system
            logger.info("🏛️ Fetching House PTRs...")
            logger.warning("⚠️ House PTRs are in XML format (annual filings) - skipping for now")
            logger.info("💡 House PTRs would require XML parsing of annual disclosure files")
            # House PTRs remain empty for all dates
            
            # Step 3: Fetch Senate PTRs (metadata only - downloader will download them)
            logger.info(f"🏛️ Fetching Senate PTRs for {target_date}...")
            scraper = CongressionalPTRScraper()
            senate_ptrs = scraper.fetch_senate_ptrs(target_date)
            
            # Return metadata for downloader Lambda (similar to SEC forms)
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
            aggregate_results['secFormsFetched'] + 
            aggregate_results['housePTRsFetched'] + 
            aggregate_results['senatePTRsFetched']
        )
        
        logger.info(f"✅ Total forms fetched across {len(target_dates)} date(s): {total_fetched}")
        logger.info(f"   - SEC Forms: {aggregate_results['secFormsFetched']}")
        logger.info(f"   - House PTRs: {aggregate_results['housePTRsFetched']}")
        logger.info(f"   - Senate PTRs: {aggregate_results['senatePTRsFetched']}")
        
        # For backwards compatibility with Step Functions, also include 'date' field
        # Use the first date if range, or the single date
        aggregate_results['date'] = target_dates[0]
        
        # Log the return value for debugging
        logger.info(f"📤 Returning results: {json.dumps({k: v if k != 'secForms' and k != 'senatePTRs' else f'[{len(v)} items]' for k, v in aggregate_results.items()}, default=str)}")
        
        # Return dict directly for Step Functions (not wrapped in statusCode/body)
        # Step Functions expects a JSON-serializable dict
        return aggregate_results
        
    except Exception as e:
        logger.error(f"❌ Fatal error in fetcher Lambda: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise

