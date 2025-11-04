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
    
    Uses SEC's daily index files which list ALL filings for a specific date:
    https://www.sec.gov/Archives/edgar/daily-index/{YEAR}/QTR{N}/{form_type}/{date}.idx
    
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
        'Accept': 'text/plain'
    })
    
    try:
        # Parse target date to get year, quarter, and date string
        date_obj = datetime.strptime(target_date, '%Y-%m-%d').date()
        year = date_obj.year
        month = date_obj.month
        quarter = ((month - 1) // 3) + 1  # Q1=Jan-Mar, Q2=Apr-Jun, Q3=Jul-Sep, Q4=Oct-Dec
        date_str = date_obj.strftime('%Y%m%d')  # Format: 20251025
        
        # SEC daily index file format: {form_type}/{date}.idx
        # Example: form4/20251025.idx
        # URL: https://www.sec.gov/Archives/edgar/daily-index/{YEAR}/QTR{N}/{form_type}/{date}.idx
        
        for form_type in form_types:
            # Build daily index file URL
            # Format: https://www.sec.gov/Archives/edgar/daily-index/{YEAR}/QTR{N}/{form_type}/{date}.idx
            index_url = f"https://www.sec.gov/Archives/edgar/daily-index/{year}/QTR{quarter}/form{form_type}/{date_str}.idx"
            
            logger.info(f"📥 Fetching SEC daily index for Form {form_type} from: {index_url}")
            
            try:
                # Fetch the daily index file
                response = session.get(index_url, timeout=30)
                
                # Check if file exists (404 means no filings for that date)
                if response.status_code == 404:
                    logger.info(f"📭 No Form {form_type} index file found for {target_date} (404) - likely no filings that day")
                    continue
                
                response.raise_for_status()
                
                # Daily index files are plain text with specific format
                # Format varies, but typically:
                # Header lines (skip)
                # Data lines: CIK|Company Name|Form Type|Date Filed|File Name
                # Or: CIK|Company Name|Form Type|Date Filed|CIK|Accession Number|File Name
                
                content = response.text
                lines = content.split('\n')
                
                logger.info(f"📄 Parsed daily index file for Form {form_type}: {len(lines)} lines")
                
                # Find the header line (usually starts with "CIK" or has "|" separator)
                header_line_idx = None
                for idx, line in enumerate(lines):
                    if line.strip().startswith('CIK') and '|' in line:
                        header_line_idx = idx
                        break
                
                if header_line_idx is None:
                    logger.warning(f"⚠️ Could not find header line in Form {form_type} index file")
                    continue
                
                # Parse data lines (skip header and empty lines)
                forms_for_this_type = 0
                for line_idx, line in enumerate(lines[header_line_idx + 1:], start=header_line_idx + 1):
                    line = line.strip()
                    if not line or line.startswith('-') or line.startswith('--'):
                        continue
                    
                    # Split by pipe separator
                    parts = [p.strip() for p in line.split('|')]
                    
                    if len(parts) < 4:
                        continue
                    
                    try:
                        # Extract CIK (first field)
                        cik = parts[0].strip()
                        if not cik or not cik.isdigit():
                            continue
                        
                        # Extract accession number (varies by format, but usually in the middle)
                        # Format examples:
                        # CIK|Company|Form|Date|Filename
                        # CIK|Company|Form|Date|CIK|Accession|Filename
                        
                        accession_number = None
                        filename = None
                        
                        # Try to find accession number - it's usually 18 digits (with or without dashes)
                        for part in parts:
                            # Remove dashes and check if it's 18 digits
                            part_clean = part.replace('-', '').strip()
                            if part_clean.isdigit() and len(part_clean) == 18:
                                accession_number = part_clean
                                break
                        
                        # If no accession found, try to extract from filename (last field usually)
                        if not accession_number and len(parts) > 4:
                            filename_field = parts[-1].strip()
                            # Try to extract accession from filename (format: 0001234567-12-345678.txt)
                            acc_match = re.search(r'(\d{10}-\d{2}-\d{6})', filename_field)
                            if acc_match:
                                accession_number = acc_match.group(1).replace('-', '')
                                filename = filename_field
                        
                        # If still no accession, skip this line
                        if not accession_number:
                            logger.debug(f"⚠️ Could not extract accession number from line: {line[:100]}")
                            continue
                        
                        # Set default filename if not found
                        if not filename:
                            accession_dashed = f"{accession_number[:10]}-{accession_number[10:12]}-{accession_number[12:]}"
                            filename = f"{accession_dashed}.txt"
                        
                        # Add to forms list (all entries in daily index are for the target date)
                        forms.append({
                            'form_type': f'form{form_type}',
                            'cik': cik,
                            'accession_number': accession_number,
                            'filename': filename,
                            'filing_date': target_date
                        })
                        
                        forms_for_this_type += 1
                        
                        if forms_for_this_type <= 5:
                            logger.debug(f"✅ Extracted Form {form_type}: CIK={cik}, Accession={accession_number[:10]}...")
                    
                    except Exception as line_error:
                        logger.debug(f"⚠️ Error parsing line {line_idx}: {line_error}")
                        continue
                
                logger.info(f"📊 Form {form_type}: Extracted {forms_for_this_type} forms from daily index for {target_date}")
            
            except requests.exceptions.HTTPError as http_error:
                if http_error.response.status_code == 404:
                    logger.info(f"📭 No Form {form_type} index file found for {target_date} (404) - likely no filings that day")
                else:
                    logger.error(f"❌ HTTP error fetching Form {form_type} daily index: {http_error}")
                continue
            except Exception as parse_error:
                logger.error(f"❌ Error fetching/parsing Form {form_type} daily index: {parse_error}")
                logger.error(f"Index URL: {index_url}")
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
    Lambda handler for fetching SEC forms and Congressional PTRs for a single date
    
    Input format (required):
    {
        "date": "2025-10-30"
    }
    
    Returns:
    {
        "date": "2025-10-30",
        "secFormsFetched": 52,
        "housePTRsFetched": 0,
        "senatePTRsFetched": 8,
        "secForms": [...],
        "housePTRs": [...],
        "senatePTRs": [...]
    }
    
    Note: Date range processing is handled by Step Functions outer layer.
    This Lambda only processes a single date.
    """
    logger.info("🚀 Politician Trades Fetcher Lambda started")
    
    if not S3_BUCKET:
        raise ValueError("S3_BUCKET environment variable not set")
    
    # Parse date input - only single date is supported
    # Date range processing is handled by Step Functions
    target_date = None
    
    if isinstance(event, dict) and event.get('date'):
        target_date = event.get('date')
        logger.info(f"📅 Processing single date: {target_date}")
    else:
        # Default: yesterday's date (fallback for backwards compatibility)
        target_date = get_yesterday_date()
        logger.warning(f"⚠️ No 'date' field in input, defaulting to yesterday: {target_date}")
    
    # Validate date format
    try:
        datetime.strptime(target_date, '%Y-%m-%d')
    except ValueError:
        raise ValueError(f"Invalid date format: '{target_date}'. Expected YYYY-MM-DD format.")
    
    try:
        # Step 1: Fetch SEC Forms (3, 4, 5) - only metadata, no downloads
        logger.info(f"📋 Fetching SEC Forms 3, 4, 5 for {target_date}...")
        sec_forms = fetch_sec_forms(target_date)
        
        # Return metadata for parallel downloading (done by separate Lambda)
        sec_forms_list = []
        for form_data in sec_forms:
            sec_forms_list.append({
                'formType': form_data.get('form_type'),
                'cik': form_data.get('cik'),
                'accessionNumber': form_data.get('accession_number'),
                'filename': form_data.get('filename'),
                'filingDate': target_date
            })
        
        logger.info(f"✅ Found {len(sec_forms)} SEC forms for {target_date}")
        
        # Step 2: Fetch House PTRs
        # NOTE: House PTRs are in XML format (annual filings), not individual PTR PDFs
        # Skipping House PTRs for now - focus on Senate PTRs which have better filing system
        logger.info("🏛️ Fetching House PTRs...")
        logger.warning("⚠️ House PTRs are in XML format (annual filings) - skipping for now")
        house_ptrs_list = []
        
        # Step 3: Fetch Senate PTRs (metadata only - downloader will download them)
        logger.info(f"🏛️ Fetching Senate PTRs for {target_date}...")
        scraper = CongressionalPTRScraper()
        senate_ptrs = scraper.fetch_senate_ptrs(target_date)
        
        # Return metadata for downloader Lambda (similar to SEC forms)
        senate_ptrs_list = []
        for ptr_data in senate_ptrs:
            # Add source field if not present
            if 'source' not in ptr_data:
                ptr_data['source'] = 'senate'
            # Ensure formType is set
            if 'formType' not in ptr_data and 'form_type' not in ptr_data:
                ptr_data['formType'] = 'senate_ptr'
                ptr_data['form_type'] = 'senate_ptr'
            
            senate_ptrs_list.append(ptr_data)
        
        logger.info(f"✅ Found {len(senate_ptrs)} Senate PTRs for {target_date}")
        
        # Build results
        results = {
            'date': target_date,
            'secFormsFetched': len(sec_forms_list),
            'housePTRsFetched': len(house_ptrs_list),
            'senatePTRsFetched': len(senate_ptrs_list),
            'secForms': sec_forms_list,
            'housePTRs': house_ptrs_list,
            'senatePTRs': senate_ptrs_list
        }
        
        total_fetched = results['secFormsFetched'] + results['housePTRsFetched'] + results['senatePTRsFetched']
        
        logger.info(f"✅ Total forms fetched for {target_date}: {total_fetched}")
        logger.info(f"   - SEC Forms: {results['secFormsFetched']}")
        logger.info(f"   - House PTRs: {results['housePTRsFetched']}")
        logger.info(f"   - Senate PTRs: {results['senatePTRsFetched']}")
        
        return results
        
    except Exception as e:
        logger.error(f"❌ Fatal error in fetcher Lambda: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise

