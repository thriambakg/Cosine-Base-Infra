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
    # SEC requires proper User-Agent and headers to avoid 403 errors
    session = requests.Session()
    session.headers.update({
        'User-Agent': SEC_USER_AGENT,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,text/plain;q=0.8,*/*;q=0.7',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1'
    })
    
    try:
        # Parse target date to get year, quarter, and date string
        date_obj = datetime.strptime(target_date, '%Y-%m-%d').date()
        year = date_obj.year
        month = date_obj.month
        quarter = ((month - 1) // 3) + 1  # Q1=Jan-Mar, Q2=Apr-Jun, Q3=Jul-Sep, Q4=Oct-Dec
        date_str = date_obj.strftime('%Y%m%d')  # Format: 20251025
        
        # SEC daily index file format
        # URL: https://www.sec.gov/Archives/edgar/daily-index/{YEAR}/QTR{N}/{form_type}/{date}.idx
        # Note: Some dates may not have index files, or format may vary
        
        for form_type in form_types:
            # Build daily index file URL
            # SEC daily index files are at: /Archives/edgar/daily-index/{YEAR}/QTR{N}/{form_type}/{date}.idx
            # Try both lowercase and uppercase form type variations
            index_urls = [
                f"https://www.sec.gov/Archives/edgar/daily-index/{year}/QTR{quarter}/form{form_type}/{date_str}.idx",
                f"https://www.sec.gov/Archives/edgar/daily-index/{year}/QTR{quarter}/FORM{form_type}/{date_str}.idx",
            ]
            
            index_url = index_urls[0]  # Try lowercase first
            
            logger.info(f"📥 Fetching SEC daily index for Form {form_type} from: {index_url}")
            
            # Add delay before each request to avoid rate limiting/403 errors
            # SEC has strict bot protection - be respectful with delays
            # Wait 1 second between requests to stay under 10 requests/second limit
            if form_type != form_types[0]:  # Don't delay before first form type
                time.sleep(1.0)
            else:
                # Small initial delay to avoid immediate 403
                time.sleep(0.5)
            
            try:
                # Try both URL formats (lowercase and uppercase)
                response = None
                for url in index_urls:
                    try:
                        # Fetch the daily index file
                        # SEC requires proper headers and respectful rate limiting to avoid 403 errors
                        response = session.get(url, timeout=30, allow_redirects=True)
                        
                        # If we get a valid response (200) or 404, use this URL
                        if response.status_code in [200, 404]:
                            index_url = url  # Update to the working URL
                            break
                        # If 403, try next URL format
                        elif response.status_code == 403:
                            logger.debug(f"⚠️ 403 Forbidden for {url}, trying alternative format...")
                            continue
                    except Exception as url_error:
                        logger.debug(f"⚠️ Error with URL {url}: {url_error}, trying next...")
                        continue
                
                # If all URLs failed, handle 403 or other errors
                if response is None or response.status_code == 403:
                    logger.warning(f"⚠️ 403 Forbidden for Form {form_type} daily index - falling back to RSS feed")
                    logger.warning(f"   URLs attempted: {index_urls}")
                    # For 403 errors, try RSS feed as fallback (for recent dates)
                    today = datetime.now().date()
                    if date_obj == today or date_obj >= today - timedelta(days=1):
                        logger.info(f"📭 Falling back to RSS feed for Form {form_type} due to 403 error")
                        forms.extend(_fetch_from_rss_feed(session, form_type, target_date))
                    else:
                        logger.warning(f"⚠️ Cannot fetch Form {form_type} for historical date {target_date} - daily index unavailable due to 403")
                    continue
                
                # Check response status - raise for errors other than 404
                response.raise_for_status()
                
                # Check if file exists (404 means no filings for that date OR file not yet created)
                if response.status_code == 404:
                    # Check if this is today's date - if so, fall back to RSS feed (index file might not be created yet)
                    today = datetime.now().date()
                    if date_obj == today:
                        logger.info(f"📭 Daily index file not available for today's date ({target_date}) - falling back to RSS feed")
                        # Fall back to RSS feed for today's filings
                        forms.extend(_fetch_from_rss_feed(session, form_type, target_date))
                        continue
                    else:
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
                elif http_error.response.status_code == 403:
                    logger.warning(f"⚠️ 403 Forbidden for Form {form_type} daily index - may need to wait or check URL format")
                    logger.warning(f"   URL attempted: {index_url}")
                    # For 403 errors, try RSS feed as fallback
                    today = datetime.now().date()
                    if date_obj == today or date_obj >= today - timedelta(days=1):
                        logger.info(f"📭 Falling back to RSS feed for Form {form_type} due to 403 error")
                        forms.extend(_fetch_from_rss_feed(session, form_type, target_date))
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


def _fetch_from_rss_feed(session: requests.Session, form_type: str, target_date: str) -> List[Dict[str, Any]]:
    """
    Fallback: Fetch SEC forms from RSS feed (for today's date when daily index isn't available yet)
    
    This is a limited approach - only returns most recent 100 filings, but works for same-day access.
    
    Args:
        session: Requests session with proper headers
        form_type: Form type ('3', '4', or '5')
        target_date: Target date in YYYY-MM-DD format
        
    Returns:
        List of form metadata dicts
    """
    forms = []
    
    # SEC RSS feed for recent filings
    rss_urls = [
        f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type={form_type}&company=&count=100&output=rss",
        f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type={form_type}&company=&count=100",
        f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type={form_type}&output=atom",
    ]
    
    for rss_url in rss_urls:
        try:
            response = session.get(rss_url, timeout=30)
            response.raise_for_status()
            
            # Check if response is actually XML/RSS
            content_type = response.headers.get('Content-Type', '').lower()
            if 'text/html' in content_type or (response.text and response.text.strip().startswith('<!')):
                logger.debug(f"⚠️ Form {form_type} RSS URL returned HTML, trying next format: {rss_url}")
                continue
            
            # If we get here, we have a valid response - try to parse
            break
            
        except Exception as url_error:
            logger.debug(f"⚠️ Error with RSS URL {rss_url}: {url_error}, trying next...")
            continue
    else:
        # If we exhausted all URLs, return empty
        logger.warning(f"⚠️ Could not fetch valid RSS feed for Form {form_type} with any URL format")
        return forms
    
    # Try to parse as XML
    try:
        # SEC feeds might have encoding issues - try to decode properly
        if isinstance(response.content, bytes):
            try:
                xml_content = response.content.decode('utf-8')
            except UnicodeDecodeError:
                try:
                    xml_content = response.content.decode('latin-1')
                except UnicodeDecodeError:
                    xml_content = response.content.decode('utf-8', errors='ignore')
        else:
            xml_content = response.text
        
        # Remove BOM if present
        if xml_content and xml_content.startswith('\ufeff'):
            xml_content = xml_content[1:]
        
        xml_content = xml_content.strip()
        
        # Try to parse XML
        parser = ET.XMLParser(encoding='utf-8')
        try:
            root = ET.fromstring(xml_content.encode('utf-8'), parser=parser)
        except ET.ParseError:
            root = ET.fromstring(xml_content)
        
        # Try without namespace first (some feeds don't use namespaces)
        items = root.findall('.//item')
        if not items:
            items = root.findall('.//{http://purl.org/rss/1.0/}item')
        if not items:
            items = root.findall('.//{http://www.w3.org/2005/Atom}entry')
        if not items:
            items = root.findall('.//{http://www.w3.org/1999/02/22-rdf-syntax-ns#}item')
        
        logger.info(f"📥 Fetched RSS feed for Form {form_type}: found {len(items)} items (fallback for today)")
        
        if len(items) == 0:
            return forms
        
        target_date_obj = datetime.strptime(target_date, '%Y-%m-%d').date()
        items_matching_date = 0
        
        for item in items:
            try:
                # Extract filing date
                pub_date_elem = None
                for tag_name in [
                    '{http://www.w3.org/2005/Atom}updated',
                    'pubDate',
                    'date',
                    '{http://purl.org/dc/elements/1.1/}date'
                ]:
                    pub_date_elem = item.find(tag_name)
                    if pub_date_elem is not None:
                        break
                
                if pub_date_elem is None or not pub_date_elem.text:
                    continue
                
                # Parse date
                filing_date_str = pub_date_elem.text.strip()
                filing_date = None
                
                date_formats = [
                    '%Y-%m-%dT%H:%M:%S%z',
                    '%Y-%m-%dT%H:%M:%SZ',
                    '%Y-%m-%dT%H:%M:%S',
                    '%a, %d %b %Y %H:%M:%S %Z',
                    '%a, %d %b %Y %H:%M:%S %z',
                    '%a, %d %b %Y %H:%M:%S',
                    '%Y-%m-%d',
                    '%Y-%m-%d %H:%M:%S',
                    '%d %b %Y',
                    '%b %d, %Y',
                    '%m/%d/%Y',
                    '%Y/%m/%d'
                ]
                
                for fmt in date_formats:
                    try:
                        filing_date = datetime.strptime(filing_date_str, fmt).date()
                        break
                    except ValueError:
                        continue
                
                if not filing_date or filing_date != target_date_obj:
                    continue
                
                items_matching_date += 1
                
                # Extract link/guid
                link_elem = None
                filing_url = None
                
                for tag_name in ['{http://www.w3.org/2005/Atom}link', 'link', 'guid']:
                    link_elem = item.find(tag_name)
                    if link_elem is not None:
                        break
                
                if link_elem is not None:
                    if link_elem.get('href'):
                        filing_url = link_elem.get('href').strip()
                    elif link_elem.text:
                        filing_url = link_elem.text.strip()
                
                if not filing_url:
                    continue
                
                # Extract CIK and accession number
                cik = None
                accession_number = None
                filename = None
                
                if '/Archives/edgar/data/' in filing_url:
                    parts = filing_url.split('/Archives/edgar/data/')[1].split('/')
                    if len(parts) >= 2:
                        cik = parts[0].strip()
                        accession_number = parts[1].strip().replace('-', '')
                        if len(parts) >= 3:
                            filename = parts[2].strip()
                
                elif 'cik=' in filing_url and 'accession_number=' in filing_url:
                    parsed_url = urlparse(filing_url)
                    params = parse_qs(parsed_url.query)
                    cik = params.get('cik', [None])[0]
                    accession_number = params.get('accession_number', [None])[0]
                    if accession_number:
                        accession_number = accession_number.replace('-', '')
                
                if not cik or not accession_number:
                    # Try description
                    desc_elem = item.find('description')
                    if desc_elem is not None and desc_elem.text:
                        cik_match = re.search(r'CIK[:\s]+(\d+)', desc_elem.text, re.IGNORECASE)
                        acc_match = re.search(r'Accession[:\s]+([\d-]+)', desc_elem.text, re.IGNORECASE)
                        if cik_match:
                            cik = cik_match.group(1)
                        if acc_match:
                            accession_number = acc_match.group(1).replace('-', '')
                
                if not cik or not accession_number:
                    continue
                
                if not filename:
                    accession_dashed = f"{accession_number[:10]}-{accession_number[10:12]}-{accession_number[12:]}"
                    filename = f"{accession_dashed}.txt"
                
                forms.append({
                    'form_type': f'form{form_type}',
                    'cik': cik,
                    'accession_number': accession_number,
                    'filename': filename,
                    'filing_date': target_date
                })
                
            except Exception as item_error:
                logger.debug(f"⚠️ Error parsing RSS item: {item_error}")
                continue
        
        logger.info(f"📊 Form {form_type} (RSS fallback): {items_matching_date} matched date {target_date}, {len(forms)} successfully extracted")
    
    except ET.ParseError as parse_error:
        logger.error(f"❌ XML parsing error for Form {form_type} RSS: {parse_error}")
    except Exception as parse_error:
        logger.error(f"❌ Unexpected error parsing Form {form_type} RSS: {parse_error}")
    
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
        
        # If scheduler passes today's date (runs at 2 AM EST), convert to yesterday
        # Daily index files for yesterday are finalized, but today's might not be available yet
        try:
            date_obj = datetime.strptime(target_date, '%Y-%m-%d').date()
            today = datetime.now().date()
            if date_obj == today:
                # Convert to yesterday for scheduler runs (processes previous day's finalized filings)
                target_date = get_yesterday_date()
                logger.info(f"📅 Converted today's date to yesterday for scheduler run: {target_date}")
        except ValueError:
            pass  # Will be caught below
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

