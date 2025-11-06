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
            # SEC RSS feed for recent filings
            # Note: SEC changed RSS feed format - try different URL patterns
            rss_urls = [
                f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type={form_type}&company=&count=100&output=rss",
                f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type={form_type}&company=&count=100",
                f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type={form_type}&output=atom",
            ]
            
            rss_url = rss_urls[0]  # Try first format
            
            # Try multiple URL formats if first fails
            parsed_successfully = False
            for rss_url in rss_urls:
                try:
                    response = session.get(rss_url, timeout=30)
                    response.raise_for_status()
                    
                    # Check if response is actually XML/RSS
                    content_type = response.headers.get('Content-Type', '').lower()
                    content_preview = (response.text[:500] if hasattr(response, 'text') and response.text else 
                                      response.content[:500].decode('utf-8', errors='ignore'))
                    
                    # SEC RSS feeds sometimes return HTML error pages
                    if 'text/html' in content_type or (response.text and response.text.strip().startswith('<!')):
                        logger.debug(f"⚠️ Form {form_type} RSS URL returned HTML, trying next format: {rss_url}")
                        continue
                    
                    # If we get here, we have a valid response - try to parse
                    break
                    
                except Exception as url_error:
                    logger.debug(f"⚠️ Error with RSS URL {rss_url}: {url_error}, trying next...")
                    continue
            else:
                # If we exhausted all URLs, skip this form type
                logger.warning(f"⚠️ Could not fetch valid RSS feed for Form {form_type} with any URL format")
                continue
            
            # Try to parse as XML
            try:
                # SEC feeds might have encoding issues - try to decode properly
                if isinstance(response.content, bytes):
                    # Try UTF-8 first, then latin-1 as fallback
                    try:
                        xml_content = response.content.decode('utf-8')
                    except UnicodeDecodeError:
                        try:
                            xml_content = response.content.decode('latin-1')
                        except UnicodeDecodeError:
                            # Try with error handling
                            xml_content = response.content.decode('utf-8', errors='ignore')
                else:
                    xml_content = response.text
                
                # Remove BOM if present
                if xml_content and xml_content.startswith('\ufeff'):
                    xml_content = xml_content[1:]
                
                # Clean up any leading whitespace
                xml_content = xml_content.strip()
                
                # Try to parse XML - use XMLParser to handle errors gracefully
                parser = ET.XMLParser(encoding='utf-8')
                try:
                    root = ET.fromstring(xml_content.encode('utf-8'), parser=parser)
                except ET.ParseError:
                    # Try without encoding
                    root = ET.fromstring(xml_content)
                
                logger.debug(f"📄 Successfully parsed XML for Form {form_type}, root tag: {root.tag}")
                
                # RSS namespace
                ns = {'rss': 'http://purl.org/rss/1.0/', 'dc': 'http://purl.org/dc/elements/1.1/'}
                
                # Try without namespace first (some feeds don't use namespaces)
                items = root.findall('.//item')
                if not items:
                    # Try with common RSS namespaces
                    items = root.findall('.//{http://purl.org/rss/1.0/}item')
                if not items:
                    # Try Atom format
                    items = root.findall('.//{http://www.w3.org/2005/Atom}entry')
                if not items:
                    # Try RDF format
                    items = root.findall('.//{http://www.w3.org/1999/02/22-rdf-syntax-ns#}item')
                
                logger.info(f"📥 Fetched RSS feed for Form {form_type}: found {len(items)} items")
                
                if len(items) == 0:
                    logger.warning(f"⚠️ No items found in RSS feed for Form {form_type}. Root tag: {root.tag}")
                    # Log first few child elements to debug structure
                    if len(root) > 0:
                        logger.debug(f"Root children tags: {[child.tag for child in root[:5]]}")
                    continue  # Skip to next form type if no items
                
                # Track items before and after date filtering
                items_before_date_filter = len(items)
                items_matching_date = 0
                
                for item_index, item in enumerate(items):
                    try:
                        # Extract filing date - SEC uses Atom format with 'updated' field
                        pub_date_elem = None
                        for tag_name in [
                            '{http://www.w3.org/2005/Atom}updated',  # Atom format (SEC uses this)
                            'pubDate',  # RSS format
                            'date',  # Generic
                            '{http://purl.org/dc/elements/1.1/}date'  # Dublin Core
                        ]:
                            pub_date_elem = item.find(tag_name)
                            if pub_date_elem is not None:
                                break
                        
                        # Log item structure for first few items to debug
                        if item_index < 3:
                            # Get all child element tags
                            child_tags = [child.tag for child in item]
                            logger.info(f"🔍 RSS item #{item_index} child tags: {child_tags}")
                            if pub_date_elem is not None:
                                logger.info(f"✅ Found date element: tag='{pub_date_elem.tag}', text='{pub_date_elem.text}'")
                            else:
                                logger.warning(f"⚠️ No date element found in item #{item_index}")
                        
                        if pub_date_elem is None or not pub_date_elem.text:
                            if item_index < 3:
                                logger.debug(f"⏭️ Skipping item #{item_index}: no date element or empty text")
                            continue
                        
                        # Parse date (format: "Wed, 30 Oct 2025 16:30:00 EST")
                        try:
                            # Common RSS date formats
                            date_formats = [
                                '%Y-%m-%dT%H:%M:%S%z',  # Atom format: 2025-10-31T12:00:00-04:00
                                '%Y-%m-%dT%H:%M:%SZ',  # Atom format: 2025-10-31T12:00:00Z
                                '%Y-%m-%dT%H:%M:%S',  # Atom format: 2025-10-31T12:00:00
                                '%a, %d %b %Y %H:%M:%S %Z',  # RSS format
                                '%a, %d %b %Y %H:%M:%S %z',
                                '%a, %d %b %Y %H:%M:%S',
                                '%Y-%m-%d',
                                '%Y-%m-%d %H:%M:%S',
                                '%d %b %Y',
                                '%b %d, %Y',
                                '%m/%d/%Y',
                                '%Y/%m/%d'
                            ]
                            
                            filing_date_str = pub_date_elem.text.strip()
                            filing_date = None
                            
                            # Log first few date strings we encounter for debugging (before parsing)
                            if item_index < 3:
                                logger.info(f"🔍 Raw date string from RSS item #{item_index}: '{filing_date_str}'")
                            
                            for fmt in date_formats:
                                try:
                                    filing_date = datetime.strptime(filing_date_str, fmt).date()
                                    break
                                except ValueError:
                                    continue
                            
                            if not filing_date:
                                # Log failed date parsing for first few items
                                if item_index < 3:
                                    logger.warning(f"⚠️ Could not parse date: '{filing_date_str}' with any standard format")
                                continue
                            
                            # Log parsed date for first few items
                            if item_index < 3:
                                logger.info(f"📅 Parsed date: {filing_date} (target: {target_date})")
                            
                            # Compare dates (YYYY-MM-DD format)
                            target_date_obj = datetime.strptime(target_date, '%Y-%m-%d').date()
                            
                            if filing_date != target_date_obj:
                                # Log why we're skipping for first few items
                                if item_index < 3:
                                    logger.debug(f"⏭️ Skipping: filing_date={filing_date}, target={target_date_obj}")
                                continue  # Skip if not matching target date
                            
                            items_matching_date += 1
                            
                        except Exception as date_error:
                            logger.debug(f"⚠️ Could not parse date for item: {date_error}")
                            continue
                        
                        # Extract link/guid to get CIK and accession number
                        # Atom format uses <link href="..."/> with href attribute, RSS uses text content
                        link_elem = None
                        filing_url = None
                        
                        for tag_name in ['{http://www.w3.org/2005/Atom}link', 'link', 'guid']:
                            link_elem = item.find(tag_name)
                            if link_elem is not None:
                                break
                        
                        if link_elem is not None:
                            # Atom links use href attribute, RSS links use text content
                            if link_elem.get('href'):
                                filing_url = link_elem.get('href').strip()
                            elif link_elem.text:
                                filing_url = link_elem.text.strip()
                        
                        if not filing_url:
                            if item_index < 3:
                                logger.debug(f"⚠️ Could not extract link URL from item #{item_index}")
                            continue
                        
                        # Extract CIK and accession number from URL
                        # Format: https://www.sec.gov/cgi-bin/viewer?action=view&cik={CIK}&accession_number={ACCESSION}&xbrl_type=v
                        # Or: https://www.sec.gov/Archives/edgar/data/{CIK}/{ACCESSION}/...
                        
                        cik = None
                        accession_number = None
                        filename = None
                        
                        # Try to extract from different URL formats
                        if '/Archives/edgar/data/' in filing_url:
                            # Format: .../Archives/edgar/data/{CIK}/{ACCESSION}/{filename}
                            parts = filing_url.split('/Archives/edgar/data/')[1].split('/')
                            if len(parts) >= 2:
                                cik = parts[0].strip()
                                accession_number = parts[1].strip().replace('-', '')  # Remove dashes
                                if len(parts) >= 3:
                                    filename = parts[2].strip()
                        
                        elif 'cik=' in filing_url and 'accession_number=' in filing_url:
                            # Format: ...?cik={CIK}&accession_number={ACCESSION}...
                            parsed_url = urlparse(filing_url)
                            params = parse_qs(parsed_url.query)
                            cik = params.get('cik', [None])[0]
                            accession_number = params.get('accession_number', [None])[0]
                            if accession_number:
                                accession_number = accession_number.replace('-', '')
                        
                        # If we still don't have CIK/accession, try to find primary document link
                        if not cik or not accession_number:
                            # Look for description or content that might have links
                            desc_elem = item.find('description')
                            if desc_elem is not None and desc_elem.text:
                                # Try to extract from description HTML
                                cik_match = re.search(r'CIK[:\s]+(\d+)', desc_elem.text, re.IGNORECASE)
                                acc_match = re.search(r'Accession[:\s]+([\d-]+)', desc_elem.text, re.IGNORECASE)
                                if cik_match:
                                    cik = cik_match.group(1)
                                if acc_match:
                                    accession_number = acc_match.group(1).replace('-', '')
                        
                        if not cik or not accession_number:
                            logger.debug(f"⚠️ Could not extract CIK/accession from: {filing_url}")
                            continue
                        
                        # Determine filename - try to get primary document
                        # Usually the primary document is: {accession}.txt or {accession}-primary-document.xml
                        if not filename:
                            # Try common patterns
                            accession_dashed = f"{accession_number[:10]}-{accession_number[10:12]}-{accession_number[12:]}"
                            
                            # Primary document is usually the .txt file with the accession number
                            filename = f"{accession_dashed}.txt"
                            # But might also be XML
                            # We'll try .txt first, then .xml if download fails
                        
                        # Add to forms list
                        forms.append({
                            'form_type': f'form{form_type}',
                            'cik': cik,
                            'accession_number': accession_number,
                            'filename': filename,
                            'filing_date': target_date
                        })
                        
                        logger.debug(f"✅ Extracted Form {form_type}: CIK={cik}, Accession={accession_number[:10]}...")
                        
                    except Exception as item_error:
                        logger.warning(f"⚠️ Error parsing RSS item: {item_error}")
                        continue
                
                # Log summary of date filtering
                if items_before_date_filter > 0:
                    forms_for_this_type = [f for f in forms if f.get('form_type') == f'form{form_type}']
                    logger.info(f"📊 Form {form_type}: {items_before_date_filter} total items in RSS, {items_matching_date} matched date {target_date}, {len(forms_for_this_type)} successfully extracted")
            
            except ET.ParseError as parse_error:
                logger.error(f"❌ XML parsing error for Form {form_type} RSS: {parse_error}")
                logger.error(f"Response URL: {rss_url}")
                if 'response' in locals():
                    logger.error(f"Response status: {response.status_code}")
                    logger.error(f"Response headers: {dict(response.headers)}")
                    # Log first 1000 chars of response for debugging
                    response_preview = (response.text[:1000] if hasattr(response, 'text') and response.text 
                                       else response.content[:1000].decode('utf-8', errors='replace'))
                    logger.error(f"Response content (first 1000 chars): {response_preview}")
                continue
            except Exception as parse_error:
                logger.error(f"❌ Unexpected error parsing Form {form_type} RSS: {parse_error}")
                logger.error(f"Response URL: {rss_url if 'rss_url' in locals() else 'unknown'}")
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
    Lambda handler for fetching SEC forms and Congressional PTRs
    
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
    
    # Parse date input - support backdate, single date, or default
    # Options:
    # 1. Backdate: {"backdate": "2025-11-05"} - fetches from today back to backdate
    # 2. Single date: {"date": "2025-10-30"}
    # 3. Default: yesterday's date (for scheduled runs)
    target_dates = []
    
    if isinstance(event, dict):
        if event.get('backdate'):
            # Backdate mode - fetch from today back to backdate
            backdate_str = event.get('backdate')
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
            
            # Step 1: SEC Forms are now handled by Glue job (not fetched here)
            # SEC pipeline is handled entirely by Glue job in Step Functions
            logger.info("📋 SEC Forms fetching skipped - handled by Glue job in Step Functions")
            aggregate_results['secForms'] = []  # Empty - Glue handles SEC
            aggregate_results['secFormsFetched'] = 0
            
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

