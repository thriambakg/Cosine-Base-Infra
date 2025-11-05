"""
AWS Glue Job: SEC Forms ETL Pipeline
Fetches, downloads, parses, matches, and stores SEC Form 3/4/5 filings to DynamoDB

This job handles the entire SEC pipeline:
1. Fetch SEC filings using paginated browse-edgar API
2. Download forms in parallel
3. Parse XML/HTML to extract trades
4. Match filers to politicians
5. Write matched trades to DynamoDB
6. Write summary to S3 for aggregator
"""

import sys
import json
import re
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from decimal import Decimal
import xml.etree.ElementTree as ET
from html import unescape
from difflib import SequenceMatcher
import csv
from io import StringIO

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import boto3
import requests

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Get job parameters
# Note: getResolvedOptions requires ALL listed arguments to be provided
# Only include required arguments here
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'date',  # Target date in YYYY-MM-DD format
    's3_bucket',
    'dynamodb_table'
])

job.init(args['JOB_NAME'], args)

# Extract parameters
target_date = args.get('date')
s3_bucket = args.get('s3_bucket')
dynamodb_table = args.get('dynamodb_table')

# Default to yesterday if date is not provided, is null, empty string, or the string "null"
# Step Functions may pass null as the string "null" or as an empty string
if not target_date or target_date.strip() == '' or target_date.lower() == 'null':
    yesterday = datetime.now() - timedelta(days=1)
    target_date = yesterday.strftime('%Y-%m-%d')
    logger.info(f"⚠️ No date provided (or date was null/empty), defaulting to yesterday: {target_date}")
else:
    logger.info(f"📅 Processing SEC forms for date: {target_date}")

# Optional date range parameters (not currently used, but can be added via --additional-python-modules or custom parsing if needed)
# start_date = args.get('start_date')  # Not currently used
# end_date = args.get('end_date')       # Not currently used

# NOTE: Do NOT create boto3 clients at module level - they contain SSLContext objects that can't be pickled
# Create clients inside functions that need them to avoid Spark serialization issues

# SEC API configuration
SEC_BASE_URL = "https://www.sec.gov"
SEC_USER_AGENT = "Cosine Financial Platform contact@cosine.financial"
SEC_BROWSE_EDGAR_URL = f"{SEC_BASE_URL}/cgi-bin/browse-edgar"

# Name matching threshold
NAME_MATCH_THRESHOLD = 0.85  # 85% similarity

# Standard Senate PTR ranges for amount mapping
SENATE_PTR_RANGES = [
    (0, 1000),
    (1001, 15000),
    (15001, 50000),
    (50001, 100000),
    (100001, 250000),
    (250001, 500000),
    (500001, 1000000),
    (1000001, 5000000),
    (5000001, 25000000),
    (25000001, 50000000),
    (50000001, None)  # Over $50,000,000
]


def find_standard_range(amount_value: float) -> tuple:
    """Find the standard Senate PTR range that contains the given amount"""
    if amount_value <= 0:
        return SENATE_PTR_RANGES[0]
    
    for range_min, range_max in SENATE_PTR_RANGES:
        if range_max is None:
            if amount_value >= range_min:
                return (range_min, None)
        else:
            if range_min <= amount_value <= range_max:
                return (range_min, range_max)
    return SENATE_PTR_RANGES[0]


def load_politician_list() -> List[Dict[str, Any]]:
    """Load congress-legislators CSV from S3"""
    # Create client locally to avoid Spark serialization issues
    s3_client_local = boto3.client('s3')
    try:
        response = s3_client_local.get_object(
            Bucket=s3_bucket,
            Key='congress-legislators.csv'
        )
        
        csv_content = response['Body'].read().decode('utf-8')
        csv_reader = csv.DictReader(StringIO(csv_content))
        
        politicians = []
        for row in csv_reader:
            # Prefer full_name if available
            if row.get('full_name') and row.get('full_name').strip():
                primary_name = row.get('full_name').strip()
            else:
                name_parts = []
                if row.get('first_name'):
                    name_parts.append(row['first_name'])
                if row.get('middle_name'):
                    name_parts.append(row['middle_name'])
                if row.get('last_name'):
                    name_parts.append(row['last_name'])
                if row.get('suffix'):
                    name_parts.append(row['suffix'])
                primary_name = ' '.join(name_parts) if name_parts else ''
            
            # Build alternative names
            alt_names = []
            if row.get('nickname'):
                alt_names.append(row['nickname'])
            
            # Determine position
            leg_type = row.get('type', '').lower().strip()
            if leg_type == 'sen':
                position = 'Senate'
            elif leg_type == 'rep':
                position = 'House'
            else:
                position = leg_type
            
            politicians.append({
                'name': primary_name,
                'party': row.get('party', '').strip(),
                'position': position,
                'websiteUrl': row.get('url', '').strip() if row.get('url') else None,
                'alternativeNames': alt_names,
                'bioguide_id': row.get('bioguide_id', '').strip() if row.get('bioguide_id') else None,
            })
        
        logger.info(f"✅ Loaded {len(politicians)} politicians from CSV")
        return politicians
        
    except Exception as e:
        logger.error(f"❌ Error loading politician list: {e}")
        raise


def fetch_sec_forms_paginated(target_date: str, form_types: List[str] = ['3', '4', '5']) -> List[Dict[str, Any]]:
    """
    Fetch SEC forms using paginated browse-edgar API
    
    Uses the browse-edgar endpoint with start parameter for pagination:
    https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&owner=only&start=0&count=100
    
    Args:
        target_date: Target date in YYYY-MM-DD format
        form_types: List of form types to fetch (default: ['3', '4', '5'])
    
    Returns:
        List of form metadata dicts with keys: form_type, cik, accession_number, filename, filing_date
    """
    all_forms = []
    target_date_obj = datetime.strptime(target_date, '%Y-%m-%d').date()
    
    session = requests.Session()
    session.headers.update({
        'User-Agent': SEC_USER_AGENT,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-US,en;q=0.9',
        'Cache-Control': 'max-age=0',
        'Upgrade-Insecure-Requests': '1'
    })
    
    for form_type in form_types:
        logger.info("")
        logger.info(f"   📋 Fetching Form {form_type} filings for {target_date}...")
        forms_for_type = []
        start = 0
        count = 100
        page = 1
        max_pages = 100  # Safety limit to prevent infinite loops
        form_type_start = datetime.now()
        
        while page <= max_pages:
            try:
                # Build paginated URL (matches user's CURL example)
                url = f"{SEC_BROWSE_EDGAR_URL}?action=getcurrent&datea=&dateb=&company=&type={form_type}&SIC=&State=&Country=&CIK=&owner=only&accno=&start={start}&count={count}"
                
                logger.info(f"   Fetching page {page} (start={start}, count={count})...")
                response = session.get(url, timeout=30)
                response.raise_for_status()
                
                html_content = response.text
                
                # Parse HTML table to extract filing information
                # SEC browse-edgar returns an HTML table with filing data
                # Each row has: CIK, Company Name, Form Type, Date Filed, File Number
                
                # Extract table rows
                # Look for table rows with filing data
                # Pattern: <tr> with links to /Archives/edgar/data/
                table_row_pattern = re.compile(
                    r'<tr[^>]*>(.*?)</tr>',
                    re.DOTALL | re.IGNORECASE
                )
                
                rows = table_row_pattern.findall(html_content)
                
                page_forms = []
                page_forms_before_date_filter = 0
                
                for row in rows:
                    # Extract CIK and accession from archive links
                    archive_link_pattern = re.compile(
                        r'/Archives/edgar/data/(\d+)/([^/"]+)/',
                        re.IGNORECASE
                    )
                    
                    archive_matches = archive_link_pattern.findall(row)
                    
                    if not archive_matches:
                        continue
                    
                    page_forms_before_date_filter += 1
                    
                    # Extract CIK and accession
                    cik, accession_raw = archive_matches[0]
                    
                    # Clean accession number (remove dashes, ensure 18 digits)
                    accession_clean = accession_raw.replace('-', '').replace('/', '').strip()
                    
                    # Accession numbers are 18 digits
                    if len(accession_clean) < 10:
                        continue
                    
                    # If shorter than 18, pad or truncate (SEC format is 10-2-6)
                    if len(accession_clean) != 18:
                        # Try to reconstruct if it has dashes in original
                        if '-' in accession_raw:
                            parts = accession_raw.split('-')
                            if len(parts) == 3:
                                accession_clean = f"{parts[0].zfill(10)}{parts[1].zfill(2)}{parts[2].zfill(6)}"
                            else:
                                continue
                        else:
                            accession_clean = accession_clean[:18].zfill(18)
                    
                    # Extract dates from row
                    # SEC HTML table structure: 
                    # Columns: Form | Formats | Description | Accepted | Filing Date | File/Film No
                    # Accepted is column 4 (index 3), Filing Date is column 5 (index 4)
                    filing_date_str = None
                    accepted_date_str = None
                    
                    # Pattern 1: Look for dates in table cells (extract <td> content)
                    td_pattern = re.compile(r'<td[^>]*>(.*?)</td>', re.DOTALL | re.IGNORECASE)
                    cells = td_pattern.findall(row)
                    
                    # Column 4 (index 3) is the Accepted column
                    # Format: YYYY-MM-DD<br>HH:MM:SS (e.g., "2025-11-04<br>21:50:26")
                    if len(cells) >= 4:
                        accepted_cell = cells[3]
                        # Replace <br> tags with space to preserve structure
                        accepted_html = re.sub(r'<br[^>]*>', ' ', accepted_cell, flags=re.IGNORECASE)
                        # Remove all other HTML tags
                        accepted_text = re.sub(r'<[^>]+>', '', accepted_html).strip()
                        # Clean up multiple spaces
                        accepted_text = re.sub(r'\s+', ' ', accepted_text)
                        
                        # Extract full timestamp: YYYY-MM-DD HH:MM:SS
                        timestamp_match = re.search(r'(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})', accepted_text)
                        if timestamp_match:
                            date_part = timestamp_match.group(1)
                            time_part = timestamp_match.group(2)
                            accepted_date_str = f"{date_part} {time_part}"  # Full timestamp: "2025-11-04 21:50:26"
                        else:
                            # Fallback: try without space separator (in case text was collapsed)
                            timestamp_match = re.search(r'(\d{4}-\d{2}-\d{2})(\d{2}:\d{2}:\d{2})', accepted_text)
                            if timestamp_match:
                                date_part = timestamp_match.group(1)
                                time_part = timestamp_match.group(2)
                                accepted_date_str = f"{date_part} {time_part}"
                    
                    # Column 5 (index 4) is the Filing Date column
                    # Format: YYYY-MM-DD (e.g., "2025-11-04")
                    if len(cells) >= 5:
                        filing_date_cell = cells[4]
                        # Clean HTML tags from cell
                        cell_text = re.sub(r'<[^>]+>', '', filing_date_cell).strip()
                        # Look for YYYY-MM-DD pattern (SEC standard format)
                        date_match = re.search(r'(\d{4}-\d{2}-\d{2})', cell_text)
                        if date_match:
                            filing_date_str = date_match.group(1)
                        else:
                            # Fallback: try MM/DD/YYYY pattern
                            date_match = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', cell_text)
                            if date_match:
                                filing_date_str = date_match.group(1)
                    
                    # Pattern 2: Fallback - search entire row for YYYY-MM-DD (SEC format)
                    if not filing_date_str:
                        date_match = re.search(r'(\d{4}-\d{2}-\d{2})', row)
                        if date_match:
                            filing_date_str = date_match.group(1)
                        else:
                            # Fallback: MM/DD/YYYY
                            date_match = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', row)
                            if date_match:
                                filing_date_str = date_match.group(1)
                    
                    # Parse and filter by target date
                    if filing_date_str:
                        try:
                            # Try YYYY-MM-DD format first (SEC standard)
                            try:
                                filing_date_obj = datetime.strptime(filing_date_str, '%Y-%m-%d').date()
                            except ValueError:
                                # Fallback to MM/DD/YYYY
                                filing_date_obj = datetime.strptime(filing_date_str, '%m/%d/%Y').date()
                            
                            # Only include if matches target date
                            if filing_date_obj != target_date_obj:
                                continue
                        except:
                            # If date parsing fails, include anyway (will verify during download)
                            pass
                    else:
                        # No date found in row - include anyway (will verify during download)
                        # This is important because some forms might not have dates in the table
                        pass
                    
                    form_data = {
                        'cik': cik,
                        'accession_number': accession_clean,
                        'form_type': f'form{form_type}',
                        'filing_date': filing_date_str or target_date,
                        'accepted_date': accepted_date_str
                    }
                    
                    page_forms.append(form_data)
                
                logger.info(f"   Page {page}: Found {len(page_forms)} forms matching date {target_date} (out of {page_forms_before_date_filter} total forms on page)")
                
                # Check if FIRST file in batch matches target date
                # If first file doesn't match target date, we've moved to a different day - stop fetching
                first_file_date = None
                if page_forms:
                    first_form = page_forms[0]
                    first_filing_date = first_form.get('filing_date')
                    if first_filing_date:
                        try:
                            # Try YYYY-MM-DD format first
                            try:
                                first_file_date = datetime.strptime(first_filing_date, '%Y-%m-%d').date()
                            except ValueError:
                                # Fallback to MM/DD/YYYY
                                first_file_date = datetime.strptime(first_filing_date, '%m/%d/%Y').date()
                        except:
                            pass
                
                # If first file's date doesn't match target date, stop fetching
                if first_file_date and first_file_date != target_date_obj:
                    logger.info(f"   First file in batch has date {first_file_date} (target: {target_date_obj}), "
                              f"stopping pagination - reached different day")
                    break
                
                # Add forms from this page
                forms_for_type.extend(page_forms)
                
                logger.info(f"   Page {page}: Found {len(page_forms)} forms matching date {target_date} "
                          f"(total so far: {len(forms_for_type)})")
                
                # Stop if no forms found (empty page)
                if page_forms_before_date_filter == 0:
                    logger.info(f"   No forms found on page {page}, stopping pagination")
                    break
                
                # 4. Continue if we still have matching dates or dates after target
                # (we might have more pages with our target date)
                
                # Move to next page
                start += count
                page += 1
                
                # Rate limiting (SEC requires 10 requests/second max)
                import time
                time.sleep(0.2)  # 200ms delay between requests
                
            except requests.exceptions.RequestException as e:
                logger.error(f"❌ Error fetching page {page} for Form {form_type}: {e}")
                break
            except Exception as e:
                logger.error(f"❌ Unexpected error on page {page} for Form {form_type}: {e}")
                import traceback
                logger.error(f"   Traceback: {traceback.format_exc()}")
                break
        
        form_type_duration = (datetime.now() - form_type_start).total_seconds()
        logger.info(f"   ✅ Form {form_type} Complete: Found {len(forms_for_type)} filings in {page-1} pages ({form_type_duration:.2f} seconds)")
        all_forms.extend(forms_for_type)
    
    logger.info(f"📊 Total forms fetched: {len(all_forms)}")
    return all_forms


def download_sec_form(form_data: Dict[str, Any], target_date: str, s3_bucket_name: str) -> Optional[Dict[str, Any]]:
    """
    Download SEC form and return file content
    
    Args:
        form_data: Form metadata
        target_date: Target date for S3 key
        s3_bucket_name: S3 bucket name (passed explicitly to avoid capturing module-level vars)
    
    Returns:
        Dict with 's3_key', 'content', 'file_ext' or None if download fails
    """
    # Import inside function to avoid serialization issues
    import logging
    local_logger = logging.getLogger()
    
    cik = form_data.get('cik', 'unknown')
    accession = form_data.get('accession_number', 'unknown')
    
    try:
        form_type = form_data.get('form_type')
        
        if not all([cik, accession]):
            local_logger.warning(f"   ⚠️ Missing CIK/accession: CIK={cik}, Accession={accession}")
            return None
        
        # Format accession number
        if len(accession) == 18:
            accession_dashed = f"{accession[:10]}-{accession[10:12]}-{accession[12:]}"
        else:
            accession_dashed = accession
        
        # Use constants directly (strings are safe to serialize)
        SEC_BASE_URL_LOCAL = "https://www.sec.gov"
        SEC_USER_AGENT_LOCAL = "Cosine Financial Platform contact@cosine.financial"
        
        base_url = f"{SEC_BASE_URL_LOCAL}/Archives/edgar/data/{cik}/{accession_dashed}"
        
        # Create session inside function - each worker gets its own
        session = requests.Session()
        session.headers.update({'User-Agent': SEC_USER_AGENT_LOCAL})
        
        # Try to download the file
        file_extensions = ['.xml', '.txt', '.pdf']
        file_content = None
        file_ext = None
        successful_url = None
        
        for ext in file_extensions:
            try:
                if ext == '.xml':
                    file_url = f"{base_url}/{accession_dashed}-primary-document.xml"
                elif ext == '.txt':
                    file_url = f"{base_url}/{accession_dashed}.txt"
                else:
                    file_url = f"{base_url}/{accession_dashed}.pdf"
                
                response = session.get(file_url, timeout=30)
                if response.status_code == 200:
                    file_content = response.content
                    file_ext = ext[1:]  # Remove dot
                    successful_url = file_url
                    
                    # For .txt files, check if it's XML
                    if ext == '.txt' and file_content.startswith(b'<?xml'):
                        file_ext = 'xml'
                    
                    local_logger.info(f"   ✅ DOWNLOAD SUCCESS: CIK={cik}, Accession={accession}, URL={file_url}, Size={len(file_content)} bytes")
                    break
                else:
                    local_logger.debug(f"      Attempted {ext}: Status {response.status_code}")
            except Exception as e:
                local_logger.debug(f"      Attempted {ext}: Error - {str(e)[:100]}")
                continue
        
        if not file_content:
            local_logger.error(f"   ❌ DOWNLOAD FAILED: CIK={cik}, Accession={accession_dashed}")
            local_logger.error(f"      Tried URLs: {base_url}/[accession]-primary-document.xml, "
                          f"{base_url}/[accession].txt, {base_url}/[accession].pdf")
            return None
        
        # Generate S3 key
        s3_key = f"trades/{target_date}/sec/{form_type}-{cik}-{target_date}.{file_ext}"
        
        # Create S3 client locally to avoid Spark serialization issues
        s3_client_local = boto3.client('s3')
        # Upload to S3
        s3_client_local.put_object(
            Bucket=s3_bucket_name,
            Key=s3_key,
            Body=file_content,
            ContentType='application/xml' if file_ext == 'xml' else 'application/pdf'
        )
        
        local_logger.info(f"   ✅ S3 UPLOAD SUCCESS: S3Key={s3_key}, Bucket={s3_bucket_name}, Size={len(file_content)} bytes")
        
        return {
            's3_key': s3_key,
            'content': file_content,
            'file_ext': file_ext,
            'cik': cik,
            'accession_number': accession,
            'form_type': form_type,
            'filing_date': target_date
        }
        
    except Exception as e:
        local_logger.error(f"   ❌ Error downloading form (CIK={cik}, Accession={accession}): {e}")
        import traceback
        local_logger.error(f"      Traceback: {traceback.format_exc()}")
        return None


def parse_sec_form_html(html_content: str, s3_key: str, filing_date: str) -> List[Dict[str, Any]]:
    """
    Parse SEC Form HTML and extract trade data (full implementation from SEC matcher)
    """
    # Import inside function to avoid serialization issues
    import logging
    local_logger = logging.getLogger()
    
    trades = []
    
    try:
        local_logger.info(f"   🔍 PARSING: Starting parse for S3Key={s3_key}")
        
        # Extract filer/owner name
        # Pattern: <a href="/cgi-bin/browse-edgar?action=getcompany&CIK=...">Name</a>
        name_match = re.search(r'<a[^>]*href="/cgi-bin/browse-edgar[^"]*CIK=\d+">([^<]+)</a>', html_content, re.IGNORECASE)
        filer_name = None
        if name_match:
            filer_name = unescape(name_match.group(1)).strip()
        
        if not filer_name:
            # Try alternative pattern: name might be in different format
            name_patterns = [
                r'Name and Address of Reporting Person[^<]*<[^>]*>([^<]+)</[^>]*>',
                r'Reporting Person[^<]*<[^>]*>([^<]+)</[^>]*>',
            ]
            for pattern in name_patterns:
                match = re.search(pattern, html_content, re.IGNORECASE | re.DOTALL)
                if match:
                    filer_name = unescape(match.group(1)).strip()
                    break
        
        if not filer_name:
            local_logger.warning(f"   ⚠️ PARSE WARNING: Could not extract filer name from HTML S3Key={s3_key}")
            return trades
        
        local_logger.info(f"   ✅ PARSED FILER NAME: FilerName={filer_name}, S3Key={s3_key}")
        
        # Detect form type
        form_number = None
        form_name_match = re.search(r'class="FormName"[^>]*>FORM\s*(\d+)', html_content, re.IGNORECASE | re.DOTALL)
        if form_name_match:
            form_number = form_name_match.group(1)
        
        if not form_number:
            form_in_filename = re.search(r'form[_-]?(\d+)', s3_key, re.IGNORECASE)
            if form_in_filename:
                form_number = form_in_filename.group(1)
        
        if not form_number:
            form_type_match = re.search(r'<title>SEC\s+FORM\s+(\d+)</title>', html_content, re.IGNORECASE | re.DOTALL)
            if form_type_match:
                form_number = form_type_match.group(1)
        
        if not form_number:
            form_match = re.search(r'\bFORM\s+([345])\b', html_content, re.IGNORECASE)
            if form_match:
                form_number = form_match.group(1)
        
        is_form3 = form_number == '3'
        is_form4 = form_number == '4'
        is_form5 = form_number == '5'
        
        if not form_number:
            is_form4 = True
            form_number = '4'
        
        # Extract issuer name and ticker
        issuer_match = re.search(r'Issuer Name[^<]*<a[^>]*>([^<]+)</a>', html_content, re.IGNORECASE)
        issuer_name = issuer_match.group(1).strip() if issuer_match else None
        
        ticker_match = re.search(r'\[ <span[^>]*>([A-Z0-9]+)</span> \]', html_content)
        ticker = ticker_match.group(1) if ticker_match else None
        
        # Extract filing date
        filing_date_extracted = None
        if is_form4:
            date_match = re.search(r'Date of Earliest Transaction[^<]*<span[^>]*>(\d{1,2}/\d{1,2}/\d{4})</span>', html_content, re.IGNORECASE)
            if date_match:
                try:
                    filing_date_obj = datetime.strptime(date_match.group(1), '%m/%d/%Y')
                    filing_date_extracted = filing_date_obj.strftime('%Y-%m-%d')
                except:
                    pass
        elif is_form3:
            date_match = re.search(r'Date of Event Requiring Statement[^<]*<span[^>]*>(\d{1,2}/\d{1,2}/\d{4})</span>', html_content, re.IGNORECASE)
            if date_match:
                try:
                    filing_date_obj = datetime.strptime(date_match.group(1), '%m/%d/%Y')
                    filing_date_extracted = filing_date_obj.strftime('%Y-%m-%d')
                except:
                    pass
        
        filing_date_final = filing_date_extracted or filing_date
        
        # Parse Table I - Non-Derivative Securities
        table1_pattern = r'Table I[^<]*<tbody>(.*?)</tbody>'
        table1_match = re.search(table1_pattern, html_content, re.IGNORECASE | re.DOTALL)
        
        if table1_match:
            tbody_content = table1_match.group(1)
            rows = re.findall(r'<tr[^>]*>(.*?)</tr>', tbody_content, re.DOTALL | re.IGNORECASE)
            
            for row in rows:
                cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL | re.IGNORECASE)
                
                def clean_cell(cell):
                    text = re.sub(r'<[^>]+>', '', cell)
                    text = unescape(text)
                    return text.strip()
                
                if is_form4 and len(cells) >= 8:
                    security_name = clean_cell(cells[0]) if len(cells) > 0 else None
                    trans_date = clean_cell(cells[1]) if len(cells) > 1 else None
                    trans_code = clean_cell(cells[3]) if len(cells) > 3 else None
                    shares_str = clean_cell(cells[5]) if len(cells) > 5 else None
                    trans_type = clean_cell(cells[6]) if len(cells) > 6 else None
                    price_str = clean_cell(cells[7]) if len(cells) > 7 else None
                    
                    has_transaction = (trans_date and trans_date.strip() and 
                                     trans_code and trans_code.strip() and
                                     shares_str and shares_str.strip())
                    
                    if security_name and has_transaction:
                        shares = None
                        if shares_str:
                            try:
                                shares = int(re.sub(r'[,\.]', '', shares_str))
                            except:
                                pass
                        
                        price = None
                        if price_str:
                            try:
                                price_str_clean = re.sub(r'[\$,]', '', price_str)
                                price = float(price_str_clean)
                            except:
                                pass
                        
                        transaction_date = None
                        try:
                            trans_date_obj = datetime.strptime(trans_date, '%m/%d/%Y')
                            transaction_date = trans_date_obj.strftime('%Y-%m-%d')
                        except:
                            transaction_date = filing_date_final
                        
                        total_amount = None
                        if shares and price:
                            total_amount = shares * price
                        
                        trade = {
                            'filerName': filer_name,
                            'issuerName': issuer_name,
                            'securitySymbol': ticker,
                            'securityName': security_name,
                            'transactionDate': transaction_date,
                            'filingDate': filing_date_final,
                            'transactionType': trans_code,
                            'shares': shares,
                            'pricePerShare': price,
                            'totalAmount': total_amount,
                            'transactionDirection': trans_type,
                            'formType': f'form{form_number}' if form_number else 'form4',
                        }
                        trades.append(trade)
                elif is_form3 and len(cells) >= 4:
                    security_name = clean_cell(cells[0]) if len(cells) > 0 else None
                    shares_owned_str = clean_cell(cells[1]) if len(cells) > 1 else None
                    ownership_form = clean_cell(cells[2]) if len(cells) > 2 else None
                    indirect_nature = clean_cell(cells[3]) if len(cells) > 3 else None
                    
                    if security_name and shares_owned_str:
                        shares = None
                        try:
                            shares = int(re.sub(r'[,\.]', '', shares_owned_str))
                        except:
                            pass
                        
                        trade = {
                            'filerName': filer_name,
                            'issuerName': issuer_name,
                            'securitySymbol': ticker,
                            'securityName': security_name,
                            'transactionDate': filing_date_final,
                            'filingDate': filing_date_final,
                            'transactionType': 'I',
                            'shares': shares,
                            'pricePerShare': None,
                            'totalAmount': None,
                            'transactionDirection': 'A',
                            'formType': 'form3',
                            'ownershipForm': ownership_form,
                            'indirectNature': indirect_nature,
                            'isInitialOwnership': True,
                        }
                        trades.append(trade)
        
        # Parse Table II - Derivative Securities
        table2_pattern = r'Table II[^<]*<tbody>(.*?)</tbody>'
        table2_match = re.search(table2_pattern, html_content, re.IGNORECASE | re.DOTALL)
        
        if table2_match:
            tbody_content = table2_match.group(1)
            rows = re.findall(r'<tr[^>]*>(.*?)</tr>', tbody_content, re.DOTALL | re.IGNORECASE)
            
            for row in rows:
                cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL | re.IGNORECASE)
                
                def clean_cell(cell):
                    text = re.sub(r'<[^>]+>', '', cell)
                    text = unescape(text)
                    return text.strip()
                
                if is_form4 and len(cells) >= 10:
                    derivative_name = clean_cell(cells[0]) if len(cells) > 0 else None
                    exercise_price_str = clean_cell(cells[1]) if len(cells) > 1 else None
                    trans_date = clean_cell(cells[2]) if len(cells) > 2 else None
                    trans_code = clean_cell(cells[4]) if len(cells) > 4 else None
                    shares_acquired = clean_cell(cells[6]) if len(cells) > 6 else None
                    shares_disposed = clean_cell(cells[7]) if len(cells) > 7 else None
                    underlying_title = clean_cell(cells[10]) if len(cells) > 10 else None
                    price_str = clean_cell(cells[12]) if len(cells) > 12 else None
                    
                    has_transaction = (trans_date and trans_date.strip() and 
                                     trans_code and trans_code.strip() and
                                     (shares_acquired and shares_acquired.strip() or 
                                      shares_disposed and shares_disposed.strip()))
                    
                    if derivative_name and has_transaction:
                        shares = None
                        if shares_acquired and shares_acquired.strip():
                            try:
                                shares = int(re.sub(r'[,\.]', '', shares_acquired))
                            except:
                                pass
                        elif shares_disposed and shares_disposed.strip():
                            try:
                                shares = -int(re.sub(r'[,\.]', '', shares_disposed))
                            except:
                                pass
                        
                        if shares is None:
                            continue
                        
                        price = None
                        if price_str and price_str.strip():
                            try:
                                price_str_clean = re.sub(r'[\$,]', '', price_str)
                                price = float(price_str_clean)
                            except:
                                pass
                        elif exercise_price_str and exercise_price_str.strip():
                            try:
                                price_str_clean = re.sub(r'[\$,]', '', exercise_price_str)
                                price = float(price_str_clean)
                            except:
                                pass
                        
                        transaction_date = None
                        try:
                            trans_date_obj = datetime.strptime(trans_date, '%m/%d/%Y')
                            transaction_date = trans_date_obj.strftime('%Y-%m-%d')
                        except:
                            transaction_date = filing_date_final
                        
                        exercise_price = None
                        if exercise_price_str and exercise_price_str.strip():
                            try:
                                exercise_price = float(re.sub(r'[\$,]', '', exercise_price_str))
                            except:
                                pass
                        
                        total_amount = None
                        if shares and price:
                            total_amount = abs(shares) * price
                        
                        trade = {
                            'filerName': filer_name,
                            'issuerName': issuer_name,
                            'securitySymbol': ticker,
                            'securityName': f"{derivative_name} (underlying: {underlying_title})" if underlying_title else derivative_name,
                            'transactionDate': transaction_date,
                            'filingDate': filing_date_final,
                            'transactionType': trans_code,
                            'shares': shares,
                            'pricePerShare': price,
                            'totalAmount': total_amount,
                            'exercisePrice': exercise_price,
                            'formType': f'form{form_number}' if form_number else 'form4',
                            'isDerivative': True,
                        }
                        trades.append(trade)
        
        local_logger.info(f"   ✅ PARSED DATA: Extracted {len(trades)} trades from S3Key={s3_key}")
        
        # Log all parsed trades for verification
        if trades:
            local_logger.info(f"   📋 PARSED TRADES DETAIL (S3Key={s3_key}):")
            for trade_idx, trade in enumerate(trades, 1):
                local_logger.info(f"      Trade {trade_idx}: Filer={trade.get('filerName', 'N/A')}, "
                                f"Security={trade.get('securityName', 'N/A')[:60]}, "
                                f"Symbol={trade.get('securitySymbol', 'N/A')}, "
                                f"Type={trade.get('transactionType', 'N/A')}, "
                                f"Amount=${trade.get('totalAmount', 'N/A')}, "
                                f"Shares={trade.get('shares', 'N/A')}, "
                                f"Date={trade.get('transactionDate', 'N/A')}")
        else:
            local_logger.warning(f"   ⚠️ PARSED DATA: No trades extracted from S3Key={s3_key}")
        
    except Exception as e:
        local_logger.error(f"   ❌ PARSE ERROR: Error parsing SEC form HTML S3Key={s3_key}: {e}")
        import traceback
        local_logger.error(f"      Traceback: {traceback.format_exc()}")
    
    return trades


def fuzzy_match_name(filer_name: str, politician: Dict[str, Any]) -> float:
    """Fuzzy match filer name to politician name"""
    politician_name = politician.get('name', '')
    
    # Try exact match first
    if filer_name.lower() == politician_name.lower():
        return 1.0
    
    # Try matching against alternative names
    for alt_name in politician.get('alternativeNames', []):
        if filer_name.lower() == alt_name.lower():
            return 0.95
    
    # Use SequenceMatcher for fuzzy matching
    similarity = SequenceMatcher(None, filer_name.lower(), politician_name.lower()).ratio()
    
    # Boost score if last names match
    filer_last = filer_name.split()[-1].lower() if filer_name.split() else ''
    politician_last = politician_name.split()[-1].lower() if politician_name.split() else ''
    if filer_last and politician_last and filer_last == politician_last:
        similarity = min(1.0, similarity + 0.1)
    
    return similarity


def find_matching_politician(filer_name: str, politicians: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Find best matching politician for a filer name"""
    # Import inside function to avoid serialization issues
    import logging
    local_logger = logging.getLogger()
    
    local_logger.info(f"      🔍 SEARCHING: Looking for filer '{filer_name}' in politician CSV list ({len(politicians)} politicians)")
    
    best_match = None
    best_score = 0.0
    checked_count = 0
    
    for politician in politicians:
        checked_count += 1
        score = fuzzy_match_name(filer_name, politician)
        if score > best_score:
            best_score = score
            best_match = politician
            if score >= NAME_MATCH_THRESHOLD:
                local_logger.info(f"      ✅ MATCH FOUND: Filer='{filer_name}' → Politician='{politician.get('name', 'N/A')}' "
                                f"(Score={score:.3f}, Threshold={NAME_MATCH_THRESHOLD})")
    
    if best_score >= NAME_MATCH_THRESHOLD:
        local_logger.info(f"      ✅ MATCH ACCEPTED: Filer='{filer_name}' → Politician='{best_match.get('name', 'N/A')}' "
                        f"(Score={best_score:.3f}, Checked={checked_count} politicians)")
        return {
            **best_match,
            'matchScore': best_score
        }
    
    local_logger.warning(f"      ❌ NO MATCH: Filer='{filer_name}' not found in politician CSV "
                        f"(BestScore={best_score:.3f} < Threshold={NAME_MATCH_THRESHOLD}, Checked={checked_count} politicians)")
    return None


def parse_sec_form_metadata(html_content: str, form_data: Dict[str, Any], accepted_date_str: Optional[str]) -> Dict[str, Any]:
    """
    Parse SEC Form HTML and extract all metadata fields (Forms 3, 4, 5)
    
    Returns:
        Dict with all extracted metadata fields
    """
    # Import inside function to avoid serialization issues
    import logging
    from html import unescape
    local_logger = logging.getLogger()
    
    result = {
        'formType': form_data.get('form_type', 'unknown'),
        'name': None,
        'address': None,
        'eventDate': None,
        'reportingDate': None,
        'issuerName': None,
        'tickerSymbol': None,
        'relationship': None,
        'relationshipAdditionalText': None,
        'signatureName': None,
        'amended': False,
        'amendment': False,
        'amendedTradeId': None,
        'nonDerivativeSecurities': [],
        'derivativeSecurities': [],
        'misc': {}
    }
    
    try:
        # Detect form type
        form_number = None
        form_name_match = re.search(r'class="FormName"[^>]*>FORM\s*(\d+)', html_content, re.IGNORECASE | re.DOTALL)
        if form_name_match:
            form_number = form_name_match.group(1)
        
        if not form_number:
            form_match = re.search(r'\bFORM\s+([345])\b', html_content, re.IGNORECASE)
            if form_match:
                form_number = form_match.group(1)
        
        is_form3 = form_number == '3'
        is_form4 = form_number == '4'
        is_form5 = form_number == '5'
        
        result['formType'] = f'form{form_number}' if form_number else 'form4'
        
        # Extract reporting person name (1. Name and Address of Reporting Person)
        name_match = re.search(r'<a[^>]*href="/cgi-bin/browse-edgar[^"]*CIK=\d+">([^<]+)</a>', html_content, re.IGNORECASE)
        if name_match:
            result['name'] = unescape(name_match.group(1)).strip()
        
        # Extract address (Street, City, State, Zip)
        address_parts = []
        
        # Street
        street_match = re.search(r'\(Street\)[^<]*<span[^>]*class="FormData"[^>]*>([^<]+)</span>', html_content, re.IGNORECASE | re.DOTALL)
        if street_match:
            street = unescape(street_match.group(1)).strip()
            if street:
                address_parts.append(street)
        
        # City, State, Zip
        city_state_zip_pattern = r'\(City\)[^<]*<span[^>]*class="FormData"[^>]*>([^<]+)</span>[^<]*<span[^>]*class="FormData"[^>]*>([^<]+)</span>[^<]*<span[^>]*class="FormData"[^>]*>([^<]+)</span>'
        csv_match = re.search(city_state_zip_pattern, html_content, re.IGNORECASE | re.DOTALL)
        if csv_match:
            city = unescape(csv_match.group(1)).strip()
            state = unescape(csv_match.group(2)).strip()
            zip_code = unescape(csv_match.group(3)).strip()
            csv_line = f"{city}, {state} {zip_code}".strip()
            if csv_line:
                address_parts.append(csv_line)
        
        result['address'] = ', '.join(address_parts) if address_parts else None
        
        # Extract event date
        # Forms 3/4: "Date of Event Requiring Statement"
        # Form 5: "Statement for Issuer's Fiscal Year Ended"
        if is_form3 or is_form4:
            event_date_match = re.search(r'Date of Event Requiring Statement[^<]*<span[^>]*class="FormData"[^>]*>(\d{1,2}/\d{1,2}/\d{4})</span>', html_content, re.IGNORECASE)
            if event_date_match:
                try:
                    date_str = event_date_match.group(1)
                    date_obj = datetime.strptime(date_str, '%m/%d/%Y')
                    result['eventDate'] = date_obj.strftime('%Y-%m-%d')
                except:
                    pass
        elif is_form5:
            fiscal_year_match = re.search(r'Statement for Issuer\'s Fiscal Year Ended[^<]*<span[^>]*class="FormData"[^>]*>(\d{1,2}/\d{1,2}/\d{4})</span>', html_content, re.IGNORECASE)
            if fiscal_year_match:
                try:
                    date_str = fiscal_year_match.group(1)
                    date_obj = datetime.strptime(date_str, '%m/%d/%Y')
                    result['eventDate'] = date_obj.strftime('%Y-%m-%d')
                except:
                    pass
        
        # Extract reporting date (accepted date from form_data or extract from HTML)
        if accepted_date_str:
            # Extract just the date part (YYYY-MM-DD) from timestamp
            date_part = accepted_date_str.split()[0] if ' ' in accepted_date_str else accepted_date_str
            result['reportingDate'] = date_part
        else:
            # Fallback: try to extract from HTML signature date
            signature_date_match = re.search(r'\*\* Signature[^<]*<span[^>]*class="FormData"[^>]*>(\d{1,2}/\d{1,2}/\d{4})</span>', html_content, re.IGNORECASE)
            if signature_date_match:
                try:
                    date_str = signature_date_match.group(1)
                    date_obj = datetime.strptime(date_str, '%m/%d/%Y')
                    result['reportingDate'] = date_obj.strftime('%Y-%m-%d')
                except:
                    pass
        
        # Extract issuer name and ticker symbol
        # Pattern: Issuer Name <a href="...">Name</a> [ <span>TICKER</span> ]
        issuer_match = re.search(r'Issuer Name[^<]*<a[^>]*>([^<]+)</a>', html_content, re.IGNORECASE)
        if issuer_match:
            result['issuerName'] = unescape(issuer_match.group(1)).strip()
        
        ticker_match = re.search(r'\[ <span[^>]*class="FormData"[^>]*>([A-Z0-9]+)</span> \]', html_content)
        if ticker_match:
            result['tickerSymbol'] = ticker_match.group(1)
        
        # Extract relationship (5. Relationship of Reporting Person(s) to Issuer)
        relationship_types = []
        relationship_additional = None
        
        # Check for Director
        director_match = re.search(r'Director[^<]*<td[^>]*align="center"[^>]*><span[^>]*class="FormData"[^>]*>X</span>', html_content, re.IGNORECASE | re.DOTALL)
        if director_match:
            relationship_types.append('Director')
        
        # Check for Officer
        officer_match = re.search(r'Officer[^<]*<td[^>]*align="center"[^>]*><span[^>]*class="FormData"[^>]*>X</span>', html_content, re.IGNORECASE | re.DOTALL)
        if officer_match:
            relationship_types.append('Officer')
            # Extract additional text (title below)
            officer_text_match = re.search(r'Officer[^<]*<td[^>]*style="color: blue"[^>]*>([^<]+)</td>', html_content, re.IGNORECASE | re.DOTALL)
            if officer_text_match:
                relationship_additional = unescape(officer_text_match.group(1)).strip()
        
        # Check for 10% Owner
        owner_match = re.search(r'10% Owner[^<]*<td[^>]*align="center"[^>]*><span[^>]*class="FormData"[^>]*>X</span>', html_content, re.IGNORECASE | re.DOTALL)
        if owner_match:
            relationship_types.append('10% Owner')
        
        # Check for Other
        other_match = re.search(r'Other[^<]*<td[^>]*align="center"[^>]*><span[^>]*class="FormData"[^>]*>X</span>', html_content, re.IGNORECASE | re.DOTALL)
        if other_match:
            relationship_types.append('Other')
            # Extract additional text
            other_text_match = re.search(r'Other[^<]*<td[^>]*style="color: blue"[^>]*>([^<]+)</td>', html_content, re.IGNORECASE | re.DOTALL)
            if other_text_match:
                other_text = unescape(other_text_match.group(1)).strip()
                if other_text:
                    relationship_additional = relationship_additional + '; ' + other_text if relationship_additional else other_text
        
        result['relationship'] = ', '.join(relationship_types) if relationship_types else None
        result['relationshipAdditionalText'] = relationship_additional if relationship_additional else None
        
        # Extract signature name
        signature_match = re.search(r'<u><span[^>]*class="FormData"[^>]*>(/s/|s/)?\s*([^<]+)</span></u>', html_content, re.IGNORECASE)
        if signature_match:
            signature_name = unescape(signature_match.group(2)).strip()
            # Remove /s/ or s/ prefix if present
            signature_name = re.sub(r'^[/]?s[/]\s*', '', signature_name, flags=re.IGNORECASE)
            result['signatureName'] = signature_name
        
        # Check for amendment
        # Forms 3/4/5: "4. If Amendment, Date of Original Filed"
        amendment_date_match = re.search(r'If Amendment, Date of Original Filed[^<]*<span[^>]*class="FormData"[^>]*>(\d{1,2}/\d{1,2}/\d{4})</span>', html_content, re.IGNORECASE)
        if amendment_date_match:
            result['amendment'] = True
        
        # Parse Table I - Non-Derivative Securities
        table1_data = parse_table_i(html_content, is_form3, is_form4, is_form5)
        result['nonDerivativeSecurities'] = table1_data
        
        # Parse Table II - Derivative Securities
        table2_data = parse_table_ii(html_content, is_form3, is_form4, is_form5)
        result['derivativeSecurities'] = table2_data
        
        # Parse explanations and remarks
        misc_data = parse_explanations_and_remarks(html_content)
        result['misc'] = misc_data
        
    except Exception as e:
        local_logger.error(f"❌ Error parsing form metadata: {e}")
        import traceback
        local_logger.error(f"   Traceback: {traceback.format_exc()}")
    
    return result


def parse_table_i(html_content: str, is_form3: bool, is_form4: bool, is_form5: bool) -> List[Dict[str, Any]]:
    """Parse Table I - Non-Derivative Securities"""
    # Import inside function to avoid serialization issues
    from html import unescape
    
    table_data = []
    
    try:
        # Find Table I tbody
        table1_pattern = r'Table I[^<]*<tbody>(.*?)</tbody>'
        table1_match = re.search(table1_pattern, html_content, re.IGNORECASE | re.DOTALL)
        
        if not table1_match:
            return table_data
        
        tbody_content = table1_match.group(1)
        rows = re.findall(r'<tr[^>]*>(.*?)</tr>', tbody_content, re.DOTALL | re.IGNORECASE)
        
        def clean_cell(cell):
            text = re.sub(r'<[^>]+>', '', cell)
            text = unescape(text)
            return text.strip()
        
        for row in rows:
            cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL | re.IGNORECASE)
            
            if is_form3 and len(cells) >= 4:
                # Form 3: Title | Amount | Ownership Form | Nature of Indirect
                row_data = {
                    'titleOfSecurity': clean_cell(cells[0]) if len(cells) > 0 else '',
                    'amountOfSecurities': clean_cell(cells[1]) if len(cells) > 1 else '',
                    'ownershipForm': clean_cell(cells[2]) if len(cells) > 2 else '',
                    'natureOfIndirectBeneficialOwnership': clean_cell(cells[3]) if len(cells) > 3 else ''
                }
                table_data.append(row_data)
            elif (is_form4 or is_form5) and len(cells) >= 8:
                # Form 4/5: Title | Transaction Date | ... | Amount | (A) or (D) | Price | ...
                row_data = {
                    'titleOfSecurity': clean_cell(cells[0]) if len(cells) > 0 else '',
                    'transactionDate': clean_cell(cells[1]) if len(cells) > 1 else '',
                    'deemedExecutionDate': clean_cell(cells[2]) if len(cells) > 2 else '',
                    'transactionCode': clean_cell(cells[3]) if len(cells) > 3 else '',
                    'transactionCodeV': clean_cell(cells[4]) if len(cells) > 4 else '',
                    'amount': clean_cell(cells[5]) if len(cells) > 5 else '',
                    'acquiredOrDisposed': clean_cell(cells[6]) if len(cells) > 6 else '',
                    'price': clean_cell(cells[7]) if len(cells) > 7 else ''
                }
                # Add remaining columns if present
                if len(cells) > 8:
                    row_data['amountOfSecuritiesBeneficiallyOwned'] = clean_cell(cells[8])
                if len(cells) > 9:
                    row_data['ownershipForm'] = clean_cell(cells[9])
                if len(cells) > 10:
                    row_data['natureOfIndirectBeneficialOwnership'] = clean_cell(cells[10])
                table_data.append(row_data)
    
    except Exception as e:
        import logging
        local_logger = logging.getLogger()
        local_logger.error(f"❌ Error parsing Table I: {e}")
    
    return table_data


def parse_table_ii(html_content: str, is_form3: bool, is_form4: bool, is_form5: bool) -> List[Dict[str, Any]]:
    """Parse Table II - Derivative Securities"""
    # Import inside function to avoid serialization issues
    from html import unescape
    
    table_data = []
    
    try:
        # Find Table II tbody
        table2_pattern = r'Table II[^<]*<tbody>(.*?)</tbody>'
        table2_match = re.search(table2_pattern, html_content, re.IGNORECASE | re.DOTALL)
        
        if not table2_match:
            return table_data
        
        tbody_content = table2_match.group(1)
        rows = re.findall(r'<tr[^>]*>(.*?)</tr>', tbody_content, re.DOTALL | re.IGNORECASE)
        
        def clean_cell(cell):
            text = re.sub(r'<[^>]+>', '', cell)
            text = unescape(text)
            return text.strip()
        
        for row in rows:
            cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL | re.IGNORECASE)
            
            if is_form3 and len(cells) >= 6:
                # Form 3: Title | Date Exercisable | Expiration Date | Title | Amount | Conversion Price | Ownership | Nature
                row_data = {
                    'titleOfDerivativeSecurity': clean_cell(cells[0]) if len(cells) > 0 else '',
                    'dateExercisable': clean_cell(cells[1]) if len(cells) > 1 else '',
                    'expirationDate': clean_cell(cells[2]) if len(cells) > 2 else '',
                    'titleOfUnderlyingSecurity': clean_cell(cells[3]) if len(cells) > 3 else '',
                    'amountOrNumberOfShares': clean_cell(cells[4]) if len(cells) > 4 else '',
                    'conversionOrExercisePrice': clean_cell(cells[5]) if len(cells) > 5 else '',
                    'ownershipForm': clean_cell(cells[6]) if len(cells) > 6 else '',
                    'natureOfIndirectBeneficialOwnership': clean_cell(cells[7]) if len(cells) > 7 else ''
                }
                table_data.append(row_data)
            elif (is_form4 or is_form5) and len(cells) >= 10:
                # Form 4/5: Title | Conversion Price | Transaction Date | ... | (A) | (D) | Date Exercisable | Expiration | Title | Amount | Price | ...
                row_data = {
                    'titleOfDerivativeSecurity': clean_cell(cells[0]) if len(cells) > 0 else '',
                    'conversionOrExercisePrice': clean_cell(cells[1]) if len(cells) > 1 else '',
                    'transactionDate': clean_cell(cells[2]) if len(cells) > 2 else '',
                    'deemedExecutionDate': clean_cell(cells[3]) if len(cells) > 3 else '',
                    'transactionCode': clean_cell(cells[4]) if len(cells) > 4 else '',
                    'transactionCodeV': clean_cell(cells[5]) if len(cells) > 5 else '',
                    'acquired': clean_cell(cells[6]) if len(cells) > 6 else '',
                    'disposed': clean_cell(cells[7]) if len(cells) > 7 else '',
                    'dateExercisable': clean_cell(cells[8]) if len(cells) > 8 else '',
                    'expirationDate': clean_cell(cells[9]) if len(cells) > 9 else '',
                    'titleOfUnderlyingSecurity': clean_cell(cells[10]) if len(cells) > 10 else '',
                    'amountOrNumberOfShares': clean_cell(cells[11]) if len(cells) > 11 else '',
                    'priceOfDerivativeSecurity': clean_cell(cells[12]) if len(cells) > 12 else ''
                }
                # Add remaining columns if present
                if len(cells) > 13:
                    row_data['numberOfDerivativeSecuritiesBeneficiallyOwned'] = clean_cell(cells[13])
                if len(cells) > 14:
                    row_data['ownershipForm'] = clean_cell(cells[14])
                if len(cells) > 15:
                    row_data['natureOfIndirectBeneficialOwnership'] = clean_cell(cells[15])
                table_data.append(row_data)
    
    except Exception as e:
        import logging
        local_logger = logging.getLogger()
        local_logger.error(f"❌ Error parsing Table II: {e}")
    
    return table_data


def parse_explanations_and_remarks(html_content: str) -> Dict[str, Any]:
    """Parse explanations and remarks into JSON object"""
    # Import inside function to avoid serialization issues
    from html import unescape
    
    misc = {}
    
    try:
        # Find "Explanation of Responses" section
        explanation_section = re.search(r'Explanation of Responses[^<]*</td>[^<]*</tr>(.*?)(?=<table|<tr><td[^>]*><b>Remarks)', html_content, re.IGNORECASE | re.DOTALL)
        
        if explanation_section:
            explanation_text = explanation_section.group(1)
            
            # Extract numbered explanations (e.g., "1. ...", "2. ...")
            # Pattern: number followed by period and space, then text until next number or end
            explanation_pattern = r'(\d+)\.\s+([^<\d]+?)(?=\d+\.|$)'
            explanations = re.findall(explanation_pattern, explanation_text, re.DOTALL)
            
            for num, text in explanations:
                cleaned_text = re.sub(r'<[^>]+>', '', text)
                cleaned_text = unescape(cleaned_text).strip()
                if cleaned_text:
                    misc[num] = cleaned_text
        
        # Find "Remarks" section
        remarks_match = re.search(r'<b>Remarks:</b>[^<]*</td>[^<]*</tr>[^<]*<tr><td[^>]*class="[^"]*FootnoteData[^"]*"[^>]*>([^<]+)</td>', html_content, re.IGNORECASE | re.DOTALL)
        if remarks_match:
            remarks_text = unescape(remarks_match.group(1)).strip()
            if remarks_text:
                misc['remarks'] = remarks_text
    
    except Exception as e:
        import logging
        local_logger = logging.getLogger()
        local_logger.error(f"❌ Error parsing explanations/remarks: {e}")
    
    return misc


def process_form(form_data: Dict[str, Any], target_date: str, politicians: List[Dict[str, Any]], s3_bucket_name: str, dynamodb_table_name: str) -> Dict[str, Any]:
    """
    Process a single SEC form: download, parse, check politician match, store
    
    Args:
        form_data: Form metadata
        target_date: Target date
        politicians: List of politicians for matching
        s3_bucket_name: S3 bucket name
        dynamodb_table_name: DynamoDB table name
    
    Returns:
        Dict with processing result
    """
    # Import inside function to avoid serialization issues
    import logging
    local_logger = logging.getLogger()
    
    cik = form_data.get('cik', 'unknown')
    accession = form_data.get('accession_number', 'unknown')
    form_type = form_data.get('form_type', 'unknown')
    filing_date_str = form_data.get('filing_date', target_date)
    accepted_date_str = form_data.get('accepted_date')
    
    form_start_time = datetime.now()
    local_logger.info(f"📄 Processing Form: CIK={cik}, Accession={accession}, Type={form_type}, FilingDate={filing_date_str}")
    
    # First check: Verify filing date matches target date before downloading
    local_logger.info(f"   🔍 Step 1/4: Validating filing date...")
    filing_date_obj = None
    if filing_date_str:
        try:
            # Try YYYY-MM-DD format first
            try:
                filing_date_obj = datetime.strptime(filing_date_str, '%Y-%m-%d').date()
            except ValueError:
                # Fallback to MM/DD/YYYY
                filing_date_obj = datetime.strptime(filing_date_str, '%m/%d/%Y').date()
        except:
            pass
    
    target_date_obj = datetime.strptime(target_date, '%Y-%m-%d').date()
    
    if filing_date_obj and filing_date_obj != target_date_obj:
        local_logger.info(f"   ⏭️ SKIPPING: Filing date {filing_date_obj} doesn't match target {target_date_obj}")
        return {'skipped': True, 'reason': 'date_mismatch'}
    
    local_logger.info(f"   ✅ Date validation passed: {filing_date_obj or 'unknown'} matches target {target_date_obj}")
    
    # Download form
    download_start = datetime.now()
    local_logger.info(f"   📥 Step 2/4: Downloading form...")
    downloaded = download_sec_form(form_data, target_date, s3_bucket_name)
    download_duration = (datetime.now() - download_start).total_seconds()
    
    if not downloaded:
        local_logger.warning(f"   ❌ FAILED: Could not download form (CIK={cik}, Accession={accession}) after {download_duration:.2f}s")
        return {'skipped': True, 'reason': 'download_failed'}
    
    s3_key = downloaded.get('s3_key', 'unknown')
    file_ext = downloaded.get('file_ext', 'unknown')
    file_size = len(downloaded.get('content', b''))
    local_logger.info(f"   ✅ Downloaded: {s3_key} (ext: {file_ext}, size: {file_size:,} bytes) in {download_duration:.2f}s")
    
    # Parse form metadata
    parse_start = datetime.now()
    local_logger.info(f"   📊 Step 3/4: Parsing form metadata from S3Key={s3_key}...")
    if downloaded['file_ext'] != 'html':
        local_logger.warning(f"   ⚠️ Unsupported file type: {downloaded['file_ext']} (CIK={cik}, Accession={accession})")
        return {'skipped': True, 'reason': 'unsupported_file_type'}
    
    content_str = downloaded['content'].decode('utf-8', errors='ignore')
    parsed_data = parse_sec_form_metadata(content_str, form_data, accepted_date_str)
    parse_duration = (datetime.now() - parse_start).total_seconds()
    
    # Log parsed data summary
    local_logger.info(f"   ✅ Parsing complete in {parse_duration:.2f}s:")
    local_logger.info(f"      - Name: {parsed_data.get('name', 'N/A')}")
    local_logger.info(f"      - Issuer: {parsed_data.get('issuerName', 'N/A')} ({parsed_data.get('tickerSymbol', 'N/A')})")
    local_logger.info(f"      - Relationship: {parsed_data.get('relationship', 'N/A')}")
    local_logger.info(f"      - Event Date: {parsed_data.get('eventDate', 'N/A')}")
    local_logger.info(f"      - Reporting Date: {parsed_data.get('reportingDate', 'N/A')}")
    local_logger.info(f"      - Table I rows: {len(parsed_data.get('nonDerivativeSecurities', []))}")
    local_logger.info(f"      - Table II rows: {len(parsed_data.get('derivativeSecurities', []))}")
    local_logger.info(f"      - Amendment: {parsed_data.get('amendment', False)}")
    
    # Check politician match
    match_start = datetime.now()
    local_logger.info(f"   🔍 Step 4/4: Checking politician match for name='{parsed_data.get('name', 'N/A')}'...")
    politician_match = None
    if parsed_data.get('name'):
        politician_match = find_matching_politician(parsed_data['name'], politicians)
        if politician_match:
            local_logger.info(f"   ✅ POLITICIAN MATCH: Name='{parsed_data['name']}' → Politician='{politician_match.get('name', 'N/A')}' (Score={politician_match.get('matchScore', 0):.3f})")
            parsed_data['politician'] = 1  # True (DynamoDB doesn't support boolean, use 1/0)
        else:
            local_logger.info(f"   ℹ️ NO POLITICIAN MATCH: Name='{parsed_data['name']}' not in politician list")
            parsed_data['politician'] = 0  # False
    else:
        local_logger.warning(f"   ⚠️ No name extracted from form, cannot check politician match")
        parsed_data['politician'] = 0  # False
    
    match_duration = (datetime.now() - match_start).total_seconds()
    local_logger.info(f"   ✅ Matching complete in {match_duration:.2f}s")
    
    # Generate trade ID
    trade_id = f"sec_{form_data.get('form_type', 'form4')}_{cik}_{accession}_{target_date.replace('-', '')}"
    parsed_data['tradeId'] = trade_id
    parsed_data['formS3Key'] = s3_key
    
    # Handle amendment logic (will be implemented later when we can query existing records)
    # For now, just mark if it's an amendment
    # TODO: Query DynamoDB to find original trade and link them
    
    # Store to DynamoDB
    store_start = datetime.now()
    local_logger.info(f"   💾 Storing to DynamoDB (TradeId={trade_id})...")
    try:
        # Create DynamoDB client locally
        dynamodb_local = boto3.resource('dynamodb')
        table_local = dynamodb_local.Table(dynamodb_table_name)
        
        # Convert to DynamoDB format
        dynamodb_item = {}
        for key, value in parsed_data.items():
            if value is None or value == '':
                continue
            elif isinstance(value, (int, float)):
                if isinstance(value, float) and (value != value or value == float('inf') or value == float('-inf')):
                    continue
                dynamodb_item[key] = Decimal(str(value))
            elif isinstance(value, list):
                # Lists of dicts (JSON arrays) - store as JSON string
                dynamodb_item[key] = json.dumps(value)
            elif isinstance(value, dict):
                # Dicts (JSON objects) - store as JSON string
                dynamodb_item[key] = json.dumps(value)
            else:
                dynamodb_item[key] = str(value)
        
        table_local.put_item(Item=dynamodb_item)
        store_duration = (datetime.now() - store_start).total_seconds()
        total_duration = (datetime.now() - form_start_time).total_seconds()
        
        local_logger.info(f"   ✅ STORED: TradeId={trade_id}, S3Key={s3_key} in {store_duration:.2f}s")
        local_logger.info(f"   ✅ Form processing complete: Total time {total_duration:.2f}s (Download: {download_duration:.2f}s, Parse: {parse_duration:.2f}s, Match: {match_duration:.2f}s, Store: {store_duration:.2f}s)")
        
        return {'success': True, 'tradeId': trade_id, 'politicianMatch': politician_match is not None}
    
    except Exception as e:
        store_duration = (datetime.now() - store_start).total_seconds()
        total_duration = (datetime.now() - form_start_time).total_seconds()
        local_logger.error(f"   ❌ STORAGE ERROR: Failed to store to DynamoDB after {store_duration:.2f}s (Total: {total_duration:.2f}s): {e}")
        import traceback
        local_logger.error(f"      Traceback: {traceback.format_exc()}")
        return {'success': False, 'error': str(e)}


def write_to_dynamodb(trades: List[Dict[str, Any]], dynamodb_table_name: str):
    """Batch write trades to DynamoDB"""
    if not trades:
        return
    
    # Create DynamoDB resource and table locally to avoid Spark serialization issues
    dynamodb_local = boto3.resource('dynamodb')
    table_local = dynamodb_local.Table(dynamodb_table_name)
    
    # Convert to DynamoDB format
    with table_local.batch_writer() as batch:
        for trade in trades:
            # Convert numeric fields to Decimal
            dynamodb_item = {}
            for key, value in trade.items():
                if value is None:
                    continue
                elif isinstance(value, (int, float)):
                    if isinstance(value, float) and (value != value or value == float('inf') or value == float('-inf')):
                        continue
                    dynamodb_item[key] = Decimal(str(value))
                elif isinstance(value, list):
                    dynamodb_item[key] = [Decimal(str(v)) if isinstance(v, (int, float)) else v for v in value]
                else:
                    dynamodb_item[key] = value
            
            batch.put_item(Item=dynamodb_item)
    
    logger.info(f"✅ Wrote {len(trades)} trades to DynamoDB")


# Main execution
try:
    logger.info("=" * 80)
    logger.info("🚀 SEC ETL GLUE JOB STARTING")
    logger.info("=" * 80)
    logger.info(f"📅 Target Date: {target_date}")
    logger.info(f"📦 S3 Bucket: {s3_bucket}")
    logger.info(f"🗄️  DynamoDB Table: {dynamodb_table}")
    logger.info(f"⏰ Job Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 80)
    
    # Step 1: Load politician list
    logger.info("")
    logger.info("=" * 80)
    logger.info("📋 STAGE 1: LOADING POLITICIAN LIST")
    logger.info("=" * 80)
    stage1_start = datetime.now()
    politicians = load_politician_list()
    stage1_duration = (datetime.now() - stage1_start).total_seconds()
    logger.info(f"✅ Stage 1 Complete: Loaded {len(politicians)} politicians in {stage1_duration:.2f} seconds")
    logger.info("=" * 80)
    
    # Step 2: Fetch SEC forms with pagination
    logger.info("")
    logger.info("=" * 80)
    logger.info("📋 STAGE 2: FETCHING SEC FORMS")
    logger.info("=" * 80)
    logger.info(f"🔍 Searching for Forms 3, 4, 5 filed on {target_date}")
    stage2_start = datetime.now()
    forms = fetch_sec_forms_paginated(target_date)
    stage2_duration = (datetime.now() - stage2_start).total_seconds()
    logger.info("")
    logger.info(f"✅ Stage 2 Complete: Fetched {len(forms)} forms in {stage2_duration:.2f} seconds")
    
    # Log form type breakdown
    form_type_counts = {}
    for form in forms:
        form_type = form.get('form_type', 'unknown')
        form_type_counts[form_type] = form_type_counts.get(form_type, 0) + 1
    logger.info(f"   Form Type Breakdown:")
    for form_type, count in sorted(form_type_counts.items()):
        logger.info(f"      - {form_type}: {count}")
    logger.info("=" * 80)
    
    # Step 3: Process forms in parallel using Spark
    logger.info("")
    logger.info("=" * 80)
    logger.info("📊 STAGE 3: PROCESSING FORMS")
    logger.info("=" * 80)
    logger.info(f"🔄 Processing {len(forms)} forms in parallel using Spark")
    logger.info(f"   This stage will: download, parse metadata, check politician match, and store to DynamoDB")
    stage3_start = datetime.now()
    
    # Broadcast necessary variables to avoid serialization issues
    # Broadcast variables are sent once to each worker, not serialized with each task
    politicians_broadcast = sc.broadcast(politicians)
    target_date_broadcast = sc.broadcast(target_date)
    s3_bucket_broadcast = sc.broadcast(s3_bucket)
    dynamodb_table_broadcast = sc.broadcast(dynamodb_table)
    
    # Create RDD from forms list
    forms_rdd = sc.parallelize(forms)
    
    # Track processing statistics
    processing_stats = {
        'total_forms': len(forms),
        'successful_stored': 0,
        'failed_stored': 0,
        'skipped_date_mismatch': 0,
        'skipped_download_failed': 0,
        'skipped_unsupported_type': 0,
        'politician_matches': 0,
        'no_politician_matches': 0
    }
    
    # Process each form (download, parse, check match, store)
    # Use a standalone function that doesn't capture module-level variables
    def process_form_wrapper(form_data):
        # Import inside function to avoid capturing module-level state
        import logging
        import traceback
        
        # Get logger locally - don't use module-level logger
        local_logger = logging.getLogger()
        local_logger.setLevel(logging.INFO)
        
        try:
            # Get broadcasted values
            politicians_local = politicians_broadcast.value
            target_date_local = target_date_broadcast.value
            s3_bucket_local = s3_bucket_broadcast.value
            dynamodb_table_local = dynamodb_table_broadcast.value
            
            cik = form_data.get('cik', 'unknown')
            accession = form_data.get('accession_number', 'unknown')
            
            local_logger.info(f"🔄 Starting processing: Form {form_data.get('form_type', 'unknown')} - CIK={cik}, Accession={accession}")
            
            # Call process_form with explicit parameters from broadcast
            # Note: process_form will create its own boto3 clients inside, so no SSLContext issues
            # process_form now stores directly to DynamoDB and returns a result dict
            result = process_form(form_data, target_date_local, politicians_local, s3_bucket_local, dynamodb_table_local)
            
            if result.get('success'):
                local_logger.info(f"✅ Form completed: CIK={cik}, TradeId={result.get('tradeId', 'N/A')}, PoliticianMatch={result.get('politicianMatch', False)}")
            elif result.get('skipped'):
                local_logger.info(f"⏭️ Form skipped: CIK={cik}, Reason={result.get('reason', 'unknown')}")
            else:
                local_logger.warning(f"⚠️ Form failed: CIK={cik}, Error={result.get('error', 'unknown')}")
            
            return result
        except Exception as e:
            cik = form_data.get('cik', 'unknown')
            accession = form_data.get('accession_number', 'unknown')
            local_logger.error(f"❌ FATAL ERROR processing form (CIK={cik}, Accession={accession}): {e}")
            local_logger.error(f"   Traceback: {traceback.format_exc()}")
            return {'success': False, 'error': str(e)}
    
    logger.info(f"   Starting Spark parallel processing...")
    results_rdd = forms_rdd.map(process_form_wrapper)
    results = results_rdd.collect()
    logger.info(f"   ✅ Spark processing completed, collecting results...")
    
    # Clean up broadcast variables
    politicians_broadcast.destroy()
    target_date_broadcast.destroy()
    s3_bucket_broadcast.destroy()
    dynamodb_table_broadcast.destroy()
    
    stage3_duration = (datetime.now() - stage3_start).total_seconds()
    
    # Calculate final statistics
    total_forms_processed = len(forms)
    successful_stored = sum(1 for r in results if r.get('success'))
    failed_stored = sum(1 for r in results if not r.get('success') and not r.get('skipped'))
    skipped_date_mismatch = sum(1 for r in results if r.get('skipped') and r.get('reason') == 'date_mismatch')
    skipped_download_failed = sum(1 for r in results if r.get('skipped') and r.get('reason') == 'download_failed')
    skipped_unsupported_type = sum(1 for r in results if r.get('skipped') and r.get('reason') == 'unsupported_file_type')
    politician_matches = sum(1 for r in results if r.get('politicianMatch'))
    no_politician_matches = successful_stored - politician_matches
    
    logger.info("")
    logger.info(f"✅ Stage 3 Complete: Processed {total_forms_processed} forms in {stage3_duration:.2f} seconds")
    logger.info(f"   Average processing time per form: {stage3_duration / total_forms_processed if total_forms_processed > 0 else 0:.2f} seconds")
    logger.info("=" * 80)
    
    # Step 4: Final Summary
    logger.info("")
    logger.info("=" * 80)
    logger.info("📊 STAGE 4: FINAL SUMMARY")
    logger.info("=" * 80)
    logger.info(f"📈 Processing Statistics:")
    logger.info(f"   - Total forms fetched: {total_forms_processed}")
    logger.info(f"   - Successfully stored: {successful_stored} ({successful_stored/total_forms_processed*100 if total_forms_processed > 0 else 0:.1f}%)")
    logger.info(f"   - Failed to store: {failed_stored} ({failed_stored/total_forms_processed*100 if total_forms_processed > 0 else 0:.1f}%)")
    logger.info(f"   - Skipped (date mismatch): {skipped_date_mismatch} ({skipped_date_mismatch/total_forms_processed*100 if total_forms_processed > 0 else 0:.1f}%)")
    logger.info(f"   - Skipped (download failed): {skipped_download_failed} ({skipped_download_failed/total_forms_processed*100 if total_forms_processed > 0 else 0:.1f}%)")
    logger.info(f"   - Skipped (unsupported file type): {skipped_unsupported_type} ({skipped_unsupported_type/total_forms_processed*100 if total_forms_processed > 0 else 0:.1f}%)")
    logger.info("")
    logger.info(f"👤 Politician Matching Statistics:")
    logger.info(f"   - Forms with politician match: {politician_matches} ({politician_matches/successful_stored*100 if successful_stored > 0 else 0:.1f}% of stored)")
    logger.info(f"   - Forms without politician match: {no_politician_matches} ({no_politician_matches/successful_stored*100 if successful_stored > 0 else 0:.1f}% of stored)")
    logger.info("")
    
    if successful_stored == 0:
        logger.warning(f"   ⚠️ WARNING: No forms were successfully stored!")
        logger.warning(f"      This could indicate:")
        logger.warning(f"      - Download failures for all forms")
        logger.warning(f"      - Parsing failures for all forms")
        logger.warning(f"      - Date mismatches for all forms")
    elif successful_stored < total_forms_processed * 0.5:
        logger.warning(f"   ⚠️ WARNING: Less than 50% of forms were successfully stored!")
        logger.warning(f"      Success rate: {successful_stored/total_forms_processed*100:.1f}%")
    
    logger.info("=" * 80)
    logger.info("")
    logger.info("=" * 80)
    logger.info("✅ SEC ETL JOB COMPLETED SUCCESSFULLY")
    logger.info("=" * 80)
    total_duration = (datetime.now() - stage1_start).total_seconds()
    logger.info(f"⏰ Total Job Duration: {total_duration:.2f} seconds ({total_duration/60:.2f} minutes)")
    logger.info(f"⏰ Job End Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 80)
    
    job.commit()
    
except Exception as e:
    logger.error(f"❌ Fatal error in SEC ETL job: {e}")
    import traceback
    logger.error(f"Traceback: {traceback.format_exc()}")
    raise

