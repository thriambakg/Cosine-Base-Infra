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
import builtins  # Import builtins to access Python's built-in sum() function

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
# Glue jobs benefit from both logger and print() for visibility
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Also use print() for critical messages - Glue shows these more reliably
def log_print(message):
    """Print to both logger and stdout for maximum visibility in Glue"""
    logger.info(message)
    print(message, file=sys.stdout, flush=True)

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
    'date',      # Target date in YYYY-MM-DD format (for normal daily runs) - may be null
    's3_bucket',
    'dynamodb_table'
])

job.init(args['JOB_NAME'], args)

# Extract required parameters
target_date = args.get('date')
s3_bucket = args.get('s3_bucket')
dynamodb_table = args.get('dynamodb_table')

# Parse optional backdate argument manually (since getResolvedOptions requires all args)
# Format: --backdate=2025-11-05 or --backdate 2025-11-05
# Step Functions may pass null as --backdate=null or --backdate null
backdate = None
for i, arg in enumerate(sys.argv):
    if arg == '--backdate' or arg.startswith('--backdate='):
        if '=' in arg:
            backdate_value = arg.split('=', 1)[1]
        elif i + 1 < len(sys.argv):
            backdate_value = sys.argv[i + 1]
        else:
            continue
        
        # Filter out null, empty string, or string "null"
        if backdate_value and backdate_value.strip() != '' and backdate_value.lower() != 'null':
            backdate = backdate_value
        break

# Determine mode: backdate mode or normal date mode
# If backdate is provided, use it; otherwise use date (or default to yesterday)
if backdate and backdate.strip() != '' and backdate.lower() != 'null':
    # Backdate mode: fetch all records until first date in batch is less than backdate
    target_date = backdate
    is_backdate_mode = True
    logger.info(f"📅 BACKDATE MODE: Processing SEC forms backdating to: {backdate}")
    print(f"📅 BACKDATE MODE: Processing SEC forms backdating to: {backdate}", flush=True)
elif target_date and target_date.strip() != '' and target_date.lower() != 'null':
    # Normal date mode: process specific date
    is_backdate_mode = False
    logger.info(f"📅 Processing SEC forms for date: {target_date}")
    print(f"📅 Processing SEC forms for date: {target_date}", flush=True)
else:
    # Default to yesterday if neither provided
    yesterday = datetime.now() - timedelta(days=1)
    target_date = yesterday.strftime('%Y-%m-%d')
    is_backdate_mode = False
    logger.info(f"⚠️ No date/backdate provided (or was null/empty), defaulting to yesterday: {target_date}")
    print(f"⚠️ No date/backdate provided (or was null/empty), defaulting to yesterday: {target_date}", flush=True)

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


def fetch_sec_forms_paginated(target_date: str, form_types: List[str] = ['3', '4', '5'], is_backdate_mode: bool = False) -> List[Dict[str, Any]]:
    """
    Fetch SEC forms using paginated browse-edgar API
    
    Uses the browse-edgar endpoint with start parameter for pagination:
    https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=4&owner=only&start=0&count=100
    
    Args:
        target_date: Target date in YYYY-MM-DD format (for normal mode) or backdate (for backdate mode)
        form_types: List of form types to fetch (default: ['3', '4', '5'])
        is_backdate_mode: If True, fetch all records until first date in batch is less than target_date.
                         If False, only fetch records matching target_date exactly.
    
    Returns:
        List of form metadata dicts with keys: form_type, cik, accession_number, filename, filing_date
    """
    all_forms = []
    target_date_obj = datetime.strptime(target_date, '%Y-%m-%d').date()
    
    session = requests.Session()
    # Use the same headers as the working test_pagination.py script
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
                
                logger.info(f"   📡 Calling SEC browse-edgar API:")
                logger.info(f"      URL: {url}")
                logger.info(f"      Method: GET")
                logger.info(f"      Parameters: start={start}, count={count}, type={form_type}")
                user_agent_preview = session.headers.get('User-Agent', 'N/A')[:50]
                logger.info(f"      Headers: User-Agent={user_agent_preview}...")
                
                api_call_start = datetime.now()
                response = session.get(url, timeout=30)
                api_call_duration = (datetime.now() - api_call_start).total_seconds()
                
                logger.info(f"   📥 SEC API Response:")
                logger.info(f"      Status Code: {response.status_code}")
                logger.info(f"      Response Headers: {dict(response.headers)}")
                logger.info(f"      Response Size: {len(response.content):,} bytes")
                logger.info(f"      Response Time: {api_call_duration:.2f}s")
                logger.info(f"      Response Preview (first 500 chars): {response.text[:500]}")
                
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
                log_print(f"      📊 HTML Parsing Results:")
                log_print(f"         Total table rows found: {len(rows)}")
                log_print(f"         HTML size: {len(html_content):,} characters")
                if rows:
                    log_print(f"         First row preview (first 200 chars): {rows[0][:200]}")
                
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
                        logger.debug(f"         Row {page_forms_before_date_filter + 1}: No archive link found, skipping")
                        continue
                    
                    logger.debug(f"         Row {page_forms_before_date_filter + 1}: Found archive link, extracting data...")
                    
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
                            
                            # Filter based on mode
                            if is_backdate_mode:
                                # Backdate mode: include all forms with filing date >= backdate
                                if filing_date_obj < target_date_obj:
                                    continue  # Skip forms before backdate
                            else:
                                # Normal mode: only include if matches target date exactly
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
                
                # Log sample of parsed forms from this page
                if page_forms:
                    log_print(f"      📋 Sample forms from page {page} (first 3):")
                    for idx, form in enumerate(page_forms[:3], 1):
                        log_print(f"         {idx}. CIK={form.get('cik', 'N/A')}, "
                                   f"Accession={form.get('accession_number', 'N/A')[:15]}..., "
                                   f"Type={form.get('form_type', 'N/A')}, "
                                   f"FilingDate={form.get('filing_date', 'N/A')}, "
                                   f"AcceptedDate={form.get('accepted_date', 'N/A') or 'N/A'}")
                
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
                
                # Stop condition depends on mode
                if is_backdate_mode:
                    # Backdate mode: stop when first file's date is less than backdate (we've gone too far back)
                    if first_file_date and first_file_date < target_date_obj:
                        logger.info(f"   First file in batch has date {first_file_date} (backdate: {target_date_obj}), "
                                  f"stopping pagination - reached date before backdate")
                        print(f"   First file in batch has date {first_file_date} (backdate: {target_date_obj}), "
                              f"stopping pagination - reached date before backdate", flush=True)
                        break
                else:
                    # Normal mode: stop when first file's date doesn't match target date
                    if first_file_date and first_file_date != target_date_obj:
                        logger.info(f"   First file in batch has date {first_file_date} (target: {target_date_obj}), "
                                  f"stopping pagination - reached different day")
                        print(f"   First file in batch has date {first_file_date} (target: {target_date_obj}), "
                              f"stopping pagination - reached different day", flush=True)
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
    import sys
    local_logger = logging.getLogger()
    
    # Force immediate logging with print statements
    print("="*80, flush=True)
    print("🔵 download_sec_form CALLED", flush=True)
    print(f"   form_data keys: {list(form_data.keys()) if form_data else 'None'}", flush=True)
    print(f"   target_date: {target_date}", flush=True)
    print(f"   s3_bucket_name: {s3_bucket_name}", flush=True)
    
    try:
        # Support both field name variations (from fetcher: accession_number, from Lambda: accessionNumber)
        cik = form_data.get('cik', 'unknown')
        accession = form_data.get('accession_number') or form_data.get('accessionNumber') or form_data.get('accession', 'unknown')
        form_type = form_data.get('form_type') or form_data.get('formType', 'unknown')
        filing_date = form_data.get('filing_date') or form_data.get('filingDate', 'unknown')
        
        print(f"   Extracted: CIK={cik}, Accession={accession}, Type={form_type}, FilingDate={filing_date}", flush=True)
        print(f"   Full form_data: {json.dumps(form_data, default=str)}", flush=True)
        
        local_logger.info(f"")
        local_logger.info(f"      " + "="*70)
        local_logger.info(f"      📥 DOWNLOAD START: CIK={cik}, Accession={accession}, Type={form_type}")
        local_logger.info(f"      📅 Filing Date: {filing_date}")
        local_logger.info(f"      📋 Full form_data: {json.dumps(form_data, default=str)}")
        print(f"", flush=True)
        print(f"      " + "="*70, flush=True)
        print(f"      📥 DOWNLOAD START: CIK={cik}, Accession={accession}, Type={form_type}", flush=True)
        print(f"      📅 Filing Date: {filing_date}", flush=True)
        
        if not all([cik, accession]) or cik == 'unknown' or accession == 'unknown':
            error_msg = f"   ⚠️ Missing CIK/accession: CIK={cik}, Accession={accession}"
            local_logger.warning(error_msg)
            print(error_msg, flush=True)
            print(f"   ⚠️ Form data keys: {list(form_data.keys())}", flush=True)
            print(f"   ⚠️ Form data values: {form_data}", flush=True)
            return None
        
        # Construct accession number with dashes (format: 0001234567-12-345678)
        # IMPORTANT: Based on working Lambda logs, SEC uses accession WITH dashes in the directory path
        # The index file is at: {accession-with-dashes}/{accession-with-dashes}-index.htm
        # Example: /Archives/edgar/data/1509282/0001509282-25-000007/0001509282-25-000007-index.htm
        
        # Remove any dashes first to get clean number
        accession_clean = accession.replace('-', '').strip()
        
        # For URL path: use accession WITH dashes (matching working Lambda behavior)
        if len(accession_clean) == 18:
            accession_dashed = f"{accession_clean[:10]}-{accession_clean[10:12]}-{accession_clean[12:]}"
        else:
            accession_dashed = accession_clean
        
        # Use constants directly (strings are safe to serialize)
        SEC_BASE_URL_LOCAL = "https://www.sec.gov"
        
        # Build base URL - use accession WITH dashes in path (matching working Lambda)
        base_url = f"{SEC_BASE_URL_LOCAL}/Archives/edgar/data/{cik}/{accession_dashed}"
        
        local_logger.info(f"      🔗 SEC Archive Base URL: {base_url}")
        local_logger.info(f"      📂 Will store to S3: trades/{target_date}/sec/{form_type}-{cik}-{target_date}.{{ext}}")
        print(f"      🔗 SEC Archive Base URL: {base_url}", flush=True)
        print(f"      📂 Will store to S3: trades/{target_date}/sec/{form_type}-{cik}-{target_date}.{{ext}}", flush=True)
        
        # Create session inside function - each worker gets its own
        # Use the same headers as the working test_pagination.py script
        session = requests.Session()
        session.headers.update({
            'User-Agent': SEC_USER_AGENT,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'max-age=0',
            'Upgrade-Insecure-Requests': '1'
        })
        
        # Build list of URLs to try (matching downloader Lambda logic exactly)
        # Priority 1: Download index.htm to find actual document links
        # Priority 2: Try known document file patterns
        # Priority 3: Try .txt file
        urls_to_try = []
        
        # Priority 1: Download index.htm to find actual document links
        # Based on working Lambda: index file is at {accession-with-dashes}-index.htm
        index_url = f"{base_url}/{accession_dashed}-index.htm"
        urls_to_try.append((index_url, f"{accession_dashed}-index.htm"))
        # Also try plain index.htm as fallback
        index_url_fallback = f"{base_url}/index.htm"
        urls_to_try.append((index_url_fallback, "index.htm"))
        
        # Priority 2: Try known document file patterns (matching downloader Lambda - XML patterns)
        doc_urls = [
            f"{accession_dashed}-primary-document.xml",
            f"{accession_dashed}-primarydoc.xml",
            "primary-document.xml",
            "doc4.xml",  # Common for Form 4
            "doc1.xml",
            f"{accession_dashed}.xml",
        ]
        for doc_name in doc_urls:
            doc_url = f"{base_url}/{doc_name}"
            urls_to_try.append((doc_url, doc_name))
        
        # Priority 3: Try .txt file
        txt_url = f"{base_url}/{accession_dashed}.txt"
        urls_to_try.append((txt_url, f"{accession_dashed}.txt"))
        
        file_content = None
        file_ext = None
        content_type = None
        failed_attempts = []
        
        local_logger.info(f"      📋 Will try {len(urls_to_try)} URL patterns:")
        print(f"      📋 Will try {len(urls_to_try)} URL patterns:", flush=True)
        for idx, (url, name) in enumerate(urls_to_try[:5], 1):  # Show first 5
            local_logger.info(f"         {idx}. {name} -> {url}")
            print(f"         {idx}. {name} -> {url}", flush=True)
        if len(urls_to_try) > 5:
            local_logger.info(f"         ... and {len(urls_to_try) - 5} more")
            print(f"         ... and {len(urls_to_try) - 5} more", flush=True)
        
        for file_url, file_name in urls_to_try:
            try:
                local_logger.info(f"")
                local_logger.info(f"      🔄 ATTEMPTING: {file_name}")
                local_logger.info(f"      🔗 URL: {file_url}")
                print(f"", flush=True)
                print(f"      🔄 ATTEMPTING: {file_name}", flush=True)
                print(f"      🔗 URL: {file_url}", flush=True)
                
                download_start_time = datetime.now()
                try:
                    response = session.get(file_url, timeout=30)
                    download_duration = (datetime.now() - download_start_time).total_seconds()
                    
                    local_logger.info(f"      📥 RESPONSE: Status={response.status_code}, Size={len(response.content):,} bytes, Time={download_duration:.2f}s")
                    print(f"      📥 RESPONSE: Status={response.status_code}, Size={len(response.content):,} bytes, Time={download_duration:.2f}s", flush=True)
                except Exception as get_error:
                    download_duration = (datetime.now() - download_start_time).total_seconds()
                    error_type = type(get_error).__name__
                    error_msg = str(get_error)
                    local_logger.error(f"      ❌ HTTP GET FAILED: {error_type}: {error_msg}")
                    print(f"      ❌ HTTP GET FAILED: {error_type}: {error_msg}", flush=True)
                    failed_attempts.append(f"{file_url} ({error_type}: {error_msg})")
                    continue
                
                if len(response.content) > 0:
                    content_preview = response.content[:200].decode('utf-8', errors='ignore')
                    local_logger.info(f"      📄 Content Preview (first 200 chars): {content_preview}")
                    print(f"      📄 Content Preview (first 200 chars): {content_preview}", flush=True)
                
                # Log response details for debugging
                local_logger.info(f"      📊 Response Details: Status={response.status_code}, Headers={dict(response.headers)}")
                print(f"      📊 Response Details: Status={response.status_code}", flush=True)
                
                if response.status_code == 200:
                    file_content = response.content
                    
                    # Determine file extension and content type (matching downloader Lambda logic)
                    if file_name.endswith('.htm') or file_name.endswith('.html') or 'index' in file_name.lower():
                        # For HTML files (including index pages), try to find the primary document link
                        file_ext = 'html'
                        content_type = 'text/html'
                        
                        # Check if HTML contains document links we should follow
                        # This is the SEC index page that lists available document formats
                        try:
                            html_text = file_content.decode('utf-8', errors='ignore')
                            
                            local_logger.info(f"      🔍 Parsing index page for document links...")
                            print(f"      🔍 Parsing index page for document links...", flush=True)
                            
                            # Parse the SEC index page table to find document links
                            # The table has rows with links like:
                            # <a href="/Archives/edgar/data/1641631/000149315225021146/xslF345X05/ownership.xml">ownership.html</a>
                            # <a href="/Archives/edgar/data/1641631/000149315225021146/ownership.xml">ownership.xml</a>
                            
                            doc_links = []
                            
                            # Match Lambda's exact strategy for finding document links
                            # Strategy 1: Find all .xml file links (prioritize these over .txt)
                            xml_pattern = r'href="([^"]*\.xml[^"]*)"'
                            xml_matches = re.findall(xml_pattern, html_text, re.IGNORECASE)
                            doc_links.extend(xml_matches)
                            local_logger.info(f"      🔍 Found {len(xml_matches)} XML links in page")
                            print(f"      🔍 Found {len(xml_matches)} XML links in page", flush=True)
                            
                            # Strategy 2: Look for primary document patterns (highest priority)
                            primary_patterns = [
                                r'href="([^"]*primary[_-]?document[^"]*\.xml[^"]*)"',
                                r'href="([^"]*primarydoc[^"]*\.xml[^"]*)"',
                                r'href="([^"]*document[^"]*\.xml[^"]*)"',
                                # Common SEC naming: doc4.xml for Form 4
                                r'href="([^"]*doc\d+\.xml[^"]*)"',
                            ]
                            primary_links = []
                            for pattern in primary_patterns:
                                matches = re.findall(pattern, html_text, re.IGNORECASE)
                                primary_links.extend(matches)
                            # Prepend primary links to prioritize them
                            doc_links = primary_links + [link for link in doc_links if link not in primary_links]
                            
                            # Strategy 3: If no XML found, look for .txt files (last resort, contains SGML+XML)
                            if not doc_links:
                                txt_pattern = r'href="([^"]*\.txt[^"]*)"'
                                txt_matches = re.findall(txt_pattern, html_text, re.IGNORECASE)
                                doc_links.extend(txt_matches)
                                local_logger.info(f"      🔍 Found {len(txt_matches)} TXT links (fallback)")
                                print(f"      🔍 Found {len(txt_matches)} TXT links (fallback)", flush=True)
                            
                            # Strategy 4: Look for links with accession number
                            if not doc_links:
                                acc_pattern = rf'href="([^"]*{re.escape(accession_dashed)}[^"]*)"'
                                acc_matches = re.findall(acc_pattern, html_text, re.IGNORECASE)
                                doc_links.extend(acc_matches)
                                local_logger.info(f"      🔍 Found {len(acc_matches)} links with accession number")
                                print(f"      🔍 Found {len(acc_matches)} links with accession number", flush=True)
                            
                            # Remove duplicates while preserving order
                            seen = set()
                            unique_doc_links = []
                            for link in doc_links:
                                if link not in seen:
                                    seen.add(link)
                                    unique_doc_links.append(link)
                            
                            # Sort: XML files first, then others (matching Lambda logic)
                            def link_priority(link):
                                if link.endswith('.xml'):
                                    return 0  # Highest priority
                                elif 'primary' in link.lower() or 'document' in link.lower():
                                    return 1
                                elif 'doc' in link.lower():
                                    return 2
                                else:
                                    return 3
                            
                            sorted_links = sorted(unique_doc_links, key=link_priority)
                            
                            local_logger.info(f"      📋 Found {len(sorted_links)} document links, will try XML first...")
                            print(f"      📋 Found {len(sorted_links)} document links, will try XML first...", flush=True)
                            
                            # Try each found link (matching Lambda: try up to 10 links)
                            for doc_link in sorted_links[:10]:
                                # Handle relative URLs (matching Lambda logic exactly)
                                if doc_link.startswith('/'):
                                    doc_link = f"https://www.sec.gov{doc_link}"
                                elif not doc_link.startswith('http'):
                                    doc_link = f"{base_url}/{doc_link}"
                                
                                # Skip if it's the same URL we just tried
                                if doc_link == file_url:
                                    continue
                                
                                local_logger.info(f"      🔗 Trying link: {doc_link}")
                                print(f"      🔗 Trying link: {doc_link}", flush=True)
                                try:
                                    doc_response = session.get(doc_link, timeout=30)
                                    if doc_response.status_code == 200:
                                        doc_content = doc_response.content
                                        content_start = doc_content[:1000].lower()
                                        
                                        # Check for HTML indicators
                                        is_html = any(indicator in content_start for indicator in [
                                            b'<!doctype html',
                                            b'<html',
                                            b'<head>',
                                            b'<body>',
                                            b'<style',
                                            b'sec form 4',
                                            b'sec form 3',
                                            b'sec form 5',
                                            b'form 4',
                                            b'form 3',
                                            b'form 5',
                                        ])
                                        
                                        # Check for XML indicators
                                        is_xml = (doc_content.startswith(b'<?xml') or 
                                                 b'<ownershipDocument' in doc_content or 
                                                 b'<document>' in doc_content or 
                                                 b'<edgarDocument' in doc_content)
                                        
                                        # Accept either HTML or XML - we can parse both (matching downloader Lambda)
                                        # But prefer HTML if both are detected (user requested HTML files)
                                        if is_html:
                                            # HTML rendering - accept it, matcher will parse it
                                            file_content = doc_content
                                            file_ext = 'html'
                                            content_type = 'text/html'
                                            local_logger.info(f"      ✅ Found HTML document (will parse): {doc_link}")
                                            print(f"      ✅ Found HTML document (will parse): {doc_link}", flush=True)
                                            break
                                        elif is_xml and not is_html:
                                            # Pure XML content
                                            file_content = doc_content
                                            file_ext = 'xml'
                                            content_type = 'application/xml'
                                            local_logger.info(f"      ✅ Found XML document: {doc_link}")
                                            print(f"      ✅ Found XML document: {doc_link}", flush=True)
                                            break
                                        elif b'<sec-header' in content_start:
                                            file_content = doc_content
                                            file_ext = 'txt'
                                            content_type = 'text/plain'
                                            local_logger.info(f"      ✅ Found SGML header: {doc_link}")
                                            print(f"      ✅ Found SGML header: {doc_link}", flush=True)
                                            break
                                        else:
                                            file_content = doc_content
                                            file_ext = 'txt'
                                            content_type = 'text/plain'
                                            local_logger.info(f"      ✅ Found file (format unclear): {doc_link}")
                                            print(f"      ✅ Found file (format unclear): {doc_link}", flush=True)
                                            break
                                except Exception as doc_error:
                                    local_logger.warning(f"      ⚠️ Could not download document link {doc_link}: {doc_error}")
                                    print(f"      ⚠️ Could not download document link {doc_link}: {doc_error}", flush=True)
                                    continue
                                    
                        except Exception as html_parse_error:
                            local_logger.warning(f"      ⚠️ Could not parse HTML for document links: {html_parse_error}")
                            print(f"      ⚠️ Could not parse HTML for document links: {html_parse_error}", flush=True)
                            import traceback
                            local_logger.warning(f"      Traceback: {traceback.format_exc()}")
                            # If HTML parsing fails, we'll store the HTML
                    
                    elif file_name.endswith('.txt'):
                        content_lower = file_content.lower()
                        
                        # Check if it's an SGML header file
                        if b'<sec-header' in content_lower or b'<acceptance-datetime' in content_lower or b'.hdr.sgml' in file_content:
                            # This is an SGML header, try to find the actual document
                            local_logger.warning(f"      ⚠️ Downloaded file is an SGML header, looking for actual document...")
                            print(f"      ⚠️ Downloaded file is an SGML header, looking for actual document...", flush=True)
                            
                            doc_candidates = [
                                f"{accession_dashed}-primary-document.xml",
                                f"{accession_dashed}-primarydoc.xml",
                                "primary-document.xml",
                                "doc4.xml",  # Common document file
                                "doc1.xml",
                                "doc2.xml",
                                "doc3.xml",
                                f"{accession_dashed}.xml",
                            ]
                            
                            found_doc = False
                            for doc_candidate in doc_candidates:
                                doc_url = f"{base_url}/{doc_candidate}"
                                try:
                                    local_logger.info(f"      🔍 Trying document candidate: {doc_url}")
                                    doc_response = session.get(doc_url, timeout=30)
                                    if doc_response.status_code == 200:
                                        doc_content = doc_response.content
                                        # Accept any content type - let matcher handle detection (matching downloader Lambda)
                                        content_sample = doc_content[:100].lower()
                                        is_html = b'<html' in content_sample or b'<!doctype html' in content_sample
                                        is_xml = doc_content.startswith(b'<?xml') or b'<ownershipDocument' in doc_content or b'<document>' in doc_content
                                        
                                        # Prefer HTML if both detected, otherwise accept XML (matching downloader Lambda logic)
                                        if is_html:
                                            file_content = doc_content
                                            file_ext = 'html'
                                            content_type = 'text/html'
                                            local_logger.info(f"      ✅ Found HTML document file: {doc_url}")
                                            found_doc = True
                                            break
                                        elif is_xml:
                                            file_content = doc_content
                                            file_ext = 'xml'
                                            content_type = 'application/xml'
                                            local_logger.info(f"      ✅ Found XML document file: {doc_url}")
                                            found_doc = True
                                            break
                                        else:
                                            file_content = doc_content
                                            file_ext = 'txt'
                                            content_type = 'text/plain'
                                            found_doc = True
                                            break
                                except Exception as doc_error:
                                    continue
                            
                            if not found_doc:
                                local_logger.info(f"      📄 No separate document file found, accepting SGML header")
                                file_ext = 'txt'
                                content_type = 'text/plain'
                        
                        elif file_content.startswith(b'<?xml'):
                            file_ext = 'xml'
                            content_type = 'application/xml'
                        elif b'<ownershipDocument' in file_content or b'<document>' in file_content or b'<XBRL>' in file_content:
                            file_ext = 'xml'
                            content_type = 'application/xml'
                        elif b'<html' in content_lower or b'<!doctype html' in content_lower:
                            file_ext = 'txt'
                            content_type = 'text/html'
                        else:
                            file_ext = 'txt'
                            content_type = 'text/plain'
                    
                    elif file_name.endswith('.xml'):
                        content_sample = file_content[:100].lower()
                        if b'<html' in content_sample or b'<!doctype html' in content_sample:
                            file_ext = 'html'
                            content_type = 'text/html'
                        elif file_content.startswith(b'<?xml') or b'<ownershipDocument' in file_content or b'<document>' in file_content:
                            file_ext = 'xml'
                            content_type = 'application/xml'
                        else:
                            file_ext = 'txt'
                            content_type = 'text/plain'
                    
                    elif file_name.endswith('.pdf'):
                        file_ext = 'pdf'
                        content_type = 'application/pdf'
                    else:
                        # Try to determine from content
                        if file_content.startswith(b'<?xml') or b'<ownershipDocument' in file_content or b'<document>' in file_content:
                            file_ext = 'xml'
                            content_type = 'application/xml'
                        elif b'<html' in file_content.lower():
                            file_ext = 'html'
                            content_type = 'text/html'
                        else:
                            file_ext = 'txt'
                            content_type = 'text/plain'
                    
                    # Accept whatever content we got
                    if file_content:
                        local_logger.info(f"      ✅ DOWNLOAD SUCCESS: CIK={cik}, Accession={accession}, URL={file_url}, Size={len(file_content):,} bytes, Ext={file_ext}")
                        print(f"      ✅ DOWNLOAD SUCCESS: CIK={cik}, Accession={accession}, URL={file_url}, Size={len(file_content):,} bytes, Ext={file_ext}", flush=True)
                        break
                else:
                    # Log non-200 responses with more detail
                    response_text_preview = response.text[:500] if hasattr(response, 'text') else 'N/A'
                    failed_attempts.append(f"{file_url} (HTTP {response.status_code})")
                    local_logger.warning(f"      ⚠️ HTTP {response.status_code} for {file_url}, trying next option...")
                    local_logger.warning(f"      📄 Response preview: {response_text_preview}")
                    print(f"      ⚠️ HTTP {response.status_code} for {file_url}, trying next option...", flush=True)
                    print(f"      📄 Response preview: {response_text_preview}", flush=True)
                    
            except requests.exceptions.Timeout as e:
                failed_attempts.append(f"{file_url} (Timeout)")
                local_logger.warning(f"      ⚠️ Timeout downloading {file_url}: {e}")
                continue
            except Exception as e:
                failed_attempts.append(f"{file_url} (Error: {str(e)})")
                local_logger.warning(f"      ⚠️ Could not download {file_url}: {e}")
                continue
        
        if not file_content:
            local_logger.error(f"")
            local_logger.error(f"      ❌ DOWNLOAD FAILED: CIK={cik}, Accession={accession_dashed}")
            local_logger.error(f"      📋 Attempted {len(urls_to_try)} URLs:")
            print(f"", flush=True)
            print(f"      ❌ DOWNLOAD FAILED: CIK={cik}, Accession={accession_dashed}", flush=True)
            print(f"      📋 Attempted {len(urls_to_try)} URLs:", flush=True)
            for idx, (url, name) in enumerate(urls_to_try, 1):
                local_logger.error(f"         {idx}. {name}: {url}")
                print(f"         {idx}. {name}: {url}", flush=True)
            local_logger.error(f"      ❌ All attempts failed:")
            print(f"      ❌ All attempts failed:", flush=True)
            if failed_attempts:
                for attempt in failed_attempts:
                    local_logger.error(f"         - {attempt}")
                    print(f"         - {attempt}", flush=True)
            else:
                local_logger.error(f"         ⚠️ No specific error details captured - all requests may have returned non-200 status codes")
                print(f"         ⚠️ No specific error details captured - all requests may have returned non-200 status codes", flush=True)
            local_logger.error(f"      " + "="*70)
            print(f"      " + "="*70, flush=True)
            return None
        
        # Generate S3 key
        s3_key = f"trades/{target_date}/sec/{form_type}-{cik}-{target_date}.{file_ext}"
        
        local_logger.info(f"")
        local_logger.info(f"      💾 UPLOADING TO S3:")
        local_logger.info(f"         Bucket: {s3_bucket_name}")
        local_logger.info(f"         Key: {s3_key}")
        local_logger.info(f"         Size: {len(file_content):,} bytes")
        local_logger.info(f"         Content-Type: {content_type}")
        print(f"", flush=True)
        print(f"      💾 UPLOADING TO S3:", flush=True)
        print(f"         Bucket: {s3_bucket_name}", flush=True)
        print(f"         Key: {s3_key}", flush=True)
        print(f"         Size: {len(file_content):,} bytes", flush=True)
        print(f"         Content-Type: {content_type}", flush=True)
        
        # Create S3 client locally to avoid Spark serialization issues
        s3_client_local = boto3.client('s3')
        # Upload to S3 with correct content type
        try:
            local_logger.info(f"      📤 Attempting S3 upload...")
            print(f"      📤 Attempting S3 upload to {s3_bucket_name}/{s3_key}...", flush=True)
            s3_client_local.put_object(
                Bucket=s3_bucket_name,
                Key=s3_key,
                Body=file_content,
                ContentType=content_type or ('application/xml' if file_ext == 'xml' else 'text/html' if file_ext == 'html' else 'application/pdf' if file_ext == 'pdf' else 'text/plain')
            )
            local_logger.info(f"      ✅ S3 UPLOAD SUCCESS!")
            local_logger.info(f"      📦 DOWNLOAD COMPLETE: File ready for parsing")
            local_logger.info(f"      " + "="*70)
            print(f"      ✅ S3 UPLOAD SUCCESS!", flush=True)
            print(f"      📦 DOWNLOAD COMPLETE: File ready for parsing", flush=True)
            print(f"      " + "="*70, flush=True)
        except Exception as s3_error:
            error_type = type(s3_error).__name__
            error_msg = str(s3_error)
            local_logger.error(f"      ❌ S3 UPLOAD FAILED: {error_type}: {error_msg}")
            print(f"      ❌ S3 UPLOAD FAILED: {error_type}: {error_msg}", flush=True)
            import traceback
            local_logger.error(f"      Traceback: {traceback.format_exc()}")
            print(f"      Traceback: {traceback.format_exc()}", flush=True)
            # Re-raise to prevent storing form if S3 upload fails
            raise ValueError(f"S3 upload failed for {s3_key}: {error_msg}") from s3_error
        
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
        error_type = type(e).__name__
        error_msg = str(e)
        import traceback
        error_traceback = traceback.format_exc()
        
        local_logger.error(f"      ❌ DOWNLOAD EXCEPTION: CIK={cik}, Accession={accession}, Error={e}")
        local_logger.error(f"         Error type: {error_type}")
        local_logger.error(f"         Traceback: {error_traceback}")
        
        print("="*80, flush=True)
        print(f"❌ DOWNLOAD EXCEPTION CAUGHT", flush=True)
        print(f"   CIK: {cik}", flush=True)
        print(f"   Accession: {accession}", flush=True)
        print(f"   Error Type: {error_type}", flush=True)
        print(f"   Error Message: {error_msg}", flush=True)
        print(f"   Traceback:", flush=True)
        print(error_traceback, flush=True)
        print("="*80, flush=True)
        
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
        similarity = builtins.min(1.0, similarity + 0.1)
    
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
        # IMPORTANT: Get the FIRST occurrence which is the primary reporting person
        # The form may have multiple reporting persons (joint filings), but we want the primary one
        name_match = re.search(r'1\.\s*Name and Address of Reporting Person[^<]*<a[^>]*href="/cgi-bin/browse-edgar[^"]*CIK=\d+">([^<]+)</a>', html_content, re.IGNORECASE | re.DOTALL)
        if name_match:
            result['name'] = unescape(name_match.group(1)).strip()
        else:
            # Fallback: try to find any reporting person link near the top of the form
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
            # Remove HTML tags but preserve text content
            # First, extract text from span.FormData and span.SmallFormData (these contain the actual data)
            # Then remove all remaining HTML tags
            text = cell
            
            # Extract text from FormData spans (these contain the actual values)
            form_data_matches = re.findall(r'<span[^>]*class="FormData"[^>]*>([^<]+)</span>', text, re.IGNORECASE)
            if form_data_matches:
                text = ' '.join(form_data_matches)
            else:
                # Fallback: extract text from SmallFormData spans
                small_form_data_matches = re.findall(r'<span[^>]*class="SmallFormData"[^>]*>([^<]+)</span>', text, re.IGNORECASE)
                if small_form_data_matches:
                    text = ' '.join(small_form_data_matches)
                else:
                    # Last resort: strip all HTML tags
                    text = re.sub(r'<[^>]+>', '', text)
            
            text = unescape(text)
            return text.strip()
        
        def extract_footnotes(cell):
            """Extract footnote references like (1), (2) from a cell"""
            # Look for <sup>(1)</sup> or <sup>(2)</sup> patterns
            footnote_pattern = r'<sup>\((\d+)\)</sup>'
            footnotes = re.findall(footnote_pattern, cell, re.IGNORECASE)
            return [int(fn) for fn in footnotes] if footnotes else []
        
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
                
                # Extract footnotes from each cell
                footnotes = {}
                if len(cells) > 1:
                    amount_footnotes = extract_footnotes(cells[1])
                    if amount_footnotes:
                        footnotes['amountOfSecurities'] = amount_footnotes
                if len(cells) > 2:
                    ownership_footnotes = extract_footnotes(cells[2])
                    if ownership_footnotes:
                        footnotes['ownershipForm'] = ownership_footnotes
                if len(cells) > 3:
                    nature_footnotes = extract_footnotes(cells[3])
                    if nature_footnotes:
                        footnotes['natureOfIndirectBeneficialOwnership'] = nature_footnotes
                
                # Add footnotes to row_data if any were found
                if footnotes:
                    row_data['footnotes'] = footnotes
                
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
            # Remove HTML tags but preserve text content
            # First, extract text from span.FormData and span.SmallFormData (these contain the actual data)
            # Then remove all remaining HTML tags
            text = cell
            
            # Extract text from FormData spans (these contain the actual values)
            form_data_matches = re.findall(r'<span[^>]*class="FormData"[^>]*>([^<]+)</span>', text, re.IGNORECASE)
            if form_data_matches:
                text = ' '.join(form_data_matches)
            else:
                # Fallback: extract text from SmallFormData spans
                small_form_data_matches = re.findall(r'<span[^>]*class="SmallFormData"[^>]*>([^<]+)</span>', text, re.IGNORECASE)
                if small_form_data_matches:
                    text = ' '.join(small_form_data_matches)
                else:
                    # Last resort: strip all HTML tags
                    text = re.sub(r'<[^>]+>', '', text)
            
            text = unescape(text)
            return text.strip()
        
        def extract_footnotes(cell):
            """Extract footnote references like (1), (2) from a cell"""
            # Look for <sup>(1)</sup> or <sup>(2)</sup> patterns
            footnote_pattern = r'<sup>\((\d+)\)</sup>'
            footnotes = re.findall(footnote_pattern, cell, re.IGNORECASE)
            return [int(fn) for fn in footnotes] if footnotes else []
        
        for row in rows:
            cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL | re.IGNORECASE)
            
            if is_form3 and len(cells) >= 6:
                # Form 3 Table II structure (8 columns):
                # 0: Title of Derivative Security
                # 1: Date Exercisable (may be empty with footnote)
                # 2: Expiration Date (may be empty with footnote)
                # 3: Title of Underlying Security
                # 4: Amount or Number of Shares
                # 5: Conversion or Exercise Price (may be empty with footnote)
                # 6: Ownership Form (D or I, may have footnote)
                # 7: Nature of Indirect Beneficial Ownership
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
                
                # Extract footnotes from each cell
                footnotes = {}
                if len(cells) > 1:
                    date_ex_footnotes = extract_footnotes(cells[1])
                    if date_ex_footnotes:
                        footnotes['dateExercisable'] = date_ex_footnotes
                if len(cells) > 2:
                    exp_date_footnotes = extract_footnotes(cells[2])
                    if exp_date_footnotes:
                        footnotes['expirationDate'] = exp_date_footnotes
                if len(cells) > 5:
                    conv_price_footnotes = extract_footnotes(cells[5])
                    if conv_price_footnotes:
                        footnotes['conversionOrExercisePrice'] = conv_price_footnotes
                if len(cells) > 6:
                    ownership_footnotes = extract_footnotes(cells[6])
                    if ownership_footnotes:
                        footnotes['ownershipForm'] = ownership_footnotes
                
                # Add footnotes to row_data if any were found
                if footnotes:
                    row_data['footnotes'] = footnotes
                
                # Only add row if it has meaningful data (at least title or amount)
                if row_data.get('titleOfDerivativeSecurity') or row_data.get('amountOrNumberOfShares'):
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
    
    # CRITICAL: Use print statements for Glue console visibility
    print("="*80, flush=True)
    print("🟢 process_form CALLED", flush=True)
    print(f"   form_data keys: {list(form_data.keys()) if form_data else 'None'}", flush=True)
    print(f"   target_date: {target_date}", flush=True)
    print(f"   s3_bucket_name: {s3_bucket_name}", flush=True)
    print(f"   dynamodb_table_name: {dynamodb_table_name}", flush=True)
    
    cik = form_data.get('cik', 'unknown')
    accession = form_data.get('accession_number', 'unknown')
    form_type = form_data.get('form_type', 'unknown')
    filing_date_str = form_data.get('filing_date', target_date)
    accepted_date_str = form_data.get('accepted_date')
    
    print(f"   Extracted: CIK={cik}, Accession={accession}, Type={form_type}, FilingDate={filing_date_str}", flush=True)
    
    form_start_time = datetime.now()
    local_logger.info(f"📄 Processing Form: CIK={cik}, Accession={accession}, Type={form_type}, FilingDate={filing_date_str}")
    print(f"📄 Processing Form: CIK={cik}, Accession={accession}, Type={form_type}, FilingDate={filing_date_str}", flush=True)
    
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
    print(f"   📥 Step 2/4: Downloading form...", flush=True)
    print(f"   🔵 About to call download_sec_form...", flush=True)
    print(f"   🔵 Form data being passed: {json.dumps(form_data, default=str)}", flush=True)
    print(f"   🔵 Target date: {target_date}, S3 bucket: {s3_bucket_name}", flush=True)
    
    try:
        downloaded = download_sec_form(form_data, target_date, s3_bucket_name)
        download_duration = (datetime.now() - download_start).total_seconds()
        
        print(f"   🔵 download_sec_form returned: {type(downloaded).__name__}", flush=True)
        if downloaded:
            print(f"   🔵 download_sec_form returned dict with keys: {list(downloaded.keys()) if isinstance(downloaded, dict) else 'N/A'}", flush=True)
        else:
            print(f"   🔵 download_sec_form returned None or False", flush=True)
    except Exception as download_exception:
        download_duration = (datetime.now() - download_start).total_seconds()
        error_type = type(download_exception).__name__
        error_msg = str(download_exception)
        import traceback
        error_traceback = traceback.format_exc()
        
        local_logger.error(f"   ❌ EXCEPTION calling download_sec_form: {error_type}: {error_msg}")
        local_logger.error(f"      Traceback: {error_traceback}")
        print(f"   ❌ EXCEPTION calling download_sec_form: {error_type}: {error_msg}", flush=True)
        print(f"      Traceback: {error_traceback}", flush=True)
        downloaded = None
    
    if not downloaded:
        error_msg = f"   ❌ FAILED: Could not download form (CIK={cik}, Accession={accession}) after {download_duration:.2f}s"
        local_logger.warning(error_msg)
        print(error_msg, flush=True)
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
    
    # Log parsed data summary with full details
    local_logger.info(f"   ✅ Parsing complete in {parse_duration:.2f}s:")
    local_logger.info(f"      📊 PARSED DATA SUMMARY:")
    local_logger.info(f"         - Form Type: {parsed_data.get('formType', 'N/A')}")
    local_logger.info(f"         - Name: {parsed_data.get('name', 'N/A')}")
    local_logger.info(f"         - Address: {parsed_data.get('address', 'N/A')}")
    local_logger.info(f"         - Issuer: {parsed_data.get('issuerName', 'N/A')}")
    local_logger.info(f"         - Ticker: {parsed_data.get('tickerSymbol', 'N/A')}")
    local_logger.info(f"         - Relationship: {parsed_data.get('relationship', 'N/A')}")
    local_logger.info(f"         - Relationship Additional: {parsed_data.get('relationshipAdditionalText', 'N/A')}")
    local_logger.info(f"         - Event Date: {parsed_data.get('eventDate', 'N/A')}")
    local_logger.info(f"         - Reporting Date: {parsed_data.get('reportingDate', 'N/A')}")
    local_logger.info(f"         - Signature Name: {parsed_data.get('signatureName', 'N/A')}")
    local_logger.info(f"         - Amendment: {parsed_data.get('amendment', False)}")
    local_logger.info(f"         - Table I rows (Non-Derivative): {len(parsed_data.get('nonDerivativeSecurities', []))}")
    if parsed_data.get('nonDerivativeSecurities'):
        local_logger.info(f"            First row: {json.dumps(parsed_data['nonDerivativeSecurities'][0], default=str)[:200]}")
    local_logger.info(f"         - Table II rows (Derivative): {len(parsed_data.get('derivativeSecurities', []))}")
    if parsed_data.get('derivativeSecurities'):
        local_logger.info(f"            First row: {json.dumps(parsed_data['derivativeSecurities'][0], default=str)[:200]}")
    local_logger.info(f"         - Misc/Explanations: {len(parsed_data.get('misc', {}))} entries")
    if parsed_data.get('misc'):
        misc_preview = {k: str(v)[:100] for k, v in list(parsed_data['misc'].items())[:3]}
        local_logger.info(f"            Preview: {json.dumps(misc_preview, default=str)[:300]}")
    
    # Check for critical missing fields
    missing_fields = []
    if not parsed_data.get('name'):
        missing_fields.append('name')
    if not parsed_data.get('formType'):
        missing_fields.append('formType')
    if not parsed_data.get('reportingDate'):
        missing_fields.append('reportingDate')
    if missing_fields:
        local_logger.warning(f"      ⚠️ WARNING: Missing critical fields: {', '.join(missing_fields)}")
    
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
    
    # Generate trade ID (includes CIK to ensure uniqueness even for joint filings with same accession)
    # Note: After deduplication, each accession should only be processed once, but tradeId still includes CIK
    # for historical consistency and to handle edge cases
    trade_id = f"sec_{form_data.get('form_type', 'form4')}_{cik}_{accession}_{target_date.replace('-', '')}"
    parsed_data['tradeId'] = trade_id
    parsed_data['formS3Key'] = s3_key
    
    # Log tradeId and S3 key for debugging
    local_logger.info(f"   📋 Generated TradeId: {trade_id}")
    local_logger.info(f"   📋 S3 Key: {s3_key}")
    print(f"   📋 Generated TradeId: {trade_id}", flush=True)
    print(f"   📋 S3 Key: {s3_key}", flush=True)
    
    # Handle amendment logic (will be implemented later when we can query existing records)
    # For now, just mark if it's an amendment
    # TODO: Query DynamoDB to find original trade and link them
    
    # Store to DynamoDB
    store_start = datetime.now()
    local_logger.info(f"   💾 Storing to DynamoDB (TradeId={trade_id})...")
    local_logger.info(f"      📊 DynamoDB Write Details:")
    local_logger.info(f"         Table: {dynamodb_table_name}")
    local_logger.info(f"         TradeId: {trade_id}")
    local_logger.info(f"         Item Keys: {list(parsed_data.keys())}")
    
    try:
        # Create DynamoDB client locally
        local_logger.info(f"      🔄 Creating DynamoDB client...")
        dynamodb_local = boto3.resource('dynamodb')
        table_local = dynamodb_local.Table(dynamodb_table_name)
        local_logger.info(f"      ✅ DynamoDB client created, accessing table: {dynamodb_table_name}")
        
        # Convert to DynamoDB format
        local_logger.info(f"      🔄 Converting to DynamoDB format...")
        dynamodb_item = {}
        conversion_stats = {'skipped': 0, 'converted': 0, 'errors': 0}
        
        for key, value in parsed_data.items():
            if value is None or value == '':
                conversion_stats['skipped'] += 1
                local_logger.debug(f"         Skipping {key}: None or empty")
                continue
            try:
                if isinstance(value, (int, float)):
                    if isinstance(value, float) and (value != value or value == float('inf') or value == float('-inf')):
                        conversion_stats['skipped'] += 1
                        local_logger.warning(f"         Skipping {key}: Invalid float value {value}")
                        continue
                    dynamodb_item[key] = Decimal(str(value))
                    conversion_stats['converted'] += 1
                elif isinstance(value, list):
                    # Lists of dicts (JSON arrays) - store as JSON string
                    dynamodb_item[key] = json.dumps(value)
                    conversion_stats['converted'] += 1
                    local_logger.debug(f"         {key}: JSON array ({len(value)} items)")
                elif isinstance(value, dict):
                    # Dicts (JSON objects) - store as JSON string
                    dynamodb_item[key] = json.dumps(value)
                    conversion_stats['converted'] += 1
                    local_logger.debug(f"         {key}: JSON object ({len(value)} keys)")
                else:
                    dynamodb_item[key] = str(value)
                    conversion_stats['converted'] += 1
                    local_logger.debug(f"         {key}: String ({len(str(value))} chars)")
            except Exception as e:
                conversion_stats['errors'] += 1
                local_logger.error(f"         Error converting {key}: {e}")
        
        local_logger.info(f"      ✅ Conversion complete: {conversion_stats['converted']} converted, {conversion_stats['skipped']} skipped, {conversion_stats['errors']} errors")
        
        local_logger.info(f"      📦 DynamoDB Item Preview:")
        local_logger.info(f"         TradeId: {dynamodb_item.get('tradeId', 'N/A')}")
        local_logger.info(f"         FormType: {dynamodb_item.get('formType', 'N/A')}")
        local_logger.info(f"         Name: {dynamodb_item.get('name', 'N/A')}")
        local_logger.info(f"         IssuerName: {dynamodb_item.get('issuerName', 'N/A')}")
        local_logger.info(f"         TickerSymbol: {dynamodb_item.get('tickerSymbol', 'N/A')}")
        local_logger.info(f"         ReportingDate: {dynamodb_item.get('reportingDate', 'N/A')}")
        local_logger.info(f"         EventDate: {dynamodb_item.get('eventDate', 'N/A')}")
        local_logger.info(f"         Relationship: {dynamodb_item.get('relationship', 'N/A')}")
        local_logger.info(f"         Politician: {dynamodb_item.get('politician', 'N/A')}")
        local_logger.info(f"         FormS3Key: {dynamodb_item.get('formS3Key', 'N/A')}")
        local_logger.info(f"         Total Fields: {len(dynamodb_item)}")
        
        # Log full item for debugging (truncated)
        item_preview = {k: (str(v)[:100] + '...' if len(str(v)) > 100 else v) for k, v in list(dynamodb_item.items())[:10]}
        local_logger.info(f"         Item Preview (first 10 fields): {json.dumps(item_preview, default=str)}")
        
        local_logger.info(f"      📡 Calling DynamoDB PutItem API...")
        db_write_start = datetime.now()
        put_response = table_local.put_item(Item=dynamodb_item)
        db_write_duration = (datetime.now() - db_write_start).total_seconds()
        
        local_logger.info(f"      ✅ DynamoDB PutItem Response:")
        local_logger.info(f"         Success: True")
        local_logger.info(f"         Response Metadata: {put_response.get('ResponseMetadata', {}).get('HTTPStatusCode', 'N/A')}")
        local_logger.info(f"         Write Time: {db_write_duration:.2f}s")
        
        store_duration = (datetime.now() - store_start).total_seconds()
        total_duration = (datetime.now() - form_start_time).total_seconds()
        
        local_logger.info(f"   ✅ STORED: TradeId={trade_id}, S3Key={s3_key} in {store_duration:.2f}s")
        local_logger.info(f"   ✅ Form processing complete: Total time {total_duration:.2f}s")
        local_logger.info(f"      Breakdown: Download={download_duration:.2f}s, Parse={parse_duration:.2f}s, Match={match_duration:.2f}s, Store={store_duration:.2f}s")
        
        return {'success': True, 'tradeId': trade_id, 'politicianMatch': politician_match is not None}
    
    except Exception as e:
        store_duration = (datetime.now() - store_start).total_seconds()
        total_duration = (datetime.now() - form_start_time).total_seconds()
        error_msg = f"   ❌ STORAGE ERROR: Failed to store to DynamoDB after {store_duration:.2f}s (Total: {total_duration:.2f}s): {e}"
        local_logger.error(error_msg)
        print(error_msg, flush=True)
        import traceback
        traceback_str = traceback.format_exc()
        local_logger.error(f"      Traceback: {traceback_str}")
        print(f"      Traceback: {traceback_str}", flush=True)
        return {'success': False, 'error': str(e), 'traceback': traceback_str}


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
    if is_backdate_mode:
        search_msg = f"🔍 BACKDATE MODE: Fetching all Forms 3, 4, 5 from {target_date} onwards (until first date in batch is before {target_date})"
    else:
        search_msg = f"🔍 Searching for Forms 3, 4, 5 filed on {target_date}"
    logger.info(search_msg)
    print(search_msg, flush=True)
    stage2_start = datetime.now()
    forms = fetch_sec_forms_paginated(target_date, is_backdate_mode=is_backdate_mode)
    stage2_duration = (datetime.now() - stage2_start).total_seconds()
    logger.info("")
    stage2_msg = f"✅ Stage 2 Complete: Fetched {len(forms)} forms in {stage2_duration:.2f} seconds"
    logger.info(stage2_msg)
    print(stage2_msg, flush=True)
    
    # Log form type breakdown
    form_type_counts = {}
    for form in forms:
        form_type = form.get('form_type', 'unknown')
        form_type_counts[form_type] = form_type_counts.get(form_type, 0) + 1
    breakdown_msg = f"   Form Type Breakdown:"
    logger.info(breakdown_msg)
    print(breakdown_msg, flush=True)
    for form_type, count in sorted(form_type_counts.items()):
        type_msg = f"      - {form_type}: {count}"
        logger.info(type_msg)
        print(type_msg, flush=True)
    
    # Log preview of fetched files (first 10)
    preview_msg = f"   📋 Preview of Fetched Files (showing first {builtins.min(10, len(forms))} of {len(forms)}):"
    logger.info("")
    logger.info(preview_msg)
    print("", flush=True)
    print(preview_msg, flush=True)
    for idx, form in enumerate(forms[:10], 1):
        form_msg = (f"      {idx}. CIK={form.get('cik', 'N/A')}, "
                   f"Accession={form.get('accession_number', 'N/A')[:20]}, "
                   f"Type={form.get('form_type', 'N/A')}, "
                   f"FilingDate={form.get('filing_date', 'N/A')}, "
                   f"AcceptedDate={form.get('accepted_date', 'N/A') or 'N/A'}")
        logger.info(form_msg)
        print(form_msg, flush=True)
    if len(forms) > 10:
        more_msg = f"      ... ({len(forms) - 10} more files)"
        logger.info(more_msg)
        print(more_msg, flush=True)
    logger.info("=" * 80)
    print("=" * 80, flush=True)
    
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
    
    # Limit to first 10 forms for testing
    test_limit = 10
    if len(forms) > test_limit:
        logger.info(f"   ⚠️ TESTING MODE: Limiting to first {test_limit} forms (out of {len(forms)} total)")
        print(f"   ⚠️ TESTING MODE: Limiting to first {test_limit} forms (out of {len(forms)} total)", flush=True)
        forms = forms[:test_limit]
    
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
                msg = f"✅ Form completed: CIK={cik}, TradeId={result.get('tradeId', 'N/A')}, PoliticianMatch={result.get('politicianMatch', False)}"
                local_logger.info(msg)
                print(msg, flush=True)
            elif result.get('skipped'):
                reason = result.get('reason', 'unknown')
                msg = f"⏭️ Form skipped: CIK={cik}, Accession={accession}, Reason={reason}"
                local_logger.info(msg)
                print(msg, flush=True)
                if reason == 'date_mismatch':
                    detail = f"   Details: Filing date doesn't match target date"
                    local_logger.warning(detail)
                    print(detail, flush=True)
                elif reason == 'download_failed':
                    detail = f"   Details: Could not download form from SEC website"
                    local_logger.warning(detail)
                    print(detail, flush=True)
                elif reason == 'unsupported_file_type':
                    detail = f"   Details: File type not supported (only HTML supported currently)"
                    local_logger.warning(detail)
                    print(detail, flush=True)
            else:
                error_msg = result.get('error', 'unknown')
                msg = f"❌ Form failed: CIK={cik}, Accession={accession}, Error={error_msg}"
                local_logger.warning(msg)
                print(msg, flush=True)
            
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
    # Use builtins.sum to avoid conflict with PySpark's sum() function
    total_forms_processed = len(forms)
    successful_stored = builtins.sum(1 for r in results if r.get('success'))
    failed_stored = builtins.sum(1 for r in results if not r.get('success') and not r.get('skipped'))
    skipped_date_mismatch = builtins.sum(1 for r in results if r.get('skipped') and r.get('reason') == 'date_mismatch')
    skipped_download_failed = builtins.sum(1 for r in results if r.get('skipped') and r.get('reason') == 'download_failed')
    skipped_unsupported_type = builtins.sum(1 for r in results if r.get('skipped') and r.get('reason') == 'unsupported_file_type')
    politician_matches = builtins.sum(1 for r in results if r.get('politicianMatch'))
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
    
    # Detailed failure analysis
    logger.info("")
    logger.info(f"🔍 Detailed Failure Analysis:")
    logger.info(f"   - Skipped (date mismatch): {skipped_date_mismatch} forms")
    logger.info(f"   - Skipped (download failed): {skipped_download_failed} forms")
    logger.info(f"   - Skipped (unsupported file type): {skipped_unsupported_type} forms")
    logger.info(f"   - Failed to store: {failed_stored} forms")
    
    # Show sample of failed results for debugging
    if failed_stored > 0:
        logger.info("")
        logger.info(f"   📋 Sample of Failed Forms (first 5):")
        print("", flush=True)
        print(f"   📋 Sample of Failed Forms (first 5):", flush=True)
        failed_samples = [r for r in results if not r.get('success') and not r.get('skipped')][:5]
        for idx, failed in enumerate(failed_samples, 1):
            error_info = f"      {idx}. Error: {failed.get('error', 'Unknown error')}"
            logger.info(error_info)
            print(error_info, flush=True)
            if failed.get('traceback'):
                traceback_preview = failed.get('traceback', '')[:500]  # First 500 chars
                logger.info(f"         Traceback (preview): {traceback_preview}")
                print(f"         Traceback (preview): {traceback_preview}", flush=True)
    
    if skipped_download_failed > 0:
        logger.info("")
        logger.info(f"   📋 Sample of Download Failures (first 5):")
        download_failed_samples = [r for r in results if r.get('skipped') and r.get('reason') == 'download_failed'][:5]
        for idx, failed in enumerate(download_failed_samples, 1):
            logger.info(f"      {idx}. Reason: {failed.get('reason', 'Unknown')}")
    
    if skipped_date_mismatch > 0:
        logger.info("")
        logger.info(f"   ⚠️ Note: {skipped_date_mismatch} forms were skipped due to date mismatch")
        logger.info(f"      This is normal if the filing date in the form doesn't match the target date")
    
    if successful_stored == 0:
        warning_header = "   ⚠️ CRITICAL WARNING: No forms were successfully stored!"
        logger.warning("")
        logger.warning(warning_header)
        print("", flush=True)
        print(warning_header, flush=True)
        
        detail_msg = "      This could indicate:"
        logger.warning(detail_msg)
        print(detail_msg, flush=True)
        
        for detail in [
            "- Download failures for all forms (check network/SEC website)",
            "- Parsing failures for all forms (check HTML structure)",
            "- Date mismatches for all forms (check target date)",
            "- DynamoDB write failures (check permissions/table)"
        ]:
            logger.warning(f"      {detail}")
            print(f"      {detail}", flush=True)
        
        review_msg = "      Review the detailed logs above for specific error messages"
        logger.warning(review_msg)
        print(review_msg, flush=True)
        
        # Log detailed breakdown of what happened
        print("", flush=True)
        print("   🔍 DETAILED BREAKDOWN:", flush=True)
        print(f"      Total forms fetched: {total_forms_processed}", flush=True)
        print(f"      Successfully stored: {successful_stored}", flush=True)
        print(f"      Failed to store: {failed_stored}", flush=True)
        print(f"      Skipped (date mismatch): {skipped_date_mismatch}", flush=True)
        print(f"      Skipped (download failed): {skipped_download_failed}", flush=True)
        print(f"      Skipped (unsupported file type): {skipped_unsupported_type}", flush=True)
    elif successful_stored < total_forms_processed * 0.5:
        logger.warning("")
        logger.warning(f"   ⚠️ WARNING: Less than 50% of forms were successfully stored!")
        logger.warning(f"      Success rate: {successful_stored/total_forms_processed*100:.1f}%")
        logger.warning(f"      Review the detailed logs above for specific error messages")
    
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

