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
import time
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
# SEC requires a browser-like User-Agent to avoid 403 Forbidden errors
# Format: Browser User-Agent with contact info appended
SEC_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 (Cosine Financial Platform; contact@cosine.financial)"
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
    Fetch SEC forms using Daily Index Files (recommended approach)
    
    Daily index files are available at:
    https://www.sec.gov/Archives/edgar/daily-index/{YEAR}/QTR{QUARTER}/master.{YYYYMMDD}.idx
    
    These files contain all filings for a specific date, including Forms 3, 4, 5.
    This approach is more reliable than browse-edgar HTML scraping and less likely to be blocked.
    
    Args:
        target_date: Target date in YYYY-MM-DD format (for normal mode) or backdate (for backdate mode)
        form_types: List of form types to fetch (default: ['3', '4', '5'])
        is_backdate_mode: If True, fetch all records from today back to target_date (multiple index files).
                         If False, only fetch records matching target_date exactly (single index file).
    
    Returns:
        List of form metadata dicts with keys: form_type, cik, accession_number, filename, filing_date
    """
    all_forms = []
    target_date_obj = datetime.strptime(target_date, '%Y-%m-%d').date()
    
    session = requests.Session()
    # Use browser-like headers to avoid 403 Forbidden errors
    session.headers.update({
        'User-Agent': SEC_USER_AGENT,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Referer': 'https://www.sec.gov/',
    })
    
    # Determine which dates to fetch
    dates_to_fetch = []
    if is_backdate_mode:
        # Backdate mode: fetch from today back to target_date
        current_date = datetime.now().date()
        date_iter = current_date
        while date_iter >= target_date_obj:
            dates_to_fetch.append(date_iter)
            date_iter -= timedelta(days=1)
    else:
        # Normal mode: fetch only target_date
        dates_to_fetch = [target_date_obj]
    
    logger.info("")
    logger.info(f"🔍 Searching for Forms 3, 4, 5 filed on {target_date}")
    print(f"🔍 Searching for Forms 3, 4, 5 filed on {target_date}", flush=True)
    
    fetch_start = datetime.now()
    
    # Fetch forms from each date's index file
    for date_to_fetch in dates_to_fetch:
        date_str = date_to_fetch.strftime('%Y-%m-%d')
        date_str_idx = date_to_fetch.strftime('%Y%m%d')
        year = date_to_fetch.year
        quarter = (date_to_fetch.month - 1) // 3 + 1
        
        # Daily index file URL
        index_url = f"{SEC_BASE_URL}/Archives/edgar/daily-index/{year}/QTR{quarter}/master.{date_str_idx}.idx"
        
        logger.info(f"📥 Fetching daily index file for {date_str}...")
        print(f"📥 Fetching daily index file for {date_str}...", flush=True)
        
        try:
            # Add small delay to avoid rate limiting
            time.sleep(0.3)
            
            response = session.get(index_url, timeout=30)
            
            if response.status_code == 404:
                logger.info(f"   ⚠️ Index file not found for {date_str} (may be weekend/holiday)")
                print(f"   ⚠️ Index file not found for {date_str} (may be weekend/holiday)", flush=True)
                continue
            
            response.raise_for_status()
            
            # Parse the index file
            # Format: CIK|Company Name|Form Type|Date Filed|File Name
            content = response.text
            lines = content.split('\n')
            
            # Find the header line and data start
            header_found = False
            data_start_idx = 0
            
            for idx, line in enumerate(lines):
                if line.startswith('CIK|'):
                    header_found = True
                    data_start_idx = idx + 1
                    break
            
            if not header_found:
                logger.warning(f"   ⚠️ No header line found in index file for {date_str}")
                continue
            
            # Parse data lines
            forms_for_date = []
            form_type_nums = [ft.replace('form', '') for ft in form_types]
            
            for line in lines[data_start_idx:]:
                if not line.strip():
                    continue
                
                # Parse pipe-delimited format: CIK|Company Name|Form Type|Date Filed|File Name
                parts = line.split('|')
                if len(parts) < 5:
                    continue
                
                try:
                    cik = parts[0].strip()
                    company_name = parts[1].strip()
                    form_type_raw = parts[2].strip()
                    date_filed = parts[3].strip()
                    filename = parts[4].strip()
                    
                    # Check if this is a Form 3, 4, or 5
                    form_match = re.search(r'(\d+)', form_type_raw)
                    if form_match:
                        form_num = form_match.group(1)
                        if form_num in form_type_nums:
                            # Check if date matches (index file date format is YYYYMMDD)
                            if date_filed == date_str_idx:
                                # Extract accession number from filename
                                # Format: {accession}-{form_type}.txt or {accession}-index.htm
                                accession_match = re.search(r'(\d{10}-\d{2}-\d{6})', filename)
                                if accession_match:
                                    accession_dashed = accession_match.group(1)
                                    accession_clean = accession_dashed.replace('-', '')
                                    
                                    form_data = {
                                        'cik': cik,
                                        'accession_number': accession_clean,  # Without dashes (matching downloader input format)
                                        'form_type': f'form{form_num}',
                                        'filing_date': date_str,
                                        'company_name': company_name,
                                        'filename': filename
                                    }
                                    forms_for_date.append(form_data)
                except Exception:
                    continue
            
            all_forms.extend(forms_for_date)
            
            if forms_for_date:
                logger.info(f"   ✅ Found {len(forms_for_date)} Forms 3/4/5 for {date_str}")
                print(f"   ✅ Found {len(forms_for_date)} Forms 3/4/5 for {date_str}", flush=True)
            
        except requests.exceptions.RequestException as e:
            logger.error(f"   ❌ Error fetching index file for {date_str}: {e}")
            print(f"   ❌ Error fetching index file for {date_str}: {e}", flush=True)
            continue
        except Exception as e:
            logger.error(f"   ❌ Unexpected error processing index file for {date_str}: {e}")
            print(f"   ❌ Unexpected error processing index file for {date_str}: {e}", flush=True)
            import traceback
            logger.error(f"   Traceback: {traceback.format_exc()}")
            continue
    
    fetch_duration = (datetime.now() - fetch_start).total_seconds()
    
    # Log summary
    logger.info("")
    logger.info(f"✅ Stage 2 Complete: Fetched {len(all_forms)} forms in {fetch_duration:.2f} seconds")
    print(f"✅ Stage 2 Complete: Fetched {len(all_forms)} forms in {fetch_duration:.2f} seconds", flush=True)
    
    # Form type breakdown
    form_counts = {}
    for form in all_forms:
        form_type = form['form_type']
        form_counts[form_type] = form_counts.get(form_type, 0) + 1
    
    logger.info("Form Type Breakdown:")
    print("Form Type Breakdown:", flush=True)
    for form_type in sorted(form_counts.keys()):
        count = form_counts[form_type]
        logger.info(f"- {form_type}: {count}")
        print(f"- {form_type}: {count}", flush=True)
    
    # Preview of fetched files
    preview_count = builtins.min(10, len(all_forms))
    logger.info(f"📋 Preview of Fetched Files (showing first {preview_count} of {len(all_forms)}):")
    print(f"📋 Preview of Fetched Files (showing first {preview_count} of {len(all_forms)}):", flush=True)
    for idx, form in enumerate(all_forms[:preview_count], 1):
        cik = form.get('cik', 'unknown')
        accession = form.get('accession_number', 'unknown')
        form_type = form.get('form_type', 'unknown')
        filing_date = form.get('filing_date', 'unknown')
        company = form.get('company_name', 'unknown')[:50]
        logger.info(f"{idx}. CIK={cik}, Accession={accession[:20]}..., Type={form_type}, FilingDate={filing_date}, Company={company}")
        print(f"{idx}. CIK={cik}, Accession={accession[:20]}..., Type={form_type}, FilingDate={filing_date}, Company={company}", flush=True)
    
    if len(all_forms) > preview_count:
        logger.info(f"... ({len(all_forms) - preview_count} more files)")
        print(f"... ({len(all_forms) - preview_count} more files)", flush=True)
    
    logger.info("")
    print("", flush=True)
    
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
        
        local_logger.info(f"")
        local_logger.info(f"      " + "="*70)
        local_logger.info(f"      📥 DOWNLOAD START: CIK={cik}, Accession={accession}, Type={form_type}")
        local_logger.info(f"      📅 Filing Date: {filing_date}")
        print(f"", flush=True)
        print(f"      " + "="*70, flush=True)
        print(f"      📥 DOWNLOAD START: CIK={cik}, Accession={accession}, Type={form_type}", flush=True)
        print(f"      📅 Filing Date: {filing_date}", flush=True)
        
        if not all([cik, accession]) or cik == 'unknown' or accession == 'unknown':
            error_msg = f"   ⚠️ Missing CIK/accession: CIK={cik}, Accession={accession}"
            local_logger.warning(error_msg)
            print(error_msg, flush=True)
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
        # Use browser-like headers to avoid 403 Forbidden errors (matching fetch function)
        session = requests.Session()
        session.headers.update({
            'User-Agent': SEC_USER_AGENT,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Cache-Control': 'max-age=0',
            'Upgrade-Insecure-Requests': '1',
            'Referer': 'https://www.sec.gov/',
            'Connection': 'keep-alive',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1'
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
                # Add small delay before download to avoid rate limiting
                time.sleep(0.1)  # 100ms delay
                
                local_logger.info(f"")
                local_logger.info(f"      🔄 ATTEMPTING: {file_name}")
                local_logger.info(f"      🔗 URL: {file_url}")
                print(f"", flush=True)
                print(f"      🔄 ATTEMPTING: {file_name}", flush=True)
                print(f"      🔗 URL: {file_url}", flush=True)
                
                download_start_time = datetime.now()
                
                # Retry logic for 403 errors
                max_retries = 2
                retry_delay = 1  # seconds
                response = None
                
                for attempt in range(max_retries):
                    try:
                        response = session.get(file_url, timeout=30)
                        if response.status_code == 403:
                            if attempt < max_retries - 1:
                                local_logger.warning(f"      ⚠️ Got 403 Forbidden (attempt {attempt + 1}/{max_retries}), waiting {retry_delay}s...")
                                print(f"      ⚠️ Got 403 Forbidden (attempt {attempt + 1}/{max_retries}), waiting {retry_delay}s...", flush=True)
                                time.sleep(retry_delay)
                                retry_delay *= 2
                                continue
                            else:
                                local_logger.error(f"      ❌ Got 403 Forbidden after {max_retries} attempts")
                                print(f"      ❌ Got 403 Forbidden after {max_retries} attempts", flush=True)
                                response.raise_for_status()
                        else:
                            break
                    except requests.exceptions.RequestException as e:
                        if attempt < max_retries - 1:
                            local_logger.warning(f"      ⚠️ Request error (attempt {attempt + 1}/{max_retries}): {e}, retrying...")
                            print(f"      ⚠️ Request error (attempt {attempt + 1}/{max_retries}): {e}, retrying...", flush=True)
                            time.sleep(retry_delay)
                            retry_delay *= 2
                        else:
                            raise
                
                download_duration = (datetime.now() - download_start_time).total_seconds()
                
                local_logger.info(f"      📥 RESPONSE: Status={response.status_code}, Size={len(response.content):,} bytes, Time={download_duration:.2f}s")
                print(f"      📥 RESPONSE: Status={response.status_code}, Size={len(response.content):,} bytes, Time={download_duration:.2f}s", flush=True)
                
                if len(response.content) > 0:
                    content_preview = response.content[:200].decode('utf-8', errors='ignore')
                    local_logger.info(f"      📄 Content Preview (first 200 chars): {content_preview}")
                    print(f"      📄 Content Preview (first 200 chars): {content_preview}", flush=True)
                
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
                                    # Small delay before download
                                    time.sleep(0.1)
                                    
                                    # Retry logic for 403 errors
                                    max_retries = 2
                                    retry_delay = 1
                                    doc_response = None
                                    
                                    for attempt in range(max_retries):
                                        try:
                                            doc_response = session.get(doc_link, timeout=30)
                                            if doc_response.status_code == 403:
                                                if attempt < max_retries - 1:
                                                    local_logger.warning(f"      ⚠️ Got 403 Forbidden (attempt {attempt + 1}/{max_retries}), waiting {retry_delay}s...")
                                                    time.sleep(retry_delay)
                                                    retry_delay *= 2
                                                    continue
                                                else:
                                                    local_logger.error(f"      ❌ Got 403 Forbidden after {max_retries} attempts")
                                                    doc_response.raise_for_status()
                                            else:
                                                break
                                        except requests.exceptions.RequestException as e:
                                            if attempt < max_retries - 1:
                                                local_logger.warning(f"      ⚠️ Request error (attempt {attempt + 1}/{max_retries}): {e}, retrying...")
                                                time.sleep(retry_delay)
                                                retry_delay *= 2
                                            else:
                                                raise
                                    
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
                                    # Small delay before download
                                    time.sleep(0.1)
                                    
                                    # Retry logic for 403 errors
                                    max_retries = 2
                                    retry_delay = 1
                                    doc_response = None
                                    
                                    for attempt in range(max_retries):
                                        try:
                                            doc_response = session.get(doc_url, timeout=30)
                                            if doc_response.status_code == 403:
                                                if attempt < max_retries - 1:
                                                    local_logger.warning(f"      ⚠️ Got 403 Forbidden (attempt {attempt + 1}/{max_retries}), waiting {retry_delay}s...")
                                                    time.sleep(retry_delay)
                                                    retry_delay *= 2
                                                    continue
                                                else:
                                                    local_logger.error(f"      ❌ Got 403 Forbidden after {max_retries} attempts")
                                                    doc_response.raise_for_status()
                                            else:
                                                break
                                        except requests.exceptions.RequestException as e:
                                            if attempt < max_retries - 1:
                                                local_logger.warning(f"      ⚠️ Request error (attempt {attempt + 1}/{max_retries}): {e}, retrying...")
                                                time.sleep(retry_delay)
                                                retry_delay *= 2
                                            else:
                                                raise
                                    
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
                    failed_attempts.append(f"{file_url} (HTTP {response.status_code})")
                    local_logger.warning(f"      ⚠️ HTTP {response.status_code} for {file_url}, trying next option...")
                    
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
            for attempt in failed_attempts:
                local_logger.error(f"         - {attempt}")
                print(f"         - {attempt}", flush=True)
            local_logger.error(f"      " + "="*70)
            print(f"      " + "="*70, flush=True)
            return None
        
        # Convert relative URLs to absolute URLs in HTML files
        # This ensures links work when viewing the file locally or from S3
        if file_ext == 'html' and file_content:
            try:
                html_text = file_content.decode('utf-8', errors='ignore')
                
                # Convert relative URLs to absolute URLs (only if not already absolute)
                # Pattern: href="/..." -> href="https://www.sec.gov/..." (but not if already https://)
                # This handles all relative URLs starting with "/"
                def convert_relative_url(match):
                    url = match.group(1)
                    # Only convert if it's a relative URL (starts with /) and not already absolute
                    if url.startswith('/') and not url.startswith('http'):
                        return f'href="https://www.sec.gov{url}"'
                    return match.group(0)  # Return original if already absolute
                
                html_text = re.sub(
                    r'href="([^"]*)"',
                    convert_relative_url,
                    html_text,
                    flags=re.IGNORECASE
                )
                
                # Re-encode to bytes
                file_content = html_text.encode('utf-8')
                
                local_logger.info(f"      🔗 Converted relative URLs to absolute URLs in HTML")
                print(f"      🔗 Converted relative URLs to absolute URLs in HTML", flush=True)
            except Exception as url_error:
                local_logger.warning(f"      ⚠️ Could not convert URLs in HTML: {url_error}")
                print(f"      ⚠️ Could not convert URLs in HTML: {url_error}", flush=True)
                # Continue with original content if conversion fails
        
        # Generate S3 key - include accession number to ensure uniqueness
        # Format: trades/{date}/sec/{form_type}-{cik}-{accession}-{date}.{ext}
        # Accession is already in dashed format (e.g., 0001140361-25-040858)
        s3_key = f"trades/{target_date}/sec/{form_type}-{cik}-{accession_dashed}-{target_date}.{file_ext}"
        
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
            local_logger.error(f"         Bucket: {s3_bucket_name}")
            local_logger.error(f"         Key: {s3_key}")
            local_logger.error(f"         Size: {len(file_content):,} bytes")
            print(f"      ❌ S3 UPLOAD FAILED: {error_type}: {error_msg}", flush=True)
            print(f"         Bucket: {s3_bucket_name}", flush=True)
            print(f"         Key: {s3_key}", flush=True)
            print(f"         Size: {len(file_content):,} bytes", flush=True)
            raise  # Re-raise to be caught by outer exception handler
        
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
        # Pattern: <a href="...cgi-bin/browse-edgar...CIK=...">Name</a>
        # Handle both relative and absolute URLs
        name_match = re.search(r'<a[^>]*href="[^"]*cgi-bin/browse-edgar[^"]*CIK=\d+[^"]*">([^<]+)</a>', html_content, re.IGNORECASE)
        if name_match:
            result['name'] = unescape(name_match.group(1)).strip().lower()
        
        # Extract address (Street, City, State, Zip)
        # HTML structure: 
        # 1. Street lines: <table><tr><td><span class="FormData">LINE1</span></td></tr><tr><td><span class="FormData">LINE2</span></td></tr></table>
        # 2. Then <hr> and (Street) label
        # 3. City/State/Zip: <table><tr><td><span>SIOUX FALLS</span></td><td><span>SD</span></td><td><span>57104</span></td></tr></table>
        address_parts = []
        
        # Extract all street lines from the table before (Street) label
        # Pattern: Find the table before (Street) label, extract all FormData spans
        street_table_pattern = r'<table[^>]*border="0"[^>]*width="100%"[^>]*>(.*?)</table>[^<]*(?:<hr|\(Street\))'
        street_table_match = re.search(street_table_pattern, html_content, re.IGNORECASE | re.DOTALL)
        if street_table_match:
            street_table_content = street_table_match.group(1)
            # Extract all street lines (each in a <tr><td><span class="FormData">...</span></td></tr>)
            street_lines = re.findall(r'<tr><td><span[^>]*class="FormData"[^>]*>([^<]+)</span></td></tr>', street_table_content, re.IGNORECASE | re.DOTALL)
            for street_line in street_lines:
                street = unescape(street_line).strip()
                if street:
                    address_parts.append(street)
        
        # City, State, Zip - they're in a table row after (City) (State) (Zip) labels
        # Pattern: (City) ... (State) ... (Zip) ... <table><tr><td><span>SIOUX FALLS</span></td><td><span>SD</span></td><td><span>57104</span></td></tr></table>
        city_state_zip_pattern = r'\(City\)[^<]*\(State\)[^<]*\(Zip\)[^<]*<table[^>]*>.*?<tr>.*?<td[^>]*><span[^>]*class="FormData"[^>]*>([^<]+)</span></td>.*?<td[^>]*><span[^>]*class="FormData"[^>]*>([^<]+)</span></td>.*?<td[^>]*><span[^>]*class="FormData"[^>]*>([^<]+)</span></td>.*?</tr>'
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
        # Form 3: "Date of Event Requiring Statement"
        # Form 4: "Date of Earliest Transaction"
        # Form 5: "Statement for Issuer's Fiscal Year Ended"
        # Note: There may be <br> tags between the label and the date
        if is_form3:
            event_date_match = re.search(r'Date of Event Requiring Statement[^<]*(?:<br[^>]*>)?[^<]*<span[^>]*class="FormData"[^>]*>(\d{1,2}/\d{1,2}/\d{4})</span>', html_content, re.IGNORECASE | re.DOTALL)
            if event_date_match:
                try:
                    date_str = event_date_match.group(1)
                    date_obj = datetime.strptime(date_str, '%m/%d/%Y')
                    result['eventDate'] = date_obj.strftime('%Y-%m-%d')
                except:
                    pass
        elif is_form4:
            # Form 4 uses "Date of Earliest Transaction"
            event_date_match = re.search(r'Date of Earliest Transaction[^<]*(?:<br[^>]*>)?[^<]*<span[^>]*class="FormData"[^>]*>(\d{1,2}/\d{1,2}/\d{4})</span>', html_content, re.IGNORECASE | re.DOTALL)
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
        # HTML structure: "3. Issuer Name <b>and</b> Ticker or Trading Symbol</span><br><a>Name</a>"
        # Pattern: Look for Issuer Name, then </span>, then <br>, then <a>Name</a>
        issuer_match = re.search(
            r'Issuer Name[^<]*<b>and</b>[^<]*Ticker[^<]*</span>[^<]*<br[^>]*>[^<]*<a[^>]*>([^<]+)</a>',
            html_content,
            re.IGNORECASE | re.DOTALL
        )
        if not issuer_match:
            # Fallback: try without </span> requirement
            issuer_match = re.search(
                r'Issuer Name[^<]*<b>and</b>[^<]*Ticker[^<]*<br[^>]*>[^<]*<a[^>]*>([^<]+)</a>',
                html_content,
                re.IGNORECASE | re.DOTALL
            )
        if not issuer_match:
            # Fallback: try without the "and Ticker" part but with </span>
            issuer_match = re.search(
                r'Issuer Name[^<]*</span>[^<]*<br[^>]*>[^<]*<a[^>]*>([^<]+)</a>',
                html_content,
                re.IGNORECASE | re.DOTALL
            )
        if not issuer_match:
            # Fallback: try without </span> and "and Ticker" but still look for <br> before <a>
            issuer_match = re.search(
                r'Issuer Name[^<]*<br[^>]*>[^<]*<a[^>]*>([^<]+)</a>',
                html_content,
                re.IGNORECASE | re.DOTALL
            )
        if not issuer_match:
            # Final fallback: try without <br> requirement
            issuer_match = re.search(
                r'Issuer Name[^<]*<a[^>]*>([^<]+)</a>',
                html_content,
                re.IGNORECASE | re.DOTALL
            )
        if issuer_match:
            result['issuerName'] = unescape(issuer_match.group(1)).strip().lower()
            local_logger.info(f"   ✅ Extracted issuerName: {result['issuerName']}")
        else:
            local_logger.warning(f"   ⚠️ Could not extract issuerName from HTML")
        
        ticker_match = re.search(r'\[ <span[^>]*class="FormData"[^>]*>([A-Z0-9]+)</span> \]', html_content)
        if ticker_match:
            result['tickerSymbol'] = ticker_match.group(1)
        
        # Extract relationship (4. Relationship of Reporting Person(s) to Issuer)
        # HTML structure: <td>Director</td><td align="center"><span class="FormData">X</span></td>
        # The X comes AFTER the relationship type text
        relationship_types = []
        relationship_additional = None
        
        # Pattern: Look for relationship type text followed by <td> with "X"
        # Director: <td>Director</td><td><span>X</span></td>
        director_match = re.search(r'<td[^>]*class="MedSmallFormText"[^>]*>Director</td>[^<]*<td[^>]*align="center"[^>]*><span[^>]*class="FormData"[^>]*>X</span></td>', html_content, re.IGNORECASE | re.DOTALL)
        if director_match:
            relationship_types.append('Director')
        
        # Officer: <td>Officer</td><td><span>X</span></td>
        officer_match = re.search(r'<td[^>]*class="MedSmallFormText"[^>]*>Officer[^<]*</td>[^<]*<td[^>]*align="center"[^>]*><span[^>]*class="FormData"[^>]*>X</span></td>', html_content, re.IGNORECASE | re.DOTALL)
        if officer_match:
            relationship_types.append('Officer')
            # Extract additional text (title below) - look for the next row with blue text
            officer_text_match = re.search(r'Officer[^<]*</td>[^<]*</tr>[^<]*<tr>[^<]*<td[^>]*style="color: blue"[^>]*>([^<]+)</td>', html_content, re.IGNORECASE | re.DOTALL)
            if officer_text_match:
                relationship_additional = unescape(officer_text_match.group(1)).strip()
        
        # 10% Owner: <td>10% Owner</td><td><span>X</span></td>
        owner_match = re.search(r'<td[^>]*class="MedSmallFormText"[^>]*>10% Owner</td>[^<]*<td[^>]*align="center"[^>]*><span[^>]*class="FormData"[^>]*>X</span></td>', html_content, re.IGNORECASE | re.DOTALL)
        if owner_match:
            relationship_types.append('10% Owner')
        
        # Other: <td>Other</td><td><span>X</span></td>
        other_match = re.search(r'<td[^>]*class="MedSmallFormText"[^>]*>Other[^<]*</td>[^<]*<td[^>]*align="center"[^>]*><span[^>]*class="FormData"[^>]*>X</span></td>', html_content, re.IGNORECASE | re.DOTALL)
        if other_match:
            relationship_types.append('Other')
            # Extract additional text - look for the next row with blue text
            other_text_match = re.search(r'Other[^<]*</td>[^<]*</tr>[^<]*<tr>[^<]*<td[^>]*style="color: blue"[^>]*>([^<]+)</td>', html_content, re.IGNORECASE | re.DOTALL)
            if other_text_match:
                other_text = unescape(other_text_match.group(1)).strip()
                if other_text:
                    relationship_additional = relationship_additional + '; ' + other_text if relationship_additional else other_text
        
        result['relationship'] = ', '.join(relationship_types) if relationship_types else None
        result['relationshipAdditionalText'] = relationship_additional if relationship_additional else None
        
        # Extract signature name
        # Pattern: <u><span class="FormData">Signature text</span></u> or "See Exhibit 99.1 for Signature"
        signature_match = re.search(r'<u><span[^>]*class="FormData"[^>]*>(/s/|s/)?\s*([^<]+)</span></u>', html_content, re.IGNORECASE)
        if signature_match:
            signature_name = unescape(signature_match.group(2)).strip()
            # Remove /s/ or s/ prefix if present
            signature_name = re.sub(r'^[/]?s[/]\s*', '', signature_name, flags=re.IGNORECASE)
            result['signatureName'] = signature_name.lower()
        else:
            # Fallback: try to find signature in the signature section
            # Pattern: ** Signature of Reporting Person ... <u><span>text</span></u>
            signature_fallback = re.search(r'\*\* Signature[^<]*<u><span[^>]*class="FormData"[^>]*>([^<]+)</span></u>', html_content, re.IGNORECASE | re.DOTALL)
            if signature_fallback:
                signature_name = unescape(signature_fallback.group(1)).strip()
                signature_name = re.sub(r'^[/]?s[/]\s*', '', signature_name, flags=re.IGNORECASE)
                result['signatureName'] = signature_name.lower()
        
        # Check for amendment
        # Forms 3/4/5: "4. If Amendment, Date of Original Filed"
        amendment_date_match = re.search(r'If Amendment, Date of Original Filed[^<]*<span[^>]*class="FormData"[^>]*>(\d{1,2}/\d{1,2}/\d{4})</span>', html_content, re.IGNORECASE)
        if amendment_date_match:
            result['amendment'] = True
        
        # Parse explanations first (needed for table parsing)
        explanations_dict = parse_explanations(html_content)
        explanations_msg = f"📝 Parsed {len(explanations_dict)} explanations: {list(explanations_dict.keys())}"
        logger.info(explanations_msg)
        print(explanations_msg, flush=True)
        if explanations_dict:
            for num, text in list(explanations_dict.items())[:2]:  # Log first 2
                explanation_preview = f"   Explanation {num}: {text[:100]}..."
                logger.info(explanation_preview)
                print(explanation_preview, flush=True)
        else:
            no_explanations_msg = "⚠️ No explanations found in document"
            logger.warning(no_explanations_msg)
            print(no_explanations_msg, flush=True)
        
        # Parse Table I - Non-Derivative Securities (pass explanations for footnote embedding)
        table1_data = parse_table_i(html_content, is_form3, is_form4, is_form5, explanations_dict)
        result['nonDerivativeSecurities'] = table1_data
        
        # Parse Table II - Derivative Securities (pass explanations for footnote embedding)
        table2_data = parse_table_ii(html_content, is_form3, is_form4, is_form5, explanations_dict)
        result['derivativeSecurities'] = table2_data
        
        # Parse remarks only (explanations are now embedded in table rows)
        misc_data = parse_remarks(html_content)
        result['misc'] = misc_data
        
    except Exception as e:
        local_logger.error(f"❌ Error parsing form metadata: {e}")
        import traceback
        local_logger.error(f"   Traceback: {traceback.format_exc()}")
    
    return result


def parse_table_i(html_content: str, is_form3: bool, is_form4: bool, is_form5: bool, explanations_dict: Dict[str, str] = None) -> List[Dict[str, Any]]:
    """Parse Table I - Non-Derivative Securities"""
    # Import inside function to avoid serialization issues
    from html import unescape
    
    table_data = []
    
    try:
        # Find Table I tbody
        # Pattern must allow HTML tags between "Table I" and <tbody> (e.g., <thead> section)
        table1_pattern = r'Table I.*?<tbody>(.*?)</tbody>'
        table1_match = re.search(table1_pattern, html_content, re.IGNORECASE | re.DOTALL)
        
        if not table1_match:
            import logging
            local_logger = logging.getLogger()
            local_logger.warning(f"⚠️ Table I tbody not found in HTML")
            return table_data
        
        tbody_content = table1_match.group(1)
        rows = re.findall(r'<tr[^>]*>(.*?)</tr>', tbody_content, re.DOTALL | re.IGNORECASE)
        
        import logging
        local_logger = logging.getLogger()
        local_logger.info(f"   📊 Table I: Found {len(rows)} rows in tbody")
        print(f"   📊 Table I: Found {len(rows)} rows in tbody", flush=True)
        
        def extract_footnote(cell):
            """Extract footnote number from a cell (e.g., <sup>(1)</sup> -> 1, or None if no footnote)"""
            # Pattern: <sup>(1)</sup> or <sup>(2)</sup> etc.
            footnote_match = re.search(r'<sup>\((\d+)\)</sup>', cell, re.IGNORECASE)
            if footnote_match:
                try:
                    return int(footnote_match.group(1))
                except:
                    pass
            return None
        
        def clean_cell(cell, remove_footnote=True):
            """Clean cell text, optionally removing footnote HTML first"""
            # Remove footnote HTML before cleaning to avoid including "(1)" in the value
            if remove_footnote:
                # Remove entire FootnoteData spans (may contain nested <sup> tags)
                # Pattern: <span class="FootnoteData">...<sup>(1)</sup>...</span>
                cell = re.sub(r'<span[^>]*class="[^"]*FootnoteData[^"]*"[^>]*>.*?</span>', '', cell, flags=re.IGNORECASE | re.DOTALL)
                # Also remove standalone <sup>(1)</sup> tags that might not be in FootnoteData spans
                cell = re.sub(r'<sup>\(\d+\)</sup>', '', cell, flags=re.IGNORECASE)
            # Now remove all remaining HTML tags
            text = re.sub(r'<[^>]+>', '', cell)
            text = unescape(text)
            return text.strip()
        
        def create_field_value(cell, explanations_dict):
            """Create field value - either string or object with value, footnote number, and explanation"""
            footnote_num = extract_footnote(cell)
            # Clean cell and remove footnote HTML so value doesn't include "(1)" text
            value = clean_cell(cell, remove_footnote=True)
            
            if footnote_num is not None:
                # Field has a footnote - create object structure
                explanation_text = ""
                if explanations_dict:
                    # Try both string and int keys
                    explanation_text = explanations_dict.get(str(footnote_num), "") or explanations_dict.get(int(footnote_num), "")
                    if not explanation_text:
                        # Debug: log when explanation is missing
                        missing_msg = f"⚠️ Footnote {footnote_num} found in Table I cell but no explanation in dict."
                        local_logger.warning(missing_msg)
                        print(missing_msg, flush=True)
                        detail_msg = f"   Footnote num type: {type(footnote_num)}, value: {footnote_num}"
                        local_logger.warning(detail_msg)
                        print(detail_msg, flush=True)
                        keys_msg = f"   Available keys: {list(explanations_dict.keys())}"
                        local_logger.warning(keys_msg)
                        print(keys_msg, flush=True)
                        key_types_msg = f"   Key types: {[type(k) for k in explanations_dict.keys()]}"
                        local_logger.warning(key_types_msg)
                        print(key_types_msg, flush=True)
                        lookup_msg = f"   Looking for: str({footnote_num})={str(footnote_num)}, int({footnote_num})={int(footnote_num)}"
                        local_logger.warning(lookup_msg)
                        print(lookup_msg, flush=True)
                else:
                    empty_dict_msg = f"⚠️ Footnote {footnote_num} found but explanations_dict is None or empty"
                    local_logger.warning(empty_dict_msg)
                    print(empty_dict_msg, flush=True)
                
                footnote_obj = {
                    "value": value,
                    "footnote": {
                        "number": footnote_num,
                        "explanation": explanation_text
                    }
                }
                return footnote_obj
            else:
                # No footnote - just return the string value
                return value
        
        for row in rows:
            cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL | re.IGNORECASE)
            
            if is_form3 and len(cells) >= 4:
                # Form 3: Title | Amount | Ownership Form | Nature of Indirect
                row_data = {
                    'titleOfSecurity': create_field_value(cells[0], explanations_dict) if len(cells) > 0 else '',
                    'amountOfSecurities': create_field_value(cells[1], explanations_dict) if len(cells) > 1 else '',
                    'ownershipForm': create_field_value(cells[2], explanations_dict) if len(cells) > 2 else '',
                    'natureOfIndirectBeneficialOwnership': create_field_value(cells[3], explanations_dict) if len(cells) > 3 else ''
                }
                table_data.append(row_data)
            elif (is_form4 or is_form5) and len(cells) >= 8:
                # Form 4/5: Title | Transaction Date | ... | Amount | (A) or (D) | Price | ...
                row_data = {
                    'titleOfSecurity': create_field_value(cells[0], explanations_dict) if len(cells) > 0 else '',
                    'transactionDate': create_field_value(cells[1], explanations_dict) if len(cells) > 1 else '',
                    'deemedExecutionDate': create_field_value(cells[2], explanations_dict) if len(cells) > 2 else '',
                    'transactionCode': create_field_value(cells[3], explanations_dict) if len(cells) > 3 else '',
                    'transactionCodeV': create_field_value(cells[4], explanations_dict) if len(cells) > 4 else '',
                    'amount': create_field_value(cells[5], explanations_dict) if len(cells) > 5 else '',
                    'acquiredOrDisposed': create_field_value(cells[6], explanations_dict) if len(cells) > 6 else '',
                    'price': create_field_value(cells[7], explanations_dict) if len(cells) > 7 else ''
                }
                # Add remaining columns if present
                if len(cells) > 8:
                    row_data['amountOfSecuritiesBeneficiallyOwned'] = create_field_value(cells[8], explanations_dict)
                if len(cells) > 9:
                    row_data['ownershipForm'] = create_field_value(cells[9], explanations_dict)
                if len(cells) > 10:
                    row_data['natureOfIndirectBeneficialOwnership'] = create_field_value(cells[10], explanations_dict)
                table_data.append(row_data)
    
    except Exception as e:
        import logging
        local_logger = logging.getLogger()
        local_logger.error(f"❌ Error parsing Table I: {e}")
    
    return table_data


def parse_table_ii(html_content: str, is_form3: bool, is_form4: bool, is_form5: bool, explanations_dict: Dict[str, str] = None) -> List[Dict[str, Any]]:
    """Parse Table II - Derivative Securities"""
    # Import inside function to avoid serialization issues
    from html import unescape
    
    table_data = []
    
    try:
        # Find Table II tbody
        # Pattern must allow HTML tags between "Table II" and <tbody> (e.g., <thead> section)
        table2_pattern = r'Table II.*?<tbody>(.*?)</tbody>'
        table2_match = re.search(table2_pattern, html_content, re.IGNORECASE | re.DOTALL)
        
        if not table2_match:
            import logging
            local_logger = logging.getLogger()
            local_logger.warning(f"⚠️ Table II tbody not found in HTML")
            return table_data
        
        tbody_content = table2_match.group(1)
        rows = re.findall(r'<tr[^>]*>(.*?)</tr>', tbody_content, re.DOTALL | re.IGNORECASE)
        
        import logging
        local_logger = logging.getLogger()
        local_logger.info(f"   📊 Table II: Found {len(rows)} rows in tbody")
        print(f"   📊 Table II: Found {len(rows)} rows in tbody", flush=True)
        
        def extract_footnote(cell):
            """Extract footnote number from a cell (e.g., <sup>(1)</sup> -> 1, or None if no footnote)"""
            # Pattern: <sup>(1)</sup> or <sup>(2)</sup> etc.
            footnote_match = re.search(r'<sup>\((\d+)\)</sup>', cell, re.IGNORECASE)
            if footnote_match:
                try:
                    return int(footnote_match.group(1))
                except:
                    pass
            return None
        
        def clean_cell(cell, remove_footnote=True):
            """Clean cell text, optionally removing footnote HTML first"""
            # Remove footnote HTML before cleaning to avoid including "(1)" in the value
            if remove_footnote:
                # Remove entire FootnoteData spans (may contain nested <sup> tags)
                # Pattern: <span class="FootnoteData">...<sup>(1)</sup>...</span>
                cell = re.sub(r'<span[^>]*class="[^"]*FootnoteData[^"]*"[^>]*>.*?</span>', '', cell, flags=re.IGNORECASE | re.DOTALL)
                # Also remove standalone <sup>(1)</sup> tags that might not be in FootnoteData spans
                cell = re.sub(r'<sup>\(\d+\)</sup>', '', cell, flags=re.IGNORECASE)
            # Now remove all remaining HTML tags
            text = re.sub(r'<[^>]+>', '', cell)
            text = unescape(text)
            return text.strip()
        
        def create_field_value(cell, explanations_dict):
            """Create field value - either string or object with value, footnote number, and explanation"""
            footnote_num = extract_footnote(cell)
            # Clean cell and remove footnote HTML so value doesn't include "(1)" text
            value = clean_cell(cell, remove_footnote=True)
            
            if footnote_num is not None:
                # Field has a footnote - create object structure
                explanation_text = ""
                if explanations_dict:
                    # Try both string and int keys
                    explanation_text = explanations_dict.get(str(footnote_num), "") or explanations_dict.get(int(footnote_num), "")
                    if not explanation_text:
                        # Debug: log when explanation is missing
                        missing_msg = f"⚠️ Footnote {footnote_num} found in Table II cell but no explanation in dict."
                        local_logger.warning(missing_msg)
                        print(missing_msg, flush=True)
                        detail_msg = f"   Footnote num type: {type(footnote_num)}, value: {footnote_num}"
                        local_logger.warning(detail_msg)
                        print(detail_msg, flush=True)
                        keys_msg = f"   Available keys: {list(explanations_dict.keys())}"
                        local_logger.warning(keys_msg)
                        print(keys_msg, flush=True)
                        key_types_msg = f"   Key types: {[type(k) for k in explanations_dict.keys()]}"
                        local_logger.warning(key_types_msg)
                        print(key_types_msg, flush=True)
                        lookup_msg = f"   Looking for: str({footnote_num})={str(footnote_num)}, int({footnote_num})={int(footnote_num)}"
                        local_logger.warning(lookup_msg)
                        print(lookup_msg, flush=True)
                else:
                    empty_dict_msg = f"⚠️ Footnote {footnote_num} found but explanations_dict is None or empty"
                    local_logger.warning(empty_dict_msg)
                    print(empty_dict_msg, flush=True)
                
                footnote_obj = {
                    "value": value,
                    "footnote": {
                        "number": footnote_num,
                        "explanation": explanation_text
                    }
                }
                return footnote_obj
            else:
                # No footnote - just return the string value
                return value
        
        for row in rows:
            cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL | re.IGNORECASE)
            
            if is_form3 and len(cells) >= 6:
                # Form 3: Title | Date Exercisable | Expiration Date | Title | Amount | Conversion Price | Ownership | Nature
                # Note: Form 3 has 8 columns, but we need at least 6 to parse basic info
                row_data = {
                    'titleOfDerivativeSecurity': create_field_value(cells[0], explanations_dict) if len(cells) > 0 else '',
                    'dateExercisable': create_field_value(cells[1], explanations_dict) if len(cells) > 1 else '',
                    'expirationDate': create_field_value(cells[2], explanations_dict) if len(cells) > 2 else '',
                    'titleOfUnderlyingSecurity': create_field_value(cells[3], explanations_dict) if len(cells) > 3 else '',
                    'amountOrNumberOfShares': create_field_value(cells[4], explanations_dict) if len(cells) > 4 else '',
                    'conversionOrExercisePrice': create_field_value(cells[5], explanations_dict) if len(cells) > 5 else '',
                    'ownershipForm': create_field_value(cells[6], explanations_dict) if len(cells) > 6 else '',
                    'natureOfIndirectBeneficialOwnership': create_field_value(cells[7], explanations_dict) if len(cells) > 7 else ''
                }
                # Only add if we have at least the title (check if it's a dict or string)
                title_value = row_data['titleOfDerivativeSecurity']
                if isinstance(title_value, dict):
                    title_str = title_value.get('value', '')
                else:
                    title_str = str(title_value)
                if title_str:
                    table_data.append(row_data)
            elif (is_form4 or is_form5) and len(cells) >= 10:
                # Form 4/5: Title | Conversion Price | Transaction Date | ... | (A) | (D) | Date Exercisable | Expiration | Title | Amount | Price | ...
                row_data = {
                    'titleOfDerivativeSecurity': create_field_value(cells[0], explanations_dict) if len(cells) > 0 else '',
                    'conversionOrExercisePrice': create_field_value(cells[1], explanations_dict) if len(cells) > 1 else '',
                    'transactionDate': create_field_value(cells[2], explanations_dict) if len(cells) > 2 else '',
                    'deemedExecutionDate': create_field_value(cells[3], explanations_dict) if len(cells) > 3 else '',
                    'transactionCode': create_field_value(cells[4], explanations_dict) if len(cells) > 4 else '',
                    'transactionCodeV': create_field_value(cells[5], explanations_dict) if len(cells) > 5 else '',
                    'acquired': create_field_value(cells[6], explanations_dict) if len(cells) > 6 else '',
                    'disposed': create_field_value(cells[7], explanations_dict) if len(cells) > 7 else '',
                    'dateExercisable': create_field_value(cells[8], explanations_dict) if len(cells) > 8 else '',
                    'expirationDate': create_field_value(cells[9], explanations_dict) if len(cells) > 9 else '',
                    'titleOfUnderlyingSecurity': create_field_value(cells[10], explanations_dict) if len(cells) > 10 else '',
                    'amountOrNumberOfShares': create_field_value(cells[11], explanations_dict) if len(cells) > 11 else '',
                    'priceOfDerivativeSecurity': create_field_value(cells[12], explanations_dict) if len(cells) > 12 else ''
                }
                # Add remaining columns if present
                if len(cells) > 13:
                    row_data['numberOfDerivativeSecuritiesBeneficiallyOwned'] = create_field_value(cells[13], explanations_dict)
                if len(cells) > 14:
                    row_data['ownershipForm'] = create_field_value(cells[14], explanations_dict)
                if len(cells) > 15:
                    row_data['natureOfIndirectBeneficialOwnership'] = create_field_value(cells[15], explanations_dict)
                table_data.append(row_data)
    
    except Exception as e:
        import logging
        local_logger = logging.getLogger()
        local_logger.error(f"❌ Error parsing Table II: {e}")
    
    return table_data


def parse_explanations(html_content: str) -> Dict[str, str]:
    """
    Parse numbered explanations from "Explanation of Responses" section.
    
    Returns:
        Dict with numbered explanation keys (e.g., "1", "2") mapping to explanation text.
        Example: {"1": "Explanation text for footnote (1)", "2": "Explanation text for footnote (2)"}
    
    Note: These explanations are embedded directly in table row fields that have footnotes.
          This function is separate from parse_remarks() to allow explanations to be passed to table parsers.
    """
    # Import inside function to avoid serialization issues
    from html import unescape
    import logging
    local_logger = logging.getLogger()
    
    explanations = {}
    
    try:
        # Find "Explanation of Responses" section
        # Pattern: Look for the header row, then capture all following rows until we hit Remarks or end of table
        # The section ends when we hit <b>Remarks:</b> or </table> or </body>
        # More flexible pattern that handles various HTML structures
        explanation_section = re.search(
            r'Explanation of Responses[^<]*</td>[^<]*</tr>(.*?)(?=<tr><td[^>]*><b>Remarks|</table>|</body>)',
            html_content,
            re.IGNORECASE | re.DOTALL
        )
        
        # If that doesn't work, try a simpler pattern
        if not explanation_section:
            explanation_section = re.search(
                r'Explanation of Responses.*?</tr>(.*?)(?=<b>Remarks|</table>|</body>)',
                html_content,
                re.IGNORECASE | re.DOTALL
            )
        
        if explanation_section:
            explanation_text = explanation_section.group(1)
            section_found_msg = f"🔍 Found Explanation section, length: {len(explanation_text)} chars"
            local_logger.info(section_found_msg)
            print(section_found_msg, flush=True)
            
            # Extract numbered explanations from FootnoteData cells
            # Pattern: Look for <tr><td class="FootnoteData">1. ...</td></tr>
            # The text can be very long, so we need to capture everything until </td>
            # Use a more robust pattern that handles the full cell content
            # First try: Match with <tr> wrapper
            footnote_rows = re.findall(
                r'<tr>\s*<td[^>]*class="[^"]*FootnoteData[^"]*"[^>]*>\s*(\d+)\.\s+(.*?)</td>\s*</tr>',
                explanation_text,
                re.IGNORECASE | re.DOTALL
            )
            
            # If primary pattern fails, try without requiring <tr> wrapper
            if not footnote_rows:
                footnote_rows = re.findall(
                    r'<td[^>]*class="[^"]*FootnoteData[^"]*"[^>]*>\s*(\d+)\.\s+(.*?)</td>',
                    explanation_text,
                    re.IGNORECASE | re.DOTALL
                )
            
            # If still no matches, try matching the entire explanation section more flexibly
            if not footnote_rows:
                # Pattern: Look for "1. " or "2. " followed by text until next number or </td> or </tr>
                # This pattern captures text that may span multiple lines
                footnote_rows = re.findall(
                    r'(\d+)\.\s+((?:(?!\d+\.)[^<])+?)(?=\d+\.|</td>|</tr>|$)',
                    explanation_text,
                    re.IGNORECASE | re.DOTALL
                )
            
            rows_found_msg = f"🔍 Found {len(footnote_rows)} footnote rows using primary pattern"
            local_logger.info(rows_found_msg)
            print(rows_found_msg, flush=True)
            
            for num, text in footnote_rows:
                cleaned_text = re.sub(r'<[^>]+>', '', text)  # Remove any remaining HTML tags
                cleaned_text = unescape(cleaned_text).strip()
                if cleaned_text:
                    explanations[num] = cleaned_text
                    parsed_msg = f"✅ Parsed explanation {num}: {cleaned_text[:80]}..."
                    local_logger.info(parsed_msg)
                    print(parsed_msg, flush=True)
                else:
                    empty_msg = f"⚠️ Explanation {num} was empty after cleaning"
                    local_logger.warning(empty_msg)
                    print(empty_msg, flush=True)
            
            # Debug: Print the final explanations dict
            final_dict_msg = f"📝 Final explanations dict: {explanations}"
            local_logger.info(final_dict_msg)
            print(final_dict_msg, flush=True)
            
            # Fallback: If no footnote rows found, try the original pattern
            if not explanations:
                fallback_msg = "⚠️ Primary pattern failed, trying fallback pattern"
                local_logger.warning(fallback_msg)
                print(fallback_msg, flush=True)
                explanation_pattern = r'(\d+)\.\s+([^<\d]+?)(?=\d+\.|$)'
                explanation_matches = re.findall(explanation_pattern, explanation_text, re.DOTALL)
                
                fallback_found_msg = f"🔍 Fallback pattern found {len(explanation_matches)} matches"
                local_logger.info(fallback_found_msg)
                print(fallback_found_msg, flush=True)
                
                for num, text in explanation_matches:
                    cleaned_text = re.sub(r'<[^>]+>', '', text)
                    cleaned_text = unescape(cleaned_text).strip()
                    if cleaned_text:
                        explanations[num] = cleaned_text
                        fallback_parsed_msg = f"✅ Parsed explanation {num} (fallback): {cleaned_text[:80]}..."
                        local_logger.info(fallback_parsed_msg)
                        print(fallback_parsed_msg, flush=True)
        else:
            not_found_msg = "⚠️ Could not find 'Explanation of Responses' section in HTML"
            local_logger.warning(not_found_msg)
            print(not_found_msg, flush=True)
    
    except Exception as e:
        import traceback
        error_msg = f"❌ Error parsing explanations: {e}\n{traceback.format_exc()}"
        local_logger.error(error_msg)
        print(error_msg, flush=True)
    
    return explanations


def get_transaction_code_meaning(code: str) -> str:
    """
    Map SEC transaction codes to human-readable meanings for AI agents.
    Based on SEC Form 4/5 instruction 8.
    """
    code_meanings = {
        'A': 'Grant, award or other acquisition',
        'C': 'Conversion of derivative security',
        'D': 'Disposition to the issuer of issuer equity securities',
        'E': 'Expiration of short derivative position',
        'F': 'Payment of exercise price or tax liability by delivering or withholding securities',
        'G': 'Bona fide gift',
        'H': 'Expiration (or cancellation) of long derivative position with value received',
        'I': 'Discretionary transaction in accordance with Rule 10b5-1',
        'J': 'Other acquisition or disposition',
        'L': 'Small acquisition under Rule 16a-6',
        'M': 'Exercise or conversion of derivative security',
        'O': 'Transaction in equity swap or instrument with similar characteristics',
        'P': 'Open market or private purchase of non-derivative or derivative security',
        'S': 'Open market or private sale of non-derivative or derivative security',
        'U': 'Disposition pursuant to a tender of shares in a change of control transaction',
        'V': 'Transaction voluntarily reported earlier than required',
        'W': 'Acquisition or disposition by will or the laws of descent and distribution',
        'X': 'Exercise of out-of-the-money derivative security',
        'Z': 'Deposit into or withdrawal from voting trust'
    }
    return code_meanings.get(code.upper(), f'Transaction code {code} (meaning not specified)')

def get_acquisition_disposition_meaning(value: str) -> str:
    """Map A/D values to human-readable meanings."""
    meanings = {
        'A': 'Acquired',
        'D': 'Disposed'
    }
    return meanings.get(value.upper(), value)

def transform_to_semantic_structure(parsed_data: Dict[str, Any], form_data: Dict[str, Any], s3_key: str) -> Dict[str, Any]:
    """
    Transform parsed SEC form data into AI-friendly semantic structure.
    
    This creates a self-describing, contextually complete data structure that
    AI agents can reason over without needing external ontologies or cross-references.
    """
    # Extract CIK from form_data
    cik = form_data.get('cik', 'unknown')
    accession = form_data.get('accession_number', 'unknown')
    
    # Build issuer object
    issuer = {
        'name': parsed_data.get('issuerName', '').title() if parsed_data.get('issuerName') else None,
        'ticker': parsed_data.get('tickerSymbol'),
        'cik': cik
    }
    
    # Build reporting person object
    reporting_person = {
        'name': parsed_data.get('name', '').title() if parsed_data.get('name') else None,
        'cik': None,  # Could extract from HTML if needed
        'title': parsed_data.get('relationshipAdditionalText'),
        'relationship_types': parsed_data.get('relationship', '').split(', ') if parsed_data.get('relationship') else []
    }
    
    # Transform non-derivative securities into semantic transactions
    transactions = []
    
    # Process non-derivative securities
    for row in parsed_data.get('nonDerivativeSecurities', []):
        transaction = {
            'date': row.get('transactionDate'),
            'code': row.get('transactionCode'),
            'code_meaning': get_transaction_code_meaning(row.get('transactionCode', '')),
            'acquisition_or_disposition': row.get('acquiredOrDisposed'),
            'acquisition_or_disposition_meaning': get_acquisition_disposition_meaning(row.get('acquiredOrDisposed', '')),
            'shares': row.get('amount'),
            'price': row.get('price'),
            'ownership_type': 'Direct' if row.get('ownershipForm') == 'D' else 'Indirect' if row.get('ownershipForm') == 'I' else row.get('ownershipForm'),
            'is_derivative': False,
            'security_title': row.get('titleOfSecurity'),
            'security_type': 'Equity',
            'footnote': None
        }
        
        # Extract footnote and clean field values
        # Check price field first (most common location for footnotes)
        price_field = row.get('price')
        if isinstance(price_field, dict) and 'footnote' in price_field:
            footnote_obj = price_field.get('footnote', {})
            transaction['footnote'] = {
                'number': footnote_obj.get('number'),
                'text': footnote_obj.get('explanation', '')
            }
            transaction['price'] = price_field.get('value', '')
        elif isinstance(price_field, dict) and 'value' in price_field:
            transaction['price'] = price_field.get('value', '')
        elif not isinstance(price_field, dict):
            transaction['price'] = price_field
        
        # Check other fields for footnotes
        for field_name in ['titleOfSecurity', 'amount', 'transactionCode']:
            field_value = row.get(field_name)
            if isinstance(field_value, dict) and 'footnote' in field_value:
                if not transaction['footnote']:  # Only set if not already set
                    footnote_obj = field_value.get('footnote', {})
                    transaction['footnote'] = {
                        'number': footnote_obj.get('number'),
                        'text': footnote_obj.get('explanation', '')
                    }
                # Clean the field value
                if field_name == 'titleOfSecurity':
                    transaction['security_title'] = field_value.get('value', '')
                elif field_name == 'amount':
                    transaction['shares'] = field_value.get('value', '')
                elif field_name == 'transactionCode':
                    transaction['code'] = field_value.get('value', '')
            elif isinstance(field_value, dict) and 'value' in field_value:
                # Field has value but no footnote
                if field_name == 'titleOfSecurity':
                    transaction['security_title'] = field_value.get('value', '')
                elif field_name == 'amount':
                    transaction['shares'] = field_value.get('value', '')
                elif field_name == 'transactionCode':
                    transaction['code'] = field_value.get('value', '')
        
        transactions.append(transaction)
    
    # Process derivative securities
    for row in parsed_data.get('derivativeSecurities', []):
        transaction = {
            'date': row.get('transactionDate'),
            'code': row.get('transactionCode'),
            'code_meaning': get_transaction_code_meaning(row.get('transactionCode', '')),
            'acquisition_or_disposition': 'A' if row.get('acquired') else 'D' if row.get('disposed') else None,
            'acquisition_or_disposition_meaning': get_acquisition_disposition_meaning('A' if row.get('acquired') else 'D' if row.get('disposed') else ''),
            'shares': row.get('acquired') or row.get('disposed'),
            'price': row.get('priceOfDerivativeSecurity'),
            'ownership_type': 'Direct' if row.get('ownershipForm') == 'D' else 'Indirect' if row.get('ownershipForm') == 'I' else row.get('ownershipForm'),
            'is_derivative': True,
            'security_title': row.get('titleOfDerivativeSecurity'),
            'security_type': 'Derivative',
            'underlying_security': row.get('titleOfUnderlyingSecurity'),
            'underlying_shares': row.get('amountOrNumberOfShares'),
            'exercise_price': row.get('conversionOrExercisePrice'),
            'date_exercisable': row.get('dateExercisable'),
            'expiration_date': row.get('expirationDate'),
            'footnote': None
        }
        
        # Extract footnote from various fields and clean values
        for field_name in ['conversionOrExercisePrice', 'dateExercisable', 'expirationDate', 'priceOfDerivativeSecurity', 'titleOfDerivativeSecurity']:
            field_value = row.get(field_name)
            if isinstance(field_value, dict) and 'footnote' in field_value:
                if not transaction['footnote']:  # Only set if not already set
                    footnote_obj = field_value.get('footnote', {})
                    transaction['footnote'] = {
                        'number': footnote_obj.get('number'),
                        'text': footnote_obj.get('explanation', '')
                    }
                # Clean the field value
                clean_value = field_value.get('value', '')
                if field_name == 'conversionOrExercisePrice':
                    transaction['exercise_price'] = clean_value
                elif field_name == 'dateExercisable':
                    transaction['date_exercisable'] = clean_value
                elif field_name == 'expirationDate':
                    transaction['expiration_date'] = clean_value
                elif field_name == 'priceOfDerivativeSecurity':
                    transaction['price'] = clean_value
                elif field_name == 'titleOfDerivativeSecurity':
                    transaction['security_title'] = clean_value
            elif isinstance(field_value, dict) and 'value' in field_value:
                # Field has value but no footnote
                clean_value = field_value.get('value', '')
                if field_name == 'conversionOrExercisePrice':
                    transaction['exercise_price'] = clean_value
                elif field_name == 'dateExercisable':
                    transaction['date_exercisable'] = clean_value
                elif field_name == 'expirationDate':
                    transaction['expiration_date'] = clean_value
                elif field_name == 'priceOfDerivativeSecurity':
                    transaction['price'] = clean_value
                elif field_name == 'titleOfDerivativeSecurity':
                    transaction['security_title'] = clean_value
        
        transactions.append(transaction)
    
    # Build the semantic structure
    semantic_data = {
        # Keep GSI fields at top level for queryability (with original names for backward compatibility)
        'tradeId': parsed_data.get('tradeId'),
        'formType': parsed_data.get('formType'),  # GSI
        'name': parsed_data.get('name'),  # GSI
        'eventDate': parsed_data.get('eventDate'),  # GSI
        'reportingDate': parsed_data.get('reportingDate'),  # GSI
        'issuerName': parsed_data.get('issuerName'),  # GSI
        'tickerSymbol': parsed_data.get('tickerSymbol'),  # GSI
        'relationship': parsed_data.get('relationship'),  # GSI
        'politician': parsed_data.get('politician', False),
        
        # Semantic structure for AI readability
        'filing_id': f"{cik}-{accession}",
        'filing_date': parsed_data.get('reportingDate'),
        'form_type': parsed_data.get('formType'),
        'form_s3_key': s3_key,
        'source_url': f"https://www.sec.gov/cgi-bin/viewer?action=view&cik={cik}&accession_number={accession}&xbrl_type=v",
        
        # Nested objects for context
        'issuer': issuer,
        'reporting_person': reporting_person,
        
        # Transactions as atomic units
        'transactions': transactions,
        
        # Additional metadata
        'address': parsed_data.get('address'),
        'signature_name': parsed_data.get('signatureName'),
        'amended': parsed_data.get('amended', False),
        'amendment': parsed_data.get('amendment', False),
        'amended_trade_id': parsed_data.get('amendedTradeId'),
        'misc': parsed_data.get('misc', {}),
        
        # Keep raw table data for reference (optional - can be removed if not needed)
        'raw_non_derivative_securities': parsed_data.get('nonDerivativeSecurities', []),
        'raw_derivative_securities': parsed_data.get('derivativeSecurities', [])
    }
    
    return semantic_data


def parse_remarks(html_content: str) -> Dict[str, Any]:
    """
    Parse remarks section only (explanations are now embedded in table rows).
    
    Returns:
        Dict with optional "remarks" key containing remarks text.
        Example: {"remarks": "Exhibit 99.1 (Signatures and Joint Filer Information) is incorporated herein by reference."}
    """
    # Import inside function to avoid serialization issues
    from html import unescape
    
    misc = {}
    
    try:
        # Find "Remarks" section
        # Pattern: <b>Remarks:</b> ... <td class="FootnoteData">...</td>
        remarks_match = re.search(
            r'<b>Remarks:</b>[^<]*</td>[^<]*</tr>[^<]*<tr><td[^>]*class="[^"]*FootnoteData[^"]*"[^>]*>(.*?)</td>',
            html_content,
            re.IGNORECASE | re.DOTALL
        )
        if remarks_match:
            remarks_text = re.sub(r'<[^>]+>', '', remarks_match.group(1))  # Remove HTML tags
            remarks_text = unescape(remarks_text).strip()
            if remarks_text:
                misc['remarks'] = remarks_text
    
    except Exception as e:
        import logging
        local_logger = logging.getLogger()
        local_logger.error(f"❌ Error parsing remarks: {e}")
    
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
    downloaded = download_sec_form(form_data, target_date, s3_bucket_name)
    download_duration = (datetime.now() - download_start).total_seconds()
    
    print(f"   🔵 download_sec_form returned: {type(downloaded).__name__}", flush=True)
    if downloaded:
        print(f"   🔵 download_sec_form returned dict with keys: {list(downloaded.keys()) if isinstance(downloaded, dict) else 'N/A'}", flush=True)
    else:
        print(f"   🔵 download_sec_form returned None or False", flush=True)
    
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
    local_logger.info(f"         - Address: {parsed_data.get('address', 'N/A')} {'⚠️ MISSING' if not parsed_data.get('address') else '✅'}")
    local_logger.info(f"         - Issuer: {parsed_data.get('issuerName', 'N/A')} {'⚠️ MISSING' if not parsed_data.get('issuerName') else '✅'}")
    local_logger.info(f"         - Ticker: {parsed_data.get('tickerSymbol', 'N/A')}")
    local_logger.info(f"         - Relationship: {parsed_data.get('relationship', 'N/A')} {'⚠️ MISSING' if not parsed_data.get('relationship') else '✅'}")
    local_logger.info(f"         - Relationship Additional: {parsed_data.get('relationshipAdditionalText', 'N/A')}")
    local_logger.info(f"         - Event Date: {parsed_data.get('eventDate', 'N/A')} {'⚠️ MISSING' if not parsed_data.get('eventDate') else '✅'}")
    local_logger.info(f"         - Reporting Date: {parsed_data.get('reportingDate', 'N/A')}")
    local_logger.info(f"         - Signature Name: {parsed_data.get('signatureName', 'N/A')}")
    local_logger.info(f"         - Amendment: {parsed_data.get('amendment', False)}")
    local_logger.info(f"         - Table I rows (Non-Derivative): {len(parsed_data.get('nonDerivativeSecurities', []))}")
    if parsed_data.get('nonDerivativeSecurities'):
        local_logger.info(f"            First row: {json.dumps(parsed_data['nonDerivativeSecurities'][0], default=str)[:200]}")
    local_logger.info(f"         - Table II rows (Derivative): {len(parsed_data.get('derivativeSecurities', []))} {'⚠️ MISSING' if len(parsed_data.get('derivativeSecurities', [])) == 0 else '✅'}")
    if parsed_data.get('derivativeSecurities'):
        local_logger.info(f"            First row: {json.dumps(parsed_data['derivativeSecurities'][0], default=str)[:200]}")
    local_logger.info(f"         - Misc/Explanations: {len(parsed_data.get('misc', {}))} entries {'⚠️ MISSING' if len(parsed_data.get('misc', {})) == 0 else '✅'}")
    if parsed_data.get('misc'):
        misc_preview = {k: str(v)[:100] for k, v in list(parsed_data['misc'].items())[:3]}
        local_logger.info(f"            Preview: {json.dumps(misc_preview, default=str)[:300]}")
    
    # Print critical missing fields to console for immediate visibility
    print(f"   ✅ Parsing complete in {parse_duration:.2f}s:", flush=True)
    missing_fields = []
    if not parsed_data.get('address'):
        missing_fields.append('address')
    if not parsed_data.get('issuerName'):
        missing_fields.append('issuerName')
    if not parsed_data.get('relationship'):
        missing_fields.append('relationship')
    if not parsed_data.get('eventDate'):
        missing_fields.append('eventDate')
    if len(parsed_data.get('derivativeSecurities', [])) == 0:
        missing_fields.append('derivativeSecurities')
    if len(parsed_data.get('misc', {})) == 0:
        missing_fields.append('misc')
    if missing_fields:
        print(f"      ⚠️ MISSING FIELDS: {', '.join(missing_fields)}", flush=True)
    else:
        print(f"      ✅ All critical fields extracted successfully", flush=True)
    
    # Check for critical missing fields (for validation)
    critical_missing = []
    if not parsed_data.get('name'):
        critical_missing.append('name')
    if not parsed_data.get('formType'):
        critical_missing.append('formType')
    if not parsed_data.get('reportingDate'):
        critical_missing.append('reportingDate')
    if critical_missing:
        local_logger.warning(f"      ⚠️ WARNING: Missing critical fields: {', '.join(critical_missing)}")
    
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
    
    # Transform to AI-friendly semantic structure
    parsed_data = transform_to_semantic_structure(parsed_data, form_data, s3_key)
    
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
        # Store all fields, including None/empty values, so columns are visible in the table
        # IMPORTANT: GSI keys cannot be NULL or empty strings - they must be omitted from the item if missing
        # GSI key fields: formType, name, address, eventDate, reportingDate, issuerName, tickerSymbol, relationship
        # Note: The semantic structure (issuer, reporting_person, transactions) will be stored as JSON strings
        gsi_key_fields = {'formType', 'name', 'address', 'eventDate', 'reportingDate', 'issuerName', 'tickerSymbol', 'relationship'}
        
        local_logger.info(f"      🔄 Converting to DynamoDB format...")
        dynamodb_item = {}
        conversion_stats = {'skipped': 0, 'converted': 0, 'errors': 0, 'null_fields': 0, 'gsi_omitted': 0}
        
        for key, value in parsed_data.items():
            try:
                # Handle None values
                if value is None:
                    # GSI keys cannot be NULL or empty - omit them from the item
                    if key in gsi_key_fields:
                        conversion_stats['gsi_omitted'] += 1
                        local_logger.debug(f"         {key}: omitted (GSI key, cannot be null/empty)")
                        continue  # Skip this field - don't include it in the item
                    else:
                        dynamodb_item[key] = None
                        conversion_stats['null_fields'] += 1
                        local_logger.debug(f"         {key}: null")
                    continue
                
                # Handle empty strings
                if value == '':
                    # GSI keys: omit empty strings (cannot be in GSI)
                    if key in gsi_key_fields:
                        conversion_stats['gsi_omitted'] += 1
                        local_logger.debug(f"         {key}: omitted (GSI key, empty string not allowed)")
                        continue  # Skip this field - don't include it in the item
                    else:
                        dynamodb_item[key] = None
                        conversion_stats['null_fields'] += 1
                        local_logger.debug(f"         {key}: null (was empty string)")
                    continue
                
                # Handle empty lists - store as empty JSON array string
                if isinstance(value, list) and len(value) == 0:
                    dynamodb_item[key] = '[]'  # Store as empty JSON array string
                    conversion_stats['converted'] += 1
                    local_logger.debug(f"         {key}: [] (empty list)")
                    continue
                
                # Handle empty dicts - store as empty JSON object string
                if isinstance(value, dict) and len(value) == 0:
                    dynamodb_item[key] = '{}'  # Store as empty JSON object string
                    conversion_stats['converted'] += 1
                    local_logger.debug(f"         {key}: {{}} (empty dict)")
                    continue
                
                # Handle numeric values
                if isinstance(value, (int, float)):
                    if isinstance(value, float) and (value != value or value == float('inf') or value == float('-inf')):
                        # Invalid float - store as null
                        dynamodb_item[key] = None
                        conversion_stats['null_fields'] += 1
                        local_logger.warning(f"         {key}: null (invalid float value {value})")
                        continue
                    dynamodb_item[key] = Decimal(str(value))
                    conversion_stats['converted'] += 1
                    local_logger.debug(f"         {key}: Decimal ({value})")
                
                # Handle lists (non-empty) - store as JSON string
                elif isinstance(value, list):
                    dynamodb_item[key] = json.dumps(value)
                    conversion_stats['converted'] += 1
                    local_logger.debug(f"         {key}: JSON array ({len(value)} items)")
                
                # Handle dicts (non-empty) - store as JSON string
                elif isinstance(value, dict):
                    dynamodb_item[key] = json.dumps(value)
                    conversion_stats['converted'] += 1
                    local_logger.debug(f"         {key}: JSON object ({len(value)} keys)")
                
                # Handle boolean values
                elif isinstance(value, bool):
                    dynamodb_item[key] = value  # DynamoDB supports boolean
                    conversion_stats['converted'] += 1
                    local_logger.debug(f"         {key}: Boolean ({value})")
                
                # Handle strings and other types
                else:
                    dynamodb_item[key] = str(value)
                    conversion_stats['converted'] += 1
                    local_logger.debug(f"         {key}: String ({len(str(value))} chars)")
                    
            except Exception as e:
                conversion_stats['errors'] += 1
                local_logger.error(f"         Error converting {key}: {e}")
                # Store as null on error
                dynamodb_item[key] = None
        
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
    stage2_msg = f"✅ Stage 2 Complete: Fetched {len(forms)} forms in {stage2_duration:.2f} seconds"
    logger.info(stage2_msg)
    print(stage2_msg, flush=True)
    
    # Log form type breakdown
    form_type_counts = {}
    for form in forms:
        form_type = form.get('form_type', 'unknown')
        form_type_counts[form_type] = form_type_counts.get(form_type, 0) + 1
    
    breakdown_header = "Form Type Breakdown:"
    logger.info(breakdown_header)
    print(breakdown_header, flush=True)
    
    for form_type, count in sorted(form_type_counts.items()):
        type_msg = f"   - {form_type}: {count}"
        logger.info(type_msg)
        print(type_msg, flush=True)
    
    # Log preview of fetched files (first 10)
    preview_header = f"📋 Preview of Fetched Files (showing first {builtins.min(10, len(forms))} of {len(forms)}):"
    logger.info(preview_header)
    print(preview_header, flush=True)
    
    for idx, form in enumerate(forms[:10], 1):
        form_msg = (f"   {idx}. CIK={form.get('cik', 'N/A')}, "
                   f"Accession={form.get('accession_number', 'N/A')[:20]}, "
                   f"Type={form.get('form_type', 'N/A')}, "
                   f"FilingDate={form.get('filing_date', 'N/A')}, "
                   f"AcceptedDate={form.get('accepted_date', 'N/A') or 'N/A'}")
        logger.info(form_msg)
        print(form_msg, flush=True)
    
    if len(forms) > 10:
        more_msg = f"   ... ({len(forms) - 10} more files)"
        logger.info(more_msg)
        print(more_msg, flush=True)
    
    separator = "=" * 80
    logger.info(separator)
    print(separator, flush=True)
    
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
    
    # Check for critical failures that should cause job to fail
    if total_forms_processed == 0:
        error_msg = "❌ CRITICAL ERROR: No forms were fetched from SEC API!"
        logger.error("")
        logger.error(error_msg)
        print("", flush=True)
        print(error_msg, flush=True)
        print("   This could indicate:", flush=True)
        print("      - SEC API is blocking requests (403 Forbidden)", flush=True)
        print("      - Network connectivity issues", flush=True)
        print("      - Invalid date parameter", flush=True)
        print("      - No forms filed on the target date", flush=True)
        raise Exception("No forms were fetched from SEC API. Check SEC API access, network connectivity, and date parameters.")
    
    if successful_stored == 0:
        error_msg = "❌ CRITICAL ERROR: No forms were successfully stored!"
        logger.error("")
        logger.error(error_msg)
        print("", flush=True)
        print(error_msg, flush=True)
        print("   This could indicate:", flush=True)
        
        for detail in [
            "- Download failures for all forms (check network/SEC website)",
            "- Parsing failures for all forms (check HTML structure)",
            "- Date mismatches for all forms (check target date)",
            "- DynamoDB write failures (check permissions/table)"
        ]:
            logger.error(f"      {detail}")
            print(f"      {detail}", flush=True)
        
        review_msg = "      Review the detailed logs above for specific error messages"
        logger.error(review_msg)
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
        
        raise Exception(f"No forms were successfully stored. {total_forms_processed} forms were fetched but none were stored. Check logs for details.")
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


