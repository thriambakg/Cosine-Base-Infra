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

# ============================================================================
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

# Parse optional OpenSearch arguments manually
opensearch_endpoint = None
opensearch_index = None
for i, arg in enumerate(sys.argv):
    if arg == '--opensearch_endpoint' or arg.startswith('--opensearch_endpoint='):
        if '=' in arg:
            opensearch_endpoint = arg.split('=', 1)[1]
        elif i + 1 < len(sys.argv):
            opensearch_endpoint = sys.argv[i + 1]
    elif arg == '--opensearch_index' or arg.startswith('--opensearch_index='):
        if '=' in arg:
            opensearch_index = arg.split('=', 1)[1]
        elif i + 1 < len(sys.argv):
            opensearch_index = sys.argv[i + 1]

if opensearch_endpoint:
    logger.info(f"🔍 OpenSearch enabled: Endpoint={opensearch_endpoint}, Index={opensearch_index or 'sec-filings'}")
    print(f"🔍 OpenSearch enabled: Endpoint={opensearch_endpoint}, Index={opensearch_index or 'sec-filings'}", flush=True)
else:
    logger.info("⚠️ OpenSearch not configured - documents will only be stored to DynamoDB")
    print("⚠️ OpenSearch not configured - documents will only be stored to DynamoDB", flush=True)

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
            
            # Ensure User-Agent is set for index file requests
            headers = {'User-Agent': SEC_USER_AGENT}
            response = session.get(index_url, headers=headers, timeout=30)
            
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
        
        # Validate accession number before proceeding
        if not accession_clean or accession_clean == 'unknown' or len(accession_clean) < 10:
            error_msg = f"   ❌ Invalid accession number: '{accession}' (cleaned: '{accession_clean}')"
            local_logger.error(error_msg)
            print(error_msg, flush=True)
            return None
        
        # For URL path: use accession WITH dashes (matching working Lambda behavior)
        if len(accession_clean) == 18:
            accession_dashed = f"{accession_clean[:10]}-{accession_clean[10:12]}-{accession_clean[12:]}"
        elif len(accession_clean) >= 10:
            # If not exactly 18 chars, try to construct with dashes anyway
            # Some accessions might be shorter, pad or use as-is
            if len(accession_clean) >= 12:
                accession_dashed = f"{accession_clean[:10]}-{accession_clean[10:12]}-{accession_clean[12:]}"
            else:
                # Fallback: use as-is (shouldn't happen with valid SEC data)
                local_logger.warning(f"      ⚠️ Accession length unexpected: {len(accession_clean)} chars, using as-is")
                print(f"      ⚠️ Accession length unexpected: {len(accession_clean)} chars, using as-is", flush=True)
                accession_dashed = accession_clean
        else:
            error_msg = f"   ❌ Accession number too short: '{accession_clean}' (length: {len(accession_clean)})"
            local_logger.error(error_msg)
            print(error_msg, flush=True)
            return None
        
        local_logger.info(f"      🔑 Accession: clean='{accession_clean}', dashed='{accession_dashed}'")
        print(f"      🔑 Accession: clean='{accession_clean}', dashed='{accession_dashed}'", flush=True)
        
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
        
        # CRITICAL: Validate file content exists before proceeding
        if not file_content or len(file_content) == 0:
            error_msg = f"   ❌ Download failed: No file content retrieved (CIK={cik}, Accession={accession_dashed})"
            local_logger.error(error_msg)
            print(error_msg, flush=True)
            return None
        
        # Generate S3 key - include accession number to ensure uniqueness
        # Format: trades/{date}/sec/{form_type}-{cik}-{accession}-{date}.{ext}
        # Accession is already in dashed format (e.g., 0001140361-25-040858)
        # CRITICAL: Validate all components before generating S3 key to prevent collisions
        if not all([form_type, cik, accession_dashed, target_date, file_ext]):
            error_msg = f"   ❌ Cannot generate S3 key: missing required components"
            error_msg += f" (form_type={form_type}, cik={cik}, accession_dashed={accession_dashed}, target_date={target_date}, file_ext={file_ext})"
            local_logger.error(error_msg)
            print(error_msg, flush=True)
            raise ValueError(f"Missing required components for S3 key generation")
        
        if accession_dashed == 'unknown' or cik == 'unknown' or form_type == 'unknown':
            error_msg = f"   ❌ Cannot generate S3 key: invalid component values"
            error_msg += f" (form_type={form_type}, cik={cik}, accession_dashed={accession_dashed})"
            local_logger.error(error_msg)
            print(error_msg, flush=True)
            raise ValueError(f"Invalid component values for S3 key generation")
        
        s3_key = f"trades/{target_date}/sec/{form_type}-{cik}-{accession_dashed}-{target_date}.{file_ext}"
        
        local_logger.info(f"      🔑 Generated S3 Key: {s3_key}")
        local_logger.info(f"         Components: form_type={form_type}, cik={cik}, accession_dashed={accession_dashed}, date={target_date}, ext={file_ext}")
        local_logger.info(f"         File size: {len(file_content):,} bytes")
        print(f"      🔑 Generated S3 Key: {s3_key}", flush=True)
        print(f"         Components: form_type={form_type}, cik={cik}, accession_dashed={accession_dashed}, date={target_date}, ext={file_ext}", flush=True)
        print(f"         File size: {len(file_content):,} bytes", flush=True)
        
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
    Routes to form-specific parsing functions based on detected form type.
    
    Returns:
        Dict with all extracted metadata fields
    """
    # Import inside function to avoid serialization issues
    import logging
    local_logger = logging.getLogger()
    
    # Detect form type
    form_number = None
    form_name_match = re.search(r'class="FormName"[^>]*>FORM\s*(\d+)', html_content, re.IGNORECASE | re.DOTALL)
    if form_name_match:
        form_number = form_name_match.group(1)
    
    if not form_number:
        form_match = re.search(r'\bFORM\s+([345])\b', html_content, re.IGNORECASE)
        if form_match:
            form_number = form_match.group(1)
    
    # Route to form-specific parser
    if form_number == '3':
        local_logger.info(f"      📋 Routing to Form 3 parser")
        return parse_form3_metadata(html_content, form_data, accepted_date_str)
    elif form_number == '4':
        local_logger.info(f"      📋 Routing to Form 4 parser")
        return parse_form4_metadata(html_content, form_data, accepted_date_str)
    elif form_number == '5':
        local_logger.info(f"      📋 Routing to Form 5 parser")
        return parse_form5_metadata(html_content, form_data, accepted_date_str)
    else:
        # Default to Form 4 if form type cannot be determined
        local_logger.warning(f"      ⚠️ Could not determine form type, defaulting to Form 4")
        return parse_form4_metadata(html_content, form_data, accepted_date_str)


def _parse_common_metadata(html_content: str, form_data: Dict[str, Any], accepted_date_str: Optional[str], form_number: str) -> Dict[str, Any]:
    """
    Parse common metadata fields shared across Forms 3, 4, and 5.
    
    Args:
        html_content: HTML content of the SEC form
        form_data: Form metadata dict
        accepted_date_str: Accepted date string
        form_number: Form number ('3', '4', or '5')
    
    Returns:
        Dict with common metadata fields
    """
    # Import inside function to avoid serialization issues
    import logging
    from html import unescape
    from datetime import datetime
    local_logger = logging.getLogger()
    
    result = {
        'formType': f'form{form_number}' if form_number else 'form4',
        'reportingPersonName': None,  # Changed from 'name' for clarity
        'address': None,
        'eventDate': None,
        'reportingDate': None,
        'issuerName': None,
        'tickerSymbol': None,
        'relationship': None,  # Primary relationship for GSI
        'relationshipTypes': None,  # All relationship types (comma-separated)
        'relationshipAdditionalText': None,  # Additional text for Officer/Other
        'filingType': None,  # 'individual' or 'joint/group'
        'signatureName': None,
        'amendment': None,  # Date of original filing if this is an amendment (MM/DD/YYYY format)
        # Note: nonDerivativeSecurities, derivativeSecurities, misc removed
        # OpenSearch will handle full-text search on raw HTML content stored in S3
    }
    
    try:
        
        # Extract reporting person name (1. Name and Address of Reporting Person)
        # Pattern: <a href="...cgi-bin/browse-edgar...CIK=...">Name</a>
        # Handle both relative and absolute URLs
        name_match = re.search(r'<a[^>]*href="[^"]*cgi-bin/browse-edgar[^"]*CIK=\d+[^"]*">([^<]+)</a>', html_content, re.IGNORECASE)
        if name_match:
            result['reportingPersonName'] = unescape(name_match.group(1)).strip().lower()
        
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
        
        # Log address extraction
        if result['address']:
            local_logger.info(f"   ✅ Extracted address: {result['address'][:100]}...")
        else:
            local_logger.warning(f"   ⚠️ Could not extract address from HTML")
        
        # Extract reporting date (accepted date from form_data or extract from HTML)
        # Note: Event date extraction is form-specific and handled in form-specific functions
        if accepted_date_str:
            # Extract just the date part (YYYY-MM-DD) from timestamp
            date_part = accepted_date_str.split()[0] if ' ' in accepted_date_str else accepted_date_str
            result['reportingDate'] = date_part
            local_logger.info(f"   ✅ Extracted reportingDate from accepted_date: {result['reportingDate']}")
        else:
            # Fallback: try to extract from HTML signature date
            # More flexible patterns for signature date
            signature_date_patterns = [
                r'\*\* Signature[^<]*<span[^>]*class="FormData"[^>]*>(\d{1,2}/\d{1,2}/\d{4})</span>',
                r'Signature[^<]*Date[^<]*<span[^>]*class="FormData"[^>]*>(\d{1,2}/\d{1,2}/\d{4})</span>',
                r'<u><span[^>]*class="FormData"[^>]*>(\d{1,2}/\d{1,2}/\d{4})</span></u>',  # Date in signature section
            ]
            
            signature_date_match = None
            for pattern in signature_date_patterns:
                signature_date_match = re.search(pattern, html_content, re.IGNORECASE | re.DOTALL)
                if signature_date_match:
                    break
            
            if signature_date_match:
                try:
                    date_str = signature_date_match.group(1)
                    date_obj = datetime.strptime(date_str, '%m/%d/%Y')
                    result['reportingDate'] = date_obj.strftime('%Y-%m-%d')
                    local_logger.info(f"   ✅ Extracted reportingDate from signature: {result['reportingDate']}")
                except Exception as e:
                    local_logger.warning(f"   ⚠️ Could not parse reportingDate '{date_str}': {e}")
            else:
                local_logger.warning(f"   ⚠️ Could not find reportingDate in HTML")
        
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
        
        # Extract relationship (4. or 5. Relationship of Reporting Person(s) to Issuer)
        # HTML structure: The table has 4 columns per row: [checkbox1] [text1] [checkbox2] [text2]
        # Row 1: [checkbox] Director [checkbox] 10% Owner
        # Row 2: [checkbox] Officer [checkbox] Other
        # Row 3: [empty] [blue text for Officer] [empty] [blue text for Other]
        # 
        # The checkbox IMMEDIATELY BEFORE the text marks that relationship type
        # Structure: <td>checkbox</td><td>text</td><td>checkbox</td><td>text</td>
        # 
        # Example:
        # <tr>
        #   <td align="center"></td>  <!-- Cell 0: checkbox (empty) -->
        #   <td class="MedSmallFormText">Director</td>  <!-- Cell 1: text -->
        #   <td align="center"><span class="FormData">X</span></td>  <!-- Cell 2: checkbox (X) -->
        #   <td class="MedSmallFormText">10% Owner</td>  <!-- Cell 3: text -->
        # </tr>
        # In this case, X in cell 2 marks "10% Owner" in cell 3
        relationship_types = []
        relationship_additional = None
        
        # Find the relationship section table
        # Pattern: Look for "Relationship of Reporting Person(s) to Issuer" followed by a table
        # Be more flexible - allow various HTML structures between the label and table
        relationship_patterns = [
            # Pattern 1: Direct match with minimal HTML between
            r'Relationship of Reporting Person\(s\) to Issuer[^<]*(?:<br[^>]*>)?[^<]*(?:\(Check all applicable\)[^<]*)?<table[^>]*>(.*?)</table>',
            # Pattern 2: More flexible - allow any HTML between label and table
            r'Relationship of Reporting Person\(s\) to Issuer.*?<table[^>]*>(.*?)</table>',
            # Pattern 3: Look for the table that contains "Director" and "Officer" text
            r'<table[^>]*>.*?Director.*?Officer.*?</table>',
        ]
        
        relationship_section_match = None
        relationship_table = None
        
        for pattern in relationship_patterns:
            relationship_section_match = re.search(pattern, html_content, re.IGNORECASE | re.DOTALL)
            if relationship_section_match:
                # Extract the table content
                if len(relationship_section_match.groups()) > 0:
                    relationship_table = relationship_section_match.group(1)
                else:
                    # Pattern 3 matches the whole table, extract it differently
                    relationship_table = relationship_section_match.group(0)
                    # Remove the opening table tag to get just the content
                    relationship_table = re.sub(r'^<table[^>]*>', '', relationship_table, flags=re.IGNORECASE)
                    relationship_table = re.sub(r'</table>$', '', relationship_table, flags=re.IGNORECASE)
                
                if relationship_table and ('Director' in relationship_table or 'Officer' in relationship_table):
                    break
        
        if relationship_section_match and relationship_table:
            # Log the full relationship table HTML for debugging
            local_logger.info(f"      🔍 Relationship table HTML (full): {relationship_table[:500]}")
            print(f"      🔍 Relationship table HTML (full): {relationship_table[:500]}", flush=True)
            
            # More robust parsing: Find all table rows and parse each cell pair
            # Pattern: Match each <tr>...</tr> block
            rows = re.findall(r'<tr[^>]*>(.*?)</tr>', relationship_table, re.IGNORECASE | re.DOTALL)
            local_logger.info(f"      📊 Found {len(rows)} rows in relationship table")
            print(f"      📊 Found {len(rows)} rows in relationship table", flush=True)
            
            # Parse each row (skip row 3 which has blue text for Officer/Other)
            for row_idx, row_html in enumerate(rows):
                # Skip the third row (index 2) which contains blue text for additional info
                if row_idx >= 2:
                    continue
                
                local_logger.info(f"      🔍 Parsing row {row_idx + 1}: {row_html[:200]}")
                print(f"      🔍 Parsing row {row_idx + 1}: {row_html[:200]}", flush=True)
                
                # Extract all <td> cells in this row
                cells = re.findall(r'<td[^>]*>(.*?)</td>', row_html, re.IGNORECASE | re.DOTALL)
                local_logger.info(f"      📋 Row {row_idx + 1} has {len(cells)} cells")
                print(f"      📋 Row {row_idx + 1} has {len(cells)} cells", flush=True)
                
                # Check each checkbox-text pair: (cell 0, cell 1) and (cell 2, cell 3)
                # The checkbox is in the odd-indexed cell (0, 2), text is in the even-indexed cell (1, 3)
                for pair_idx in [0, 2]:
                    if pair_idx + 1 < len(cells):
                        checkbox_cell = cells[pair_idx]
                        text_cell = cells[pair_idx + 1]
                        
                        # Check if checkbox contains X (look for FormData class with X)
                        # Pattern: <span class="FormData">X</span> or just X in the cell
                        has_x = bool(re.search(r'<span[^>]*class="FormData"[^>]*>X</span>', checkbox_cell, re.IGNORECASE))
                        if not has_x:
                            # Also check for just X in the cell (some forms might not have FormData class)
                            has_x = 'X' in checkbox_cell.strip()
                        
                        # Extract text from text cell (remove HTML tags)
                        text_content = re.sub(r'<[^>]+>', '', text_cell).strip()
                        
                        local_logger.info(f"      🔍 Pair {pair_idx//2 + 1}: checkbox='{checkbox_cell[:100]}', has_x={has_x}, text='{text_content}'")
                        print(f"      🔍 Pair {pair_idx//2 + 1}: checkbox='{checkbox_cell[:100]}', has_x={has_x}, text='{text_content}'", flush=True)
                        
                        if has_x and text_content:
                            # Match relationship type - check for exact matches first
                            text_lower = text_content.lower()
                            
                            if 'director' in text_lower and 'director' not in relationship_types:
                                relationship_types.append('Director')
                                local_logger.info(f"      ✅ Found Director relationship")
                                print(f"      ✅ Found Director relationship", flush=True)
                            elif 'officer' in text_lower and 'officer' not in relationship_types:
                                relationship_types.append('Officer')
                                local_logger.info(f"      ✅ Found Officer relationship")
                                print(f"      ✅ Found Officer relationship", flush=True)
                            elif '10%' in text_content and 'owner' in text_lower and '10% Owner' not in relationship_types:
                                relationship_types.append('10% Owner')
                                local_logger.info(f"      ✅ Found 10% Owner relationship")
                                print(f"      ✅ Found 10% Owner relationship", flush=True)
                            elif 'other' in text_lower and 'other' not in relationship_types:
                                relationship_types.append('Other')
                                local_logger.info(f"      ✅ Found Other relationship")
                                print(f"      ✅ Found Other relationship", flush=True)
            
            # Extract additional text from blue text cells (third row)
            if len(rows) >= 3:
                blue_text_row = rows[2]  # Third row (index 2)
                blue_text_cells = re.findall(
                    r'<td[^>]*style="color:\s*blue"[^>]*>([^<]+)</td>',
                    blue_text_row,
                    re.IGNORECASE | re.DOTALL
                )
                if blue_text_cells:
                    # First blue cell is for Officer, second is for Other
                    if len(blue_text_cells) >= 1 and 'Officer' in relationship_types:
                        officer_text = unescape(blue_text_cells[0]).strip()
                        if officer_text:
                            relationship_additional = officer_text
                            local_logger.info(f"      ✅ Found Officer additional text: {officer_text}")
                            print(f"      ✅ Found Officer additional text: {officer_text}", flush=True)
                    if len(blue_text_cells) >= 2 and 'Other' in relationship_types:
                        other_text = unescape(blue_text_cells[1]).strip()
                        if other_text:
                            if relationship_additional:
                                relationship_additional = relationship_additional + '; ' + other_text
                            else:
                                relationship_additional = other_text
                            local_logger.info(f"      ✅ Found Other additional text: {other_text}")
                            print(f"      ✅ Found Other additional text: {other_text}", flush=True)
            
            # Log what we found
            if relationship_types:
                local_logger.info(f"      ✅ Parsed relationship types: {', '.join(relationship_types)}")
                print(f"      ✅ Parsed relationship types: {', '.join(relationship_types)}", flush=True)
            else:
                local_logger.warning(f"      ⚠️ No relationship types found in table")
                print(f"      ⚠️ No relationship types found in table", flush=True)
                local_logger.info(f"      Full relationship table HTML: {relationship_table}")
                print(f"      Full relationship table HTML: {relationship_table}", flush=True)
        else:
            local_logger.warning(f"      ⚠️ Could not find relationship section table")
            print(f"      ⚠️ Could not find relationship section table", flush=True)
        
        # Store relationship - use first relationship type as primary (for GSI)
        # Store all relationship types as comma-separated string for completeness
        result['relationship'] = relationship_types[0] if relationship_types else None  # Primary relationship for GSI
        result['relationshipTypes'] = ', '.join(relationship_types) if relationship_types else None  # All relationship types
        result['relationshipAdditionalText'] = relationship_additional if relationship_additional else None
        
        # Log relationship extraction results
        if result['relationship']:
            local_logger.info(f"   ✅ Extracted relationship: {result['relationship']}")
            if result['relationshipTypes']:
                local_logger.info(f"   ✅ All relationship types: {result['relationshipTypes']}")
        else:
            local_logger.warning(f"   ⚠️ Could not extract relationship from HTML")
        
        # Extract Individual/Group Filing (6. Individual or Joint/Group Filing)
        # HTML structure: Two rows, each with checkbox (td) and text (td)
        # Row 1: <td><span>X</span></td><td>Form filed by One Reporting Person</td>
        # Row 2: <td></td><td>Form filed by More than One Reporting Person</td>
        # The checkbox is in the first td, text is in the second td
        # Try multiple patterns to find the section
        filing_patterns = [
            r'Individual or Joint/Group Filing[^<]*(?:\(Check Applicable Line\)[^<]*)?<table[^>]*>(.*?)</table>',
            r'6\.\s*Individual or Joint/Group Filing[^<]*<table[^>]*>(.*?)</table>',
            r'Individual or Joint/Group Filing.*?<table[^>]*>(.*?)</table>',
        ]
        
        filing_table = None
        for pattern in filing_patterns:
            individual_filing_match = re.search(pattern, html_content, re.IGNORECASE | re.DOTALL)
            if individual_filing_match:
                filing_table = individual_filing_match.group(1)
                local_logger.info(f"   🔍 Found filing type table using pattern")
                break
        
        if filing_table:
            # Parse table rows - similar to relationship parsing
            rows = re.findall(r'<tr[^>]*>(.*?)</tr>', filing_table, re.IGNORECASE | re.DOTALL)
            local_logger.info(f"   📊 Found {len(rows)} rows in filing type table")
            
            # Parse each row to find which checkbox is checked
            for row_idx, row_html in enumerate(rows):
                # Extract all <td> cells in this row
                cells = re.findall(r'<td[^>]*>(.*?)</td>', row_html, re.IGNORECASE | re.DOTALL)
                
                if len(cells) >= 2:
                    checkbox_cell = cells[0]  # First cell is checkbox
                    text_cell = cells[1]     # Second cell is text
                    
                    # Check if checkbox contains X
                    has_x = bool(re.search(r'<span[^>]*class="FormData"[^>]*>X</span>', checkbox_cell, re.IGNORECASE))
                    if not has_x:
                        # Also check for just X in the cell
                        has_x = 'X' in checkbox_cell.strip()
                    
                    # Extract text from text cell (remove HTML tags)
                    text_content = re.sub(r'<[^>]+>', '', text_cell).strip()
                    
                    local_logger.info(f"   🔍 Row {row_idx + 1}: checkbox has_x={has_x}, text='{text_content}'")
                    
                    if has_x and text_content:
                        text_lower = text_content.lower()
                        
                        # Check which type it is
                        if 'one reporting person' in text_lower or 'individual' in text_lower:
                            result['filingType'] = 'individual'
                            local_logger.info(f"   ✅ Extracted filingType: individual")
                            break
                        elif 'more than one' in text_lower or 'joint' in text_lower or 'group' in text_lower:
                            result['filingType'] = 'joint/group'
                            local_logger.info(f"   ✅ Extracted filingType: joint/group")
                            break
            
            # If we didn't find a match, log warning
            if not result.get('filingType'):
                result['filingType'] = None
                local_logger.warning(f"   ⚠️ Could not determine filingType from table")
                local_logger.info(f"   🔍 Filing table HTML: {filing_table[:300]}")
        else:
            result['filingType'] = None
            local_logger.warning(f"   ⚠️ Could not find Individual or Joint/Group Filing section")
        
        # Extract signature name
        # Patterns to handle:
        # 1. Exhibit reference: "See Exhibit 99.1 for Signature" or "See Exhibit 99.1 for Signatures" (check first)
        #    - When found, search Remarks/Explanation sections for Exhibit 99.1 content and extract signature
        # 2. Direct signature with /s/ prefix: <u><span class="FormData">/s/ Name</span></u>
        # 3. Signature in signature section with /s/: ** Signature ... <u><span>/s/ Name</span></u>
        # 4. Signature in signature section without /s/: ** Signature ... <u><span>Name</span></u>
        # 5. Direct signature without /s/ (but not "See Exhibit"): <u><span>Name</span></u>
        
        signature_name = None
        
        # First, check for "See Exhibit" pattern (most specific)
        exhibit_match = re.search(r'<u><span[^>]*class="FormData"[^>]*>See Exhibit 99\.1 for Signature[s]?</span></u>', html_content, re.IGNORECASE)
        if exhibit_match:
            # Search for Exhibit 99.1 content in Remarks or Explanation of Responses sections
            # Look for patterns like "Exhibit 99.1 (Signature)" or "Exhibit 99.1 (Signatures and Joint Filer Information)"
            # and extract signature information from that section
            local_logger.info(f"   🔍 Found 'See Exhibit 99.1 for signature' - searching for Exhibit 99.1 content")
            
            # Search in Remarks section first (most common location)
            remarks_section = re.search(
                r'<b>Remarks:</b>.*?</table>',
                html_content,
                re.IGNORECASE | re.DOTALL
            )
            
            # Also search in Explanation of Responses section
            explanation_section = re.search(
                r'Explanation of Responses.*?<b>Remarks:</b>',
                html_content,
                re.IGNORECASE | re.DOTALL
            )
            
            # Search for Exhibit 99.1 references in these sections
            exhibit_patterns = [
                # Pattern 1: Look for signature names after "Exhibit 99.1" in the text
                r'Exhibit 99\.1[^<]*\([^)]*Signature[^)]*\)[^<]*is incorporated[^<]*by reference[^<]*\.',
                # Pattern 2: Look for /s/ Name patterns near Exhibit 99.1
                r'Exhibit 99\.1.*?(/s/|s/)\s*([A-Z][^<\n]+)',
                # Pattern 3: Look for signature names in table cells after Exhibit 99.1 mention
                r'Exhibit 99\.1.*?<td[^>]*class="[^"]*FootnoteData[^"]*"[^>]*>([^<]+)</td>',
            ]
            
            # Search in both sections
            search_areas = []
            if remarks_section:
                search_areas.append(('Remarks', remarks_section.group(0)))
            if explanation_section:
                search_areas.append(('Explanation', explanation_section.group(0)))
            
            # If no specific sections found, search the entire document for Exhibit 99.1
            if not search_areas:
                search_areas.append(('Document', html_content))
            
            for section_name, section_content in search_areas:
                local_logger.info(f"   🔍 Searching {section_name} section for Exhibit 99.1 signature")
                
                # First, check if Exhibit 99.1 is mentioned
                if re.search(r'Exhibit 99\.1', section_content, re.IGNORECASE):
                    # Try to extract signature from various patterns
                    # Look for /s/ Name patterns in the section
                    signature_patterns = [
                        r'(/s/|s/)\s*([A-Z][A-Za-z\s,\.]+?)(?:\s+Date|\s+\d{1,2}/\d{1,2}/\d{4}|</td>|</span>|$)',
                        r'Signature[^<]*<u><span[^>]*class="FormData"[^>]*>(/s/|s/)\s*([^<]+)</span></u>',
                        r'Signature[^<]*<u><span[^>]*class="FormData"[^>]*>([^<]+)</span></u>',
                    ]
                    
                    for pattern in signature_patterns:
                        sig_match = re.search(pattern, section_content, re.IGNORECASE | re.DOTALL)
                        if sig_match:
                            # Extract the signature name
                            if len(sig_match.groups()) >= 2:
                                sig_name = sig_match.group(2)  # Second group is usually the name
                            else:
                                sig_name = sig_match.group(1)  # First group if only one
                            
                            sig_name = unescape(sig_name).strip()
                            # Remove /s/ or s/ prefix if present
                            sig_name = re.sub(r'^[/]?s[/]\s*', '', sig_name, flags=re.IGNORECASE)
                            sig_name = sig_name.strip()
                            
                            if sig_name and len(sig_name) > 2:  # Valid signature name
                                signature_name = sig_name
                                local_logger.info(f"   ✅ Extracted signature from Exhibit 99.1 in {section_name}: {signature_name}")
                                break
                    
                    if signature_name:
                        break
            
            # If we still didn't find a signature, fall back to the literal text
            if not signature_name:
                signature_name = 'see exhibit 99.1 for signature'
                local_logger.warning(f"   ⚠️ Found 'See Exhibit 99.1 for signature' but could not extract signature from document")
        else:
            # Try patterns in order of specificity
            signature_patterns = [
                # Pattern 1: Direct signature with /s/ prefix
                (r'<u><span[^>]*class="FormData"[^>]*>(/s/|s/)\s*([^<]+)</span></u>', 2),
                # Pattern 2: Signature in signature section with /s/
                (r'\*\* Signature[^<]*<u><span[^>]*class="FormData"[^>]*>(/s/|s/)\s*([^<]+)</span></u>', 2),
                # Pattern 3: Signature in signature section without /s/
                (r'\*\* Signature[^<]*<u><span[^>]*class="FormData"[^>]*>([^<]+)</span></u>', 1),
                # Pattern 4: Direct signature without /s/ (exclude "See Exhibit" and common prefixes)
                (r'<u><span[^>]*class="FormData"[^>]*>(?!/s/|s/|See Exhibit)([^<]+)</span></u>', 1),
            ]
            
            for pattern, group_idx in signature_patterns:
                signature_match = re.search(pattern, html_content, re.IGNORECASE | re.DOTALL)
                if signature_match:
                    # Extract the signature name from the appropriate group
                    if group_idx <= len(signature_match.groups()):
                        signature_name = unescape(signature_match.group(group_idx)).strip()
                        
                        # Skip if it's "See Exhibit" or empty
                        if signature_name and 'see exhibit' not in signature_name.lower():
                            # Remove /s/ or s/ prefix if present (shouldn't be needed but just in case)
                            signature_name = re.sub(r'^[/]?s[/]\s*', '', signature_name, flags=re.IGNORECASE)
                            signature_name = signature_name.strip()
                            if signature_name:
                                break
        
        if signature_name:
            result['signatureName'] = signature_name.lower()
            local_logger.info(f"   ✅ Extracted signatureName: {result['signatureName']}")
        else:
            local_logger.warning(f"   ⚠️ Could not extract signatureName from HTML")
        
        # Extract amendment date (4. or 5. If Amendment, Date of Original Filed)
        # HTML structure: Label followed by <br> and optional <span class="FormData">DATE</span>
        # If date is present, this is an amendment; otherwise it's an original filing
        # Pattern matches both "4. If Amendment" (Form 4, 5) and "5. If Amendment" (Form 3)
        # The date must be within the same <td> cell as the label, immediately after the label
        # We match the cell content between <td> and </td>, ensuring the date comes after "If Amendment"
        # and before the closing </td> tag
        amendment_patterns = [
            # Pattern 1: Date immediately after label with (Month/Day/Year) text, within same <td> cell
            # Use a more restrictive pattern that stops at </td> to prevent matching across cells
            r'<td[^>]*valign="top"[^>]*>.*?If Amendment, Date of Original Filed[^<]*(?:\(Month/Day/Year\))?[^<]*(?:<br[^>]*>)?[^<]*<span[^>]*class="FormData"[^>]*>(\d{1,2}/\d{1,2}/\d{4})</span>[^<]*</td>',
            # Pattern 2: More flexible but still within same cell - ensure </td> comes after the date
            r'<td[^>]*>.*?If Amendment, Date of Original Filed[^<]*(?:\(Month/Day/Year\))?[^<]*(?:<br[^>]*>)?[^<]*<span[^>]*class="FormData"[^>]*>(\d{1,2}/\d{1,2}/\d{4})</span>(?=[^<]*</td>)',
        ]
        
        amendment_date = None
        for pattern in amendment_patterns:
            amendment_match = re.search(pattern, html_content, re.IGNORECASE | re.DOTALL)
            if amendment_match:
                amendment_date = amendment_match.group(1)
                local_logger.info(f"   ✅ Extracted amendment date: {amendment_date}")
                print(f"   ✅ Extracted amendment date: {amendment_date}", flush=True)
                break
        
        if not amendment_date:
            local_logger.info(f"   ℹ️ No amendment date found - this is an original filing")
        
        result['amendment'] = amendment_date
        
        # Note: Table parsing (Table I, Table II, explanations, remarks) is form-specific
        # and handled in form-specific functions (parse_form3_metadata, parse_form4_metadata, parse_form5_metadata)
        
    except Exception as e:
        local_logger.error(f"❌ Error parsing form metadata: {e}")
        import traceback
        local_logger.error(f"   Traceback: {traceback.format_exc()}")
    
    return result


def parse_form3_metadata(html_content: str, form_data: Dict[str, Any], accepted_date_str: Optional[str]) -> Dict[str, Any]:
    """
    Parse Form 3 specific metadata.
    Form 3 differences:
    - Event date label: "Date of Event Requiring Statement"
    - Table structure: Different column layout
    """
    import logging
    from html import unescape
    from datetime import datetime
    local_logger = logging.getLogger()
    
    # Get common metadata
    result = _parse_common_metadata(html_content, form_data, accepted_date_str, '3')
    
    try:
        # Form 3 specific: Extract event date
        # More flexible pattern to handle various HTML structures
        # Pattern 1: Use .*? to match any characters (including newlines and HTML) between label and date
        # This handles cases where "(Month/Day/Year)" text appears between label and date
        event_date_patterns = [
            r'Date of Event Requiring Statement.*?<span[^>]*class="FormData"[^>]*>(\d{1,2}/\d{1,2}/\d{4})</span>',
            r'Date of Event Requiring Statement[^<]*(?:\(Month/Day/Year\))?[^<]*(?:<br[^>]*>)?[^<]*<span[^>]*class="FormData"[^>]*>(\d{1,2}/\d{1,2}/\d{4})</span>',
            r'Date of Event Requiring Statement[^<]*(\d{1,2}/\d{1,2}/\d{4})',
        ]
        
        event_date_match = None
        for pattern in event_date_patterns:
            event_date_match = re.search(pattern, html_content, re.IGNORECASE | re.DOTALL)
            if event_date_match:
                break
        
        if event_date_match:
            try:
                date_str = event_date_match.group(1)
                date_obj = datetime.strptime(date_str, '%m/%d/%Y')
                result['eventDate'] = date_obj.strftime('%Y-%m-%d')
                local_logger.info(f"   ✅ Extracted eventDate: {result['eventDate']}")
            except Exception as e:
                local_logger.warning(f"   ⚠️ Could not parse eventDate '{date_str}': {e}")
        else:
            local_logger.warning(f"   ⚠️ Could not find eventDate in HTML")
        
        # Note: Table parsing (nonDerivativeSecurities, derivativeSecurities, misc) removed
        # OpenSearch will handle full-text search on the raw HTML content stored in S3
        # This simplifies the pipeline and avoids parsing errors from non-uniform forms
        
    except Exception as e:
        local_logger.error(f"❌ Error parsing Form 3 metadata: {e}")
        import traceback
        local_logger.error(f"   Traceback: {traceback.format_exc()}")
    
    return result


def parse_form4_metadata(html_content: str, form_data: Dict[str, Any], accepted_date_str: Optional[str]) -> Dict[str, Any]:
    """
    Parse Form 4 specific metadata.
    Form 4 differences:
    - Event date label: "Date of Earliest Transaction"
    - Table structure: Standard column layout
    """
    import logging
    from html import unescape
    from datetime import datetime
    local_logger = logging.getLogger()
    
    # Get common metadata
    result = _parse_common_metadata(html_content, form_data, accepted_date_str, '4')
    
    try:
        # Form 4 specific: Extract event date
        # More flexible pattern to handle various HTML structures
        # Pattern 1: Use .*? to match any characters (including newlines and HTML) between label and date
        # This handles cases where "(Month/Day/Year)" text appears between label and date
        event_date_patterns = [
            r'Date of Earliest Transaction.*?<span[^>]*class="FormData"[^>]*>(\d{1,2}/\d{1,2}/\d{4})</span>',
            r'Date of Earliest Transaction[^<]*(?:\(Month/Day/Year\))?[^<]*(?:<br[^>]*>)?[^<]*<span[^>]*class="FormData"[^>]*>(\d{1,2}/\d{1,2}/\d{4})</span>',
            r'Date of Earliest Transaction[^<]*(\d{1,2}/\d{1,2}/\d{4})',
        ]
        
        event_date_match = None
        for pattern in event_date_patterns:
            event_date_match = re.search(pattern, html_content, re.IGNORECASE | re.DOTALL)
            if event_date_match:
                break
        
        if event_date_match:
            try:
                date_str = event_date_match.group(1)
                date_obj = datetime.strptime(date_str, '%m/%d/%Y')
                result['eventDate'] = date_obj.strftime('%Y-%m-%d')
                local_logger.info(f"   ✅ Extracted eventDate: {result['eventDate']}")
            except Exception as e:
                local_logger.warning(f"   ⚠️ Could not parse eventDate '{date_str}': {e}")
        else:
            local_logger.warning(f"   ⚠️ Could not find eventDate in HTML")
        
        # Note: Table parsing (nonDerivativeSecurities, derivativeSecurities, misc) removed
        # OpenSearch will handle full-text search on the raw HTML content stored in S3
        # This simplifies the pipeline and avoids parsing errors from non-uniform forms
        
    except Exception as e:
        local_logger.error(f"❌ Error parsing Form 4 metadata: {e}")
        import traceback
        local_logger.error(f"   Traceback: {traceback.format_exc()}")
    
    return result


def parse_form5_metadata(html_content: str, form_data: Dict[str, Any], accepted_date_str: Optional[str]) -> Dict[str, Any]:
    """
    Parse Form 5 specific metadata.
    Form 5 differences:
    - Event date label: "Statement for Issuer's Fiscal Year Ended"
    - Table structure: Similar to Form 4
    """
    import logging
    from html import unescape
    from datetime import datetime
    local_logger = logging.getLogger()
    
    # Get common metadata
    result = _parse_common_metadata(html_content, form_data, accepted_date_str, '5')
    
    try:
        # Form 5 specific: Extract event date (fiscal year end)
        # More flexible pattern to handle various HTML structures
        # Pattern 1: Use .*? to match any characters (including newlines and HTML) between label and date
        # This handles cases where "(Month/Day/Year)" text appears between label and date
        event_date_patterns = [
            r'Statement for Issuer\'s Fiscal Year Ended.*?<span[^>]*class="FormData"[^>]*>(\d{1,2}/\d{1,2}/\d{4})</span>',
            r'Statement for Issuer\'s Fiscal Year Ended[^<]*(?:\(Month/Day/Year\))?[^<]*(?:<br[^>]*>)?[^<]*<span[^>]*class="FormData"[^>]*>(\d{1,2}/\d{1,2}/\d{4})</span>',
            r'Statement for Issuer\'s Fiscal Year Ended[^<]*(\d{1,2}/\d{1,2}/\d{4})',
        ]
        
        event_date_match = None
        for pattern in event_date_patterns:
            event_date_match = re.search(pattern, html_content, re.IGNORECASE | re.DOTALL)
            if event_date_match:
                break
        
        if event_date_match:
            try:
                date_str = event_date_match.group(1)
                date_obj = datetime.strptime(date_str, '%m/%d/%Y')
                result['eventDate'] = date_obj.strftime('%Y-%m-%d')
                local_logger.info(f"   ✅ Extracted eventDate: {result['eventDate']}")
            except Exception as e:
                local_logger.warning(f"   ⚠️ Could not parse eventDate '{date_str}': {e}")
        else:
            local_logger.warning(f"   ⚠️ Could not find eventDate in HTML")
        
        # Note: Table parsing (nonDerivativeSecurities, derivativeSecurities, misc) removed
        # OpenSearch will handle full-text search on the raw HTML content stored in S3
        # This simplifies the pipeline and avoids parsing errors from non-uniform forms
        
    except Exception as e:
        local_logger.error(f"❌ Error parsing Form 5 metadata: {e}")
        import traceback
        local_logger.error(f"   Traceback: {traceback.format_exc()}")
    
    return result


def process_form(form_data: Dict[str, Any], target_date: str, politicians: List[Dict[str, Any]], s3_bucket_name: str, dynamodb_table_name: str,
                 opensearch_endpoint: Optional[str] = None, opensearch_index: Optional[str] = None) -> Dict[str, Any]:
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
    print(f"   🔍 Step 1/4: Validating filing date...", flush=True)
    print(f"      Filing date string: '{filing_date_str}'", flush=True)
    print(f"      Target date: '{target_date}'", flush=True)
    
    filing_date_obj = None
    if filing_date_str and filing_date_str != 'unknown':
        try:
            # Try YYYY-MM-DD format first
            try:
                filing_date_obj = datetime.strptime(filing_date_str, '%Y-%m-%d').date()
                local_logger.info(f"      ✅ Parsed filing date (YYYY-MM-DD): {filing_date_obj}")
                print(f"      ✅ Parsed filing date (YYYY-MM-DD): {filing_date_obj}", flush=True)
            except ValueError:
                # Fallback to MM/DD/YYYY
                try:
                    filing_date_obj = datetime.strptime(filing_date_str, '%m/%d/%Y').date()
                    local_logger.info(f"      ✅ Parsed filing date (MM/DD/YYYY): {filing_date_obj}")
                    print(f"      ✅ Parsed filing date (MM/DD/YYYY): {filing_date_obj}", flush=True)
                except ValueError:
                    local_logger.warning(f"      ⚠️ Could not parse filing date: '{filing_date_str}' (neither YYYY-MM-DD nor MM/DD/YYYY)")
                    print(f"      ⚠️ Could not parse filing date: '{filing_date_str}' (neither YYYY-MM-DD nor MM/DD/YYYY)", flush=True)
        except Exception as date_parse_error:
            local_logger.warning(f"      ⚠️ Date parsing exception: {date_parse_error}")
            print(f"      ⚠️ Date parsing exception: {date_parse_error}", flush=True)
    
    target_date_obj = datetime.strptime(target_date, '%Y-%m-%d').date()
    
    # If we couldn't parse the filing date, skip the form (don't process forms with invalid dates)
    if filing_date_obj is None:
        local_logger.warning(f"   ⏭️ SKIPPING: Could not parse filing date '{filing_date_str}' - skipping form to avoid processing invalid data")
        print(f"   ⏭️ SKIPPING: Could not parse filing date '{filing_date_str}' - skipping form", flush=True)
        return {'skipped': True, 'reason': 'date_parse_failed'}
    
    if filing_date_obj != target_date_obj:
        local_logger.info(f"   ⏭️ SKIPPING: Filing date {filing_date_obj} doesn't match target {target_date_obj}")
        print(f"   ⏭️ SKIPPING: Filing date {filing_date_obj} doesn't match target {target_date_obj}", flush=True)
        return {'skipped': True, 'reason': 'date_mismatch'}
    
    local_logger.info(f"   ✅ Date validation passed: {filing_date_obj} matches target {target_date_obj}")
    print(f"   ✅ Date validation passed: {filing_date_obj} matches target {target_date_obj}", flush=True)
    
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
    
    s3_key = downloaded.get('s3_key')
    content_str = downloaded.get('content')
    
    if not s3_key or not content_str:
        error_msg = f"   ❌ FAILED: Downloaded form missing s3_key or content (CIK={cik}, Accession={accession})"
        local_logger.warning(error_msg)
        print(error_msg, flush=True)
        return {'skipped': True, 'reason': 'download_failed'}
    
    local_logger.info(f"   ✅ Downloaded form: S3Key={s3_key}, Size={len(content_str)} bytes, Duration={download_duration:.2f}s")
    print(f"   ✅ Downloaded form: S3Key={s3_key}, Size={len(content_str)} bytes, Duration={download_duration:.2f}s", flush=True)
    
    # Parse form metadata
    parse_start = datetime.now()
    local_logger.info(f"   🔍 Step 3/4: Parsing form metadata...")
    print(f"   🔍 Step 3/4: Parsing form metadata...", flush=True)
    
    parsed_data = parse_sec_form_metadata(content_str, form_data, accepted_date_str)
    parse_duration = (datetime.now() - parse_start).total_seconds()
    
    if not parsed_data:
        error_msg = f"   ❌ FAILED: Could not parse form metadata (CIK={cik}, Accession={accession}) after {parse_duration:.2f}s"
        local_logger.warning(error_msg)
        print(error_msg, flush=True)
        return {'skipped': True, 'reason': 'parse_failed'}
    
    local_logger.info(f"   ✅ Parsing complete in {parse_duration:.2f}s:")
    print(f"   ✅ Parsing complete in {parse_duration:.2f}s:", flush=True)
    
    trade_id = parsed_data.get('tradeId')
    reporting_person = parsed_data.get('reportingPersonName', 'N/A')
    issuer = parsed_data.get('issuerName', 'N/A')
    ticker = parsed_data.get('tickerSymbol', 'N/A')
    
    local_logger.info(f"      TradeId: {trade_id}")
    print(f"      TradeId: {trade_id}", flush=True)
    local_logger.info(f"      Reporting Person: {reporting_person}")
    print(f"      Reporting Person: {reporting_person}", flush=True)
    local_logger.info(f"      Issuer: {issuer} ({ticker})")
    print(f"      Issuer: {issuer} ({ticker})", flush=True)
    
    # Check politician match
    match_start = datetime.now()
    local_logger.info(f"   🔍 Step 4/4: Checking politician match...")
    print(f"   🔍 Step 4/4: Checking politician match...", flush=True)
    
    politician_match = find_matching_politician(reporting_person, politicians)
    match_duration = (datetime.now() - match_start).total_seconds()
    
    if politician_match:
        parsed_data['politician'] = 1
        parsed_data['politicianName'] = politician_match.get('name', '')
        parsed_data['politicianParty'] = politician_match.get('party', '')
        parsed_data['politicianState'] = politician_match.get('state', '')
        local_logger.info(f"   ✅ Politician match: {politician_match.get('name', 'N/A')} ({politician_match.get('party', 'N/A')}, {politician_match.get('state', 'N/A')})")
        print(f"   ✅ Politician match: {politician_match.get('name', 'N/A')} ({politician_match.get('party', 'N/A')}, {politician_match.get('state', 'N/A')})", flush=True)
    else:
        parsed_data['politician'] = 0
        local_logger.info(f"   ℹ️ No politician match found")
        print(f"   ℹ️ No politician match found", flush=True)
    
    # Store to DynamoDB
    store_start = datetime.now()
    local_logger.info(f"   💾 Storing to DynamoDB...")
    print(f"   💾 Storing to DynamoDB...", flush=True)
    
    try:
        dynamodb_local = boto3.resource('dynamodb')
        table_local = dynamodb_local.Table(dynamodb_table_name)
        
        # Convert to DynamoDB format
        dynamodb_item = {}
        for key, value in parsed_data.items():
            # Store amendment field even if None (to explicitly mark as original filing, not amendment)
            # DynamoDB doesn't support null values, so we store empty string to make field present
            if value is None and key == 'amendment':
                dynamodb_item[key] = ''  # Empty string represents null/not an amendment
                continue
            elif value is None:
                continue
            elif isinstance(value, (int, float)):
                if isinstance(value, float) and (value != value or value == float('inf') or value == float('-inf')):
                    continue
                dynamodb_item[key] = Decimal(str(value))
            elif isinstance(value, list):
                dynamodb_item[key] = [Decimal(str(v)) if isinstance(v, (int, float)) else v for v in value]
            else:
                dynamodb_item[key] = value
        
        table_local.put_item(Item=dynamodb_item)
        store_duration = (datetime.now() - store_start).total_seconds()
        
        local_logger.info(f"   ✅ Stored to DynamoDB in {store_duration:.2f}s: TradeId={trade_id}")
        print(f"   ✅ Stored to DynamoDB in {store_duration:.2f}s: TradeId={trade_id}", flush=True)
        
        # Store to OpenSearch if configured
        if opensearch_endpoint and opensearch_index:
            opensearch_start = datetime.now()
            local_logger.info(f"   🔍 Storing to OpenSearch...")
            print(f"   🔍 Storing to OpenSearch...", flush=True)
            
            try:
                import requests
                from requests_aws4auth import AWS4Auth
                import json
                
                # Create AWS4Auth for signing requests
                credentials = boto3.Session().get_credentials()
                awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, 'us-east-1', 'es', session_token=credentials.token)
                
                # Create OpenSearch document
                opensearch_doc = {
                    'tradeId': trade_id,
                    'formType': parsed_data.get('formType'),
                    'reportingPersonName': parsed_data.get('reportingPersonName'),
                    'signatureName': parsed_data.get('signatureName'),
                    'issuerName': parsed_data.get('issuerName'),
                    'tickerSymbol': parsed_data.get('tickerSymbol'),
                    'relationship': parsed_data.get('relationship'),
                    'relationshipTypes': parsed_data.get('relationshipTypes'),
                    'relationshipAdditionalText': parsed_data.get('relationshipAdditionalText'),
                    'eventDate': parsed_data.get('eventDate'),
                    'reportingDate': parsed_data.get('reportingDate'),
                    'address': parsed_data.get('address'),
                    'politician': parsed_data.get('politician', 0),
                    'filingType': parsed_data.get('filingType'),
                    'amendment': parsed_data.get('amendment'),
                    'formS3Key': s3_key,
                    'htmlContent': content_str  # Store full HTML for full-text search
                }
                
                # Index document
                url = f"https://{opensearch_endpoint}/{opensearch_index}/_doc/{trade_id}"
                response = requests.put(url, auth=awsauth, json=opensearch_doc, headers={'Content-Type': 'application/json'})
                
                if response.status_code in [200, 201]:
                    opensearch_duration = (datetime.now() - opensearch_start).total_seconds()
                    local_logger.info(f"   ✅ Stored to OpenSearch in {opensearch_duration:.2f}s: TradeId={trade_id}")
                    print(f"   ✅ Stored to OpenSearch in {opensearch_duration:.2f}s: TradeId={trade_id}", flush=True)
                else:
                    local_logger.warning(f"   ⚠️ OpenSearch indexing failed: {response.status_code} - {response.text}")
                    print(f"   ⚠️ OpenSearch indexing failed: {response.status_code}", flush=True)
            except Exception as opensearch_error:
                local_logger.warning(f"   ⚠️ OpenSearch error: {opensearch_error}")
                print(f"   ⚠️ OpenSearch error: {opensearch_error}", flush=True)
        
        form_duration = (datetime.now() - form_start_time).total_seconds()
        local_logger.info(f"   ✅ Form processing complete in {form_duration:.2f}s")
        print(f"   ✅ Form processing complete in {form_duration:.2f}s", flush=True)
        local_logger.info(f"      Breakdown: Download={download_duration:.2f}s, Parse={parse_duration:.2f}s, Match={match_duration:.2f}s, Store={store_duration:.2f}s")
        print(f"      Breakdown: Download={download_duration:.2f}s, Parse={parse_duration:.2f}s, Match={match_duration:.2f}s, Store={store_duration:.2f}s", flush=True)
        
        return {
            'success': True,
            'tradeId': trade_id,
            'politicianMatch': bool(politician_match),
            'downloadDuration': download_duration,
            'parseDuration': parse_duration,
            'matchDuration': match_duration,
            'storeDuration': store_duration
        }
        
    except Exception as e:
        import traceback
        error_msg = f"   ❌ FAILED: Error storing to DynamoDB: {e}"
        local_logger.error(error_msg)
        print(error_msg, flush=True)
        traceback_str = traceback.format_exc()
        local_logger.error(f"      Traceback: {traceback_str}")
        print(f"      Traceback: {traceback_str}", flush=True)
        return {'success': False, 'error': str(e), 'traceback': traceback_str}


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
    logger.info(f"✅ Stage 2 Complete: Found {len(forms)} forms in {stage2_duration:.2f} seconds")
    logger.info("=" * 80)
    
    # Step 3: Process forms
    logger.info("")
    logger.info("=" * 80)
    logger.info("📋 STAGE 3: PROCESSING FORMS")
    logger.info("=" * 80)
    stage3_start = datetime.now()
    
    # Get OpenSearch endpoint and index from job parameters if available
    opensearch_endpoint = None
    opensearch_index = None
    try:
        opensearch_endpoint = args.get('--opensearch-endpoint')
        opensearch_index = args.get('--opensearch-index', 'sec-filings')
    except:
        pass
    
    # Process forms using Spark if available, otherwise process sequentially
    if 'sc' in globals() and sc:
        logger.info("   Using Spark for parallel processing...")
        # Create RDD and process in parallel
        forms_rdd = sc.parallelize(forms)
        results = forms_rdd.map(lambda form: process_form(
            form, target_date, politicians, s3_bucket, dynamodb_table,
            opensearch_endpoint, opensearch_index
        )).collect()
    else:
        logger.info("   Processing forms sequentially (Spark not available)...")
        results = []
        for i, form in enumerate(forms, 1):
            logger.info(f"   Processing form {i}/{len(forms)}...")
            result = process_form(
                form, target_date, politicians, s3_bucket, dynamodb_table,
                opensearch_endpoint, opensearch_index
            )
            results.append(result)
    
    stage3_duration = (datetime.now() - stage3_start).total_seconds()
    
    # Count results
    successful = sum(1 for r in results if r.get('success', False))
    skipped = sum(1 for r in results if r.get('skipped', False))
    failed = sum(1 for r in results if not r.get('success', False) and not r.get('skipped', False))
    politician_matches = sum(1 for r in results if r.get('politicianMatch', False))
    
    logger.info(f"✅ Stage 3 Complete: Processed {len(results)} forms in {stage3_duration:.2f} seconds")
    logger.info(f"   ✅ Successful: {successful}")
    logger.info(f"   ⏭️  Skipped: {skipped}")
    logger.info(f"   ❌ Failed: {failed}")
    logger.info(f"   👤 Politician Matches: {politician_matches}")
    logger.info("=" * 80)
    
    # Final summary
    total_duration = (datetime.now() - stage1_start).total_seconds()
    logger.info("")
    logger.info("=" * 80)
    logger.info("🎉 JOB COMPLETE")
    logger.info("=" * 80)
    logger.info(f"📊 Summary:")
    logger.info(f"   Total Duration: {total_duration:.2f} seconds")
    logger.info(f"   Forms Found: {len(forms)}")
    logger.info(f"   Forms Processed: {successful}")
    logger.info(f"   Forms Skipped: {skipped}")
    logger.info(f"   Forms Failed: {failed}")
    logger.info(f"   Politician Matches: {politician_matches}")
    logger.info("=" * 80)
    
except Exception as e:
    logger.error(f"❌ Fatal error in SEC ETL job: {e}")
    import traceback
    logger.error(f"Traceback: {traceback.format_exc()}")
    raise
