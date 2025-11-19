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
# Helper Functions for Human-Readable Code Mappings
# ============================================================================

def get_transaction_code_meaning(code: str) -> str:
    """
    Map SEC transaction codes to human-readable meanings.
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

def get_ownership_type_meaning(code: str) -> str:
    """Map ownership form codes to human-readable meanings."""
    meanings = {
        'D': 'Direct',
        'I': 'Indirect'
    }
    return meanings.get(code.upper(), code)

def get_transaction_direction_meaning(code: str) -> str:
    """Map A/D values to human-readable meanings."""
    meanings = {
        'A': 'Acquired',
        'D': 'Disposed'
    }
    return meanings.get(code.upper(), code)

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
# Only include truly required arguments here (date is optional - will be parsed manually)
required_args = ['JOB_NAME', 's3_bucket', 'dynamodb_table']

args = getResolvedOptions(sys.argv, required_args)

job.init(args['JOB_NAME'], args)

# Extract required parameters
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

# OpenSearch disabled for MVP - agent will use DynamoDB queries + S3 file reads instead
# if opensearch_endpoint:
#     logger.info(f"🔍 OpenSearch enabled: Endpoint={opensearch_endpoint}, Index={opensearch_index or 'sec-filings'}")
#     print(f"🔍 OpenSearch enabled: Endpoint={opensearch_endpoint}, Index={opensearch_index or 'sec-filings'}", flush=True)
# else:
logger.info("ℹ️ OpenSearch disabled for MVP - documents stored to DynamoDB and S3 only")
print("ℹ️ OpenSearch disabled for MVP - documents stored to DynamoDB and S3 only", flush=True)

# Parse optional date and backdate arguments manually (since getResolvedOptions requires all args)
# Format: --date=2025-11-05 or --date 2025-11-05
# Step Functions may pass null as --date=null or --date null
target_date = None
backdate = None

for i, arg in enumerate(sys.argv):
    # Parse date argument
    if arg == '--date' or arg.startswith('--date='):
        if '=' in arg:
            date_value = arg.split('=', 1)[1]
        elif i + 1 < len(sys.argv):
            date_value = sys.argv[i + 1]
        else:
            continue
        
        # Filter out null, empty string, or string "null"
        if date_value and date_value.strip() != '' and date_value.lower() != 'null':
            target_date = date_value
    
    # Parse backdate argument
    elif arg == '--backdate' or arg.startswith('--backdate='):
        if '=' in arg:
            backdate_value = arg.split('=', 1)[1]
        elif i + 1 < len(sys.argv):
            backdate_value = sys.argv[i + 1]
        else:
            continue
        
        # Filter out null, empty string, or string "null"
        if backdate_value and backdate_value.strip() != '' and backdate_value.lower() != 'null':
            backdate = backdate_value

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
    # Default to yesterday if neither provided (for scheduler - processes previous day's filings)
    # This is the typical use case: scheduler runs daily to process yesterday's filings
    yesterday = (datetime.now().date() - timedelta(days=1))
    target_date = yesterday.strftime('%Y-%m-%d')
    is_backdate_mode = False
    logger.info(f"ℹ️ No date/backdate provided (or was null/empty), defaulting to yesterday: {target_date}")
    print(f"ℹ️ No date/backdate provided (or was null/empty), defaulting to yesterday: {target_date}", flush=True)

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


def fetch_sec_forms_paginated(target_date: str, form_types: List[str] = ['3', '4', '5']) -> List[Dict[str, Any]]:
    """
    Fetch SEC forms using Daily Index Files (recommended approach)
    
    Daily index files are available at:
    https://www.sec.gov/Archives/edgar/daily-index/{YEAR}/QTR{QUARTER}/master.{YYYYMMDD}.idx
    
    These files contain all filings for a specific date, including Forms 3, 4, 5.
    This approach is more reliable than browse-edgar HTML scraping and less likely to be blocked.
    
    Args:
        target_date: Target date in YYYY-MM-DD format (must match exactly)
        form_types: List of form types to fetch (default: ['3', '4', '5'])
    
    Returns:
        List of form metadata dicts with keys: form_type, cik, accession_number, filename, filing_date
    """
    all_forms = []
    target_date_obj = datetime.strptime(target_date, '%Y-%m-%d').date()
    
    # Filter out future dates (files won't exist yet)
    today = datetime.now().date()
    if target_date_obj > today:
        logger.warning(f"   ⚠️ Target date {target_date} is in the future, skipping")
        print(f"   ⚠️ Target date {target_date} is in the future, skipping", flush=True)
        return []
    
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
    
    date_str = target_date_obj.strftime('%Y-%m-%d')
    date_str_idx = target_date_obj.strftime('%Y%m%d')
    year = target_date_obj.year
    quarter = (target_date_obj.month - 1) // 3 + 1
    
    logger.info("")
    logger.info(f"🔍 Searching for Forms 3, 4, 5 filed on {target_date}")
    print(f"🔍 Searching for Forms 3, 4, 5 filed on {target_date}", flush=True)
    
    fetch_start = datetime.now()
    
    # Fetch forms from the single date's index file
    date_to_fetch = target_date_obj
    # Daily index file URL
    index_url = f"{SEC_BASE_URL}/Archives/edgar/daily-index/{year}/QTR{quarter}/master.{date_str_idx}.idx"
    
    logger.info(f"📥 Fetching daily index file for {date_str}...")
    print(f"📥 Fetching daily index file for {date_str}...", flush=True)
    
    try:
        # Add delay to avoid rate limiting
        time.sleep(0.3)
        
        # Retry logic for 403 errors (SEC may temporarily block rapid requests)
        max_retries = 3
        retry_delay = 1.0
        response = None
        
        for attempt in range(max_retries):
            try:
                # Use session with pre-configured headers (includes User-Agent)
                # The session already has User-Agent and other headers set
                response = session.get(index_url, timeout=30)
                
                if response.status_code == 200:
                    break  # Success, exit retry loop
                elif response.status_code == 403:
                    # SEC returns 403 for non-existent index files (weekends/holidays/future dates)
                    # Check if it's a weekend first
                    weekday = date_to_fetch.weekday()  # 0=Monday, 6=Sunday
                    if weekday >= 5:  # Saturday (5) or Sunday (6)
                        logger.info(f"   ⏭️ Skipping {date_str} (weekend - no filings)")
                        print(f"   ⏭️ Skipping {date_str} (weekend - no filings)", flush=True)
                        response = None  # Mark as skipped
                        break
                    elif attempt < max_retries - 1:
                        # Might be temporary rate limit, retry
                        wait_time = retry_delay * (2 ** attempt)  # Exponential backoff
                        logger.warning(f"   ⚠️ 403 Forbidden for {date_str} (attempt {attempt + 1}/{max_retries}), retrying in {wait_time:.1f}s...")
                        print(f"   ⚠️ 403 Forbidden for {date_str} (attempt {attempt + 1}/{max_retries}), retrying in {wait_time:.1f}s...", flush=True)
                        time.sleep(wait_time)
                        continue
                    else:
                        # Last attempt failed - likely no index file exists (holiday or not available yet)
                        logger.info(f"   ⏭️ Skipping {date_str} (403 Forbidden - likely no index file exists, may be holiday or not yet available)")
                        print(f"   ⏭️ Skipping {date_str} (403 Forbidden - likely no index file exists, may be holiday or not yet available)", flush=True)
                        response = None  # Mark as skipped
                        break
                elif response.status_code == 404:
                    logger.info(f"   ⏭️ Skipping {date_str} (404 - no index file, may be weekend/holiday)")
                    print(f"   ⏭️ Skipping {date_str} (404 - no index file, may be weekend/holiday)", flush=True)
                    response = None  # Mark as skipped
                    break  # 404 is expected for weekends/holidays, don't retry
                else:
                    response.raise_for_status()
            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    wait_time = retry_delay * (2 ** attempt)
                    logger.warning(f"   ⚠️ Request error for {date_str} (attempt {attempt + 1}/{max_retries}): {e}, retrying in {wait_time:.1f}s...")
                    print(f"   ⚠️ Request error for {date_str} (attempt {attempt + 1}/{max_retries}): {e}, retrying in {wait_time:.1f}s...", flush=True)
                    time.sleep(wait_time)
                    continue
                else:
                    raise
        
        if response is None:
            logger.info(f"   ⏭️ Skipping {date_str} (no index file available)")
            print(f"   ⏭️ Skipping {date_str} (no index file available)", flush=True)
            return []
        
        if response.status_code != 200:
            logger.error(f"   ❌ Unexpected status code {response.status_code} for {date_str}")
            print(f"   ❌ Unexpected status code {response.status_code} for {date_str}", flush=True)
            return []
        
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
            return []
        
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
                # Be strict: only match exact form types (3, 4, 5) or "FORM 3", "FORM 4", "FORM 5"
                # Don't match numbers from other form types like "N-MFP3", "10-K", "8-K", etc.
                form_num = None
                
                # Try exact match first (e.g., "3", "4", "5")
                if form_type_raw.strip() in form_type_nums:
                    form_num = form_type_raw.strip()
                else:
                    # Try "FORM 3", "FORM 4", "FORM 5" pattern (case insensitive)
                    form_match = re.search(r'\bFORM\s+([345])\b', form_type_raw, re.IGNORECASE)
                    if form_match:
                        form_num = form_match.group(1)
                
                if form_num and form_num in form_type_nums:
                    # Parse filing date for comparison
                    try:
                        filing_date_obj = datetime.strptime(date_filed, '%Y%m%d').date()
                        filing_date_str = filing_date_obj.strftime('%Y-%m-%d')
                        
                        # Only include forms matching target_date exactly
                        if date_filed != date_str_idx:
                            continue
                        
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
                                'filing_date': filing_date_str,  # Use parsed date in YYYY-MM-DD format
                                'company_name': company_name,
                                'filename': filename
                            }
                            forms_for_date.append(form_data)
                    except (ValueError, TypeError):
                        # If date parsing fails, skip this form
                        continue
            except Exception:
                continue
        
        all_forms.extend(forms_for_date)
        
        if forms_for_date:
            logger.info(f"   ✅ Found {len(forms_for_date)} Forms 3/4/5 for {date_str}")
            print(f"   ✅ Found {len(forms_for_date)} Forms 3/4/5 for {date_str}", flush=True)
        
    except requests.exceptions.RequestException as e:
        logger.error(f"   ❌ Error fetching index file for {date_str}: {e}")
        print(f"   ❌ Error fetching index file for {date_str}: {e}", flush=True)
    except Exception as e:
        logger.error(f"   ❌ Unexpected error processing index file for {date_str}: {e}")
        print(f"   ❌ Unexpected error processing index file for {date_str}: {e}", flush=True)
        import traceback
        logger.error(f"   Traceback: {traceback.format_exc()}")
    
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
        local_logger.info(f"      📂 Will store to S3: sec/{target_date}/{form_type}-{cik}-{target_date}.{{ext}}")
        print(f"      🔗 SEC Archive Base URL: {base_url}", flush=True)
        print(f"      📂 Will store to S3: sec/{target_date}/{form_type}-{cik}-{target_date}.{{ext}}", flush=True)
        
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
        
        # Initialize variables for XML/HTML from index page (used later)
        xml_content_from_index = None
        html_content_from_index = None
        
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
                            
                            # NEW: Download BOTH XML and HTML files from links (not just one)
                            xml_content_found = None
                            html_content_found = None
                            
                            # Try each found link (matching Lambda: try up to 10 links)
                            for doc_link in sorted_links[:10]:
                                # Handle relative URLs (matching Lambda logic exactly)
                                if doc_link.startswith('/'):
                                    doc_link_full = f"https://www.sec.gov{doc_link}"
                                elif not doc_link.startswith('http'):
                                    doc_link_full = f"{base_url}/{doc_link}"
                                else:
                                    doc_link_full = doc_link
                                
                                # Skip if it's the same URL we just tried
                                if doc_link_full == file_url:
                                    continue
                                
                                # Skip index pages
                                if 'index' in doc_link.lower():
                                    continue
                                
                                local_logger.info(f"      🔗 Trying link: {doc_link_full}")
                                print(f"      🔗 Trying link: {doc_link_full}", flush=True)
                                try:
                                    # Small delay before download
                                    time.sleep(0.1)
                                    
                                    # Retry logic for 403 errors
                                    max_retries = 2
                                    retry_delay = 1
                                    doc_response = None
                                    
                                    for attempt in range(max_retries):
                                        try:
                                            doc_response = session.get(doc_link_full, timeout=30)
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
                                        
                                        # Store both XML and HTML if found (don't break after first match)
                                        if is_xml and not is_html and not xml_content_found:
                                            # Pure XML content
                                            xml_content_found = doc_content
                                            local_logger.info(f"      ✅ Found XML document: {doc_link_full}")
                                            print(f"      ✅ Found XML document: {doc_link_full}", flush=True)
                                        
                                        if is_html and not html_content_found:
                                            # HTML rendering
                                            html_content_found = doc_content
                                            local_logger.info(f"      ✅ Found HTML document: {doc_link_full}")
                                            print(f"      ✅ Found HTML document: {doc_link_full}", flush=True)
                                        
                                        # If we have both, we can stop looking
                                        if xml_content_found and html_content_found:
                                            break
                                        
                                        # Also handle other formats for backward compatibility
                                        elif b'<sec-header' in content_start and not file_content:
                                            file_content = doc_content
                                            file_ext = 'txt'
                                            content_type = 'text/plain'
                                            local_logger.info(f"      ✅ Found SGML header: {doc_link_full}")
                                            print(f"      ✅ Found SGML header: {doc_link_full}", flush=True)
                                except Exception as doc_error:
                                    local_logger.warning(f"      ⚠️ Could not download document link {doc_link_full}: {doc_error}")
                                    print(f"      ⚠️ Could not download document link {doc_link_full}: {doc_error}", flush=True)
                                    continue
                            
                            # Use XML if found, otherwise use HTML, otherwise use original file_content
                            if xml_content_found:
                                file_content = xml_content_found
                                file_ext = 'xml'
                                content_type = 'application/xml'
                            elif html_content_found:
                                file_content = html_content_found
                                file_ext = 'html'
                                content_type = 'text/html'
                            
                            # Store both for later use
                            xml_content_from_index = xml_content_found
                            html_content_from_index = html_content_found
                                    
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
        # Format: sec/{date}/{form_type}-{cik}-{accession}-{date}.{ext}
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
        
        s3_key = f"sec/{target_date}/{form_type}-{cik}-{accession_dashed}-{target_date}.{file_ext}"
        
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
        
        # NEW: Store both XML and HTML in folder structure if we found them from index page
        # Create folder name (without extension)
        folder_name = f"{form_type}-{cik}-{accession_dashed}-{target_date}"
        folder_key = f"sec/{target_date}/{folder_name}"
        
        # Use XML/HTML from index page if found, otherwise use what we downloaded
        xml_content = xml_content_from_index if xml_content_from_index else None
        html_content = html_content_from_index if html_content_from_index else (file_content if file_ext == 'html' else None)
        
        xml_s3_key = None
        html_s3_key = None
        
        # Upload both files to S3 in folder structure if we have them
        if xml_content:
            xml_s3_key = f"{folder_key}/{folder_name}.xml"
            try:
                s3_client_local.put_object(
                    Bucket=s3_bucket_name,
                    Key=xml_s3_key,
                    Body=xml_content,
                    ContentType='application/xml'
                )
                local_logger.info(f"      ✅ Uploaded XML to: {xml_s3_key}")
                print(f"      ✅ Uploaded XML to: {xml_s3_key}", flush=True)
            except Exception as e:
                local_logger.warning(f"      ⚠️ Failed to upload XML: {e}")
        
        if html_content:
            html_s3_key = f"{folder_key}/{folder_name}.html"
            try:
                s3_client_local.put_object(
                    Bucket=s3_bucket_name,
                    Key=html_s3_key,
                    Body=html_content,
                    ContentType='text/html'
                )
                local_logger.info(f"      ✅ Uploaded HTML to: {html_s3_key}")
                print(f"      ✅ Uploaded HTML to: {html_s3_key}", flush=True)
            except Exception as e:
                local_logger.warning(f"      ⚠️ Failed to upload HTML: {e}")
        
        # Upload original file only if we're NOT using folder structure (backward compatibility)
        if not (xml_content or html_content):
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
        else:
            # Using folder structure
            local_logger.info(f"      ✅ S3 UPLOAD SUCCESS (folder structure)!")
            local_logger.info(f"      📦 DOWNLOAD COMPLETE: Files ready for parsing")
            local_logger.info(f"      " + "="*70)
            print(f"      ✅ S3 UPLOAD SUCCESS (folder structure)!", flush=True)
            print(f"      📦 DOWNLOAD COMPLETE: Files ready for parsing", flush=True)
            print(f"      " + "="*70, flush=True)
        
        # Return folder key as the main S3 key (frontend can choose which file to download)
        # Use folder structure if we have XML or HTML, otherwise use original single file structure
        if xml_content or html_content:
            return {
                's3_key': folder_key + '/',  # Use folder path
                'content': xml_content if xml_content else html_content,  # Prefer XML for parsing
                'xml_content': xml_content,
                'html_content': html_content,
                'file_ext': 'xml' if xml_content else 'html' if html_content else file_ext,
                'xml_s3_key': xml_s3_key,
                'html_s3_key': html_s3_key,
                'folder_key': folder_key,
                'cik': cik,
                'accession_number': accession,
                'form_type': form_type,
                'filing_date': target_date
            }
        else:
            # Fallback to original structure if we don't have folder structure
            return {
                's3_key': s3_key,
                'content': file_content,
                'xml_content': None,
                'html_content': html_content if file_ext == 'html' else None,
                'file_ext': file_ext,
                'xml_s3_key': None,
                'html_s3_key': s3_key if file_ext == 'html' else None,
                'folder_key': None,
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


def parse_sec_form_xml(xml_content: bytes, folder_key: str, filing_date: str) -> Dict[str, Any]:
    """
    Parse SEC Form XML and extract metadata (transplanted from new script)
    
    Args:
        xml_content: XML file content as bytes
        folder_key: S3 folder key (for reference)
        filing_date: Filing date string
    
    Returns:
        Dict with parsed metadata (compatible with parse_sec_form_metadata format)
    """
    import logging
    local_logger = logging.getLogger()
    
    result = {
        'formType': None,
        'reportingPersonName': None,
        'address': None,
        'issuerName': None,
        'tickerSymbol': None,
        'relationship': None,
        'relationshipAdditionalText': None,
        'eventDate': None,
        'reportingDate': None,
        'amendmentDate': None,
        'signatureName': None,
        'filingType': None,
    }
    
    try:
        # Parse XML
        root = ET.fromstring(xml_content)
        
        # Get namespace if present
        ns = {}
        if root.tag.startswith('{'):
            ns_uri = root.tag.split('}')[0][1:]
            ns = {'ns': ns_uri}
        
        # Helper to find text with namespace handling
        def find_text(elem, path, default=''):
            if ns and 'ns' in ns:
                found = elem.find(f'.//ns:{path}', ns)
                if found is not None:
                    return found.text or default
            found = elem.find(f'.//{path}')
            if found is not None:
                return found.text or default
            return default
        
        def find_value(elem, path, default=''):
            """Find value element (common pattern in SEC XML)"""
            value_elem = None
            if ns and 'ns' in ns:
                value_elem = elem.find(f'.//ns:{path}/ns:value', ns)
                if value_elem is None:
                    value_elem = elem.find(f'.//{path}/value')
            else:
                value_elem = elem.find(f'.//{path}/value')
            
            if value_elem is not None:
                return value_elem.text or default
            return default
        
        # Extract form type
        doc_type = find_text(root, 'documentType', '')
        result['formType'] = f"form{doc_type}" if doc_type else None
        
        # Extract period of report (event date)
        period_of_report = find_text(root, 'periodOfReport', '')
        if period_of_report:
            try:
                # Convert YYYY-MM-DD to YYYY-MM-DD (keep same format for consistency)
                date_obj = datetime.strptime(period_of_report, '%Y-%m-%d')
                result['eventDate'] = date_obj.strftime('%Y-%m-%d')
                result['reportingDate'] = date_obj.strftime('%Y-%m-%d')
            except:
                result['eventDate'] = period_of_report
                result['reportingDate'] = period_of_report
        
        # Extract Form 3 specific fields
        no_securities_owned = find_text(root, 'noSecuritiesOwned', '')
        if no_securities_owned:
            # 0 = false (owns securities), 1 = true (owns no securities)
            result['noSecuritiesOwned'] = no_securities_owned in ['1', 'true', 'True', 'TRUE']
        
        # Extract issuer information
        issuer = root.find('.//issuer') if not ns or 'ns' not in ns else root.find('.//ns:issuer', ns)
        if issuer is not None:
            result['issuerName'] = find_text(issuer, 'issuerName', '').lower()
            result['tickerSymbol'] = find_text(issuer, 'issuerTradingSymbol', '')
        
        # Extract ALL reporting owners (Forms 3/4/5 can have multiple reporting owners in joint/group filings)
        reporting_owners = root.findall('.//reportingOwner') if not ns or 'ns' not in ns else root.findall('.//ns:reportingOwner', ns)
        all_reporting_owners = []
        
        if reporting_owners:
            for owner in reporting_owners:
                owner_data = {}
                
                # Get owner ID (CIK and name)
                owner_id = owner.find('reportingOwnerId') if not ns or 'ns' not in ns else owner.find('ns:reportingOwnerId', ns)
                if owner_id is not None:
                    owner_data['cik'] = find_text(owner_id, 'rptOwnerCik', '')
                    owner_data['name'] = find_text(owner_id, 'rptOwnerName', '').strip()
                
                # Get address
                owner_address = owner.find('reportingOwnerAddress') if not ns or 'ns' not in ns else owner.find('ns:reportingOwnerAddress', ns)
                if owner_address is not None:
                    address_parts = []
                    
                    street1 = find_text(owner_address, 'rptOwnerStreet1', '').strip()
                    street2 = find_text(owner_address, 'rptOwnerStreet2', '').strip()
                    city = find_text(owner_address, 'rptOwnerCity', '').strip()
                    state = find_text(owner_address, 'rptOwnerState', '').strip()
                    zip_code = find_text(owner_address, 'rptOwnerZipCode', '').strip()
                    
                    if street1:
                        address_parts.append(street1)
                    if street2:
                        address_parts.append(street2)
                    if city:
                        # Convert to title case
                        city = city.title()
                        address_parts.append(city)
                    if state:
                        address_parts.append(state)
                    if zip_code:
                        address_parts.append(zip_code)
                    
                    if address_parts:
                        owner_data['address'] = ", ".join(address_parts)
                
                # Get relationship
                owner_rel = owner.find('reportingOwnerRelationship') if not ns or 'ns' not in ns else owner.find('ns:reportingOwnerRelationship', ns)
                if owner_rel is not None:
                    # Build relationship code (bitmask: 1=Director, 2=Officer, 4=10%Owner, 8=Other)
                    rel_code = 0
                    relationship_additional_dict = {}
                    
                    # Check for boolean or numeric values
                    is_director = find_text(owner_rel, 'isDirector', '')
                    is_officer = find_text(owner_rel, 'isOfficer', '')
                    is_ten_percent = find_text(owner_rel, 'isTenPercentOwner', '')
                    is_other = find_text(owner_rel, 'isOther', '')
                    
                    # Convert to boolean
                    if is_director in ['1', 'true', 'True', 'TRUE']:
                        rel_code |= 1
                    if is_officer in ['1', 'true', 'True', 'TRUE']:
                        rel_code |= 2
                    if is_ten_percent in ['1', 'true', 'True', 'TRUE']:
                        rel_code |= 4
                    if is_other in ['1', 'true', 'True', 'TRUE']:
                        rel_code |= 8
                    
                    owner_data['relationship'] = str(rel_code) if rel_code > 0 else None
                    
                    # Extract additional text for Officer and Other
                    officer_text = find_text(owner_rel, 'officerText', '').strip()
                    other_text = find_text(owner_rel, 'otherText', '').strip()
                    officer_title = find_text(owner_rel, 'officerTitle', '').strip()
                    
                    if officer_text:
                        relationship_additional_dict['Officer'] = officer_text
                    elif officer_title:
                        relationship_additional_dict['Officer'] = officer_title
                    
                    if other_text:
                        relationship_additional_dict['Other'] = other_text
                    
                    if relationship_additional_dict:
                        owner_data['relationshipAdditionalText'] = relationship_additional_dict
                
                all_reporting_owners.append(owner_data)
        
        # Store all reporting owners
        result['reportingOwners'] = all_reporting_owners if all_reporting_owners else None
        
        # For backward compatibility and GSI queries, use the FIRST reporting owner as primary
        # (This maintains existing query patterns while allowing detailed analysis of all owners)
        if all_reporting_owners:
            primary_owner = all_reporting_owners[0]
            result['reportingPersonName'] = primary_owner.get('name', '').lower() if primary_owner.get('name') else None
            result['address'] = primary_owner.get('address')
            result['relationship'] = primary_owner.get('relationship')
            result['relationshipAdditionalText'] = primary_owner.get('relationshipAdditionalText')
        
        # Extract ALL signatures (Forms can have multiple signatures for joint/group filings)
        signatures = root.findall('.//ownerSignature') if not ns or 'ns' not in ns else root.findall('.//ns:ownerSignature', ns)
        all_signatures = []
        
        if signatures:
            for signature in signatures:
                sig_data = {}
                sig_name = find_text(signature, 'signatureName', '').strip()
                sig_date = find_text(signature, 'signatureDate', '')
                
                if sig_name:
                    sig_data['signatureName'] = sig_name
                if sig_date:
                    try:
                        date_obj = datetime.strptime(sig_date, '%Y-%m-%d')
                        sig_data['signatureDate'] = date_obj.strftime('%Y-%m-%d')
                    except:
                        sig_data['signatureDate'] = sig_date
                
                if sig_data:
                    all_signatures.append(sig_data)
        
        # Store all signatures
        result['signatures'] = all_signatures if all_signatures else None
        
        # For backward compatibility, use the FIRST signature as primary
        if all_signatures:
            primary_sig = all_signatures[0]
            result['signatureName'] = primary_sig.get('signatureName', '').lower() if primary_sig.get('signatureName') else None
            sig_date = primary_sig.get('signatureDate', '')
            if sig_date:
                # Use signature date for reportingDate if periodOfReport wasn't set
                if not result.get('reportingDate'):
                    result['reportingDate'] = sig_date
        
        # Extract footnotes first (needed for footnoteId references)
        footnotes_dict = {}
        footnotes_elem = root.find('.//footnotes') if not ns or 'ns' not in ns else root.find('.//ns:footnotes', ns)
        if footnotes_elem is not None:
            footnote_elems = footnotes_elem.findall('.//footnote') if not ns or 'ns' not in ns else footnotes_elem.findall('.//ns:footnote', ns)
            for footnote_elem in footnote_elems:
                footnote_id = footnote_elem.get('id', '')
                footnote_text = (footnote_elem.text or '').strip()
                if footnote_id and footnote_text:
                    footnotes_dict[footnote_id] = footnote_text
        
        # Helper to find value and footnote ID (returns dict with 'value' and/or 'footnoteId')
        def find_value_with_footnote(elem, path):
            """
            Find element and return dict with 'value' and/or 'footnoteId'.
            Returns:
                - String if only value exists (no footnote) - for backward compatibility
                - Dict with 'value' and 'footnoteId' if both exist
                - Dict with 'footnoteId' if only footnote exists
                - None if neither exists
            """
            if ns and 'ns' in ns:
                found = elem.find(f'.//ns:{path}', ns)
            else:
                found = elem.find(f'.//{path}')
            
            if found is None:
                return None
            
            result_dict = {}
            
            # Check for value element
            value_elem = found.find('value') if not ns or 'ns' not in ns else found.find('ns:value', ns)
            if value_elem is not None and value_elem.text:
                result_dict['value'] = value_elem.text.strip()
            elif found.text and found.text.strip():
                # Fallback: use element text directly
                result_dict['value'] = found.text.strip()
            
            # Check for footnoteId reference
            footnote_id_elem = found.find('footnoteId') if not ns or 'ns' not in ns else found.find('ns:footnoteId', ns)
            if footnote_id_elem is not None:
                footnote_id = footnote_id_elem.get('id', '')
                if footnote_id:
                    result_dict['footnoteId'] = footnote_id
            
            # Return format:
            # - If only value exists (no footnote): return string for backward compatibility
            # - If footnote exists (with or without value): return dict
            if 'footnoteId' in result_dict:
                # Has footnote - return dict
                return result_dict if result_dict else None
            elif 'value' in result_dict:
                # Only value - return string for backward compatibility
                return result_dict['value']
            else:
                return None
        
        # Extract nonDerivativeTable (transactions for non-derivative securities like common stock)
        non_derivative_transactions = []
        non_derivative_table = root.find('.//nonDerivativeTable') if not ns or 'ns' not in ns else root.find('.//ns:nonDerivativeTable', ns)
        if non_derivative_table is not None:
            transactions = non_derivative_table.findall('.//nonDerivativeTransaction') if not ns or 'ns' not in ns else non_derivative_table.findall('.//ns:nonDerivativeTransaction', ns)
            
            for trans in transactions:
                transaction = {}
                
                # Security title
                transaction['securityTitle'] = find_value(trans, 'securityTitle', '')
                
                # Transaction date
                transaction['transactionDate'] = find_value(trans, 'transactionDate', '')
                
                # Transaction coding
                trans_coding = trans.find('transactionCoding') if not ns or 'ns' not in ns else trans.find('ns:transactionCoding', ns)
                if trans_coding is not None:
                    transaction['transactionCode'] = find_text(trans_coding, 'transactionCode', '')
                    transaction['transactionFormType'] = find_text(trans_coding, 'transactionFormType', '')
                    transaction['equitySwapInvolved'] = find_text(trans_coding, 'equitySwapInvolved', '')
                
                # Transaction amounts (with footnote support)
                trans_amounts = trans.find('transactionAmounts') if not ns or 'ns' not in ns else trans.find('ns:transactionAmounts', ns)
                if trans_amounts is not None:
                    transaction['transactionShares'] = find_value_with_footnote(trans_amounts, 'transactionShares')
                    transaction['transactionPricePerShare'] = find_value_with_footnote(trans_amounts, 'transactionPricePerShare')
                    transaction['transactionAcquiredDisposedCode'] = find_value_with_footnote(trans_amounts, 'transactionAcquiredDisposedCode')
                
                # Post-transaction amounts (with footnote support)
                post_trans = trans.find('postTransactionAmounts') if not ns or 'ns' not in ns else trans.find('ns:postTransactionAmounts', ns)
                if post_trans is not None:
                    transaction['sharesOwnedFollowingTransaction'] = find_value_with_footnote(post_trans, 'sharesOwnedFollowingTransaction')
                
                # Ownership nature
                ownership = trans.find('ownershipNature') if not ns or 'ns' not in ns else trans.find('ns:ownershipNature', ns)
                if ownership is not None:
                    transaction['directOrIndirectOwnership'] = find_value(ownership, 'directOrIndirectOwnership', '')
                    transaction['natureOfOwnership'] = find_value(ownership, 'natureOfOwnership', '')
                
                non_derivative_transactions.append(transaction)
        
        result['nonDerivativeTransactions'] = non_derivative_transactions if non_derivative_transactions else None
        
        # Extract derivativeTable (holdings/transactions for derivative securities like options, warrants, phantom stock)
        derivative_holdings = []
        derivative_table = root.find('.//derivativeTable') if not ns or 'ns' not in ns else root.find('.//ns:derivativeTable', ns)
        if derivative_table is not None:
            # Form 3 has derivativeHolding, Form 4/5 have derivativeTransaction
            holdings = derivative_table.findall('.//derivativeHolding') if not ns or 'ns' not in ns else derivative_table.findall('.//ns:derivativeHolding', ns)
            transactions = derivative_table.findall('.//derivativeTransaction') if not ns or 'ns' not in ns else derivative_table.findall('.//ns:derivativeTransaction', ns)
            
            # Process holdings (Form 3)
            for holding in holdings:
                derivative = {}
                
                # Security title
                derivative['securityTitle'] = find_value(holding, 'securityTitle', '')
                
                # Conversion/exercise price (may be footnote)
                conversion_price = find_value_with_footnote(holding, 'conversionOrExercisePrice')
                if conversion_price:
                    derivative['conversionOrExercisePrice'] = conversion_price
                
                # Exercise date (may be footnote)
                exercise_date = find_value_with_footnote(holding, 'exerciseDate')
                if exercise_date:
                    derivative['exerciseDate'] = exercise_date
                
                # Expiration date (may be footnote)
                expiration_date = find_value_with_footnote(holding, 'expirationDate')
                if expiration_date:
                    derivative['expirationDate'] = expiration_date
                
                # Underlying security
                underlying = holding.find('underlyingSecurity') if not ns or 'ns' not in ns else holding.find('ns:underlyingSecurity', ns)
                if underlying is not None:
                    derivative['underlyingSecurityTitle'] = find_value(underlying, 'underlyingSecurityTitle', '')
                    derivative['underlyingSecurityShares'] = find_value(underlying, 'underlyingSecurityShares', '')
                
                # Ownership nature
                ownership = holding.find('ownershipNature') if not ns or 'ns' not in ns else holding.find('ns:ownershipNature', ns)
                if ownership is not None:
                    derivative['directOrIndirectOwnership'] = find_value(ownership, 'directOrIndirectOwnership', '')
                    derivative['natureOfOwnership'] = find_value(ownership, 'natureOfOwnership', '')
                
                derivative_holdings.append(derivative)
            
            # Process transactions (Form 4/5)
            for trans in transactions:
                derivative = {}
                
                # Security title
                derivative['securityTitle'] = find_value(trans, 'securityTitle', '')
                
                # Transaction date
                derivative['transactionDate'] = find_value(trans, 'transactionDate', '')
                
                # Transaction coding
                trans_coding = trans.find('transactionCoding') if not ns or 'ns' not in ns else trans.find('ns:transactionCoding', ns)
                if trans_coding is not None:
                    derivative['transactionCode'] = find_text(trans_coding, 'transactionCode', '')
                    derivative['transactionFormType'] = find_text(trans_coding, 'transactionFormType', '')
                    derivative['equitySwapInvolved'] = find_text(trans_coding, 'equitySwapInvolved', '')
                
                # Conversion/exercise price (may be footnote)
                conversion_price = find_value_with_footnote(trans, 'conversionOrExercisePrice')
                if conversion_price:
                    derivative['conversionOrExercisePrice'] = conversion_price
                
                # Exercise date (may be footnote)
                exercise_date = find_value_with_footnote(trans, 'exerciseDate')
                if exercise_date:
                    derivative['exerciseDate'] = exercise_date
                
                # Expiration date (may be footnote)
                expiration_date = find_value_with_footnote(trans, 'expirationDate')
                if expiration_date:
                    derivative['expirationDate'] = expiration_date
                
                # Transaction amounts (for Form 4/5 transactions) - with footnote support
                trans_amounts = trans.find('transactionAmounts') if not ns or 'ns' not in ns else trans.find('ns:transactionAmounts', ns)
                if trans_amounts is not None:
                    derivative['transactionShares'] = find_value_with_footnote(trans_amounts, 'transactionShares')
                    derivative['transactionPricePerShare'] = find_value_with_footnote(trans_amounts, 'transactionPricePerShare')
                    derivative['transactionAcquiredDisposedCode'] = find_value_with_footnote(trans_amounts, 'transactionAcquiredDisposedCode')
                
                # Post-transaction amounts (with footnote support)
                post_trans = trans.find('postTransactionAmounts') if not ns or 'ns' not in ns else trans.find('ns:postTransactionAmounts', ns)
                if post_trans is not None:
                    derivative['sharesOwnedFollowingTransaction'] = find_value_with_footnote(post_trans, 'sharesOwnedFollowingTransaction')
                
                # Underlying security
                underlying = trans.find('underlyingSecurity') if not ns or 'ns' not in ns else trans.find('ns:underlyingSecurity', ns)
                if underlying is not None:
                    derivative['underlyingSecurityTitle'] = find_value(underlying, 'underlyingSecurityTitle', '')
                    derivative['underlyingSecurityShares'] = find_value(underlying, 'underlyingSecurityShares', '')
                
                # Ownership nature
                ownership = trans.find('ownershipNature') if not ns or 'ns' not in ns else trans.find('ns:ownershipNature', ns)
                if ownership is not None:
                    derivative['directOrIndirectOwnership'] = find_value(ownership, 'directOrIndirectOwnership', '')
                    derivative['natureOfOwnership'] = find_value(ownership, 'natureOfOwnership', '')
                
                derivative_holdings.append(derivative)
        
        result['derivativeHoldings'] = derivative_holdings if derivative_holdings else None
        
        # Store footnotes as a dictionary
        result['footnotes'] = footnotes_dict if footnotes_dict else None
        
        # Extract remarks if present
        remarks_elem = root.find('.//remarks') if not ns or 'ns' not in ns else root.find('.//ns:remarks', ns)
        if remarks_elem is not None:
            remarks_text = (remarks_elem.text or '').strip()
            if remarks_text:
                result['remarks'] = remarks_text
        
        local_logger.info(f"   ✅ Parsed XML metadata successfully")
        local_logger.info(f"      - Reporting owners: {len(all_reporting_owners)}")
        local_logger.info(f"      - Signatures: {len(all_signatures)}")
        local_logger.info(f"      - Non-derivative transactions: {len(non_derivative_transactions)}")
        local_logger.info(f"      - Derivative holdings/transactions: {len(derivative_holdings)}")
        local_logger.info(f"      - Footnotes: {len(footnotes_dict)}")
        if result.get('noSecuritiesOwned') is not None:
            local_logger.info(f"      - No Securities Owned: {result['noSecuritiesOwned']}")
        print(f"   ✅ Parsed XML metadata successfully", flush=True)
        print(f"      - Reporting owners: {len(all_reporting_owners)}", flush=True)
        print(f"      - Signatures: {len(all_signatures)}", flush=True)
        print(f"      - Non-derivative transactions: {len(non_derivative_transactions)}", flush=True)
        print(f"      - Derivative holdings/transactions: {len(derivative_holdings)}", flush=True)
        print(f"      - Footnotes: {len(footnotes_dict)}", flush=True)
        
        return result
        
    except ET.ParseError as e:
        local_logger.error(f"   ❌ XML Parse Error: {e}")
        print(f"   ❌ XML Parse Error: {e}", flush=True)
        return result
    except Exception as e:
        local_logger.error(f"   ❌ Error parsing XML: {e}")
        import traceback
        local_logger.error(traceback.format_exc())
        print(f"   ❌ Error parsing XML: {e}", flush=True)
        return result


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
        'relationship': None,  # Relationship combination code (bitmask) for GSI
        'relationshipAdditionalText': None,  # Additional text for Officer/Other (dict, stored as JSON object)
        'filingType': None,  # 'individual' or 'joint/group'
        'signatureName': None,
        'amendmentDate': None,  # Amendment date if form is an amendment (GSI)
    }
    
    try:
        # Extract reporting person name (1. Name and Address of Reporting Person)
        name_match = re.search(r'<a[^>]*href="[^"]*cgi-bin/browse-edgar[^"]*CIK=\d+[^"]*">([^<]+)</a>', html_content, re.IGNORECASE)
        if name_match:
            result['reportingPersonName'] = unescape(name_match.group(1)).strip().lower()
        
        # Extract address (Street, City, State, Zip)
        address_parts = []
        
        # Extract all street lines from the table before (Street) label
        name_to_street_section = re.search(
            r'<a[^>]*href="[^"]*cgi-bin/browse-edgar[^"]*CIK=\d+[^"]*">[^<]+</a>.*?</table>(.*?)<hr[^>]*>\s*<span[^>]*>\(Street\)',
            html_content, re.IGNORECASE | re.DOTALL
        )
        
        if name_to_street_section:
            section = name_to_street_section.group(1)
            all_tables = list(re.finditer(
                r'<table[^>]*border="0"[^>]*width="100%"[^>]*>(.*?)</table>',
                section, re.IGNORECASE | re.DOTALL
            ))
            
            street_table_content = None
            for table_match in reversed(all_tables):
                table_content = table_match.group(1)
                if 'FormData' in table_content and '(Last)' not in table_content and '(First)' not in table_content:
                    street_table_content = table_content
                    break
            
            if street_table_content:
                street_lines = re.findall(
                    r'<tr><td><span[^>]*class="FormData"[^>]*>([^<]*)</span></td></tr>',
                    street_table_content, re.IGNORECASE | re.DOTALL
                )
                for street_line in street_lines:
                    street = unescape(street_line).strip()
                    if street:
                        address_parts.append(street)
        
        # City, State, Zip
        city_state_zip_patterns = [
            r'\(Street\)[^<]*</span><table[^>]*border="0"[^>]*width="100%"[^>]*>.*?<tr>.*?<td[^>]*width="33%"[^>]*><span[^>]*class="FormData"[^>]*>([^<]+)</span></td>.*?<td[^>]*width="33%"[^>]*><span[^>]*class="FormData"[^>]*>([^<]+)</span></td>.*?<td[^>]*width="33%"[^>]*><span[^>]*class="FormData"[^>]*>([^<]+)</span></td>.*?</tr>.*?</table>',
            r'\(Street\)[^<]*</span><table[^>]*>.*?<tr>.*?<td[^>]*><span[^>]*class="FormData"[^>]*>([^<]+)</span></td>.*?<td[^>]*><span[^>]*class="FormData"[^>]*>([^<]+)</span></td>.*?<td[^>]*><span[^>]*class="FormData"[^>]*>([^<]+)</span></td>.*?</tr>.*?</table>',
            r'\(Street\)[^<]*<table[^>]*border="0"[^>]*width="100%"[^>]*>.*?<tr>.*?<td[^>]*width="33%"[^>]*><span[^>]*class="FormData"[^>]*>([^<]+)</span></td>.*?<td[^>]*width="33%"[^>]*><span[^>]*class="FormData"[^>]*>([^<]+)</span></td>.*?<td[^>]*width="33%"[^>]*><span[^>]*class="FormData"[^>]*>([^<]+)</span></td>.*?</tr>.*?</table>',
        ]
        
        csv_match = None
        for pattern in city_state_zip_patterns:
            csv_match = re.search(pattern, html_content, re.IGNORECASE | re.DOTALL)
            if csv_match:
                break
        
        if csv_match:
            city = unescape(csv_match.group(1)).strip()
            state = unescape(csv_match.group(2)).strip()
            zip_code = unescape(csv_match.group(3)).strip()
            if city:
                address_parts.append(city)
            if state:
                address_parts.append(state)
            if zip_code:
                address_parts.append(zip_code)
        
        if address_parts:
            result['address'] = ", ".join(address_parts)
        else:
            result['address'] = None
        
        # Extract reporting date
        if accepted_date_str:
            date_part = accepted_date_str.split()[0] if ' ' in accepted_date_str else accepted_date_str
            result['reportingDate'] = date_part
        else:
            signature_date_patterns = [
                r'\*\* Signature[^<]*<span[^>]*class="FormData"[^>]*>(\d{1,2}/\d{1,2}/\d{4})</span>',
                r'Signature[^<]*Date[^<]*<span[^>]*class="FormData"[^>]*>(\d{1,2}/\d{1,2}/\d{4})</span>',
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
                except Exception as e:
                    local_logger.warning(f"   ⚠️ Could not parse reportingDate '{date_str}': {e}")
        
        # Extract issuer name and ticker symbol
        issuer_match = re.search(
            r'Issuer Name[^<]*<b>and</b>[^<]*Ticker[^<]*</span>[^<]*<br[^>]*>[^<]*<a[^>]*>([^<]+)</a>',
            html_content,
            re.IGNORECASE | re.DOTALL
        )
        if issuer_match:
            result['issuerName'] = unescape(issuer_match.group(1)).strip().lower()
        
        ticker_match = re.search(r'\[ <span[^>]*class="FormData"[^>]*>([A-Z0-9]+)</span> \]', html_content)
        if ticker_match:
            result['tickerSymbol'] = ticker_match.group(1)
        
        # Extract relationship (simplified - see old script for full implementation)
        RELATIONSHIP_BITMASK = {
            'Director': 1,
            'Officer': 2,
            '10% Owner': 4,
            'Other': 8
        }
        
        relationship_types = []
        relationship_additional_dict = {}
        
        relationship_patterns = [
            r'Relationship of Reporting Person\(s\) to Issuer[^<]*(?:<br[^>]*>)?[^<]*(?:\(Check all applicable\)[^<]*)?<table[^>]*>(.*?)</table>',
            r'Relationship of Reporting Person\(s\) to Issuer.*?<table[^>]*>(.*?)</table>',
        ]
        
        relationship_table = None
        for pattern in relationship_patterns:
            relationship_section_match = re.search(pattern, html_content, re.IGNORECASE | re.DOTALL)
            if relationship_section_match:
                relationship_table = relationship_section_match.group(1)
                if relationship_table and ('Director' in relationship_table or 'Officer' in relationship_table):
                    break
        
        if relationship_table:
            rows = re.findall(r'<tr[^>]*>(.*?)</tr>', relationship_table, re.IGNORECASE | re.DOTALL)
            
            if len(rows) >= 2:
                checkbox_row = rows[1]
                checkbox_cells = re.findall(r'<td[^>]*>(.*?)</td>', checkbox_row, re.IGNORECASE | re.DOTALL)
                
                for pair_idx in [0, 2]:
                    if pair_idx + 1 < len(checkbox_cells):
                        checkbox_cell = checkbox_cells[pair_idx]
                        text_cell = checkbox_cells[pair_idx + 1]
                        
                        has_x = bool(re.search(r'<span[^>]*class="FormData"[^>]*>X</span>', checkbox_cell, re.IGNORECASE))
                        if not has_x:
                            has_x = 'X' in checkbox_cell.strip()
                        
                        text_content = re.sub(r'<[^>]+>', '', text_cell).strip()
                        text_lower = text_content.lower()
                        
                        if has_x and text_content:
                            if 'officer' in text_lower:
                                relationship_types.append('Officer')
                            elif 'other' in text_lower:
                                relationship_types.append('Other')
                
                if len(rows) >= 1:
                    first_row = rows[0]
                    first_row_cells = re.findall(r'<td[^>]*>(.*?)</td>', first_row, re.IGNORECASE | re.DOTALL)
                    
                    for pair_idx in [0, 2]:
                        if pair_idx + 1 < len(first_row_cells):
                            checkbox_cell = first_row_cells[pair_idx]
                            text_cell = first_row_cells[pair_idx + 1]
                            
                            has_x = bool(re.search(r'<span[^>]*class="FormData"[^>]*>X</span>', checkbox_cell, re.IGNORECASE))
                            if not has_x:
                                has_x = 'X' in checkbox_cell.strip()
                            
                            text_content = re.sub(r'<[^>]+>', '', text_cell).strip()
                            text_lower = text_content.lower()
                            
                            if has_x and text_content:
                                if 'director' in text_lower and 'Director' not in relationship_types:
                                    relationship_types.append('Director')
                                elif '10%' in text_content and 'owner' in text_lower and '10% Owner' not in relationship_types:
                                    relationship_types.append('10% Owner')
            
            # Extract additional text (simplified)
            if len(rows) >= 3:
                additional_text_row = rows[2]
                additional_text_cells = re.findall(r'<td[^>]*>(.*?)</td>', additional_text_row, re.IGNORECASE | re.DOTALL)
                
                if len(additional_text_cells) > 1:
                    officer_cell_html = additional_text_cells[1]
                    officer_text = re.sub(r'<[^>]+>', '', officer_cell_html).strip()
                    officer_text = unescape(officer_text) if officer_text else None
                    if officer_text:
                        relationship_additional_dict['Officer'] = officer_text
                
                if len(additional_text_cells) > 3:
                    other_cell_html = additional_text_cells[3]
                    other_text = re.sub(r'<[^>]+>', '', other_cell_html).strip()
                    other_text = unescape(other_text) if other_text else None
                    if other_text:
                        relationship_additional_dict['Other'] = other_text
        
        relationship_code = 0
        for rel_type in relationship_types:
            if rel_type in RELATIONSHIP_BITMASK:
                relationship_code |= RELATIONSHIP_BITMASK[rel_type]
        
        result['relationship'] = str(relationship_code) if relationship_code > 0 else None
        result['relationshipAdditionalText'] = relationship_additional_dict if relationship_additional_dict else None
        
        # Extract filing type
        filing_patterns = [
            r'Individual or Joint/Group Filing[^<]*(?:\(Check Applicable Line\)[^<]*)?<table[^>]*>(.*?)</table>',
        ]
        
        filing_table = None
        for pattern in filing_patterns:
            individual_filing_match = re.search(pattern, html_content, re.IGNORECASE | re.DOTALL)
            if individual_filing_match:
                filing_table = individual_filing_match.group(1)
                break
        
        if filing_table:
            rows = re.findall(r'<tr[^>]*>(.*?)</tr>', filing_table, re.IGNORECASE | re.DOTALL)
            for row_html in rows:
                cells = re.findall(r'<td[^>]*>(.*?)</td>', row_html, re.IGNORECASE | re.DOTALL)
                if len(cells) >= 2:
                    checkbox_cell = cells[0]
                    text_cell = cells[1]
                    
                    has_x = bool(re.search(r'<span[^>]*class="FormData"[^>]*>X</span>', checkbox_cell, re.IGNORECASE))
                    if not has_x:
                        has_x = 'X' in checkbox_cell.strip()
                    
                    text_content = re.sub(r'<[^>]+>', '', text_cell).strip()
                    text_lower = text_content.lower()
                    
                    if has_x and text_content:
                        if 'one reporting person' in text_lower or 'individual' in text_lower:
                            result['filingType'] = 'individual'
                            break
                        elif 'more than one' in text_lower or 'joint' in text_lower or 'group' in text_lower:
                            result['filingType'] = 'joint/group'
                            break
        
        # Extract signature name (simplified)
        signature_patterns = [
            (r'<u><span[^>]*class="FormData"[^>]*>(/s/|s/)\s*([^<]+)</span></u>', 2),
            (r'\*\* Signature[^<]*<u><span[^>]*class="FormData"[^>]*>(/s/|s/)\s*([^<]+)</span></u>', 2),
            (r'\*\* Signature[^<]*<u><span[^>]*class="FormData"[^>]*>([^<]+)</span></u>', 1),
        ]
        
        signature_name = None
        for pattern, group_idx in signature_patterns:
            signature_match = re.search(pattern, html_content, re.IGNORECASE | re.DOTALL)
            if signature_match:
                if group_idx <= len(signature_match.groups()):
                    signature_name = unescape(signature_match.group(group_idx)).strip()
                    if signature_name and 'see exhibit' not in signature_name.lower():
                        signature_name = re.sub(r'^[/]?s[/]\s*', '', signature_name, flags=re.IGNORECASE)
                        signature_name = signature_name.strip()
                        if signature_name:
                            break
        
        if signature_name:
            result['signatureName'] = signature_name.lower()
        
    except Exception as e:
        local_logger.error(f"❌ Error parsing form metadata: {e}")
        import traceback
        local_logger.error(f"   Traceback: {traceback.format_exc()}")
    
    return result


def parse_form3_metadata(html_content: str, form_data: Dict[str, Any], accepted_date_str: Optional[str]) -> Dict[str, Any]:
    """Parse Form 3 specific metadata."""
    import logging
    from html import unescape
    from datetime import datetime
    local_logger = logging.getLogger()
    
    result = _parse_common_metadata(html_content, form_data, accepted_date_str, '3')
    
    try:
        event_date_patterns = [
            r'2\.\s*Date of Event Requiring Statement[^<]*(?:\(Month/Day/Year\)[^<]*)?</span>[^<]*(?:<br[^>]*>)?[^<]*<span[^>]*class="FormData"[^>]*>(\d{1,2}/\d{1,2}/\d{4})</span>',
            r'Date of Event Requiring Statement[^<]*(?:\(Month/Day/Year\)[^<]*)?</span>[^<]*(?:<br[^>]*>)?[^<]*<span[^>]*class="FormData"[^>]*>(\d{1,2}/\d{1,2}/\d{4})</span>',
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
            except Exception as e:
                local_logger.warning(f"   ⚠️ Could not parse eventDate '{date_str}': {e}")
        
        amendment_date_patterns = [
            r'5\.\s*If Amendment, Date of Original Filed[^<]*(?:\(Month/Day/Year\)[^<]*)?</span>[^<]*(?:<br[^>]*>)?[^<]*<span[^>]*class="FormData"[^>]*>(\d{1,2}/\d{1,2}/\d{4})</span>',
        ]
        
        amendment_date_match = None
        for pattern in amendment_date_patterns:
            amendment_date_match = re.search(pattern, html_content, re.IGNORECASE | re.DOTALL)
            if amendment_date_match:
                break
        
        if amendment_date_match:
            try:
                date_str = amendment_date_match.group(1)
                date_obj = datetime.strptime(date_str, '%m/%d/%Y')
                result['amendmentDate'] = date_obj.strftime('%Y-%m-%d')
            except Exception as e:
                local_logger.warning(f"   ⚠️ Could not parse amendmentDate '{date_str}': {e}")
        else:
            result['amendmentDate'] = None
        
    except Exception as e:
        local_logger.error(f"❌ Error parsing Form 3 metadata: {e}")
        import traceback
        local_logger.error(f"   Traceback: {traceback.format_exc()}")
    
    return result


def parse_form4_metadata(html_content: str, form_data: Dict[str, Any], accepted_date_str: Optional[str]) -> Dict[str, Any]:
    """Parse Form 4 specific metadata."""
    import logging
    from html import unescape
    from datetime import datetime
    local_logger = logging.getLogger()
    
    result = _parse_common_metadata(html_content, form_data, accepted_date_str, '4')
    
    try:
        event_date_patterns = [
            r'3\.\s*Date of Earliest Transaction[^<]*(?:\(Month/Day/Year\)[^<]*)?</span>[^<]*(?:<br[^>]*>)?[^<]*<span[^>]*class="FormData"[^>]*>(\d{1,2}/\d{1,2}/\d{4})</span>',
            r'Date of Earliest Transaction[^<]*(?:\(Month/Day/Year\)[^<]*)?</span>[^<]*(?:<br[^>]*>)?[^<]*<span[^>]*class="FormData"[^>]*>(\d{1,2}/\d{1,2}/\d{4})</span>',
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
            except Exception as e:
                local_logger.warning(f"   ⚠️ Could not parse eventDate '{date_str}': {e}")
        
        amendment_date_patterns = [
            r'4\.\s*If Amendment, Date of Original Filed[^<]*(?:\(Month/Day/Year\)[^<]*)?</span>[^<]*(?:<br[^>]*>)?[^<]*<span[^>]*class="FormData"[^>]*>(\d{1,2}/\d{1,2}/\d{4})</span>',
        ]
        
        amendment_date_match = None
        for pattern in amendment_date_patterns:
            amendment_date_match = re.search(pattern, html_content, re.IGNORECASE | re.DOTALL)
            if amendment_date_match:
                break
        
        if amendment_date_match:
            try:
                date_str = amendment_date_match.group(1)
                date_obj = datetime.strptime(date_str, '%m/%d/%Y')
                result['amendmentDate'] = date_obj.strftime('%Y-%m-%d')
            except Exception as e:
                local_logger.warning(f"   ⚠️ Could not parse amendmentDate '{date_str}': {e}")
        else:
            result['amendmentDate'] = None
        
    except Exception as e:
        local_logger.error(f"❌ Error parsing Form 4 metadata: {e}")
        import traceback
        local_logger.error(f"   Traceback: {traceback.format_exc()}")
    
    return result


def parse_form5_metadata(html_content: str, form_data: Dict[str, Any], accepted_date_str: Optional[str]) -> Dict[str, Any]:
    """Parse Form 5 specific metadata."""
    import logging
    from html import unescape
    from datetime import datetime
    local_logger = logging.getLogger()
    
    result = _parse_common_metadata(html_content, form_data, accepted_date_str, '5')
    
    try:
        event_date_patterns = [
            r'3\.\s*Statement for Issuer\'s Fiscal Year Ended[^<]*(?:\(Month/Day/Year\)[^<]*)?</span>[^<]*(?:<br[^>]*>)?[^<]*<span[^>]*class="FormData"[^>]*>(\d{1,2}/\d{1,2}/\d{4})</span>',
            r'Statement for Issuer\'s Fiscal Year Ended[^<]*(?:\(Month/Day/Year\)[^<]*)?</span>[^<]*(?:<br[^>]*>)?[^<]*<span[^>]*class="FormData"[^>]*>(\d{1,2}/\d{1,2}/\d{4})</span>',
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
            except Exception as e:
                local_logger.warning(f"   ⚠️ Could not parse eventDate '{date_str}': {e}")
        
        amendment_date_patterns = [
            r'4\.\s*If Amendment, Date of Original Filed[^<]*(?:\(Month/Day/Year\)[^<]*)?</span>[^<]*(?:<br[^>]*>)?[^<]*<span[^>]*class="FormData"[^>]*>(\d{1,2}/\d{1,2}/\d{4})</span>',
        ]
        
        amendment_date_match = None
        for pattern in amendment_date_patterns:
            amendment_date_match = re.search(pattern, html_content, re.IGNORECASE | re.DOTALL)
            if amendment_date_match:
                break
        
        if amendment_date_match:
            try:
                date_str = amendment_date_match.group(1)
                date_obj = datetime.strptime(date_str, '%m/%d/%Y')
                result['amendmentDate'] = date_obj.strftime('%Y-%m-%d')
            except Exception as e:
                local_logger.warning(f"   ⚠️ Could not parse amendmentDate '{date_str}': {e}")
        else:
            result['amendmentDate'] = None
        
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
    
    # Helper builder for skipped results (ensures consistent metadata)
    def build_skip_result(reason: str):
        return {
            'success': False,
            'skipped': True,
            'reason': reason,
            'cik': cik,
            'accession': accession,
            'formType': form_type,
        }

    # First check: Verify filing date matches target date before downloading
    # NOTE: Since forms are fetched from daily index file for target_date, they should all match
    # However, we validate to catch any edge cases or data issues
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
    
    # If we couldn't parse the filing date, use target_date as fallback (forms from index should match)
    # Only skip if date is clearly invalid and very different from target
    if filing_date_obj is None:
        # If filing_date_str is missing or 'unknown', use target_date (forms from index should match)
        if not filing_date_str or filing_date_str == 'unknown':
            local_logger.info(f"      ℹ️ Filing date not provided, using target date: {target_date_obj}")
            print(f"      ℹ️ Filing date not provided, using target date: {target_date_obj}", flush=True)
            filing_date_obj = target_date_obj
        else:
            # Could not parse - log warning but proceed with target_date (forms from index should match)
            local_logger.warning(f"      ⚠️ Could not parse filing date '{filing_date_str}', using target date {target_date_obj} (forms from index should match)")
            print(f"      ⚠️ Could not parse filing date '{filing_date_str}', using target date {target_date_obj}", flush=True)
            filing_date_obj = target_date_obj
    
    # Only skip if date is clearly wrong (more than 7 days difference) - forms from index should match
    # Use builtins.abs() to avoid conflict with PySpark's abs() function (imported via from pyspark.sql.functions import *)
    date_diff = builtins.abs((filing_date_obj - target_date_obj).days)
    if date_diff > 7:
        local_logger.warning(f"   ⏭️ SKIPPING: Filing date {filing_date_obj} is {date_diff} days from target {target_date_obj} (likely data error)")
        print(f"   ⏭️ SKIPPING: Filing date {filing_date_obj} is {date_diff} days from target {target_date_obj}", flush=True)
        return build_skip_result('date_mismatch')
    elif filing_date_obj != target_date_obj:
        # Small difference (1-7 days) - log but proceed (could be timezone or edge case)
        local_logger.info(f"   ℹ️ Filing date {filing_date_obj} differs from target {target_date_obj} by {date_diff} days, proceeding anyway")
        print(f"   ℹ️ Filing date {filing_date_obj} differs from target {target_date_obj} by {date_diff} days, proceeding", flush=True)
    
    local_logger.info(f"   ✅ Date validation passed: {filing_date_obj} (target: {target_date_obj})")
    print(f"   ✅ Date validation passed: {filing_date_obj} (target: {target_date_obj})", flush=True)
    
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
        return build_skip_result('download_failed')
    
    s3_key = downloaded.get('s3_key', 'unknown')
    file_ext = downloaded.get('file_ext', 'unknown')
    file_size = len(downloaded.get('content', b''))
    local_logger.info(f"   ✅ Downloaded: {s3_key} (ext: {file_ext}, size: {file_size:,} bytes) in {download_duration:.2f}s")
    
    # Parse form metadata - MATCHING OLD SCRIPT PATTERN
    parse_start = datetime.now()
    local_logger.info(f"   📊 Step 3/4: Parsing form metadata from S3Key={s3_key}...")
    
    # Get content from download result (matching old script: use downloaded['content'] directly)
    xml_content = downloaded.get('xml_content')
    html_content = downloaded.get('html_content')
    folder_key = downloaded.get('folder_key')
    file_ext = downloaded.get('file_ext', 'unknown')
    file_content = downloaded.get('content')
    
    parsed_data = None
    parse_method = None
    
    # PRIORITY 1: Try XML parsing if XML content is available
    if xml_content:
        try:
            local_logger.info(f"   📊 Attempting XML parsing...")
            print(f"   📊 Attempting XML parsing...", flush=True)
            parsed_data = parse_sec_form_xml(xml_content, folder_key or s3_key, filing_date_str)
            if parsed_data:  # Don't check formType - just check if not None (matching old script)
                parse_method = 'xml'
                local_logger.info(f"   ✅ XML parsing successful")
                print(f"   ✅ XML parsing successful", flush=True)
            else:
                local_logger.warning(f"   ⚠️ XML parsing returned None, will try HTML fallback")
                print(f"   ⚠️ XML parsing returned None, will try HTML fallback", flush=True)
                parsed_data = None  # Reset to try HTML
        except Exception as xml_error:
            local_logger.warning(f"   ⚠️ XML parsing failed: {xml_error}, will try HTML fallback")
            print(f"   ⚠️ XML parsing failed: {xml_error}, will try HTML fallback", flush=True)
            import traceback
            local_logger.debug(f"      XML parsing traceback: {traceback.format_exc()}")
            parsed_data = None  # Reset to try HTML
    
    # PRIORITY 2: Fall back to HTML parsing if XML failed or not available (MATCHING OLD SCRIPT)
    if not parsed_data:
        html_content_to_parse = None
        
        # Try to get HTML content from various sources (matching old script pattern)
        if html_content:
            html_content_to_parse = html_content
            local_logger.info(f"   📊 Using HTML content from download result...")
        elif file_ext == 'html' and file_content:
            # OLD SCRIPT PATTERN: Use downloaded['content'] directly if file_ext is 'html'
            html_content_to_parse = file_content
            local_logger.info(f"   📊 Using HTML content from single file (matching old script pattern)...")
        elif file_content:
            # Check if file_content is actually HTML
            try:
                content_preview = file_content[:500].decode('utf-8', errors='ignore') if isinstance(file_content, bytes) else str(file_content)[:500]
                if '<html' in content_preview.lower() or '<!doctype html' in content_preview.lower():
                    html_content_to_parse = file_content
                    local_logger.info(f"   📊 Detected HTML in file_content...")
            except:
                pass
        
        if html_content_to_parse:
            try:
                local_logger.info(f"   📊 Attempting HTML parsing...")
                print(f"   📊 Attempting HTML parsing...", flush=True)
                
                # Convert to string if bytes (matching old script: downloaded['content'].decode('utf-8', errors='ignore'))
                if isinstance(html_content_to_parse, bytes):
                    content_str = html_content_to_parse.decode('utf-8', errors='ignore')
                else:
                    content_str = html_content_to_parse
                
                # OLD SCRIPT PATTERN: Call parse_sec_form_metadata directly, don't validate result
                parsed_data = parse_sec_form_metadata(content_str, form_data, accepted_date_str)
                
                if parsed_data:  # Don't check formType - just check if not None (matching old script)
                    parse_method = 'html'
                    local_logger.info(f"   ✅ HTML parsing successful")
                    print(f"   ✅ HTML parsing successful", flush=True)
                else:
                    local_logger.warning(f"   ⚠️ HTML parsing returned None")
                    print(f"   ⚠️ HTML parsing returned None", flush=True)
            except Exception as html_error:
                local_logger.error(f"   ❌ HTML parsing failed: {html_error}")
                print(f"   ❌ HTML parsing failed: {html_error}", flush=True)
                import traceback
                local_logger.error(f"      HTML parsing traceback: {traceback.format_exc()}")
                parsed_data = None
        else:
            # OLD SCRIPT PATTERN: Check file_ext and skip if not 'html'
            if file_ext != 'html':
                local_logger.warning(f"   ⚠️ Unsupported file type: {file_ext} (CIK={cik}, Accession={accession})")
                print(f"   ⚠️ Unsupported file type: {file_ext}", flush=True)
                return build_skip_result('unsupported_file_type')
            else:
                local_logger.warning(f"   ⚠️ No HTML content available for parsing (CIK={cik}, Accession={accession})")
                print(f"   ⚠️ No HTML content available for parsing", flush=True)
    
    # OLD SCRIPT PATTERN: Don't validate parsed_data - just proceed with whatever parse_sec_form_metadata returned
    # The old script doesn't check if parsed_data is None or has formType - it just uses whatever is returned
    # Only skip if we truly have no parsed_data (which would cause errors later)
    if not parsed_data:
        # This should rarely happen since parse_sec_form_metadata should always return a dict
        # But if it does return None, we need to skip to avoid errors
        local_logger.warning(f"   ⚠️ parse_sec_form_metadata returned None (CIK={cik}, Accession={accession})")
        print(f"   ⚠️ parse_sec_form_metadata returned None", flush=True)
        return build_skip_result('parse_returned_none')
    
    # Log which parsing method was used
    if parse_method:
        local_logger.info(f"   📋 Parsing method used: {parse_method.upper()}")
        print(f"   📋 Parsing method used: {parse_method.upper()}", flush=True)
    
    parse_duration = (datetime.now() - parse_start).total_seconds()
    
    # Log parsed data summary with full details
    local_logger.info(f"   ✅ Parsing complete in {parse_duration:.2f}s:")
    local_logger.info(f"      📊 PARSED DATA SUMMARY:")
    local_logger.info(f"         - Form Type: {parsed_data.get('formType', 'N/A')}")
    local_logger.info(f"         - ReportingPersonName: {parsed_data.get('reportingPersonName', 'N/A')}")
    local_logger.info(f"         - Address: {parsed_data.get('address', 'N/A')} {'⚠️ MISSING' if not parsed_data.get('address') else '✅'}")
    local_logger.info(f"         - Issuer: {parsed_data.get('issuerName', 'N/A')} {'⚠️ MISSING' if not parsed_data.get('issuerName') else '✅'}")
    local_logger.info(f"         - Ticker: {parsed_data.get('tickerSymbol', 'N/A')}")
    relationship_code = parsed_data.get('relationship')
    if relationship_code:
        # Decode relationship code for display (convert string to int for bitwise operations)
        try:
            rel_code_int = int(relationship_code)
            rel_names = []
            if rel_code_int & 1: rel_names.append('Director')
            if rel_code_int & 2: rel_names.append('Officer')
            if rel_code_int & 4: rel_names.append('10% Owner')
            if rel_code_int & 8: rel_names.append('Other')
            local_logger.info(f"         - Relationship Code: {relationship_code} ({', '.join(rel_names)}) ✅")
        except (ValueError, TypeError):
            local_logger.info(f"         - Relationship Code: {relationship_code} ✅")
    else:
        local_logger.info(f"         - Relationship Code: N/A ⚠️ MISSING")
    local_logger.info(f"         - Relationship Additional: {parsed_data.get('relationshipAdditionalText', 'N/A')}")
    local_logger.info(f"         - Event Date: {parsed_data.get('eventDate', 'N/A')} {'⚠️ MISSING' if not parsed_data.get('eventDate') else '✅'}")
    local_logger.info(f"         - Reporting Date: {parsed_data.get('reportingDate', 'N/A')}")
    local_logger.info(f"         - Amendment Date: {parsed_data.get('amendmentDate', 'N/A')} {'(not an amendment)' if not parsed_data.get('amendmentDate') else '(amendment)'}")
    local_logger.info(f"         - Signature Name: {parsed_data.get('signatureName', 'N/A')}")
    local_logger.info(f"         - Filing Type: {parsed_data.get('filingType', 'N/A')}")
    
    # Log transaction data summary
    reporting_owners_count = len(parsed_data.get('reportingOwners', [])) if parsed_data.get('reportingOwners') else 0
    signatures_count = len(parsed_data.get('signatures', [])) if parsed_data.get('signatures') else 0
    non_deriv_count = len(parsed_data.get('nonDerivativeTransactions', [])) if parsed_data.get('nonDerivativeTransactions') else 0
    deriv_count = len(parsed_data.get('derivativeHoldings', [])) if parsed_data.get('derivativeHoldings') else 0
    footnotes_count = len(parsed_data.get('footnotes', {})) if parsed_data.get('footnotes') else 0
    
    if reporting_owners_count > 1:
        local_logger.info(f"         - Reporting Owners: {reporting_owners_count} (JOINT/GROUP FILING)")
        print(f"         - Reporting Owners: {reporting_owners_count} (JOINT/GROUP FILING)", flush=True)
    if signatures_count > 1:
        local_logger.info(f"         - Signatures: {signatures_count} (multiple signers)")
        print(f"         - Signatures: {signatures_count} (multiple signers)", flush=True)
    
    local_logger.info(f"         - Non-Derivative Transactions: {non_deriv_count}")
    local_logger.info(f"         - Derivative Holdings/Transactions: {deriv_count}")
    local_logger.info(f"         - Footnotes: {footnotes_count}")
    if parsed_data.get('remarks'):
        remarks_preview = parsed_data['remarks'][:100] + '...' if len(parsed_data['remarks']) > 100 else parsed_data['remarks']
        local_logger.info(f"         - Remarks: {remarks_preview}")
    if parsed_data.get('noSecuritiesOwned') is not None:
        local_logger.info(f"         - No Securities Owned: {parsed_data['noSecuritiesOwned']} (Form 3)")
    
    # Print critical missing GSI fields to console for immediate visibility
    print(f"   ✅ Parsing complete in {parse_duration:.2f}s:", flush=True)
    missing_gsi_fields = []
    if not parsed_data.get('address'):
        missing_gsi_fields.append('address')
    if not parsed_data.get('issuerName'):
        missing_gsi_fields.append('issuerName')
    if not parsed_data.get('relationship'):
        missing_gsi_fields.append('relationship')
    if not parsed_data.get('eventDate'):
        missing_gsi_fields.append('eventDate')
    if missing_gsi_fields:
        print(f"      ⚠️ MISSING GSI FIELDS: {', '.join(missing_gsi_fields)}", flush=True)
    else:
        print(f"      ✅ All critical GSI fields extracted successfully", flush=True)
    
    # Check for critical missing fields (for validation)
    critical_missing = []
    if not parsed_data.get('reportingPersonName'):
        critical_missing.append('reportingPersonName')
    if not parsed_data.get('formType'):
        critical_missing.append('formType')
    if not parsed_data.get('reportingDate'):
        critical_missing.append('reportingDate')
    if critical_missing:
        local_logger.warning(f"      ⚠️ WARNING: Missing critical fields: {', '.join(critical_missing)}")
    
    # Check politician match
    match_start = datetime.now()
    local_logger.info(f"   🔍 Step 4/4: Checking politician match for name='{parsed_data.get('reportingPersonName', 'N/A')}'...")
    politician_match = None
    if parsed_data.get('reportingPersonName'):
        politician_match = find_matching_politician(parsed_data['reportingPersonName'], politicians)
        if politician_match:
            local_logger.info(f"   ✅ POLITICIAN MATCH: Name='{parsed_data['reportingPersonName']}' → Politician='{politician_match.get('name', 'N/A')}' (Score={politician_match.get('matchScore', 0):.3f})")
            parsed_data['politician'] = 1  # True (DynamoDB doesn't support boolean, use 1/0)
        else:
            local_logger.info(f"   ℹ️ NO POLITICIAN MATCH: Name='{parsed_data['reportingPersonName']}' not in politician list")
            parsed_data['politician'] = 0  # False
    else:
        local_logger.warning(f"   ⚠️ No name extracted from form, cannot check politician match")
        parsed_data['politician'] = 0  # False
    
    match_duration = (datetime.now() - match_start).total_seconds()
    local_logger.info(f"   ✅ Matching complete in {match_duration:.2f}s")
    
    # Generate trade ID
    trade_id = f"sec_{form_data.get('form_type', 'form4')}_{cik}_{accession}_{target_date.replace('-', '')}"
    parsed_data['tradeId'] = trade_id
    # Use folder key if available (allows frontend to choose XML or HTML), otherwise use single file key
    parsed_data['formS3Key'] = folder_key + '/' if folder_key else s3_key
    
    # Note: Amendment logic removed - can be determined via OpenSearch full-text search if needed
    
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
        # Simplified: Only store GSI fields + essential metadata (tradeId, formS3Key, signatureName, filingType, relationshipAdditionalText, politician)
        # OpenSearch handles full-text search on HTML content
        # IMPORTANT: GSI keys cannot be NULL or empty strings - they must be omitted from the item if missing
        # GSI key fields: formType, reportingPersonName, address, eventDate, reportingDate, issuerName, tickerSymbol, relationship, amendmentDate
        # 
        # NOTE: Form-specific checkbox fields (e.g., noLongerSubjectToSection16, rule10b51c, form3HoldingsReported, form4TransactionsReported)
        # are NOT parsed or stored in DynamoDB. These fields are available in OpenSearch via the full htmlContent field
        # for the AI agent to search through. This keeps DynamoDB lightweight with only essential metadata for fast queries.
        gsi_key_fields = {'formType', 'reportingPersonName', 'address', 'eventDate', 'reportingDate', 'issuerName', 'tickerSymbol', 'relationship', 'amendmentDate'}
        
        # Fields to store in DynamoDB (GSI fields + essential metadata + transaction data)
        fields_to_store = {
            'tradeId',  # Primary key
            'formS3Key',  # S3 key for document retrieval
            'formType',  # GSI
            'reportingPersonName',  # GSI
            'address',  # GSI
            'eventDate',  # GSI
            'reportingDate',  # GSI
            'issuerName',  # GSI
            'tickerSymbol',  # GSI
            'relationship',  # GSI
            'politician',  # GSI
            'amendmentDate',  # GSI (null if not an amendment)
            'signatureName',  # Basic metadata
            'filingType',  # Basic metadata
            'relationshipAdditionalText',  # Basic metadata
            'nonDerivativeTransactions',  # Transaction data (stored as JSON)
            'derivativeHoldings',  # Derivative holdings/transactions (stored as JSON)
            'footnotes',  # Footnotes (stored as JSON)
            'remarks',  # Remarks text
            'reportingOwners',  # All reporting owners (stored as JSON array)
            'signatures',  # All signatures (stored as JSON array)
            'noSecuritiesOwned'  # Form 3 specific: true if reporting person owns no securities
        }
        
        local_logger.info(f"      🔄 Converting to DynamoDB format (simplified: GSI fields + essential metadata only)...")
        dynamodb_item = {}
        conversion_stats = {'skipped': 0, 'converted': 0, 'errors': 0, 'null_fields': 0, 'gsi_omitted': 0, 'filtered_out': 0}
        
        for key, value in parsed_data.items():
            # Skip fields not in our simplified schema
            if key not in fields_to_store:
                conversion_stats['filtered_out'] += 1
                local_logger.debug(f"         {key}: filtered out (not in simplified schema)")
                continue
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
        
        local_logger.info(f"      ✅ Conversion complete: {conversion_stats['converted']} converted, {conversion_stats['filtered_out']} filtered out, {conversion_stats['gsi_omitted']} GSI keys omitted, {conversion_stats['errors']} errors")
        
        local_logger.info(f"      📦 DynamoDB Item Preview:")
        local_logger.info(f"         TradeId: {dynamodb_item.get('tradeId', 'N/A')}")
        local_logger.info(f"         FormType: {dynamodb_item.get('formType', 'N/A')}")
        local_logger.info(f"         ReportingPersonName: {dynamodb_item.get('reportingPersonName', 'N/A')}")
        local_logger.info(f"         IssuerName: {dynamodb_item.get('issuerName', 'N/A')}")
        local_logger.info(f"         TickerSymbol: {dynamodb_item.get('tickerSymbol', 'N/A')}")
        local_logger.info(f"         ReportingDate: {dynamodb_item.get('reportingDate', 'N/A')}")
        local_logger.info(f"         EventDate: {dynamodb_item.get('eventDate', 'N/A')} {'⚠️ OMITTED (null/empty GSI)' if 'eventDate' not in dynamodb_item else ''}")
        local_logger.info(f"         AmendmentDate: {dynamodb_item.get('amendmentDate', 'N/A')} {'⚠️ OMITTED (null/empty GSI)' if 'amendmentDate' not in dynamodb_item else ''}")
        local_logger.info(f"         Address: {dynamodb_item.get('address', 'N/A')} {'⚠️ OMITTED (null/empty GSI)' if 'address' not in dynamodb_item else ''}")
        local_logger.info(f"         Relationship: {dynamodb_item.get('relationship', 'N/A')}")
        local_logger.info(f"         Politician: {dynamodb_item.get('politician', 'N/A')}")
        local_logger.info(f"         FormS3Key: {dynamodb_item.get('formS3Key', 'N/A')}")
        local_logger.info(f"         Total Fields: {len(dynamodb_item)}")
        
        # Log full item for debugging (truncated)
        item_preview = {k: (str(v)[:100] + '...' if len(str(v)) > 100 else v) for k, v in list(dynamodb_item.items())[:10]}
        local_logger.info(f"         Item Preview (first 10 fields): {json.dumps(item_preview, default=str)}")
        
        # CRITICAL: Validate tradeId exists before writing
        if 'tradeId' not in dynamodb_item or not dynamodb_item.get('tradeId'):
            error_msg = f"   ❌ CRITICAL: tradeId is missing or empty in dynamodb_item! Cannot write to DynamoDB."
            local_logger.error(error_msg)
            print(error_msg, flush=True)
            raise ValueError("tradeId is required but missing from dynamodb_item")
        
        local_logger.info(f"      📡 Calling DynamoDB PutItem API...")
        local_logger.info(f"         TradeId: {dynamodb_item.get('tradeId')}")
        db_write_start = datetime.now()
        put_response = table_local.put_item(Item=dynamodb_item)
        db_write_duration = (datetime.now() - db_write_start).total_seconds()
        
        local_logger.info(f"      ✅ DynamoDB PutItem Response:")
        local_logger.info(f"         Success: True")
        local_logger.info(f"         Response Metadata: {put_response.get('ResponseMetadata', {}).get('HTTPStatusCode', 'N/A')}")
        local_logger.info(f"         Write Time: {db_write_duration:.2f}s")
        
        # Index to OpenSearch if configured
        # DISABLED FOR MVP - OpenSearch removed to save costs (~$200/month)
        # Agent will use DynamoDB queries + S3 file reads instead
        # Can be re-enabled when funding is available
        opensearch_success = False
        
        store_duration = (datetime.now() - store_start).total_seconds()
        total_duration = (datetime.now() - form_start_time).total_seconds()
        
        local_logger.info(f"   ✅ STORED: TradeId={trade_id}, S3Key={s3_key} in {store_duration:.2f}s")
        local_logger.info(f"   ✅ Form processing complete: Total time {total_duration:.2f}s")
        local_logger.info(f"      Breakdown: Download={download_duration:.2f}s, Parse={parse_duration:.2f}s, Match={match_duration:.2f}s, Store={store_duration:.2f}s")
        
        return {
            'success': True, 
            'tradeId': trade_id, 
            'politicianMatch': politician_match is not None,
            's3_key': s3_key,
            'formS3Key': s3_key,  # Also include for compatibility
        }
    
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
    
    # Helper function to process forms for a single date
    def process_single_date(date_str: str) -> Dict[str, Any]:
        """Process all forms for a single date: fetch, then process with Spark"""
        logger.info("")
        logger.info("=" * 80)
        logger.info(f"📋 Processing date: {date_str}")
        logger.info("=" * 80)
        
        # Step 2a: Fetch forms for this date
        stage2_start = datetime.now()
        forms = fetch_sec_forms_paginated(date_str)
        stage2_duration = (datetime.now() - stage2_start).total_seconds()
        
        if not forms:
            logger.info(f"   ⏭️ No forms found for {date_str}, skipping processing")
            print(f"   ⏭️ No forms found for {date_str}, skipping processing", flush=True)
            return {
                'date': date_str,
                'total_forms': 0,
                'successful_stored': 0,
                'failed_stored': 0,
                'skipped_date_mismatch': 0,
                'skipped_download_failed': 0,
                'skipped_unsupported_type': 0,
                'politician_matches': 0,
                'no_politician_matches': 0,
                'results': []
            }
        
        logger.info(f"   ✅ Fetched {len(forms)} forms for {date_str} in {stage2_duration:.2f} seconds")
        print(f"   ✅ Fetched {len(forms)} forms for {date_str} in {stage2_duration:.2f} seconds", flush=True)
        
        # Step 2b: Process forms with Spark
        logger.info("")
        logger.info(f"   📊 Processing {len(forms)} forms for {date_str} using Spark")
        print(f"   📊 Processing {len(forms)} forms for {date_str} using Spark", flush=True)
        stage3_start = datetime.now()
        
        # Broadcast necessary variables
        politicians_broadcast = sc.broadcast(politicians)
        target_date_broadcast = sc.broadcast(date_str)
        s3_bucket_broadcast = sc.broadcast(s3_bucket)
        dynamodb_table_broadcast = sc.broadcast(dynamodb_table)
        # OpenSearch disabled for MVP
        # opensearch_endpoint_broadcast = sc.broadcast(opensearch_endpoint)
        # opensearch_index_broadcast = sc.broadcast(opensearch_index)
        
        # Process each form (download, parse, check match, store)
        def process_form_wrapper(form_data):
            import logging
            import traceback
            local_logger = logging.getLogger()
            local_logger.setLevel(logging.INFO)
            
            # Initialize default result structure
            default_result = {
                'success': False,
                'skipped': False,
                'error': None,
                'traceback': None
            }
            
            try:
                politicians_local = politicians_broadcast.value
                target_date_local = target_date_broadcast.value
                s3_bucket_local = s3_bucket_broadcast.value
                dynamodb_table_local = dynamodb_table_broadcast.value
                # OpenSearch disabled for MVP
                # opensearch_endpoint_local = opensearch_endpoint_broadcast.value
                # opensearch_index_local = opensearch_index_broadcast.value
                
                cik = form_data.get('cik', 'unknown')
                accession = form_data.get('accession_number', 'unknown')
                
                result = process_form(form_data, target_date_local, politicians_local, s3_bucket_local, 
                                     dynamodb_table_local, None, None)  # OpenSearch disabled
                
                # Ensure result is a dict with required keys
                if not isinstance(result, dict):
                    local_logger.error(f"❌ FATAL ERROR: process_form returned non-dict: {type(result)}")
                    return {**default_result, 'error': f'process_form returned {type(result).__name__} instead of dict'}
                
                # Ensure result has required keys with proper types
                if 'success' not in result:
                    result['success'] = False
                else:
                    # Ensure success is a boolean, not an integer
                    result['success'] = bool(result['success'])
                if 'skipped' not in result:
                    result['skipped'] = False
                else:
                    # Ensure skipped is a boolean, not an integer
                    result['skipped'] = bool(result['skipped'])
                
                return result
            except Exception as e:
                error_msg = str(e)
                error_traceback = traceback.format_exc()
                local_logger.error(f"❌ FATAL ERROR processing form: {error_msg}")
                local_logger.error(f"   Traceback: {error_traceback}")
                return {
                    **default_result,
                    'error': error_msg,
                    'traceback': error_traceback
                }
        
        # Create RDD and process
        try:
            forms_rdd = sc.parallelize(forms)
            results_rdd = forms_rdd.map(process_form_wrapper)
            logger.info(f"   🔄 Collecting results from Spark RDD...")
            print(f"   🔄 Collecting results from Spark RDD...", flush=True)
            results = results_rdd.collect()
            logger.info(f"   ✅ Collected {len(results)} results from Spark")
            print(f"   ✅ Collected {len(results)} results from Spark", flush=True)
        except Exception as rdd_error:
            error_msg = str(rdd_error)
            error_type = type(rdd_error).__name__
            logger.error(f"   ❌ ERROR collecting results from Spark RDD: {error_type}: {error_msg}")
            print(f"   ❌ ERROR collecting results from Spark RDD: {error_type}: {error_msg}", flush=True)
            import traceback
            error_traceback = traceback.format_exc()
            logger.error(f"   Traceback: {error_traceback}")
            print(f"   Traceback: {error_traceback}", flush=True)
            # Return empty results to avoid further errors
            results = []
        
        # Clean up broadcast variables
        politicians_broadcast.destroy()
        target_date_broadcast.destroy()
        s3_bucket_broadcast.destroy()
        dynamodb_table_broadcast.destroy()
        # OpenSearch disabled for MVP
        # opensearch_endpoint_broadcast.destroy()
        # opensearch_index_broadcast.destroy()
        
        stage3_duration = (datetime.now() - stage3_start).total_seconds()
        
        # Calculate statistics
        # Ensure all results are dicts and have required keys with proper types
        valid_results = []
        invalid_count = 0
        for r in results:
            if not isinstance(r, dict):
                logger.warning(f"   ⚠️ Invalid result type: {type(r)}, skipping")
                invalid_count += 1
                continue
            # Ensure required keys exist with proper boolean types (not integers)
            if 'success' not in r:
                r['success'] = False
            else:
                r['success'] = bool(r['success'])  # Convert to boolean if it's 0/1
            if 'skipped' not in r:
                r['skipped'] = False
            else:
                r['skipped'] = bool(r['skipped'])  # Convert to boolean if it's 0/1
            valid_results.append(r)
        
        if invalid_count > 0:
            logger.warning(f"   ⚠️ Found {invalid_count} invalid results (non-dict), excluded from statistics")
            print(f"   ⚠️ Found {invalid_count} invalid results (non-dict), excluded from statistics", flush=True)
        
        total_forms_processed = len(forms)
        # Use explicit boolean checks to avoid PySpark type issues
        successful_stored = builtins.sum(1 for r in valid_results if r.get('success') is True)
        failed_stored = builtins.sum(1 for r in valid_results if r.get('success') is not True and r.get('skipped') is not True)
        skipped_date_mismatch = builtins.sum(1 for r in valid_results if r.get('skipped') is True and r.get('reason') == 'date_mismatch')
        skipped_date_parse_failed = builtins.sum(1 for r in valid_results if r.get('skipped') is True and r.get('reason') == 'date_parse_failed')
        skipped_download_failed = builtins.sum(1 for r in valid_results if r.get('skipped') is True and r.get('reason') == 'download_failed')
        skipped_unsupported_type = builtins.sum(1 for r in valid_results if r.get('skipped') is True and r.get('reason') == 'unsupported_file_type')
        skipped_parse_returned_none = builtins.sum(1 for r in valid_results if r.get('skipped') is True and r.get('reason') == 'parse_returned_none')
        politician_matches = builtins.sum(1 for r in valid_results if r.get('politicianMatch') is True)
        no_politician_matches = successful_stored - politician_matches
        
        logger.info(f"   ✅ Completed processing {date_str}: {successful_stored}/{total_forms_processed} stored in {stage3_duration:.2f} seconds")
        print(f"   ✅ Completed processing {date_str}: {successful_stored}/{total_forms_processed} stored in {stage3_duration:.2f} seconds", flush=True)
        
        return {
            'date': date_str,
            'total_forms': total_forms_processed,
            'successful_stored': successful_stored,
            'failed_stored': failed_stored,
            'skipped_date_mismatch': skipped_date_mismatch,
            'skipped_date_parse_failed': skipped_date_parse_failed,
            'skipped_download_failed': skipped_download_failed,
            'skipped_unsupported_type': skipped_unsupported_type,
            'skipped_parse_returned_none': skipped_parse_returned_none,
            'politician_matches': politician_matches,
            'no_politician_matches': no_politician_matches,
            'results': results
        }
    
    # Step 2 & 3: Fetch and process forms (sequential batches for backdate mode)
    logger.info("")
    logger.info("=" * 80)
    logger.info("📋 STAGE 2 & 3: FETCHING AND PROCESSING SEC FORMS")
    logger.info("=" * 80)
    
    all_results = []
    all_date_stats = []
    
    if is_backdate_mode:
        # Backdate mode: process each date sequentially
        logger.info(f"🔍 BACKDATE MODE: Processing dates from today back to {target_date}")
        print(f"🔍 BACKDATE MODE: Processing dates from today back to {target_date}", flush=True)
        
        target_date_obj = datetime.strptime(target_date, '%Y-%m-%d').date()
        current_date = datetime.now().date()
        date_iter = current_date
        
        while date_iter >= target_date_obj:
            date_str = date_iter.strftime('%Y-%m-%d')
            date_stats = process_single_date(date_str)
            all_date_stats.append(date_stats)
            all_results.extend(date_stats['results'])
            date_iter -= timedelta(days=1)
    else:
        # Normal mode: process single date
        date_stats = process_single_date(target_date)
        all_date_stats.append(date_stats)
        all_results = date_stats['results']
    
    # Aggregate statistics across all dates
    total_forms_processed = builtins.sum(s['total_forms'] for s in all_date_stats)
    successful_stored = builtins.sum(s['successful_stored'] for s in all_date_stats)
    failed_stored = builtins.sum(s['failed_stored'] for s in all_date_stats)
    skipped_date_mismatch = builtins.sum(s['skipped_date_mismatch'] for s in all_date_stats)
    skipped_date_parse_failed = builtins.sum(s.get('skipped_date_parse_failed', 0) for s in all_date_stats)
    skipped_download_failed = builtins.sum(s['skipped_download_failed'] for s in all_date_stats)
    skipped_unsupported_type = builtins.sum(s['skipped_unsupported_type'] for s in all_date_stats)
    skipped_parse_returned_none = builtins.sum(s.get('skipped_parse_returned_none', 0) for s in all_date_stats)
    politician_matches = builtins.sum(s['politician_matches'] for s in all_date_stats)
    no_politician_matches = builtins.sum(s['no_politician_matches'] for s in all_date_stats)
    
    # Log summary across all dates
    logger.info("")
    print("", flush=True)
    logger.info("=" * 80)
    print("=" * 80, flush=True)
    logger.info("📊 SUMMARY ACROSS ALL DATES")
    print("📊 SUMMARY ACROSS ALL DATES", flush=True)
    logger.info("=" * 80)
    print("=" * 80, flush=True)
    for date_stat in all_date_stats:
        if date_stat['total_forms'] > 0:
            logger.info(f"   {date_stat['date']}: {date_stat['successful_stored']}/{date_stat['total_forms']} stored")
            print(f"   {date_stat['date']}: {date_stat['successful_stored']}/{date_stat['total_forms']} stored", flush=True)
    
    logger.info("")
    print("", flush=True)
    logger.info(f"   Total across all dates: {successful_stored}/{total_forms_processed} stored")
    print(f"   Total across all dates: {successful_stored}/{total_forms_processed} stored", flush=True)
    
    # Track S3 keys to detect collisions
    s3_keys_generated = []
    trade_ids_generated = []
    for result in all_results:
        if result.get('success'):
            s3_key = result.get('s3_key') or result.get('formS3Key')
            if s3_key:
                s3_keys_generated.append(s3_key)
            trade_id = result.get('tradeId')
            if trade_id:
                trade_ids_generated.append(trade_id)
    
    if s3_keys_generated:
        unique_keys = set(s3_keys_generated)
        logger.info(f"   📦 S3 Keys Generated: {len(s3_keys_generated)} total, {len(unique_keys)} unique")
        print(f"   📦 S3 Keys Generated: {len(s3_keys_generated)} total, {len(unique_keys)} unique", flush=True)
        if len(s3_keys_generated) != len(unique_keys):
            logger.warning(f"   ⚠️ WARNING: S3 KEY COLLISIONS DETECTED!")
            print(f"   ⚠️ WARNING: S3 KEY COLLISIONS DETECTED!", flush=True)
    
    # Track tradeIds to detect duplicates (which would cause overwrites in DynamoDB)
    if trade_ids_generated:
        unique_trade_ids = set(trade_ids_generated)
        logger.info(f"   🔑 TradeIds Generated: {len(trade_ids_generated)} total, {len(unique_trade_ids)} unique")
        print(f"   🔑 TradeIds Generated: {len(trade_ids_generated)} total, {len(unique_trade_ids)} unique", flush=True)
        if len(trade_ids_generated) != len(unique_trade_ids):
            duplicate_count = len(trade_ids_generated) - len(unique_trade_ids)
            logger.warning(f"   ⚠️ WARNING: DUPLICATE TRADEIDS DETECTED! {duplicate_count} duplicates found")
            logger.warning(f"      This will cause DynamoDB overwrites - fewer items in table than 'successful_stored' count")
            print(f"   ⚠️ WARNING: DUPLICATE TRADEIDS DETECTED! {duplicate_count} duplicates found", flush=True)
            print(f"      This will cause DynamoDB overwrites - fewer items in table than 'successful_stored' count", flush=True)
            
            # Find and log duplicate tradeIds
            from collections import Counter
            trade_id_counts = Counter(trade_ids_generated)
            duplicates = {tid: count for tid, count in trade_id_counts.items() if count > 1}
            if duplicates:
                logger.warning(f"   📋 Duplicate TradeIds (showing first 10):")
                print(f"   📋 Duplicate TradeIds (showing first 10):", flush=True)
                for idx, (tid, count) in enumerate(list(duplicates.items())[:10], 1):
                    logger.warning(f"      {idx}. {tid} (appears {count} times)")
                    print(f"      {idx}. {tid} (appears {count} times)", flush=True)
                if len(duplicates) > 10:
                    logger.warning(f"      ... and {len(duplicates) - 10} more duplicates")
                    print(f"      ... and {len(duplicates) - 10} more duplicates", flush=True)
    
    separator = "=" * 80
    logger.info(separator)
    print(separator, flush=True)
    
    # Step 4: Final Summary
    logger.info("")
    print("", flush=True)
    logger.info("=" * 80)
    print("=" * 80, flush=True)
    logger.info("📊 STAGE 4: FINAL SUMMARY")
    print("📊 STAGE 4: FINAL SUMMARY", flush=True)
    logger.info("=" * 80)
    print("=" * 80, flush=True)
    logger.info(f"📈 Processing Statistics:")
    print(f"📈 Processing Statistics:", flush=True)
    logger.info(f"   - Total forms fetched: {total_forms_processed}")
    print(f"   - Total forms fetched: {total_forms_processed}", flush=True)
    logger.info(f"   - Successfully stored: {successful_stored} ({successful_stored/total_forms_processed*100 if total_forms_processed > 0 else 0:.1f}%)")
    print(f"   - Successfully stored: {successful_stored} ({successful_stored/total_forms_processed*100 if total_forms_processed > 0 else 0:.1f}%)", flush=True)
    logger.info(f"   - Failed to store: {failed_stored} ({failed_stored/total_forms_processed*100 if total_forms_processed > 0 else 0:.1f}%)")
    print(f"   - Failed to store: {failed_stored} ({failed_stored/total_forms_processed*100 if total_forms_processed > 0 else 0:.1f}%)", flush=True)
    logger.info(f"   - Skipped (date mismatch): {skipped_date_mismatch} ({skipped_date_mismatch/total_forms_processed*100 if total_forms_processed > 0 else 0:.1f}%)")
    print(f"   - Skipped (date mismatch): {skipped_date_mismatch} ({skipped_date_mismatch/total_forms_processed*100 if total_forms_processed > 0 else 0:.1f}%)", flush=True)
    logger.info(f"   - Skipped (date parse failed): {skipped_date_parse_failed} ({skipped_date_parse_failed/total_forms_processed*100 if total_forms_processed > 0 else 0:.1f}%)")
    print(f"   - Skipped (date parse failed): {skipped_date_parse_failed} ({skipped_date_parse_failed/total_forms_processed*100 if total_forms_processed > 0 else 0:.1f}%)", flush=True)
    logger.info(f"   - Skipped (download failed): {skipped_download_failed} ({skipped_download_failed/total_forms_processed*100 if total_forms_processed > 0 else 0:.1f}%)")
    print(f"   - Skipped (download failed): {skipped_download_failed} ({skipped_download_failed/total_forms_processed*100 if total_forms_processed > 0 else 0:.1f}%)", flush=True)
    logger.info(f"   - Skipped (unsupported file type): {skipped_unsupported_type} ({skipped_unsupported_type/total_forms_processed*100 if total_forms_processed > 0 else 0:.1f}%)")
    print(f"   - Skipped (unsupported file type): {skipped_unsupported_type} ({skipped_unsupported_type/total_forms_processed*100 if total_forms_processed > 0 else 0:.1f}%)", flush=True)
    logger.info(f"   - Skipped (parse returned none): {skipped_parse_returned_none} ({skipped_parse_returned_none/total_forms_processed*100 if total_forms_processed > 0 else 0:.1f}%)")
    print(f"   - Skipped (parse returned none): {skipped_parse_returned_none} ({skipped_parse_returned_none/total_forms_processed*100 if total_forms_processed > 0 else 0:.1f}%)", flush=True)
    logger.info("")
    print("", flush=True)
    logger.info(f"👤 Politician Matching Statistics:")
    print(f"👤 Politician Matching Statistics:", flush=True)
    logger.info(f"   - Forms with politician match: {politician_matches} ({politician_matches/successful_stored*100 if successful_stored > 0 else 0:.1f}% of stored)")
    print(f"   - Forms with politician match: {politician_matches} ({politician_matches/successful_stored*100 if successful_stored > 0 else 0:.1f}% of stored)", flush=True)
    logger.info(f"   - Forms without politician match: {no_politician_matches} ({no_politician_matches/successful_stored*100 if successful_stored > 0 else 0:.1f}% of stored)")
    print(f"   - Forms without politician match: {no_politician_matches} ({no_politician_matches/successful_stored*100 if successful_stored > 0 else 0:.1f}% of stored)", flush=True)
    logger.info("")
    print("", flush=True)
    
    # Detailed failure analysis
    logger.info("")
    print("", flush=True)
    logger.info(f"🔍 Detailed Failure Analysis:")
    print(f"🔍 Detailed Failure Analysis:", flush=True)
    logger.info(f"   - Skipped (date mismatch): {skipped_date_mismatch} forms")
    print(f"   - Skipped (date mismatch): {skipped_date_mismatch} forms", flush=True)
    logger.info(f"   - Skipped (date parse failed): {skipped_date_parse_failed} forms")
    print(f"   - Skipped (date parse failed): {skipped_date_parse_failed} forms", flush=True)
    logger.info(f"   - Skipped (download failed): {skipped_download_failed} forms")
    print(f"   - Skipped (download failed): {skipped_download_failed} forms", flush=True)
    logger.info(f"   - Skipped (unsupported file type): {skipped_unsupported_type} forms")
    print(f"   - Skipped (unsupported file type): {skipped_unsupported_type} forms", flush=True)
    logger.info(f"   - Skipped (parse returned none): {skipped_parse_returned_none} forms")
    print(f"   - Skipped (parse returned none): {skipped_parse_returned_none} forms", flush=True)
    logger.info(f"   - Failed to store: {failed_stored} forms")
    print(f"   - Failed to store: {failed_stored} forms", flush=True)
    
    # Calculate total skipped for verification
    total_skipped = (skipped_date_mismatch + skipped_date_parse_failed + skipped_download_failed + 
                     skipped_unsupported_type + skipped_parse_returned_none)
    logger.info(f"   - Total skipped: {total_skipped} forms")
    print(f"   - Total skipped: {total_skipped} forms", flush=True)
    logger.info(f"   - Verification: {successful_stored} stored + {total_skipped} skipped + {failed_stored} failed = {successful_stored + total_skipped + failed_stored} (expected: {total_forms_processed})")
    print(f"   - Verification: {successful_stored} stored + {total_skipped} skipped + {failed_stored} failed = {successful_stored + total_skipped + failed_stored} (expected: {total_forms_processed})", flush=True)
    
    # Show sample of download failures (helps identify problematic CIKs)
    download_failed_samples = [r for r in all_results if r.get('skipped') is True and r.get('reason') == 'download_failed']
    if download_failed_samples:
        sample_download = download_failed_samples[:10]
        logger.info(f"   📋 Sample of Download Failures (showing first {len(sample_download)} of {len(download_failed_samples)}):")
        print(f"   📋 Sample of Download Failures (showing first {len(sample_download)} of {len(download_failed_samples)}):", flush=True)
        for idx, sample in enumerate(sample_download, 1):
            cik_sample = sample.get('cik', 'unknown')
            accession_sample = sample.get('accession', 'unknown')
            form_type_sample = sample.get('formType', sample.get('form_type', 'unknown'))
            logger.info(f"      {idx}. CIK={cik_sample}, Accession={accession_sample}, FormType={form_type_sample}")
            print(f"      {idx}. CIK={cik_sample}, Accession={accession_sample}, FormType={form_type_sample}", flush=True)
        
        # Show sample of failed results for debugging
    if failed_stored > 0:
        logger.info("")
        logger.info(f"   📋 Sample of Failed Forms (first 5):")
        print("", flush=True)
        print(f"   📋 Sample of Failed Forms (first 5):", flush=True)
        failed_samples = [r for r in all_results if not r.get('success') and not r.get('skipped')][:5]
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
        download_failed_samples = [r for r in all_results if r.get('skipped') and r.get('reason') == 'download_failed'][:5]
        for idx, failed in enumerate(download_failed_samples, 1):
            logger.info(f"      {idx}. Reason: {failed.get('reason', 'Unknown')}")
    
    if skipped_date_mismatch > 0:
        logger.info("")
        logger.info(f"   ⚠️ Note: {skipped_date_mismatch} forms were skipped due to date mismatch")
        logger.info(f"      This is normal if the filing date in the form doesn't match the target date")
    
    # Check for critical failures that should cause job to fail
    # Note: No forms found is valid for holidays/weekends - only fail on actual errors
    if total_forms_processed == 0:
        # Check if this was due to skipped dates (holidays/weekends) vs actual errors
        dates_processed = len(all_date_stats)
        dates_with_forms = builtins.sum(1 for s in all_date_stats if s.get('total_forms', 0) > 0)
        
        if dates_processed > 0 and dates_with_forms == 0:
            # All dates were skipped (likely holidays/weekends) - this is valid, don't fail
            logger.warning("")
            logger.warning("⚠️ WARNING: No forms were fetched from SEC API for any processed dates")
            print("", flush=True)
            print("⚠️ WARNING: No forms were fetched from SEC API for any processed dates", flush=True)
            print("   This is normal for:", flush=True)
            print("      - Federal holidays (SEC is closed)", flush=True)
            print("      - Weekends (SEC is closed)", flush=True)
            print("      - Dates before SEC filings began", flush=True)
            print("   Job will complete successfully - no action needed.", flush=True)
            # Don't raise exception - allow job to succeed
        else:
            # This shouldn't happen, but if it does, log as error but don't fail
            # (Could be a legitimate case where no forms were filed)
            logger.warning("")
            logger.warning("⚠️ WARNING: No forms were fetched from SEC API")
            print("", flush=True)
            print("⚠️ WARNING: No forms were fetched from SEC API", flush=True)
            print("   This could indicate:", flush=True)
            print("      - No forms filed on the target date(s)", flush=True)
            print("      - Federal holiday or weekend", flush=True)
            print("      - SEC API temporarily unavailable (check logs for 403/404 errors)", flush=True)
            print("   Job will complete successfully - review logs if this is unexpected.", flush=True)
            # Don't raise exception - allow job to succeed
    
    if successful_stored == 0 and total_forms_processed > 0:
        # Only fail if we fetched forms but couldn't store any (actual error)
        # If total_forms_processed == 0, that's handled above (holiday/weekend - valid)
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
        print(f"      Skipped (date parse failed): {skipped_date_parse_failed}", flush=True)
        print(f"      Skipped (download failed): {skipped_download_failed}", flush=True)
        print(f"      Skipped (unsupported file type): {skipped_unsupported_type}", flush=True)
        print(f"      Skipped (parse returned none): {skipped_parse_returned_none}", flush=True)
        
        raise Exception(f"No forms were successfully stored. {total_forms_processed} forms were fetched but none were stored. Check logs for details.")
    elif successful_stored == 0 and total_forms_processed == 0:
        # No forms fetched and none stored - this is expected for holidays/weekends
        # Already handled above, just log for completeness
        logger.info("")
        logger.info("ℹ️ No forms were fetched or stored - this is expected for holidays/weekends")
        print("ℹ️ No forms were fetched or stored - this is expected for holidays/weekends", flush=True)
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
