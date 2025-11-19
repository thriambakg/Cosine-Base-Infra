"""
AWS Glue Job: SEC Forms ETL Pipeline (XML-based)
Fetches, downloads, parses, matches, and stores SEC Form 3/4/5 filings to DynamoDB

This job handles the entire SEC pipeline:
1. Fetch SEC filings using paginated browse-edgar API
2. Download both HTML and XML files
3. Parse XML to extract trades (cleaner than HTML parsing)
4. Match filers to politicians
5. Write matched trades to DynamoDB
6. Write summary to S3 for aggregator

Key improvements:
- Downloads both HTML and XML versions
- Stores files in folder structure: sec/{filename-without-extension}/filename.xml and filename.html
- Uses XML parsing instead of HTML (more reliable, structured data)
- S3 key in DynamoDB is the folder path (allows frontend to choose which file to download)
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
    """Map SEC transaction codes to human-readable meanings."""
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
# ============================================================================
logger = logging.getLogger()
logger.setLevel(logging.INFO)

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
required_args = ['JOB_NAME', 's3_bucket', 'dynamodb_table']
args = getResolvedOptions(sys.argv, required_args)
job.init(args['JOB_NAME'], args)

# Extract required parameters
s3_bucket = args.get('s3_bucket')
dynamodb_table = args.get('dynamodb_table')

# Parse optional date and backdate arguments manually
target_date = None
backdate = None

for i, arg in enumerate(sys.argv):
    if arg == '--date' or arg.startswith('--date='):
        if '=' in arg:
            date_value = arg.split('=', 1)[1]
        elif i + 1 < len(sys.argv):
            date_value = sys.argv[i + 1]
else:
            continue
        if date_value and date_value.strip() != '' and date_value.lower() != 'null':
            target_date = date_value
    elif arg == '--backdate' or arg.startswith('--backdate='):
        if '=' in arg:
            backdate_value = arg.split('=', 1)[1]
        elif i + 1 < len(sys.argv):
            backdate_value = sys.argv[i + 1]
        else:
            continue
        if backdate_value and backdate_value.strip() != '' and backdate_value.lower() != 'null':
            backdate = backdate_value

# Determine mode: backdate mode or normal date mode
if backdate and backdate.strip() != '' and backdate.lower() != 'null':
    target_date = backdate
    is_backdate_mode = True
    logger.info(f"📅 BACKDATE MODE: Processing SEC forms backdating to: {backdate}")
    print(f"📅 BACKDATE MODE: Processing SEC forms backdating to: {backdate}", flush=True)
elif target_date and target_date.strip() != '' and target_date.lower() != 'null':
    is_backdate_mode = False
    logger.info(f"📅 Processing SEC forms for date: {target_date}")
    print(f"📅 Processing SEC forms for date: {target_date}", flush=True)
else:
    yesterday = (datetime.now().date() - timedelta(days=1))
    target_date = yesterday.strftime('%Y-%m-%d')
    is_backdate_mode = False
    logger.info(f"ℹ️ No date/backdate provided, defaulting to yesterday: {target_date}")
    print(f"ℹ️ No date/backdate provided, defaulting to yesterday: {target_date}", flush=True)

# SEC API configuration
SEC_BASE_URL = "https://www.sec.gov"
SEC_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 (Cosine Financial Platform; contact@cosine.financial)"

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
            
            alt_names = []
            if row.get('nickname'):
                alt_names.append(row['nickname'])
            
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
    Fetch SEC forms using Daily Index Files
    
    Args:
        target_date: Target date in YYYY-MM-DD format
        form_types: List of form types to fetch (default: ['3', '4', '5'])
    
    Returns:
        List of form metadata dicts
    """
    all_forms = []
    target_date_obj = datetime.strptime(target_date, '%Y-%m-%d').date()
    
    today = datetime.now().date()
    if target_date_obj > today:
        logger.warning(f"   ⚠️ Target date {target_date} is in the future, skipping")
        print(f"   ⚠️ Target date {target_date} is in the future, skipping", flush=True)
        return []
    
    session = requests.Session()
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
    
        index_url = f"{SEC_BASE_URL}/Archives/edgar/daily-index/{year}/QTR{quarter}/master.{date_str_idx}.idx"
        
        logger.info(f"📥 Fetching daily index file for {date_str}...")
        print(f"📥 Fetching daily index file for {date_str}...", flush=True)
        
        try:
            time.sleep(0.3)
            
        max_retries = 3
        retry_delay = 1.0
        response = None
        
        for attempt in range(max_retries):
            try:
            response = session.get(index_url, timeout=30)
            
                if response.status_code == 200:
                    break
                elif response.status_code == 403:
                    # SEC returns 403 for non-existent index files (weekends/holidays/future dates)
                    # Check if it's a weekend first
                    weekday = target_date_obj.weekday()  # 0=Monday, 6=Sunday
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
                    logger.warning(f"   ⚠️ Request error (attempt {attempt + 1}/{max_retries}): {e}, retrying in {wait_time:.1f}s...")
                    print(f"   ⚠️ Request error (attempt {attempt + 1}/{max_retries}): {e}, retrying in {wait_time:.1f}s...", flush=True)
                    time.sleep(wait_time)
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
            print(f"   ⚠️ No header line found in index file for {date_str}", flush=True)
            return []
            
            # Parse data lines
        form_type_nums = [ft.replace('form', '') if 'form' in ft.lower() else ft for ft in form_types]
            
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
                    
                # Extract form type number (e.g., "4" from "4" or "4/A")
                form_type_match = re.match(r'^(\d+)', form_type_raw)
                if not form_type_match:
                    continue
                form_type = form_type_match.group(1)
                
                if form_type not in form_type_nums:
                    continue
                
                                # Extract accession number from filename
                # Format: {accession}-{something}.txt or {accession}-index.htm
                accession_match = re.match(r'^([\d-]+)', filename)
                if not accession_match:
                    continue
                accession_with_dashes = accession_match.group(1)
                accession_clean = accession_with_dashes.replace('-', '')
                
                all_forms.append({
                    'form_type': form_type,
                                        'cik': cik,
                    'accession_number': accession_clean,
                    'accession_dashed': accession_with_dashes,
                    'filename': filename,
                    'filing_date': date_filed,
                })
            except (ValueError, IndexError) as e:
                # Skip malformed lines
                    continue
            
        logger.info(f"✅ Found {len(all_forms)} forms (Types: {', '.join(form_types)})")
        print(f"✅ Found {len(all_forms)} forms (Types: {', '.join(form_types)})", flush=True)
            
        return all_forms
            
        except Exception as e:
        logger.error(f"❌ Error fetching SEC forms: {e}")
            import traceback
        logger.error(traceback.format_exc())
        raise


def download_sec_form_both(form_data: Dict[str, Any], target_date: str, s3_bucket_name: str) -> Optional[Dict[str, Any]]:
    """
    Download both HTML and XML versions of SEC form
    
    Stores files in folder structure:
    sec/{form_type}-{cik}-{accession}-{date}/filename.xml
    sec/{form_type}-{cik}-{accession}-{date}/filename.html
    
    Returns:
        Dict with 'folder_key' (S3 folder path), 'xml_content', 'html_content', 'xml_s3_key', 'html_s3_key'
        or None if download fails
    """
    import logging
    local_logger = logging.getLogger()
    
    try:
        cik = form_data.get('cik', 'unknown')
        accession = form_data.get('accession_number', 'unknown')
        form_type = form_data.get('form_type', 'unknown')
        accession_dashed = form_data.get('accession_dashed', '')
        
        if not accession_dashed:
            accession_clean = accession.replace('-', '').strip()
            if len(accession_clean) == 18:
                accession_dashed = f"{accession_clean[:10]}-{accession_clean[10:12]}-{accession_clean[12:]}"
            else:
                accession_dashed = accession
        
        local_logger.info(f"      📥 DOWNLOAD START: CIK={cik}, Accession={accession}, Type={form_type}")
        print(f"      📥 DOWNLOAD START: CIK={cik}, Accession={accession}, Type={form_type}", flush=True)
        
        if not all([cik, accession]) or cik == 'unknown' or accession == 'unknown':
            local_logger.warning(f"   ⚠️ Missing CIK/accession: CIK={cik}, Accession={accession}")
            print(f"   ⚠️ Missing CIK/accession: CIK={cik}, Accession={accession}", flush=True)
            return None
        
        # Build base URL
        base_url = f"{SEC_BASE_URL}/Archives/edgar/data/{cik}/{accession_dashed}"
        
        # Create folder name (without extension)
        folder_name = f"{form_type}-{cik}-{accession_dashed}-{target_date}"
        folder_key = f"sec/{folder_name}"
        
        local_logger.info(f"      🔗 SEC Archive Base URL: {base_url}")
        local_logger.info(f"      📂 Will store to S3 folder: {folder_key}/")
        print(f"      🔗 SEC Archive Base URL: {base_url}", flush=True)
        print(f"      📂 Will store to S3 folder: {folder_key}/", flush=True)
        
        # Create session
        session = requests.Session()
        session.headers.update({
            'User-Agent': SEC_USER_AGENT,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Referer': 'https://www.sec.gov/',
        })
        
        # Try to find both XML and HTML files
        # Priority: Look for actual XML files first, then HTML files
        xml_content = None
        html_content = None
        xml_s3_key = None
        html_s3_key = None
        
        # Common XML file patterns
        xml_patterns = [
            f"{accession_dashed}-primary-document.xml",
            f"{accession_dashed}-primarydoc.xml",
            "primary-document.xml",
            f"doc{form_type}.xml",  # doc4.xml for Form 4
            "doc1.xml",
            "doc2.xml",
            "doc3.xml",
            f"{accession_dashed}.xml",
            "ownership.xml",
        ]
        
        # Common HTML file patterns
        html_patterns = [
            f"{accession_dashed}-primary-document.html",
            f"{accession_dashed}-primary-document.htm",
            "primary-document.html",
            "primary-document.htm",
            f"doc{form_type}.html",
            f"doc{form_type}.htm",
            f"{accession_dashed}.html",
            f"{accession_dashed}.htm",
        ]
        
        # Download index page to find document links (matching old script's approach)
        # Priority 1: Download index.htm to find actual document links
        index_url = f"{base_url}/{accession_dashed}-index.htm"
        index_url_fallback = f"{base_url}/index.htm"
        
        local_logger.info(f"      🔍 Searching for XML and HTML files...")
        print(f"      🔍 Searching for XML and HTML files...", flush=True)
        
        # Try index page first to find document links (mild web scraping with regex)
        # We download the index page, parse it for links, then download those documents (not the index page itself)
        doc_links_to_try = []  # Will store links found from index page
        
        for index_url_to_try in [index_url, index_url_fallback]:
            try:
                time.sleep(0.1)
                index_response = session.get(index_url_to_try, timeout=30)
                if index_response.status_code == 200:
                    index_html = index_response.text
                            
                            local_logger.info(f"      🔍 Parsing index page for document links...")
                            print(f"      🔍 Parsing index page for document links...", flush=True)
                            
                            # Strategy 1: Find all .xml file links (prioritize these over .txt)
                            xml_pattern = r'href="([^"]*\.xml[^"]*)"'
                    xml_matches = re.findall(xml_pattern, index_html, re.IGNORECASE)
                    doc_links_to_try.extend(xml_matches)
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
                        matches = re.findall(pattern, index_html, re.IGNORECASE)
                                primary_links.extend(matches)
                            # Prepend primary links to prioritize them
                    doc_links_to_try = primary_links + [link for link in doc_links_to_try if link not in primary_links]
                    
                    # Strategy 3: Find HTML links
                    html_pattern = r'href="([^"]*\.(?:html?)[^"]*)"'
                    html_matches = re.findall(html_pattern, index_html, re.IGNORECASE)
                    html_primary_patterns = [
                        r'href="([^"]*primary[_-]?document[^"]*\.(?:html?)[^"]*)"',
                        r'href="([^"]*primarydoc[^"]*\.(?:html?)[^"]*)"',
                        r'href="([^"]*document[^"]*\.(?:html?)[^"]*)"',
                    ]
                    html_primary_links = []
                    for pattern in html_primary_patterns:
                        matches = re.findall(pattern, index_html, re.IGNORECASE)
                        html_primary_links.extend(matches)
                    html_links = html_primary_links + [link for link in html_matches if link not in html_primary_links]
                    doc_links_to_try.extend(html_links)
                    
                    # Strategy 4: Look for links with accession number (fallback)
                    if not doc_links_to_try:
                                acc_pattern = rf'href="([^"]*{re.escape(accession_dashed)}[^"]*)"'
                        acc_matches = re.findall(acc_pattern, index_html, re.IGNORECASE)
                        doc_links_to_try.extend(acc_matches)
                                local_logger.info(f"      🔍 Found {len(acc_matches)} links with accession number")
                                print(f"      🔍 Found {len(acc_matches)} links with accession number", flush=True)
                            
                            # Remove duplicates while preserving order
                            seen = set()
                            unique_doc_links = []
                    for link in doc_links_to_try:
                                if link not in seen:
                                    seen.add(link)
                                    unique_doc_links.append(link)
                            
                    # Sort: XML files first, then others
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
                    doc_links_to_try = sorted_links
                    
                    local_logger.info(f"      📋 Found {len(doc_links_to_try)} document links from index page, will download documents...")
                    print(f"      📋 Found {len(doc_links_to_try)} document links from index page, will download documents...", flush=True)
                    break  # Found index page, stop trying fallback
            except Exception as e:
                local_logger.warning(f"      ⚠️ Could not fetch index page {index_url_to_try}: {e}")
                print(f"      ⚠️ Could not fetch index page {index_url_to_try}: {e}", flush=True)
                continue
        
        # Now try downloading documents from the links we found (matching old script logic)
        # Try up to 10 links from index page
        for doc_link in doc_links_to_try[:10]:
            # Handle relative URLs (matching old script logic)
                                if doc_link.startswith('/'):
                doc_url = f"https://www.sec.gov{doc_link}"
            elif doc_link.startswith('http'):
                doc_url = doc_link
            else:
                doc_url = f"{base_url}/{doc_link}"
            
            # Skip index pages - we only want actual documents
            if 'index' in doc_link.lower():
                                    continue
                                
                                try:
                                    time.sleep(0.1)
                doc_response = session.get(doc_url, timeout=30)
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
                                        
                    # Store HTML document if found and we don't have one yet
                    if is_html and not html_content:
                        html_content = doc_content
                        html_s3_key = f"{folder_key}/filename.html"
                        local_logger.info(f"      ✅ Found HTML document: {doc_link}")
                        print(f"      ✅ Found HTML document: {doc_link}", flush=True)
                        # Continue looking for XML
                    
                    # Store XML document if found and we don't have one yet
                    elif is_xml and not is_html and not xml_content:
                        xml_content = doc_content
                        xml_s3_key = f"{folder_key}/filename.xml"
                                            local_logger.info(f"      ✅ Found XML document: {doc_link}")
                                            print(f"      ✅ Found XML document: {doc_link}", flush=True)
                        # Continue looking for HTML
                    
                    # If we have both, we're done
                    if xml_content and html_content:
                                            break
            except Exception as e:
                                    continue
                                    
        # Fallback: If we didn't find documents from index page, try direct file patterns
        # (matching old script's Priority 2 approach)
        if not xml_content or not html_content:
            local_logger.info(f"      🔄 Trying direct file patterns as fallback...")
            print(f"      🔄 Trying direct file patterns as fallback...", flush=True)
            
            # Try XML files
            if not xml_content:
                for xml_pattern in xml_patterns[:10]:  # Limit to first 10 patterns
                    # Handle relative URLs (matching old script logic)
                    if xml_pattern.startswith('/'):
                        xml_url = f"https://www.sec.gov{xml_pattern}"
                    elif xml_pattern.startswith('http'):
                        xml_url = xml_pattern
                    else:
                        xml_url = f"{base_url}/{xml_pattern}"
                    try:
                                    time.sleep(0.1)
                        xml_response = session.get(xml_url, timeout=30)
                        if xml_response.status_code == 200:
                            content = xml_response.content
                            # Check if it's actual XML (not HTML masquerading as XML)
                            if content.startswith(b'<?xml') or (b'<ownershipDocument' in content and b'<!DOCTYPE html' not in content):
                                xml_content = content
                                xml_s3_key = f"{folder_key}/filename.xml"
                                local_logger.info(f"      ✅ Found XML file (fallback): {xml_pattern}")
                                print(f"      ✅ Found XML file (fallback): {xml_pattern}", flush=True)
                                                break
                            elif b'<!DOCTYPE html' in content or b'<html' in content.lower():
                                # This is HTML but has .xml extension - store as HTML
                                if not html_content:
                                    html_content = content
                                    html_s3_key = f"{folder_key}/filename.html"
                                    local_logger.info(f"      ✅ Found HTML file (with .xml extension, fallback): {xml_pattern}")
                                    print(f"      ✅ Found HTML file (with .xml extension, fallback): {xml_pattern}", flush=True)
                    except Exception as e:
                                    continue
                            
            # Try HTML files
            if not html_content:
                for html_pattern in html_patterns[:10]:  # Limit to first 10 patterns
                    # Handle relative URLs (matching old script logic)
                    if html_pattern.startswith('/'):
                        html_url = f"https://www.sec.gov{html_pattern}"
                    elif html_pattern.startswith('http'):
                        html_url = html_pattern
                        else:
                        html_url = f"{base_url}/{html_pattern}"
                    try:
                        time.sleep(0.1)
                        html_response = session.get(html_url, timeout=30)
                        if html_response.status_code == 200:
                            content = html_response.content
                            # Check if it's HTML
                            if b'<html' in content.lower() or b'<!doctype html' in content.lower():
                                html_content = content
                                html_s3_key = f"{folder_key}/filename.html"
                                local_logger.info(f"      ✅ Found HTML file (fallback): {html_pattern}")
                                print(f"      ✅ Found HTML file (fallback): {html_pattern}", flush=True)
                        break
            except Exception as e:
                continue
        
        # If we found at least one file, upload both to S3
        if xml_content or html_content:
            s3_client_local = boto3.client('s3')
            
            # Upload XML if found
            if xml_content and xml_s3_key:
                try:
                    s3_client_local.put_object(
                        Bucket=s3_bucket_name,
                        Key=xml_s3_key,
                        Body=xml_content,
                        ContentType='application/xml'
                    )
                    local_logger.info(f"      ✅ Uploaded XML to S3: {xml_s3_key}")
                    print(f"      ✅ Uploaded XML to S3: {xml_s3_key}", flush=True)
                except Exception as e:
                    local_logger.error(f"      ❌ Failed to upload XML: {e}")
                    print(f"      ❌ Failed to upload XML: {e}", flush=True)
            
            # Upload HTML if found
            if html_content and html_s3_key:
        try:
            s3_client_local.put_object(
                Bucket=s3_bucket_name,
                        Key=html_s3_key,
                        Body=html_content,
                        ContentType='text/html'
                    )
                    local_logger.info(f"      ✅ Uploaded HTML to S3: {html_s3_key}")
                    print(f"      ✅ Uploaded HTML to S3: {html_s3_key}", flush=True)
                except Exception as e:
                    local_logger.error(f"      ❌ Failed to upload HTML: {e}")
                    print(f"      ❌ Failed to upload HTML: {e}", flush=True)
            
            # Return folder key (not individual file keys) - frontend can choose which file to download
        return {
                'folder_key': folder_key,  # This is what gets stored in DynamoDB
                'xml_content': xml_content,
                'html_content': html_content,
                'xml_s3_key': xml_s3_key,
                'html_s3_key': html_s3_key,
            'cik': cik,
            'accession_number': accession,
            'form_type': form_type,
            'filing_date': target_date
        }
        else:
            local_logger.warning(f"      ❌ Could not find XML or HTML files")
            print(f"      ❌ Could not find XML or HTML files", flush=True)
            return None
        
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
        print("="*80, flush=True)
        
        return None


def parse_sec_form_xml(xml_content: bytes, folder_key: str, filing_date: str) -> Dict[str, Any]:
    """
    Parse SEC Form XML and extract metadata and trades
    
    Args:
        xml_content: XML file content as bytes
        folder_key: S3 folder key (for reference)
        filing_date: Filing date string
    
    Returns:
        Dict with parsed metadata and trades
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
        'trades': [],
    }
    
    try:
        # Parse XML
        root = ET.fromstring(xml_content)
        
        # Get namespace if present
        ns = {'': ''}  # Default namespace
        if root.tag.startswith('{'):
            # Extract namespace
            ns_uri = root.tag.split('}')[0][1:]
            ns = {'ns': ns_uri}
        
        # Helper to find text with namespace handling
        def find_text(elem, path, default=''):
            if ns and 'ns' in ns:
                # Try with namespace first
                found = elem.find(f'.//ns:{path}', ns)
                if found is not None:
                    return found.text or default
                # Try without namespace
                found = elem.find(f'.//{path}')
                if found is not None:
                    return found.text or default
            else:
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
                # Convert YYYY-MM-DD to MM/DD/YYYY for consistency
                date_obj = datetime.strptime(period_of_report, '%Y-%m-%d')
                result['eventDate'] = date_obj.strftime('%m/%d/%Y')
                result['reportingDate'] = date_obj.strftime('%m/%d/%Y')
                except:
                result['eventDate'] = period_of_report
                result['reportingDate'] = period_of_report
        
        # Extract issuer information
        issuer = root.find('.//issuer') if not ns or 'ns' not in ns else root.find('.//ns:issuer', ns)
        if issuer is not None:
            result['issuerName'] = find_text(issuer, 'issuerName', '')
            result['tickerSymbol'] = find_text(issuer, 'issuerTradingSymbol', '')
        
        # Extract reporting owner information (use first one)
        reporting_owners = root.findall('.//reportingOwner') if not ns or 'ns' not in ns else root.findall('.//ns:reportingOwner', ns)
        if reporting_owners:
            owner = reporting_owners[0]
            
            # Get name
            owner_id = owner.find('reportingOwnerId') if not ns or 'ns' not in ns else owner.find('ns:reportingOwnerId', ns)
            if owner_id is not None:
                result['reportingPersonName'] = find_text(owner_id, 'rptOwnerName', '')
            
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
                    result['address'] = ", ".join(address_parts)
            
            # Get relationship
            owner_rel = owner.find('reportingOwnerRelationship') if not ns or 'ns' not in ns else owner.find('ns:reportingOwnerRelationship', ns)
            if owner_rel is not None:
                # Build relationship code (bitmask: 1=Director, 2=Officer, 4=10%Owner, 8=Other)
                rel_code = 0
                
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
                
                result['relationship'] = str(rel_code) if rel_code > 0 else None
        
        # Extract signature
        signatures = root.findall('.//ownerSignature') if not ns or 'ns' not in ns else root.findall('.//ns:ownerSignature', ns)
        if signatures:
            signature = signatures[0]
            result['signatureName'] = find_text(signature, 'signatureName', '')
            sig_date = find_text(signature, 'signatureDate', '')
            if sig_date:
                try:
                    date_obj = datetime.strptime(sig_date, '%Y-%m-%d')
                    result['reportingDate'] = date_obj.strftime('%m/%d/%Y')
            except:
                    result['reportingDate'] = sig_date
        
        # Extract non-derivative transactions (Table I)
        non_deriv_table = root.find('.//nonDerivativeTable') if not ns or 'ns' not in ns else root.find('.//ns:nonDerivativeTable', ns)
        if non_deriv_table is not None:
            transactions = non_deriv_table.findall('.//nonDerivativeTransaction') if not ns or 'ns' not in ns else non_deriv_table.findall('.//ns:nonDerivativeTransaction', ns)
            
            for trans in transactions:
                trade = {}
                
                # Security title
                trade['securityName'] = find_value(trans, 'securityTitle', '')
                
                # Transaction date
                trans_date = find_value(trans, 'transactionDate', '')
                if trans_date:
                    try:
                        date_obj = datetime.strptime(trans_date, '%Y-%m-%d')
                        trade['transactionDate'] = date_obj.strftime('%m/%d/%Y')
            except:
                        trade['transactionDate'] = trans_date
                
                # Transaction code
                trans_coding = trans.find('transactionCoding') if not ns or 'ns' not in ns else trans.find('ns:transactionCoding', ns)
                if trans_coding is not None:
                    trade['transactionCode'] = find_text(trans_coding, 'transactionCode', '')
                    trade['transactionCodeMeaning'] = get_transaction_code_meaning(trade['transactionCode'])
                
                # Transaction amounts
                trans_amounts = trans.find('transactionAmounts') if not ns or 'ns' not in ns else trans.find('ns:transactionAmounts', ns)
                if trans_amounts is not None:
                    shares = find_value(trans_amounts, 'transactionShares', '0')
                    price = find_value(trans_amounts, 'transactionPricePerShare', '0')
                    direction = find_value(trans_amounts, 'transactionAcquiredDisposedCode', '')
                    
                    try:
                        trade['shares'] = float(shares) if shares else 0.0
            except:
                        trade['shares'] = 0.0
                    
                    try:
                        trade['price'] = float(price) if price else 0.0
                    except:
                        trade['price'] = 0.0
                    
                    trade['transactionDirection'] = direction
                    trade['transactionDirectionMeaning'] = get_transaction_direction_meaning(direction)
                    
                    # Calculate value
                    trade['value'] = trade['shares'] * trade['price']
                
                # Post-transaction amounts
                post_trans = trans.find('postTransactionAmounts') if not ns or 'ns' not in ns else trans.find('ns:postTransactionAmounts', ns)
                if post_trans is not None:
                    shares_owned = find_value(post_trans, 'sharesOwnedFollowingTransaction', '0')
                    try:
                        trade['sharesOwnedAfter'] = float(shares_owned) if shares_owned else 0.0
                except:
                        trade['sharesOwnedAfter'] = 0.0
                
                # Ownership nature
                ownership = trans.find('ownershipNature') if not ns or 'ns' not in ns else trans.find('ns:ownershipNature', ns)
                if ownership is not None:
                    trade['ownershipType'] = find_value(ownership, 'directOrIndirectOwnership', '')
                    trade['ownershipTypeMeaning'] = get_ownership_type_meaning(trade['ownershipType'])
                
                if trade.get('securityName') or trade.get('transactionDate'):
                    result['trades'].append(trade)
        
        # Extract derivative transactions (Table II)
        deriv_table = root.find('.//derivativeTable') if not ns or 'ns' not in ns else root.find('.//ns:derivativeTable', ns)
        if deriv_table is not None:
            holdings = deriv_table.findall('.//derivativeHolding') if not ns or 'ns' not in ns else deriv_table.findall('.//ns:derivativeHolding', ns)
            
            for holding in holdings:
                trade = {}
                
                # Security title
                trade['securityName'] = find_value(holding, 'securityTitle', '')
                
                # Conversion/exercise price
                conv_price = find_value(holding, 'conversionOrExercisePrice', '0')
                try:
                    trade['exercisePrice'] = float(conv_price) if conv_price else 0.0
                except:
                    trade['exercisePrice'] = 0.0
                
                # Transaction date
                trans_date = find_value(holding, 'transactionDate', '')
                if trans_date:
                    try:
                        date_obj = datetime.strptime(trans_date, '%Y-%m-%d')
                        trade['transactionDate'] = date_obj.strftime('%m/%d/%Y')
                except:
                        trade['transactionDate'] = trans_date
                
                # Transaction code
                trans_coding = holding.find('transactionCoding') if not ns or 'ns' not in ns else holding.find('ns:transactionCoding', ns)
                if trans_coding is not None:
                    trade['transactionCode'] = find_text(trans_coding, 'transactionCode', '')
                    trade['transactionCodeMeaning'] = get_transaction_code_meaning(trade['transactionCode'])
                
                # Transaction amounts
                trans_amounts = holding.find('transactionAmounts') if not ns or 'ns' not in ns else holding.find('ns:transactionAmounts', ns)
                if trans_amounts is not None:
                    shares_acquired = find_value(trans_amounts, 'transactionShares', '0')
                    shares_disposed = find_value(trans_amounts, 'transactionShares', '0')  # May need adjustment
                    direction = find_value(trans_amounts, 'transactionAcquiredDisposedCode', '')
                    
                    try:
                        trade['shares'] = float(shares_acquired) if shares_acquired else 0.0
                    except:
                        trade['shares'] = 0.0
                    
                    trade['transactionDirection'] = direction
                    trade['transactionDirectionMeaning'] = get_transaction_direction_meaning(direction)
                
                # Underlying security
                underlying = holding.find('underlyingSecurity') if not ns or 'ns' not in ns else holding.find('ns:underlyingSecurity', ns)
                if underlying is not None:
                    trade['underlyingSecurityName'] = find_value(underlying, 'underlyingSecurityTitle', '')
                    underlying_shares = find_value(underlying, 'underlyingSecurityShares', '0')
                    try:
                        trade['underlyingShares'] = float(underlying_shares) if underlying_shares else 0.0
                    except:
                        trade['underlyingShares'] = 0.0
                
                # Ownership nature
                ownership = holding.find('ownershipNature') if not ns or 'ns' not in ns else holding.find('ns:ownershipNature', ns)
                if ownership is not None:
                    trade['ownershipType'] = find_value(ownership, 'directOrIndirectOwnership', '')
                    trade['ownershipTypeMeaning'] = get_ownership_type_meaning(trade['ownershipType'])
                    trade['natureOfOwnership'] = find_value(ownership, 'natureOfOwnership', '')
                
                if trade.get('securityName') or trade.get('transactionDate'):
                    result['trades'].append(trade)
        
        local_logger.info(f"   ✅ Parsed XML: {len(result['trades'])} trades extracted")
        print(f"   ✅ Parsed XML: {len(result['trades'])} trades extracted", flush=True)
        
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
    """Calculate similarity score between filer name and politician name"""
    if not filer_name or not politician.get('name'):
        return 0.0
    
    # Try primary name
    primary_name = politician.get('name', '')
    primary_score = SequenceMatcher(None, filer_name.lower(), primary_name.lower()).ratio()
    
    # Try alternative names
    max_score = primary_score
    for alt_name in politician.get('alternativeNames', []):
        alt_score = SequenceMatcher(None, filer_name.lower(), alt_name.lower()).ratio()
        max_score = max(max_score, alt_score)
    
    return max_score


def find_matching_politician(filer_name: str, politicians: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Find matching politician by name"""
    if not filer_name:
        return None
    
    best_match = None
    best_score = 0.0
    
    for politician in politicians:
        score = fuzzy_match_name(filer_name, politician)
        if score > best_score and score >= NAME_MATCH_THRESHOLD:
            best_score = score
            best_match = politician.copy()
            best_match['matchScore'] = score
    
    return best_match


def process_form(form_data: Dict[str, Any], target_date: str, politicians: List[Dict[str, Any]], s3_bucket_name: str, dynamodb_table_name: str) -> Dict[str, Any]:
    """
    Process a single SEC form: download both HTML/XML, parse XML, check politician match, store
    """
    import logging
    local_logger = logging.getLogger()
    
    cik = form_data.get('cik', 'unknown')
    accession = form_data.get('accession_number', 'unknown')
    form_type = form_data.get('form_type', 'unknown')
    filing_date_str = form_data.get('filing_date', target_date)
    
    local_logger.info(f"📄 Processing Form: CIK={cik}, Accession={accession}, Type={form_type}")
    print(f"📄 Processing Form: CIK={cik}, Accession={accession}, Type={form_type}", flush=True)
    
    # Validate filing date
    filing_date_obj = None
    if filing_date_str and filing_date_str != 'unknown':
        try:
            try:
                filing_date_obj = datetime.strptime(filing_date_str, '%Y-%m-%d').date()
            except ValueError:
                    filing_date_obj = datetime.strptime(filing_date_str, '%m/%d/%Y').date()
        except:
            pass
    
    target_date_obj = datetime.strptime(target_date, '%Y-%m-%d').date()
    
    if filing_date_obj is None or filing_date_obj != target_date_obj:
        local_logger.info(f"   ⏭️ SKIPPING: Date mismatch")
        print(f"   ⏭️ SKIPPING: Date mismatch", flush=True)
        return {'skipped': True, 'reason': 'date_mismatch'}
    
    # Download both HTML and XML
    download_start = datetime.now()
    downloaded = download_sec_form_both(form_data, target_date, s3_bucket_name)
    download_duration = (datetime.now() - download_start).total_seconds()
    
    if not downloaded:
        local_logger.warning(f"   ❌ FAILED: Could not download form")
        print(f"   ❌ FAILED: Could not download form", flush=True)
        return {'skipped': True, 'reason': 'download_failed'}
    
    folder_key = downloaded.get('folder_key', 'unknown')
    xml_content = downloaded.get('xml_content')
    
    local_logger.info(f"   ✅ Downloaded: {folder_key} in {download_duration:.2f}s")
    print(f"   ✅ Downloaded: {folder_key} in {download_duration:.2f}s", flush=True)
    
    # Parse XML (prefer XML over HTML)
    parse_start = datetime.now()
    if xml_content:
        parsed_data = parse_sec_form_xml(xml_content, folder_key, filing_date_str)
    else:
        local_logger.warning(f"   ⚠️ No XML content available, skipping")
        print(f"   ⚠️ No XML content available, skipping", flush=True)
        return {'skipped': True, 'reason': 'no_xml_content'}
    
    parse_duration = (datetime.now() - parse_start).total_seconds()
    local_logger.info(f"   ✅ Parsing complete in {parse_duration:.2f}s")
    print(f"   ✅ Parsing complete in {parse_duration:.2f}s", flush=True)
    
    # Check politician match
    politician_match = None
    if parsed_data.get('reportingPersonName'):
        politician_match = find_matching_politician(parsed_data['reportingPersonName'], politicians)
        if politician_match:
            local_logger.info(f"   ✅ POLITICIAN MATCH: {politician_match.get('name', 'N/A')}")
            print(f"   ✅ POLITICIAN MATCH: {politician_match.get('name', 'N/A')}", flush=True)
            parsed_data['politician'] = 1
        else:
            parsed_data['politician'] = 0
    else:
        parsed_data['politician'] = 0
    
    # Generate trade ID
    trade_id = f"sec_{form_type}_{cik}_{accession}_{target_date.replace('-', '')}"
    parsed_data['tradeId'] = trade_id
    parsed_data['formS3Key'] = folder_key  # Store folder key, not individual file
    
    # Store to DynamoDB (simplified - full implementation would match existing logic)
    # This is a placeholder - full implementation would include all fields
    try:
        dynamodb_client = boto3.client('dynamodb')
        
        item = {
            'tradeId': {'S': trade_id},
            'formS3Key': {'S': folder_key},
            'formType': {'S': parsed_data.get('formType', 'unknown')},
            'reportingPersonName': {'S': parsed_data.get('reportingPersonName', '')},
            'address': {'S': parsed_data.get('address', '')} if parsed_data.get('address') else {'NULL': True},
            'issuerName': {'S': parsed_data.get('issuerName', '')} if parsed_data.get('issuerName') else {'NULL': True},
            'tickerSymbol': {'S': parsed_data.get('tickerSymbol', '')} if parsed_data.get('tickerSymbol') else {'NULL': True},
            'relationship': {'S': parsed_data.get('relationship', '')} if parsed_data.get('relationship') else {'NULL': True},
            'eventDate': {'S': parsed_data.get('eventDate', '')} if parsed_data.get('eventDate') else {'NULL': True},
            'reportingDate': {'S': parsed_data.get('reportingDate', '')} if parsed_data.get('reportingDate') else {'NULL': True},
            'politician': {'N': str(parsed_data.get('politician', 0))},
            'filingDate': {'S': target_date},
        }
        
        dynamodb_client.put_item(
            TableName=dynamodb_table_name,
            Item=item
        )
        
        local_logger.info(f"   ✅ Stored to DynamoDB: {trade_id}")
        print(f"   ✅ Stored to DynamoDB: {trade_id}", flush=True)
        
        return {
            'success': True, 
            'trade_id': trade_id,
            'folder_key': folder_key,
            'trades_count': len(parsed_data.get('trades', [])),
            'politician_match': politician_match is not None
        }
    
    except Exception as e:
        local_logger.error(f"   ❌ Error storing to DynamoDB: {e}")
        print(f"   ❌ Error storing to DynamoDB: {e}", flush=True)
        return {'skipped': True, 'reason': 'dynamodb_error'}


# Main execution
if __name__ == '__main__':
try:
        logger.info("="*80)
        logger.info("🚀 SEC Forms ETL Pipeline (XML-based) Starting")
    logger.info(f"📅 Target Date: {target_date}")
    logger.info(f"📦 S3 Bucket: {s3_bucket}")
        logger.info(f"💾 DynamoDB Table: {dynamodb_table}")
        print("="*80, flush=True)
        print("🚀 SEC Forms ETL Pipeline (XML-based) Starting", flush=True)
        print(f"📅 Target Date: {target_date}", flush=True)
        print(f"📦 S3 Bucket: {s3_bucket}", flush=True)
        print(f"💾 DynamoDB Table: {dynamodb_table}", flush=True)
        
        # Load politician list
    politicians = load_politician_list()
        
        # Fetch forms
        forms = fetch_sec_forms_paginated(target_date, ['3', '4', '5'])
        
        if not forms:
            logger.info("ℹ️ No forms found for target date")
            print("ℹ️ No forms found for target date", flush=True)
            job.commit()
            sys.exit(0)
        
        # Process forms in parallel using Spark
    forms_rdd = sc.parallelize(forms)
    
    def process_form_wrapper(form_data):
            return process_form(
                form_data,
                target_date,
                politicians,
                s3_bucket,
                dynamodb_table
            )
        
        results = forms_rdd.map(process_form_wrapper).collect()
        
        # Summary
        successful = sum(1 for r in results if r.get('success'))
        skipped = len(results) - successful
    
    logger.info("")
        logger.info("="*80)
        logger.info("✅ Processing Complete")
        logger.info(f"   Total Forms: {len(results)}")
        logger.info(f"   Successful: {successful}")
        logger.info(f"   Skipped: {skipped}")
        print("="*80, flush=True)
        print("✅ Processing Complete", flush=True)
        print(f"   Total Forms: {len(results)}", flush=True)
        print(f"   Successful: {successful}", flush=True)
        print(f"   Skipped: {skipped}", flush=True)
    
    job.commit()
    
except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
    import traceback
        logger.error(traceback.format_exc())
        print(f"❌ Fatal error: {e}", flush=True)
        job.commit()
    raise

