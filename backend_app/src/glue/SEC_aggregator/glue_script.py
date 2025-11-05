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
        logger.info(f"📋 Fetching Form {form_type} filings for {target_date}...")
        forms_for_type = []
        start = 0
        count = 100
        page = 1
        max_pages = 100  # Safety limit to prevent infinite loops
        
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
                    
                    # Extract filing date from row
                    # Look for date pattern MM/DD/YYYY in the row
                    date_match = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', row)
                    filing_date_str = None
                    if date_match:
                        filing_date_str = date_match.group(1)
                        try:
                            filing_date_obj = datetime.strptime(filing_date_str, '%m/%d/%Y').date()
                            # Only include if matches target date
                            if filing_date_obj != target_date_obj:
                                continue
                        except:
                            # If date parsing fails, include anyway (will verify during download)
                            pass
                    else:
                        # No date found in row - include anyway (will verify during download)
                        pass
                    
                    form_data = {
                        'cik': cik,
                        'accession_number': accession_clean,
                        'form_type': f'form{form_type}',
                        'filing_date': filing_date_str or target_date
                    }
                    
                    page_forms.append(form_data)
                
                logger.info(f"   Page {page}: Found {len(page_forms)} forms matching date {target_date} (out of {page_forms_before_date_filter} total forms on page)")
                
                # If no forms found on this page, we've reached the end
                if not page_forms and page_forms_before_date_filter == 0:
                    logger.info(f"   No forms found on page {page}, stopping pagination")
                    break
                
                # Add forms from this page
                forms_for_type.extend(page_forms)
                
                # Check if we should continue
                # If we got fewer than count forms on the page, we're likely at the end
                # But we also need to check if we're getting dates beyond our target
                if page_forms_before_date_filter < count:
                    logger.info(f"   Reached last page (got {page_forms_before_date_filter} < {count} forms on page)")
                    break
                
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
        
        logger.info(f"✅ Found {len(forms_for_type)} Form {form_type} filings for {target_date} (across {page-1} pages)")
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
                    
                    local_logger.debug(f"      Downloaded from: {file_url} ({len(file_content)} bytes)")
                    break
                else:
                    local_logger.debug(f"      Attempted {ext}: Status {response.status_code}")
            except Exception as e:
                local_logger.debug(f"      Attempted {ext}: Error - {str(e)[:100]}")
                continue
        
        if not file_content:
            local_logger.warning(f"   ⚠️ Could not download form from any URL: CIK={cik}, accession={accession_dashed}")
            local_logger.warning(f"      Tried URLs: {base_url}/[accession]-primary-document.xml, "
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
        
        local_logger.debug(f"      Uploaded to S3: {s3_key} ({len(file_content)} bytes)")
        
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
    trades = []
    
    try:
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
            logger.warning(f"⚠️ Could not extract filer name from HTML {s3_key}")
            return trades
        
        logger.info(f"✅ Extracted filer name from HTML: {filer_name}")
        
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
        
        logger.info(f"✅ Extracted {len(trades)} trades from {s3_key}")
        
    except Exception as e:
        logger.error(f"❌ Error parsing SEC form HTML {s3_key}: {e}")
        import traceback
        logger.error(f"   Traceback: {traceback.format_exc()}")
    
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
    best_match = None
    best_score = 0.0
    
    for politician in politicians:
        score = fuzzy_match_name(filer_name, politician)
        if score > best_score:
            best_score = score
            best_match = politician
    
    if best_score >= NAME_MATCH_THRESHOLD:
        return {
            **best_match,
            'matchScore': best_score
        }
    
    return None


def process_form(form_data: Dict[str, Any], target_date: str, politicians: List[Dict[str, Any]], s3_bucket_name: str) -> List[Dict[str, Any]]:
    """
    Process a single SEC form: download, parse, match
    
    Args:
        form_data: Form metadata
        target_date: Target date
        politicians: List of politicians for matching
        s3_bucket_name: S3 bucket name (passed explicitly to avoid capturing module-level vars)
    
    Logs detailed information at each stage for tracking and debugging.
    """
    # Import inside function to avoid serialization issues
    import logging
    local_logger = logging.getLogger()
    
    cik = form_data.get('cik', 'unknown')
    accession = form_data.get('accession_number', 'unknown')
    form_type = form_data.get('form_type', 'unknown')
    
    local_logger.info(f"📄 Processing Form: CIK={cik}, Accession={accession}, Type={form_type}")
    
    # Download form
    local_logger.info(f"   📥 Step 1/3: Downloading form...")
    downloaded = download_sec_form(form_data, target_date, s3_bucket_name)
    if not downloaded:
        local_logger.warning(f"   ❌ FAILED: Could not download form (CIK={cik}, Accession={accession})")
        return []
    
    s3_key = downloaded.get('s3_key', 'unknown')
    file_ext = downloaded.get('file_ext', 'unknown')
    local_logger.info(f"   ✅ Downloaded: {s3_key} (ext: {file_ext})")
    
    # Parse form
    local_logger.info(f"   📊 Step 2/3: Parsing form content...")
    trades = []
    if downloaded['file_ext'] == 'html' or downloaded['file_ext'] == 'xml':
        content_str = downloaded['content'].decode('utf-8', errors='ignore')
        trades = parse_sec_form_html(content_str, downloaded['s3_key'], target_date)
        local_logger.info(f"   ✅ Parsed: Extracted {len(trades)} trades from form")
        
        # Log sample trades for debugging
        if trades:
            sample_trade = trades[0]
            local_logger.info(f"   📋 Sample trade: filer={sample_trade.get('filerName', 'N/A')}, "
                       f"security={sample_trade.get('securityName', 'N/A')[:50]}, "
                       f"amount=${sample_trade.get('totalAmount', 'N/A')}")
    else:
        local_logger.warning(f"   ⚠️ Unsupported file type: {downloaded['file_ext']} (CIK={cik}, Accession={accession})")
        return []
    
    if not trades:
        local_logger.warning(f"   ⚠️ No trades extracted from form (CIK={cik}, Accession={accession}, S3={s3_key})")
        return []
    
    # Match trades to politicians
    local_logger.info(f"   🔍 Step 3/3: Matching {len(trades)} trades to politicians...")
    matched_trades = []
    unmatched_trades = []
    unique_filers = set()
    
    for trade_idx, trade in enumerate(trades, 1):
        filer_name = trade.get('filerName')
        if not filer_name:
            local_logger.debug(f"      Trade {trade_idx}/{len(trades)}: Skipping (no filer name)")
            unmatched_trades.append(trade)
            continue
        
        unique_filers.add(filer_name)
        
        matched_politician = find_matching_politician(filer_name, politicians)
        if matched_politician:
            # Map amount to standard range
            total_amount = trade.get('totalAmount')
            exact_amount = None
            amount_range = None
            amount_min = None
            amount_max = None
            
            if total_amount and isinstance(total_amount, (int, float)):
                exact_amount = int(total_amount)
                standard_range = find_standard_range(float(total_amount))
                
                if standard_range[1] is None:
                    amount_range = [standard_range[0], 999999999]
                    amount_min = standard_range[0]
                    amount_max = 999999999
                else:
                    amount_range = [standard_range[0], standard_range[1]]
                    amount_min = standard_range[0]
                    amount_max = standard_range[1]
            
            # Convert transaction date to numeric
            transaction_date_str = trade.get('transactionDate') or target_date
            try:
                transaction_date_obj = datetime.strptime(transaction_date_str, '%Y-%m-%d').date()
                transaction_date_num = int(transaction_date_obj.strftime('%Y%m%d'))
            except:
                transaction_date_num = int(target_date.replace('-', ''))
            
            matched_trade = {
                'tradeId': f"trade_{target_date.replace('-', '_')}_sec_{len(matched_trades)}",
                'politicianName': matched_politician['name'],
                'party': matched_politician['party'],
                'position': matched_politician['position'],
                'websiteUrl': matched_politician.get('websiteUrl'),
                'formType': downloaded['form_type'],
                'filingDate': target_date,
                'transactionDate': transaction_date_num,
                'securitySymbol': trade.get('securitySymbol'),
                'securityName': trade.get('securityName'),
                'transactionType': trade.get('transactionType'),
                'shares': trade.get('shares'),
                'pricePerShare': trade.get('pricePerShare'),
                'totalAmount': total_amount,
                'amountMin': amount_min,
                'amountMax': amount_max,
                'amountRange': amount_range,
                'exactAmount': exact_amount,
                'formS3Key': downloaded['s3_key'],
                'matchConfidence': matched_politician.get('matchScore', 1.0),
                'source': 'sec',
                'processingDate': int(datetime.now().timestamp())
            }
            matched_trades.append(matched_trade)
            
            local_logger.debug(f"      Trade {trade_idx}/{len(trades)}: ✅ MATCHED - "
                        f"filer='{filer_name}' → politician='{matched_politician['name']}' "
                        f"(confidence={matched_politician.get('matchScore', 1.0):.2f}), "
                        f"security={trade.get('securityName', 'N/A')[:40]}, "
                        f"amount=${total_amount}")
        else:
            unmatched_trades.append(trade)
            local_logger.debug(f"      Trade {trade_idx}/{len(trades)}: ❌ NO MATCH - "
                        f"filer='{filer_name}' (no politician match found)")
    
    # Summary logging
    local_logger.info(f"   📊 Matching Summary:")
    local_logger.info(f"      - Total trades extracted: {len(trades)}")
    local_logger.info(f"      - Trades matched to politicians: {len(matched_trades)}")
    local_logger.info(f"      - Trades unmatched: {len(unmatched_trades)}")
    local_logger.info(f"      - Unique filers in form: {len(unique_filers)}")
    
    if unique_filers:
        filer_list = ', '.join(list(unique_filers)[:5])  # Show first 5
        if len(unique_filers) > 5:
            filer_list += f" ... (+{len(unique_filers) - 5} more)"
        local_logger.info(f"      - Filer names: {filer_list}")
    
    if unmatched_trades and len(unmatched_trades) > 0:
        unmatched_filers = set(t.get('filerName') for t in unmatched_trades if t.get('filerName'))
        if unmatched_filers:
            local_logger.warning(f"      ⚠️ Unmatched filers: {', '.join(list(unmatched_filers)[:5])}")
    
    local_logger.info(f"   ✅ Completed processing form: CIK={cik}, Accession={accession}, "
               f"Matched={len(matched_trades)}/{len(trades)}, S3={s3_key}")
    
    return matched_trades


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
    logger.info(f"🚀 SEC ETL Glue Job started for date: {target_date}")
    
    # Step 1: Load politician list
    logger.info("📋 Loading politician list...")
    politicians = load_politician_list()
    
    # Step 2: Fetch SEC forms with pagination
    logger.info(f"📋 Fetching SEC forms for {target_date}...")
    forms = fetch_sec_forms_paginated(target_date)
    logger.info(f"✅ Fetched {len(forms)} forms")
    
    # Step 3: Process forms in parallel using Spark
    logger.info(f"📊 Processing {len(forms)} forms...")
    logger.info(f"   Using Spark to process forms in parallel")
    
    # Broadcast necessary variables to avoid serialization issues
    # Broadcast variables are sent once to each worker, not serialized with each task
    politicians_broadcast = sc.broadcast(politicians)
    target_date_broadcast = sc.broadcast(target_date)
    s3_bucket_broadcast = sc.broadcast(s3_bucket)
    
    # Create RDD from forms list
    forms_rdd = sc.parallelize(forms)
    
    # Track processing statistics
    processing_stats = {
        'total_forms': len(forms),
        'successful_downloads': 0,
        'failed_downloads': 0,
        'successful_parses': 0,
        'failed_parses': 0,
        'forms_with_trades': 0,
        'forms_without_trades': 0,
        'forms_with_matches': 0,
        'forms_without_matches': 0,
        'total_trades_extracted': 0,
        'total_trades_matched': 0
    }
    
    # Process each form (download, parse, match)
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
            
            cik = form_data.get('cik', 'unknown')
            accession = form_data.get('accession_number', 'unknown')
            
            local_logger.info(f"🔄 Starting processing: Form {form_data.get('form_type', 'unknown')} - CIK={cik}, Accession={accession}")
            
            # Call process_form with explicit parameters from broadcast
            # Note: process_form will create its own boto3 clients inside, so no SSLContext issues
            matched_trades = process_form(form_data, target_date_local, politicians_local, s3_bucket_local)
            
            # Update stats (these will be approximate since we're in distributed processing)
            if matched_trades:
                local_logger.info(f"✅ Form completed: CIK={cik}, Matched trades={len(matched_trades)}")
            else:
                local_logger.info(f"⚠️ Form completed (no matches): CIK={cik}, Accession={accession}")
            
            return matched_trades
        except Exception as e:
            cik = form_data.get('cik', 'unknown')
            accession = form_data.get('accession_number', 'unknown')
            local_logger.error(f"❌ FATAL ERROR processing form (CIK={cik}, Accession={accession}): {e}")
            local_logger.error(f"   Traceback: {traceback.format_exc()}")
            return []
    
    matched_trades_rdd = forms_rdd.flatMap(process_form_wrapper)
    matched_trades = matched_trades_rdd.collect()
    
    # Clean up broadcast variables
    politicians_broadcast.destroy()
    target_date_broadcast.destroy()
    s3_bucket_broadcast.destroy()
    
    # Calculate final statistics
    total_trades = len(matched_trades)
    total_forms_processed = len(forms)
    
    logger.info(f"📊 Processing Complete - Final Statistics:")
    logger.info(f"   - Total forms fetched: {total_forms_processed}")
    logger.info(f"   - Total trades matched: {total_trades}")
    logger.info(f"   - Average trades per form: {total_trades / total_forms_processed if total_forms_processed > 0 else 0:.2f}")
    
    if total_trades == 0:
        logger.warning(f"   ⚠️ WARNING: No trades were matched from any forms!")
        logger.warning(f"      This could indicate:")
        logger.warning(f"      - Download failures for all forms")
        logger.warning(f"      - Parsing failures for all forms")
        logger.warning(f"      - No filer names matched to politicians")
    elif total_trades < total_forms_processed:
        logger.info(f"   ℹ️ Note: {total_forms_processed - total_trades} forms did not produce matched trades")
        logger.info(f"      (This is normal - some forms may not have trades or may not match politicians)")
    
    # Step 4: Write to DynamoDB
    logger.info(f"💾 Writing {len(matched_trades)} trades to DynamoDB...")
    write_to_dynamodb(matched_trades, dynamodb_table)
    
    # Step 5: Write summary to S3
    summary = {
        'date': target_date,
        'secFormsFetched': len(forms),
        'matchedTrades': matched_trades,
        'unmatchedCount': len(forms) - len(matched_trades),
        'source': 'sec'
    }
    
    # Create S3 client locally to avoid Spark serialization issues
    s3_client_local = boto3.client('s3')
    summary_key = f"temp/sec-results-{target_date}.json"
    s3_client_local.put_object(
        Bucket=s3_bucket,
        Key=summary_key,
        Body=json.dumps(summary, default=str),
        ContentType='application/json'
    )
    
    logger.info(f"✅ Wrote summary to s3://{s3_bucket}/{summary_key}")
    logger.info(f"✅ SEC ETL Job completed successfully")
    
    job.commit()
    
except Exception as e:
    logger.error(f"❌ Fatal error in SEC ETL job: {e}")
    import traceback
    logger.error(f"Traceback: {traceback.format_exc()}")
    raise

