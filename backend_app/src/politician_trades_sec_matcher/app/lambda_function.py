"""
Lambda function to match trades from a single SEC form to politicians.
This function is invoked in parallel via Step Functions Map state.
"""

import json
import os
import logging
import re
import boto3
from typing import List, Dict, Any, Optional
from datetime import datetime
import csv
from io import StringIO
import xml.etree.ElementTree as ET
from html.parser import HTMLParser
from html import unescape
from difflib import SequenceMatcher

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS clients
s3_client = boto3.client('s3')

# Environment variables
S3_BUCKET = os.environ.get('S3_BUCKET')

# Name matching threshold (0.0 to 1.0)
NAME_MATCH_THRESHOLD = 0.85  # 85% similarity


def load_politician_list() -> List[Dict[str, Any]]:
    """
    Load congress-legislators CSV from S3
    
    Returns:
        List of politician dicts with name, party, position, url, and alternativeNames
    """
    try:
        # Download congress-legislators.csv from S3
        response = s3_client.get_object(
            Bucket=S3_BUCKET,
            Key='congress-legislators.csv'
        )
        
        csv_content = response['Body'].read().decode('utf-8')
        csv_reader = csv.DictReader(StringIO(csv_content))
        
        politicians = []
        for row in csv_reader:
            # Construct full name from components
            # CSV format: last_name,first_name,middle_name,suffix,nickname,full_name,...
            name_parts = []
            if row.get('first_name'):
                name_parts.append(row['first_name'])
            if row.get('middle_name'):
                name_parts.append(row['middle_name'])
            if row.get('last_name'):
                name_parts.append(row['last_name'])
            if row.get('suffix'):
                name_parts.append(row['suffix'])
            
            # Use constructed name or fall back to full_name
            # Prefer full_name if available as it's more reliable
            if row.get('full_name') and row.get('full_name').strip():
                primary_name = row.get('full_name').strip()
            else:
                primary_name = ' '.join(name_parts) if name_parts else ''
            
            # Build alternative names
            alt_names = []
            if row.get('nickname'):
                alt_names.append(row['nickname'])
            if row.get('full_name') and row.get('full_name').strip() != primary_name:
                alt_names.append(row['full_name'].strip())
            
            # Determine position from type
            leg_type = row.get('type', '').lower().strip()
            if leg_type == 'sen':
                position = 'Senate'
            elif leg_type == 'rep':
                position = 'House'
            else:
                position = leg_type  # fallback
            
            # Get party
            party = row.get('party', '').strip()
            
            # Get website URL
            website_url = row.get('url', '').strip()
            
            politicians.append({
                'name': primary_name,
                'party': party,
                'position': position,
                'websiteUrl': website_url if website_url else None,
                'alternativeNames': alt_names,
                'bioguide_id': row.get('bioguide_id', '').strip() if row.get('bioguide_id') else None,
                'state': row.get('state', '').strip() if row.get('state') else None,
                'district': row.get('district', '').strip() if row.get('district') else None
            })
        
        return politicians
        
    except Exception as e:
        logger.error(f"❌ Error loading congress-legislators list: {e}")
        return []


def fuzzy_match_name(filer_name: str, politician: Dict[str, Any]) -> float:
    """
    Fuzzy match a filer name to a politician using Levenshtein distance
    Handles various name formats:
    - "Last, First (Senator)" vs "First Last"
    - "(Senator)" suffix removal
    - Case insensitivity
    """
    # Normalize names (lowercase, strip)
    filer_normalized = filer_name.lower().strip()
    politician_normalized = politician['name'].lower().strip()
    
    # Remove position markers like "(Senator)", "(Representative)" from filer name
    filer_clean = re.sub(r'\s*\([^)]*(?:senator|representative)[^)]*\)', '', filer_normalized, flags=re.IGNORECASE)
    filer_clean = filer_clean.strip()
    
    # Check exact match first (after cleaning)
    if filer_clean == politician_normalized:
        return 1.0
    
    # Check alternative names
    for alt_name in politician.get('alternativeNames', []):
        alt_normalized = alt_name.lower().strip()
        if filer_clean == alt_normalized:
            return 1.0
    
    # Handle "Last, First" vs "First Last" format differences
    # If filer name contains a comma, try reversing the order
    if ',' in filer_clean:
        # Split by comma and reverse: "blumenthal, richard" -> "richard blumenthal"
        parts = [p.strip() for p in filer_clean.split(',')]
        if len(parts) == 2:
            filer_reversed = f"{parts[1]} {parts[0]}".strip()
            if filer_reversed == politician_normalized:
                return 1.0
            # Also check similarity with reversed format
            reversed_similarity = SequenceMatcher(None, filer_reversed, politician_normalized).ratio()
            if reversed_similarity > 0.9:
                return reversed_similarity
    
    # Calculate similarity using SequenceMatcher
    similarity = SequenceMatcher(None, filer_clean, politician_normalized).ratio()
    
    # Also check if names are subsets (e.g., "John Doe" vs "John A. Doe")
    if filer_clean in politician_normalized or politician_normalized in filer_clean:
        similarity = max(similarity, 0.9)
    
    # If similarity is still low, try with reversed name format
    if similarity < 0.85 and ',' in filer_clean:
        parts = [p.strip() for p in filer_clean.split(',')]
        if len(parts) == 2:
            filer_reversed = f"{parts[1]} {parts[0]}".strip()
            reversed_similarity = SequenceMatcher(None, filer_reversed, politician_normalized).ratio()
            similarity = max(similarity, reversed_similarity)
    
    return similarity


def find_matching_politician(filer_name: str, politicians: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Find best matching politician for a filer name
    """
    best_match = None
    best_score = 0.0
    
    for politician in politicians:
        score = fuzzy_match_name(filer_name, politician)
        if score > best_score:
            best_score = score
            best_match = politician
    
    # Return match if above threshold
    if best_score >= NAME_MATCH_THRESHOLD:
        return {
            **best_match,
            'matchScore': best_score
        }
    
    return None


def sanitize_xml_content(content: bytes) -> bytes:
    """
    Clean up XML content to handle SGML headers and malformed XML
    """
    try:
        # Try to decode as UTF-8
        text = content.decode('utf-8', errors='ignore')
    except:
        # Fallback to latin-1 or other encoding
        text = content.decode('latin-1', errors='ignore')
    
    # Remove SGML header if present (everything before <?xml or <ownershipDocument)
    # SGML headers typically look like: <SEC-HEADER>...</SEC-HEADER>
    if '<SEC-HEADER>' in text.upper() or '<ACCEPTANCE-DATETIME>' in text.upper():
        # Find the start of the actual XML document
        xml_start_patterns = [
            r'<\?xml',
            r'<ownershipDocument',
            r'<document',
            r'<edgarDocument',
        ]
        
        for pattern in xml_start_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                text = text[match.start():]
                logger.info(f"🔧 Removed SGML header, found XML at position {match.start()}")
                break
    
    # Fix common XML encoding issues
    # Replace smart quotes and other problematic characters
    text = text.replace('\x92', "'")  # Smart apostrophe
    text = text.replace('\x93', '"')  # Smart quote left
    text = text.replace('\x94', '"')  # Smart quote right
    text = text.replace('\x96', '-')  # En dash
    text = text.replace('\x97', '--')  # Em dash
    
    # Remove control characters that can break XML parsing (except newlines, tabs, carriage returns)
    text = ''.join(char for char in text if ord(char) >= 32 or char in '\n\r\t')
    
    return text.encode('utf-8')


def parse_sec_form_xml(s3_key: str) -> List[Dict[str, Any]]:
    """
    Parse SEC Form XML and extract trade data
    """
    trades = []
    
    try:
        # Download XML from S3
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
        xml_content_raw = response['Body'].read()
        
        # Sanitize XML content to handle SGML headers and malformed XML
        xml_content = sanitize_xml_content(xml_content_raw)
        
        # Parse XML - use iterparse for large files, but for now use fromstring
        try:
            root = ET.fromstring(xml_content)
        except ET.ParseError as parse_error:
            # If parsing fails, try to extract just the XML document part
            logger.warning(f"⚠️ XML parse error for {s3_key}: {parse_error}. Attempting recovery...")
            
            # Try to find and extract the ownershipDocument section
            text = xml_content.decode('utf-8', errors='ignore')
            ownership_match = re.search(r'<ownershipDocument.*?</ownershipDocument>', text, re.DOTALL | re.IGNORECASE)
            if ownership_match:
                xml_content = ownership_match.group(0).encode('utf-8')
                root = ET.fromstring(xml_content)
                logger.info(f"✅ Recovered XML by extracting ownershipDocument section")
            else:
                # Try to find any document section
                doc_match = re.search(r'<document[^>]*>.*?</document>', text, re.DOTALL | re.IGNORECASE)
                if doc_match:
                    xml_content = doc_match.group(0).encode('utf-8')
                    root = ET.fromstring(xml_content)
                    logger.info(f"✅ Recovered XML by extracting document section")
                else:
                    raise parse_error
        
        # Extract filer name - SEC XML uses various namespaces
        # Try multiple strategies to find the owner/filer name
        filer_name = None
        
        # Strategy 1: Try common namespace variations
        namespace_patterns = [
            '{http://www.sec.gov/edgar/document/edgardocument}',
            '{http://www.sec.gov/edgar/common}',
            '{http://xbrl.sec.gov/edgar/document/edgardocument}',
            '',  # No namespace
        ]
        
        name_elements = [
            'rptOwnerName',
            'rptOwner',
            'ownerName',
            'filerName',
            'filer',
            'issuerName',
            'reportingOwner',
        ]
        
        for ns in namespace_patterns:
            for elem_name in name_elements:
                full_name = f'{ns}{elem_name}' if ns else elem_name
                filer_name_elem = root.find(f'.//{full_name}')
                if filer_name_elem is not None and filer_name_elem.text:
                    filer_name = filer_name_elem.text.strip()
                    logger.info(f"✅ Found filer name using {full_name}: {filer_name}")
                    break
            if filer_name:
                break
        
        # Strategy 2: Search by tag name with wildcard namespace
        if not filer_name:
            for elem_name in name_elements:
                # Search all elements with this tag name regardless of namespace
                for elem in root.iter():
                    # Remove namespace from tag for comparison
                    tag_name = elem.tag.split('}')[-1] if '}' in elem.tag else elem.tag
                    if tag_name == elem_name and elem.text:
                        filer_name = elem.text.strip()
                        logger.info(f"✅ Found filer name using wildcard search for {elem_name}: {filer_name}")
                        break
                if filer_name:
                    break
        
        # Strategy 3: Look for reportingOwner element and find name inside it
        if not filer_name:
            owner_elems = root.findall('.//{http://www.sec.gov/edgar/document/edgardocument}reportingOwner')
            if not owner_elems:
                # Try without namespace
                owner_elems = root.findall('.//reportingOwner')
            if not owner_elems:
                # Try with different namespace
                owner_elems = root.findall('.//{http://xbrl.sec.gov/edgar/document/edgardocument}reportingOwner')
            
            for owner_elem in owner_elems:
                # Look for name elements inside reportingOwner
                for ns in namespace_patterns:
                    for elem_name in ['rptOwnerName', 'ownerName', 'name']:
                        full_name = f'{ns}{elem_name}' if ns else elem_name
                        name_elem = owner_elem.find(f'./{full_name}')
                        if name_elem is not None and name_elem.text:
                            filer_name = name_elem.text.strip()
                            logger.info(f"✅ Found filer name in reportingOwner using {full_name}: {filer_name}")
                            break
                    if filer_name:
                        break
                if filer_name:
                    break
        
        if not filer_name:
            logger.warning(f"⚠️ Could not extract filer name from {s3_key}. XML structure may be different.")
            logger.debug(f"   Root tag: {root.tag}, Root attributes: {root.attrib}")
            # Log some child elements for debugging
            child_tags = [child.tag for child in root[:5]]  # First 5 children
            logger.debug(f"   Sample child tags: {child_tags}")
            return trades
        
        # Extract transactions
        # Note: SEC XML is complex with namespaces - this is simplified
        # Full implementation would need to handle all transaction types
        
        logger.info(f"✅ Extracted filer name: {filer_name}")
        
    except Exception as e:
        logger.error(f"❌ Error parsing SEC form XML {s3_key}: {e}")
    
    return trades


def parse_sec_form_pdf(s3_key: str) -> List[Dict[str, Any]]:
    """
    Parse SEC Form PDF and extract trade data
    """
    trades = []
    
    try:
        # Download PDF from S3
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
        pdf_content = response['Body'].read()
        
        # Note: PDF parsing requires AWS Textract or pdf parsing library
        logger.warning("⚠️ PDF parsing not fully implemented - requires Textract or pdf library")
        
    except Exception as e:
        logger.error(f"❌ Error parsing SEC form PDF {s3_key}: {e}")
    
    return trades


def parse_sec_form_html(s3_key: str) -> List[Dict[str, Any]]:
    """
    Parse SEC Form HTML rendering and extract trade data
    HTML forms contain structured data in tables we can extract
    """
    trades = []
    
    try:
        # Download HTML from S3
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
        html_content = response['Body'].read().decode('utf-8', errors='ignore')
        
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
        
        # Detect form type - try multiple patterns in order of reliability
        form_number = None
        
        # Pattern 1 (Most Reliable): Look for "FORM 3", "FORM 4", "FORM 5" in FormName class
        # This is the most reliable as it's in the actual form header
        form_name_match = re.search(r'class="FormName"[^>]*>FORM\s*(\d+)', html_content, re.IGNORECASE | re.DOTALL)
        if form_name_match:
            form_number = form_name_match.group(1)
            logger.debug(f"📋 Detected form number from FormName class: {form_number}")
        
        # Pattern 2: Check filename if it contains form number (very reliable)
        if not form_number:
            form_in_filename = re.search(r'form[_-]?(\d+)', s3_key, re.IGNORECASE)
            if form_in_filename:
                form_number = form_in_filename.group(1)
                logger.info(f"📋 Detected form number from filename: {form_number}")
        
        # Pattern 3: <title>SEC FORM 3</title> or <title>SEC FORM\n            3</title>
        if not form_number:
            form_type_match = re.search(r'<title>SEC\s+FORM\s+(\d+)</title>', html_content, re.IGNORECASE | re.DOTALL)
            if form_type_match:
                form_number = form_type_match.group(1)
                logger.debug(f"📋 Detected form number from title: {form_number}")
        
        # Pattern 4 (Least Reliable): FORM 3 or FORM3 in body text (can match wrong things)
        # Only use if other patterns failed, and be more specific
        if not form_number:
            # Look for "FORM 3" or "FORM 4" or "FORM 5" followed by whitespace or HTML tag
            form_match = re.search(r'\bFORM\s+([345])\b', html_content, re.IGNORECASE)
            if form_match:
                form_number = form_match.group(1)
                logger.debug(f"📋 Detected form number from body text: {form_number}")
        
        is_form3 = form_number == '3'
        is_form4 = form_number == '4'
        is_form5 = form_number == '5'
        
        if form_number:
            logger.info(f"📋 Detected Form {form_number} (Form 3={is_form3}, Form 4={is_form4}, Form 5={is_form5})")
        else:
            logger.warning(f"⚠️ Could not detect form type from HTML content, defaulting to Form 4")
            # Default to Form 4 if we can't detect (most common)
            is_form4 = True
            is_form3 = False
            is_form5 = False
            form_number = '4'
        
        # Extract issuer name and ticker
        issuer_match = re.search(r'Issuer Name[^<]*<a[^>]*>([^<]+)</a>', html_content, re.IGNORECASE)
        issuer_name = issuer_match.group(1).strip() if issuer_match else None
        
        ticker_match = re.search(r'\[ <span[^>]*>([A-Z0-9]+)</span> \]', html_content)
        ticker = ticker_match.group(1) if ticker_match else None
        
        # Extract filing date
        filing_date = None
        
        # Form 4 has "Date of Earliest Transaction"
        if is_form4:
            date_match = re.search(r'Date of Earliest Transaction[^<]*<span[^>]*>(\d{1,2}/\d{1,2}/\d{4})</span>', html_content, re.IGNORECASE)
            if date_match:
                try:
                    filing_date_obj = datetime.strptime(date_match.group(1), '%m/%d/%Y')
                    filing_date = filing_date_obj.strftime('%Y-%m-%d')
                except:
                    pass
        
        # Form 3 has "Date of Event Requiring Statement"
        if is_form3:
            date_match = re.search(r'Date of Event Requiring Statement[^<]*<span[^>]*>(\d{1,2}/\d{1,2}/\d{4})</span>', html_content, re.IGNORECASE)
            if date_match:
                try:
                    filing_date_obj = datetime.strptime(date_match.group(1), '%m/%d/%Y')
                    filing_date = filing_date_obj.strftime('%Y-%m-%d')
                except:
                    pass
        
        # Parse Table I based on form type
        # Form 4: "Table I - Non-Derivative Securities Acquired, Disposed of, or Beneficially Owned" (transactions)
        # Form 3: "Table I - Non-Derivative Securities Beneficially Owned" (ownership snapshot, not transactions)
        
        table1_pattern = r'Table I[^<]*<tbody>(.*?)</tbody>'
        table1_match = re.search(table1_pattern, html_content, re.IGNORECASE | re.DOTALL)
        
        if table1_match:
            tbody_content = table1_match.group(1)
            # Extract table rows
            rows = re.findall(r'<tr[^>]*>(.*?)</tr>', tbody_content, re.DOTALL | re.IGNORECASE)
            
            for row in rows:
                # Extract data from table cells
                cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL | re.IGNORECASE)
                
                # Form 4 has 8+ columns with transaction data
                # Form 3 has 4 columns with ownership data (no transactions)
                if is_form4 and len(cells) >= 8:
                    # Clean HTML tags from cell content
                    def clean_cell(cell):
                        # Remove all HTML tags
                        text = re.sub(r'<[^>]+>', '', cell)
                        # Decode HTML entities
                        text = unescape(text)
                        # Clean whitespace
                        return text.strip()
                    
                    security_name = clean_cell(cells[0]) if len(cells) > 0 else None
                    trans_date = clean_cell(cells[1]) if len(cells) > 1 else None
                    trans_code = clean_cell(cells[3]) if len(cells) > 3 else None  # Code column
                    shares_str = clean_cell(cells[5]) if len(cells) > 5 else None  # Amount column
                    trans_type = clean_cell(cells[6]) if len(cells) > 6 else None  # (A) or (D)
                    price_str = clean_cell(cells[7]) if len(cells) > 7 else None  # Price column
                    
                    # Only extract rows that have actual transaction data
                    # Skip rows that only show ownership (no transaction date/code)
                    has_transaction = (trans_date and trans_date.strip() and 
                                     trans_code and trans_code.strip() and
                                     shares_str and shares_str.strip())
                    
                    if security_name and has_transaction:
                        # Parse shares (remove commas)
                        shares = None
                        if shares_str:
                            try:
                                shares = int(re.sub(r'[,\.]', '', shares_str))
                            except:
                                pass
                        
                        # Parse price (remove $ and commas)
                        price = None
                        if price_str:
                            try:
                                price_str_clean = re.sub(r'[\$,]', '', price_str)
                                price = float(price_str_clean)
                            except:
                                pass
                        
                        # Parse date
                        transaction_date = None
                        try:
                            trans_date_obj = datetime.strptime(trans_date, '%m/%d/%Y')
                            transaction_date = trans_date_obj.strftime('%Y-%m-%d')
                        except:
                            transaction_date = filing_date
                        
                        # Calculate total amount
                        total_amount = None
                        if shares and price:
                            total_amount = shares * price
                        
                        trade = {
                            'filerName': filer_name,
                            'issuerName': issuer_name,
                            'securitySymbol': ticker,
                            'securityName': security_name,
                            'transactionDate': transaction_date,
                            'filingDate': filing_date,
                            'transactionType': trans_code,
                            'shares': shares,
                            'pricePerShare': price,
                            'totalAmount': total_amount,
                            'transactionDirection': trans_type,  # A = Acquired, D = Disposed
                            'formType': f'form{form_number}' if form_number else 'form4',
                        }
                        trades.append(trade)
                elif is_form3 and len(cells) >= 4:
                    # Form 3: Table I shows ownership, not transactions
                    # Columns: Title, Amount Owned, Ownership Form (D/I), Nature of Indirect Ownership
                    def clean_cell(cell):
                        text = re.sub(r'<[^>]+>', '', cell)
                        text = unescape(text)
                        return text.strip()
                    
                    security_name = clean_cell(cells[0]) if len(cells) > 0 else None
                    shares_owned_str = clean_cell(cells[1]) if len(cells) > 1 else None
                    ownership_form = clean_cell(cells[2]) if len(cells) > 2 else None  # D or I
                    indirect_nature = clean_cell(cells[3]) if len(cells) > 3 else None
                    
                    if security_name and shares_owned_str:
                        # Parse shares owned
                        shares = None
                        try:
                            shares = int(re.sub(r'[,\.]', '', shares_owned_str))
                        except:
                            pass
                        
                        # Form 3 represents initial ownership at filing date
                        # Not a transaction, but we can treat it as an "initial acquisition" for tracking
                        trade = {
                            'filerName': filer_name,
                            'issuerName': issuer_name,
                            'securitySymbol': ticker,
                            'securityName': security_name,
                            'transactionDate': filing_date,  # Use filing date as reference
                            'filingDate': filing_date,
                            'transactionType': 'I',  # I = Initial Statement
                            'shares': shares,
                            'pricePerShare': None,  # Form 3 doesn't have price info
                            'totalAmount': None,
                            'transactionDirection': 'A',  # Treat as acquisition for initial ownership
                            'formType': 'form3',
                            'ownershipForm': ownership_form,
                            'indirectNature': indirect_nature,
                            'isInitialOwnership': True,  # Flag to indicate this is ownership, not a transaction
                        }
                        trades.append(trade)
        
        # Parse Table II - Derivative Securities
        # Form 4: "Table II - Derivative Securities Acquired, Disposed of, or Beneficially Owned" (transactions)
        # Form 3: "Table II - Derivative Securities Beneficially Owned" (ownership)
        table2_pattern = r'Table II[^<]*<tbody>(.*?)</tbody>'
        table2_match = re.search(table2_pattern, html_content, re.IGNORECASE | re.DOTALL)
        
        if table2_match:
            tbody_content = table2_match.group(1)
            rows = re.findall(r'<tr[^>]*>(.*?)</tr>', tbody_content, re.DOTALL | re.IGNORECASE)
            
            for row in rows:
                cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL | re.IGNORECASE)
                
                # Form 4 has 10+ columns with transaction data
                # Form 3 has fewer columns (ownership data)
                if is_form4 and len(cells) >= 10:
                    def clean_cell(cell):
                        text = re.sub(r'<[^>]+>', '', cell)
                        text = unescape(text)
                        return text.strip()
                    
                    derivative_name = clean_cell(cells[0]) if len(cells) > 0 else None
                    exercise_price_str = clean_cell(cells[1]) if len(cells) > 1 else None  # Conversion/Exercise Price
                    trans_date = clean_cell(cells[2]) if len(cells) > 2 else None
                    trans_code = clean_cell(cells[4]) if len(cells) > 4 else None  # Transaction Code
                    shares_acquired = clean_cell(cells[6]) if len(cells) > 6 else None
                    shares_disposed = clean_cell(cells[7]) if len(cells) > 7 else None
                    underlying_title = clean_cell(cells[10]) if len(cells) > 10 else None
                    underlying_shares = clean_cell(cells[11]) if len(cells) > 11 else None
                    price_str = clean_cell(cells[12]) if len(cells) > 12 else None  # Price of Derivative Security
                    
                    # Only extract rows that have actual transaction data
                    # Skip rows that only show ownership positions (no transaction date/code)
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
                                shares = -int(re.sub(r'[,\.]', '', shares_disposed))  # Negative for disposed
                            except:
                                pass
                        
                        # If no shares acquired or disposed, skip this row (it's just ownership)
                        if shares is None:
                            continue
                        
                        # Parse price - use exercise price if available, otherwise use derivative price
                        price = None
                        if price_str and price_str.strip():
                            try:
                                price_str_clean = re.sub(r'[\$,]', '', price_str)
                                price = float(price_str_clean)
                            except:
                                pass
                        elif exercise_price_str and exercise_price_str.strip():
                            # Use exercise price as the reference price
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
                            transaction_date = filing_date
                        
                        # Parse exercise price
                        exercise_price = None
                        if exercise_price_str and exercise_price_str.strip():
                            try:
                                exercise_price = float(re.sub(r'[\$,]', '', exercise_price_str))
                            except:
                                pass
                        
                        # Calculate total amount
                        total_amount = None
                        if shares and price:
                            total_amount = abs(shares) * price  # Use absolute value for amount
                        
                        trade = {
                            'filerName': filer_name,
                            'issuerName': issuer_name,
                            'securitySymbol': ticker,
                            'securityName': f"{derivative_name} (underlying: {underlying_title})" if underlying_title else derivative_name,
                            'transactionDate': transaction_date,
                            'filingDate': filing_date,
                            'transactionType': trans_code,
                            'shares': shares,
                            'pricePerShare': price,
                            'totalAmount': total_amount,
                            'exercisePrice': exercise_price,
                            'formType': f'form{form_number}' if form_number else 'form4',
                            'isDerivative': True,
                        }
                        trades.append(trade)
                elif is_form3 and len(cells) >= 6:
                    # Form 3 Table II: Derivative ownership (similar structure but for ownership)
                    def clean_cell(cell):
                        text = re.sub(r'<[^>]+>', '', cell)
                        text = unescape(text)
                        return text.strip()
                    
                    derivative_name = clean_cell(cells[0]) if len(cells) > 0 else None
                    underlying_title = clean_cell(cells[3]) if len(cells) > 3 else None
                    underlying_shares = clean_cell(cells[4]) if len(cells) > 4 else None
                    
                    if derivative_name:
                        # Form 3 derivatives represent ownership, not transactions
                        trade = {
                            'filerName': filer_name,
                            'issuerName': issuer_name,
                            'securitySymbol': ticker,
                            'securityName': f"{derivative_name} (underlying: {underlying_title})" if underlying_title else derivative_name,
                            'transactionDate': filing_date,
                            'filingDate': filing_date,
                            'transactionType': 'I',  # I = Initial Statement
                            'shares': None,  # Form 3 doesn't always show share amounts in same format
                            'pricePerShare': None,
                            'totalAmount': None,
                            'formType': 'form3',
                            'isDerivative': True,
                            'isInitialOwnership': True,
                        }
                        trades.append(trade)
        
        form_name = f"Form {form_number}" if form_number else "Form 4"
        if len(trades) == 0:
            logger.warning(f"⚠️ No trades/ownership records extracted from HTML {form_name} in {s3_key}")
            logger.debug(f"   Filer name extracted: {filer_name}")
            logger.debug(f"   Issuer: {issuer_name}, Ticker: {ticker}, Filing Date: {filing_date}")
        else:
            logger.info(f"✅ Extracted {len(trades)} trades/ownership records from HTML {form_name}")
            # Log first trade as example
            if trades:
                logger.debug(f"   Example trade: {trades[0]}")
        
    except Exception as e:
        logger.error(f"❌ Error parsing SEC form HTML {s3_key}: {e}")
        import traceback
        logger.error(f"   Traceback: {traceback.format_exc()}")
    
    return trades


def lambda_handler(event, context):
    """
    Lambda handler for matching trades from a single SEC form to politicians
    
    Expected input (from Step Functions Map state):
    {
        "s3Key": "trades/2024-01-15/sec/form4-1234567-2024-01-15.xml",
        "formType": "form4",
        "cik": "1234567",
        "filingDate": "2024-01-15",
        "source": "sec"
    }
    
    Returns:
    {
        "matchedTrades": [...],
        "unmatchedCount": 0,
        "s3Key": "...",
        "formType": "..."
    }
    """
    logger.info(f"🚀 Politician Trades SEC Matcher Lambda started: {json.dumps(event)}")
    
    if not S3_BUCKET:
        raise ValueError("S3_BUCKET environment variable not set")
    
    # Extract file info from event
    s3_key = event.get('s3Key') or event.get('s3_key')
    form_type = event.get('formType')
    filing_date = event.get('filingDate') or event.get('date', '')
    cik = event.get('cik')
    
    # Check if this is a failed download (from downloader Lambda error handling)
    if event.get('success') is False:
        logger.warning(f"⚠️ Skipping failed download: {event.get('error', 'Unknown error')}")
        return {
            "matchedTrades": [],
            "unmatchedCount": 0,
            "s3Key": None,
            "formType": form_type,
            "error": event.get('error', 'Download failed')
        }
    
    if not s3_key:
        logger.warning("⚠️ No s3Key provided in event")
        return {
            "matchedTrades": [],
            "unmatchedCount": 0,
            "s3Key": None,
            "formType": form_type
        }
    
    try:
        # Load politician list
        logger.info("📋 Loading congress-legislators list from S3")
        politicians = load_politician_list()
        if not politicians:
            raise ValueError("Failed to load congress-legislators.csv from S3")
        
        logger.info(f"✅ Loaded {len(politicians)} legislators")
        
        # Parse the SEC form file
        # Detect content type based on actual file content, not just extension
        trades = []
        
        # Download a sample to detect content type
        try:
            response = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
            content_sample = response['Body'].read(1000)  # Read first 1000 bytes
            content_sample_lower = content_sample.lower()
            
            # Check content type
            is_html = any(indicator in content_sample_lower for indicator in [
                b'<!doctype html',
                b'<html',
                b'<head>',
                b'<body>',
                b'<style',
            ])
            
            is_xml = (content_sample.startswith(b'<?xml') or 
                     b'<ownershipDocument' in content_sample or 
                     b'<document>' in content_sample)
            
            # Route to appropriate parser based on content
            if is_xml and not is_html:
                logger.info(f"📄 Detected XML content, parsing as XML")
                trades = parse_sec_form_xml(s3_key)
            elif is_html:
                logger.info(f"📄 Detected HTML content, parsing as HTML")
                trades = parse_sec_form_html(s3_key)
            elif s3_key.endswith('.pdf'):
                trades = parse_sec_form_pdf(s3_key)
            else:
                # Try XML parser first, fallback to HTML
                logger.warning(f"⚠️ Content type unclear for {s3_key}, trying XML parser first")
                try:
                    trades = parse_sec_form_xml(s3_key)
                    if not trades:
                        logger.info(f"📄 XML parser returned no trades, trying HTML parser")
                        trades = parse_sec_form_html(s3_key)
                except:
                    logger.info(f"📄 XML parser failed, trying HTML parser")
                    trades = parse_sec_form_html(s3_key)
        except Exception as e:
            logger.error(f"❌ Error detecting/parsing file type for {s3_key}: {e}")
            return {
                "matchedTrades": [],
                "unmatchedCount": 1,
                "s3Key": s3_key,
                "formType": form_type
            }
        
        # Match trades to politicians
        matched_trades = []
        unmatched_count = 0
        
        logger.info(f"📊 Processing {len(trades)} extracted trades/ownership records")
        
        for trade in trades:
            filer_name = trade.get('filerName')
            if not filer_name:
                continue
            
            matched_politician = find_matching_politician(filer_name, politicians)
            
            if matched_politician:
                logger.info(f"✅ Matched filer '{filer_name}' to politician: {matched_politician['name']}")
            else:
                logger.debug(f"❌ No politician match found for filer: {filer_name}")
            
            if matched_politician:
                # Map exact amounts from SEC forms to standard Senate PTR ranges
                # Standard Senate PTR ranges:
                # $0 - $1,000 (handles sub-$1k amounts, though minimum reporting threshold is $1,000)
                # $1,001 - $15,000
                # $15,001 - $50,000
                # $50,001 - $100,000
                # $100,001 - $250,000
                # $250,001 - $500,000
                # $500,001 - $1,000,000
                # $1,000,001 - $5,000,000
                # $5,000,001 - $25,000,000
                # $25,000,001 - $50,000,000
                # Over $50,000,000
                
                SENATE_PTR_RANGES = [
                    (0, 1000),  # $0 - $1,000 (handles sub-$1k amounts)
                    (1001, 15000),
                    (15001, 50000),
                    (50001, 100000),
                    (100001, 250000),
                    (250001, 500000),
                    (500001, 1000000),
                    (1000001, 5000000),
                    (5000001, 25000000),
                    (25000001, 50000000),
                    (50000001, None)  # Over $50,000,000 - max is None/unbounded
                ]
                
                def find_standard_range(amount_value: float) -> tuple:
                    """Find the standard Senate PTR range that contains the given amount"""
                    # Handle zero or negative amounts (use first range)
                    if amount_value <= 0:
                        return SENATE_PTR_RANGES[0]
                    
                    for range_min, range_max in SENATE_PTR_RANGES:
                        if range_max is None:
                            if amount_value >= range_min:
                                return (range_min, None)
                        else:
                            if range_min <= amount_value <= range_max:
                                return (range_min, range_max)
                    # Fallback: if amount is less than minimum, use first range
                    return SENATE_PTR_RANGES[0]
                
                # Get exact amount from SEC trade
                total_amount = trade.get('totalAmount') or trade.get('amount')
                exact_amount = None
                amount_range = None
                amount_min = None
                amount_max = None
                
                # If we have an exact amount, map it to a standard range
                if total_amount and isinstance(total_amount, (int, float)):
                    exact_amount = int(total_amount)  # Store exact amount
                    
                    # Find the standard range this exact amount falls into
                    standard_range = find_standard_range(float(total_amount))
                    
                    # Set amount_range to the standard range [min, max]
                    if standard_range[1] is None:
                        # For unbounded ranges (over $50M), use a large number
                        amount_range = [standard_range[0], 999999999]
                        amount_min = standard_range[0]
                        amount_max = 999999999
                    else:
                        amount_range = [standard_range[0], standard_range[1]]
                        amount_min = standard_range[0]
                        amount_max = standard_range[1]
                else:
                    # If amountMin/amountMax are already set (from range in SEC form), use those
                    amount_min = trade.get('amountMin')
                    amount_max = trade.get('amountMax')
                    if amount_min is not None and amount_max is not None:
                        amount_range = [int(amount_min), int(amount_max)]
                
                # Convert transactionDate to numeric format for GSI range key
                transaction_date_str = trade.get('transactionDate') or ''
                transaction_date_num = None
                if transaction_date_str:
                    try:
                        # If it's already in YYYY-MM-DD format, convert to YYYYMMDD integer
                        if len(transaction_date_str) == 10 and '-' in transaction_date_str:
                            transaction_date_num = int(transaction_date_str.replace('-', ''))
                        # If it's already numeric, use it
                        elif transaction_date_str.isdigit():
                            transaction_date_num = int(transaction_date_str)
                        # Otherwise try to parse and convert
                        else:
                            parsed_date = datetime.strptime(transaction_date_str, '%Y-%m-%d').date()
                            transaction_date_num = int(parsed_date.strftime('%Y%m%d'))
                    except:
                        # Fallback: use filing date as transaction date
                        try:
                            filing_date_obj = datetime.strptime(filing_date, '%Y-%m-%d').date()
                            transaction_date_num = int(filing_date_obj.strftime('%Y%m%d'))
                        except:
                            pass
                else:
                    # Use filing date if transaction date is missing
                    try:
                        filing_date_obj = datetime.strptime(filing_date, '%Y-%m-%d').date()
                        transaction_date_num = int(filing_date_obj.strftime('%Y%m%d'))
                    except:
                        pass
                
                matched_trade = {
                    'tradeId': f"trade_{filing_date}_{cik or 'unknown'}_{len(matched_trades)}",
                    'politicianName': matched_politician['name'],  # GSI: PoliticianTradeDateIndex
                    'party': matched_politician['party'],  # GSI: PartyTradeDateIndex
                    'position': matched_politician['position'],  # GSI: PositionTradeDateIndex
                    'websiteUrl': matched_politician.get('websiteUrl'),  # Regular attribute (not GSI)
                    'formType': form_type,  # GSI: FormTypeTradeDateIndex
                    'filingDate': filing_date,
                    'transactionDate': transaction_date_num,  # GSI range key (numeric: YYYYMMDD format)
                    'transactionTime': trade.get('transactionTime'),
                    'securitySymbol': trade.get('securitySymbol'),  # GSI: SecurityTradeDateIndex
                    'securityName': trade.get('securityName'),
                    'transactionType': trade.get('transactionType'),  # GSI: TransactionTypeTradeDateIndex
                    'shares': trade.get('shares'),
                    'pricePerShare': trade.get('pricePerShare'),
                    'totalAmount': total_amount,  # Keep exact amount for backwards compatibility
                    'amountMin': amount_min,  # Min of the standard range (for GSI: AmountRangeTradeDateIndex)
                    'amountMax': amount_max,  # Max of the standard range
                    'amountRange': amount_range,  # List of two integers [min, max] for standard Senate PTR range
                    'exactAmount': exact_amount,  # Exact dollar amount if provided (not a GSI)
                    'formCIK': cik,
                    'formS3Key': s3_key,
                    'matchConfidence': matched_politician.get('matchScore', 1.0),
                    'source': 'sec'
                }
                matched_trades.append(matched_trade)
            else:
                unmatched_count += 1
        
        logger.info(f"✅ Matched {len(matched_trades)} trades from {s3_key}")
        
        return {
            "matchedTrades": matched_trades,
            "unmatchedCount": unmatched_count,
            "s3Key": s3_key,
            "formType": form_type,
            "source": "sec"
        }
        
    except Exception as e:
        logger.error(f"❌ Error in SEC matcher Lambda: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return {
            "matchedTrades": [],
            "unmatchedCount": 1,
            "s3Key": s3_key,
            "formType": form_type,
            "error": str(e)
        }

