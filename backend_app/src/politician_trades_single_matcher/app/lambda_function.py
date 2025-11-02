"""
Lambda function to match trades from a single SEC form or PTR file to politicians.
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
            primary_name = ' '.join(name_parts) if name_parts else row.get('full_name', '').strip()
            
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
    """
    # Normalize names (lowercase, strip)
    filer_normalized = filer_name.lower().strip()
    politician_normalized = politician['name'].lower().strip()
    
    # Check exact match first
    if filer_normalized == politician_normalized:
        return 1.0
    
    # Check alternative names
    for alt_name in politician.get('alternativeNames', []):
        if filer_normalized == alt_name.lower().strip():
            return 1.0
    
    # Calculate similarity using SequenceMatcher
    similarity = SequenceMatcher(None, filer_normalized, politician_normalized).ratio()
    
    # Also check if names are subsets (e.g., "John Doe" vs "John A. Doe")
    if filer_normalized in politician_normalized or politician_normalized in filer_normalized:
        similarity = max(similarity, 0.9)
    
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
    
    # Try to fix mismatched tags (basic fix - if a tag is self-closing, ensure it ends with />)
    # This is a basic fix - full XML repair would require a proper XML repair library
    
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


def parse_ptr_with_textract(pdf_content: bytes, source: str = 'house') -> List[Dict[str, Any]]:
    """
    Parse PTR PDF using AWS Textract to extract trade data
    
    Args:
        pdf_content: PDF file content as bytes
        source: 'house' or 'senate' - determines parsing strategy
        
    Returns:
        List of trade dicts with filerName, securitySymbol, shares, pricePerShare, transactionDate, etc.
    """
    trades = []
    
    try:
        import boto3
        textract_client = boto3.client('textract')
        
        logger.info(f"📄 Using Textract to parse {source.upper()} PTR PDF...")
        
        # Call Textract to extract text and forms/tables
        # Use analyze_document with FORMS and TABLES for structured data
        response = textract_client.analyze_document(
            Document={'Bytes': pdf_content},
            FeatureTypes=['FORMS', 'TABLES']
        )
        
        # Extract text and structured data
        text_lines = []
        form_fields = {}  # Key-value pairs from forms
        tables = []  # Table data
        
        for block in response.get('Blocks', []):
            block_type = block.get('BlockType')
            
            if block_type == 'LINE':
                text = block.get('Text', '').strip()
                if text:
                    text_lines.append(text)
            
            elif block_type == 'KEY_VALUE_SET':
                # Extract form field key-value pairs
                entity_type = block.get('EntityTypes', [])
                if 'KEY' in entity_type:
                    key_text = ''
                    # Get the key text from child relationships
                    for relationship in block.get('Relationships', []):
                        if relationship.get('Type') == 'CHILD':
                            for child_id in relationship.get('Ids', []):
                                # Find the child block and get its text
                                for child_block in response.get('Blocks', []):
                                    if child_block.get('Id') == child_id and child_block.get('BlockType') == 'WORD':
                                        key_text += child_block.get('Text', '') + ' '
                    key_text = key_text.strip()
                    
                    # Find the corresponding value
                    value_text = ''
                    for relationship in block.get('Relationships', []):
                        if relationship.get('Type') == 'VALUE':
                            for value_id in relationship.get('Ids', []):
                                for value_block in response.get('Blocks', []):
                                    if value_block.get('Id') == value_id:
                                        if value_block.get('BlockType') == 'WORD':
                                            value_text += value_block.get('Text', '') + ' '
                                        elif value_block.get('BlockType') == 'SELECTION_ELEMENT':
                                            # Checkbox selected
                                            if value_block.get('SelectionStatus') == 'SELECTED':
                                                value_text = 'Yes'
                    value_text = value_text.strip()
                    
                    if key_text and value_text:
                        form_fields[key_text.lower()] = value_text
        
        # Extract tables
        table_blocks = [b for b in response.get('Blocks', []) if b.get('BlockType') == 'TABLE']
        for table_block in table_blocks:
            table_data = extract_table_data(table_block, response.get('Blocks', []))
            if table_data:
                tables.append(table_data)
        
        full_text = ' '.join(text_lines)
        
        # Extract filer name
        filer_name = None
        
        if source == 'senate':
            # Senate format: "The Honorable Rick Scott (Scott, Rick)"
            senate_patterns = [
                r'The Honorable\s+([A-Z][a-z]+(?:\s+[A-Z][a-z.]+)+)\s+\([^)]+\)',  # "The Honorable First Last (Last, First)"
                r'The Honorable\s+([A-Z][a-z]+(?:\s+[A-Z][a-z.]+)+)',  # "The Honorable First Last" (fallback)
            ]
            for pattern in senate_patterns:
                match = re.search(pattern, full_text)
                if match:
                    filer_name = match.group(1).strip()
                    # Clean up common titles
                    filer_name = re.sub(r'\b(Honorable|Hon\.?|Senator|Sen\.?|Representative|Rep\.?)\b', '', filer_name, flags=re.IGNORECASE).strip()
                    break
        else:
            # House format (for future implementation)
            house_patterns = [
                r'(?:Representative|Rep\.?|Name)[\s:]*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)',
                r'([A-Z][a-z]+\s+[A-Z]\.?\s+[A-Z][a-z]+)',  # First M. Last
            ]
            for pattern in house_patterns:
                match = re.search(pattern, full_text)
                if match:
                    filer_name = match.group(1).strip()
                    break
        
        # If not found in text, check form fields
        if not filer_name:
            for key, value in form_fields.items():
                if 'name' in key or 'filer' in key:
                    filer_name = value
                    break
        
        if not filer_name:
            logger.warning(f"⚠️ Could not extract filer name from {source} PTR")
        
        # Extract filing date
        filing_date = None
        
        if source == 'senate':
            # Senate format: "Filed 10/30/2025 @ 5 PM" or "Periodic Transaction Report for 10/30/2025"
            senate_date_patterns = [
                r'Filed\s+(\d{1,2}/\d{1,2}/\d{4})',
                r'Periodic Transaction Report for\s+(\d{1,2}/\d{1,2}/\d{4})',
            ]
            for pattern in senate_date_patterns:
                match = re.search(pattern, full_text)
                if match:
                    date_str = match.group(1)
                    try:
                        filing_date = datetime.strptime(date_str, '%m/%d/%Y').strftime('%Y-%m-%d')
                        break
                    except ValueError:
                        continue
        else:
            # House format (for future implementation)
            date_patterns = [
                r'(?:Date Filed|Filing Date|Report Date)[\s:]*(\d{1,2}[/-]\d{1,2}[/-]\d{4})',
                r'(\d{1,2}[/-]\d{1,2}[/-]\d{4})',
            ]
            for pattern in date_patterns:
                match = re.search(pattern, full_text)
                if match:
                    date_str = match.group(1)
                    try:
                        # Try to parse date
                        for fmt in ['%m/%d/%Y', '%m-%d-%Y']:
                            try:
                                filing_date = datetime.strptime(date_str, fmt).strftime('%Y-%m-%d')
                                break
                            except ValueError:
                                continue
                        if filing_date:
                            break
                    except:
                        continue
        
        # Parse tables for trade data
        # PTR tables typically have columns: Transaction Date, Asset Description, Transaction Type, Amount, etc.
        for table in tables:
            # Look for trade transactions in table rows
            if len(table) < 2:  # Need at least header + data row
                continue
            
            # Find header row (first row with column names)
            header_row = table[0]
            
            # Map column indices based on Senate PTR structure
            # Senate PTR columns: #, Transaction Date, Owner, Ticker, Asset Name, Asset Type, Type, Amount, Comment
            date_col = None
            owner_col = None  # Self, Joint, Spouse, Dependent Child
            asset_col = None
            asset_type_col = None  # Stock, Bond, Municipal Security, etc.
            symbol_col = None
            type_col = None  # Purchase, Sale, etc.
            amount_col = None
            shares_col = None
            
            for idx, cell in enumerate(header_row):
                cell_lower = cell.lower().strip()
                if 'transaction date' in cell_lower or ('date' in cell_lower and 'transaction' in cell_lower):
                    date_col = idx
                elif 'owner' in cell_lower:
                    owner_col = idx
                elif 'asset name' in cell_lower or ('asset' in cell_lower and 'name' in cell_lower):
                    asset_col = idx
                elif 'asset type' in cell_lower or ('asset' in cell_lower and 'type' in cell_lower):
                    asset_type_col = idx
                elif 'ticker' in cell_lower or 'symbol' in cell_lower:
                    symbol_col = idx
                elif ('type' in cell_lower and 'asset' not in cell_lower) or 'transaction type' in cell_lower:
                    type_col = idx
                elif 'amount' in cell_lower or 'value' in cell_lower:
                    amount_col = idx
                elif 'shares' in cell_lower or 'quantity' in cell_lower:
                    shares_col = idx
            
            # Parse data rows
            for row in table[1:]:
                # Check if we have minimum required columns
                min_cols = max(filter(None, [date_col, asset_col, type_col, amount_col]))
                if min_cols is None or len(row) < min_cols + 1:
                    continue
                
                # Extract transaction date
                transaction_date = None
                if date_col is not None and date_col < len(row):
                    date_str = row[date_col].strip()
                    # Senate format: MM/DD/YYYY
                    for fmt in ['%m/%d/%Y', '%m-%d-%Y', '%Y-%m-%d', '%m/%d/%y']:
                        try:
                            transaction_date = datetime.strptime(date_str, fmt).strftime('%Y-%m-%d')
                            break
                        except ValueError:
                            continue
                
                # Extract owner (Self, Joint, Spouse, Dependent Child)
                owner = None
                if owner_col is not None and owner_col < len(row):
                    owner = row[owner_col].strip()
                
                # Extract asset name and symbol
                security_name = None
                security_symbol = None
                
                if asset_col is not None and asset_col < len(row):
                    asset_text = row[asset_col].strip()
                    security_name = asset_text
                    # Try to extract ticker symbol if present (usually uppercase letters, 1-5 chars)
                    symbol_match = re.search(r'\b([A-Z]{1,5})\b', asset_text)
                    if symbol_match:
                        security_symbol = symbol_match.group(1)
                
                if symbol_col is not None and symbol_col < len(row):
                    symbol_text = row[symbol_col].strip()
                    if symbol_text and symbol_text != '--':
                        security_symbol = symbol_text
                
                # Extract asset type (Stock, Bond, Municipal Security, etc.)
                asset_type = None
                if asset_type_col is not None and asset_type_col < len(row):
                    asset_type = row[asset_type_col].strip()
                
                # Extract transaction type
                transaction_type = None
                if type_col is not None and type_col < len(row):
                    trans_text = row[type_col].strip().upper()
                    if 'PURCHASE' in trans_text or 'BUY' in trans_text:
                        transaction_type = 'P'  # Purchase
                    elif 'SALE' in trans_text or 'SELL' in trans_text:
                        transaction_type = 'S'  # Sale
                    else:
                        transaction_type = trans_text[:1] if trans_text else 'U'  # First letter or Unknown
                
                # Extract amount (could be range like "$100,001 - $250,000")
                shares = None
                price_per_share = None
                total_amount = None
                amount_min = None
                amount_max = None
                
                if shares_col is not None and shares_col < len(row):
                    shares_str = row[shares_col].strip()
                    # Remove commas, parse number
                    try:
                        shares = int(re.sub(r'[^\d]', '', shares_str))
                    except:
                        pass
                
                if amount_col is not None and amount_col < len(row):
                    amount_str = row[amount_col].strip()
                    
                    # Handle range format: "$100,001 - $250,000" or "$1,000 - $15,000"
                    range_match = re.search(r'\$([\d,]+)\s*-\s*\$([\d,]+)', amount_str)
                    if range_match:
                        try:
                            amount_min = float(re.sub(r'[^\d.]', '', range_match.group(1)))
                            amount_max = float(re.sub(r'[^\d.]', '', range_match.group(2)))
                            # Use midpoint of range as estimate
                            total_amount = (amount_min + amount_max) / 2
                        except:
                            pass
                    else:
                        # Single amount format
                        try:
                            total_amount = float(re.sub(r'[^\d.]', '', amount_str))
                        except:
                            pass
                    
                    # Calculate price per share if we have shares and total amount
                    if shares and shares > 0 and total_amount:
                        price_per_share = total_amount / shares
                
                # Only create trade if we have minimum required data
                if transaction_date or security_name or security_symbol or total_amount:
                    trade = {
                        'filerName': filer_name,
                        'securityName': security_name,
                        'securitySymbol': security_symbol,
                        'assetType': asset_type,  # Stock, Bond, Municipal Security, etc.
                        'owner': owner,  # Self, Joint, Spouse, Dependent Child
                        'transactionDate': transaction_date or filing_date,
                        'filingDate': filing_date,
                        'transactionType': transaction_type or 'U',  # U = Unknown
                        'shares': shares,
                        'pricePerShare': price_per_share,
                        'totalAmount': total_amount,
                        'amountMin': amount_min,  # For range amounts
                        'amountMax': amount_max,  # For range amounts
                        'formType': f'{source}_ptr',
                        'source': source
                    }
                    trades.append(trade)
        
        logger.info(f"✅ Extracted {len(trades)} trades from {source.upper()} PTR using Textract")
        
    except Exception as e:
        logger.error(f"❌ Error parsing PTR with Textract: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
    
    return trades


def extract_table_data(table_block: Dict[str, Any], all_blocks: List[Dict[str, Any]]) -> Optional[List[List[str]]]:
    """
    Extract table data from Textract table block
    
    Args:
        table_block: Textract table block
        all_blocks: All blocks from Textract response
        
    Returns:
        List of rows, each row is a list of cell values
    """
    try:
        # Build block lookup
        block_map = {block.get('Id'): block for block in all_blocks}
        
        # Get cells from table relationships
        rows = {}
        cols = {}
        
        for relationship in table_block.get('Relationships', []):
            if relationship.get('Type') == 'CHILD':
                for cell_id in relationship.get('Ids', []):
                    cell_block = block_map.get(cell_id)
                    if cell_block:
                        row_index = cell_block.get('RowIndex', 0)
                        col_index = cell_block.get('ColumnIndex', 0)
                        
                        # Get cell text from child words
                        cell_text = ''
                        for cell_rel in cell_block.get('Relationships', []):
                            if cell_rel.get('Type') == 'CHILD':
                                for word_id in cell_rel.get('Ids', []):
                                    word_block = block_map.get(word_id)
                                    if word_block and word_block.get('BlockType') == 'WORD':
                                        cell_text += word_block.get('Text', '') + ' '
                        cell_text = cell_text.strip()
                        
                        if row_index not in rows:
                            rows[row_index] = {}
                        rows[row_index][col_index] = cell_text
        
        # Convert to list of lists
        if not rows:
            return None
        
        table_data = []
        for row_idx in sorted(rows.keys()):
            row_data = []
            for col_idx in sorted(rows[row_idx].keys()):
                row_data.append(rows[row_idx][col_idx])
            table_data.append(row_data)
        
        return table_data
        
    except Exception as e:
        logger.error(f"❌ Error extracting table data: {e}")
        return None


def parse_house_ptr(s3_key: str) -> List[Dict[str, Any]]:
    """
    Parse House PTR PDF using Textract and extract trade data
    """
    trades = []
    
    try:
        logger.info(f"📄 Parsing House PTR: {s3_key}")
        
        # Download PDF from S3
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
        pdf_content = response['Body'].read()
        
        # Use Textract to parse
        trades = parse_ptr_with_textract(pdf_content, source='house')
        
    except Exception as e:
        logger.error(f"❌ Error parsing House PTR {s3_key}: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
    
    return trades


def parse_senate_ptr_html(html_content: str) -> List[Dict[str, Any]]:
    """
    Parse Senate PTR HTML page with transaction table
    Extracts transaction data directly from HTML table (no Textract needed)
    """
    trades = []
    
    try:
        # Find the transactions table
        # Table structure: <table class="table table-striped"> with transaction rows
        
        # Extract filer name from the page
        filer_name = None
        name_patterns = [
            r'<h[1-6][^>]*>([^<]+(?:Senator|Representative)[^<]*)</h[1-6]>',
            r'class=["\']filer[^"\']*["\'][^>]*>([^<]+)</',
            r'<td[^>]*>([^<]+\([^)]+\)[^<]*)</td>',  # "Name (Last, First)" format
        ]
        for pattern in name_patterns:
            match = re.search(pattern, html_content, re.IGNORECASE)
            if match:
                filer_name = match.group(1).strip()
                break
        
        # Find the transactions table
        # Look for table with headers: #, Transaction Date, Owner, Ticker, Asset Name, Asset Type, Type, Amount, Comment
        table_match = re.search(
            r'<table[^>]*class=["\']table[^"\']*["\'][^>]*>.*?<tbody>(.*?)</tbody>',
            html_content,
            re.IGNORECASE | re.DOTALL
        )
        
        if not table_match:
            logger.warning("⚠️ Could not find transactions table in HTML")
            return trades
        
        tbody_content = table_match.group(1)
        
        # Extract all table rows (<tr>...</tr>)
        row_pattern = r'<tr[^>]*>(.*?)</tr>'
        rows = re.finditer(row_pattern, tbody_content, re.IGNORECASE | re.DOTALL)
        
        for row_num, row_match in enumerate(rows, start=1):
            row_html = row_match.group(1)
            
            # Extract cells from row
            cell_pattern = r'<td[^>]*>(.*?)</td>'
            cells = re.findall(cell_pattern, row_html, re.IGNORECASE | re.DOTALL)
            
            if len(cells) < 8:  # Need at least 8 columns
                continue
            
            # Column mapping:
            # 0: # (row number)
            # 1: Transaction Date
            # 2: Owner
            # 3: Ticker
            # 4: Asset Name
            # 5: Asset Type
            # 6: Type
            # 7: Amount
            # 8: Comment
            
            # Clean HTML from cells
            def clean_html(text):
                # Remove HTML tags
                text = re.sub(r'<[^>]+>', '', text)
                # Decode HTML entities
                text = unescape(text)
                # Strip whitespace
                text = text.strip()
                # Remove extra whitespace
                text = re.sub(r'\s+', ' ', text)
                return text
            
            try:
                transaction_date_str = clean_html(cells[1]) if len(cells) > 1 else ''
                owner = clean_html(cells[2]) if len(cells) > 2 else ''
                ticker = clean_html(cells[3]) if len(cells) > 3 else ''
                asset_name = clean_html(cells[4]) if len(cells) > 4 else ''
                asset_type = clean_html(cells[5]) if len(cells) > 5 else ''
                transaction_type = clean_html(cells[6]) if len(cells) > 6 else ''
                amount_str = clean_html(cells[7]) if len(cells) > 7 else ''
                
                # Parse transaction date
                transaction_date = None
                if transaction_date_str:
                    try:
                        transaction_date = datetime.strptime(transaction_date_str, '%m/%d/%Y').date()
                    except:
                        pass
                
                # Parse amount (handles ranges like "$100,001 - $250,000")
                amount_min = None
                amount_max = None
                total_amount = None
                
                if amount_str and amount_str not in ['--', '']:
                    # Remove $ and commas
                    amount_clean = amount_str.replace('$', '').replace(',', '').strip()
                    
                    # Check for range (e.g., "100001 - 250000")
                    if ' - ' in amount_clean or '-' in amount_clean:
                        parts = re.split(r'\s*-\s*', amount_clean)
                        if len(parts) == 2:
                            try:
                                amount_min = float(parts[0].strip())
                                amount_max = float(parts[1].strip())
                                total_amount = (amount_min + amount_max) / 2
                            except:
                                pass
                    else:
                        # Single amount
                        try:
                            total_amount = float(amount_clean)
                            amount_min = total_amount
                            amount_max = total_amount
                        except:
                            pass
                
                # Map transaction type
                transaction_code = None
                if transaction_type.lower() in ['purchase', 'buy', 'acquired']:
                    transaction_code = 'P'
                elif transaction_type.lower() in ['sale', 'sell', 'disposed']:
                    transaction_code = 'S'
                else:
                    transaction_code = transaction_type[:1].upper() if transaction_type else 'P'
                
                # Skip if ticker is missing or "--"
                if not ticker or ticker.strip() in ['--', '']:
                    ticker = None
                else:
                    ticker = ticker.strip()
                
                trade = {
                    'transactionDate': transaction_date.strftime('%Y-%m-%d') if transaction_date else '',
                    'transaction_code': transaction_code,
                    'owner': owner,
                    'securitySymbol': ticker,
                    'securityName': asset_name,
                    'assetType': asset_type,
                    'transactionType': transaction_type,
                    'amount': total_amount,
                    'amountMin': amount_min,
                    'amountMax': amount_max,
                    'shares': None,  # Not provided in Senate PTR HTML
                    'comment': clean_html(cells[8]) if len(cells) > 8 else ''
                }
                
                trades.append(trade)
                logger.info(f"✅ Extracted trade: {transaction_type} {asset_name} ({ticker or 'N/A'}) on {transaction_date_str}")
                
            except Exception as e:
                logger.warning(f"⚠️ Error parsing row {row_num}: {e}")
                continue
        
        logger.info(f"✅ Parsed {len(trades)} trades from Senate PTR HTML")
        
    except Exception as e:
        logger.error(f"❌ Error parsing Senate PTR HTML: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
    
    return trades


def parse_senate_ptr(s3_key: str) -> List[Dict[str, Any]]:
    """
    Parse Senate PTR - handles both PDF and HTML formats
    """
    trades = []
    
    try:
        logger.info(f"📄 Parsing Senate PTR: {s3_key}")
        
        # Download file from S3
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
        file_content = response['Body'].read()
        
        # Check if it's HTML or PDF
        content_start = file_content[:100].lower()
        
        if b'<html' in content_start or b'<!doctype' in content_start:
            # It's HTML - parse the transaction table directly
            logger.info(f"📄 Senate PTR is HTML format - parsing table directly")
            html_content = file_content.decode('utf-8', errors='ignore')
            trades = parse_senate_ptr_html(html_content)
        else:
            # It's PDF - use Textract
            logger.info(f"📄 Senate PTR is PDF format - using Textract")
            trades = parse_ptr_with_textract(file_content, source='senate')
        
    except Exception as e:
        logger.error(f"❌ Error parsing Senate PTR {s3_key}: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
    
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
    Lambda handler for matching trades from a single file to politicians
    
    Expected input (from Step Functions Map state):
    {
        "s3Key": "trades/2024-01-15/sec/form4-1234567-2024-01-15.xml",
        "formType": "form4",
        "cik": "1234567",
        "filingDate": "2024-01-15",
        "source": "sec"  // or "house" or "senate"
    }
    
    Returns:
    {
        "matchedTrades": [...],
        "unmatchedCount": 0,
        "s3Key": "...",
        "formType": "..."
    }
    """
    logger.info(f"🚀 Politician Trades Single Matcher Lambda started: {json.dumps(event)}")
    
    if not S3_BUCKET:
        raise ValueError("S3_BUCKET environment variable not set")
    
    # Extract file info from event
    # Support both SEC form format and PTR format
    s3_key = event.get('s3Key') or event.get('s3_key')
    form_type = event.get('formType') or event.get('form_type')
    filing_date = event.get('filingDate') or event.get('filing_date') or event.get('date', '')
    
    # Determine source from form type or explicit source field
    # PTRs come with form_type='house_ptr' or 'senate_ptr'
    if form_type and ('house' in form_type.lower() or 'house_ptr' in form_type.lower()):
        source = 'house'
    elif form_type and ('senate' in form_type.lower() or 'senate_ptr' in form_type.lower()):
        source = 'senate'
    else:
        source = event.get('source', 'sec')  # sec, house, or senate
    
    # For PTRs, s3_key might be directly in the event (from fetcher)
    if not s3_key and source in ['house', 'senate']:
        s3_key = event.get('s3_key')
    
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
        
        # Parse the form/PTR file
        # Detect content type based on actual file content, not just extension
        trades = []
        if source == 'sec':
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
        elif source == 'house':
            trades = parse_house_ptr(s3_key)
        elif source == 'senate':
            # Check if transactions are already extracted at fetcher level
            if event.get('transactions'):
                logger.info(f"✅ Using {len(event.get('transactions', []))} pre-extracted transactions from fetcher")
                trades = event.get('transactions', [])
            else:
                # Fallback: parse from S3 file
                logger.info(f"📄 No pre-extracted transactions found, parsing from S3 file")
                trades = parse_senate_ptr(s3_key)
        else:
            logger.warning(f"⚠️ Unknown source type: {source}")
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
        if len(trades) == 0:
            logger.warning(f"⚠️ No trades/ownership records extracted from {s3_key}. File may contain no transactions or parsing failed.")
        
        for trade in trades:
            if source == 'sec':
                filer_name = trade.get('filerName')
                if not filer_name:
                    continue
                
                matched_politician = find_matching_politician(filer_name, politicians)
                
                if matched_politician:
                    logger.info(f"✅ Matched filer '{filer_name}' to politician: {matched_politician['name']}")
                else:
                    logger.debug(f"❌ No politician match found for filer: {filer_name}")
                
                if matched_politician:
                    matched_trade = {
                        'tradeId': f"trade_{filing_date}_{cik or 'unknown'}_{len(matched_trades)}",
                        'politicianName': matched_politician['name'],
                        'party': matched_politician['party'],
                        'position': matched_politician['position'],
                        'websiteUrl': matched_politician.get('websiteUrl'),
                        'formType': form_type,
                        'filingDate': filing_date,
                        'transactionDate': trade.get('transactionDate'),
                        'transactionTime': trade.get('transactionTime'),
                        'securitySymbol': trade.get('securitySymbol'),
                        'securityName': trade.get('securityName'),
                        'transactionType': trade.get('transactionType'),
                        'shares': trade.get('shares'),
                        'pricePerShare': trade.get('pricePerShare'),
                        'totalAmount': trade.get('totalAmount'),
                        'formCIK': cik,
                        'formS3Key': s3_key,
                        'matchConfidence': matched_politician.get('matchScore', 1.0),
                        'source': 'sec'
                    }
                    matched_trades.append(matched_trade)
                else:
                    unmatched_count += 1
            else:  # house or senate
                # For Senate PTRs, use filerName from trade (pre-extracted)
                # For House PTRs, may have politicianName or filerName
                filer_name = trade.get('filerName') or trade.get('politicianName')
                if not filer_name:
                    unmatched_count += 1
                    continue
                
                matched_politician = find_matching_politician(filer_name, politicians)
                if matched_politician:
                    # Map Senate PTR transaction fields to standard format
                    # Senate PTRs use: securityName, assetType, order, amount
                    # Standard format uses: securityName, transactionType, totalAmount
                    transaction_type = trade.get('order') or trade.get('transactionType')
                    
                    matched_trade = {
                        'tradeId': f"trade_{filing_date}_{source}_{len(matched_trades)}",
                        'politicianName': matched_politician['name'],
                        'party': matched_politician['party'],
                        'position': matched_politician['position'],
                        'websiteUrl': matched_politician.get('websiteUrl'),
                        'formType': form_type or f'{source}_ptr',
                        'filingDate': filing_date,
                        'transactionDate': trade.get('transactionDate'),
                        'transactionTime': trade.get('transactionTime'),
                        'securitySymbol': trade.get('ticker') or trade.get('securitySymbol'),
                        'securityName': trade.get('securityName'),
                        'assetType': trade.get('assetType'),  # Include assetType for Senate PTRs
                        'transactionType': transaction_type,  # "Purchase", "Sale", etc.
                        'order': trade.get('order'),  # Keep original "order" field
                        'shares': trade.get('shares'),
                        'pricePerShare': trade.get('pricePerShare'),
                        'totalAmount': trade.get('amount') or trade.get('totalAmount'),
                        'amountMin': trade.get('amountMin'),
                        'amountMax': trade.get('amountMax'),
                        'owner': trade.get('owner'),
                        'comment': trade.get('comment'),
                        'formS3Key': s3_key,
                        'matchConfidence': matched_politician.get('matchScore', 1.0),
                        'source': source
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
            "source": source
        }
        
    except Exception as e:
        logger.error(f"❌ Error in single matcher Lambda: {e}")
        return {
            "matchedTrades": [],
            "unmatchedCount": 1,
            "s3Key": s3_key,
            "formType": form_type,
            "error": str(e)
        }

