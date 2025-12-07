"""
Lambda function to match trades from a single Senate PTR file to politicians.
This function is invoked in parallel via Step Functions Map state.
"""

import json
import os
import logging
import re
import boto3
import math
from typing import List, Dict, Any, Optional
from datetime import datetime
from decimal import Decimal
import csv
from io import StringIO
from html import unescape
from difflib import SequenceMatcher

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS clients
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

# Environment variables
S3_BUCKET = os.environ.get('S3_BUCKET')
DYNAMODB_TABLE_NAME = os.environ.get('DYNAMODB_TABLE_NAME')

# Name matching threshold (0.0 to 1.0)
NAME_MATCH_THRESHOLD = 0.85  # 85% similarity

# Constant for unparsed documents - use a large number that can be searched as "N/A"
# This value represents unreadable/unparsed documents and allows "N/A" searches to map to it
UNPARSED_AMOUNT_VALUE = 999999999999  # 999.999 billion - high enough to be clearly distinguishable

# Standard Senate PTR ranges (as tuples of (min, max))
# Note: Minimum reporting threshold is $1,000, but we handle sub-$1k amounts
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

def format_state_district(politician: Dict[str, Any]) -> Optional[str]:
    """
    Format state/district for a politician
    - Senators: Just state (e.g., "IL", "WA")
    - House reps: State + district (e.g., "IL02", "TX31")
    
    Args:
        politician: Politician dict with state and district fields
        
    Returns:
        Formatted state/district string or None
    """
    state = (politician.get('state') or '').strip()
    if not state:
        return None
    
    position = (politician.get('position') or '').strip()
    district = (politician.get('district') or '').strip()
    
    # Senators don't have districts
    if position == 'Senate':
        return state
    
    # House reps have districts
    if position == 'House' and district:
        # Format district with zero-padding if needed (e.g., "2" -> "02", "31" -> "31")
        try:
            district_num = int(district)
            return f"{state}{district_num:02d}"
        except (ValueError, TypeError):
            # If district is not a number, just append it
            return f"{state}{district}"
    
    # Fallback: just return state if we can't format properly
    return state

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


def is_valid_filer_name(filer_name: str) -> bool:
    """
    Filter out obviously invalid filer names (OCR noise, garbage text, table headers)
    
    Args:
        filer_name: Filer name to validate
    
    Returns:
        True if name appears valid, False otherwise
    """
    if not filer_name or len(filer_name.strip()) < 3:
        return False
    
    filer_name_lower = filer_name.strip().lower()
    
    # Filter out common table headers and form labels (not actual names)
    invalid_words = [
        'report', 'type', 'date', 'owner', 'ticker', 'asset', 'security',
        'transaction', 'amount', 'comment', 'filer', 'name', 'senator',
        'representative', 'the honorable', 'periodic', 'filing', 'form'
    ]
    
    # Check if the entire name is just one of these invalid words
    if filer_name_lower in invalid_words:
        return False
    
    # Check if it starts with these words (likely table headers)
    for invalid_word in invalid_words:
        if filer_name_lower.startswith(invalid_word + ' ') or filer_name_lower == invalid_word:
            return False
    
    # Filter out common OCR noise patterns
    invalid_patterns = [
        r'^[Yy]r\s+[Dd]ay',  # "Yr Day" - common OCR error
        r'^[A-Z]{1,2}\s+[A-Z]{1,2}$',  # Single letters like "A B"
        r'^\d+$',  # Pure numbers
        r'^[^\w\s]+$',  # Only special characters
    ]
    
    for pattern in invalid_patterns:
        if re.match(pattern, filer_name.strip()):
            return False
    
    # Must contain at least one letter
    if not re.search(r'[A-Za-z]', filer_name):
        return False
    
    return True


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


def is_valid_trade(trade: Dict[str, Any]) -> bool:
    """
    Validate if a trade has meaningful data (not just noise from Textract)
    
    Args:
        trade: Trade dictionary to validate
    
    Returns:
        True if trade has meaningful data, False if it's likely noise/invalid
    """
    # Check if this is explicitly marked as unparsed - those are valid placeholder trades
    if trade.get('isUnparsed', False) or trade.get('requiresManualReview', False):
        return True
    
    # Validate filer name if present (filter out table headers like "Report", "Type", etc.)
    filer_name = trade.get('filerName') or trade.get('politicianName')
    if filer_name:
        # Check if filer name is valid (not OCR noise, not table headers)
        if not is_valid_filer_name(filer_name):
            logger.debug(f"   Invalid filer name in trade: '{filer_name}' - likely table header/noise")
            return False
    
    # Validate transactionType - it should NOT be an amount (contains "$" or looks like a range)
    transaction_type = trade.get('transactionType') or trade.get('order')
    if transaction_type:
        trans_str = str(transaction_type).strip()
        # If transactionType contains "$" or looks like an amount range, it's invalid
        if '$' in trans_str or re.search(r'\d+.*[-–—].*\d+', trans_str):
            logger.debug(f"   Invalid transactionType in trade: '{trans_str}' - looks like an amount, not a transaction type")
            return False
    
    # A valid trade should have at least one of:
    # - securityName (not empty, not just whitespace, not "--")
    # - securitySymbol (not empty, not "--")
    # - transactionType (not empty, valid) AND amountRange or amountMin/amountMax (not None)
    
    has_security_name = trade.get('securityName') and trade.get('securityName').strip() and trade.get('securityName') != '--'
    has_security_symbol = trade.get('securitySymbol') and trade.get('securitySymbol').strip() and trade.get('securitySymbol') != '--'
    has_transaction_type = transaction_type and str(transaction_type).strip() and '$' not in str(transaction_type)
    has_amount = trade.get('amountRange') is not None or trade.get('amountMin') is not None or trade.get('amountMax') is not None
    
    # Need at least security info OR (transaction type AND amount) to be valid
    # This prevents trades with just "Purchase" or just "$1000" from being considered valid
    if has_security_name or has_security_symbol or (has_transaction_type and has_amount):
        return True
    
    # If it has none of these, it's likely noise/invalid
    return False


def extract_image_urls_from_html(html_content: str, flexible: bool = False) -> List[str]:
    """
    Extract all image URLs from HTML content
    Looks for <img> tags with class="filingImage" or similar patterns
    
    Args:
        html_content: HTML content as string
        flexible: If True, use more flexible patterns to find any image
    
    Returns:
        List of image URLs found (empty list if none found)
    """
    image_urls = []
    try:
        # Pattern 1: Look for filingImage class (most common for Senate PTRs) - get ALL matches
        pattern1 = r'<img[^>]*class=["\']filingImage["\'][^>]*src=["\']([^"\']+)["\']'
        matches = re.findall(pattern1, html_content, re.IGNORECASE)
        if matches:
            image_urls.extend(matches)
            logger.info(f"✅ Extracted {len(matches)} image URL(s) using filingImage pattern")
            return image_urls
        
        # Pattern 2: Look for any img tag with src containing efd-media-public.senate.gov
        if flexible:
            pattern2 = r'<img[^>]*src=["\']([^"\']*efd-media-public\.senate\.gov[^"\']+)["\']'
            matches = re.findall(pattern2, html_content, re.IGNORECASE)
            if matches:
                image_urls.extend(matches)
                logger.info(f"✅ Extracted {len(matches)} image URL(s) using efd-media-public pattern")
                return image_urls
            
            # Pattern 3: Any img tag with src (most flexible)
            pattern3 = r'<img[^>]*src=["\']([^"\']+\.(?:gif|jpg|jpeg|png|webp))["\']'
            matches = re.findall(pattern3, html_content, re.IGNORECASE)
            if matches:
                image_urls.extend(matches)
                logger.info(f"✅ Extracted {len(matches)} image URL(s) using flexible pattern")
                return image_urls
        
        return image_urls
    except Exception as e:
        logger.error(f"❌ Error extracting image URLs from HTML: {e}")
        return []


def parse_senate_ptr_from_image(image_url: str, s3_key: str, filer_name: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Download image from URL and process with Textract to extract transaction data
    Handles amendments and regular filings
    
    Args:
        image_url: URL of the image to download
        s3_key: S3 key of the original HTML file (for context)
        filer_name: Optional filer name from event (from fetcher)
    
    Returns:
        List of extracted trades
    """
    trades = []
    
    try:
        from urllib.request import urlopen, Request
        from urllib.parse import urlparse, urljoin
        
        logger.info(f"📥 Downloading image from: {image_url}")
        
        # Handle relative URLs - convert to absolute if needed
        if image_url.startswith('//'):
            image_url = 'https:' + image_url
        elif image_url.startswith('/'):
            # Extract domain from s3_key context or use default
            image_url = 'https://efdsearch.senate.gov' + image_url
        
        # Download image using urllib (standard library)
        req = Request(image_url, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        with urlopen(req, timeout=30) as response:
            image_content = response.read()
            logger.info(f"✅ Downloaded image ({len(image_content)} bytes)")
            
            # Check image format
            content_type = response.headers.get('Content-Type', '').lower()
            logger.info(f"   Content-Type: {content_type}")
        
        # Textract only supports PNG, JPEG, PDF, TIFF - convert GIF and other formats
        # Check file extension and content type
        is_gif = (content_type == 'image/gif' or 
                 image_url.lower().endswith('.gif') or
                 image_content[:6] == b'GIF89a' or 
                 image_content[:6] == b'GIF87a')
        
        if is_gif:
            logger.info(f"🔄 Converting GIF to PNG (Textract doesn't support GIF format)...")
            try:
                from PIL import Image
                from io import BytesIO
                
                # Open GIF image
                img = Image.open(BytesIO(image_content))
                
                # Convert to RGB if needed (GIFs may have palette mode)
                if img.mode in ('RGBA', 'LA', 'P'):
                    # Convert palette/transparency to RGB
                    rgb_img = Image.new('RGB', img.size, (255, 255, 255))
                    if img.mode == 'P':
                        img = img.convert('RGBA')
                    rgb_img.paste(img, mask=img.split()[-1] if img.mode == 'RGBA' else None)
                    img = rgb_img
                elif img.mode != 'RGB':
                    img = img.convert('RGB')
                
                # Save as PNG to bytes
                png_buffer = BytesIO()
                img.save(png_buffer, format='PNG')
                image_content = png_buffer.getvalue()
                logger.info(f"✅ Converted GIF to PNG ({len(image_content)} bytes)")
                
            except ImportError:
                logger.error(f"❌ PIL/Pillow not available - cannot convert GIF to PNG")
                logger.error(f"   Please add 'Pillow' to requirements.txt")
                raise Exception("GIF format not supported by Textract and PIL not available for conversion")
            except Exception as e:
                logger.error(f"❌ Error converting GIF to PNG: {e}")
                raise Exception(f"Failed to convert GIF to PNG: {e}")
        
        # Use Textract to extract text from image
        logger.info(f"🔍 Using Textract to extract text from image...")
        textract_client = boto3.client('textract')
        
        # Textract supports: PNG, JPEG, PDF, TIFF
        try:
            response_textract = textract_client.analyze_document(
                Document={'Bytes': image_content},
                FeatureTypes=['FORMS', 'TABLES']
            )
        except Exception as e:
            # Check if it's an unsupported format error
            error_str = str(e).lower()
            if 'unsupported' in error_str or 'format' in error_str:
                logger.error(f"❌ Textract doesn't support this image format")
                logger.error(f"   Content-Type: {content_type}")
                logger.error(f"   Supported formats: PNG, JPEG, PDF, TIFF")
                logger.error(f"   Consider using LLM-based extraction for this format")
                raise Exception(f"Unsupported image format for Textract: {content_type}")
            else:
                raise
        
        # Extract text and tables from Textract response
        logger.info(f"✅ Textract analysis complete - extracting structured data...")
        
        # Parse Textract response to extract transactions
        # Pass filer_name if available from event
        trades = parse_textract_response_for_senate_ptr(response_textract, s3_key, filer_name=filer_name)
        
        logger.info(f"✅ Extracted {len(trades)} trades from image using Textract")
        
    except Exception as e:
        logger.error(f"❌ Error parsing Senate PTR from image {image_url}: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
    
    return trades


def parse_textract_response_for_senate_ptr(textract_response: Dict[str, Any], s3_key: str, filer_name: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Parse Textract response to extract Senate PTR transaction data
    Handles both regular filings and amendments
    
    Args:
        textract_response: Textract AnalyzeDocument response
        s3_key: S3 key for context
        filer_name: Optional filer name from event (from fetcher)
    
    Returns:
        List of extracted trades
    """
    trades = []
    
    try:
        blocks = textract_response.get('Blocks', [])
        
        # Extract all text
        all_text = []
        for block in blocks:
            if block.get('BlockType') == 'LINE':
                text = block.get('Text', '').strip()
                if text:
                    all_text.append(text)
        
        full_text = '\n'.join(all_text)
        logger.info(f"📋 Extracted text from image ({len(full_text)} characters)")
        logger.debug(f"   Text preview: {full_text[:500]}...")
        
        # Use filer_name from event if provided, otherwise extract from text
        if not filer_name:
            # Extract filer name from text (look for senator name patterns)
            # Pattern 1: Look for "Richard Blumenthal" or "Blumenthal, Richard" format
            name_patterns = [
                r'(?:Senator|The Honorable)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z.]+)+)',  # "Senator Richard Blumenthal"
                r'([A-Z][a-z]+,\s+[A-Z][a-z]+(?:\s+[A-Z][a-z.]+)?)',  # "Blumenthal, Richard"
                r'([A-Z][a-z]+(?:\s+[A-Z][a-z.]+)+)\s*\(',  # "Richard Blumenthal (Senator)"
            ]
            for pattern in name_patterns:
                match = re.search(pattern, full_text)
                if match:
                    extracted_name = match.group(1).strip()
                    # Convert "Last, First" to "First Last" format
                    if ',' in extracted_name:
                        parts = [p.strip() for p in extracted_name.split(',', 1)]
                        if len(parts) == 2:
                            extracted_name = f"{parts[1]} {parts[0]}".strip()
                    
                    # Validate extracted name (filter OCR noise)
                    if is_valid_filer_name(extracted_name):
                        filer_name = extracted_name
                        logger.info(f"✅ Extracted valid filer name from text: {filer_name}")
                        break
                    else:
                        logger.warning(f"⚠️ Extracted name failed validation (likely OCR noise): '{extracted_name}', continuing search...")
        
        if filer_name and is_valid_filer_name(filer_name):
            logger.info(f"✅ Using filer name: {filer_name}")
        else:
            logger.warning(f"⚠️ Could not extract valid filer name from document or event")
            filer_name = None  # Clear invalid filer name
        
        # Detect if this is an amendment
        is_amendment = 'amendment' in full_text.lower() or 'amend' in full_text.lower()
        if is_amendment:
            logger.info(f"📝 Detected amendment document")
            # Extract amendment details
            # Look for patterns like "amendment to a Periodic Transaction Report filed on [date]"
            amendment_date_match = re.search(
                r'(?:filed|filing)\s+on\s+(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})',
                full_text,
                re.IGNORECASE
            )
            if amendment_date_match:
                original_filing_date = amendment_date_match.group(1)
                logger.info(f"   Original filing date: {original_filing_date}")
        
        # Extract transaction table from Textract tables
        tables = [b for b in blocks if b.get('BlockType') == 'TABLE']
        logger.info(f"📊 Found {len(tables)} table(s) in document")
        
        if tables:
            # Process each table
            for table_idx, table_block in enumerate(tables, 1):
                logger.info(f"   Processing table {table_idx}...")
                table_data = extract_table_data(table_block, blocks)
                
                if table_data:
                    # Try to parse table as transaction data
                    table_trades = parse_table_as_senate_transactions(table_data, full_text, s3_key, filer_name=filer_name)
                    trades.extend(table_trades)
                    logger.info(f"   ✅ Extracted {len(table_trades)} trade(s) from table {table_idx}")
                else:
                    logger.warning(f"   ⚠️ Could not extract table data from table {table_idx}")
        else:
            # No structured tables - try to extract from text patterns
            logger.info(f"   No structured tables found - attempting pattern-based extraction...")
            text_trades = extract_transactions_from_text(full_text, s3_key)
            # Add filer name to text trades
            for trade in text_trades:
                if filer_name and not trade.get('filerName'):
                    trade['filerName'] = filer_name
            trades.extend(text_trades)
        
    except Exception as e:
        logger.error(f"❌ Error parsing Textract response: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
    
    return trades


def parse_table_as_senate_transactions(table_data: List[List[str]], full_text: str, s3_key: str, filer_name: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Parse table data as Senate PTR transactions
    Handles standard transaction table format
    
    Args:
        table_data: Extracted table data (list of rows)
        full_text: Full text from document (for context)
        s3_key: S3 key for context
        filer_name: Optional filer name (extracted from document or event)
    """
    trades = []
    
    try:
        if not table_data or len(table_data) < 2:
            logger.warning(f"⚠️ Table data insufficient for parsing")
            return trades
        
        # First row is typically headers
        headers = [h.strip().lower() if h else '' for h in table_data[0]]
        logger.info(f"📋 Table headers: {headers}")
        
        # Find column indices
        col_indices = {}
        for i, header in enumerate(headers):
            if 'owner' in header:
                col_indices['owner'] = i
            elif 'asset' in header or 'security' in header or 'ticker' in header:
                col_indices['asset'] = i
            elif 'transaction' in header or 'type' in header:
                col_indices['transaction_type'] = i
            elif 'date' in header:
                col_indices['date'] = i
            elif 'amount' in header:
                col_indices['amount'] = i
        
        # Process data rows
        for row_idx, row in enumerate(table_data[1:], 1):
            if len(row) < len(headers):
                logger.warning(f"   ⚠️ Row {row_idx} has {len(row)} cells, expected {len(headers)}")
                continue
            
            try:
                trade = {}
                
                # Add filer name if provided
                if filer_name:
                    trade['filerName'] = filer_name
                
                # Extract fields
                if 'owner' in col_indices:
                    owner_val = row[col_indices['owner']]
                    trade['owner'] = owner_val.strip() if owner_val else None
                if 'asset' in col_indices:
                    asset_val = row[col_indices['asset']]
                    asset = asset_val.strip() if asset_val else ''
                    trade['securityName'] = asset
                    # Try to extract ticker if present
                    if asset:
                        ticker_match = re.search(r'\(([A-Z]{1,5})\)', asset)
                        if ticker_match:
                            trade['securitySymbol'] = ticker_match.group(1)
                if 'transaction_type' in col_indices:
                    trans_val = row[col_indices['transaction_type']]
                    trans_type = trans_val.strip() if trans_val else None
                    trade['transactionType'] = trans_type
                if 'date' in col_indices:
                    date_val = row[col_indices['date']]
                    date_str = date_val.strip() if date_val else None
                    trade['transactionDate'] = date_str
                if 'amount' in col_indices:
                    amount_val = row[col_indices['amount']]
                    amount_str = amount_val.strip() if amount_val else None
                    # Parse amount range
                    amount_range = parse_amount_range(amount_str)
                    if amount_range:
                        trade['amountMin'] = amount_range[0]
                        trade['amountMax'] = amount_range[1]
                        trade['amountRange'] = amount_range
                
                if trade:
                    trades.append(trade)
                    logger.debug(f"   Extracted trade {row_idx}: {trade.get('securityName', 'N/A')} - {trade.get('transactionType', 'N/A')} on {trade.get('transactionDate', 'N/A')}")
            
            except Exception as e:
                logger.warning(f"⚠️ Error parsing row {row_idx}: {e}")
                import traceback
                logger.warning(f"   Traceback: {traceback.format_exc()}")
                continue
        
    except Exception as e:
        logger.error(f"❌ Error parsing table as transactions: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
    
    return trades


def parse_amount_range(amount_str: str) -> Optional[List[int]]:
    """
    Parse amount string like "$1,001-$15,000" or "$1,001 - $15,000" into [min, max]
    
    Args:
        amount_str: Amount string from document
    
    Returns:
        [min, max] as list of integers, or None if parsing fails
    """
    if not amount_str:
        return None
    try:
        # Remove currency symbols and whitespace
        amount_str = re.sub(r'[\$,\s]', '', amount_str)
        
        # Pattern: number-number or number - number
        range_match = re.search(r'(\d+)\s*[-–—]\s*(\d+)', amount_str)
        if range_match:
            min_val = int(range_match.group(1))
            max_val = int(range_match.group(2))
            return [min_val, max_val]
        
        # Pattern: single number (use as both min and max)
        single_match = re.search(r'(\d+)', amount_str)
        if single_match:
            val = int(single_match.group(1))
            return [val, val]
        
        return None
    except Exception as e:
        logger.debug(f"⚠️ Error parsing amount range '{amount_str}': {e}")
        return None


def extract_transactions_from_text(text: str, s3_key: str) -> List[Dict[str, Any]]:
    """
    Extract transaction data from unstructured text using patterns
    Fallback when no structured tables are found
    
    Note: For better results on complex documents, consider using Bedrock for semantic understanding
    """
    trades = []
    
    # This is a simplified pattern-based extraction
    # For better results, consider using Bedrock for semantic understanding
    logger.info(f"📋 Attempting pattern-based extraction from text...")
    
    # Look for transaction patterns
    # Example: "Sale" followed by date and amount
    # This is a basic implementation - can be enhanced
    
    # Pattern: Look for transaction type, date, and amount
    # Example: "Sale 8/14/25 $1,001-$15,000"
    transaction_pattern = re.compile(
        r'(?:Sale|Purchase|Exchange|Conversion)\s+(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})\s+([^\n]+?)(?:\s+(\$?[\d,]+(?:\s*[-–—]\s*\$?[\d,]+)?))?',
        re.IGNORECASE
    )
    
    matches = transaction_pattern.finditer(text)
    for match in matches:
        try:
            trans_type = match.group(0).split()[0].title()  # "Sale" or "Purchase"
            date_str = match.group(1)
            details = match.group(2).strip() if match.group(2) else ''
            amount_str = match.group(3) if match.group(3) else ''
            
            trade = {
                'transactionType': trans_type,
                'transactionDate': date_str,
            }
            
            # Extract security name/ticker from details
            if details:
                trade['securityName'] = details
                ticker_match = re.search(r'\(([A-Z]{1,5})\)', details)
                if ticker_match:
                    trade['securitySymbol'] = ticker_match.group(1)
            
            # Parse amount
            if amount_str:
                amount_range = parse_amount_range(amount_str)
                if amount_range:
                    trade['amountMin'] = amount_range[0]
                    trade['amountMax'] = amount_range[1]
                    trade['amountRange'] = amount_range
            
            if trade:
                trades.append(trade)
                logger.debug(f"   Extracted trade from text: {trade}")
        
        except Exception as e:
            logger.debug(f"   ⚠️ Error parsing text match: {e}")
            continue
    
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


def parse_ptr_with_textract(pdf_content: bytes, source: str = 'senate') -> List[Dict[str, Any]]:
    """
    Parse PTR PDF using AWS Textract to extract trade data (Senate-specific)
    
    Args:
        pdf_content: PDF file content as bytes
        source: 'senate' - determines parsing strategy
        
    Returns:
        List of trade dicts with filerName, securitySymbol, shares, pricePerShare, transactionDate, etc.
    """
    trades = []
    
    try:
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
        
        # Extract filer name (Senate-specific)
        filer_name = None
        
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
        
        # If not found in text, check form fields
        if not filer_name:
            for key, value in form_fields.items():
                if 'name' in key or 'filer' in key:
                    filer_name = value
                    break
        
        if not filer_name:
            logger.warning(f"⚠️ Could not extract filer name from {source} PTR")
        
        # Extract filing date (Senate-specific)
        filing_date = None
        
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
                cell_lower = cell.lower().strip() if cell else ''
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
                    date_val = row[date_col]
                    date_str = date_val.strip() if date_val else None
                    if date_str:
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
                    owner_val = row[owner_col]
                    owner = owner_val.strip() if owner_val else None
                
                # Extract asset name and symbol
                security_name = None
                security_symbol = None
                
                if asset_col is not None and asset_col < len(row):
                    asset_val = row[asset_col]
                    asset_text = asset_val.strip() if asset_val else None
                    if asset_text:
                        security_name = asset_text
                        # Try to extract ticker symbol if present (usually uppercase letters, 1-5 chars)
                        symbol_match = re.search(r'\b([A-Z]{1,5})\b', asset_text)
                        if symbol_match:
                            security_symbol = symbol_match.group(1)
                
                if symbol_col is not None and symbol_col < len(row):
                    symbol_val = row[symbol_col]
                    symbol_text = symbol_val.strip() if symbol_val else None
                    if symbol_text and symbol_text != '--':
                        security_symbol = symbol_text
                
                # Extract asset type (Stock, Bond, Municipal Security, etc.)
                asset_type = None
                if asset_type_col is not None and asset_type_col < len(row):
                    asset_type_val = row[asset_type_col]
                    asset_type = asset_type_val.strip() if asset_type_val else None
                
                # Extract transaction type
                transaction_type = None
                if type_col is not None and type_col < len(row):
                    trans_val = row[type_col]
                    trans_text = trans_val.strip() if trans_val else None
                    if trans_text:
                        trans_text_upper = trans_text.upper()
                        # If the text looks like an amount (contains "$" or digits with dashes), skip it
                        if '$' in trans_text or re.search(r'\d+.*[-–—].*\d+', trans_text):
                            logger.debug(f"   Skipping invalid transaction type (looks like amount): '{trans_text}'")
                            transaction_type = None
                        elif 'PURCHASE' in trans_text_upper or 'BUY' in trans_text_upper:
                            transaction_type = 'Purchase'
                        elif 'SALE' in trans_text_upper or 'SELL' in trans_text_upper:
                            transaction_type = 'Sale'
                        elif len(trans_text) < 50:  # Only use if it's a reasonable transaction type
                            transaction_type = trans_text
                        else:
                            transaction_type = None
                
                # Extract amount (could be range like "$100,001 - $250,000")
                shares = None
                price_per_share = None
                amount_min = None
                amount_max = None
                
                if shares_col is not None and shares_col < len(row):
                    shares_val = row[shares_col]
                    shares_str = shares_val.strip() if shares_val else None
                    # Remove commas, parse number
                    try:
                        shares = int(re.sub(r'[^\d]', '', shares_str))
                    except:
                        pass
                
                if amount_col is not None and amount_col < len(row):
                    amount_val = row[amount_col]
                    amount_str = amount_val.strip() if amount_val else None
                    
                    # Handle range format: "$100,001 - $250,000" or "$1,000 - $15,000"
                    if amount_str:
                        range_match = re.search(r'\$([\d,]+)\s*-\s*\$([\d,]+)', amount_str)
                        if range_match:
                            try:
                                amount_min = float(re.sub(r'[^\d.]', '', range_match.group(1)))
                                amount_max = float(re.sub(r'[^\d.]', '', range_match.group(2)))
                            except:
                                pass
                    
                    # Calculate price per share if we have shares and amount range
                    if shares and shares > 0 and amount_min and amount_max:
                        # Use midpoint of range for price per share calculation
                        midpoint = (amount_min + amount_max) / 2
                        price_per_share = midpoint / shares
                
                # Only create trade if we have minimum required data
                # Don't create trade if transactionType is invalid (looks like amount)
                if (transaction_date or security_name or security_symbol or amount_min) and transaction_type is not None:
                    # Map amount range to standard Senate PTR ranges if we have amounts
                    amount_range = None
                    if amount_min is not None and amount_max is not None:
                        # Use midpoint to find standard range
                        midpoint = (amount_min + amount_max) / 2
                        amount_range = find_standard_range(midpoint)
                        if amount_range:
                            amount_min = amount_range[0]
                            amount_max = amount_range[1]
                    
                    trade = {
                        'filerName': filer_name,
                        'securityName': security_name,
                        'securitySymbol': security_symbol,
                        'assetType': asset_type,  # Stock, Bond, Municipal Security, etc.
                        'owner': owner,  # Self, Joint, Spouse, Dependent Child
                        'transactionDate': transaction_date or filing_date,
                        'filingDate': filing_date,
                        'transactionType': transaction_type,  # Don't use 'U' - only valid transaction types
                        'order': transaction_type,  # Map to order field for compatibility
                        'shares': shares,
                        'pricePerShare': price_per_share,
                        'amountMin': amount_min,  # For range amounts
                        'amountMax': amount_max,  # For range amounts
                        'amountRange': amount_range if amount_range else ([amount_min, amount_max] if amount_min is not None and amount_max is not None else None),
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
        # Look for <h2 class="filedReport">The Honorable Rick Scott (Scott, Rick)</h2>
        filer_name = None
        name_patterns = [
            r'<h2[^>]*class=["\']filedReport["\'][^>]*>(?:The\s+Honorable\s+)?([^<(]+)(?:\s*\([^)]+\))?</h2>',  # "The Honorable Rick Scott (Scott, Rick)"
            r'<h2[^>]*class=["\']filedReport["\'][^>]*>([^<]+)</h2>',  # Fallback for h2 with filedReport class
            r'<h[1-6][^>]*>([^<]+(?:Senator|Representative)[^<]*)</h[1-6]>',
            r'class=["\']filer[^"\']*["\'][^>]*>([^<]+)</',
            r'<td[^>]*>([^<]+\([^)]+\)[^<]*)</td>',  # "Name (Last, First)" format
        ]
        for pattern in name_patterns:
            match = re.search(pattern, html_content, re.IGNORECASE)
            if match:
                name_text = match.group(1).strip()
                # Clean up "The Honorable" prefix and extract just the name
                name_text = re.sub(r'^The\s+Honorable\s+', '', name_text, flags=re.IGNORECASE).strip()
                # If we have parentheses, prefer the format inside (e.g., "Scott, Rick")
                # Otherwise use the full name
                paren_match = re.search(r'\(([^)]+)\)', name_text)
                if paren_match:
                    # Extract name from parentheses and convert "Last, First" to "First Last"
                    paren_name = paren_match.group(1).strip()
                    if ',' in paren_name:
                        parts = [p.strip() for p in paren_name.split(',', 1)]
                        if len(parts) == 2:
                            filer_name = f"{parts[1]} {parts[0]}".strip()  # "First Last"
                        else:
                            filer_name = paren_name
                    else:
                        filer_name = paren_name
                else:
                    filer_name = name_text
                if filer_name:
                    logger.info(f"✅ Extracted filer name: {filer_name}")
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
        
        # Clean HTML helper function (defined once before loop)
        def clean_html(text):
            if not text:
                return ''
            # Remove HTML tags
            text = re.sub(r'<[^>]+>', '', text)
            # Decode HTML entities
            text = unescape(text)
            # Strip whitespace
            text = text.strip()
            # Remove extra whitespace
            text = re.sub(r'\s+', ' ', text)
            return text
        
        # Extract metadata from asset name cell (e.g., Rate/Coupon, Matures)
        def extract_asset_metadata(cell_html):
            """
            Extract metadata from the asset name cell.
            Looks for <div class="text-muted"> with <em> tags containing key-value pairs.
            Returns a dictionary with extracted metadata.
            """
            metadata = {}
            if not cell_html:
                return metadata
            
            # Find the text-muted div
            text_muted_match = re.search(r'<div[^>]*class="text-muted"[^>]*>(.*?)</div>', cell_html, re.IGNORECASE | re.DOTALL)
            if not text_muted_match:
                return metadata
            
            text_muted_content = text_muted_match.group(1)
            
            # Extract key-value pairs from <em>Key:</em> Value format
            # Pattern: <em>Key:</em> Value<br> or <em>Key:</em> Value</div> or <em>Key:</em> Value (end of string)
            # Examples: <em>Rate/Coupon:</em> 5%<br> <em>Matures:</em> 12/01/2030
            # Handle both cases: with <br> separator and without (just whitespace)
            pattern = r'<em>([^<]+):</em>\s*([^<]+?)(?=<em>|</div>|$|<br)'
            matches = re.finditer(pattern, text_muted_content, re.IGNORECASE | re.DOTALL)
            
            for match in matches:
                key = clean_html(match.group(1)).strip().rstrip(':')
                value = clean_html(match.group(2)).strip()
                if key and value:
                    # Normalize key (remove extra spaces, make consistent)
                    key_normalized = re.sub(r'\s+', ' ', key).strip()
                    metadata[key_normalized] = value
            
            return metadata
        
        for row_num, row_match in enumerate(rows, start=1):
            row_html = row_match.group(1)
            
            # Extract cells from row - handle malformed HTML with duplicate/nested cells
            # Use a more robust pattern that handles closing tags properly
            cell_pattern = r'<td[^>]*>(.*?)</td>'
            cells = re.findall(cell_pattern, row_html, re.IGNORECASE | re.DOTALL)
            
            if len(cells) < 7:  # Need at least 7 data columns
                logger.debug(f"⚠️ Row {row_num} has only {len(cells)} cells, skipping")
                continue
            
            # Column mapping:
            # 0: # (row number)
            # 1: Transaction Date
            # 2: Owner
            # 3: Ticker
            # 4: Asset Name (may have nested div with Rate/Coupon, Matures info)
            # 5: Asset Type
            # 6: Type (Purchase/Sale)
            # 7: Amount
            # 8: Comment (optional)
            
            try:
                # Try to find Amount column by looking for "$" - helps with malformed HTML
                amount_index = None
                for i, cell in enumerate(cells):
                    cleaned = clean_html(cell)
                    if '$' in cleaned and ('-' in cleaned or re.search(r'\d', cleaned)):
                        amount_index = i
                        break
                
                # Extract columns - use smart mapping if we found amount, otherwise sequential
                asset_name_cell_html = None  # Store raw HTML for metadata extraction
                if amount_index is not None and amount_index >= 6:
                    # Smart mapping: work backwards from Amount column
                    transaction_date_str = clean_html(cells[amount_index - 6] if amount_index >= 6 and amount_index - 6 < len(cells) and cells[amount_index - 6] else None) or ''
                    owner = clean_html(cells[amount_index - 5] if amount_index >= 5 and amount_index - 5 < len(cells) and cells[amount_index - 5] else None) or ''
                    ticker = clean_html(cells[amount_index - 4] if amount_index >= 4 and amount_index - 4 < len(cells) and cells[amount_index - 4] else None) or ''
                    asset_name_cell_html = cells[amount_index - 3] if amount_index >= 3 and amount_index - 3 < len(cells) else None
                    # Extract metadata from asset name cell BEFORE cleaning (so we can remove it from asset name)
                    asset_metadata = extract_asset_metadata(asset_name_cell_html) if asset_name_cell_html else {}
                    # Remove the metadata div from asset name before cleaning
                    if asset_name_cell_html:
                        # Remove the <div class="text-muted"> section from the HTML before cleaning
                        asset_name_html_clean = re.sub(r'<div[^>]*class="text-muted"[^>]*>.*?</div>', '', asset_name_cell_html, flags=re.IGNORECASE | re.DOTALL)
                        asset_name = clean_html(asset_name_html_clean) if asset_name_html_clean else ''
                    else:
                        asset_name = ''
                    asset_type = clean_html(cells[amount_index - 2] if amount_index >= 2 and amount_index - 2 < len(cells) and cells[amount_index - 2] else None) or ''
                    transaction_type = clean_html(cells[amount_index - 1] if amount_index >= 1 and amount_index - 1 < len(cells) and cells[amount_index - 1] else None) or ''
                    amount_str = clean_html(cells[amount_index] if amount_index < len(cells) and cells[amount_index] else None) or ''
                    comment = clean_html(cells[amount_index + 1] if amount_index + 1 < len(cells) and cells[amount_index + 1] else None) or ''
                else:
                    # Sequential mapping (standard case)
                    # Skip first cell if it's just a row number
                    start_idx = 1 if (len(cells) > 0 and cells[0] and re.match(r'^\s*\d+\s*$', clean_html(cells[0]))) else 0
                    transaction_date_str = clean_html(cells[start_idx + 0] if len(cells) > start_idx + 0 and cells[start_idx + 0] else None) or ''
                    owner = clean_html(cells[start_idx + 1] if len(cells) > start_idx + 1 and cells[start_idx + 1] else None) or ''
                    ticker = clean_html(cells[start_idx + 2] if len(cells) > start_idx + 2 and cells[start_idx + 2] else None) or ''
                    asset_name_cell_html = cells[start_idx + 3] if len(cells) > start_idx + 3 else None
                    # Extract metadata from asset name cell BEFORE cleaning (so we can remove it from asset name)
                    asset_metadata = extract_asset_metadata(asset_name_cell_html) if asset_name_cell_html else {}
                    # Remove the metadata div from asset name before cleaning
                    if asset_name_cell_html:
                        # Remove the <div class="text-muted"> section from the HTML before cleaning
                        asset_name_html_clean = re.sub(r'<div[^>]*class="text-muted"[^>]*>.*?</div>', '', asset_name_cell_html, flags=re.IGNORECASE | re.DOTALL)
                        asset_name = clean_html(asset_name_html_clean) if asset_name_html_clean else ''
                    else:
                        asset_name = ''
                    asset_type = clean_html(cells[start_idx + 4] if len(cells) > start_idx + 4 and cells[start_idx + 4] else None) or ''
                    transaction_type = clean_html(cells[start_idx + 5] if len(cells) > start_idx + 5 and cells[start_idx + 5] else None) or ''
                    amount_str = clean_html(cells[start_idx + 6] if len(cells) > start_idx + 6 and cells[start_idx + 6] else None) or ''
                    comment = clean_html(cells[start_idx + 7] if len(cells) > start_idx + 7 and cells[start_idx + 7] else None) or ''
                
                # Clean asset name - remove extra whitespace from nested HTML
                asset_name = ' '.join(asset_name.split()) if asset_name else ''
                
                # Validate that we got essential fields
                if not transaction_type or not amount_str or amount_str in ['--', '']:
                    logger.warning(f"⚠️ Row {row_num} missing essential fields (type: {transaction_type}, amount: {amount_str}), skipping")
                    continue
                
                # Parse transaction date
                transaction_date = None
                if transaction_date_str:
                    try:
                        transaction_date = datetime.strptime(transaction_date_str, '%m/%d/%Y').date()
                    except:
                        pass
                
                # Parse amount (handles ranges like "$100,001 - $250,000")
                # Senate PTR uses fixed standard ranges for reporting
                # Use the module-level find_standard_range function
                
                amount_min = None
                amount_max = None
                exact_amount = None  # For exact amounts (not a GSI)
                amount_range = None  # List of two integers [min, max] for the standard range
                
                if amount_str and amount_str not in ['--', '']:
                    # Remove $ and commas
                    amount_clean = amount_str.replace('$', '').replace(',', '').strip()
                    
                    # Check for range (e.g., "100001 - 250000")
                    if ' - ' in amount_clean or '-' in amount_clean:
                        parts = re.split(r'\s*-\s*', amount_clean)
                        if len(parts) == 2:
                            try:
                                raw_min = float(parts[0].strip())
                                raw_max = float(parts[1].strip())
                                
                                # Most trades are in ranges - use the provided range as-is
                                # amount_range is a list of two integers from the fixed Senate PTR ranges
                                amount_min = int(raw_min)
                                amount_max = int(raw_max)
                                amount_range = [amount_min, amount_max]  # Use provided range
                            except:
                                pass
                    else:
                        # Single exact amount - map to standard range AND store exact amount
                        try:
                            exact_value = float(amount_clean)
                            exact_amount = int(exact_value)  # Store exact amount (not a GSI)
                            
                            # Find the standard range this exact amount falls into
                            standard_range = find_standard_range(exact_value)
                            
                            # Set amount_range to the standard range [min, max]
                            # For unbounded ranges, use a large number for max
                            if standard_range[1] is None:
                                amount_range = [standard_range[0], 999999999]  # Use large number instead of None
                            else:
                                amount_range = [standard_range[0], standard_range[1]]
                            
                            # Also set amountMin/amountMax to the exact value for backwards compatibility
                            amount_min = int(exact_value)
                            amount_max = int(exact_value)
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
                    'amountMin': amount_min,
                    'amountMax': amount_max,
                    'amountRange': amount_range,  # List of two integers [min, max] for standard Senate PTR range
                    'exactAmount': exact_amount,  # Exact dollar amount if provided (not a GSI)
                    'shares': None,  # Not provided in Senate PTR HTML
                    'comment': comment,
                    'filerName': filer_name,  # Include filer name from HTML for matching
                    'metadata': asset_metadata if asset_metadata else None  # Flexible JSON blob for asset metadata
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


def parse_senate_ptr(s3_key: str, event: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    Parse Senate PTR - handles:
    1. HTML with transaction table (direct parsing)
    2. HTML with embedded image (Textract)
    3. PDF format (Textract)
    
    Args:
        s3_key: S3 key of the file to parse
        event: Optional event dict that may contain filer_name from fetcher
    """
    trades = []
    
    try:
        logger.info(f"📄 Parsing Senate PTR: {s3_key}")
        
        # Get filer_name from event if available (from fetcher)
        filer_name_from_event = None
        if event:
            filer_name_from_event = event.get('filer_name') or event.get('filerName')
            if filer_name_from_event:
                logger.info(f"✅ Using filer_name from event: {filer_name_from_event}")
        
        # Download file from S3
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
        file_content = response['Body'].read()
        
        # Check if it's HTML or PDF
        content_start = file_content[:100].lower()
        
        if b'<html' in content_start or b'<!doctype' in content_start:
            # It's HTML - check if it contains an image or transaction table
            logger.info(f"📄 Senate PTR is HTML format")
            html_content = file_content.decode('utf-8', errors='ignore')
            
            # Check for embedded images (image-based filings like amendments)
            # Multi-page documents can have multiple images
            image_urls = extract_image_urls_from_html(html_content)
            
            if image_urls:
                logger.info(f"🖼️ HTML contains {len(image_urls)} embedded image(s) - will use Textract to process")
                for idx, image_url in enumerate(image_urls, 1):
                    logger.info(f"   Image {idx}/{len(image_urls)}: {image_url}")
                    image_trades = parse_senate_ptr_from_image(image_url, s3_key, filer_name=filer_name_from_event)
                    trades.extend(image_trades)
                
                logger.info(f"✅ Processed {len(image_urls)} image(s), extracted {len(trades)} trade(s) total")
            else:
                # Try to parse transaction table directly
                logger.info(f"📋 HTML appears to contain transaction table - parsing directly")
                trades = parse_senate_ptr_html(html_content)
                
                # If no trades found, log more details for debugging
                if not trades:
                    logger.warning(f"⚠️ No trades extracted from HTML table parsing")
                    logger.info(f"📋 Checking HTML structure for debugging...")
                    # Check for common indicators
                    has_table = 'table' in html_content.lower() and 'table-striped' in html_content.lower()
                    has_image_tag = '<img' in html_content.lower()
                    logger.info(f"   Has table-striped: {has_table}")
                    logger.info(f"   Has image tag: {has_image_tag}")
                    if has_image_tag and not has_table:
                        logger.info(f"   💡 HTML contains image but no transaction table - this may be an image-based filing")
                        logger.info(f"   💡 Attempting to extract image URLs for Textract processing...")
                        # Try to extract image URLs with more flexible patterns
                        image_urls = extract_image_urls_from_html(html_content, flexible=True)
                        if image_urls:
                            logger.info(f"   ✅ Found {len(image_urls)} image URL(s)")
                            for idx, image_url in enumerate(image_urls, 1):
                                logger.info(f"   Processing image {idx}/{len(image_urls)}: {image_url}")
                                image_trades = parse_senate_ptr_from_image(image_url, s3_key, filer_name=filer_name_from_event)
                                trades.extend(image_trades)
        else:
            # It's PDF - use Textract
            logger.info(f"📄 Senate PTR is PDF format - using Textract")
            trades = parse_ptr_with_textract(file_content, source='senate')
        
    except Exception as e:
        logger.error(f"❌ Error parsing Senate PTR {s3_key}: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
    
    return trades


def convert_to_dynamodb_format(item: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert Python types to DynamoDB-compatible types
    
    GSIs:
    - PoliticianTradeDateIndex: hash_key=politicianName, range_key=transactionDate
    - PositionTradeDateIndex: hash_key=position, range_key=transactionDate
    - PartyTradeDateIndex: hash_key=party, range_key=transactionDate
    - SecurityTradeDateIndex: hash_key=securitySymbol, range_key=transactionDate
    - FormTypeTradeDateIndex: hash_key=formType, range_key=transactionDate
    - TransactionTypeTradeDateIndex: hash_key=transactionType, range_key=transactionDate
    - AmountRangeTradeDateIndex: hash_key=amountMin, range_key=transactionDate
    - StateDistrictTradeDateIndex: hash_key=stateDistrict, range_key=transactionDate
    
    Note: GSI hash keys cannot be null. If securitySymbol or amountMin is null, we exclude it
    so the item won't appear in those GSIs.
    
    Args:
        item: Trade dict with Python types (includes websiteUrl as a string attribute, not GSI)
        
    Returns:
        Dict with DynamoDB-compatible types (Decimal for numbers, strings preserved)
    """
    dynamodb_item = {}
    
    # GSI hash keys that cannot be null
    # Note: amountMin is numeric, others are strings
    gsi_hash_keys_string = ['politicianName', 'party', 'position', 'securitySymbol', 'formType', 'transactionType', 'stateDistrict']
    gsi_hash_keys_numeric = ['amountMin']
    
    # Extract amountMin from amountRange if present
    # amountRange is a list [min, max], we need amountMin as a number for the GSI
    if 'amountRange' in item and isinstance(item.get('amountRange'), list) and len(item.get('amountRange', [])) > 0:
        amount_range = item.get('amountRange')
        if amount_range[0] is not None:
            # Validate the value is numeric before using it
            try:
                # Try to convert to float to validate it's a number
                min_val = float(amount_range[0])
                # Check for NaN or Inf
                if min_val != min_val or min_val == float('inf') or min_val == float('-inf'):
                    logger.warning(f"⚠️ Invalid amountRange[0] value: {amount_range[0]} (NaN or Inf)")
                else:
                    # Ensure amountMin is set from amountRange if not already present
                    if 'amountMin' not in item or item.get('amountMin') is None:
                        item['amountMin'] = amount_range[0]
            except (ValueError, TypeError) as e:
                logger.warning(f"⚠️ Invalid amountRange[0] value: {amount_range[0]} - {e}")
    
    for key, value in item.items():
        if value is None:
            # For GSI hash keys, skip null values (item won't appear in that GSI)
            # For other fields, just skip them
            if key in gsi_hash_keys_string or key in gsi_hash_keys_numeric:
                logger.debug(f"⚠️ Skipping null GSI hash key '{key}' - item won't appear in {key} GSI")
            continue
        elif isinstance(value, bool):
            dynamodb_item[key] = value
        elif isinstance(value, (int, float)):
            # Handle special float values (NaN, Inf) that can't be converted to Decimal
            if isinstance(value, float) and (value != value or value == float('inf') or value == float('-inf')):
                logger.warning(f"⚠️ Skipping invalid numeric value for '{key}': {value} (NaN or Inf)")
                continue
            try:
                # Convert to string first, then to Decimal
                value_str = str(value)
                # Check for invalid string representations
                if value_str.lower() in ['none', 'null', 'nan', 'inf', '-inf', '']:
                    logger.warning(f"⚠️ Skipping invalid numeric value for '{key}': '{value_str}'")
                    continue
                dynamodb_item[key] = Decimal(value_str)
            except (ValueError, TypeError, Exception) as e:
                logger.error(f"❌ Error converting '{key}' to Decimal: value={value} (type={type(value).__name__}), error={type(e).__name__}: {e}")
                # Skip this field rather than failing the entire trade
                continue
        elif isinstance(value, str):
            # Check if string represents a number (for edge cases)
            if value.strip().lower() in ['none', 'null', 'nan', 'inf', '-inf', '']:
                # Skip invalid string representations
                if key in gsi_hash_keys_string or key in gsi_hash_keys_numeric:
                    logger.debug(f"⚠️ Skipping invalid string value for GSI hash key '{key}': '{value}'")
                continue
            # Ensure GSI hash keys are non-empty strings
            if key in gsi_hash_keys_string and not value.strip():
                logger.debug(f"⚠️ Skipping empty GSI hash key '{key}' - item won't appear in {key} GSI")
                continue
            dynamodb_item[key] = value
        elif isinstance(value, list):
            # Convert list elements to DynamoDB-compatible types (e.g., amountRange)
            # DynamoDB lists can contain Decimal values, so convert numeric elements
            converted_list = []
            for item in value:
                if item is None:
                    converted_list.append(None)
                elif isinstance(item, (int, float)):
                    # Handle special float values (NaN, Inf)
                    if isinstance(item, float) and (item != item or item == float('inf') or item == float('-inf')):
                        logger.warning(f"⚠️ Skipping invalid numeric value in list: {item} (NaN or Inf)")
                        continue
                    try:
                        converted_list.append(Decimal(str(item)))
                    except (ValueError, TypeError) as e:
                        logger.warning(f"⚠️ Error converting list element to Decimal: {item} - {e}")
                        converted_list.append(item)  # Keep original if conversion fails
                else:
                    converted_list.append(item)
            dynamodb_item[key] = converted_list
        elif isinstance(value, dict):
            dynamodb_item[key] = value
        else:
            # Try to convert to string, but skip if it's None or invalid
            try:
                dynamodb_item[key] = str(value)
            except Exception as e:
                logger.warning(f"⚠️ Error converting '{key}' to string: {type(value).__name__} - {e}")
                continue
    
    # Validate required GSI fields are present (securitySymbol and amountMin are optional)
    required_gsi_fields = ['politicianName', 'party', 'position', 'formType', 'transactionType', 'transactionDate']
    missing_fields = [field for field in required_gsi_fields if field not in dynamodb_item]
    if missing_fields:
        logger.warning(f"⚠️ Missing required GSI fields: {missing_fields}")
    
    return dynamodb_item


def save_trade(table, trade: Dict[str, Any]) -> bool:
    """
    Save a single matched trade to DynamoDB immediately
    
    Args:
        table: DynamoDB table resource
        trade: Trade dict to save
        
    Returns:
        True if saved successfully, False otherwise
    """
    try:
        # Convert to DynamoDB format
        dynamodb_item = convert_to_dynamodb_format(trade)
        
        # Add processing timestamp
        dynamodb_item['processingDate'] = Decimal(str(int(datetime.now().timestamp())))
        
        # Save the trade
        table.put_item(Item=dynamodb_item)
        return True
    except Exception as e:
        trade_id = trade.get('tradeId', 'unknown')
        logger.error(f"❌ Error saving trade {trade_id}: {type(e).__name__}: {e}")
        logger.error(f"   Trade data: {json.dumps(trade, default=str)[:500]}")  # Log first 500 chars of trade
        return False


def lambda_handler(event, context):
    """
    Lambda handler for matching trades from a single Senate PTR file to politicians
    
    Expected input (from Step Functions Map state):
    {
        "s3Key": "trades/senate/2024-01-15/senate-ptr-uuid.html",
        "formType": "senate_ptr",
        "filingDate": "2024-01-15",
        "filer_name": "Scott, Rick (Senator)",
        "source": "senate"
    }
    
    Returns:
    {
        "matchedTradeIds": ["trade_2024-01-15_senate_0", "trade_2024-01-15_senate_1", ...],
        "matchedCount": 2,
        "unmatchedCount": 0,
        "s3Key": "...",
        "formType": "...",
        "source": "senate",
        "tradesSaved": 2,
        "saveErrors": 0
    }
    
    Note: Only trade IDs are returned to reduce Step Functions payload size.
    Full trade data is saved directly to DynamoDB during matching.
    """
    logger.info(f"🚀 Politician Trades Senate Matcher Lambda started: {json.dumps(event)}")
    
    if not S3_BUCKET:
        raise ValueError("S3_BUCKET environment variable not set")
    
    # Initialize DynamoDB table if saving is enabled
    table = None
    if DYNAMODB_TABLE_NAME:
        try:
            table = dynamodb.Table(DYNAMODB_TABLE_NAME)
            logger.info(f"✅ DynamoDB table initialized: {DYNAMODB_TABLE_NAME}")
        except Exception as e:
            logger.warning(f"⚠️ Could not initialize DynamoDB table: {e}")
            logger.warning(f"   Trades will be matched but not saved to DynamoDB")
    else:
        logger.warning(f"⚠️ DYNAMODB_TABLE_NAME not set - trades will be matched but not saved")
    
    # Extract file info from event
    s3_key = event.get('s3Key') or event.get('s3_key')
    form_type = event.get('formType')
    filing_date = event.get('filingDate') or event.get('date', '')
    
    # Check if this is a failed download (from downloader Lambda error handling)
    if event.get('success') is False:
        logger.warning(f"⚠️ Skipping failed download: {event.get('error', 'Unknown error')}")
        return {
            "matchedTradeIds": [],
            "matchedCount": 0,
            "unmatchedCount": 0,
            "s3Key": None,
            "formType": form_type,
            "error": event.get('error', 'Download failed'),
            "tradesSaved": 0,
            "saveErrors": 0
        }
    
    if not s3_key:
        logger.warning("⚠️ No s3Key provided in event")
        return {
            "matchedTradeIds": [],
            "matchedCount": 0,
            "unmatchedCount": 0,
            "s3Key": None,
            "formType": form_type,
            "tradesSaved": 0,
            "saveErrors": 0
        }
    
    try:
        # Load politician list
        logger.info("📋 Loading congress-legislators list from S3")
        politicians = load_politician_list()
        if not politicians:
            raise ValueError("Failed to load congress-legislators.csv from S3")
        
        logger.info(f"✅ Loaded {len(politicians)} legislators")
        
        # Parse the Senate PTR file
        # Get filer_name and filingDate from event (from downloader/fetcher)
        # These are more reliable than extracting from text
        filer_name_from_event = event.get('filer_name')
        filing_date_from_event = event.get('filingDate') or filing_date
        
        # Ensure filing_date_from_event is in YYYY-MM-DD format and not empty
        if filing_date_from_event:
            try:
                # Normalize to YYYY-MM-DD format
                if '/' in filing_date_from_event:
                    # MM/DD/YYYY format
                    date_obj = datetime.strptime(filing_date_from_event, '%m/%d/%Y')
                    filing_date_from_event = date_obj.strftime('%Y-%m-%d')
                elif '-' in filing_date_from_event and len(filing_date_from_event) == 10:
                    # Already YYYY-MM-DD format
                    datetime.strptime(filing_date_from_event, '%Y-%m-%d')  # Validate format
                else:
                    # Try other formats
                    try:
                        date_obj = datetime.strptime(filing_date_from_event, '%Y%m%d')
                        filing_date_from_event = date_obj.strftime('%Y-%m-%d')
                    except:
                        logger.warning(f"⚠️ Could not parse filing_date format: {filing_date_from_event}")
                        filing_date_from_event = None
            except ValueError as e:
                logger.warning(f"⚠️ Error normalizing filing_date '{filing_date_from_event}': {e}")
                filing_date_from_event = None
        
        # Use normalized filing_date_from_event, fallback to filing_date if needed
        final_filing_date = filing_date_from_event or filing_date
        
        if not final_filing_date or final_filing_date == '':
            logger.error(f"❌ No valid filing_date available for tradeId generation. Event: {json.dumps(event, default=str)}")
            raise ValueError("filingDate is required and must be in YYYY-MM-DD format")
        
        # Check if transactions are already extracted at fetcher level
        if event.get('transactions'):
            logger.info(f"✅ Using {len(event.get('transactions', []))} pre-extracted transactions from fetcher")
            trades = event.get('transactions', [])
            # Ensure all trades have filer_name from event
            for trade in trades:
                if not trade.get('filerName') and filer_name_from_event:
                    trade['filerName'] = filer_name_from_event
        else:
            # Fallback: parse from S3 file
            logger.info(f"📄 No pre-extracted transactions found, parsing from S3 file")
            # Pass filer_name from event - this is more reliable than OCR extraction
            trades = parse_senate_ptr(s3_key, event=event)
        
        # Filter out invalid trades (noise from Textract, incomplete data, etc.)
        valid_trades = []
        for trade in trades:
            # Validate the trade has meaningful data
            if is_valid_trade(trade):
                valid_trades.append(trade)
            else:
                logger.debug(f"⚠️ Filtered out invalid trade (likely noise): {json.dumps(trade, default=str)[:200]}")
        
        logger.info(f"📊 Processing {len(valid_trades)} valid trades (filtered {len(trades) - len(valid_trades)} invalid trades)")
        
        # If no valid trades were extracted, create a single placeholder trade
        if len(valid_trades) == 0:
            logger.warning(f"⚠️ No valid trades extracted from {s3_key}. File may be unparseable or contain no transactions.")
            
            # For Senate PTRs, create a placeholder trade if we have a filer name
            # This allows users to download the original filing via S3 key
            # Use filer_name from event (from downloader/fetcher) - more reliable
            filer_name = event.get('filer_name')
            if filer_name and is_valid_filer_name(filer_name):
                logger.info(f"📋 Creating single placeholder trade for unparsed document (filer: {filer_name})")
                # Create placeholder trade with all required fields
                # Use UNPARSED_AMOUNT_VALUE for amountMin/Max to allow "N/A" searches to map to this high value
                placeholder_trade = {
                    'filerName': filer_name,
                    'transactionDate': final_filing_date,  # Use filing date as placeholder
                    'securityName': None,
                    'securitySymbol': None,
                    'transactionType': None,  # Must be null for unparsed documents (no speculation)
                    'owner': None,
                    'amountRange': [UNPARSED_AMOUNT_VALUE, UNPARSED_AMOUNT_VALUE],  # High value for unreadable documents
                    'amountMin': UNPARSED_AMOUNT_VALUE,  # High value to allow "N/A" search mapping
                    'amountMax': UNPARSED_AMOUNT_VALUE,  # High value to allow "N/A" search mapping
                    'exactAmount': None,
                    'comment': None,
                    'isUnparsed': True,  # Flag indicating this requires manual review
                    'requiresManualReview': True  # Alternative flag for clarity
                }
                valid_trades.append(placeholder_trade)
                logger.info(f"✅ Created placeholder trade - users can download original filing from S3: {s3_key}")
            else:
                logger.warning(f"⚠️ Cannot create placeholder trade - no valid filer_name available")
        
        # Match trades to politicians and save each one immediately
        matched_trade_ids = []  # Only store IDs to reduce payload size
        unmatched_count = 0
        saved_count = 0
        save_errors = 0
        
        for trade in valid_trades:
            # For Senate PTRs, use filerName from trade (pre-extracted from HTML or Textract)
            filer_name = trade.get('filerName') or trade.get('politicianName')
            
            # If no filer_name in trade, try to get from event (for Senate PTRs)
            if not filer_name:
                filer_name = event.get('filer_name')
            
            if not filer_name:
                logger.warning(f"⚠️ Trade missing filerName/politicianName, skipping: {json.dumps(trade, default=str)}")
                unmatched_count += 1
                continue
            
            # Filter out invalid filer names (OCR noise)
            if not is_valid_filer_name(filer_name):
                logger.warning(f"⚠️ Invalid filer name detected (likely OCR noise): '{filer_name}', skipping trade")
                unmatched_count += 1
                continue
            
            logger.info(f"🔍 Attempting to match filer: {filer_name}")
            matched_politician = find_matching_politician(filer_name, politicians)
            if matched_politician:
                logger.info(f"✅ Matched '{filer_name}' to politician: {matched_politician.get('name')} (confidence: {matched_politician.get('matchScore', 'N/A')})")
            else:
                logger.warning(f"❌ No match found for filer: {filer_name}")
            if matched_politician:
                # Check if this is an unparsed placeholder trade
                is_unparsed = trade.get('isUnparsed', False) or trade.get('requiresManualReview', False)
                
                # Map Senate PTR transaction fields to standard format
                # Senate PTRs use: securityName, assetType, order, amount
                # Standard format uses: securityName, transactionType, amountMin/amountMax
                # For unparsed trades, transactionType must be None (not speculation)
                if is_unparsed:
                    transaction_type = None
                else:
                    transaction_type = trade.get('order') or trade.get('transactionType')
                
                # Get amountRange (list of two integers) and exactAmount
                # For unparsed trades, use UNPARSED_AMOUNT_VALUE for amounts to allow "N/A" search mapping
                if is_unparsed:
                    amount_range = [UNPARSED_AMOUNT_VALUE, UNPARSED_AMOUNT_VALUE]
                    exact_amount = None
                else:
                    amount_range = trade.get('amountRange')  # [min, max] or [min, None]
                    exact_amount = trade.get('exactAmount')  # Exact dollar amount if provided
                
                # Convert transactionDate to numeric format for GSI range key
                # transactionDate should be stored as YYYYMMDD integer (e.g., 20251002 for 2025-10-02)
                transaction_date_str = trade.get('transactionDate') or ''
                transaction_date_num = None
                
                # For placeholder/unparsed trades, use filing date directly
                if is_unparsed:
                    try:
                        filing_date_obj = datetime.strptime(final_filing_date, '%Y-%m-%d').date()
                        transaction_date_num = int(filing_date_obj.strftime('%Y%m%d'))
                    except:
                        pass
                elif transaction_date_str:
                    try:
                        # If it's already in YYYY-MM-DD format, convert to YYYYMMDD integer
                        if len(transaction_date_str) == 10 and '-' in transaction_date_str:
                            transaction_date_num = int(transaction_date_str.replace('-', ''))
                        # If it's in MM/DD/YYYY format (from Senate PTR HTML), parse and convert
                        elif len(transaction_date_str) == 10 and '/' in transaction_date_str:
                            parsed_date = datetime.strptime(transaction_date_str, '%m/%d/%Y').date()
                            transaction_date_num = int(parsed_date.strftime('%Y%m%d'))
                        # If it's in M/D/YY format (from Textract - e.g., "8/14/25")
                        elif '/' in transaction_date_str and len(transaction_date_str) < 10:
                            # Try to parse M/D/YY or MM/DD/YY format
                            parts = transaction_date_str.split('/')
                            if len(parts) == 3:
                                month = int(parts[0])
                                day = int(parts[1])
                                year_str = parts[2].strip()
                                # Handle 2-digit year (assume 20xx for years < 50, 19xx for years >= 50)
                                if len(year_str) == 2:
                                    year_int = int(year_str)
                                    year = 2000 + year_int if year_int < 50 else 1900 + year_int
                                else:
                                    year = int(year_str)
                                parsed_date = datetime(year, month, day).date()
                                transaction_date_num = int(parsed_date.strftime('%Y%m%d'))
                                logger.debug(f"   Parsed date '{transaction_date_str}' -> {parsed_date.strftime('%Y-%m-%d')} ({transaction_date_num})")
                        # If it's already numeric, use it
                        elif transaction_date_str.isdigit():
                            transaction_date_num = int(transaction_date_str)
                        # Otherwise try to parse as YYYY-MM-DD
                        else:
                            parsed_date = datetime.strptime(transaction_date_str, '%Y-%m-%d').date()
                            transaction_date_num = int(parsed_date.strftime('%Y%m%d'))
                    except Exception as e:
                        logger.warning(f"⚠️ Error parsing transaction date '{transaction_date_str}': {e}")
                        # Fallback: use filing date as transaction date
                        try:
                            filing_date_obj = datetime.strptime(final_filing_date, '%Y-%m-%d').date()
                            transaction_date_num = int(filing_date_obj.strftime('%Y%m%d'))
                            logger.warning(f"   Using filing date as fallback: {transaction_date_num}")
                        except:
                            pass
                else:
                    # Use filing date if transaction date is missing
                    logger.warning(f"⚠️ No transaction date in trade, using filing date")
                    try:
                        filing_date_obj = datetime.strptime(final_filing_date, '%Y-%m-%d').date()
                        transaction_date_num = int(filing_date_obj.strftime('%Y%m%d'))
                    except:
                        pass
                
                # Format state/district for the politician
                state_district = format_state_district(matched_politician)
                
                # Get securitySymbol and set to "OTHER" if missing or empty
                security_symbol = trade.get('ticker') or trade.get('securitySymbol')
                if not security_symbol or (isinstance(security_symbol, str) and security_symbol.strip() in ('', '--')):
                    security_symbol = 'OTHER'
                
                # Generate trade ID
                trade_id = f"trade_{final_filing_date}_senate_{len(matched_trade_ids)}"
                
                matched_trade = {
                    'tradeId': trade_id,
                    'politicianName': matched_politician['name'],  # GSI: PoliticianTradeDateIndex
                    'party': matched_politician['party'],  # GSI: PartyTradeDateIndex
                    'position': matched_politician['position'],  # GSI: PositionTradeDateIndex
                    'websiteUrl': matched_politician.get('websiteUrl'),  # Regular attribute (not GSI)
                    'formType': form_type or 'senate_ptr',  # GSI: FormTypeTradeDateIndex
                    'filingDate': final_filing_date,
                    'transactionDate': transaction_date_num,  # GSI range key (numeric: YYYYMMDD format)
                    'transactionTime': trade.get('transactionTime'),
                    'securitySymbol': security_symbol,  # GSI: SecurityTradeDateIndex
                    'securityName': trade.get('securityName'),
                    'assetType': trade.get('assetType'),  # Include assetType for Senate PTRs
                    'transactionType': transaction_type,  # GSI: TransactionTypeTradeDateIndex ("Purchase", "Sale", etc.) - None for unparsed
                    'order': trade.get('order'),  # Keep original "order" field
                    'shares': trade.get('shares'),
                    'pricePerShare': trade.get('pricePerShare'),
                    # For unparsed trades, use UNPARSED_AMOUNT_VALUE for amounts; otherwise use trade values
                    'amountMin': UNPARSED_AMOUNT_VALUE if is_unparsed else trade.get('amountMin'),  # High value for unparsed, allows "N/A" search
                    'amountMax': UNPARSED_AMOUNT_VALUE if is_unparsed else trade.get('amountMax'),  # High value for unparsed, allows "N/A" search
                    'amountRange': amount_range,  # [UNPARSED_AMOUNT_VALUE, UNPARSED_AMOUNT_VALUE] for unparsed, [min, max] for valid trades
                    'exactAmount': exact_amount,  # Exact dollar amount if provided (not a GSI) - None for unparsed
                    'owner': trade.get('owner'),
                    'metadata': trade.get('metadata'),  # Flexible JSON blob for asset metadata (e.g., Rate/Coupon, Matures)
                    'comment': trade.get('comment'),
                    'formS3Key': s3_key,  # Critical: S3 key for downloading original filing
                    'matchConfidence': matched_politician.get('matchScore', 1.0),
                    'source': 'senate',
                    'isUnparsed': is_unparsed,  # Flag indicating this filing could not be parsed automatically
                    'requiresManualReview': is_unparsed,  # Alternative flag for clarity
                    'stateDistrict': state_district  # GSI: StateDistrictTradeDateIndex
                }
                
                if is_unparsed:
                    logger.info(f"📋 Matched unparsed placeholder trade - original filing available at S3: {s3_key}")
                
                # Save the matched trade immediately to DynamoDB
                if table:
                    if save_trade(table, matched_trade):
                        saved_count += 1
                        logger.debug(f"💾 Saved trade {matched_trade.get('tradeId', 'unknown')} to DynamoDB")
                    else:
                        save_errors += 1
                        logger.warning(f"⚠️ Failed to save trade {matched_trade.get('tradeId', 'unknown')} to DynamoDB")
                else:
                    logger.debug(f"⚠️ DynamoDB table not initialized - skipping save for trade {matched_trade.get('tradeId', 'unknown')}")
                
                # Only store the trade ID to reduce payload size
                matched_trade_ids.append(trade_id)
            else:
                unmatched_count += 1
        
        logger.info(f"✅ Matched {len(matched_trade_ids)} trades from {s3_key}")
        if table:
            logger.info(f"💾 Saved {saved_count} trades to DynamoDB ({save_errors} errors)")
        
        return {
            "matchedTradeIds": matched_trade_ids,  # Only return IDs to reduce payload size
            "matchedCount": len(matched_trade_ids),
            "unmatchedCount": unmatched_count,
            "s3Key": s3_key,
            "formType": form_type,
            "source": "senate",
            "tradesSaved": saved_count,
            "saveErrors": save_errors
        }
        
    except Exception as e:
        logger.error(f"❌ Error in Senate matcher Lambda: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        return {
            "matchedTradeIds": [],
            "matchedCount": 0,
            "unmatchedCount": 1,
            "s3Key": s3_key,
            "formType": form_type,
            "error": str(e),
            "tradesSaved": 0,
            "saveErrors": 0
        }