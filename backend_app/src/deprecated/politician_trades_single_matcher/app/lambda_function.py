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
import defusedxml.ElementTree as ET
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


def is_valid_filer_name(filer_name: str) -> bool:
    """
    Filter out obviously invalid filer names (OCR noise, garbage text)
    
    Args:
        filer_name: Filer name to validate
    
    Returns:
        True if name appears valid, False otherwise
    """
    if not filer_name or len(filer_name.strip()) < 3:
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
        import requests
        from urllib.parse import urlparse, urljoin
        
        logger.info(f"📥 Downloading image from: {image_url}")
        
        # Handle relative URLs - convert to absolute if needed
        if image_url.startswith('//'):
            image_url = 'https:' + image_url
        elif image_url.startswith('/'):
            # Extract domain from s3_key context or use default
            image_url = 'https://efdsearch.senate.gov' + image_url
        
        # Download image using requests (avoids urllib file:// scheme risk)
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        response = requests.get(image_url, headers=headers, timeout=30)
        response.raise_for_status()
        image_content = response.content
        logger.info(f"✅ Downloaded image ({len(image_content)} bytes)")
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
        headers = [h.strip().lower() for h in table_data[0]]
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
                    trade['owner'] = row[col_indices['owner']].strip()
                if 'asset' in col_indices:
                    asset = row[col_indices['asset']].strip()
                    trade['securityName'] = asset
                    # Try to extract ticker if present
                    ticker_match = re.search(r'\(([A-Z]{1,5})\)', asset)
                    if ticker_match:
                        trade['securitySymbol'] = ticker_match.group(1)
                if 'transaction_type' in col_indices:
                    trans_type = row[col_indices['transaction_type']].strip()
                    trade['transactionType'] = trans_type
                if 'date' in col_indices:
                    date_str = row[col_indices['date']].strip()
                    trade['transactionDate'] = date_str
                if 'amount' in col_indices:
                    amount_str = row[col_indices['amount']].strip()
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
            # Remove HTML tags
            text = re.sub(r'<[^>]+>', '', text)
            # Decode HTML entities
            text = unescape(text)
            # Strip whitespace
            text = text.strip()
            # Remove extra whitespace
            text = re.sub(r'\s+', ' ', text)
            return text
        
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
                if amount_index is not None and amount_index >= 6:
                    # Smart mapping: work backwards from Amount column
                    transaction_date_str = clean_html(cells[amount_index - 6]) if amount_index >= 6 else ''
                    owner = clean_html(cells[amount_index - 5]) if amount_index >= 5 else ''
                    ticker = clean_html(cells[amount_index - 4]) if amount_index >= 4 else ''
                    asset_name = clean_html(cells[amount_index - 3]) if amount_index >= 3 else ''
                    asset_type = clean_html(cells[amount_index - 2]) if amount_index >= 2 else ''
                    transaction_type = clean_html(cells[amount_index - 1]) if amount_index >= 1 else ''
                    amount_str = clean_html(cells[amount_index])
                    comment = clean_html(cells[amount_index + 1]) if amount_index + 1 < len(cells) else ''
                else:
                    # Sequential mapping (standard case)
                    # Skip first cell if it's just a row number
                    start_idx = 1 if (len(cells) > 0 and re.match(r'^\s*\d+\s*$', clean_html(cells[0]))) else 0
                    transaction_date_str = clean_html(cells[start_idx + 0]) if len(cells) > start_idx + 0 else ''
                    owner = clean_html(cells[start_idx + 1]) if len(cells) > start_idx + 1 else ''
                    ticker = clean_html(cells[start_idx + 2]) if len(cells) > start_idx + 2 else ''
                    asset_name = clean_html(cells[start_idx + 3]) if len(cells) > start_idx + 3 else ''
                    asset_type = clean_html(cells[start_idx + 4]) if len(cells) > start_idx + 4 else ''
                    transaction_type = clean_html(cells[start_idx + 5]) if len(cells) > start_idx + 5 else ''
                    amount_str = clean_html(cells[start_idx + 6]) if len(cells) > start_idx + 6 else ''
                    comment = clean_html(cells[start_idx + 7]) if len(cells) > start_idx + 7 else ''
                
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
                # Standard Senate PTR ranges (fixed):
                # $1 - $1,000
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
                
                # Define standard Senate PTR ranges (as tuples of (min, max))
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
                
                amount_min = None
                amount_max = None
                total_amount = None
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
                                
                                total_amount = (raw_min + raw_max) / 2
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
                            total_amount = exact_value
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
                    'amountRange': amount_range,  # List of two integers [min, max] for standard Senate PTR range
                    'exactAmount': exact_amount,  # Exact dollar amount if provided (not a GSI)
                    'shares': None,  # Not provided in Senate PTR HTML
                    'comment': clean_html(cells[8]) if len(cells) > 8 else '',
                    'filerName': filer_name  # Include filer name from HTML for matching
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
            # Get filer_name and filingDate from event (from downloader/fetcher)
            # These are more reliable than extracting from text
            filer_name_from_event = event.get('filer_name')
            filing_date_from_event = event.get('filingDate') or filing_date
            
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
            
            # For Senate PTRs, create a placeholder trade if we have a filer name
            # This allows users to download the original filing via S3 key
            if source == 'senate':
                # Use filer_name from event (from downloader/fetcher) - more reliable
                filer_name = event.get('filer_name')
                if filer_name and is_valid_filer_name(filer_name):
                    logger.info(f"📋 Creating placeholder trade for unparsed document (filer: {filer_name})")
                    # Create placeholder trade with all required fields
                    placeholder_trade = {
                        'filerName': filer_name,
                        'transactionDate': filing_date,  # Use filing date as placeholder
                        'securityName': None,
                        'securitySymbol': None,
                        'transactionType': None,
                        'owner': None,
                        'amountRange': None,
                        'amountMin': None,
                        'amountMax': None,
                        'exactAmount': None,
                        'comment': None,
                        'isUnparsed': True,  # Flag indicating this requires manual review
                        'requiresManualReview': True  # Alternative flag for clarity
                    }
                    trades.append(placeholder_trade)
                    logger.info(f"✅ Created placeholder trade - users can download original filing from S3: {s3_key}")
                else:
                    logger.warning(f"⚠️ Cannot create placeholder trade - no filer_name available")
        
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
            else:  # house or senate
                # For Senate PTRs, use filerName from trade (pre-extracted from HTML or Textract)
                # For House PTRs, may have politicianName or filerName
                filer_name = trade.get('filerName') or trade.get('politicianName')
                
                # If no filer_name in trade, try to get from event (for Senate PTRs)
                if not filer_name and source == 'senate':
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
                    # Standard format uses: securityName, transactionType, totalAmount
                    transaction_type = trade.get('order') or trade.get('transactionType')
                    
                    # Get amountRange (list of two integers) and exactAmount
                    amount_range = trade.get('amountRange')  # [min, max] or [min, None]
                    exact_amount = trade.get('exactAmount')  # Exact dollar amount if provided
                    
                    # Convert transactionDate to numeric format for GSI range key
                    # transactionDate should be stored as YYYYMMDD integer (e.g., 20251002 for 2025-10-02)
                    transaction_date_str = trade.get('transactionDate') or ''
                    transaction_date_num = None
                    
                    # For placeholder/unparsed trades, use filing date directly
                    if is_unparsed:
                        try:
                            filing_date_obj = datetime.strptime(filing_date, '%Y-%m-%d').date()
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
                                filing_date_obj = datetime.strptime(filing_date, '%Y-%m-%d').date()
                                transaction_date_num = int(filing_date_obj.strftime('%Y%m%d'))
                                logger.warning(f"   Using filing date as fallback: {transaction_date_num}")
                            except:
                                pass
                    else:
                        # Use filing date if transaction date is missing
                        logger.warning(f"⚠️ No transaction date in trade, using filing date")
                        try:
                            filing_date_obj = datetime.strptime(filing_date, '%Y-%m-%d').date()
                            transaction_date_num = int(filing_date_obj.strftime('%Y%m%d'))
                        except:
                            pass
                    
                    matched_trade = {
                        'tradeId': f"trade_{filing_date}_{source}_{len(matched_trades)}",
                        'politicianName': matched_politician['name'],  # GSI: PoliticianTradeDateIndex
                        'party': matched_politician['party'],  # GSI: PartyTradeDateIndex
                        'position': matched_politician['position'],  # GSI: PositionTradeDateIndex
                        'websiteUrl': matched_politician.get('websiteUrl'),  # Regular attribute (not GSI)
                        'formType': form_type or f'{source}_ptr',  # GSI: FormTypeTradeDateIndex
                        'filingDate': filing_date,
                        'transactionDate': transaction_date_num,  # GSI range key (numeric: YYYYMMDD format)
                        'transactionTime': trade.get('transactionTime'),
                        'securitySymbol': trade.get('ticker') or trade.get('securitySymbol'),  # GSI: SecurityTradeDateIndex
                        'securityName': trade.get('securityName'),
                        'assetType': trade.get('assetType'),  # Include assetType for Senate PTRs
                        'transactionType': transaction_type,  # GSI: TransactionTypeTradeDateIndex ("Purchase", "Sale", etc.)
                        'order': trade.get('order'),  # Keep original "order" field
                        'shares': trade.get('shares'),
                        'pricePerShare': trade.get('pricePerShare'),
                        'totalAmount': trade.get('amount') or trade.get('totalAmount'),
                        'amountMin': trade.get('amountMin'),  # For backwards compatibility
                        'amountMax': trade.get('amountMax'),  # For backwards compatibility
                        'amountRange': amount_range,  # List of two integers [min, max] for standard Senate PTR range
                        'exactAmount': exact_amount,  # Exact dollar amount if provided (not a GSI)
                        'owner': trade.get('owner'),
                        'comment': trade.get('comment'),
                        'formS3Key': s3_key,  # Critical: S3 key for downloading original filing
                        'matchConfidence': matched_politician.get('matchScore', 1.0),
                        'source': source,
                        'isUnparsed': is_unparsed,  # Flag indicating this filing could not be parsed automatically
                        'requiresManualReview': is_unparsed  # Alternative flag for clarity
                    }
                    
                    if is_unparsed:
                        logger.info(f"📋 Matched unparsed placeholder trade - original filing available at S3: {s3_key}")
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

