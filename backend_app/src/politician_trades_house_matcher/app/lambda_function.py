"""
Politician Trades House Matcher Lambda
Parses House PTRs, extracts trades, and matches to politicians

This Lambda handles House PTR matching only.
"""

import json
import os
import logging
import re
import boto3
import time
from botocore.exceptions import ClientError
from typing import List, Dict, Any, Optional
from datetime import datetime
import csv
from io import StringIO, BytesIO
from difflib import SequenceMatcher
from pypdf import PdfReader

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

# Module-level cache for asset codes mapping (loaded once per lambda container)
_ASSET_CODES_CACHE: Optional[Dict[str, str]] = None

def get_asset_codes_mapping() -> Dict[str, str]:
    """
    Get House PTR asset codes mapping, loading from S3 if not already cached.
    This is cached at module level to avoid reloading for each House PTR in a batch.
    
    Returns:
        Dict mapping asset codes to asset names
    """
    global _ASSET_CODES_CACHE
    
    if _ASSET_CODES_CACHE is not None:
        return _ASSET_CODES_CACHE
    
    logger.info("📋 Loading House PTR asset codes mapping from S3 (first time)")
    
    asset_codes = {}
    
    try:
        response = s3_client.get_object(
            Bucket=S3_BUCKET,
            Key='house_ptr_asset_codes.csv'
        )
        
        csv_content = response['Body'].read().decode('utf-8')
        csv_reader = csv.DictReader(StringIO(csv_content))
        
        for row in csv_reader:
            code = row.get('Asset Code', '').strip()
            name = row.get('Asset Name', '').strip()
            if code and name:
                asset_codes[code] = name
        
        logger.info(f"✅ Loaded {len(asset_codes)} asset codes")
        _ASSET_CODES_CACHE = asset_codes
        return asset_codes
        
    except Exception as e:
        logger.warning(f"⚠️ Error loading asset codes mapping: {e}. Continuing without asset code mapping.")
        _ASSET_CODES_CACHE = {}  # Cache empty dict to avoid retrying on every call
        return {}

# Module-level variable to store last error (for debugging)
_last_politician_load_error: Optional[str] = None

def load_politician_list() -> List[Dict[str, Any]]:
    """
    Load congress-legislators CSV from S3
    
    Returns:
        List of politician dicts with name, party, position, url, and alternativeNames
    """
    global _last_politician_load_error
    _last_politician_load_error = None
    
    logger.info(f"📋 Loading congress-legislators list from S3: s3://{S3_BUCKET}/congress-legislators.csv")
    
    if not S3_BUCKET:
        error_msg = "❌ S3_BUCKET environment variable is not set!"
        logger.error(error_msg)
        _last_politician_load_error = error_msg
        return []
    
    try:
        # Download congress-legislators.csv from S3
        response = s3_client.get_object(
            Bucket=S3_BUCKET,
            Key='congress-legislators.csv'
        )
        
        csv_content = response['Body'].read().decode('utf-8-sig')  # Use utf-8-sig to automatically strip BOM
        logger.info(f"📄 Downloaded CSV file ({len(csv_content)} bytes)")
        
        if not csv_content or len(csv_content.strip()) == 0:
            error_msg = "❌ CSV file is empty!"
            logger.error(error_msg)
            _last_politician_load_error = error_msg
            return []
        
        csv_reader = csv.DictReader(StringIO(csv_content))
        
        # Normalize column names to remove any BOM characters that might have slipped through
        if csv_reader.fieldnames:
            # Strip BOM and whitespace from all field names
            normalized_fieldnames = []
            for field in csv_reader.fieldnames:
                normalized = field.strip().lstrip('\ufeff')  # Remove BOM if present
                normalized_fieldnames.append(normalized)
            csv_reader.fieldnames = normalized_fieldnames
            logger.info(f"📋 CSV headers: {csv_reader.fieldnames[:10]}...")  # First 10 headers
        
        politicians = []
        rows_processed = 0
        rows_skipped = 0
        
        for row in csv_reader:
            rows_processed += 1
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
            
            # Store name components for flexible matching
            # CSV format: last_name,first_name,middle_name,suffix,...
            first_name = row.get('first_name', '').strip()
            middle_name = row.get('middle_name', '').strip()
            last_name = row.get('last_name', '').strip()
            suffix = row.get('suffix', '').strip()
            
            # Validate that we have at least first and last name
            if not first_name or not last_name:
                rows_skipped += 1
                if rows_skipped <= 5:  # Only log first 5 skipped rows to avoid spam
                    logger.warning(f"⚠️ Skipping politician with missing name components: first_name='{first_name}', last_name='{last_name}', full_name='{primary_name}'")
                continue
            
            politicians.append({
                'name': primary_name,
                'first_name': first_name,
                'middle_name': middle_name,
                'last_name': last_name,
                'suffix': suffix,
                'party': party,
                'position': position,
                'websiteUrl': website_url if website_url else None,
                'alternativeNames': alt_names,
                'bioguide_id': row.get('bioguide_id', '').strip() if row.get('bioguide_id') else None,
                'state': row.get('state', '').strip() if row.get('state') else None,
                'district': row.get('district', '').strip() if row.get('district') else None
            })
        
        logger.info(f"✅ Loaded {len(politicians)} legislators from CSV (processed {rows_processed} rows, skipped {rows_skipped})")
        if len(politicians) == 0:
            error_msg = f"❌ No politicians loaded from CSV - file may be empty or malformed. Rows processed: {rows_processed}, Rows skipped: {rows_skipped}"
            logger.error(error_msg)
            # Log a sample row to help debug
            if rows_processed > 0:
                csv_reader_debug = csv.DictReader(StringIO(csv_content))
                sample_row = next(csv_reader_debug, None)
                if sample_row:
                    logger.error(f"❌ Sample row data: {json.dumps({k: v for k, v in list(sample_row.items())[:5]})}")
            _last_politician_load_error = error_msg
        return politicians
        
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        error_message = e.response.get('Error', {}).get('Message', str(e))
        if error_code == 'NoSuchKey':
            error_msg = f"❌ CSV file not found in S3: s3://{S3_BUCKET}/congress-legislators.csv. Please ensure the file is uploaded via Terraform."
            logger.error(error_msg)
        elif error_code == 'AccessDenied':
            error_msg = f"❌ Access denied to S3 bucket: s3://{S3_BUCKET}/congress-legislators.csv. Check IAM permissions."
            logger.error(error_msg)
        else:
            error_msg = f"❌ S3 ClientError loading congress-legislators list ({error_code}): {error_message}"
            logger.error(error_msg)
        logger.error(f"Full error details: {json.dumps(e.response.get('Error', {}), default=str)}")
        _last_politician_load_error = error_msg
        return []
    except Exception as e:
        error_msg = f"❌ Unexpected error loading congress-legislators list: {type(e).__name__}: {str(e)}"
        logger.error(error_msg)
        import traceback
        logger.error(traceback.format_exc())
        _last_politician_load_error = error_msg
        return []

# Common name shortening mappings (full name -> common nicknames/shortenings)
# This is bidirectional - both directions are checked
NAME_SHORTENINGS = {
    # Michael variations
    'michael': ['mike', 'mikey', 'mick', 'mickey'],
    'mike': ['michael', 'mikey', 'mick', 'mickey'],
    'mikey': ['michael', 'mike'],
    'mick': ['michael', 'mike'],
    'mickey': ['michael', 'mike'],
    
    # William variations
    'william': ['will', 'bill', 'billy', 'willy'],
    'will': ['william', 'bill', 'billy'],
    'bill': ['william', 'will', 'billy'],
    'billy': ['william', 'will', 'bill'],
    'willy': ['william', 'will'],
    
    # Robert variations
    'robert': ['bob', 'rob', 'robby', 'bobby', 'bert'],
    'bob': ['robert', 'rob', 'bobby'],
    'rob': ['robert', 'bob', 'robby'],
    'robby': ['robert', 'rob'],
    'bobby': ['robert', 'bob'],
    'bert': ['robert'],
    
    # Richard variations
    'richard': ['rick', 'rich', 'dick', 'ricky'],
    'rick': ['richard', 'rich', 'ricky'],
    'rich': ['richard', 'rick'],
    'dick': ['richard', 'rick'],
    'ricky': ['richard', 'rick'],
    
    # James variations
    'james': ['jim', 'jimmy', 'jamie'],
    'jim': ['james', 'jimmy'],
    'jimmy': ['james', 'jim'],
    'jamie': ['james'],
    
    # John variations
    'john': ['jack', 'johnny', 'jon'],
    'jack': ['john', 'johnny'],
    'johnny': ['john', 'jack'],
    'jon': ['john'],
    
    # Joseph variations
    'joseph': ['joe', 'joey'],
    'joe': ['joseph', 'joey'],
    'joey': ['joseph', 'joe'],
    
    # Charles variations
    'charles': ['chuck', 'charlie', 'charley'],
    'chuck': ['charles', 'charlie'],
    'charlie': ['charles', 'chuck'],
    'charley': ['charles', 'charlie'],
    
    # Thomas variations
    'thomas': ['tom', 'tommy'],
    'tom': ['thomas', 'tommy'],
    'tommy': ['thomas', 'tom'],
    
    # Daniel variations
    'daniel': ['dan', 'danny'],
    'dan': ['daniel', 'danny'],
    'danny': ['daniel', 'dan'],
    
    # Christopher variations
    'christopher': ['chris', 'christy'],
    'chris': ['christopher', 'christy'],
    'christy': ['christopher', 'chris'],
    
    # Edward variations
    'edward': ['ed', 'eddie', 'ted', 'ned'],
    'ed': ['edward', 'eddie', 'ted'],
    'eddie': ['edward', 'ed'],
    'ted': ['edward', 'ed', 'theodore'],
    'ned': ['edward', 'ed'],
    
    # Theodore variations
    'theodore': ['ted', 'teddy'],
    'teddy': ['theodore', 'ted'],
    
    # Matthew variations
    'matthew': ['matt', 'matty'],
    'matt': ['matthew', 'matty'],
    'matty': ['matthew', 'matt'],
    
    # Andrew variations
    'andrew': ['andy', 'drew'],
    'andy': ['andrew', 'drew'],
    'drew': ['andrew', 'andy'],
    
    # Anthony variations
    'anthony': ['tony', 'ant'],
    'tony': ['anthony', 'ant'],
    'ant': ['anthony', 'tony'],
    
    # Benjamin variations
    'benjamin': ['ben', 'benny'],
    'ben': ['benjamin', 'benny'],
    'benny': ['benjamin', 'ben'],
    
    # David variations
    'david': ['dave', 'davey'],
    'dave': ['david', 'davey'],
    'davey': ['david', 'dave'],
    
    # Patrick variations
    'patrick': ['pat', 'paddy'],
    'pat': ['patrick', 'paddy'],
    'paddy': ['patrick', 'pat'],
    
    # Timothy variations
    'timothy': ['tim', 'timmy'],
    'tim': ['timothy', 'timmy'],
    'timmy': ['timothy', 'tim'],
    
    # Samuel variations
    'samuel': ['sam', 'sammy'],
    'sam': ['samuel', 'sammy'],
    'sammy': ['samuel', 'sam'],
    
    # Alexander variations
    'alexander': ['alex', 'al'],
    'alex': ['alexander', 'al'],
    'al': ['alexander', 'alex', 'albert', 'alfred', 'allen'],
    
    # Albert variations
    'albert': ['al', 'bert'],
    
    # Alfred variations
    'alfred': ['al', 'alfie'],
    'alfie': ['alfred', 'al'],
    
    # Allen variations
    'allen': ['al'],
    
    # Nicholas variations
    'nicholas': ['nick', 'nickie'],
    'nick': ['nicholas', 'nickie'],
    'nickie': ['nicholas', 'nick'],
    
    # Jonathan variations
    'jonathan': ['jon', 'johnny'],
    
    # Stephen/Steven variations
    'stephen': ['steve', 'stevie'],
    'steven': ['steve', 'stevie'],
    'steve': ['stephen', 'steven', 'stevie'],
    'stevie': ['stephen', 'steven', 'steve'],
    
    # Kenneth variations
    'kenneth': ['ken', 'kenny'],
    'ken': ['kenneth', 'kenny'],
    'kenny': ['kenneth', 'ken'],
    
    # Joshua variations
    'joshua': ['josh'],
    'josh': ['joshua'],
    
    # Kevin variations
    'kevin': ['kev'],
    'kev': ['kevin'],
    
    # Brian variations
    'brian': ['bryan'],
    'bryan': ['brian'],
    
    # Gregory variations
    'gregory': ['greg'],
    'greg': ['gregory'],
    
    # Jeffrey variations
    'jeffrey': ['jeff'],
    'jeff': ['jeffrey'],
    
    # Raymond variations
    'raymond': ['ray'],
    'ray': ['raymond'],
    
    # Harold variations
    'harold': ['harry', 'hal'],
    'harry': ['harold', 'harry'],
    'hal': ['harold'],
}

def get_name_variations(name: str) -> List[str]:
    """
    Get all variations of a name including common shortenings
    
    Args:
        name: First name (e.g., "Michael", "Mike")
        
    Returns:
        List of name variations including the original
    """
    if not name:
        return ['']
    
    name_lower = name.lower().strip()
    variations = [name_lower]  # Always include original
    
    # Add common shortenings if they exist
    if name_lower in NAME_SHORTENINGS:
        variations.extend(NAME_SHORTENINGS[name_lower])
    
    # Also check reverse - if this is a nickname, add the full name
    for full_name, nicknames in NAME_SHORTENINGS.items():
        if name_lower in nicknames:
            variations.append(full_name)
            break  # Only add one full name to avoid duplicates
    
    # Remove duplicates while preserving order
    seen = set()
    unique_variations = []
    for var in variations:
        if var not in seen:
            seen.add(var)
            unique_variations.append(var)
    
    return unique_variations

def normalize_middle_name(middle_name: str) -> List[str]:
    """
    Generate variations of a middle name for matching
    Returns list of: full name, first letter, first letter with period, empty string
    """
    if not middle_name:
        return ['']
    
    middle = middle_name.strip()
    variations = [middle.lower()]  # Full name
    
    if len(middle) > 0:
        first_letter = middle[0].upper()
        variations.append(first_letter.lower())  # "f"
        variations.append(f"{first_letter.lower()}.")  # "f."
    
    variations.append('')  # No middle name
    return variations

def fuzzy_match_name(filer_name: str, politician: Dict[str, Any]) -> float:
    """
    Fuzzy match a filer name to a politician using flexible name matching with middle name support
    
    Args:
        filer_name: Name from SEC form or PTR (e.g., "Laurel Lee", "John A. Doe")
        politician: Politician dict with name components and alternativeNames
        
    Returns:
        Similarity score (0.0 to 1.0)
    """
    # Normalize filer name (lowercase, strip, remove extra spaces)
    filer_normalized = ' '.join(filer_name.lower().strip().split())
    
    # Get politician name components
    first_name = politician.get('first_name', '').lower().strip()
    middle_name = politician.get('middle_name', '').lower().strip()
    last_name = politician.get('last_name', '').lower().strip()
    suffix = politician.get('suffix', '').lower().strip()
    politician_full = politician['name'].lower().strip()
    
    # Check exact match with full name first
    if filer_normalized == politician_full:
        return 1.0
    
    # Check alternative names
    for alt_name in politician.get('alternativeNames', []):
        if filer_normalized == alt_name.lower().strip():
            return 1.0
    
    # Try matching with different name combinations
    # Parse filer name into components (try different patterns)
    filer_parts = filer_normalized.split()
    
    # Pattern 1: "First Last" (no middle name) - e.g., "Laurel Lee" should match "Laurel Frances Lee"
    # Also handles name shortenings: "Michael Collins" should match "Mike Collins"
    if len(filer_parts) >= 2:
        filer_first = filer_parts[0]
        filer_last = filer_parts[-1]  # Last part (might be last name or suffix)
        
        # Get name variations for both filer and politician first names
        filer_first_variations = get_name_variations(filer_first)
        politician_first_variations = get_name_variations(first_name)
        
        # Check if any variation of filer first name matches any variation of politician first name
        first_name_matches = any(fv in politician_first_variations for fv in filer_first_variations)
        
        # Check if last part matches last name
        if first_name_matches and filer_last == last_name:
            logger.debug(f"✅ First+last match (with name variations): '{filer_name}' -> '{politician['name']}' (filer_first='{filer_first}', politician_first='{first_name}')")
            return 1.0
        
        # Check if last part matches last name with suffix
        if suffix and filer_last == f"{last_name}{suffix}":
            if first_name_matches:
                logger.debug(f"✅ First+last+suffix match (with name variations): '{filer_name}' -> '{politician['name']}'")
                return 1.0
    
    # Pattern 2: "First Middle Last" or "First M. Last" or "First M Last"
    # Also handles name shortenings: "Michael A. Collins" should match "Mike Allen Collins"
    if len(filer_parts) >= 3:
        filer_first = filer_parts[0]
        filer_middle = filer_parts[1]
        filer_last = filer_parts[-1]
        
        # Get name variations for both filer and politician first names
        filer_first_variations = get_name_variations(filer_first)
        politician_first_variations = get_name_variations(first_name)
        first_name_matches = any(fv in politician_first_variations for fv in filer_first_variations)
        
        # Check if matches with middle name variations
        if first_name_matches and filer_last == last_name:
            # Generate middle name variations
            middle_variations = normalize_middle_name(middle_name)
            for middle_var in middle_variations:
                middle_var_lower = middle_var.lower().strip()
                if middle_var_lower and filer_middle == middle_var_lower:
                    logger.debug(f"✅ First+middle+last match (with name variations): '{filer_name}' -> '{politician['name']}'")
                    return 1.0
                # Also check if middle is just first letter
                if middle_var_lower and len(middle_var_lower) == 1 and filer_middle == middle_var_lower:
                    logger.debug(f"✅ First+middle+last match (with name variations, middle initial): '{filer_name}' -> '{politician['name']}'")
                    return 1.0
                # Check with period
                if middle_var_lower and filer_middle == middle_var_lower.replace('.', ''):
                    logger.debug(f"✅ First+middle+last match (with name variations, middle with period): '{filer_name}' -> '{politician['name']}'")
                    return 1.0
    
    # Pattern 3: "Last, First" or "Last, First Middle"
    # Also handles name shortenings: "Collins, Michael" should match "Mike Collins"
    if ',' in filer_normalized:
        parts = [p.strip() for p in filer_normalized.split(',')]
        if len(parts) >= 2:
            filer_last_part = parts[0]
            filer_first_part = parts[1].split()[0] if parts[1] else ''
            
            # Get name variations for both filer and politician first names
            filer_first_variations = get_name_variations(filer_first_part)
            politician_first_variations = get_name_variations(first_name)
            first_name_matches = any(fv in politician_first_variations for fv in filer_first_variations)
            
            if filer_last_part == last_name and first_name_matches:
                # Check middle name if present
                if len(parts[1].split()) > 1:
                    filer_middle_part = parts[1].split()[1]
                    middle_variations = normalize_middle_name(middle_name)
                    for middle_var in middle_variations:
                        if filer_middle_part == middle_var.lower().strip():
                            logger.debug(f"✅ Last,First+Middle match (with name variations): '{filer_name}' -> '{politician['name']}'")
                            return 1.0
                else:
                    # No middle name in filer name - still match if politician has no middle or we're flexible
                    logger.debug(f"✅ Last,First match (with name variations): '{filer_name}' -> '{politician['name']}'")
                    return 0.95
    
    # Calculate similarity using SequenceMatcher on full names
    similarity = SequenceMatcher(None, filer_normalized, politician_full).ratio()
    
    # Also check if names are subsets (e.g., "John Doe" vs "John A. Doe" or "Laurel Lee" vs "Laurel M. Lee")
    if filer_normalized in politician_full or politician_full in filer_normalized:
        similarity = max(similarity, 0.9)
    
    # Try matching first + last name only (ignore middle) - this is critical for "Laurel Lee" -> "Laurel Frances Lee"
    # Also handles name shortenings: "Michael Collins" -> "Mike Collins"
    # This should return 1.0 since it's a perfect match (just missing middle name or using nickname)
    if len(filer_parts) >= 2:
        filer_first = filer_parts[0]
        filer_last = filer_parts[-1]
        
        # Get name variations for both filer and politician first names
        filer_first_variations = get_name_variations(filer_first)
        politician_first_variations = get_name_variations(first_name)
        first_name_matches = any(fv in politician_first_variations for fv in filer_first_variations)
        
        if first_name_matches and filer_last == last_name and first_name and last_name:
            logger.debug(f"✅ First+last name match (ignoring middle, with name variations): '{filer_name}' -> '{politician['name']}' (score: 1.0)")
            return 1.0  # Return 1.0 for perfect first+last match (even if middle name is missing or using nickname)
    
    # Log if we have a good match but it's below threshold
    if similarity >= 0.8 and similarity < NAME_MATCH_THRESHOLD:
        logger.debug(f"⚠️ Good match but below threshold: '{filer_name}' -> '{politician['name']}' (score: {similarity:.3f}, threshold: {NAME_MATCH_THRESHOLD})")
    
    return similarity

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
    state = politician.get('state', '').strip()
    if not state:
        return None
    
    position = politician.get('position', '').strip()
    district = politician.get('district', '').strip()
    
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

def find_matching_politician(filer_name: str, politicians: List[Dict[str, Any]], position_filter: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """
    Find best matching politician for a filer name
    
    Args:
        filer_name: Name from form
        politicians: List of politician dicts
        position_filter: Optional position to filter by (e.g., "House", "Senate")
        
    Returns:
        Best matching politician dict or None
    """
    best_match = None
    best_score = 0.0
    
    for politician in politicians:
        # Filter by position if specified
        if position_filter:
            politician_position = politician.get('position', '').strip()
            if politician_position != position_filter:
                continue  # Skip politicians that don't match the position filter
        
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

    
def parse_house_ptr_with_textract(s3_key: str) -> List[Dict[str, Any]]:
    """
    Parse House PTR PDF using PyPDF to extract trade data
    
    Args:
        s3_key: S3 key of the House PTR PDF
        
    Returns:
        List of trade dicts with filerName, securitySymbol, transactionDate, amount, etc.
    """
    logger.info(f"📄 Parsing House PTR with PyPDF: {s3_key}")
    
    trades = []
    
    try:
        # Download PDF from S3
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
        pdf_content = response['Body'].read()
        
        # Validate PDF format
        if not pdf_content.startswith(b'%PDF'):
            error_msg = f"File {s3_key} is not a valid PDF (doesn't start with %PDF)"
            logger.error(f"❌ {error_msg}")
            raise Exception(error_msg)
        
        # Check minimum PDF size (very small files might be corrupted)
        if len(pdf_content) < 100:
            error_msg = f"File {s3_key} is too small ({len(pdf_content)} bytes) - likely corrupted"
            logger.error(f"❌ {error_msg}")
            raise Exception(error_msg)
        
        # Get asset codes mapping (cached at module level)
        asset_codes = get_asset_codes_mapping()
        
        # Parse PDF with PyPDF
        logger.info(f"📄 Using PyPDF to parse House PTR PDF ({len(pdf_content)} bytes)...")
        
        pdf_file = BytesIO(pdf_content)
        pdf_reader = PdfReader(pdf_file)
        page_count = len(pdf_reader.pages)
        
        logger.info(f"📄 PDF has {page_count} page(s)")
        
        # Extract text from all pages
        full_text = ''
        text_lines = []
        page_texts = []  # Store text per page for logging
        
        for page_num, page in enumerate(pdf_reader.pages):
            try:
                page_text = page.extract_text()
                if page_text:
                    page_texts.append(page_text)
                    full_text += page_text + '\n'
                    # Split into lines for easier parsing
                    page_lines = page_text.split('\n')
                    text_lines.extend([line.strip() for line in page_lines if line.strip()])
                else:
                    page_texts.append('')  # Empty page
            except Exception as e:
                logger.warning(f"⚠️ Error extracting text from page {page_num + 1}: {e}")
                page_texts.append('')  # Error page
                continue
        
        # Comprehensive logging of extracted text for debugging - per page
        logger.info(f"📊 PyPDF Output Summary for {s3_key}:")
        logger.info(f"   Total pages: {page_count}")
        logger.info(f"   Text lines extracted: {len(text_lines)}")
        logger.info(f"   Full text length: {len(full_text)} characters")
        
        # Log each page separately
        for page_num, page_text in enumerate(page_texts):
            page_lines = [line.strip() for line in page_text.split('\n') if line.strip()]
            logger.info(f"   Page {page_num + 1} ({len(page_lines)} lines, {len(page_text)} chars):")
            # Log all lines from this page
            for i, line in enumerate(page_lines):
                logger.info(f"      [{i+1}] {line}")
            if not page_text.strip():
                logger.info(f"      (empty page)")
        
        logger.info(f"   Full text (first 1000 chars): {full_text[:1000]}")
        
        # Extract filer name from House PTR
        filer_name = None
        
        # Look for FILER INFORMATION section
        # Pattern: "Name:" followed by name, stopping before "Status:"
        # Example: "Name: Hon. Virginia Foxx Status: Member"
        name_patterns = [
            r'Name:\s*(?:Hon\.?\s+)?([A-Z][a-z]+(?:\s+[A-Z][a-z.]+)+?)(?:\s+Status:)',  # Stop before "Status:"
            r'Name:\s*(?:Hon\.?\s+)?([A-Z][a-z]+(?:\s+[A-Z][a-z.]+)+)',  # Fallback without Status
        ]
        
        for pattern in name_patterns:
            match = re.search(pattern, full_text, re.IGNORECASE | re.MULTILINE)
            if match:
                filer_name = match.group(1).strip()
                # Remove "Hon." prefix if present
                filer_name = re.sub(r'^Hon\.?\s+', '', filer_name, flags=re.IGNORECASE).strip()
                # Remove any trailing "Status" that might have been captured
                filer_name = re.sub(r'\s+Status\s*$', '', filer_name, flags=re.IGNORECASE).strip()
                logger.info(f"✅ Extracted filer name from FILER INFORMATION: {filer_name}")
                break
        
        # If not found, try other patterns
        if not filer_name:
            house_patterns = [
                r'Hon\.?\s+([A-Z][a-z]+(?:\s+[A-Z][a-z.]+)+)',
                r'The Honorable\s+([A-Z][a-z]+(?:\s+[A-Z][a-z.]+)+)',
            ]
            for pattern in house_patterns:
                match = re.search(pattern, full_text)
                if match:
                    filer_name = match.group(1).strip()
                    filer_name = re.sub(r'\b(Honorable|Hon\.?|Representative|Rep\.?)\b', '', filer_name, flags=re.IGNORECASE).strip()
                    break
        
        if not filer_name:
            logger.warning(f"⚠️ Could not extract filer name from House PTR: {s3_key}")
        
        # Extract filing date from e-signature at bottom
        filing_date = None
        
        # Look for "Digitally Signed: [Name] [Date]" pattern
        signature_patterns = [
            r'Digitally Signed:\s+[^,]+,\s*(\d{1,2}/\d{1,2}/\d{4})',  # "Digitally Signed: Name, MM/DD/YYYY"
            r'Digitally Signed:\s+[^\d]+(\d{1,2}/\d{1,2}/\d{4})',    # "Digitally Signed: Name MM/DD/YYYY"
            r'(?:signed|signature)[:\s]+[^,]+,\s*(\d{1,2}/\d{1,2}/\d{4})',
            r'(?:signed|signature)[:\s]+[^\d]+(\d{1,2}/\d{1,2}/\d{4})',
        ]
        
        # Search in reverse order (bottom of document) for signature date
        for pattern in signature_patterns:
            matches = list(re.finditer(pattern, full_text, re.IGNORECASE))
            if matches:
                # Take the last match (likely at bottom near signature)
                match = matches[-1]
                date_str = match.group(1)
                try:
                    filing_date = datetime.strptime(date_str, '%m/%d/%Y').strftime('%Y-%m-%d')
                    logger.info(f"✅ Extracted filing date from signature: {filing_date}")
                    break
                except ValueError:
                    continue
        
        # Fallback: any date in MM/DD/YYYY format at the end of the document
        if not filing_date:
            date_matches = list(re.finditer(r'(\d{1,2}/\d{1,2}/\d{4})', full_text))
            if date_matches:
                # Take the last date found (likely signature date)
                date_str = date_matches[-1].group(1)
                try:
                    filing_date = datetime.strptime(date_str, '%m/%d/%Y').strftime('%Y-%m-%d')
                    logger.info(f"✅ Extracted filing date (fallback): {filing_date}")
                except ValueError:
                    pass
        
        # Parse transaction tables using structured approach
        # House PTR format: Trade data comes FIRST, then metadata at the END
        # Structure:
        #   Trade data line(s): Asset name + (optional ticker) + [asset type] + Transaction type + Dates + Amount
        #   F S: New (Filing Status - marks END of trade)
        #   S O: [account name] (Subholding Of - marks END of trade)
        #   D: [description] (optional - Description, e.g., call options details)
        #
        # Asset parsing:
        #   - Asset name = text before parentheses (or before brackets if no ticker)
        #   - Ticker = characters in parentheses (optional, can be null for bonds)
        #   - Asset type = characters in brackets (always present)
        
        # Split into lines for processing
        lines = [line.strip() for line in full_text.split('\n') if line.strip()]
        
        # Find all trade entries by looking for trade data lines (with asset type and dates/amount)
        # Then collect the following F S and S O lines as metadata
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            
            # Skip header lines
            line_lower = line.lower()
            if any(skip in line_lower for skip in ['ownerasset', 'transaction', 'notification', 'cap. gains', 'filing id', 'digitally signed', 'certify', 'id owner', 'type date', 'dateamount', 'gains >', '$200?']):
                i += 1
                continue
            
            # Skip metadata lines (F S, S O, D) - we'll collect these after finding trade data
            if re.match(r'^(F\s+S:|S\s+O:|D:)\s*', line, re.IGNORECASE):
                i += 1
                continue
                
            # Look for trade data line - has asset type in brackets and dates/amount
            has_asset_type = bool(re.search(r'\[([A-Z]{2,3})\]', line))
            has_dates = bool(re.search(r'\d{1,2}/\d{1,2}/\d{4}', line))
            has_amount = bool(re.search(r'\$\d+', line))
            has_owner_code = bool(re.match(r'^[A-Z]{1,3}\s+', line))
            
            # Trade data line should have asset type and (dates or amount or owner code)
            if not has_asset_type or not (has_dates or has_amount or has_owner_code):
                i += 1
                continue
            
            # Found potential trade data - collect it and following metadata
            logger.debug(f"   Found trade data at line {i+1}: {line[:100]}")
            
            # Initialize metadata - metadata appears at the END of trade entries, so we'll look ahead
            trade_metadata = {
                'filing_status': None,
                'subholding_of': None,
                'description': None
            }
            
            # Look backwards to collect asset name if it's on previous line(s)
            # Sometimes the asset name is on a line before the asset type bracket
            # NOTE: We skip metadata lines here - metadata appears at the END of trades, not before
            trade_data_lines = []
            lookback_start = max(0, i - 3)  # Look back up to 3 lines
            for back_idx in range(lookback_start, i):
                back_line = lines[back_idx].strip()
                if not back_line:
                    continue
                # Skip if it's metadata or header - metadata is at the END of trades, not before
                if re.match(r'^(F\s+S:|S\s+O:|D:)\s*', back_line, re.IGNORECASE):
                    continue
                if any(header in back_line.lower() for header in ['ownerasset', 'transaction', 'notification', 'id owner', 'type date', 'dateamount', 'filing id']):
                    continue
                # If this line doesn't have asset type bracket or dates/amount, it might be asset name
                if not re.search(r'\[([A-Z]{2,3})\]', back_line) and not re.search(r'\d{1,2}/\d{1,2}/\d{4}', back_line):
                    # Check if it has owner code - if so, it's likely part of asset name
                    if re.match(r'^[A-Z]{1,3}\s+', back_line):
                        trade_data_lines.append(back_line)
                    # Or if it looks like part of an asset name (has ticker or common words)
                    elif re.search(r'\([A-Z]{1,5}\)', back_line) or any(word in back_line.lower() for word in ['stock', 'common', 'corporation', 'inc', 'ltd', 'company', 'shares']):
                        trade_data_lines.append(back_line)
            
            # Add the current line (which has asset type and dates/amount)
            trade_data_lines.append(line)
            j = i + 1
            
            # Collect continuation lines (multi-line asset names, amount on separate line)
            # Stop when we hit metadata (F S, S O, D) or next trade
            metadata_start_idx = None  # Track where metadata starts
            while j < len(lines):
                next_line = lines[j].strip()
                if not next_line:
                    j += 1
                    continue
                
                # Stop if we hit metadata (F S, S O, D) - these mark the end of trade data
                # But remember this position so we can collect the metadata
                if re.match(r'^(F\s+S:|S\s+O:|D:)\s*', next_line, re.IGNORECASE):
                    metadata_start_idx = j  # Remember where metadata starts
                    break
                
                # Stop if we hit next trade data (has asset type and dates/amount and owner code)
                if re.search(r'\[([A-Z]{2,3})\]', next_line) and re.match(r'^[A-Z]{1,3}\s+', next_line):
                    break
                
                # Stop if we hit a header
                if any(header in next_line.lower() for header in ['ownerasset', 'transaction', 'notification', 'id owner', 'type date', 'dateamount', 'filing id']):
                    break
                
                # If this is amount continuation (just starts with $ and no dates)
                if re.match(r'^\$\d+', next_line) and not re.search(r'\d{1,2}/\d{1,2}/\d{4}', next_line):
                    trade_data_lines.append(next_line)
                    j += 1
                    break
                
                # If this could be part of multi-line asset name (no asset type bracket yet)
                if not re.search(r'\[([A-Z]{2,3})\]', next_line):
                    # Check if it has owner code - might be start of next trade
                    if re.match(r'^[A-Z]{1,3}\s+', next_line) and j > i + 3:
                        # Too far from start, probably next trade
                        break
                    trade_data_lines.append(next_line)
                    j += 1
                    continue
        
                # If we already have asset type and dates/amount in collected lines, this is probably next trade
                collected_text = ' '.join(trade_data_lines)
                if re.search(r'\[([A-Z]{2,3})\]', collected_text) and re.search(r'\d{1,2}/\d{1,2}/\d{4}', collected_text) and re.search(r'\$\d+', collected_text):
                    # We have complete trade data, stop
                    break
            
                j += 1
            
            # Combine trade data lines into single text for parsing
            trade_text = ' '.join(trade_data_lines)
            
            # Look ahead for F S, S O, and D lines
            # IMPORTANT: Metadata appears at the END of trade entries, so it comes AFTER the trade data
            # When we see F S, S O, or D, it applies to the trade data ABOVE it
            # Metadata can span across pages, so we need to look further ahead (up to 15 lines to handle page breaks)
            # Also check for full format: "FILING STATUS:", "SUBHOLDING OF:", "DESCRIPTION:"
            # Start from where metadata begins (if we found it) or from j (end of trade data)
            k = metadata_start_idx if metadata_start_idx is not None else j
            look_ahead_limit = 15  # Increased limit to handle cross-page metadata (page breaks can add extra lines)
            start_pos = metadata_start_idx if metadata_start_idx is not None else j
            end_limit = start_pos + look_ahead_limit
            
            logger.debug(f"   Looking for metadata starting from line {k+1} (metadata_start_idx={metadata_start_idx}, j={j}) up to line {end_limit}")
            
            while k < len(lines) and k < end_limit:
                meta_line = lines[k].strip()
                if not meta_line:
                    k += 1
                    continue
                
                # Skip headers that might appear between pages
                if any(header in meta_line.lower() for header in ['id ownerasset', 'transaction', 'type date', 'dateamount', 'cap. gains', '$200?']):
                    k += 1
                    continue
                
                # Check for full format first (FILING STATUS:, SUBHOLDING OF:, DESCRIPTION:)
                if re.match(r'^FILING\s+STATUS:\s*', meta_line, re.IGNORECASE):
                    # Extract value after "FILING STATUS:"
                    value = re.sub(r'^FILING\s+STATUS:\s*', '', meta_line, flags=re.IGNORECASE).strip()
                    trade_metadata['filing_status'] = value if value else 'New'
                    logger.info(f"   ✅ Found filing_status metadata at line {k+1}: '{trade_metadata['filing_status']}'")
                elif re.match(r'^F\s+S:\s*', meta_line, re.IGNORECASE):
                    # Extract value after "F S:" or "F S: New"
                    value = re.sub(r'^F\s+S:\s*', '', meta_line, flags=re.IGNORECASE).strip()
                    trade_metadata['filing_status'] = value if value else 'New'
                    logger.info(f"   ✅ Found filing_status metadata at line {k+1}: '{trade_metadata['filing_status']}'")
                elif re.match(r'^SUBHOLDING\s+OF:\s*', meta_line, re.IGNORECASE):
                    # Extract value after "SUBHOLDING OF:"
                    value = re.sub(r'^SUBHOLDING\s+OF:\s*', '', meta_line, flags=re.IGNORECASE).strip()
                    trade_metadata['subholding_of'] = value
                    logger.info(f"   ✅ Found subholding_of metadata at line {k+1}: '{trade_metadata['subholding_of']}'")
                elif re.match(r'^S\s+O:\s*', meta_line, re.IGNORECASE):
                    # Extract value after "S O:"
                    value = re.sub(r'^S\s+O:\s*', '', meta_line, flags=re.IGNORECASE).strip()
                    trade_metadata['subholding_of'] = value
                    logger.info(f"   ✅ Found subholding_of metadata at line {k+1}: '{trade_metadata['subholding_of']}'")
                elif re.match(r'^DESCRIPTION:\s*', meta_line, re.IGNORECASE):
                    # Extract value after "DESCRIPTION:"
                    value = re.sub(r'^DESCRIPTION:\s*', '', meta_line, flags=re.IGNORECASE).strip()
                    trade_metadata['description'] = value
                    logger.info(f"   ✅ Found description metadata at line {k+1}: '{trade_metadata['description']}'")
                elif re.match(r'^D:\s*', meta_line, re.IGNORECASE):
                    # Extract value after "D:"
                    value = re.sub(r'^D:\s*', '', meta_line, flags=re.IGNORECASE).strip()
                    trade_metadata['description'] = value
                    logger.info(f"   ✅ Found description metadata at line {k+1}: '{trade_metadata['description']}'")
                elif re.search(r'\[([A-Z]{2,3})\]', meta_line) and re.match(r'^[A-Z]{1,3}\s+', meta_line):  # Next trade data (has asset type and owner code), stop
                    # Only stop if it's actually a new trade (has both asset type and owner code)
                    logger.debug(f"   Stopping metadata search at line {k+1} - found next trade data: {meta_line[:50]}")
                    break
                k += 1
            
            # Log extracted metadata for debugging
            if trade_metadata['filing_status'] or trade_metadata['subholding_of'] or trade_metadata['description']:
                logger.info(f"   ✅ Extracted metadata for trade at line {i+1}: filing_status='{trade_metadata['filing_status']}', subholding_of='{trade_metadata['subholding_of']}', description='{trade_metadata['description']}'")
            else:
                logger.warning(f"   ⚠️ No metadata found for trade at line {i+1} (looked ahead from line {k} to {end_limit}, metadata_start_idx={metadata_start_idx})")
            
            # Parse the collected trade text
            owner = None
            transaction_type = None
            transaction_date = None
            amount = None
            amount_min = None
            amount_max = None
            security_name = None
            security_symbol = None
            asset_type = None
            
            # Extract owner (optional, first 1-3 uppercase letters at start)
            owner_match = re.match(r'^([A-Z]{1,3})\s+', trade_text)
            if owner_match:
                owner = owner_match.group(1)
            
            # Extract transaction type (single letter: P, S, E, etc., usually after asset type bracket)
            # Format: [asset type]TransactionType [optional text like "(partial)"] Date
            # Examples: "[ST]S 12/20/2024", "[OP]P 12/20/2024", "[ST] S 12/20/2024", "[ST]S (partial) 12/27/2024"
            # The transaction type is the single letter AFTER the asset type bracket, before the date
            # There may be optional text like "(partial)" between the transaction type and date
            type_char = None
            
            # Primary pattern: [asset type]TransactionType [optional text] Date
            # Allow for optional text like "(partial)" between transaction type and date
            # Group 1 = asset type, Group 2 = transaction type
            type_match = re.search(r'\[([A-Z]{2,3})\]\s*([A-Z])(?:\s*\([^)]*\))?\s+\d{1,2}/\d{1,2}/\d{4}', trade_text)
            if type_match:
                asset_type_code = type_match.group(1)  # This is the asset type (ST, OP, etc.)
                type_char = type_match.group(2)  # This is the transaction type (P, S, E, etc.)
                logger.debug(f"   Extracted transaction type: '{type_char}' (asset type: '{asset_type_code}')")
            else:
                # Try pattern: ) [asset type]TransactionType [optional text] Date (when ticker and asset type are on same line)
                type_match = re.search(r'\)\s*\[([A-Z]{2,3})\]\s*([A-Z])(?:\s*\([^)]*\))?\s+\d{1,2}/\d{1,2}/\d{4}', trade_text)
                if type_match:
                    asset_type_code = type_match.group(1)
                    type_char = type_match.group(2)
                    logger.debug(f"   Extracted transaction type (pattern 2): '{type_char}' (asset type: '{asset_type_code}')")
                else:
                    # Try pattern: ) [asset type]TransactionType [optional text] Date (no space between bracket and type)
                    type_match = re.search(r'\)\s*\[([A-Z]{2,3})\]([A-Z])(?:\s*\([^)]*\))?\s+\d{1,2}/\d{1,2}/\d{4}', trade_text)
                    if type_match:
                        asset_type_code = type_match.group(1)
                        type_char = type_match.group(2)
                        logger.debug(f"   Extracted transaction type (pattern 3): '{type_char}' (asset type: '{asset_type_code}')")
                    else:
                        # Fallback: Try just letter before date (less reliable, but allow for optional text)
                        type_match = re.search(r'\s+([PS])(?:\s*\([^)]*\))?\s+\d{1,2}/\d{1,2}/\d{4}', trade_text)
                        if type_match:
                            type_char = type_match.group(1)
                            logger.debug(f"   Extracted transaction type (fallback): '{type_char}'")
            
            # Validate that type_char is actually a transaction type, not an asset type code
            if type_char:
                # Transaction types are single letters: P, S, E, etc.
                # Asset type codes are 2-3 letters: ST, OP, GS, etc.
                if len(type_char) > 1:
                    # This might be an asset type code, not a transaction type
                    logger.warning(f"   ⚠️ Extracted transaction type looks like asset type code: '{type_char}' from trade_text: {trade_text[:100]}")
                    type_char = None
            
            if type_char:
                if type_char == 'P':
                    transaction_type = 'Purchase'
                elif type_char == 'S':
                    transaction_type = 'Sale'
                elif type_char == 'E':
                    transaction_type = 'Exercise'
                else:
                    transaction_type = type_char  # Keep as-is for other types
            
            # Extract dates (MM/DD/YYYY format)
            date_matches = list(re.finditer(r'(\d{1,2}/\d{1,2}/\d{4})', trade_text))
            if date_matches and len(date_matches) >= 1:
                try:
                    transaction_date = datetime.strptime(date_matches[0].group(1), '%m/%d/%Y').strftime('%Y-%m-%d')
                except ValueError:
                    pass
            
            # Extract amount (range like "$1,001 - $15,000" or "$1,001 $15,000")
            amount_match = re.search(r'\$([\d,]+)\s*[-–]?\s*\$?([\d,]+)', trade_text)
            if amount_match:
                try:
                    min_str = amount_match.group(1).replace(',', '')
                    max_str = amount_match.group(2).replace(',', '')
                    amount_min = float(min_str)
                    amount_max = float(max_str)
                    amount = (amount_min + amount_max) / 2
                except ValueError:
                    pass
                    
            # Extract asset information according to the structured format
            # Format: Asset name + (optional ticker) + [asset type] + Transaction type + Dates + Amount
            # Rules:
            #   - Asset name = text before parentheses (or before brackets if no ticker)
            #   - Ticker = characters in parentheses (optional, can be null for bonds)
            #   - Asset type = characters in brackets (always present)
            
            # First, remove any metadata that might have been captured (F S, S O, D, C: Ref)
            # Remove from anywhere in the text, not just the end
            trade_text = re.sub(r'\s*F\s+S:\s*[^\s]*.*?(?=\s|$)', '', trade_text, flags=re.IGNORECASE)
            trade_text = re.sub(r'\s*S\s+O:\s*[^S]*?(?=\s*[A-Z]{1,3}\s+|\[|$)', '', trade_text, flags=re.IGNORECASE)
            trade_text = re.sub(r'\s*D:\s*.*?(?=\s*[A-Z]{1,3}\s+|\[|$)', '', trade_text, flags=re.IGNORECASE)
            trade_text = re.sub(r'\s*C:\s*Ref:\s*[^\s]*.*?(?=\s|$)', '', trade_text, flags=re.IGNORECASE)
            
            # Remove owner code if present (JT, SP, etc.)
            asset_text = trade_text
            if owner:
                asset_text = re.sub(r'^' + re.escape(owner) + r'\s+', '', asset_text)
            
            # Extract asset type code (in brackets) - always present
            asset_code_match = re.search(r'\[([A-Z]{2,3})\]', asset_text)
            if asset_code_match:
                code = asset_code_match.group(1)
                if code in asset_codes:
                    asset_type = asset_codes[code]
                # Find position of asset type bracket
                asset_type_pos = asset_text.find(asset_code_match.group(0))
            else:
                asset_type_pos = len(asset_text)  # If no asset type found, use end of text
            
            # Extract ticker symbol (in parentheses) - optional
            # Look for ticker BEFORE the asset type bracket
            ticker_match = None
            if asset_type_pos < len(asset_text):
                # Search for ticker in the portion before asset type
                text_before_asset_type = asset_text[:asset_type_pos]
                ticker_match = re.search(r'\(([A-Z]{1,5})\)', text_before_asset_type)
            
            if ticker_match:
                security_symbol = ticker_match.group(1)
                ticker_pos = asset_text.find(ticker_match.group(0))
                # Asset name is everything before the ticker parentheses
                security_name = asset_text[:ticker_pos].strip()
            else:
                # No ticker found - asset name is everything before the asset type bracket
                security_name = asset_text[:asset_type_pos].strip()
            
            # Clean up security name - remove transaction data that might have been captured
            if security_name:
                # Remove asset type codes in brackets (e.g., [ST], [OP])
                security_name = re.sub(r'\[([A-Z]{2,3})\]', '', security_name)
                # Remove transaction type letters (P, S, E, etc.) that might be anywhere
                security_name = re.sub(r'\s+[PS]\s+', ' ', security_name)
                security_name = re.sub(r'\s+[PS]\s*$', '', security_name)
                security_name = re.sub(r'^\s*[PS]\s+', '', security_name)
                # Remove owner codes that might have been missed
                security_name = re.sub(r'^[A-Z]{1,3}\s+', '', security_name)
                # Remove dates
                security_name = re.sub(r'\d{1,2}/\d{1,2}/\d{4}', '', security_name)
                # Remove amounts
                security_name = re.sub(r'\$\d+[\d,]*\s*[-–]?\s*\$?\d+[\d,]*', '', security_name)
                # Remove "(partial)" if present
                security_name = re.sub(r'\s*\(partial\)\s*', '', security_name, flags=re.IGNORECASE)
                # Remove metadata markers (F S, S O, D, C: Ref) that might have been missed
                security_name = re.sub(r'\s*F\s+S:\s*[^\s]*', '', security_name, flags=re.IGNORECASE)
                security_name = re.sub(r'\s*S\s+O:\s*[^S]*', '', security_name, flags=re.IGNORECASE)
                security_name = re.sub(r'\s*D:\s*[^D]*', '', security_name, flags=re.IGNORECASE)
                security_name = re.sub(r'\s*C:\s*Ref:\s*[^\s]*', '', security_name, flags=re.IGNORECASE)
                # Remove common account names that might have been captured
                security_name = re.sub(r'\s*Morgan\s+Stanley[^S]*', '', security_name, flags=re.IGNORECASE)
                security_name = re.sub(r'\s*Account\s*#?\s*\d*', '', security_name, flags=re.IGNORECASE)
                # Remove any trailing commas or periods
                security_name = re.sub(r'[,.]\s*$', '', security_name)
                # Normalize whitespace (multiple spaces/newlines to single space)
                security_name = re.sub(r'\s+', ' ', security_name)
                security_name = security_name.strip()
                
                # Only create trade if we have minimum required fields
                if transaction_date and (security_name or security_symbol) and transaction_type and amount:
                    trade = {
                        'filerName': filer_name,
                        'filingDate': filing_date,
                        'transactionDate': transaction_date,
                        'securityName': security_name,
                        'securitySymbol': security_symbol,
                        'assetType': asset_type,
                        'transactionType': transaction_type,
                        'amount': amount,
                        'amountMin': amount_min,
                        'amountMax': amount_max,
                    'owner': owner,
                        'formType': 'house_ptr',
                    'source': 'house',
                    'filingMetadata': {
                        'filingStatus': trade_metadata.get('filing_status'),
                        'subholdingOf': trade_metadata.get('subholding_of'),
                        'description': trade_metadata.get('description')
                    }
                    }
                    trades.append(trade)
                logger.info(f"   ✅ Extracted trade: {security_symbol or security_name} - {transaction_type} - ${amount_min}-${amount_max} (name: '{security_name}')")
            else:
                # Log why trade wasn't created for debugging
                missing = []
                if not transaction_date:
                    missing.append('transaction_date')
                if not (security_name or security_symbol):
                    missing.append('security_name/symbol')
                if not transaction_type:
                    missing.append('transaction_type')
                if not amount:
                    missing.append('amount')
                logger.info(f"   ⚠️ Skipped potential trade (missing: {', '.join(missing)}): {trade_text[:150]}")
            
            i = j  # Move to next potential trade (skip the metadata lines we just processed)
        
        logger.info(f"✅ Extracted {len(trades)} trades from House PTR: {s3_key}")
        
    except Exception as e:
        # Log full traceback for errors
        logger.error(f"❌ Error parsing House PTR {s3_key}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise  # Re-raise to fail completely
    
    return trades

def check_if_filing_processed(s3_key: str) -> bool:
    """
    Check if a House PTR filing has already been processed by checking DynamoDB
    
    Args:
        s3_key: S3 key of the House PTR PDF (e.g., "trades/house/2025/20033394.pdf")
        
    Returns:
        True if filing has been processed, False otherwise
    """
    if not DYNAMODB_TABLE_NAME:
        logger.warning("⚠️ DYNAMODB_TABLE_NAME not set - skipping duplicate check")
        return False
    
    try:
        # Extract UUID/filename from S3 key
        # Format: "trades/house/2025/20033394.pdf" -> "20033394"
        filename = s3_key.split('/')[-1]  # Get "20033394.pdf"
        uuid = filename.replace('.pdf', '').replace('.PDF', '')  # Get "20033394"
        
        if not uuid or not uuid.isdigit():
            logger.warning(f"⚠️ Could not extract valid UUID from S3 key: {s3_key}")
            return False
        
        logger.info(f"🔍 Checking if House PTR {uuid} has already been processed...")
        
        # Get DynamoDB table
        table = dynamodb.Table(DYNAMODB_TABLE_NAME)
        
        # Scan for tradeIds that contain this UUID
        # House PTR tradeIds likely contain the UUID (e.g., "house_ptr_20033394_..." or "trade_2025-01-15_house_20033394")
        # Use a filter expression to check if tradeId contains the UUID
        response = table.scan(
            FilterExpression='contains(tradeId, :uuid) AND formType = :formType',
            ExpressionAttributeValues={
                ':uuid': uuid,
                ':formType': 'house_ptr'
            },
            Limit=1  # We only need to know if at least one exists
        )
        
        if response.get('Items') and len(response.get('Items', [])) > 0:
            logger.info(f"✅ House PTR {uuid} has already been processed (found {len(response['Items'])} existing trade(s))")
            return True
        
        logger.info(f"📋 House PTR {uuid} has not been processed yet")
        return False
        
    except Exception as e:
        logger.warning(f"⚠️ Error checking if filing was processed: {e}. Proceeding with processing.")
        return False
    
def match_house_ptr_trades(s3_key: str, politicians: List[Dict[str, Any]], skip_duplicate_check: bool = False) -> Dict[str, Any]:
    """
    Parse House PTR PDF, extract trades, and match to politicians
    
    Args:
        s3_key: S3 key of the House PTR PDF
        politicians: List of politician dicts for matching
        skip_duplicate_check: If True, skip the duplicate check (already done upstream)
        
    Returns:
        Dict with matchedTrades, unmatchedCount, s3Key, formType
    """
    logger.info(f"🔍 Matching House PTR trades: {s3_key}")
    
    # Check if this filing has already been processed (unless check was already done)
    if not skip_duplicate_check:
        if check_if_filing_processed(s3_key):
            logger.info(f"⏭️ Skipping House PTR {s3_key} - already processed")
            return {
                'matchedTrades': [],
                'unmatchedCount': 0,
                's3Key': s3_key,
                'formType': 'house_ptr',
                'skipped': True
            }
    
    matched_trades = []
    unmatched_count = 0
    
    try:
        # Parse House PTR with Textract
        trades = parse_house_ptr_with_textract(s3_key)
        
        if not trades:
            logger.warning(f"⚠️ No trades extracted from House PTR: {s3_key}")
            return {
                'matchedTrades': [],
                'unmatchedCount': 1,
                's3Key': s3_key,
                'formType': 'house_ptr'
            }
        
        # Get filing date from first trade (all trades from same filing have same filing date)
        filing_date = trades[0].get('filingDate') if trades else None
        if not filing_date:
            logger.warning(f"⚠️ No filing date found in trades from {s3_key}")
            filing_date = None
        
        # Extract UUID from S3 key (filename without extension)
        # Format: "trades/house/2025/20026533.pdf" -> "20026533"
        filename = s3_key.split('/')[-1]  # Get "20026533.pdf"
        uuid = filename.replace('.pdf', '').replace('.PDF', '') if filename else None
        
        if not uuid or not uuid.isdigit():
            logger.warning(f"⚠️ Could not extract valid UUID from S3 key: {s3_key}, using filename as fallback")
            uuid = filename.replace('.pdf', '').replace('.PDF', '') if filename else 'unknown'
        
        # Match each trade to a politician and format like Senate output
        # Filter politicians to only House members for House PTRs
        house_politicians = [p for p in politicians if p.get('position', '').strip() == 'House']
        logger.info(f"📋 Filtered to {len(house_politicians)} House politicians (out of {len(politicians)} total)")
        
        for idx, trade in enumerate(trades):
            filer_name = trade.get('filerName')
            if not filer_name:
                unmatched_count += 1
                continue
            
            # Only match to House politicians
            politician = find_matching_politician(filer_name, house_politicians, position_filter='House')
            if politician:
                # Convert transaction date to integer format (YYYYMMDD)
                transaction_date_str = trade.get('transactionDate')
                transaction_date_int = None
                if transaction_date_str:
                    try:
                        # Parse YYYY-MM-DD format
                        date_obj = datetime.strptime(transaction_date_str, '%Y-%m-%d')
                        transaction_date_int = int(date_obj.strftime('%Y%m%d'))
                    except (ValueError, TypeError):
                        # Try other formats
                        try:
                            if isinstance(transaction_date_str, int):
                                transaction_date_int = transaction_date_str
                            else:
                                date_obj = datetime.strptime(transaction_date_str, '%m/%d/%Y')
                                transaction_date_int = int(date_obj.strftime('%Y%m%d'))
                        except (ValueError, TypeError):
                            logger.warning(f"⚠️ Could not parse transaction date: {transaction_date_str}")
                
                # Map owner codes to full names
                owner = trade.get('owner')
                if owner:
                    owner_mapping = {
                        'SP': 'Spouse',
                        'S': 'Self',
                        'J': 'Joint',
                        'D': 'Dependent'
                    }
                    owner = owner_mapping.get(owner.upper(), owner)
                else:
                    owner = None
                
                # Build amountRange array
                amount_min = trade.get('amountMin')
                amount_max = trade.get('amountMax')
                amount_range = None
                if amount_min is not None and amount_max is not None:
                    amount_range = [amount_min, amount_max]
                
                # Generate tradeId: trade_{filing_date}_house_{uuid}_{index}
                trade_id = None
                if filing_date and uuid:
                    trade_id = f"trade_{filing_date}_house_{uuid}_{idx}"
                elif filing_date:
                    trade_id = f"trade_{filing_date}_house_{idx}"
                elif uuid:
                    trade_id = f"trade_house_{uuid}_{idx}"
                else:
                    # Fallback: use index only
                    trade_id = f"trade_house_{idx}"
                
                # Format state/district for the politician
                state_district = format_state_district(politician)
                
                # Format matched trade to match Senate output structure
                matched_trade = {
                    'tradeId': trade_id,
                    'politicianName': politician.get('name'),
                    'party': politician.get('party'),
                    'position': politician.get('position'),
                    'websiteUrl': politician.get('websiteUrl'),
                    'formType': 'house_ptr',
                    'filingDate': filing_date,
                    'transactionDate': transaction_date_int,
                    'transactionTime': None,
                    'securitySymbol': trade.get('securitySymbol'),
                    'securityName': trade.get('securityName'),
                    'assetType': trade.get('assetType'),
                    'transactionType': trade.get('transactionType'),
                    'order': None,
                    'shares': None,
                    'pricePerShare': None,
                    'amountMin': amount_min,
                    'amountMax': amount_max,
                    'amountRange': amount_range,
                    'exactAmount': None,
                    'owner': owner,
                    'comment': '--',
                    'formS3Key': s3_key,
                    'matchConfidence': politician.get('matchScore', 1.0),
                    'source': 'house',
                    'isUnparsed': False,
                    'requiresManualReview': False,
                    'stateDistrict': state_district,
                    'filingMetadata': trade.get('filingMetadata', {
                        'filingStatus': None,
                        'subholdingOf': None,
                        'description': None
                    })
                }
                matched_trades.append(matched_trade)
            else:
                unmatched_count += 1
                logger.warning(f"⚠️ Could not match filer '{filer_name}' to any politician")
        
        logger.info(f"✅ Matched {len(matched_trades)}/{len(trades)} trades from House PTR: {s3_key}")
        
    except Exception as e:
        # Check if this is an UnsupportedDocumentException - should fail completely
        error_type = type(e).__name__
        error_str = str(e)
        
        if 'UnsupportedDocumentException' in error_type or 'UnsupportedDocumentException' in error_str:
            # Re-raise to fail completely - already logged in parse_house_ptr_with_textract
            logger.error(f"❌ Failing completely due to UnsupportedDocumentException for {s3_key}")
            raise  # Fail completely instead of returning error dict
        else:
            # Log full traceback for unexpected errors and re-raise
            logger.error(f"❌ Error matching House PTR trades {s3_key}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            raise  # Re-raise to fail completely
    
    return {
        'matchedTrades': matched_trades,
        'unmatchedCount': unmatched_count,
        's3Key': s3_key,
        'formType': 'house_ptr',
        'source': 'house'
    }

def list_s3_files_by_prefix(prefix: str) -> List[str]:
    """
    List all S3 object keys with the given prefix
    
    Args:
        prefix: S3 key prefix (e.g., "trades/2024-01-15/sec/")
        
    Returns:
        List of S3 keys
    """
    keys = []
    paginator = s3_client.get_paginator('list_objects_v2')
    
    try:
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
            if 'Contents' in page:
                for obj in page['Contents']:
                    keys.append(obj['Key'])
        
        logger.info(f"📁 Found {len(keys)} files in S3 prefix: {prefix}")
        return keys
        
    except Exception as e:
        logger.error(f"❌ Error listing S3 files with prefix {prefix}: {e}")
        return []

def lambda_handler(event, context):
    """
    Lambda handler for matching House PTR trades
    
    Expected input from Step Functions:
    {
        "downloadResults": {
            "s3Keys": ["trades/house/2025/file1.pdf", ...],
            "count": 5,
            "success": true,
            "summary": "...",
            "folderName": "trades/house/2025"
        },
        "date": "2025-01-15",
        "source": "house"
    }
    
    Returns:
    {
        "matchedTrades": [...],
        "unmatchedCount": 0,
        "date": "2025-01-15",
        "source": "house"
    }
    """
    logger.info("🚀 Politician Trades House Matcher Lambda started")
    
    # Process House PTR matching
    download_results = event.get('downloadResults')
    if not download_results:
        raise ValueError("downloadResults not provided in event")
    
    logger.info("🔍 Processing House PTR matching from downloader output")
    return handle_house_ptr_matching(event, download_results)

def handle_house_ptr_matching(event: Dict[str, Any], download_results: Dict[str, Any]) -> Dict[str, Any]:
    """
    Handle House PTR matching by scanning entire year folder and processing only new files
    
    Args:
        event: Full event from Step Functions
        download_results: Output from downloader lambda (contains folderName/year info)
        
    Returns:
        Dict with matchedTrades and unmatchedCount
    """
    # Extract year from download_results or event
    folder_name = download_results.get('folderName', '')
    if folder_name:
        # Extract year from folder name like "trades/house/2025"
        year = folder_name.split('/')[-1] if '/' in folder_name else folder_name
    else:
        # Fallback: try to get year from date in event
        date = event.get('date') or download_results.get('date')
        if date:
            try:
                year = datetime.strptime(date, '%Y-%m-%d').strftime('%Y')
            except:
                year = datetime.now().strftime('%Y')
        else:
            year = datetime.now().strftime('%Y')
    
    logger.info(f"📁 Scanning House PTR folder for year: {year}")
    
    # List all PDFs in the year folder
    folder_prefix = f"trades/house/{year}/"
    logger.info(f"🔍 Listing all PDFs in S3 folder: {folder_prefix}")
    
    all_s3_keys = list_s3_files_by_prefix(folder_prefix)
    # Filter to only PDF files
    pdf_keys = [key for key in all_s3_keys if key.lower().endswith('.pdf')]
    
    logger.info(f"📋 Found {len(pdf_keys)} PDF files in folder")
    
    if not pdf_keys:
        logger.info("✅ No House PTR PDF files found in folder")
        return {
            "matchedTrades": [],
            "unmatchedCount": 0,
            "date": event.get('date')
        }
    
    # Check each file against DynamoDB to see if it's already processed
    logger.info(f"🔍 Checking {len(pdf_keys)} files against DynamoDB to find unprocessed ones...")
    unprocessed_keys = []
    
    for s3_key in pdf_keys:
        if not check_if_filing_processed(s3_key):
            unprocessed_keys.append(s3_key)
        else:
            logger.debug(f"⏭️ Skipping already processed file: {s3_key}")
    
    logger.info(f"📊 Found {len(unprocessed_keys)} unprocessed files out of {len(pdf_keys)} total files")
    
    if not unprocessed_keys:
        logger.info("✅ All House PTR files have already been processed")
        return {
            "matchedTrades": [],
            "unmatchedCount": 0,
            "date": event.get('date')
        }
    
    # Filter to specific test file for debugging (20026533.pdf for multi-page testing)
    test_uuid = "20026533"
    test_file = None
    for key in unprocessed_keys:
        if test_uuid in key:
            test_file = key
            break
    
    if test_file:
        logger.info(f"🧪 Testing with specific file: {test_file} (UUID: {test_uuid})")
        unprocessed_keys = [test_file]
    else:
        # Fallback: limit to first 1 file if test file not found
        if len(unprocessed_keys) > 1:
            logger.info(f"⚠️ Test file {test_uuid} not found, limiting to first 1 file (found {len(unprocessed_keys)} unprocessed files)")
            unprocessed_keys = unprocessed_keys[:1]
    
    # Load politician list for matching
    logger.info("📋 Loading politician list for matching")
    try:
        politicians = load_politician_list()
    except Exception as e:
        error_msg = f"❌ CRITICAL: Exception loading politician list from S3: {str(e)}. S3_BUCKET={S3_BUCKET}, Key=congress-legislators.csv"
        logger.error(error_msg)
        import traceback
        logger.error(traceback.format_exc())
        raise Exception(error_msg) from e
    
    if not politicians or len(politicians) == 0:
        # Include the last error if available
        error_details = f"Last error: {_last_politician_load_error}" if _last_politician_load_error else "No error logged (may be empty CSV or all rows skipped)"
        error_msg = f"❌ CRITICAL: Failed to load politician list from S3 - returned empty list. S3_BUCKET={S3_BUCKET}, Key=congress-legislators.csv. {error_details}. Check CloudWatch logs for detailed error information."
        logger.error(error_msg)
        raise Exception(error_msg)
    
    logger.info(f"✅ Loaded {len(politicians)} total politicians for matching")
    
    # Process only the unprocessed files with Textract
    logger.info(f"🔍 Processing {len(unprocessed_keys)} new House PTR files with Textract...")
    all_matched_trades = []
    total_unmatched = 0
    successful_files = 0
    failed_files = 0
    files_with_no_trades = 0
    files_with_no_filer_name = 0
    
    for s3_key in unprocessed_keys:
        try:
            match_result = match_house_ptr_trades(s3_key, politicians, skip_duplicate_check=True)
            matched_count = len(match_result.get('matchedTrades', []))
            unmatched_count = match_result.get('unmatchedCount', 0)
            
            all_matched_trades.extend(match_result.get('matchedTrades', []))
            total_unmatched += unmatched_count
            
            # Track statistics
            if matched_count > 0:
                successful_files += 1
            elif unmatched_count > 0:
                failed_files += 1
                # Check if it's because no trades were extracted or no filer name
                if match_result.get('unmatchedCount', 0) == 1 and matched_count == 0:
                    files_with_no_trades += 1
                elif 'filerName' in str(match_result):
                    files_with_no_filer_name += 1
        except Exception as e:
            # Check if this is an UnsupportedDocumentException - should fail completely
            error_type = type(e).__name__
            error_str = str(e)
            
            if 'UnsupportedDocumentException' in error_type or 'UnsupportedDocumentException' in error_str:
                # Fail completely - UnsupportedDocumentException means the file cannot be processed
                logger.error(f"❌ Failing completely: UnsupportedDocumentException for {s3_key}. This file cannot be processed by Textract.")
                raise  # Fail completely instead of continuing
            else:
                # For other errors, log and re-raise to fail completely
                logger.error(f"❌ Error processing House PTR {s3_key}: {e}")
                import traceback
                logger.error(traceback.format_exc())
                raise  # Fail completely
    
    logger.info(f"📊 Processing Summary:")
    logger.info(f"   ✅ Successfully matched: {successful_files} files")
    logger.info(f"   ❌ Failed/Unmatched: {failed_files} files")
    logger.info(f"   📄 Total trades matched: {len(all_matched_trades)}")
    logger.info(f"   ⚠️ Total unmatched trades/files: {total_unmatched}")
    
    return {
        "matchedTrades": all_matched_trades,
        "unmatchedCount": total_unmatched,
        "date": event.get('date')
    }


