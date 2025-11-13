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
from io import StringIO
from difflib import SequenceMatcher

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS clients
s3_client = boto3.client('s3')
textract_client = boto3.client('textract')
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


def extract_table_data_from_blocks(table_block: Dict[str, Any], all_blocks: List[Dict[str, Any]]) -> Optional[List[List[str]]]:
    """
    Extract table data from Textract blocks
    
    Args:
        table_block: The TABLE block from Textract
        all_blocks: All blocks from Textract response
        
    Returns:
        List of rows, each row is a list of cell values
    """
    try:
        # Create a mapping of block IDs to blocks for quick lookup
        block_map = {block['Id']: block for block in all_blocks}
        
        # Get all cells in the table
        cells = []
        for relationship in table_block.get('Relationships', []):
            if relationship.get('Type') == 'CHILD':
                for cell_id in relationship.get('Ids', []):
                    if cell_id in block_map:
                        cells.append(block_map[cell_id])
        
        if not cells:
            return None
        
        # Determine table dimensions
        max_row = max(cell.get('RowIndex', 0) for cell in cells)
        max_col = max(cell.get('ColumnIndex', 0) for cell in cells)
        
        # Create 2D array
        table = [['' for _ in range(max_col)] for _ in range(max_row)]
        
        # Fill in cell values
        for cell in cells:
            row_idx = cell.get('RowIndex', 1) - 1
            col_idx = cell.get('ColumnIndex', 1) - 1
            
            # Extract text from cell
            cell_text = ''
            for relationship in cell.get('Relationships', []):
                if relationship.get('Type') == 'CHILD':
                    for child_id in relationship.get('Ids', []):
                        if child_id in block_map:
                            child_block = block_map[child_id]
                            if child_block.get('BlockType') == 'WORD':
                                cell_text += child_block.get('Text', '') + ' '
            
            table[row_idx][col_idx] = cell_text.strip()
        
        return table
        
    except Exception as e:
        logger.error(f"❌ Error extracting table data: {e}")
        return None
    
def parse_house_ptr_with_textract(s3_key: str) -> List[Dict[str, Any]]:
    """
    Parse House PTR PDF using AWS Textract to extract trade data
    
    Args:
        s3_key: S3 key of the House PTR PDF
        
    Returns:
        List of trade dicts with filerName, securitySymbol, transactionDate, amount, etc.
    """
    logger.info(f"📄 Parsing House PTR with Textract: {s3_key}")
    
    trades = []
    
    try:
        # Download PDF from S3
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
        pdf_content = response['Body'].read()
        
        # Validate PDF format before sending to Textract
        if not pdf_content.startswith(b'%PDF'):
            error_msg = f"File {s3_key} is not a valid PDF (doesn't start with %PDF)"
            logger.error(f"❌ {error_msg}")
            raise Exception(error_msg)
        
        # Check minimum PDF size (very small files might be corrupted)
        if len(pdf_content) < 100:
            error_msg = f"File {s3_key} is too small ({len(pdf_content)} bytes) - likely corrupted"
            logger.error(f"❌ {error_msg}")
            raise Exception(error_msg)
        
        # Check if PDF is multi-page (synchronous analyze_document only supports single-page PDFs)
        # Count /Page objects in PDF - simple heuristic
        page_count = pdf_content.count(b'/Type/Page') + pdf_content.count(b'/Type /Page')
        is_multi_page = page_count > 1
        
        # Get asset codes mapping (cached at module level)
        asset_codes = get_asset_codes_mapping()
        
        logger.info(f"📄 Using Textract to parse House PTR PDF ({len(pdf_content)} bytes, {page_count} page(s))...")
        
        # Call Textract to extract text and forms/tables
        # Use synchronous analyze_document for single-page PDFs, asynchronous for multi-page
        all_blocks = []
        
        if is_multi_page:
            # Asynchronous Textract for multi-page PDFs
            logger.info(f"📄 Using asynchronous Textract for multi-page PDF ({page_count} pages)...")
            try:
                # Start asynchronous document analysis (requires S3, not bytes)
                response = textract_client.start_document_analysis(
                    DocumentLocation={
                        'S3Object': {
                            'Bucket': S3_BUCKET,
                            'Name': s3_key
                        }
                    },
                    FeatureTypes=['FORMS', 'TABLES']
                )
                job_id = response['JobId']
                logger.info(f"📄 Started Textract job {job_id} for {s3_key}")
                
                # Poll for job completion (max 5 minutes = 60 iterations * 5 seconds)
                max_iterations = 60
                for i in range(max_iterations):
                    time.sleep(5)  # Wait 5 seconds between polls
                    response = textract_client.get_document_analysis(JobId=job_id)
                    status = response['JobStatus']
                    
                    if status == 'SUCCEEDED':
                        logger.info(f"✅ Textract job {job_id} completed successfully")
                        all_blocks = response.get('Blocks', [])
                        # Get all pages (pagination)
                        next_token = response.get('NextToken')
                        while next_token:
                            response = textract_client.get_document_analysis(JobId=job_id, NextToken=next_token)
                            all_blocks.extend(response.get('Blocks', []))
                            next_token = response.get('NextToken')
                        break
                    elif status == 'FAILED':
                        error_msg = f"Textract job {job_id} failed: {response.get('StatusMessage', 'Unknown error')}"
                        logger.error(f"❌ {error_msg}")
                        raise Exception(error_msg)
                    elif status in ['IN_PROGRESS', 'PARTIAL_SUCCESS']:
                        if i % 10 == 0:  # Log every 50 seconds
                            logger.info(f"⏳ Textract job {job_id} still in progress... (iteration {i+1}/{max_iterations})")
                    else:
                        error_msg = f"Textract job {job_id} returned unexpected status: {status}"
                        logger.error(f"❌ {error_msg}")
                        raise Exception(error_msg)
                else:
                    # Loop completed without breaking (timeout)
                    error_msg = f"Textract job {job_id} timed out after {max_iterations * 5} seconds"
                    logger.error(f"❌ {error_msg}")
                    raise Exception(error_msg)
                    
            except ClientError as e:
                error_code = e.response.get('Error', {}).get('Code', 'Unknown')
                if error_code == 'UnsupportedDocumentException':
                    error_msg = f"Textract UnsupportedDocumentException for {s3_key}: File may be corrupted, encrypted, password-protected, or in unsupported format. Textract supports PNG, JPEG, PDF, or TIFF formats."
                    logger.error(f"❌ {error_msg}")
                    raise Exception(error_msg)  # Fail completely instead of returning empty list
                else:
                    logger.error(f"❌ Textract ClientError ({error_code}) for {s3_key}: {e}")
                    raise  # Re-raise other ClientErrors
            except Exception as e:
                # Catch UnsupportedDocumentException from botocore.errorfactory
                error_type = type(e).__name__
                error_str = str(e)
                
                if 'UnsupportedDocumentException' in error_type or 'UnsupportedDocumentException' in error_str:
                    error_msg = f"Textract UnsupportedDocumentException for {s3_key}: File may be corrupted, encrypted, password-protected, or in unsupported format. Textract supports PNG, JPEG, PDF, or TIFF formats."
                    logger.error(f"❌ {error_msg}")
                    raise Exception(error_msg)  # Fail completely instead of returning empty list
                else:
                    logger.error(f"❌ Textract error for {s3_key}: {error_type}: {error_str}")
                    raise  # Re-raise other exceptions
        else:
            # Synchronous Textract for single-page PDFs
            logger.info(f"📄 Using synchronous Textract for single-page PDF...")
            try:
                textract_response = textract_client.analyze_document(
                    Document={'Bytes': pdf_content},
                    FeatureTypes=['FORMS', 'TABLES']
                )
                all_blocks = textract_response.get('Blocks', [])
            except ClientError as e:
                error_code = e.response.get('Error', {}).get('Code', 'Unknown')
                if error_code == 'UnsupportedDocumentException':
                    error_msg = f"Textract UnsupportedDocumentException for {s3_key}: File may be corrupted, encrypted, password-protected, or in unsupported format. Textract supports PNG, JPEG, PDF, or TIFF formats."
                    logger.error(f"❌ {error_msg}")
                    raise Exception(error_msg)  # Fail completely instead of returning empty list
                else:
                    logger.error(f"❌ Textract ClientError ({error_code}) for {s3_key}: {e}")
                    raise  # Re-raise other ClientErrors
            except Exception as e:
                # Catch UnsupportedDocumentException from botocore.errorfactory
                error_type = type(e).__name__
                error_str = str(e)
                
                if 'UnsupportedDocumentException' in error_type or 'UnsupportedDocumentException' in error_str:
                    error_msg = f"Textract UnsupportedDocumentException for {s3_key}: File may be corrupted, encrypted, password-protected, or in unsupported format. Textract supports PNG, JPEG, PDF, or TIFF formats."
                    logger.error(f"❌ {error_msg}")
                    raise Exception(error_msg)  # Fail completely instead of returning empty list
                else:
                    logger.error(f"❌ Textract error for {s3_key}: {error_type}: {error_str}")
                    raise  # Re-raise other exceptions
        
        # Extract text and structured data
        text_lines = []
        form_fields = {}
        tables = []
        
        for block in all_blocks:
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
                    for relationship in block.get('Relationships', []):
                        if relationship.get('Type') == 'CHILD':
                            for child_id in relationship.get('Ids', []):
                                for child_block in all_blocks:
                                    if child_block.get('Id') == child_id and child_block.get('BlockType') == 'WORD':
                                        key_text += child_block.get('Text', '') + ' '
                    key_text = key_text.strip()
                    
                    value_text = ''
                    for relationship in block.get('Relationships', []):
                        if relationship.get('Type') == 'VALUE':
                            for value_id in relationship.get('Ids', []):
                                for value_block in all_blocks:
                                    if value_block.get('Id') == value_id:
                                        if value_block.get('BlockType') == 'WORD':
                                            value_text += value_block.get('Text', '') + ' '
                                        elif value_block.get('BlockType') == 'SELECTION_ELEMENT':
                                            if value_block.get('SelectionStatus') == 'SELECTED':
                                                value_text = 'Yes'
                    value_text = value_text.strip()
                    
                    if key_text and value_text:
                        form_fields[key_text.lower()] = value_text
        
        # Extract tables
        table_blocks = [b for b in all_blocks if b.get('BlockType') == 'TABLE']
        for table_block in table_blocks:
            table_data = extract_table_data_from_blocks(table_block, all_blocks)
            if table_data:
                tables.append(table_data)
        
        # Comprehensive logging of Textract output for debugging
        logger.info(f"📊 Textract Output Summary for {s3_key}:")
        logger.info(f"   Total blocks: {len(all_blocks)}")
        
        # Count block types
        block_type_counts = {}
        for block in all_blocks:
            block_type = block.get('BlockType', 'UNKNOWN')
            block_type_counts[block_type] = block_type_counts.get(block_type, 0) + 1
        logger.info(f"   Block type counts: {json.dumps(block_type_counts, indent=2)}")
        
        logger.info(f"   Text lines extracted: {len(text_lines)}")
        logger.info(f"   Form fields extracted: {len(form_fields)}")
        logger.info(f"   Tables extracted: {len(tables)}")
        
        # Log all text lines (first 50 lines to avoid log spam)
        logger.info(f"   First 50 text lines:")
        for i, line in enumerate(text_lines[:50]):
            logger.info(f"      [{i+1}] {line}")
        if len(text_lines) > 50:
            logger.info(f"      ... ({len(text_lines) - 50} more lines)")
        
        # Log all form fields
        if form_fields:
            logger.info(f"   Form fields:")
            for key, value in form_fields.items():
                logger.info(f"      '{key}': '{value}'")
        else:
            logger.info(f"   No form fields extracted")
        
        # Log all tables
        if tables:
            logger.info(f"   Tables extracted ({len(tables)} total):")
            for table_idx, table in enumerate(tables):
                logger.info(f"      Table {table_idx + 1} ({len(table)} rows):")
                for row_idx, row in enumerate(table[:10]):  # First 10 rows per table
                    logger.info(f"         Row {row_idx}: {row}")
                if len(table) > 10:
                    logger.info(f"         ... ({len(table) - 10} more rows)")
        else:
            logger.info(f"   No tables extracted")
        
        full_text = ' '.join(text_lines)
        logger.info(f"   Full text length: {len(full_text)} characters")
        logger.info(f"   Full text (first 1000 chars): {full_text[:1000]}")
        
        # Extract filer name from House PTR
        filer_name = None
        
        # First, try to extract from Table 1 (FILER INFORMATION table)
        # Table 1 format: [['Name:', 'Hon. John McGuire'], ['Status:', 'Member'], ...]
        for table in tables:
            if len(table) >= 1:
                # Check if this looks like the FILER INFORMATION table
                first_row = table[0]
                if len(first_row) >= 2 and first_row[0].strip().lower() == 'name:':
                    name_value = first_row[1].strip()
                    # Remove "Hon." prefix if present
                    filer_name = re.sub(r'^Hon\.?\s+', '', name_value, flags=re.IGNORECASE).strip()
                    logger.info(f"✅ Extracted filer name from FILER INFORMATION table: {filer_name}")
                    break
        
        # If not found in table, try text patterns
        if not filer_name:
            # House format: "The Honorable [Name]" or "[Name] (House Representative)" or "Hon. [Name]"
            house_patterns = [
                r'Hon\.?\s+([A-Z][a-z]+(?:\s+[A-Z][a-z.]+)+)',
                r'The Honorable\s+([A-Z][a-z]+(?:\s+[A-Z][a-z.]+)+)',
                r'([A-Z][a-z]+(?:\s+[A-Z][a-z.]+)+)\s*\(House Representative\)',
            ]
            for pattern in house_patterns:
                match = re.search(pattern, full_text)
                if match:
                    filer_name = match.group(1).strip()
                    filer_name = re.sub(r'\b(Honorable|Hon\.?|Representative|Rep\.?)\b', '', filer_name, flags=re.IGNORECASE).strip()
                    break
        
        # If not found in text, check form fields
        if not filer_name:
            for key, value in form_fields.items():
                if 'name' in key or 'filer' in key or 'representative' in key:
                    filer_name = value
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
        
        # Parse tables for trade data
        # House PTR columns: ID, Owner, Asset, Transaction type (S/P), Date, Notification date, Amount, Cap gains
        # Skip Table 1 (FILER INFORMATION) - only process transaction tables
        for table_idx, table in enumerate(tables):
            if len(table) < 2:  # Need at least header + data row
                continue
            
            # Skip FILER INFORMATION table (Table 1) - it has "Name:" in first row
            if len(table) > 0 and len(table[0]) >= 2:
                first_cell = table[0][0].strip().lower()
                if first_cell == 'name:':
                    logger.debug(f"⏭️ Skipping FILER INFORMATION table (Table {table_idx + 1})")
                    continue
            
            # Find header row
            header_row = table[0]
            
            # Map column indices
            id_col = None
            owner_col = None
            asset_col = None
            transaction_type_col = None
            date_col = None
            notification_date_col = None
            amount_col = None
            cap_gains_col = None
            
            for idx, cell in enumerate(header_row):
                cell_lower = cell.lower().strip()
                if cell_lower == 'id' or 'id' in cell_lower:
                    id_col = idx
                elif 'owner' in cell_lower:
                    owner_col = idx
                elif 'asset' in cell_lower:
                    asset_col = idx
                elif 'transaction type' in cell_lower or ('type' in cell_lower and 'transaction' in cell_lower):
                    transaction_type_col = idx
                elif 'date' in cell_lower and 'notification' not in cell_lower:
                    date_col = idx
                elif 'notification date' in cell_lower or ('notification' in cell_lower and 'date' in cell_lower):
                    notification_date_col = idx
                elif 'amount' in cell_lower or 'value' in cell_lower:
                    amount_col = idx
                elif 'cap gain' in cell_lower or 'capital gain' in cell_lower:
                    cap_gains_col = idx
            
            # Parse data rows
            for row in table[1:]:
                # Skip if row is too short - check required columns
                required_cols = [c for c in [asset_col, transaction_type_col, date_col, amount_col] if c is not None]
                if not required_cols or len(row) < max(required_cols) + 1:
                    continue
                
                # Extract transaction date
                transaction_date = None
                if date_col is not None and date_col < len(row):
                    date_str = row[date_col].strip()
                    for fmt in ['%m/%d/%Y', '%m-%d-%Y', '%Y-%m-%d', '%m/%d/%y']:
                        try:
                            transaction_date = datetime.strptime(date_str, fmt).strftime('%Y-%m-%d')
                            break
                        except ValueError:
                            continue
                
                # Extract asset information
                # Asset column contains: "Company Name (TICKER) [ST] FILING STATUS: New SUBHOLDING OF: ..."
                security_name = None
                security_symbol = None
                asset_type = None
                
                if asset_col is not None and asset_col < len(row):
                    asset_text = row[asset_col].strip()
                    
                    # Remove common suffixes that aren't part of the security name
                    # Remove "FILING STATUS: New" and "SUBHOLDING OF: ..."
                    cleaned_asset = re.sub(r'\s*FILING STATUS:\s*\w+.*$', '', asset_text, flags=re.IGNORECASE)
                    cleaned_asset = re.sub(r'\s*SUBHOLDING OF:\s*[^\[\]]*$', '', cleaned_asset, flags=re.IGNORECASE)
                    
                    # Extract ticker symbol (in parentheses, e.g., "(UNH)", "(GOOG)", "(AMZN)")
                    ticker_match = re.search(r'\(([A-Z]{1,5})\)', cleaned_asset)
                    if ticker_match:
                        security_symbol = ticker_match.group(1)
                        # Remove ticker from cleaned text to get security name
                        cleaned_asset = re.sub(r'\s*\([A-Z]{1,5}\)\s*', '', cleaned_asset)
                    
                    # Extract asset type code (in brackets, e.g., "[ST]" for Stock)
                    asset_code_match = re.search(r'\[([A-Z]{2,3})\]', cleaned_asset)
                    if asset_code_match:
                        code = asset_code_match.group(1)
                        if code in asset_codes:
                            asset_type = asset_codes[code]
                        # Remove asset code from cleaned text
                        cleaned_asset = re.sub(r'\s*\[[A-Z]{2,3}\]\s*', '', cleaned_asset)
                    
                    # The remaining text is the security name
                    security_name = cleaned_asset.strip()
                    
                    # If we still don't have a name, use the original (fallback)
                    if not security_name:
                        security_name = asset_text
                
                # Extract transaction type
                transaction_type = None
                if transaction_type_col is not None and transaction_type_col < len(row):
                    type_str = row[transaction_type_col].strip().upper()
                    if 'S' in type_str or 'sale' in type_str.lower():
                        transaction_type = 'Sale'
                    elif 'P' in type_str or 'purchase' in type_str.lower() or 'buy' in type_str.lower():
                        transaction_type = 'Purchase'
                
                # Extract owner (SP = Spouse, etc.)
                owner = None
                if owner_col is not None and owner_col < len(row):
                    owner_str = row[owner_col].strip()
                    if owner_str:
                        owner = owner_str  # Could be "SP" (Spouse), "Self", etc.
                
                # Extract amount
                amount = None
                amount_min = None
                amount_max = None
                
                if amount_col is not None and amount_col < len(row):
                    amount_str = row[amount_col].strip()
                    # Remove currency symbols and commas
                    amount_str = re.sub(r'[$,]', '', amount_str)
                    
                    # Try to parse range first (e.g., "$1,001 - $15,000" or "$1,001 $15,000")
                    range_match = re.search(r'(\d+(?:,\d{3})*(?:\.\d+)?)\s*[-–]\s*(\d+(?:,\d{3})*(?:\.\d+)?)', amount_str)
                    if not range_match:
                        # Try without dash (e.g., "$1,001 $15,000")
                        range_match = re.search(r'(\d+(?:,\d{3})*(?:\.\d+)?)\s+(\d+(?:,\d{3})*(?:\.\d+)?)', amount_str)
                    
                    if range_match:
                        # Remove commas from numbers
                        min_str = range_match.group(1).replace(',', '')
                        max_str = range_match.group(2).replace(',', '')
                        try:
                            amount_min = float(min_str)
                            amount_max = float(max_str)
                            amount = (amount_min + amount_max) / 2  # Use midpoint
                        except ValueError:
                            pass
                    else:
                        # Try to parse as single number
                        try:
                            amount_str_clean = amount_str.replace(',', '')
                            amount = float(amount_str_clean)
                            amount_min = amount
                            amount_max = amount
                        except ValueError:
                            pass
                
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
                        'owner': owner,  # Add owner field
                        'formType': 'house_ptr',
                        'source': 'house'
                    }
                    trades.append(trade)
                    logger.debug(f"   ✅ Extracted trade: {security_symbol or security_name} - {transaction_type} - ${amount_min}-${amount_max}")
        
        logger.info(f"✅ Extracted {len(trades)} trades from House PTR: {s3_key}")
        
    except Exception as e:
        # Check if this is an UnsupportedDocumentException (should fail completely)
        error_type = type(e).__name__
        error_str = str(e)
        
        if 'UnsupportedDocumentException' in error_type or 'UnsupportedDocumentException' in error_str:
            # Re-raise to fail completely - already logged in inner handler
            raise  # Fail completely instead of returning empty list
        else:
            # Log full traceback for unexpected errors
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
                    'totalAmount': trade.get('amount'),
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
                    'stateDistrict': state_district
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
    
    # Limit to first 10 files for processing
    if len(unprocessed_keys) > 10:
        logger.info(f"⚠️ Limiting processing to first 10 files (found {len(unprocessed_keys)} unprocessed files)")
        unprocessed_keys = unprocessed_keys[:10]
    
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


