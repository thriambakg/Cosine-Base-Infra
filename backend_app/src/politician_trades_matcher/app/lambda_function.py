"""
Politician Trades Matcher Lambda
Parses Congressional PTRs, extracts trades, and matches to politicians

This is Step 2 of the 3-step politician trades aggregation workflow.
"""

import json
import os
import logging
import re
import boto3
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

def load_politician_list() -> List[Dict[str, Any]]:
    """
    Load congress-legislators CSV from S3
    
    Returns:
        List of politician dicts with name, party, position, url, and alternativeNames
    """
    logger.info("📋 Loading congress-legislators list from S3")
    
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
        
        logger.info(f"✅ Loaded {len(politicians)} legislators from CSV")
        return politicians
        
    except Exception as e:
        logger.error(f"❌ Error loading congress-legislators list: {e}")
        return []

def fuzzy_match_name(filer_name: str, politician: Dict[str, Any]) -> float:
    """
    Fuzzy match a filer name to a politician using Levenshtein distance
    
    Args:
        filer_name: Name from SEC form or PTR
        politician: Politician dict with name and alternativeNames
        
    Returns:
        Similarity score (0.0 to 1.0)
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
    
    Args:
        filer_name: Name from form
        politicians: List of politician dicts
        
    Returns:
        Best matching politician dict or None
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
            logger.error(f"❌ File {s3_key} is not a valid PDF (doesn't start with %PDF)")
            return []
        
        # Check minimum PDF size (very small files might be corrupted)
        if len(pdf_content) < 100:
            logger.error(f"❌ File {s3_key} is too small ({len(pdf_content)} bytes) - likely corrupted")
            return []
        
        # Get asset codes mapping (cached at module level)
        asset_codes = get_asset_codes_mapping()
        
        logger.info(f"📄 Using Textract to parse House PTR PDF ({len(pdf_content)} bytes)...")
        
        # Call Textract to extract text and forms/tables
        try:
            textract_response = textract_client.analyze_document(
                Document={'Bytes': pdf_content},
                FeatureTypes=['FORMS', 'TABLES']
            )
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
            # botocore.errorfactory.UnsupportedDocumentException is not a ClientError
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
        
        all_blocks = textract_response.get('Blocks', [])
        
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
        
        full_text = ' '.join(text_lines)
        
        # Extract filer name from House PTR
        filer_name = None
        
        # House format: "The Honorable [Name]" or "[Name] (House Representative)"
        house_patterns = [
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
        
        # Look for date patterns near signature text
        signature_date_patterns = [
            r'(?:signed|signature|date)[:\s]+(\d{1,2}/\d{1,2}/\d{4})',
            r'(\d{1,2}/\d{1,2}/\d{4})\s*(?:signed|signature)',
            r'(\d{1,2}/\d{1,2}/\d{4})',  # Fallback: any date in MM/DD/YYYY format
        ]
        
        # Search in reverse order (bottom of document) for signature date
        for pattern in signature_date_patterns:
            matches = list(re.finditer(pattern, full_text, re.IGNORECASE))
            if matches:
                # Take the last match (likely at bottom near signature)
                match = matches[-1]
                date_str = match.group(1)
                try:
                    filing_date = datetime.strptime(date_str, '%m/%d/%Y').strftime('%Y-%m-%d')
                    break
                except ValueError:
                    continue
        
        # Parse tables for trade data
        # House PTR columns: ID, Owner, Asset, Transaction type (S/P), Date, Notification date, Amount, Cap gains
        for table in tables:
            if len(table) < 2:  # Need at least header + data row
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
                security_name = None
                security_symbol = None
                asset_type = None
                
                if asset_col is not None and asset_col < len(row):
                    asset_text = row[asset_col].strip()
                    security_name = asset_text
                    
                    # Try to extract ticker symbol (usually uppercase letters, 1-5 chars, in parentheses or after name)
                    ticker_match = re.search(r'\(([A-Z]{1,5})\)|([A-Z]{1,5})\s*$', asset_text)
                    if ticker_match:
                        security_symbol = ticker_match.group(1) or ticker_match.group(2)
                    
                    # Extract asset class code (usually 2-3 uppercase letters at start or end)
                    asset_code_match = re.search(r'\b([A-Z]{2,3})\b', asset_text)
                    if asset_code_match:
                        code = asset_code_match.group(1)
                        if code in asset_codes:
                            asset_type = asset_codes[code]
                
                # Extract transaction type
                transaction_type = None
                if transaction_type_col is not None and transaction_type_col < len(row):
                    type_str = row[transaction_type_col].strip().upper()
                    if 'S' in type_str or 'sale' in type_str.lower():
                        transaction_type = 'Sale'
                    elif 'P' in type_str or 'purchase' in type_str.lower() or 'buy' in type_str.lower():
                        transaction_type = 'Purchase'
                
                # Extract amount
                amount = None
                amount_min = None
                amount_max = None
                
                if amount_col is not None and amount_col < len(row):
                    amount_str = row[amount_col].strip()
                    # Remove currency symbols and commas
                    amount_str = re.sub(r'[$,]', '', amount_str)
                    
                    # Try to parse as number
                    try:
                        amount = float(amount_str)
                        amount_min = amount
                        amount_max = amount
                    except ValueError:
                        # Try to parse range (e.g., "$1,000 - $15,000")
                        range_match = re.search(r'(\d+(?:\.\d+)?)\s*-\s*(\d+(?:\.\d+)?)', amount_str)
                        if range_match:
                            amount_min = float(range_match.group(1))
                            amount_max = float(range_match.group(2))
                            amount = (amount_min + amount_max) / 2  # Use midpoint
                
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
                        'formType': 'house_ptr',
                        'source': 'house'
                    }
                    trades.append(trade)
        
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
        
        # Match each trade to a politician
        for trade in trades:
            filer_name = trade.get('filerName')
            if not filer_name:
                unmatched_count += 1
                continue
            
            politician = find_matching_politician(filer_name, politicians)
            if politician:
                # Add politician info to trade
                matched_trade = {
                    **trade,
                    'politicianName': politician.get('name'),
                    'party': politician.get('party'),
                    'position': politician.get('position'),
                    'websiteUrl': politician.get('websiteUrl'),
                    'bioguideId': politician.get('bioguide_id'),
                    'state': politician.get('state'),
                    'district': politician.get('district'),
                    'matchScore': politician.get('matchScore', 1.0)
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
        'formType': 'house_ptr'
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
    Lambda handler for matching House PTR trades and aggregating all matched trades
    
    Expected input from Step Functions:
    - For House PTR matching (direct call):
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
    - For aggregation (from parallel branches):
      {
        "matchResults": [...],
        "date": "2024-01-15"
      }
    
    Returns:
    {
        "matchedTrades": [...],
        "unmatchedCount": 0,
        "date": "2024-01-15"
    }
    """
    logger.info("🚀 Politician Trades Matcher Lambda started")
    
    # Check if this is a direct House PTR matching call (from Step Functions after download)
    download_results = event.get('downloadResults')
    if download_results:
        logger.info("🔍 Processing House PTR matching from downloader output")
        return handle_house_ptr_matching(event, download_results)
    
    # Otherwise, this is an aggregation call
    logger.info("📊 Aggregating matched trades from parallel processing")
    return handle_aggregation(event)

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
    
    # Load politician list for matching
    logger.info("📋 Loading politician list for matching")
    politicians = load_politician_list()
    
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

def handle_aggregation(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Handle aggregation of matched trades from parallel processing
    
    Args:
        event: Event from Step Functions with match results
        
    Returns:
        Aggregated results
    """
    # Get match results from parallel processing
    # SEC results come from S3 (written by Glue job), Senate/House from Lambda outputs
    sec_glue_output = event.get('secGlueOutput', {})
    senate_match_results = event.get('senateMatchResults', [])
    house_match_results = event.get('houseMatchResults', [])
    s3_bucket = event.get('s3Bucket', S3_BUCKET)
    
    date = event.get('date') or event.get('fetchResults', {}).get('date')
    
    if not date:
        raise ValueError("Date not provided in event")
    
    logger.info(f"📅 Aggregating results for date: {date}")
    
    # Load SEC results from S3 (written by Glue job)
    sec_matched_trades = []
    sec_unmatched_count = 0
    sec_forms_fetched = 0
    
    try:
        sec_summary_key = f"temp/sec-results-{date}.json"
        logger.info(f"📋 Reading SEC summary from S3: s3://{s3_bucket}/{sec_summary_key}")
        
        try:
            response = s3_client.get_object(Bucket=s3_bucket, Key=sec_summary_key)
            sec_summary = json.loads(response['Body'].read().decode('utf-8'))
            
            sec_matched_trades = sec_summary.get('matchedTrades', [])
            sec_unmatched_count = sec_summary.get('unmatchedCount', 0)
            sec_forms_fetched = sec_summary.get('secFormsFetched', 0)
            
            logger.info(f"✅ Loaded SEC results: {len(sec_matched_trades)} matched trades, {sec_unmatched_count} unmatched, {sec_forms_fetched} forms fetched")
        except s3_client.exceptions.NoSuchKey:
            logger.warning(f"⚠️ SEC summary not found in S3: {sec_summary_key}. Glue job may have failed or not completed yet.")
        except Exception as e:
            logger.error(f"❌ Error reading SEC summary from S3: {e}")
    except Exception as e:
        logger.error(f"❌ Error loading SEC results: {e}")
    
    # Process House PTR results (if any)
    logger.info(f"📋 Processing {len(house_match_results)} House PTR results")
    
    house_matched_trades = []
    house_unmatched_count = 0
    
    # house_match_results should contain match results from House PTR matching
    for house_result in house_match_results:
        if isinstance(house_result, dict):
            matched_trades = house_result.get('matchedTrades', [])
            unmatched_count = house_result.get('unmatchedCount', 0)
            house_matched_trades.extend(matched_trades)
            house_unmatched_count += unmatched_count
    
    # Get Senate results from Lambda outputs
    logger.info(f"📋 Processing {len(senate_match_results)} Senate results")
    
    # Filter out failed downloads (success: false)
    valid_senate_results = [r for r in senate_match_results if r.get('success') is not False]
    
    # Aggregate Senate matched trades and count unmatched
    senate_matched_trades = []
    senate_unmatched_count = 0
    
    for result in valid_senate_results:
        matched_trades = result.get('matchedTrades', [])
        unmatched_count = result.get('unmatchedCount', 0)
        
        senate_matched_trades.extend(matched_trades)
        senate_unmatched_count += unmatched_count
    
    # Combine all results
    all_matched_trades = sec_matched_trades + senate_matched_trades + house_matched_trades
    total_unmatched = sec_unmatched_count + senate_unmatched_count + house_unmatched_count
    
    logger.info(f"📊 Total: {len(all_matched_trades)} matched trades ({len(sec_matched_trades)} SEC, {len(senate_matched_trades)} Senate, {len(house_matched_trades)} House)")
    logger.info(f"⚠️ {total_unmatched} files/trades could not be matched ({sec_unmatched_count} SEC, {senate_unmatched_count} Senate, {house_unmatched_count} House)")
    
    logger.info(f"✅ Aggregated {len(all_matched_trades)} matched trades")
    logger.info(f"⚠️ {total_unmatched} files/trades could not be matched")
    
    try:
        return {
            "date": date,
            "matchedTrades": all_matched_trades,
            "totalMatched": len(all_matched_trades),
            "unmatchedForms": total_unmatched
        }
    except Exception as e:
        logger.error(f"❌ Fatal error in aggregator Lambda: {e}")
        raise

