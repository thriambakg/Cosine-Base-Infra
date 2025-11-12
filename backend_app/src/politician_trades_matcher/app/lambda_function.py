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

def load_asset_codes_mapping() -> Dict[str, str]:
    """
    Load House PTR asset codes mapping from S3
    
    Returns:
        Dict mapping asset codes to asset names
    """
    logger.info("📋 Loading House PTR asset codes mapping from S3")
    
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
        return asset_codes
        
    except Exception as e:
        logger.warning(f"⚠️ Error loading asset codes mapping: {e}. Continuing without asset code mapping.")
        return {}

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
        
        # Load asset codes mapping
        asset_codes = load_asset_codes_mapping()
        
        logger.info("📄 Using Textract to parse House PTR PDF...")
        
        # Call Textract to extract text and forms/tables
        textract_response = textract_client.analyze_document(
            Document={'Bytes': pdf_content},
            FeatureTypes=['FORMS', 'TABLES']
        )
        
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
                asset_class = None
                
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
                            asset_class = asset_codes[code]
                
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
                        'assetClass': asset_class,
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
        logger.error(f"❌ Error parsing House PTR {s3_key}: {e}")
        import traceback
        logger.error(traceback.format_exc())
    
    return trades

def match_house_ptr_trades(s3_key: str, politicians: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Parse House PTR PDF, extract trades, and match to politicians
    
    Args:
        s3_key: S3 key of the House PTR PDF
        politicians: List of politician dicts for matching
        
    Returns:
        Dict with matchedTrades, unmatchedCount, s3Key, formType
    """
    logger.info(f"🔍 Matching House PTR trades: {s3_key}")
    
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
        logger.error(f"❌ Error matching House PTR trades {s3_key}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        unmatched_count = 1
    
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
    Lambda handler for aggregating matched trades from parallel processing
    
    Expected input from Step Functions (after Map state):
    {
        "matchResults": [
            {
                "matchedTrades": [...],
                "unmatchedCount": 0,
                "s3Key": "...",
                "formType": "..."
            },
            ...
        ],
        "date": "2024-01-15"
    }
    
    Returns:
    {
        "date": "2024-01-15",
        "matchedTrades": [...],
        "totalMatched": 45,
        "unmatchedForms": 3
    }
    """
    logger.info("🚀 Politician Trades Aggregator Lambda started")
    
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
    
    # Process House PTR results
    logger.info(f"📋 Processing {len(house_match_results)} House PTR results")
    
    # Load politician list for matching
    politicians = load_politician_list()
    
    # Process House PTR files
    house_matched_trades = []
    house_unmatched_count = 0
    
    # house_match_results should contain S3 keys of House PTR PDFs
    for house_result in house_match_results:
        if isinstance(house_result, dict):
            s3_key = house_result.get('s3Key') or house_result.get('s3_key')
        else:
            s3_key = house_result  # Assume it's a string S3 key
        
        if not s3_key:
            logger.warning(f"⚠️ House result missing S3 key: {house_result}")
            house_unmatched_count += 1
            continue
        
        # Match House PTR trades
        match_result = match_house_ptr_trades(s3_key, politicians)
        house_matched_trades.extend(match_result.get('matchedTrades', []))
        house_unmatched_count += match_result.get('unmatchedCount', 0)
    
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

