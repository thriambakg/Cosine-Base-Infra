"""
Politician Trades Matcher Lambda
Parses SEC forms and Congressional PTRs, extracts trades, and matches to politicians

This is Step 2 of the 3-step politician trades aggregation workflow.
"""

import json
import os
import logging
import boto3
from typing import List, Dict, Any, Optional
from datetime import datetime
import csv
from io import StringIO
import xml.etree.ElementTree as ET
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
    Load politician CSV from S3
    
    Returns:
        List of politician dicts with name, party, position, alternativeNames
    """
    logger.info("📋 Loading politician list from S3")
    
    try:
        # Download politicians.csv from S3
        response = s3_client.get_object(
            Bucket=S3_BUCKET,
            Key='politicians.csv'
        )
        
        csv_content = response['Body'].read().decode('utf-8')
        csv_reader = csv.DictReader(StringIO(csv_content))
        
        politicians = []
        for row in csv_reader:
            # Parse alternative names (comma-separated)
            alt_names = []
            if row.get('alternativeNames'):
                alt_names = [name.strip() for name in row['alternativeNames'].split(',')]
            
            politicians.append({
                'name': row['name'].strip(),
                'party': row['party'].strip(),
                'position': row['position'].strip(),
                'alternativeNames': alt_names
            })
        
        logger.info(f"✅ Loaded {len(politicians)} politicians from CSV")
        return politicians
        
    except Exception as e:
        logger.error(f"❌ Error loading politician list: {e}")
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

def parse_sec_form_xml(s3_key: str) -> List[Dict[str, Any]]:
    """
    Parse SEC Form XML and extract trade data
    
    Args:
        s3_key: S3 key of the form XML
        
    Returns:
        List of trade dicts extracted from form
    """
    logger.info(f"📄 Parsing SEC form XML: {s3_key}")
    
    trades = []
    
    try:
        # Download XML from S3
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
        xml_content = response['Body'].read()
        
        # Parse XML
        root = ET.fromstring(xml_content)
        
        # SEC forms have complex XML structure
        # Form 4 structure example:
        # <ownershipDocument>
        #   <reportingOwner>
        #     <reportingOwnerId>
        #       <rptOwnerName>
        #   <nonDerivativeTable>
        #     <nonDerivativeTransaction>
        #       <transactionDate>
        #       <transactionCode>
        #       <securityTitle>
        #       <transactionShares>
        #       <transactionPricePerShare>
        
        # Extract filer name
        filer_name = None
        filer_name_elem = root.find('.//{http://www.sec.gov/edgar/document/edgardocument}rptOwnerName')
        if filer_name_elem is not None:
            filer_name = filer_name_elem.text.strip() if filer_name_elem.text else None
        
        if not filer_name:
            logger.warning(f"⚠️ Could not extract filer name from {s3_key}")
            return trades
        
        # Extract transactions
        # Note: SEC XML is complex with namespaces - this is simplified
        # Full implementation would need to handle all transaction types
        # (Purchases, Sales, Grants, Exercises, Conversions, etc.)
        
        # For now, return placeholder structure
        logger.info(f"✅ Extracted filer name: {filer_name}")
        
    except Exception as e:
        logger.error(f"❌ Error parsing SEC form XML {s3_key}: {e}")
    
    return trades

def parse_sec_form_pdf(s3_key: str) -> List[Dict[str, Any]]:
    """
    Parse SEC Form PDF and extract trade data
    
    Args:
        s3_key: S3 key of the form PDF
        
    Returns:
        List of trade dicts extracted from form
    """
    logger.info(f"📄 Parsing SEC form PDF: {s3_key}")
    
    trades = []
    
    try:
        # Download PDF from S3
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
        pdf_content = response['Body'].read()
        
        # Note: PDF parsing requires AWS Textract or pdf parsing library
        # For now, this is a placeholder
        # Full implementation would:
        # 1. Use AWS Textract to extract text
        # 2. Parse structured data from text
        # 3. Extract filer name, transaction details, etc.
        
        logger.warning("⚠️ PDF parsing not fully implemented - requires Textract or pdf library")
        
    except Exception as e:
        logger.error(f"❌ Error parsing SEC form PDF {s3_key}: {e}")
    
    return trades

def parse_house_ptr(s3_key: str) -> List[Dict[str, Any]]:
    """
    Parse House PTR PDF and extract trade data
    
    Args:
        s3_key: S3 key of the House PTR PDF
        
    Returns:
        List of trade dicts extracted from PTR
    """
    logger.info(f"📄 Parsing House PTR: {s3_key}")
    
    trades = []
    
    try:
        # Download PDF from S3
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
        pdf_content = response['Body'].read()
        
        # Note: Similar to SEC PDF parsing - requires Textract or pdf library
        # House PTRs have different format than SEC forms
        # Would need to extract: representative name, transaction date, security, amount
        
        logger.warning("⚠️ House PTR PDF parsing not fully implemented")
        
    except Exception as e:
        logger.error(f"❌ Error parsing House PTR {s3_key}: {e}")
    
    return trades

def parse_senate_ptr(s3_key: str) -> List[Dict[str, Any]]:
    """
    Parse Senate PTR PDF and extract trade data
    
    Args:
        s3_key: S3 key of the Senate PTR PDF
        
    Returns:
        List of trade dicts extracted from PTR
    """
    logger.info(f"📄 Parsing Senate PTR: {s3_key}")
    
    trades = []
    
    try:
        # Download PDF from S3
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
        pdf_content = response['Body'].read()
        
        # Note: Similar to House PTR parsing
        # Senate PTRs have different format
        
        logger.warning("⚠️ Senate PTR PDF parsing not fully implemented")
        
    except Exception as e:
        logger.error(f"❌ Error parsing Senate PTR {s3_key}: {e}")
    
    return trades

def lambda_handler(event, context):
    """
    Lambda handler for matching trades to politicians
    
    Expected input from Step 1:
    {
        "date": "2024-01-15",
        "secForms": [...],
        "housePTRs": [...],
        "senatePTRs": [...]
    }
    
    Returns:
    {
        "date": "2024-01-15",
        "matchedTrades": [...],
        "totalMatched": 45,
        "unmatchedForms": 3
    }
    """
    logger.info("🚀 Politician Trades Matcher Lambda started")
    
    if not S3_BUCKET:
        raise ValueError("S3_BUCKET environment variable not set")
    
    # Get input from previous step
    date = event.get('date') or event.get('fetchResults', {}).get('date')
    sec_forms = event.get('secForms') or event.get('fetchResults', {}).get('secForms', [])
    house_ptrs = event.get('housePTRs') or event.get('fetchResults', {}).get('housePTRs', [])
    senate_ptrs = event.get('senatePTRs') or event.get('fetchResults', {}).get('senatePTRs', [])
    
    logger.info(f"📅 Processing date: {date}")
    logger.info(f"📋 Processing {len(sec_forms)} SEC forms, {len(house_ptrs)} House PTRs, {len(senate_ptrs)} Senate PTRs")
    
    # Load politician list
    politicians = load_politician_list()
    if not politicians:
        raise ValueError("Failed to load politician list from S3")
    
    matched_trades = []
    unmatched_count = 0
    
    try:
        # Process SEC Forms
        for form in sec_forms:
            s3_key = form.get('s3Key')
            form_type = form.get('formType')
            
            if not s3_key:
                continue
            
            # Parse form based on file extension
            if s3_key.endswith('.xml'):
                trades = parse_sec_form_xml(s3_key)
            elif s3_key.endswith('.pdf'):
                trades = parse_sec_form_pdf(s3_key)
            else:
                logger.warning(f"⚠️ Unknown file type for {s3_key}")
                unmatched_count += 1
                continue
            
            # Match each trade to a politician
            for trade in trades:
                filer_name = trade.get('filerName')
                if not filer_name:
                    continue
                
                matched_politician = find_matching_politician(filer_name, politicians)
                
                if matched_politician:
                    # Create matched trade object
                    matched_trade = {
                        'tradeId': f"trade_{date}_{form.get('cik', 'unknown')}_{len(matched_trades)}",
                        'politicianName': matched_politician['name'],
                        'party': matched_politician['party'],
                        'position': matched_politician['position'],
                        'formType': form_type,
                        'filingDate': form.get('filingDate', date),
                        'transactionDate': trade.get('transactionDate'),
                        'transactionTime': trade.get('transactionTime'),
                        'securitySymbol': trade.get('securitySymbol'),
                        'securityName': trade.get('securityName'),
                        'transactionType': trade.get('transactionType'),  # Purchase, Sale, etc.
                        'shares': trade.get('shares'),
                        'pricePerShare': trade.get('pricePerShare'),
                        'totalAmount': trade.get('totalAmount'),
                        'formCIK': form.get('cik'),
                        'formS3Key': s3_key,
                        'matchConfidence': matched_politician.get('matchScore', 1.0),
                        'source': 'sec'
                    }
                    matched_trades.append(matched_trade)
                else:
                    unmatched_count += 1
                    logger.debug(f"⚠️ No match found for filer: {filer_name}")
        
        # Process House PTRs
        for ptr in house_ptrs:
            s3_key = ptr.get('s3Key')
            if not s3_key:
                continue
            
            trades = parse_house_ptr(s3_key)
            
            for trade in trades:
                # House PTRs already have politician name, but verify against our list
                politician_name = trade.get('politicianName')
                if politician_name:
                    matched_politician = find_matching_politician(politician_name, politicians)
                    if matched_politician:
                        matched_trade = {
                            'tradeId': f"trade_{date}_house_{len(matched_trades)}",
                            'politicianName': matched_politician['name'],
                            'party': matched_politician['party'],
                            'position': matched_politician['position'],
                            'formType': 'house_ptr',
                            'filingDate': date,
                            'transactionDate': trade.get('transactionDate'),
                            'transactionTime': trade.get('transactionTime'),
                            'securitySymbol': trade.get('securitySymbol'),
                            'securityName': trade.get('securityName'),
                            'transactionType': trade.get('transactionType'),
                            'shares': trade.get('shares'),
                            'pricePerShare': trade.get('pricePerShare'),
                            'totalAmount': trade.get('totalAmount'),
                            'formS3Key': s3_key,
                            'matchConfidence': 1.0,
                            'source': 'house'
                        }
                        matched_trades.append(matched_trade)
        
        # Process Senate PTRs
        for ptr in senate_ptrs:
            s3_key = ptr.get('s3Key')
            if not s3_key:
                continue
            
            trades = parse_senate_ptr(s3_key)
            
            for trade in trades:
                # Similar to House PTRs
                politician_name = trade.get('politicianName')
                if politician_name:
                    matched_politician = find_matching_politician(politician_name, politicians)
                    if matched_politician:
                        matched_trade = {
                            'tradeId': f"trade_{date}_senate_{len(matched_trades)}",
                            'politicianName': matched_politician['name'],
                            'party': matched_politician['party'],
                            'position': matched_politician['position'],
                            'formType': 'senate_ptr',
                            'filingDate': date,
                            'transactionDate': trade.get('transactionDate'),
                            'transactionTime': trade.get('transactionTime'),
                            'securitySymbol': trade.get('securitySymbol'),
                            'securityName': trade.get('securityName'),
                            'transactionType': trade.get('transactionType'),
                            'shares': trade.get('shares'),
                            'pricePerShare': trade.get('pricePerShare'),
                            'totalAmount': trade.get('totalAmount'),
                            'formS3Key': s3_key,
                            'matchConfidence': 1.0,
                            'source': 'senate'
                        }
                        matched_trades.append(matched_trade)
        
        logger.info(f"✅ Matched {len(matched_trades)} trades to politicians")
        logger.info(f"⚠️ {unmatched_count} forms/trades could not be matched")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'date': date,
                'matchedTrades': matched_trades,
                'totalMatched': len(matched_trades),
                'unmatchedForms': unmatched_count
            })
        }
        
    except Exception as e:
        logger.error(f"❌ Fatal error in matcher Lambda: {e}")
        raise

