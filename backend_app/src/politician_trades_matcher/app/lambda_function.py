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
    # Support both old format (single matchResults array) and new format (separate arrays)
    # SEC results come from Glue job as JSON string (secResultsJson)
    sec_results_json = event.get('secResultsJson')
    sec_match_results = []
    
    if sec_results_json:
        try:
            # Parse JSON string from S3 Body (Glue job writes summary as JSON)
            if isinstance(sec_results_json, str):
                sec_results = json.loads(sec_results_json)
            else:
                sec_results = sec_results_json
            
            # Extract matched trades from SEC results
            # Format: {"matchedTrades": [...], "totalMatched": 1500, "unmatchedCount": 500, "date": "2025-10-30"}
            matched_trades = sec_results.get('matchedTrades', [])
            if matched_trades:
                # Create a single match result object for consistency
                sec_match_results = [{
                    'matchedTrades': matched_trades,
                    'unmatchedCount': sec_results.get('unmatchedCount', 0),
                    'totalMatched': sec_results.get('totalMatched', len(matched_trades)),
                    'source': 'sec'
                }]
                logger.info(f"✅ Parsed SEC results: {len(matched_trades)} matched trades")
            else:
                logger.info("⚠️ SEC results contain no matched trades")
        except (json.JSONDecodeError, TypeError) as e:
            logger.error(f"❌ Error parsing SEC results JSON: {e}")
            logger.error(f"   SEC results JSON (first 500 chars): {str(sec_results_json)[:500]}")
    
    # Get Senate and House match results (from Lambda pipeline)
    house_match_results = event.get('houseMatchResults', [])
    senate_match_results = event.get('senateMatchResults', [])
    
    # Backward compatibility: if old format, use matchResults
    if not sec_match_results and not house_match_results and not senate_match_results:
        match_results = event.get('matchResults', [])
        sec_match_results = match_results
        logger.info("📋 Using legacy matchResults format (SEC forms only)")
    
    # Combine all match results
    all_match_results = sec_match_results + house_match_results + senate_match_results
    
    date = event.get('date') or event.get('fetchResults', {}).get('date')
    
    if not date:
        raise ValueError("Date not provided in event")
    
    logger.info(f"📅 Aggregating results for date: {date}")
    logger.info(f"📋 Processing {len(sec_match_results)} SEC, {len(house_match_results)} House, {len(senate_match_results)} Senate results")
    logger.info(f"📊 Total: {len(all_match_results)} match results")
    
    # Filter out failed downloads (success: false)
    valid_results = [r for r in all_match_results if r.get('success') is not False]
    
    # Aggregate all matched trades and count unmatched
    all_matched_trades = []
    total_unmatched = 0
    
    for result in valid_results:
        matched_trades = result.get('matchedTrades', [])
        unmatched_count = result.get('unmatchedCount', 0)
        
        all_matched_trades.extend(matched_trades)
        total_unmatched += unmatched_count
    
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

