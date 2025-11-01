"""
Lambda function to match trades from a single SEC form or PTR file to politicians.
This function is invoked in parallel via Step Functions Map state.
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


def parse_sec_form_xml(s3_key: str) -> List[Dict[str, Any]]:
    """
    Parse SEC Form XML and extract trade data
    """
    trades = []
    
    try:
        # Download XML from S3
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
        xml_content = response['Body'].read()
        
        # Parse XML
        root = ET.fromstring(xml_content)
        
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


def parse_house_ptr(s3_key: str) -> List[Dict[str, Any]]:
    """
    Parse House PTR PDF and extract trade data
    """
    trades = []
    
    try:
        # Download PDF from S3
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
        pdf_content = response['Body'].read()
        
        # Note: Similar to SEC PDF parsing - requires Textract or pdf library
        logger.warning("⚠️ House PTR PDF parsing not fully implemented")
        
    except Exception as e:
        logger.error(f"❌ Error parsing House PTR {s3_key}: {e}")
    
    return trades


def parse_senate_ptr(s3_key: str) -> List[Dict[str, Any]]:
    """
    Parse Senate PTR PDF and extract trade data
    """
    trades = []
    
    try:
        # Download PDF from S3
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=s3_key)
        pdf_content = response['Body'].read()
        
        # Note: Similar to House PTR parsing
        logger.warning("⚠️ Senate PTR PDF parsing not fully implemented")
        
    except Exception as e:
        logger.error(f"❌ Error parsing Senate PTR {s3_key}: {e}")
    
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
    s3_key = event.get('s3Key')
    form_type = event.get('formType') or event.get('form_type')
    filing_date = event.get('filingDate') or event.get('date', '')
    # Determine source from form type or explicit source field
    if form_type and ('house' in form_type.lower() or 'house_ptr' in form_type.lower()):
        source = 'house'
    elif form_type and ('senate' in form_type.lower() or 'senate_ptr' in form_type.lower()):
        source = 'senate'
    else:
        source = event.get('source', 'sec')  # sec, house, or senate
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
        trades = []
        if source == 'sec':
            if s3_key.endswith('.xml'):
                trades = parse_sec_form_xml(s3_key)
            elif s3_key.endswith('.pdf'):
                trades = parse_sec_form_pdf(s3_key)
            else:
                logger.warning(f"⚠️ Unknown file type for {s3_key}")
                return {
                    "matchedTrades": [],
                    "unmatchedCount": 1,
                    "s3Key": s3_key,
                    "formType": form_type
                }
        elif source == 'house':
            trades = parse_house_ptr(s3_key)
        elif source == 'senate':
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
        
        for trade in trades:
            if source == 'sec':
                filer_name = trade.get('filerName')
                if not filer_name:
                    continue
                
                matched_politician = find_matching_politician(filer_name, politicians)
                
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
                politician_name = trade.get('politicianName')
                if politician_name:
                    matched_politician = find_matching_politician(politician_name, politicians)
                    if matched_politician:
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
                            'securitySymbol': trade.get('securitySymbol'),
                            'securityName': trade.get('securityName'),
                            'transactionType': trade.get('transactionType'),
                            'shares': trade.get('shares'),
                            'pricePerShare': trade.get('pricePerShare'),
                            'totalAmount': trade.get('totalAmount'),
                            'formS3Key': s3_key,
                            'matchConfidence': 1.0,
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

