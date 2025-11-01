"""
Lambda function to download a single SEC form and store it in S3.
This function is invoked in parallel via Step Functions Map state.
"""

import json
import logging
import os
from typing import Dict, Any, Optional

import boto3
import requests

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
s3_client = boto3.client('s3')

# Environment variables
S3_BUCKET = os.environ.get('S3_BUCKET')

# SEC User Agent requirement
SEC_USER_AGENT = os.environ.get('SEC_USER_AGENT', 'Company Name admin@company.com')


def download_and_store_sec_form(form_data: Dict[str, Any], target_date: str) -> Optional[str]:
    """
    Download SEC form file and store in S3
    
    Args:
        form_data: Form metadata (CIK, accession number, form type, filename)
        target_date: Date string for S3 key structure
        
    Returns:
        S3 key if successful, None otherwise
    """
    try:
        # Construct SEC filing URL
        # Format: https://www.sec.gov/Archives/edgar/data/{CIK}/{accession_no}/{filename}
        
        cik = form_data.get('cik')
        accession = form_data.get('accessionNumber') or form_data.get('accession_number')
        form_type = form_data.get('formType') or form_data.get('form_type')
        filename = form_data.get('filename')
        
        if not all([cik, accession]):
            logger.warning(f"⚠️ Missing required fields (CIK/accession) for form download: {form_data}")
            return None
        
        # Construct accession number with dashes (format: 0001234567-12-345678)
        # Accession numbers are 18 digits, formatted as 10-2-6
        if len(accession) == 18:
            accession_dashed = f"{accession[:10]}-{accession[10:12]}-{accession[12:]}"
        else:
            accession_dashed = accession
        
        # Determine filename and file type
        # Try .txt first (most common), then .xml if not found
        if not filename:
            filename = f'{accession_dashed}.txt'
        
        # Build base URL
        base_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_dashed}"
        
        session = requests.Session()
        session.headers.update({'User-Agent': SEC_USER_AGENT})
        
        # Try to download the file - try .txt first, then .xml
        file_content = None
        file_ext = None
        content_type = None
        
        file_extensions = [
            ('.txt', 'application/xml'),  # .txt files are usually XML content
            ('.xml', 'application/xml'),
            ('.pdf', 'application/pdf')
        ]
        
        # If filename already has extension, try that first
        if filename.endswith(('.txt', '.xml', '.pdf')):
            ext = filename[filename.rfind('.'):]
            for fe, ct in file_extensions:
                if ext == fe:
                    file_extensions.insert(0, (fe, ct))
                    break
        
        for ext, ct in file_extensions:
            try:
                if filename.endswith(ext):
                    file_url = f"{base_url}/{filename}"
                else:
                    file_url = f"{base_url}/{accession_dashed}{ext}"
                
                logger.debug(f"📥 Attempting to download: {file_url}")
                response = session.get(file_url, timeout=30)
                
                if response.status_code == 200:
                    file_content = response.content
                    file_ext = ext[1:]  # Remove the dot
                    content_type = ct
                    
                    # For .txt files, check if it's actually XML (common for SEC forms)
                    if ext == '.txt' and file_content.startswith(b'<?xml'):
                        file_ext = 'xml'
                        content_type = 'application/xml'
                    
                    logger.info(f"✅ Successfully downloaded: {file_url}")
                    break
                else:
                    logger.debug(f"⚠️ HTTP {response.status_code} for {file_url}, trying next extension...")
                    
            except requests.exceptions.RequestException as e:
                logger.debug(f"⚠️ Could not download {file_url}: {e}")
                continue
        
        if not file_content:
            # Log more details about what we tried
            attempted_urls = []
            for ext, ct in file_extensions:
                if filename.endswith(ext):
                    attempted_urls.append(f"{base_url}/{filename}")
                else:
                    attempted_urls.append(f"{base_url}/{accession_dashed}{ext}")
            logger.error(f"❌ Could not download form for CIK {cik}, accession {accession_dashed}")
            logger.error(f"   Attempted URLs: {attempted_urls}")
            raise Exception(f"Could not download form - all URLs failed: {attempted_urls}")
        
        # Generate S3 key
        s3_key = f"trades/{target_date}/sec/{form_type}-{cik}-{target_date}.{file_ext}"
        
        # Upload to S3
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=file_content,
            ContentType=content_type
        )
        
        logger.info(f"✅ Stored SEC form to S3: {s3_key}")
        return s3_key
        
    except Exception as e:
        logger.error(f"❌ Error downloading/storing SEC form: {e}")
        raise


def lambda_handler(event, context):
    """
    Lambda handler for downloading a single SEC form
    
    Expected input (from Step Functions Map state):
    {
        "formType": "form4",
        "cik": "1234567",
        "accessionNumber": "0001234567-12-345678",
        "filename": "0001234567-12-345678.txt",
        "filingDate": "2024-01-15"
    }
    
    Returns:
    {
        "success": true,
        "s3Key": "trades/2024-01-15/sec/form4-1234567-2024-01-15.xml",
        "formType": "form4",
        "cik": "1234567"
    }
    """
    logger.info(f"🚀 Politician Trades Downloader Lambda started: {json.dumps(event)}")
    
    if not S3_BUCKET:
        raise ValueError("S3_BUCKET environment variable not set")
    
    try:
        # Extract target date from event
        target_date = event.get('filingDate') or event.get('date')
        if not target_date:
            raise ValueError("filingDate or date must be provided in event")
        
        # Download and store the form
        s3_key = download_and_store_sec_form(event, target_date)
        
        if not s3_key:
            raise Exception("Failed to download form - download_and_store_sec_form returned None")
        
        # Return format that matches matcher Lambda expectations
        return {
            "s3Key": s3_key,
            "formType": event.get('formType') or event.get('form_type'),
            "cik": event.get('cik'),
            "filingDate": target_date,
            "accessionNumber": event.get('accessionNumber') or event.get('accession_number'),
            "success": True
        }
        
    except Exception as e:
        logger.error(f"❌ Error in downloader Lambda: {e}")
        # Return None/empty to indicate failure (will be filtered out)
        return {
            "success": False,
            "error": str(e),
            "formType": event.get('formType') or event.get('form_type'),
            "cik": event.get('cik'),
            "filingDate": event.get('filingDate') or event.get('date')
        }

