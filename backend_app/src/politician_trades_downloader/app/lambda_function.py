"""
Lambda function to download a single SEC form and store it in S3.
This function is invoked in parallel via Step Functions Map state.
"""

import json
import logging
import os
import re
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
        
        # Build base URL
        base_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_dashed}"
        
        session = requests.Session()
        session.headers.update({'User-Agent': SEC_USER_AGENT})
        
        # Try to download the file
        file_content = None
        file_ext = None
        content_type = None
        
        # Build list of URLs to try
        # According to SEC EDGAR structure:
        # - Forms are stored at: /Archives/edgar/data/{CIK}/{ACCESSION}/
        # - index.htm contains links to all documents in the filing
        # - The actual XML document may be:
        #   1. {accession}-primary-document.xml
        #   2. doc4.xml or doc{N}.xml (numbered documents)
        #   3. {accession}.txt (but this often contains SGML header + XML)
        # - We should parse index.htm first to find the correct document link
        
        urls_to_try = []
        
        # Priority 1: Download index.htm to find actual document links
        index_url = f"{base_url}/index.htm"
        urls_to_try.append((index_url, "index.htm"))
        
        # Priority 2: Try known document file patterns (if index.htm fails)
        doc_urls = [
            f"{accession_dashed}-primary-document.xml",
            f"{accession_dashed}-primarydoc.xml",
            "primary-document.xml",
            "doc4.xml",  # Common for Form 4
            "doc1.xml",
            f"{accession_dashed}.xml",
        ]
        for doc_name in doc_urls:
            doc_url = f"{base_url}/{doc_name}"
            urls_to_try.append((doc_url, doc_name))
        
        # Priority 3: Try .txt file (often contains SGML + XML, needs parsing)
        txt_url = f"{base_url}/{accession_dashed}.txt"
        urls_to_try.append((txt_url, f"{accession_dashed}.txt"))
        
        # Priority 4: If RSS feed provided a specific filename, try it
        if filename and filename not in [doc for _, doc in urls_to_try]:
            file_url = f"{base_url}/{filename}"
            urls_to_try.append((file_url, filename))
        
        failed_attempts = []  # Store failed attempts with status codes
        
        for file_url, file_name in urls_to_try:
            try:
                logger.info(f"📥 Attempting to download: {file_url}")
                response = session.get(file_url, timeout=30)
                
                logger.info(f"📊 HTTP {response.status_code} for {file_url}")
                
                if response.status_code == 200:
                    file_content = response.content
                    
                    # Determine file extension and content type
                    if file_name.endswith('.htm') or file_name.endswith('.html'):
                        # For HTML files, try to find the primary document link
                        # SEC index.htm files contain links to the actual form documents
                        file_ext = 'html'
                        content_type = 'text/html'
                        
                        # Check if HTML contains document links we should follow
                        try:
                            html_text = file_content.decode('utf-8', errors='ignore')
                            
                            # SEC index.htm has a specific structure:
                            # - Document table with rows containing links
                            # - Links are relative to the filing directory
                            # - XML documents are typically named: doc{N}.xml or {accession}-primary-document.xml
                            # - Priority: Look for .xml files first, especially doc4.xml (common for Form 4)
                            
                            doc_links = []
                            
                            # Strategy 1: Find all .xml file links (prioritize these over .txt)
                            # Look for links ending in .xml in the same directory
                            xml_pattern = r'href="([^"]*\.xml[^"]*)"'
                            xml_matches = re.findall(xml_pattern, html_text, re.IGNORECASE)
                            doc_links.extend(xml_matches)
                            
                            # Strategy 2: Look for primary document patterns (highest priority)
                            primary_patterns = [
                                r'href="([^"]*primary[_-]?document[^"]*\.xml[^"]*)"',
                                r'href="([^"]*primarydoc[^"]*\.xml[^"]*)"',
                                r'href="([^"]*document[^"]*\.xml[^"]*)"',
                                # Common SEC naming: doc4.xml for Form 4
                                r'href="([^"]*doc\d+\.xml[^"]*)"',
                            ]
                            primary_links = []
                            for pattern in primary_patterns:
                                matches = re.findall(pattern, html_text, re.IGNORECASE)
                                primary_links.extend(matches)
                            # Prepend primary links to prioritize them
                            doc_links = primary_links + [link for link in doc_links if link not in primary_links]
                            
                            # Strategy 3: If no XML found, look for .txt files (last resort, contains SGML+XML)
                            if not doc_links:
                                txt_pattern = r'href="([^"]*\.txt[^"]*)"'
                                txt_matches = re.findall(txt_pattern, html_text, re.IGNORECASE)
                                doc_links.extend(txt_matches)
                            
                            # Strategy 4: Look for links with accession number
                            if not doc_links:
                                acc_pattern = rf'href="([^"]*{re.escape(accession_dashed)}[^"]*)"'
                                acc_matches = re.findall(acc_pattern, html_text, re.IGNORECASE)
                                doc_links.extend(acc_matches)
                            
                            # Remove duplicates while preserving order
                            seen = set()
                            unique_doc_links = []
                            for link in doc_links:
                                if link not in seen:
                                    seen.add(link)
                                    unique_doc_links.append(link)
                            
                            # Try each found link (prioritize XML files)
                            # Sort: XML files first, then others
                            def link_priority(link):
                                if link.endswith('.xml'):
                                    return 0  # Highest priority
                                elif 'primary' in link.lower() or 'document' in link.lower():
                                    return 1
                                elif 'doc' in link.lower():
                                    return 2
                                else:
                                    return 3
                            
                            sorted_links = sorted(unique_doc_links, key=link_priority)
                            
                            for doc_link in sorted_links[:10]:  # Try up to 10 links
                                # Handle relative URLs
                                if doc_link.startswith('/'):
                                    doc_link = f"https://www.sec.gov{doc_link}"
                                elif not doc_link.startswith('http'):
                                    doc_link = f"{base_url}/{doc_link}"
                                
                                # Skip if it's the same URL we just tried
                                if doc_link == file_url:
                                    continue
                                
                                logger.info(f"🔗 Found document link in HTML, trying: {doc_link}")
                                try:
                                    doc_response = session.get(doc_link, timeout=30)
                                    if doc_response.status_code == 200:
                                        doc_content = doc_response.content
                                        # Prefer XML/structured content over HTML
                                        if doc_content.startswith(b'<?xml') or doc_link.endswith('.xml'):
                                            file_content = doc_content
                                            file_ext = 'xml'
                                            content_type = 'application/xml'
                                            logger.info(f"✅ Successfully extracted XML document from HTML link")
                                            break
                                        elif doc_link.endswith('.txt') or doc_content.startswith(b'<'):
                                            # Check if it's XML content
                                            if doc_content.startswith(b'<?xml') or (b'<ownershipDocument' in doc_content or b'<document>' in doc_content):
                                                file_content = doc_content
                                                file_ext = 'xml'
                                                content_type = 'application/xml'
                                                logger.info(f"✅ Successfully extracted XML document from HTML link (.txt file)")
                                                break
                                except Exception as doc_error:
                                    logger.debug(f"⚠️ Could not download document link {doc_link}: {doc_error}")
                                    continue
                                    
                        except Exception as html_parse_error:
                            logger.warning(f"⚠️ Could not parse HTML for document links: {html_parse_error}")
                            # If HTML parsing fails, we'll store the HTML (not ideal, but better than nothing)
                        
                    elif file_name.endswith('.txt'):
                        # SEC .txt files are typically XML-structured documents
                        # But sometimes they're SGML headers or HTML
                        content_lower = file_content.lower()
                        
                        # Check if it's an SGML header file (starts with header tags)
                        if b'<sec-header' in content_lower or b'<acceptance-datetime' in content_lower or b'.hdr.sgml' in file_content:
                            # This is an SGML header, not the actual document
                            logger.warning(f"⚠️ Downloaded file is an SGML header, not the document. Looking for actual document file...")
                            
                            # Try to find the actual document file
                            doc_candidates = [
                                f"{accession_dashed}-primary-document.xml",
                                f"{accession_dashed}-primarydoc.xml",
                                "primary-document.xml",
                                "doc4.xml",  # Common document file
                                "doc1.xml",
                                "doc2.xml",
                                "doc3.xml",
                                f"{accession_dashed}.xml",
                            ]
                            
                            found_doc = False
                            for doc_candidate in doc_candidates:
                                doc_url = f"{base_url}/{doc_candidate}"
                                try:
                                    logger.info(f"🔍 Trying document candidate: {doc_url}")
                                    doc_response = session.get(doc_url, timeout=30)
                                    if doc_response.status_code == 200:
                                        doc_content = doc_response.content
                                        # Check if it's actual XML content
                                        if doc_content.startswith(b'<?xml') or b'<ownershipDocument' in doc_content or b'<document>' in doc_content:
                                            file_content = doc_content
                                            file_ext = 'xml'
                                            content_type = 'application/xml'
                                            logger.info(f"✅ Found actual document file: {doc_url}")
                                            found_doc = True
                                            break
                                except Exception as doc_error:
                                    logger.debug(f"⚠️ Could not download candidate {doc_url}: {doc_error}")
                                    continue
                            
                            if not found_doc:
                                # If we can't find the document, don't store the SGML header
                                # Continue to try next URL in urls_to_try (e.g., try .xml file)
                                logger.warning(f"⚠️ Could not find actual document file for {accession_dashed}, will try next URL option")
                                file_content = None  # Reset to None so we continue the loop
                                continue  # Continue to next URL instead of storing SGML header
                        
                        elif file_content.startswith(b'<?xml'):
                            file_ext = 'xml'
                            content_type = 'application/xml'
                        elif b'<ownershipDocument' in file_content or b'<document>' in file_content or b'<XBRL>' in file_content:
                            # XML content without XML declaration
                            file_ext = 'xml'
                            content_type = 'application/xml'
                        elif b'<html' in content_lower or b'<!doctype html' in content_lower:
                            # This is HTML, not XML - try to extract XML link
                            logger.warning(f"⚠️ .txt file contains HTML, not XML. File might be misnamed.")
                            file_ext = 'txt'  # Store as-is, matcher will need to handle HTML
                            content_type = 'text/html'
                        else:
                            # Plain text or unknown format
                            file_ext = 'txt'
                            content_type = 'text/plain'
                    elif file_name.endswith('.xml'):
                        file_ext = 'xml'
                        content_type = 'application/xml'
                        # Validate it's actually XML
                        if not (file_content.startswith(b'<?xml') or b'<ownershipDocument' in file_content or b'<document>' in file_content):
                            logger.warning(f"⚠️ .xml file doesn't appear to contain XML content")
                    elif file_name.endswith('.pdf'):
                        file_ext = 'pdf'
                        content_type = 'application/pdf'
                    else:
                        # Try to determine from content
                        if file_content.startswith(b'<?xml') or b'<ownershipDocument' in file_content or b'<document>' in file_content:
                            file_ext = 'xml'
                            content_type = 'application/xml'
                        elif b'<html' in file_content.lower():
                            file_ext = 'html'
                            content_type = 'text/html'
                            logger.warning(f"⚠️ Downloaded file appears to be HTML, not structured data")
                        else:
                            file_ext = 'txt'
                            content_type = 'text/plain'
                    
                    # Final validation: Check if we got actual document content
                    # SEC forms should be XML with ownershipDocument or document tags
                    if file_content and file_ext in ['xml', 'txt']:
                        content_lower = file_content.lower()
                        # Check if we have actual form content
                        if (file_ext == 'xml' and not (
                            file_content.startswith(b'<?xml') or 
                            b'<ownershipDocument' in file_content or 
                            b'<document' in file_content or
                            b'<xbrl' in content_lower
                        )):
                            logger.warning(f"⚠️ XML file doesn't appear to contain form document structure")
                        elif (file_ext == 'txt' and b'<sec-header' in content_lower):
                            logger.warning(f"⚠️ Downloaded file is SGML header, actual document not found")
                    
                    # Only accept this as successful if we got actual content
                    # Don't accept HTML/SGML headers as successful downloads of structured data
                    if file_content:
                        logger.info(f"✅ Successfully downloaded: {file_url} ({len(file_content)} bytes, type: {file_ext})")
                        break
                else:
                    failed_attempts.append(f"{file_url} (HTTP {response.status_code})")
                    logger.warning(f"⚠️ HTTP {response.status_code} for {file_url}, trying next option...")
                    
            except requests.exceptions.Timeout as e:
                failed_attempts.append(f"{file_url} (Timeout)")
                logger.warning(f"⚠️ Timeout downloading {file_url}: {e}")
                continue
            except requests.exceptions.RequestException as e:
                failed_attempts.append(f"{file_url} (Error: {str(e)})")
                logger.warning(f"⚠️ Could not download {file_url}: {e}")
                continue
        
        if not file_content:
            # Log more details about what we tried
            attempted_urls = [url for url, _ in urls_to_try]
            error_msg = f"Could not download form - all URLs failed: {failed_attempts}"
            logger.error(f"❌ Could not download form for CIK {cik}, accession {accession_dashed}")
            logger.error(f"   Attempted URLs: {attempted_urls}")
            logger.error(f"   Failed attempts: {failed_attempts}")
            raise Exception(error_msg)
        
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
        # Raise exception so Step Functions Catch block handles it
        # Include original event data in error message for context
        error_with_context = {
            "error": str(e),
            "formType": event.get('formType') or event.get('form_type'),
            "cik": event.get('cik'),
            "accessionNumber": event.get('accessionNumber') or event.get('accession_number'),
            "filingDate": event.get('filingDate') or event.get('date')
        }
        raise Exception(json.dumps(error_with_context))

