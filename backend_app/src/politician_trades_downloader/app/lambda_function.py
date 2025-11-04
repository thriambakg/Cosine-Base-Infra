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
        file_path = form_data.get('filePath') or form_data.get('file_path')  # Full path from daily index if available
        
        if not all([cik, accession]):
            logger.warning(f"⚠️ Missing required fields (CIK/accession) for form download: {form_data}")
            return None
        
        # Initialize session
        session = requests.Session()
        session.headers.update({'User-Agent': SEC_USER_AGENT})
        
        # If we have a full file path from the daily index, use it directly
        if file_path:
            # file_path format: edgar/data/{CIK}/{ACCESSION}/{FILENAME}
            # Construct full URL
            full_url = f"https://www.sec.gov/Archives/{file_path}"
            logger.info(f"📥 Using file path from daily index: {full_url}")
            
            try:
                response = session.get(full_url, timeout=30)
                if response.status_code == 200:
                    file_content = response.content
                    # Determine file extension from filename
                    if filename and '.' in filename:
                        file_ext = filename.split('.')[-1].lower()
                    else:
                        file_ext = 'xml'  # Default
                    
                    # Detect content type
                    if file_ext == 'xml' or file_content.startswith(b'<?xml'):
                        content_type = 'application/xml'
                    elif file_ext == 'txt':
                        content_type = 'text/plain'
                    elif file_ext in ['htm', 'html']:
                        content_type = 'text/html'
                    else:
                        content_type = 'application/octet-stream'
                    
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
                else:
                    logger.warning(f"⚠️ HTTP {response.status_code} for index file path, falling back to URL construction")
            except Exception as e:
                logger.warning(f"⚠️ Error downloading from index file path: {e}, falling back to URL construction")
        
        # Fallback: Construct accession number with dashes (format: 0001234567-12-345678)
        # Accession numbers are 18 digits, formatted as 10-2-6
        if len(accession) == 18:
            accession_dashed = f"{accession[:10]}-{accession[10:12]}-{accession[12:]}"
        else:
            accession_dashed = accession
        
        # Build base URL
        base_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_dashed}"
        
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
                                        
                                        # Detect content type based on actual content, not filename
                                        content_start = doc_content[:1000].lower()
                                        
                                        # Check for HTML indicators
                                        is_html = any(indicator in content_start for indicator in [
                                            b'<!doctype html',
                                            b'<html',
                                            b'<head>',
                                            b'<body>',
                                            b'<style',
                                            b'sec form 4',
                                        ])
                                        
                                        # Check for XML indicators
                                        is_xml = (doc_content.startswith(b'<?xml') or 
                                                 b'<ownershipDocument' in doc_content or 
                                                 b'<document>' in doc_content or 
                                                 b'<edgarDocument' in doc_content)
                                        
                                        # Accept either HTML or XML - we can parse both
                                        if is_xml and not is_html:
                                            # Pure XML content
                                            file_content = doc_content
                                            file_ext = 'xml'
                                            content_type = 'application/xml'
                                            logger.info(f"✅ Found XML document: {doc_link}")
                                            break
                                        elif is_html:
                                            # HTML rendering - accept it, matcher will parse it
                                            file_content = doc_content
                                            file_ext = 'html'
                                            content_type = 'text/html'
                                            logger.info(f"✅ Found HTML document (will parse): {doc_link}")
                                            break
                                        elif b'<sec-header' in content_start:
                                            # SGML header - accept it, matcher can process it
                                            file_content = doc_content
                                            file_ext = 'txt'
                                            content_type = 'text/plain'
                                            logger.info(f"✅ Found SGML header: {doc_link} (will process)")
                                            break
                                        else:
                                            # Unknown format - accept it anyway, let matcher figure it out
                                            file_content = doc_content
                                            file_ext = 'txt'
                                            content_type = 'text/plain'
                                            logger.info(f"✅ Found file (format unclear): {doc_link} (will process)")
                                            break
                                except Exception as doc_error:
                                    logger.warning(f"⚠️ Could not download document link {doc_link}: {doc_error}")
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
                                        # Accept any content type - let matcher handle detection
                                        content_sample = doc_content[:100].lower()
                                        is_html = b'<html' in content_sample or b'<!doctype html' in content_sample
                                        is_xml = doc_content.startswith(b'<?xml') or b'<ownershipDocument' in doc_content or b'<document>' in doc_content
                                        
                                        if is_xml:
                                            file_content = doc_content
                                            file_ext = 'xml'
                                            content_type = 'application/xml'
                                            logger.info(f"✅ Found XML document file: {doc_url}")
                                            found_doc = True
                                            break
                                        elif is_html:
                                            file_content = doc_content
                                            file_ext = 'html'
                                            content_type = 'text/html'
                                            logger.info(f"✅ Found HTML document file: {doc_url}")
                                            found_doc = True
                                            break
                                        else:
                                            # Accept any content - matcher will handle it
                                            file_content = doc_content
                                            file_ext = 'txt'
                                            content_type = 'text/plain'
                                            logger.info(f"✅ Found document file (unknown format): {doc_url}")
                                            found_doc = True
                                            break
                                except Exception as doc_error:
                                    logger.debug(f"⚠️ Could not download candidate {doc_url}: {doc_error}")
                                    continue
                            
                            if not found_doc:
                                # If we can't find a separate document file, accept the SGML header
                                # Matcher can parse it or extract what it needs
                                logger.info(f"📄 No separate document file found, accepting SGML header for processing")
                                file_ext = 'txt'
                                content_type = 'text/plain'
                        
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
                        # Detect actual content type, not just extension
                        content_sample = file_content[:100].lower()
                        if b'<html' in content_sample or b'<!doctype html' in content_sample:
                            file_ext = 'html'
                            content_type = 'text/html'
                            logger.info(f"📄 .xml file contains HTML content, treating as HTML")
                        elif file_content.startswith(b'<?xml') or b'<ownershipDocument' in file_content or b'<document>' in file_content:
                            file_ext = 'xml'
                            content_type = 'application/xml'
                        else:
                            # Unknown format, store as-is
                            file_ext = 'txt'
                            content_type = 'text/plain'
                            logger.info(f"📄 .xml file format unclear, storing as text")
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
                            logger.info(f"📄 Detected HTML content")
                        else:
                            file_ext = 'txt'
                            content_type = 'text/plain'
                    
                    # Accept whatever content we got - matcher will detect and parse appropriately
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


def download_and_store_ptr(ptr_data: Dict[str, Any], target_date: str) -> Optional[str]:
    """
    Download PTR file from URL and store in S3
    
    Args:
        ptr_data: PTR metadata with url, source (house/senate), s3_key
        target_date: Date string for S3 key structure
        
    Returns:
        S3 key if successful, None otherwise
    """
    try:
        url = ptr_data.get('url')
        s3_key = ptr_data.get('s3_key') or ptr_data.get('s3Key')
        source = ptr_data.get('source')  # 'house' or 'senate'
        form_type = ptr_data.get('formType') or ptr_data.get('form_type', 'house_ptr' if source == 'house' else 'senate_ptr')
        
        if not url:
            logger.error(f"❌ Missing URL for PTR download: {ptr_data}")
            return None
        
        # Construct S3 key - always use YYYY-MM-DD format for target_date
        # target_date is already normalized to YYYY-MM-DD format in lambda_handler
        if not s3_key:
            # Generate filename from UUID or URL
            if source == 'senate':
                uuid = ptr_data.get('uuid', '')
                if uuid:
                    filename = f"senate-ptr-{uuid}.html"
                else:
                    # Fallback: extract from URL
                    filename = url.split('/')[-1].rstrip('/') or 'senate-ptr.html'
                s3_key = f"trades/{target_date}/senate/{filename}"
            elif source == 'house':
                filename = url.split('/')[-1] or 'house-ptr.pdf'
                s3_key = f"trades/{target_date}/house/{filename}"
            else:
                # Default to senate if source unclear
                uuid = ptr_data.get('uuid', '')
                filename = f"ptr-{uuid}.html" if uuid else 'ptr.html'
                s3_key = f"trades/{target_date}/senate/{filename}"
                logger.warning(f"⚠️ Source unclear, defaulting to senate: {ptr_data}")
        
        logger.info(f"📦 Will store PTR to S3: {s3_key}")
        
        # For Senate PTRs, the view URL already contains the transaction table in HTML
        # View URL: /search/view/ptr/{uuid}/ (contains transaction table directly)
        # Note: If transactions are already extracted in fetcher, we still need to download HTML for storage
        if source == 'senate':
            # Check if view_url is already provided (from fetcher)
            view_url = ptr_data.get('view_url')
            
            # Use view_url if provided, otherwise use url
            url_to_process = view_url if view_url else url
            
            # IMPORTANT: Always convert /view/paper/ URLs to /print/paper/ for image-based filings
            # /view/paper/ URLs contain scanned images, /print/paper/ has parseable HTML
            if '/view/paper/' in url_to_process:
                logger.info(f"⚠️ /view/paper/ URL detected - converting to /print/paper/ endpoint for parseable content")
                logger.info(f"   Original URL: {url_to_process}")
                # Extract UUID from URL
                import re
                uuid_match = re.search(r'/paper/([a-f0-9-]+)', url_to_process, re.IGNORECASE)
                if uuid_match:
                    uuid = uuid_match.group(1)
                    # Use print endpoint which should have structured data
                    print_url = f"https://efdsearch.senate.gov/search/print/paper/{uuid}/"
                    logger.info(f"🔄 Converted /view/paper/ to /print/paper/ endpoint: {print_url}")
                    url = print_url
                    logger.info(f"   Final URL to download: {url}")
                else:
                    logger.warning(f"⚠️ Could not extract UUID from /view/paper/ URL, using original: {url_to_process}")
                    url = url_to_process
            elif '/view/ptr/' in url_to_process:
                # /view/ptr/ URLs contain transaction table directly in HTML - use as-is
                logger.info(f"✅ Using /view/ptr/ URL directly (contains transaction table): {url_to_process}")
                url = url_to_process
            else:
                # Extract UUID and construct view URL if needed
                import re
                # Support both /ptr/ and /paper/ UUIDs (amendments use /paper/)
                uuid_match = re.search(r'/(?:ptr|paper)/([a-f0-9-]+)', url_to_process, re.IGNORECASE)
                if uuid_match:
                    uuid = uuid_match.group(1)
                    # Determine if this should be /paper/ or /ptr/ based on original URL
                    # Amendments and some report types use /paper/ instead of /ptr/
                    if '/paper/' in url_to_process.lower() or '/paper/' in str(ptr_data.get('link_html', '')).lower():
                        # For /paper/ URLs, use /print/paper/ endpoint for parseable content
                        print_url = f"https://efdsearch.senate.gov/search/print/paper/{uuid}/"
                        logger.info(f"🔄 Constructing /print/paper/ URL (amendment or special report): {print_url}")
                        url = print_url
                    else:
                        # For /ptr/ URLs, use /view/ptr/ which contains transaction table directly
                        view_url = f"https://efdsearch.senate.gov/search/view/ptr/{uuid}/"
                        logger.info(f"🔄 Constructing /view/ptr/ URL: {view_url}")
                        logger.info(f"💡 View URL contains transaction table directly (cheaper/faster than PDF + Textract)")
                        url = view_url
                else:
                    url = url_to_process
        
        logger.info(f"📥 Downloading PTR from {url}")
        logger.info(f"📦 Will store to S3: {s3_key}")
        
        # Initialize variables for response handling
        is_pdf = False
        html_content = None
        
        # Download the PTR file
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        # For Senate PTRs, we may need to accept agreement first to access the print page
        if source == 'senate' and 'efdsearch.senate.gov' in url:
            # First access the main search page to get session cookies and accept agreement if needed
            search_url = "https://efdsearch.senate.gov/search/"
            logger.info(f"🔍 Accessing Senate search page first to establish session: {search_url}")
            search_response = session.get(search_url, timeout=30)
            search_response.raise_for_status()
            
            # Check for session cookie from initial GET - Django might set it on first request
            initial_set_cookie = search_response.headers.get('Set-Cookie', '')
            if initial_set_cookie:
                logger.info(f"📋 Set-Cookie from initial GET: {initial_set_cookie[:200]}...")
                if 'sessionid=' in initial_set_cookie:
                    import re
                    from http.cookies import SimpleCookie
                    try:
                        cookie = SimpleCookie()
                        cookie.load(initial_set_cookie)
                        for key, morsel in cookie.items():
                            domain = '.senate.gov'
                            if 'Domain=' in initial_set_cookie:
                                domain_match = re.search(r'Domain=([^;]+)', initial_set_cookie)
                                if domain_match:
                                    domain = domain_match.group(1).strip()
                            session.cookies.set(key, morsel.value, domain=domain)
                        logger.info(f"✅ Set session cookie from initial GET")
                    except Exception as e:
                        logger.warning(f"⚠️ Could not parse Set-Cookie from initial GET: {e}")
            
            # Log all cookies after initial GET
            initial_cookies = list(session.cookies.keys())
            logger.info(f"📋 Cookies after initial GET: {initial_cookies}")
            
            # CRITICAL: Perform search FIRST to establish session cookie
            # THEN accept agreement so it's stored in that session
            # This mimics how a browser would work - search creates session, then agreement is stored in it
            logger.info(f"📋 Performing search operation FIRST to establish session cookie...")
            
            # Get CSRF token before performing search
            csrf_cookie = session.cookies.get('csrftoken') or session.cookies.get('csrfmiddlewaretoken')
            if not csrf_cookie:
                # Try to extract from HTML if not in cookies
                import re
                csrf_pattern = r'name=["\']csrfmiddlewaretoken["\'][^>]*value=["\']([^"\']+)["\']'
                csrf_match = re.search(csrf_pattern, search_response.text, re.IGNORECASE)
                if csrf_match:
                    csrf_cookie = csrf_match.group(1)
                    logger.info(f"📋 Extracted CSRF token from HTML: {csrf_cookie[:20]}...")
            
            # Perform search to establish session (use today's date if filing date not available)
            filing_date = ptr_data.get('filingDate') or ptr_data.get('filing_date', '')
            if filing_date:
                try:
                    from datetime import datetime
                    if '/' in filing_date:
                        date_obj = datetime.strptime(filing_date, '%m/%d/%Y')
                    else:
                        date_obj = datetime.strptime(filing_date, '%Y-%m-%d')
                    search_start_date = date_obj.strftime('%m/%d/%Y')
                    search_end_date = search_start_date
                except:
                    from datetime import datetime
                    today = datetime.now()
                    search_start_date = today.strftime('%m/%d/%Y')
                    search_end_date = search_start_date
            else:
                from datetime import datetime
                today = datetime.now()
                search_start_date = today.strftime('%m/%d/%Y')
                search_end_date = search_start_date
            
            # Perform AJAX search to create session cookie
            ajax_url = "https://efdsearch.senate.gov/search/report/data/"
            ajax_date_start = f"{search_start_date} 00:00:00"
            ajax_date_end = f"{search_end_date} 23:59:59"
            
            datatables_payload = {
                'draw': '1',
                'columns[0][data]': '0', 'columns[0][name]': '', 'columns[0][searchable]': 'true', 'columns[0][orderable]': 'true', 'columns[0][search][value]': '', 'columns[0][search][regex]': 'false',
                'columns[1][data]': '1', 'columns[1][name]': '', 'columns[1][searchable]': 'true', 'columns[1][orderable]': 'true', 'columns[1][search][value]': '', 'columns[1][search][regex]': 'false',
                'columns[4][data]': '4', 'columns[4][name]': '', 'columns[4][searchable]': 'true', 'columns[4][orderable]': 'true', 'columns[4][search][value]': '', 'columns[4][search][regex]': 'false',
                'order[0][column]': '1', 'order[0][dir]': 'asc',
                'order[1][column]': '0', 'order[1][dir]': 'asc',
                'start': '0', 'length': '25',
                'search[value]': '', 'search[regex]': 'false',
                'report_types': '[11]', 'filer_types': '[]',
                'submitted_start_date': ajax_date_start,
                'submitted_end_date': ajax_date_end,
                'candidate_state': '', 'senator_state': '', 'office_id': '',
                'first_name': '', 'last_name': '',
                'csrfmiddlewaretoken': csrf_cookie if csrf_cookie else ''
            }
            
            ajax_headers = {
                'Content-Type': 'application/x-www-form-urlencoded',
                'X-Requested-With': 'XMLHttpRequest',
                'X-CSRFToken': csrf_cookie if csrf_cookie else '',
                'Referer': search_url,
                'Origin': 'https://efdsearch.senate.gov'
            }
            
            logger.info(f"📋 Performing search to establish session...")
            search_ajax_response = session.post(
                ajax_url,
                data=datatables_payload,
                headers=ajax_headers,
                timeout=30
            )
            
            # Parse session cookie from search response
            search_set_cookie = search_ajax_response.headers.get('Set-Cookie', '')
            if search_set_cookie:
                logger.info(f"📋 Set-Cookie from search (establishing session): {search_set_cookie[:200]}...")
                from http.cookies import SimpleCookie
                try:
                    cookie_jar = SimpleCookie()
                    cookie_jar.load(search_set_cookie)
                    for cookie_name, morsel in cookie_jar.items():
                        cookie_value = morsel.value
                        domain = morsel.get('domain', '') or 'efdsearch.senate.gov'
                        path = morsel.get('path', '/')
                        if domain.startswith('.'):
                            domain = domain[1:]
                        from http.cookiejar import Cookie
                        cookie_obj = Cookie(
                            version=0, name=cookie_name, value=cookie_value,
                            port=None, port_specified=False,
                            domain=domain if domain else None, domain_specified=bool(domain),
                            domain_initial_dot=False,
                            path=path, path_specified=bool(path),
                            secure=morsel.get('secure', False) or 'Secure' in str(morsel),
                            expires=None, discard=False, comment=None, comment_url=None, rest={}
                        )
                        session.cookies.set_cookie(cookie_obj)
                        logger.info(f"✅ Set session cookie '{cookie_name}' from search")
                except Exception as e:
                    logger.warning(f"⚠️ Could not parse Set-Cookie from search: {e}")
            
            cookies_after_search = list(session.cookies.keys())
            logger.info(f"📋 Cookies after search (session established): {cookies_after_search}")
            
            # NOW accept agreement so it's stored in the session created by the search
            # Loop until agreement form is gone (accepting redirects back to search page)
            max_agreement_attempts = 5
            agreement_accepted = False
            
            for attempt in range(max_agreement_attempts):
                # Get current page (search page after agreement redirect, or initial search page)
                logger.info(f"📋 GETting search page (attempt {attempt + 1}/{max_agreement_attempts})...")
                current_page_response = session.get(search_url, timeout=30)
                current_page_response.raise_for_status()
                current_page_html = current_page_response.text
                
                # Log what we received
                logger.info(f"📋 Search page response status: {current_page_response.status_code}")
                logger.info(f"📋 Search page response URL: {current_page_response.url}")
                logger.info(f"📋 Search page content length: {len(current_page_html)}")
                
                # Check if agreement form is still present - look for multiple indicators
                has_agreement_form_id = 'id="agreement_form"' in current_page_html
                has_prohibition_field = 'prohibition_agreement' in current_page_html
                has_get_access = 'Get Access' in current_page_html or 'get access' in current_page_html.lower()
                has_terms_text = 'terms of use' in current_page_html.lower() or 'terms and conditions' in current_page_html.lower()
                
                # Also check for search form (if present, agreement might already be accepted)
                has_search_form = 'report_type' in current_page_html or 'submitted_start_date' in current_page_html
                
                has_agreement_form = has_agreement_form_id or (has_prohibition_field and not has_search_form) or (has_get_access and not has_search_form)
                
                logger.info(f"📋 Agreement form check:")
                logger.info(f"   - Has agreement_form id: {has_agreement_form_id}")
                logger.info(f"   - Has prohibition_agreement field: {has_prohibition_field}")
                logger.info(f"   - Has 'Get Access' text: {has_get_access}")
                logger.info(f"   - Has search form: {has_search_form}")
                logger.info(f"   - Overall: has_agreement_form = {has_agreement_form}")
                
                if not has_agreement_form:
                    logger.info(f"✅ No agreement form found - agreement already accepted OR search form is available (attempt {attempt + 1})")
                    agreement_accepted = True
                    break
                
                logger.info(f"📋 Found agreement form - accepting terms (attempt {attempt + 1}/{max_agreement_attempts})...")
                
                # Extract CSRF token and form action URL from agreement form
                import re
                agreement_csrf_pattern = r'name=["\']csrfmiddlewaretoken["\'][^>]*value=["\']([^"\']+)["\']'
                agreement_csrf_match = re.search(agreement_csrf_pattern, current_page_html, re.IGNORECASE)
                
                # Try to find the form action URL - it might be in a form tag
                form_action_pattern = r'<form[^>]*(?:id=["\']agreement_form["\']|name=["\']agreement_form["\'])[^>]*action=["\']([^"\']+)["\']'
                form_action_match = re.search(form_action_pattern, current_page_html, re.IGNORECASE)
                
                # If no action found in form tag, try to find any form with prohibition_agreement field
                if not form_action_match:
                    form_action_pattern = r'<form[^>]*>.*?prohibition_agreement.*?</form>'
                    form_match = re.search(form_action_pattern, current_page_html, re.IGNORECASE | re.DOTALL)
                    if form_match:
                        action_match = re.search(r'action=["\']([^"\']+)["\']', form_match.group(0), re.IGNORECASE)
                        if action_match:
                            form_action_match = action_match
                
                # Use form action URL if found, otherwise default to search_url
                agreement_post_url = search_url
                if form_action_match:
                    action_path = form_action_match.group(1)
                    if action_path.startswith('/'):
                        agreement_post_url = f"https://efdsearch.senate.gov{action_path}"
                    elif action_path.startswith('http'):
                        agreement_post_url = action_path
                    else:
                        agreement_post_url = f"{search_url.rstrip('/')}/{action_path}"
                    logger.info(f"📋 Found form action URL: {agreement_post_url}")
                else:
                    # Since agreement POST redirects to /search/home/, try POSTing there directly
                    home_search_url = "https://efdsearch.senate.gov/search/home/"
                    logger.info(f"📋 No form action found - will try POSTing to /search/home/ since that's where redirects go: {home_search_url}")
                    agreement_post_url = home_search_url
                
                if agreement_csrf_match:
                    agreement_csrf = agreement_csrf_match.group(1)
                    
                    # Submit agreement form to the correct URL
                    agreement_data = {
                        'prohibition_agreement': '1',
                        'csrfmiddlewaretoken': agreement_csrf
                    }
                    
                    logger.info(f"📋 Submitting agreement form to: {agreement_post_url}")
                    logger.info(f"📋 CSRF token: {agreement_csrf[:20]}...")
                    
                    # Log cookies before POST
                    cookies_before_post = list(session.cookies.keys())
                    logger.info(f"📋 Cookies before agreement POST: {cookies_before_post}")
                    for cookie_name in cookies_before_post:
                        if cookie_name not in ['csrftoken', 'csrfmiddlewaretoken']:
                            cookie_value = session.cookies.get(cookie_name)
                            logger.info(f"📋 Cookie '{cookie_name}': {cookie_value[:50] if cookie_value else 'None'}...")
                    
                    agreement_response = session.post(
                        agreement_post_url,
                        data=agreement_data,
                        headers={
                            'Content-Type': 'application/x-www-form-urlencoded',
                            'Referer': search_url,
                            'Origin': 'https://efdsearch.senate.gov',
                            'X-CSRFToken': agreement_csrf
                        },
                        allow_redirects=True
                    )
                    agreement_response.raise_for_status()
                    
                    # Log response details for debugging
                    logger.info(f"📋 Agreement POST response status: {agreement_response.status_code}")
                    logger.info(f"📋 Agreement POST response URL: {agreement_response.url}")
                    logger.info(f"📋 Agreement POST response headers: {dict(agreement_response.headers)}")
                    
                    # Check response content - does it indicate success or still show agreement form?
                    response_text_preview = agreement_response.text[:500] if agreement_response.text else ''
                    has_agreement_in_response = 'id="agreement_form"' in response_text_preview or 'prohibition_agreement' in response_text_preview
                    logger.info(f"📋 Agreement POST response contains agreement form: {has_agreement_in_response}")
                    
                    # Parse Set-Cookie headers to ensure session cookies are captured
                    set_cookie_header = agreement_response.headers.get('Set-Cookie', '')
                    if set_cookie_header:
                        logger.info(f"📋 Set-Cookie from agreement response: {set_cookie_header[:200]}...")
                        # Parse all cookies, not just sessionid
                        from http.cookies import SimpleCookie
                        try:
                            cookie_jar = SimpleCookie()
                            cookie_jar.load(set_cookie_header)
                            for cookie_name, morsel in cookie_jar.items():
                                domain = morsel.get('domain', '') or 'efdsearch.senate.gov'
                                path = morsel.get('path', '/')
                                if domain.startswith('.'):
                                    domain = domain[1:]
                                from http.cookiejar import Cookie
                                cookie_obj = Cookie(
                                    version=0, name=cookie_name, value=morsel.value,
                                    port=None, port_specified=False,
                                    domain=domain if domain else None, domain_specified=bool(domain),
                                    domain_initial_dot=False,
                                    path=path, path_specified=bool(path),
                                    secure=morsel.get('secure', False) or 'Secure' in str(morsel),
                                    expires=None, discard=False, comment=None, comment_url=None, rest={}
                                )
                                session.cookies.set_cookie(cookie_obj)
                            logger.info(f"✅ Updated session cookies from agreement response")
                            
                            # Log cookies after parsing
                            cookies_after_parse = list(session.cookies.keys())
                            logger.info(f"📋 Cookies after parsing Set-Cookie: {cookies_after_parse}")
                        except Exception as e:
                            logger.warning(f"⚠️ Could not parse Set-Cookie: {e}")
                            import traceback
                            logger.warning(f"Traceback: {traceback.format_exc()}")
                    else:
                        logger.warning(f"⚠️ No Set-Cookie header in agreement response")
                    
                    # Check if we were redirected back to search page (agreement acceptance redirects)
                    if '/search/' in agreement_response.url:
                        logger.info(f"📋 Agreement POST redirected back to search page (normal behavior)")
                        # The next iteration will check if agreement form is still present
                    else:
                        logger.info(f"📋 Agreement POST redirected to: {agreement_response.url}")
                    
                    logger.info(f"✅ Agreement POST completed (attempt {attempt + 1})")
                    
                    # Small delay to ensure server has processed the agreement
                    import time
                    time.sleep(0.1)
                else:
                    logger.warning(f"⚠️ Could not extract CSRF token from agreement form (attempt {attempt + 1})")
                    # Continue to next attempt - maybe the page changed
            
            if not agreement_accepted:
                logger.warning(f"⚠️ Could not accept agreement after {max_agreement_attempts} attempts")
                # Continue anyway - might still work
            
            # Verify cookies after agreement loop
            final_cookies_after_agreement = list(session.cookies.keys())
            logger.info(f"📋 Final cookies after agreement loop: {final_cookies_after_agreement}")
            for cookie_name in final_cookies_after_agreement:
                cookie_value = session.cookies.get(cookie_name)
                if cookie_name not in ['csrftoken', 'csrfmiddlewaretoken', 'messages']:
                    logger.info(f"📋 Session cookie '{cookie_name}': {cookie_value[:50] if cookie_value else 'None'}...")
            
            # IMPORTANT: After accepting agreement, perform the search again to simulate
            # clicking the PTR link from the search results page (which opens in a new tab)
            # This ensures we have the proper context and session state for accessing the PTR
            logger.info(f"📋 Performing search after agreement acceptance to simulate clicking PTR link from results...")
            
            # Get CSRF token for the search
            csrf_cookie = session.cookies.get('csrftoken') or session.cookies.get('csrfmiddlewaretoken')
            if not csrf_cookie:
                # Re-fetch search page to get fresh CSRF token
                search_page_refresh = session.get(search_url, timeout=30)
                search_page_refresh.raise_for_status()
                import re
                csrf_pattern = r'name=["\']csrfmiddlewaretoken["\'][^>]*value=["\']([^"\']+)["\']'
                csrf_match = re.search(csrf_pattern, search_page_refresh.text, re.IGNORECASE)
                if csrf_match:
                    csrf_cookie = csrf_match.group(1)
                    logger.info(f"📋 Extracted fresh CSRF token: {csrf_cookie[:20]}...")
            
            # Perform search with the filing date
            filing_date = ptr_data.get('filingDate') or ptr_data.get('filing_date', '')
            if filing_date:
                try:
                    from datetime import datetime
                    if '/' in filing_date:
                        date_obj = datetime.strptime(filing_date, '%m/%d/%Y')
                    else:
                        date_obj = datetime.strptime(filing_date, '%Y-%m-%d')
                    search_start_date = date_obj.strftime('%m/%d/%Y')
                    search_end_date = search_start_date
                except:
                    from datetime import datetime
                    today = datetime.now()
                    search_start_date = today.strftime('%m/%d/%Y')
                    search_end_date = search_start_date
            else:
                from datetime import datetime
                today = datetime.now()
                search_start_date = today.strftime('%m/%d/%Y')
                search_end_date = search_start_date
            
            # Perform AJAX search to get results (simulating what user sees before clicking PTR link)
            ajax_url = "https://efdsearch.senate.gov/search/report/data/"
            ajax_date_start = f"{search_start_date} 00:00:00"
            ajax_date_end = f"{search_end_date} 23:59:59"
            
            datatables_payload = {
                'draw': '1',
                'columns[0][data]': '0', 'columns[0][name]': '', 'columns[0][searchable]': 'true', 'columns[0][orderable]': 'true', 'columns[0][search][value]': '', 'columns[0][search][regex]': 'false',
                'columns[1][data]': '1', 'columns[1][name]': '', 'columns[1][searchable]': 'true', 'columns[1][orderable]': 'true', 'columns[1][search][value]': '', 'columns[1][search][regex]': 'false',
                'columns[4][data]': '4', 'columns[4][name]': '', 'columns[4][searchable]': 'true', 'columns[4][orderable]': 'true', 'columns[4][search][value]': '', 'columns[4][search][regex]': 'false',
                'order[0][column]': '1', 'order[0][dir]': 'asc',
                'order[1][column]': '0', 'order[1][dir]': 'asc',
                'start': '0', 'length': '25',
                'search[value]': '', 'search[regex]': 'false',
                'report_types': '[11]', 'filer_types': '[]',
                'submitted_start_date': ajax_date_start,
                'submitted_end_date': ajax_date_end,
                'candidate_state': '', 'senator_state': '', 'office_id': '',
                'first_name': '', 'last_name': '',
                'csrfmiddlewaretoken': csrf_cookie if csrf_cookie else ''
            }
            
            ajax_headers = {
                'Content-Type': 'application/x-www-form-urlencoded',
                'X-Requested-With': 'XMLHttpRequest',
                'X-CSRFToken': csrf_cookie if csrf_cookie else '',
                'Referer': search_url,
                'Origin': 'https://efdsearch.senate.gov'
            }
            
            logger.info(f"📋 Performing search to get results page (simulating user viewing search results)...")
            post_agreement_search_response = session.post(
                ajax_url,
                data=datatables_payload,
                headers=ajax_headers,
                timeout=30
            )
            post_agreement_search_response.raise_for_status()
            
            # Check for session cookie updates
            post_search_set_cookie = post_agreement_search_response.headers.get('Set-Cookie', '')
            if post_search_set_cookie:
                logger.info(f"📋 Set-Cookie from post-agreement search: {post_search_set_cookie[:200]}...")
            
            # Log cookies after search
            cookies_after_post_search = list(session.cookies.keys())
            logger.info(f"📋 Cookies after post-agreement search: {cookies_after_post_search}")
            for cookie_name in cookies_after_post_search:
                if cookie_name not in ['csrftoken', 'csrfmiddlewaretoken', 'messages']:
                    cookie_value = session.cookies.get(cookie_name)
                    logger.info(f"📋 Session cookie '{cookie_name}': {cookie_value[:50] if cookie_value else 'None'}...")
            
            logger.info(f"✅ Performed search after agreement - session should now be ready for PTR access")
            
            # For Senate PTRs, the view URL is what we want (it contains the transaction table)
            # No need to access a separate print URL
            # IMPORTANT: We've performed the search after accepting the agreement
            # The session cookie should now allow access to PTR URLs
        
        # Before downloading PTR, check if we need to accept agreement on the PTR page itself
        # Some PTR pages require accepting agreement directly on that page
        # IMPORTANT: The PTR link opens in a new tab in browsers, so we need to ensure
        # the session cookie is properly maintained and sent with the request
        if source == 'senate':
            logger.info(f"📋 Attempting to access PTR URL: {url}")
            
            # Log cookies before attempting access - verify session cookie is present
            pre_access_cookies = list(session.cookies.keys())
            logger.info(f"📋 Cookies before PTR access: {pre_access_cookies}")
            
            # Verify session cookie value is present and log it
            session_cookie_name = None
            session_cookie_value = None
            for cookie_name in pre_access_cookies:
                if cookie_name not in ['csrftoken', 'csrfmiddlewaretoken', 'messages']:
                    cookie_value = session.cookies.get(cookie_name)
                    if cookie_value:
                        session_cookie_name = cookie_name
                        session_cookie_value = cookie_value
                        logger.info(f"📋 Session cookie '{cookie_name}': {cookie_value[:50]}...")
                        break
            
            if not session_cookie_value:
                logger.warning(f"⚠️ No session cookie found before PTR access!")
            
            # Build cookies string manually to ensure it's sent (requests should handle this, but let's be explicit)
            cookies_dict = {}
            for cookie_name in pre_access_cookies:
                cookie_value = session.cookies.get(cookie_name)
                if cookie_value:
                    cookies_dict[cookie_name] = cookie_value
            
            logger.info(f"📋 Cookies to be sent: {list(cookies_dict.keys())}")
            
            # Use browser-like headers matching the working test script
            # requests.Session() automatically handles cookies - no need to manually set them
            browser_headers = {
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'accept-encoding': 'gzip, deflate, br',
                'accept-language': 'en-US,en;q=0.9',
                'connection': 'keep-alive',
                'host': 'efdsearch.senate.gov',
                'sec-ch-ua': '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
                'sec-fetch-dest': 'document',
                'sec-fetch-mode': 'navigate',
                'sec-fetch-site': 'none',
                'sec-fetch-user': '?1',
                'upgrade-insecure-requests': '1',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36',
                'referer': search_url if source == 'senate' else url
            }
            
            logger.info(f"📋 Making GET request with browser-like headers...")
            # Use allow_redirects=True to match browser behavior
            response = session.get(url, timeout=30, allow_redirects=True, headers=browser_headers)
            
            # Log response details
            logger.info(f"📋 PTR URL response status: {response.status_code}")
            logger.info(f"📋 PTR URL final URL: {response.url}")
            logger.info(f"📋 Content length: {len(response.content)} bytes")
            logger.info(f"📋 Content type: {response.headers.get('Content-Type', 'N/A')}")
            
            response.raise_for_status()
            
            # Check response content
            response_text = response.text
            content_type = response.headers.get('Content-Type', '').lower()
            
            # Check if this is a PDF response (for /print/paper/ endpoints)
            is_pdf = 'application/pdf' in content_type or (response.content and response.content[:4] == b'%PDF')
            
            # For /view/ptr/ URLs, check for transactions table
            # For /print/paper/ URLs, accept PDF or HTML (may be image-based, will use Textract later)
            has_transactions = 'Transactions' in response_text and 'table-striped' in response_text
            is_home_redirect = '/search/home/' in response.url or 'eFD: Home' in response.text[:500] or response.text.find('<title>eFD: Home</title>') != -1
            has_agreement_form = 'id="agreement_form"' in response.text or 'prohibition_agreement' in response.text or 'Get Access' in response.text
            
            # Check if URL is a /print/paper/ endpoint (these may return PDFs or different HTML)
            is_print_paper_url = '/print/paper/' in url
            
            logger.info(f"📋 Response analysis:")
            logger.info(f"   Is PDF: {is_pdf}")
            logger.info(f"   Is /print/paper/ URL: {is_print_paper_url}")
            logger.info(f"   Has transactions table: {has_transactions}")
            logger.info(f"   Is home redirect: {is_home_redirect}")
            logger.info(f"   Has agreement form: {has_agreement_form}")
            
            # Success criteria:
            # 1. For /view/ptr/ URLs: Must have transactions table
            # 2. For /print/paper/ URLs: Must have PDF or HTML content (not agreement form or redirect)
            # 3. Never acceptable: Agreement form or home redirect
            if has_agreement_form or is_home_redirect:
                # Agreement form or redirect = failure
                logger.error(f"❌ Session agreement not properly set - still seeing agreement form or redirect")
                logger.error(f"   Request URL: {url}")
                logger.error(f"   Response URL: {response.url}")
                logger.error(f"   Original input URL: {ptr_data.get('url', 'N/A')}")
                logger.error(f"   Original view_url: {ptr_data.get('view_url', 'N/A')}")
                raise Exception(f"Cannot access PTR URL - agreement not accepted in session. Request URL: {url}, Response URL: {response.url}")
            elif has_transactions:
                # /view/ptr/ URL with transactions table = success
                logger.info(f"✅ Successfully accessed PTR page with transactions!")
                html_content = response_text
                # Extract transaction count for logging
                import re
                transaction_count_match = re.search(r'\((\d+)\s+transaction', response_text, re.IGNORECASE)
                if transaction_count_match:
                    logger.info(f"   Found {transaction_count_match.group(1)} transaction(s) in the report")
            elif is_pdf or is_print_paper_url:
                # /print/paper/ URL returning PDF or HTML (image-based) = success
                # Will use Textract in matcher to extract transactions
                logger.info(f"✅ Successfully accessed /print/paper/ endpoint (PDF or image-based content)")
                if is_pdf:
                    html_content = None  # PDF will be stored as binary
                else:
                    html_content = response_text
            else:
                # Unexpected response format
                logger.warning(f"⚠️ Unexpected response format - no transactions table, not PDF, not /print/paper/")
                logger.warning(f"   Content preview: {response_text[:500]}")
                # Still try to use it - might be valid HTML without transactions table
                html_content = response_text
        
        # For Senate PTRs, we already have html_content from the transaction table check above
        # For other sources, we'll process the response below
        if source == 'senate':
            # Check if we got a PDF (from /print/paper/ endpoint)
            if is_pdf:
                # PDF content - store as binary
                logger.info(f"✅ Senate PTR is PDF format (from /print/paper/ endpoint)")
                file_content = response.content
                file_ext = 'pdf'
                content_type = 'application/pdf'
            elif html_content:
                # HTML content - store as text
                file_content = html_content.encode('utf-8')
                file_ext = 'html'
                content_type = 'text/html; charset=utf-8'
            else:
                # Fallback - use response content
                logger.warning(f"⚠️ html_content is None, using response.text as fallback")
                file_content = response.text.encode('utf-8')
                file_ext = 'html'
                content_type = 'text/html; charset=utf-8'
        else:
            # Check if response is actually a PDF
            content_type = response.headers.get('Content-Type', '').lower()
            content_sample = response.content[:100]
            
            if 'application/pdf' in content_type or content_sample.startswith(b'%PDF'):
                logger.info(f"✅ Downloaded PDF file ({len(response.content)} bytes)")
                file_content = response.content
                file_ext = 'pdf'
            else:
                # Determine file extension from URL or content type
                if s3_key.endswith('.pdf'):
                    file_ext = 'pdf'
                elif url.endswith('.pdf'):
                    file_ext = 'pdf'
                elif 'pdf' in content_type.lower():
                    file_ext = 'pdf'
                else:
                    # Try to extract from URL
                    if '.' in url.split('/')[-1]:
                        file_ext = url.split('/')[-1].split('.')[-1]
                    else:
                        file_ext = 'pdf'  # Default for PTRs
                
                file_content = response.content
        
        # Update s3_key with correct extension if needed
        if not s3_key.endswith(f'.{file_ext}'):
            base_key = s3_key.rsplit('.', 1)[0] if '.' in s3_key else s3_key
            s3_key = f"{base_key}.{file_ext}"
        
        logger.info(f"✅ Downloaded PTR file ({len(file_content)} bytes, type: {file_ext})")
        
        # Upload to S3
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=file_content,
            ContentType=content_type
        )
        
        logger.info(f"✅ Stored PTR to S3: {s3_key}")
        return s3_key
        
    except Exception as e:
        logger.error(f"❌ Error downloading/storing PTR: {e}")
        raise


def lambda_handler(event, context):
    """
    Lambda handler for downloading SEC forms or PTR files
    
    Expected input (from Step Functions Map state):
    
    SEC Form:
    {
        "formType": "form4",
        "cik": "1234567",
        "accessionNumber": "0001234567-12-345678",
        "filename": "0001234567-12-345678.txt",
        "filingDate": "2024-01-15"
    }
    
    PTR File:
    {
        "url": "https://clerk.house.gov/public_disc/ptr-pdfs/2025/example.pdf",
        "s3_key": "trades/2025-10-31/house/example.pdf",
        "source": "house",
        "formType": "house_ptr",
        "filingDate": "2025-10-31"
    }
    
    Returns:
    {
        "success": true,
        "s3Key": "trades/2024-01-15/sec/form4-1234567-2024-01-15.xml",
        "formType": "form4",
        "source": "sec" (or "house"/"senate")
    }
    """
    logger.info(f"🚀 Politician Trades Downloader Lambda started: {json.dumps(event)}")
    
    if not S3_BUCKET:
        raise ValueError("S3_BUCKET environment variable not set")
    
    try:
        # Extract target date from event
        # Get target_date from event and normalize to YYYY-MM-DD format
        target_date_raw = event.get('filingDate') or event.get('filing_date') or event.get('date')
        if not target_date_raw:
            raise ValueError("filingDate, filing_date, or date must be provided in event")
        
        # Normalize date to YYYY-MM-DD format for consistent S3 key structure
        from datetime import datetime
        try:
            # Try parsing various date formats
            if '/' in target_date_raw:
                # MM/DD/YYYY format (from Senate PTRs)
                date_obj = datetime.strptime(target_date_raw, '%m/%d/%Y')
            elif '-' in target_date_raw:
                # Check if it's YYYY-MM-DD or MM-DD-YYYY
                parts = target_date_raw.split('-')
                if len(parts) == 3 and len(parts[0]) == 4:
                    # Already YYYY-MM-DD
                    date_obj = datetime.strptime(target_date_raw, '%Y-%m-%d')
                elif len(parts) == 3 and len(parts[0]) <= 2:
                    # MM-DD-YYYY format (incorrect format)
                    date_obj = datetime.strptime(target_date_raw, '%m-%d-%Y')
                else:
                    # Assume YYYY-MM-DD
                    date_obj = datetime.strptime(target_date_raw, '%Y-%m-%d')
            else:
                # Try YYYYMMDD
                date_obj = datetime.strptime(target_date_raw, '%Y%m%d')
            
            # Format as YYYY-MM-DD for S3 key
            target_date = date_obj.strftime('%Y-%m-%d')
            logger.info(f"📅 Normalized date: {target_date_raw} -> {target_date}")
        except ValueError as e:
            logger.error(f"❌ Could not parse date format: {target_date_raw}")
            raise ValueError(f"Invalid date format: {target_date_raw}. Expected YYYY-MM-DD, MM/DD/YYYY, or MM-DD-YYYY")
        
        # Determine if this is a PTR download (has URL) or SEC form (has CIK/accession)
        url = event.get('url')
        source = event.get('source')
        is_ptr = bool(url) or source in ['house', 'senate']
        
        if is_ptr:
            # Download PTR file
            logger.info(f"📋 Detected PTR download request (source: {source})")
            
            # For Senate PTRs, transactions are already extracted at fetcher level
            # Just pass through the transactions if they exist
            if source == 'senate' and event.get('transactions'):
                logger.info(f"✅ Senate PTR has {len(event.get('transactions', []))} pre-extracted transactions - passing through")
                # Still download and store HTML for reference, but use pre-extracted transactions
                s3_key = download_and_store_ptr(event, target_date)
                
                if not s3_key:
                    raise Exception("Failed to download PTR - download_and_store_ptr returned None")
                
                # Return format that includes pre-extracted transactions
                return {
                    "s3Key": s3_key,
                    "s3_key": s3_key,
                    "formType": event.get('formType'),
                    "source": source,
                    "filingDate": target_date,
                    "filer_name": event.get('filer_name'),  # Always pass through filer_name
                    "transactions": event.get('transactions', []),  # Pass through pre-extracted transactions
                    "success": True
                }
            else:
                # House PTRs or Senate PTRs without pre-extracted transactions - download and parse
                s3_key = download_and_store_ptr(event, target_date)
                
                if not s3_key:
                    raise Exception("Failed to download PTR - download_and_store_ptr returned None")
                
                # Return format that matches matcher Lambda expectations
                return {
                    "s3Key": s3_key,
                    "s3_key": s3_key,  # Support both formats
                    "formType": event.get('formType'),
                    "source": source or ('house' if 'house' in str(event.get('formType', '')).lower() else 'senate'),
                    "filingDate": target_date,
                    "filer_name": event.get('filer_name'),  # Always pass through filer_name if available
                    "success": True
                }
        else:
            # Download SEC form
            logger.info(f"📋 Detected SEC form download request")
            s3_key = download_and_store_sec_form(event, target_date)
            
            if not s3_key:
                raise Exception("Failed to download form - download_and_store_sec_form returned None")
            
            # Return format that matches matcher Lambda expectations
            return {
                "s3Key": s3_key,
                "formType": event.get('formType'),
                "cik": event.get('cik'),
                "filingDate": target_date,
                "accessionNumber": event.get('accessionNumber'),
                "source": "sec",
                "success": True
            }
        
    except Exception as e:
        logger.error(f"❌ Error in downloader Lambda: {e}")
        # Raise exception so Step Functions Catch block handles it
        # Include original event data in error message for context
        error_with_context = {
            "error": str(e),
            "formType": event.get('formType') or event.get('form_type'),
            "source": event.get('source'),
            "url": event.get('url'),
            "cik": event.get('cik'),
            "accessionNumber": event.get('accessionNumber') or event.get('accession_number'),
            "filingDate": event.get('filingDate') or event.get('filing_date') or event.get('date')
        }
        raise Exception(json.dumps(error_with_context))

