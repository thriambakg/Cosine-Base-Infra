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
        
        # If s3_key not provided, construct it from source and filename
        if not s3_key:
            filename = url.split('/')[-1]
            if source == 'house':
                s3_key = f"trades/{target_date}/house/{filename}"
            elif source == 'senate':
                s3_key = f"trades/{target_date}/senate/{filename}"
            else:
                # Default to house if source unclear
                s3_key = f"trades/{target_date}/house/{filename}"
                logger.warning(f"⚠️ Source unclear, defaulting to house: {ptr_data}")
        
        # For Senate PTRs, the view URL already contains the transaction table in HTML
        # View URL: /search/view/ptr/{uuid}/ (contains transaction table directly)
        # Note: If transactions are already extracted in fetcher, we still need to download HTML for storage
        if source == 'senate':
            # Check if view_url is already provided (from fetcher)
            view_url = ptr_data.get('view_url')
            
            if view_url:
                logger.info(f"✅ Using provided view URL from fetcher: {view_url}")
                url = view_url
            elif '/view/ptr/' in url:
                # URL is already the view URL - no conversion needed
                # The view URL already has the transaction table
                logger.info(f"✅ Using view URL directly (contains transaction table): {url}")
            else:
                # Extract UUID and construct view URL if needed
                import re
                uuid_match = re.search(r'/ptr/([a-f0-9-]+)', url, re.IGNORECASE)
                if uuid_match:
                    uuid = uuid_match.group(1)
                    view_url = f"https://efdsearch.senate.gov/search/view/ptr/{uuid}/"
                    logger.info(f"🔄 Constructing view URL: {view_url}")
                    logger.info(f"💡 View URL contains transaction table directly (cheaper/faster than PDF + Textract)")
                    url = view_url
        
        logger.info(f"📥 Downloading PTR from {url}")
        logger.info(f"📦 Will store to S3: {s3_key}")
        
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
            
            # Check if agreement form is present
            search_html = search_response.text
            if 'id="agreement_form"' in search_html or 'prohibition_agreement' in search_html:
                logger.info("📋 Found agreement form - accepting terms...")
                
                # Extract CSRF token from agreement form
                import re
                agreement_csrf_pattern = r'name=["\']csrfmiddlewaretoken["\'][^>]*value=["\']([^"\']+)["\']'
                agreement_csrf_match = re.search(agreement_csrf_pattern, search_html, re.IGNORECASE)
                
                if agreement_csrf_match:
                    agreement_csrf = agreement_csrf_match.group(1)
                    
                    # Submit agreement form
                    agreement_data = {
                        'prohibition_agreement': '1',
                        'csrfmiddlewaretoken': agreement_csrf
                    }
                    
                    logger.info(f"📋 Submitting agreement form...")
                    agreement_response = session.post(
                        search_url,
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
                    
                    # Parse Set-Cookie headers to ensure sessionid is captured
                    set_cookie_header = agreement_response.headers.get('Set-Cookie', '')
                    if set_cookie_header:
                        logger.info(f"📋 Set-Cookie from agreement response: {set_cookie_header[:200]}...")
                        # Manually parse sessionid if present
                        if 'sessionid=' in set_cookie_header:
                            import re
                            from http.cookies import SimpleCookie
                            try:
                                cookie = SimpleCookie()
                                cookie.load(set_cookie_header)
                                for key, morsel in cookie.items():
                                    domain = '.senate.gov'
                                    if 'Domain=' in set_cookie_header:
                                        domain_match = re.search(r'Domain=([^;]+)', set_cookie_header)
                                        if domain_match:
                                            domain = domain_match.group(1).strip()
                                    session.cookies.set(key, morsel.value, domain=domain)
                                logger.info(f"✅ Manually set session cookie from agreement response")
                            except Exception as e:
                                logger.warning(f"⚠️ Could not parse Set-Cookie: {e}")
                    
                    # Verify session cookie - Django might use a different name or create it later
                    sessionid_cookie = session.cookies.get('sessionid')
                    
                    # Check all cookies - sometimes Django uses different session cookie names
                    all_cookies = list(session.cookies.keys())
                    logger.info(f"📋 All cookies after agreement: {all_cookies}")
                    
                    # Look for any cookie that might be a session cookie (long alphanumeric strings)
                    # Django sometimes uses different cookie names or the sessionid might be in a different format
                    potential_session_cookies = []
                    for cookie_name in all_cookies:
                        if cookie_name not in ['csrftoken', 'csrfmiddlewaretoken', 'messages']:
                            cookie_value = session.cookies.get(cookie_name)
                            if cookie_value:
                                if len(cookie_value) > 20:  # Session cookies are usually long
                                    potential_session_cookies.append((cookie_name, cookie_value))
                                    logger.info(f"📋 Found potential session cookie '{cookie_name}': {cookie_value[:50]}...")
                                # Also check if the cookie name itself looks like a session ID
                                elif len(cookie_name) > 20 and cookie_name.replace('-', '').replace('_', '').isalnum():
                                    potential_session_cookies.append((cookie_name, cookie_value))
                                    logger.info(f"📋 Cookie name '{cookie_name}' might be session ID itself: {cookie_value[:50]}...")
                    
                    # If we found a potential session cookie but no 'sessionid', treat it as the session
                    if not sessionid_cookie and potential_session_cookies:
                        logger.info(f"💡 No 'sessionid' cookie, but found {len(potential_session_cookies)} potential session cookies")
                        logger.info(f"💡 Django may be using a different session cookie name - proceeding with available cookies")
                    
                    if sessionid_cookie:
                        logger.info(f"✅ Session ID cookie present after agreement: {sessionid_cookie[:50]}...")
                    else:
                        logger.warning(f"⚠️ No sessionid cookie after agreement acceptance - Django may create it on first access")
                        logger.info(f"💡 Will attempt to access PTR URL - Django may set session cookie on first request")
                    
                    logger.info("✅ Agreement accepted")
                else:
                    logger.warning("⚠️ Could not extract CSRF token from agreement form")
            
            # Get CSRF cookie if available
            csrf_cookie = session.cookies.get('csrftoken') or session.cookies.get('csrfmiddlewaretoken')
            if csrf_cookie:
                logger.debug(f"✅ Have CSRF cookie: {csrf_cookie[:20]}...")
            
            # For Senate PTRs, the view URL is what we want (it contains the transaction table)
            # No need to access a separate print URL
        
        # Before downloading PTR, check if we need to accept agreement on the PTR page itself
        # Some PTR pages require accepting agreement directly on that page
        if source == 'senate':
            logger.info(f"📋 Attempting to access PTR URL: {url}")
            
            # Log cookies before attempting access
            pre_access_cookies = list(session.cookies.keys())
            logger.info(f"📋 Cookies before PTR access: {pre_access_cookies}")
            
            # First attempt - with current session cookies
            # Use allow_redirects=False first to see if we get redirected
            response = session.get(url, timeout=30, allow_redirects=False, headers={
                'Referer': search_url if source == 'senate' else url,
                'User-Agent': session.headers.get('User-Agent'),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1'
            })
            
            # Check if we got redirected
            if response.status_code in [301, 302, 303, 307, 308]:
                redirect_location = response.headers.get('Location', '')
                logger.info(f"📋 PTR URL returned {response.status_code} redirect to: {redirect_location}")
                # Follow redirect manually to see where we end up
                if redirect_location.startswith('/'):
                    redirect_location = f"https://efdsearch.senate.gov{redirect_location}"
                elif not redirect_location.startswith('http'):
                    redirect_location = f"https://efdsearch.senate.gov/{redirect_location}"
                logger.info(f"📋 Following redirect to: {redirect_location}")
                response = session.get(redirect_location, timeout=30, allow_redirects=True, headers={
                    'Referer': url,
                    'User-Agent': session.headers.get('User-Agent')
                })
            else:
                response.raise_for_status()
            
            # Check if we got redirected to home page OR got agreement form on PTR page
            is_home_redirect = '/search/home/' in response.url or 'eFD: Home' in response.text[:500] or response.text.find('<title>eFD: Home</title>') != -1
            has_agreement_form = 'id="agreement_form"' in response.text or 'prohibition_agreement' in response.text or 'Get Access' in response.text
            
            if is_home_redirect:
                logger.warning(f"⚠️ Got redirected to home page - trying to accept agreement on PTR page directly")
                # Try accessing the PTR URL and accepting agreement there
                # Sometimes the agreement needs to be accepted on the specific page
                logger.info(f"📋 Re-fetching PTR URL to get agreement form...")
                ptr_page_response = session.get(url, timeout=30, allow_redirects=False, headers={
                    'Referer': search_url,
                    'User-Agent': session.headers.get('User-Agent')
                })
                
                # Check response - might be 302 redirect or 200 with agreement form
                if ptr_page_response.status_code == 302:
                    logger.info(f"📋 PTR URL redirects to: {ptr_page_response.headers.get('Location', 'unknown')}")
                    # Follow redirect manually
                    redirect_url = ptr_page_response.headers.get('Location', '')
                    if redirect_url.startswith('/'):
                        redirect_url = f"https://efdsearch.senate.gov{redirect_url}"
                    logger.info(f"📋 Following redirect to: {redirect_url}")
                    ptr_page_response = session.get(redirect_url, timeout=30, allow_redirects=True)
                
                ptr_html = ptr_page_response.text
                if 'id="agreement_form"' in ptr_html or 'prohibition_agreement' in ptr_html:
                    logger.info(f"📋 Found agreement form on PTR page - accepting...")
                    import re
                    agreement_csrf_pattern = r'name=["\']csrfmiddlewaretoken["\'][^>]*value=["\']([^"\']+)["\']'
                    agreement_csrf_match = re.search(agreement_csrf_pattern, ptr_html, re.IGNORECASE)
                    
                    if agreement_csrf_match:
                        ptr_csrf = agreement_csrf_match.group(1)
                        # POST agreement to the PTR URL itself
                        agreement_post_response = session.post(
                            url,
                            data={'prohibition_agreement': '1', 'csrfmiddlewaretoken': ptr_csrf},
                            headers={
                                'Content-Type': 'application/x-www-form-urlencoded',
                                'Referer': url,
                                'Origin': 'https://efdsearch.senate.gov',
                                'X-CSRFToken': ptr_csrf
                            },
                            allow_redirects=True
                        )
                        agreement_post_response.raise_for_status()
                        
                        # Check Set-Cookie from agreement POST
                        post_set_cookie = agreement_post_response.headers.get('Set-Cookie', '')
                        if post_set_cookie:
                            logger.info(f"📋 Set-Cookie from PTR agreement POST: {post_set_cookie[:200]}...")
                            # Parse sessionid if present
                            if 'sessionid=' in post_set_cookie:
                                from http.cookies import SimpleCookie
                                try:
                                    cookie = SimpleCookie()
                                    cookie.load(post_set_cookie)
                                    for key, morsel in cookie.items():
                                        domain = '.senate.gov'
                                        if 'Domain=' in post_set_cookie:
                                            domain_match = re.search(r'Domain=([^;]+)', post_set_cookie)
                                            if domain_match:
                                                domain = domain_match.group(1).strip()
                                        session.cookies.set(key, morsel.value, domain=domain)
                                    logger.info(f"✅ Set session cookie from PTR agreement POST")
                                except Exception as e:
                                    logger.warning(f"⚠️ Could not parse Set-Cookie: {e}")
                        
                        # Now try accessing the PTR page again
                        logger.info(f"📋 Re-accessing PTR URL after accepting agreement on page...")
                        response = session.get(url, timeout=30, allow_redirects=True, headers={
                            'Referer': url,
                            'User-Agent': session.headers.get('User-Agent')
                        })
                        response.raise_for_status()
                        
                        # Verify we got the PTR page, not home page
                        if '/search/home/' in response.url or 'eFD: Home' in response.text[:500]:
                            raise Exception(f"Still redirected to home page after accepting agreement on PTR page")
                        logger.info(f"✅ Successfully accessed PTR page after accepting agreement")
                    else:
                        raise Exception(f"Could not extract CSRF token from PTR agreement form")
                else:
                    raise Exception(f"PTR page does not contain agreement form but still redirects")
            elif has_agreement_form:
                # We got the agreement form on the PTR page - accept it
                logger.info(f"📋 Found agreement form on PTR page - accepting...")
                import re
                agreement_csrf_pattern = r'name=["\']csrfmiddlewaretoken["\'][^>]*value=["\']([^"\']+)["\']'
                agreement_csrf_match = re.search(agreement_csrf_pattern, response.text, re.IGNORECASE)
                
                if agreement_csrf_match:
                    ptr_csrf = agreement_csrf_match.group(1)
                    agreement_post_response = session.post(
                        url,
                        data={'prohibition_agreement': '1', 'csrfmiddlewaretoken': ptr_csrf},
                        headers={
                            'Content-Type': 'application/x-www-form-urlencoded',
                            'Referer': url,
                            'Origin': 'https://efdsearch.senate.gov',
                            'X-CSRFToken': ptr_csrf
                        },
                        allow_redirects=True
                    )
                    agreement_post_response.raise_for_status()
                    
                    # If POST response contains the PTR content, use it
                    if 'Transactions' in agreement_post_response.text or 'table-striped' in agreement_post_response.text:
                        logger.info(f"✅ Agreement POST response contains PTR content - using it")
                        response = agreement_post_response
                    else:
                        # Retry GET request
                        logger.info(f"📋 Re-accessing PTR URL after agreement acceptance...")
                        response = session.get(url, timeout=30, allow_redirects=True, headers={
                            'Referer': url,
                            'User-Agent': session.headers.get('User-Agent')
                        })
                        response.raise_for_status()
                else:
                    raise Exception(f"Could not extract CSRF token from PTR agreement form")
            else:
                # We got the actual PTR page - success!
                logger.info(f"✅ Successfully accessed PTR page")
        
        # Check if response is actually a PDF
        content_type = response.headers.get('Content-Type', '').lower()
        content_sample = response.content[:100]
        
        if 'application/pdf' in content_type or content_sample.startswith(b'%PDF'):
            logger.info(f"✅ Downloaded PDF file ({len(response.content)} bytes)")
            file_content = response.content
            file_ext = 'pdf'
        elif source == 'senate' and ('text/html' in content_type or content_sample.startswith(b'<html') or content_sample.startswith(b'<!DOCTYPE')):
            # Senate PTR print page returns HTML with transaction table
            # We prefer HTML over PDF because:
            # 1. It's free (no Textract costs)
            # 2. It's faster (direct parsing vs Textract processing)
            # 3. It's more reliable (structured HTML vs OCR extraction)
            # The HTML contains the same transaction data in a structured table
            logger.info(f"📄 Senate PTR is HTML format - storing HTML page with transaction table")
            logger.info(f"💡 Matcher will parse HTML table directly (cheaper/faster than PDF + Textract)")
            file_content = response.content
            file_ext = 'html'
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
        target_date = event.get('filingDate') or event.get('filing_date') or event.get('date')
        if not target_date:
            raise ValueError("filingDate, filing_date, or date must be provided in event")
        
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
                    "formType": event.get('formType') or event.get('form_type'),
                    "form_type": event.get('formType') or event.get('form_type'),
                    "source": source,
                    "filingDate": target_date,
                    "filing_date": target_date,
                    "filer_name": event.get('filer_name'),
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
                    "formType": event.get('formType') or event.get('form_type'),
                    "form_type": event.get('formType') or event.get('form_type'),
                    "source": source or ('house' if 'house' in str(event.get('formType', '')).lower() else 'senate'),
                    "filingDate": target_date,
                    "filing_date": target_date,
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
                "formType": event.get('formType') or event.get('form_type'),
                "cik": event.get('cik'),
                "filingDate": target_date,
                "accessionNumber": event.get('accessionNumber') or event.get('accession_number'),
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

