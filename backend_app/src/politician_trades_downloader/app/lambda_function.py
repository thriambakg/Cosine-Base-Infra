"""
Lambda function to download PTR files (Senate and House) and store them in S3.
This function is invoked in parallel via Step Functions Map state.
"""

import json
import logging
import os
import re
import time
from typing import Dict, Any, Optional
from datetime import datetime
from http.cookies import SimpleCookie
from http.cookiejar import Cookie

import boto3
import requests

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
s3_client = boto3.client('s3')

# Environment variables
S3_BUCKET = os.environ.get('S3_BUCKET')


def download_and_store_senate_ptr(ptr_data: Dict[str, Any], target_date: str) -> Optional[str]:
    """
    Download Senate PTR file from URL and store in S3
    
    Args:
        ptr_data: PTR metadata with url, view_url, uuid, filer_name, etc.
        target_date: Date string for S3 key structure (YYYY-MM-DD)
        
    Returns:
        S3 key if successful, None otherwise
    """
    try:
        url = ptr_data.get('url')
        view_url = ptr_data.get('view_url')
        uuid = ptr_data.get('uuid', '')
        s3_key = ptr_data.get('s3_key') or ptr_data.get('s3Key')
        
        if not url:
            error_msg = f"Missing URL for Senate PTR download: {ptr_data}"
            logger.error(f"❌ {error_msg}")
            raise ValueError(error_msg)
        
        # Generate S3 key if not provided
        if not s3_key:
            if uuid:
                filename = f"senate-ptr-{uuid}.html"
            else:
                # Fallback: extract from URL
                filename = url.split('/')[-1].rstrip('/') or 'senate-ptr.html'
            s3_key = f"trades/senate/{target_date}/{filename}"
        
        logger.info(f"📦 Will store Senate PTR to S3: {s3_key}")
        
        # Use view_url if provided, otherwise use url
        url_to_process = view_url if view_url else url
        
        # IMPORTANT: Always convert /view/paper/ URLs to /print/paper/ for image-based filings
        # /view/paper/ URLs contain scanned images, /print/paper/ has parseable HTML
        if '/view/paper/' in url_to_process:
            logger.info(f"⚠️ /view/paper/ URL detected - converting to /print/paper/ endpoint for parseable content")
            logger.info(f"   Original URL: {url_to_process}")
            # Extract UUID from URL
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
        
        logger.info(f"📥 Downloading Senate PTR from {url}")
        logger.info(f"📦 Will store to S3: {s3_key}")
        
        # Initialize variables for response handling
        is_pdf = False
        html_content = None
        
        # Download the PTR file
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        # For Senate PTRs, we need to accept agreement first to access the print page
        if 'efdsearch.senate.gov' in url:
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
                csrf_pattern = r'name=["\']csrfmiddlewaretoken["\'][^>]*value=["\']([^"\']+)["\']'
                csrf_match = re.search(csrf_pattern, search_response.text, re.IGNORECASE)
                if csrf_match:
                    csrf_cookie = csrf_match.group(1)
                    logger.info(f"📋 Extracted CSRF token from HTML: {csrf_cookie[:20]}...")
            
            # Perform search to establish session (use today's date if filing date not available)
            filing_date = ptr_data.get('filingDate') or ptr_data.get('filing_date', '')
            if filing_date:
                try:
                    if '/' in filing_date:
                        date_obj = datetime.strptime(filing_date, '%m/%d/%Y')
                    else:
                        date_obj = datetime.strptime(filing_date, '%Y-%m-%d')
                    search_start_date = date_obj.strftime('%m/%d/%Y')
                    search_end_date = search_start_date
                except:
                    today = datetime.now()
                    search_start_date = today.strftime('%m/%d/%Y')
                    search_end_date = search_start_date
            else:
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
                try:
                    cookie_jar = SimpleCookie()
                    cookie_jar.load(search_set_cookie)
                    for cookie_name, morsel in cookie_jar.items():
                        cookie_value = morsel.value
                        domain = morsel.get('domain', '') or 'efdsearch.senate.gov'
                        path = morsel.get('path', '/')
                        if domain.startswith('.'):
                            domain = domain[1:]
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
                        try:
                            cookie_jar = SimpleCookie()
                            cookie_jar.load(set_cookie_header)
                            for cookie_name, morsel in cookie_jar.items():
                                domain = morsel.get('domain', '') or 'efdsearch.senate.gov'
                                path = morsel.get('path', '/')
                                if domain.startswith('.'):
                                    domain = domain[1:]
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
                csrf_pattern = r'name=["\']csrfmiddlewaretoken["\'][^>]*value=["\']([^"\']+)["\']'
                csrf_match = re.search(csrf_pattern, search_page_refresh.text, re.IGNORECASE)
                if csrf_match:
                    csrf_cookie = csrf_match.group(1)
                    logger.info(f"📋 Extracted fresh CSRF token: {csrf_cookie[:20]}...")
            
            # Perform search with the filing date
            filing_date = ptr_data.get('filingDate') or ptr_data.get('filing_date', '')
            if filing_date:
                try:
                    if '/' in filing_date:
                        date_obj = datetime.strptime(filing_date, '%m/%d/%Y')
                    else:
                        date_obj = datetime.strptime(filing_date, '%Y-%m-%d')
                    search_start_date = date_obj.strftime('%m/%d/%Y')
                    search_end_date = search_start_date
                except:
                    today = datetime.now()
                    search_start_date = today.strftime('%m/%d/%Y')
                    search_end_date = search_start_date
            else:
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
            'referer': search_url if 'efdsearch.senate.gov' in url else url
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
        
        # Update s3_key with correct extension if needed
        if not s3_key.endswith(f'.{file_ext}'):
            base_key = s3_key.rsplit('.', 1)[0] if '.' in s3_key else s3_key
            s3_key = f"{base_key}.{file_ext}"
        
        logger.info(f"✅ Downloaded Senate PTR file ({len(file_content)} bytes, type: {file_ext})")
        
        # Upload to S3
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=file_content,
            ContentType=content_type
        )
        
        logger.info(f"✅ Stored Senate PTR to S3: {s3_key}")
        return s3_key
        
    except Exception as e:
        logger.error(f"❌ Error downloading/storing Senate PTR: {e}")
        raise


def download_house_ptrs_from_metadata(metadata_s3_key: str, event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Download House PTRs sequentially from S3 metadata file
    
    Args:
        metadata_s3_key: S3 key of the metadata JSON file
        event: Event dict with date and other context
        
    Returns:
        Summary dict with folderName and count
    """
    try:
        logger.info(f"📦 Reading House PTRs metadata from S3: {metadata_s3_key}")
        
        # Read metadata from S3
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=metadata_s3_key)
        metadata = json.loads(response['Body'].read().decode('utf-8'))
        
        house_ptrs = metadata.get('housePTRs', [])
        year = metadata.get('year', datetime.now().strftime('%Y'))
        
        logger.info(f"📋 Found {len(house_ptrs)} House PTRs in metadata for year {year}")
        
        # Check if housePTRs array is empty (hash matched - no downloads needed)
        if not house_ptrs or len(house_ptrs) == 0:
            logger.info(f"✅ House PTRs array is empty - hash matched, no downloads needed")
            folder_name = f"trades/house/{year}"
            return {
                "summary": "House PTR downloads skipped - no changes detected",
                "folderName": folder_name,
                "count": 0,
                "failed": 0,
                "total": 0,
                "success": True,
                "s3Keys": []  # Empty list when skipped
            }
        
        # Extract target date from event (for determining year if needed)
        target_date_raw = event.get('date') or event.get('filingDate')
        if target_date_raw:
            try:
                date_obj = datetime.strptime(target_date_raw, '%Y-%m-%d')
                target_date = date_obj.strftime('%Y-%m-%d')
            except:
                target_date = None
                else:
                logger.warning(f"⚠️ Failed to parse target date: {target_date_raw}")
                target_date = None
        
        # Process each House PTR sequentially
        successful_downloads = 0
        failed_downloads = 0
        folder_name = f"trades/house/{year}"
        s3_keys = []  # List of successfully downloaded S3 keys
        
        for idx, ptr_data in enumerate(house_ptrs, 1):
            try:
                logger.info(f"📥 Downloading House PTR {idx}/{len(house_ptrs)}: {ptr_data.get('filer_name', 'Unknown')}")
                
                # Use year from metadata for S3 key generation
                year_for_s3 = year
                s3_key = download_and_store_house_ptr(ptr_data, target_date or f"{year}-01-01")
                
                if s3_key:
                    successful_downloads += 1
                    s3_keys.append(s3_key)
                    logger.info(f"✅ Successfully downloaded House PTR {idx}/{len(house_ptrs)}")
                else:
                    failed_downloads += 1
                    logger.warning(f"⚠️ Failed to download House PTR {idx}/{len(house_ptrs)}")
                    
            except Exception as e:
                failed_downloads += 1
                logger.error(f"❌ Error downloading House PTR {idx}/{len(house_ptrs)}: {e}")
                # Continue with next PTR even if one fails
        
        logger.info(f"✅ Completed House PTR downloads: {successful_downloads} successful, {failed_downloads} failed")
        
        return {
            "summary": "House PTR downloads completed",
            "folderName": folder_name,
            "count": successful_downloads,
            "failed": failed_downloads,
            "total": len(house_ptrs),
            "success": True,
            "s3Keys": s3_keys  # List of S3 keys for matcher
        }
        
                except Exception as e:
        logger.error(f"❌ Error processing House PTRs from metadata: {e}")
                    raise
        

def download_and_store_house_ptr(ptr_data: Dict[str, Any], target_date: str) -> Optional[str]:
    """
    Download House PTR PDF from URL and store in S3
    
    Args:
        ptr_data: PTR metadata with url, uuid, filer_name, etc.
        target_date: Date string for S3 key structure (YYYY-MM-DD)
        
    Returns:
        S3 key if successful, None otherwise
    """
    try:
        url = ptr_data.get('url')
        uuid = ptr_data.get('uuid', '')
        s3_key = ptr_data.get('s3_key') or ptr_data.get('s3Key')
        
        if not url:
            error_msg = f"Missing URL for House PTR download: {ptr_data}"
            logger.error(f"❌ {error_msg}")
            raise ValueError(error_msg)
        
        # Generate S3 key if not provided
            if not s3_key:
            # Extract filename from URL (e.g., "20033394.pdf" from "public_disc/ptr-pdfs/2025/20033394.pdf")
            filename = url.split('/')[-1] or f'house-ptr-{uuid}.pdf' if uuid else 'house-ptr.pdf'
            # Extract year from target_date (YYYY-MM-DD format)
            year = target_date.split('-')[0] if target_date and '-' in target_date else datetime.now().strftime('%Y')
            s3_key = f"trades/house/{year}/{filename}"
        
        logger.info(f"📦 Will store House PTR to S3: {s3_key}")
        logger.info(f"📥 Downloading House PTR PDF from {url}")
        
        # Download the PDF directly from House Clerk website
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
        response = session.get(url, timeout=30)
            response.raise_for_status()
            
            # Check if response is actually a PDF
            content_type = response.headers.get('Content-Type', '').lower()
            content_sample = response.content[:100]
            
            if 'application/pdf' in content_type or content_sample.startswith(b'%PDF'):
                logger.info(f"✅ Downloaded PDF file ({len(response.content)} bytes)")
                file_content = response.content
                file_ext = 'pdf'
            content_type = 'application/pdf'
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
        
        logger.info(f"✅ Downloaded House PTR file ({len(file_content)} bytes, type: {file_ext})")
        
        # Upload to S3
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=file_content,
            ContentType=content_type
        )
        
        logger.info(f"✅ Stored House PTR to S3: {s3_key}")
        return s3_key
        
    except Exception as e:
        logger.error(f"❌ Error downloading/storing House PTR: {e}")
        raise


def lambda_handler(event, context):
    """
    Lambda handler for downloading PTR files (Senate and House)
    
    Expected input (from Step Functions):
    
    House PTR Batch (from S3 metadata):
    {
        "metadataS3Key": "trades/house/2025/metadata.json",
        "date": "2025-11-07",
        "source": "house"
    }
    
    Senate PTR (individual):
    {
        "url": "https://efdsearch.senate.gov/search/view/ptr/{uuid}/",
        "view_url": "https://efdsearch.senate.gov/search/view/ptr/{uuid}/",
        "uuid": "b930d1b3-c58c-4b98-b28f-fc95c4a03bba",
        "formType": "senate_ptr",
        "source": "senate",
        "filingDate": "2025-11-07",
        "filer_name": "McCormick, David H (Senator)",
        "transactions": [...]  # Optional: pre-extracted transactions
    }
    
    House PTR (individual - legacy):
    {
        "url": "https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2025/20033394.pdf",
        "formType": "house_ptr",
        "source": "house",
        "filingDate": "2025-11-07",
        "filer_name": "Suozzi, Hon.. Thomas (House Representative)",
        "uuid": "20033394"
    }
    
    Returns:
    House PTR Batch:
    {
        "summary": "House PTR downloads completed",
        "folderName": "trades/house/2025",
        "count": 150,
        "success": true
    }
    
    Individual PTR:
    {
        "success": true,
        "s3Key": "trades/senate/2025-11-07/senate-ptr-uuid.html",
        "formType": "senate_ptr" or "house_ptr",
        "source": "senate" or "house",
        "filingDate": "2025-11-07",
        "filer_name": "...",
        "transactions": [...]  # Only for Senate PTRs with pre-extracted transactions
    }
    """
    logger.info(f"🚀 Politician Trades Downloader Lambda started: {json.dumps(event)}")
    
    if not S3_BUCKET:
        raise ValueError("S3_BUCKET environment variable not set")
    
    try:
        # Check if this is a House PTR batch request (from S3 metadata)
        metadata_s3_key = event.get('metadataS3Key') or event.get('metadata_s3_key')
        source = event.get('source')
        
        # Handle House PTR batch mode
        if source == 'house':
            if metadata_s3_key:
                logger.info(f"📦 House PTR batch mode: Reading metadata from S3: {metadata_s3_key}")
                return download_house_ptrs_from_metadata(metadata_s3_key, event)
            else:
                # metadataS3Key is null - hash matched, no downloads needed
                logger.info(f"✅ House PTR batch mode: metadataS3Key is null - hash matched, no downloads needed")
                # Extract year from date if available, otherwise use current year
                date_str = event.get('date') or event.get('filingDate')
                if date_str:
                    try:
                        year = date_str.split('-')[0] if '-' in date_str else datetime.now().strftime('%Y')
                    except:
                        year = datetime.now().strftime('%Y')
                else:
                    year = datetime.now().strftime('%Y')
                
                return {
                    "summary": "House PTR downloads skipped - no changes detected",
                    "folderName": f"trades/house/{year}",
                    "count": 0,
                    "failed": 0,
                    "total": 0,
                    "success": True
                }
        
        # Extract target date from event
        target_date_raw = event.get('filingDate') or event.get('filing_date') or event.get('date')
        if not target_date_raw:
            raise ValueError("filingDate, filing_date, or date must be provided in event")
        
        # Normalize date to YYYY-MM-DD format for consistent S3 key structure
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
        
        # Determine source (house or senate)
        source = event.get('source')
        form_type = event.get('formType') or event.get('form_type')
        
        # Auto-detect source from formType if not explicitly provided
        if not source:
            if form_type:
                if 'house' in form_type.lower():
                    source = 'house'
                elif 'senate' in form_type.lower():
                    source = 'senate'
            else:
                # Default to senate if unclear
                source = 'senate'
                logger.warning(f"⚠️ Source unclear, defaulting to senate: {event}")
        
        # Route to appropriate download function
        if source == 'senate':
            logger.info(f"📋 Detected Senate PTR download request")
            
            # For Senate PTRs, transactions may already be extracted at fetcher level
            # Still download and store HTML for reference
            s3_key = download_and_store_senate_ptr(event, target_date)
                
                if not s3_key:
                raise Exception("Failed to download Senate PTR - download_and_store_senate_ptr returned None")
                
            # Return format that includes pre-extracted transactions if available
                return {
                    "s3Key": s3_key,
                "formType": event.get('formType', 'senate_ptr'),
                "source": "senate",
                    "filingDate": target_date,
                    "filer_name": event.get('filer_name'),  # Always pass through filer_name
                "transactions": event.get('transactions', []),  # Pass through pre-extracted transactions if available
                    "success": True
                }
        elif source == 'house':
            logger.info(f"📋 Detected House PTR download request")
            
            s3_key = download_and_store_house_ptr(event, target_date)
                
                if not s3_key:
                raise Exception("Failed to download House PTR - download_and_store_house_ptr returned None")
                
                # Return format that matches matcher Lambda expectations
                return {
                    "s3Key": s3_key,
                "formType": event.get('formType', 'house_ptr'),
                "source": "house",
                    "filingDate": target_date,
                    "filer_name": event.get('filer_name'),  # Always pass through filer_name if available
                    "success": True
                }
        else:
            raise ValueError(f"Unknown source: {source}. Expected 'house' or 'senate'")
            
        
    except Exception as e:
        logger.error(f"❌ Error in downloader Lambda: {e}")
        # Raise exception so Step Functions Catch block handles it
        # Include original event data in error message for context
        error_with_context = {
            "error": str(e),
            "formType": event.get('formType') or event.get('form_type'),
            "source": event.get('source'),
            "url": event.get('url'),
            "filingDate": event.get('filingDate') or event.get('filing_date') or event.get('date')
        }
        raise Exception(json.dumps(error_with_context))