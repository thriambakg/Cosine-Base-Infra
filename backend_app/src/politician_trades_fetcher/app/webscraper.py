"""
Web Scraper Helper Class for Congressional PTRs
Scrapes House and Senate financial disclosure forms (PTRs) from public websites
"""

import logging
import requests
import time
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import re
from urllib.parse import urljoin, urlparse
import boto3
import traceback

logger = logging.getLogger()
logger.setLevel(logging.INFO)

class CongressionalPTRScraper:
    
    def _extract_senate_ptr_transactions(self, html_content: str, filer_name: str, filing_date_str: str) -> List[Dict[str, Any]]:
        """
        Extract transaction data from Senate PTR HTML table
        
        Args:
            html_content: HTML content from the print page
            filer_name: Name of the filer
            filing_date_str: Filing date string
            
        Returns:
            List of transaction dictionaries with: securityName, assetType, order (type), amount
        """
        transactions = []
        
        try:
            # Find the transactions table
            # Try multiple patterns to match different HTML structures
            table_patterns = [
                # Pattern 1: Standard table with class="table table-striped"
                r'<table[^>]*class=["\'][^"\']*table[^"\']*table-striped[^"\']*["\'][^>]*>.*?<tbody>(.*?)</tbody>',
                # Pattern 2: Table with class containing "table" and "striped" (may be in different order)
                r'<table[^>]*class=["\'][^"\']*table[^"\']*["\'][^>]*>.*?<tbody>(.*?)</tbody>',
                # Pattern 3: Any table with tbody (fallback)
                r'<table[^>]*>.*?<tbody>(.*?)</tbody>',
            ]
            
            table_match = None
            matched_pattern = None
            for i, pattern in enumerate(table_patterns, 1):
                table_match = re.search(pattern, html_content, re.IGNORECASE | re.DOTALL)
                if table_match:
                    matched_pattern = i
                    logger.info(f"✅ Matched table pattern {i} - found transactions table")
                    break
            
            if not table_match:
                logger.warning("⚠️ Could not find transactions table in HTML")
                # Debug: Look for any table tags
                all_tables = re.findall(r'<table[^>]*>', html_content, re.IGNORECASE)
                logger.info(f"   Found {len(all_tables)} <table> tags in HTML")
                if all_tables:
                    for i, table_tag in enumerate(all_tables[:3], 1):
                        logger.info(f"   Table {i}: {table_tag[:300]}")
                
                # Also check for tbody tags
                tbody_tags = re.findall(r'<tbody[^>]*>', html_content, re.IGNORECASE)
                logger.info(f"   Found {len(tbody_tags)} <tbody> tags in HTML")
                
                # Look specifically for "Transactions" heading and nearby table
                if 'Transactions' in html_content:
                    transactions_idx = html_content.find('Transactions')
                    sample = html_content[max(0, transactions_idx-100):min(len(html_content), transactions_idx+500)]
                    logger.info(f"   Sample HTML around 'Transactions': {sample}")
                
                return transactions
            
            logger.info(f"✅ Successfully matched table pattern {matched_pattern}")
            
            tbody_content = table_match.group(1)
            logger.debug(f"✅ Extracted tbody content ({len(tbody_content)} chars)")
            
            # Extract all table rows
            row_pattern = r'<tr[^>]*>(.*?)</tr>'
            rows = list(re.finditer(row_pattern, tbody_content, re.IGNORECASE | re.DOTALL))
            logger.info(f"✅ Found {len(rows)} table rows in tbody")
            
            for row_num, row_match in enumerate(rows, start=1):
                row_html = row_match.group(1)
                
                # Extract cells from row
                cell_pattern = r'<td[^>]*>(.*?)</td>'
                cells = re.findall(cell_pattern, row_html, re.IGNORECASE | re.DOTALL)
                
                if len(cells) < 8:  # Need at least 8 columns
                    continue
                
                # Clean HTML from cells
                def clean_html(text):
                    text = re.sub(r'<[^>]+>', '', text)
                    from html import unescape
                    text = unescape(text)
                    text = text.strip()
                    text = re.sub(r'\s+', ' ', text)
                    return text
                
                try:
                    # Column mapping from the HTML table:
                    # 0: # (row number)
                    # 1: Transaction Date
                    # 2: Owner
                    # 3: Ticker
                    # 4: Asset Name (may have nested div with Rate/Coupon, Matures info)
                    # 5: Asset Type
                    # 6: Type (Purchase/Sale)
                    # 7: Amount
                    # 8: Comment
                    transaction_date_str = clean_html(cells[1]) if len(cells) > 1 else ''
                    owner = clean_html(cells[2]) if len(cells) > 2 else ''
                    ticker = clean_html(cells[3]) if len(cells) > 3 else ''
                    
                    # Asset name may have nested HTML (e.g., <div class="text-muted"> with Rate/Coupon info)
                    # clean_html will remove all HTML tags, so we'll get just the text
                    asset_name = clean_html(cells[4]) if len(cells) > 4 else ''
                    # Remove extra whitespace that might result from nested content
                    asset_name = ' '.join(asset_name.split())
                    
                    asset_type = clean_html(cells[5]) if len(cells) > 5 else ''
                    transaction_type = clean_html(cells[6]) if len(cells) > 6 else ''
                    amount_str = clean_html(cells[7]) if len(cells) > 7 else ''
                    comment = clean_html(cells[8]) if len(cells) > 8 else ''
                    
                    # Parse amount (handles ranges like "$100,001 - $250,000")
                    amount_min = None
                    amount_max = None
                    total_amount = None
                    
                    if amount_str and amount_str not in ['--', '']:
                        amount_clean = amount_str.replace('$', '').replace(',', '').strip()
                        if ' - ' in amount_clean or '-' in amount_clean:
                            parts = re.split(r'\s*-\s*', amount_clean)
                            if len(parts) == 2:
                                try:
                                    amount_min = float(parts[0].strip())
                                    amount_max = float(parts[1].strip())
                                    total_amount = (amount_min + amount_max) / 2
                                except:
                                    pass
                        else:
                            try:
                                total_amount = float(amount_clean)
                                amount_min = total_amount
                                amount_max = total_amount
                            except:
                                pass
                    
                    # Create transaction entry with required fields
                    transaction = {
                        'securityName': asset_name,
                        'assetType': asset_type,
                        'order': transaction_type,  # "Purchase", "Sale", etc.
                        'amount': total_amount,
                        'amountMin': amount_min,
                        'amountMax': amount_max,
                        'transactionDate': transaction_date_str,
                        'owner': owner,
                        'ticker': ticker if ticker and ticker.strip() not in ['--', ''] else None,
                        'comment': comment,
                        'filerName': filer_name,
                        'filingDate': filing_date_str
                    }
                    
                    transactions.append(transaction)
                    logger.debug(f"   ✅ Parsed row {row_num}: {transaction_type} - {asset_name} ({asset_type}) - {amount_str}")
                    
                except Exception as e:
                    logger.warning(f"⚠️ Error parsing row {row_num}: {e}")
                    logger.debug(f"   Row HTML: {row_html[:300]}")
                    continue
            
        except Exception as e:
            logger.error(f"❌ Error extracting transactions from HTML: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
        
        return transactions
    """Scraper for Congressional Periodic Transaction Reports (PTRs)"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
    
    def fetch_house_ptrs(self, target_date: str) -> List[Dict[str, Any]]:
        """
        Fetch House PTRs for a specific date from Clerk of House website
        
        House PTRs are published at: https://clerk.house.gov/public_disc/ptr-pdfs/{year}/
        Files are typically named: {lastname}-{firstname}-{date}.pdf or similar patterns
        
        Args:
            target_date: Date in YYYY-MM-DD format
            
        Returns:
            List of PTR metadata dicts with url, s3_key, filing_date, etc.
        """
        logger.info(f"🏛️ Fetching House PTRs for date: {target_date}")
        
        ptrs = []
        try:
            # House Clerk's website structure
            # URL: https://clerk.house.gov/public_disc/ptr-pdfs/{year}/
            year = target_date[:4]
            base_url = f"https://clerk.house.gov/public_disc/ptr-pdfs/{year}/"
            
            # Try to fetch the directory listing page
            # House Clerk site may have an index page or directory listing
            try:
                # Try common index page names
                index_urls = [
                    base_url,  # Direct directory access
                    f"{base_url}index.html",
                    f"{base_url}index.htm",
                ]
                
                html_content = None
                for index_url in index_urls:
                    try:
                        logger.info(f"🔍 Trying to access House PTR directory: {index_url}")
                        response = self.session.get(index_url, timeout=30)
                        if response.status_code == 200:
                            html_content = response.text
                            logger.info(f"✅ Successfully accessed directory listing")
                            break
                    except Exception as e:
                        logger.debug(f"⚠️ Could not access {index_url}: {e}")
                        continue
                
                if not html_content:
                    logger.warning(f"⚠️ Could not access House PTR directory for {year}")
                    return ptrs
                
                # Find all PDF links
                pdf_pattern = r'href=["\']([^"\']*\.pdf[^"\']*)["\']'
                pdf_matches = re.findall(pdf_pattern, html_content, re.IGNORECASE)
                
                logger.info(f"📋 Found {len(pdf_matches)} PDF links in directory")
                
                # Filter out clearly non-PTR documents
                # Non-PTR indicators: generic filenames like "statistics", "terms", "guide", etc.
                non_ptr_indicators = [
                    'statistics', 'terms_of_service', 'terms', 'guide', 'user', 'manual',
                    'index', 'readme', 'help', 'faq', 'about', 'contact', 'privacy',
                    'duplicate', 'olm', 'ttd', 'oal', 'scsoal', 'artificial-intelligence'
                ]
                
                # Download candidate PTRs - Textract will verify dates and PTR content
                for pdf_link in pdf_matches:
                    # Convert relative URLs to absolute
                    if pdf_link.startswith('/'):
                        pdf_url = f"https://clerk.house.gov{pdf_link}"
                    elif pdf_link.startswith('http'):
                        pdf_url = pdf_link
                    else:
                        pdf_url = urljoin(base_url, pdf_link)
                    
                    # Extract filename
                    filename = pdf_link.split('/')[-1].lower()
                    
                    # Skip obviously non-PTR files
                    is_likely_non_ptr = any(indicator in filename for indicator in non_ptr_indicators)
                    if is_likely_non_ptr:
                        logger.debug(f"⏭️ Skipping likely non-PTR file: {filename}")
                        continue
                    
                    # Extract filer name from filename if possible
                    filer_name = filename.replace('.pdf', '').replace('-', ' ').title()
                    
                    ptr_data = {
                        'filer_name': filer_name,
                        'filing_date': target_date,  # Will be verified/updated by Textract
                        'form_type': 'house_ptr',
                        'url': pdf_url,
                        's3_key': f"trades/{target_date}/house/{filename}"
                    }
                    ptrs.append(ptr_data)
                    logger.info(f"📄 Queueing House PTR candidate for download and verification: {filename}")
                
                logger.info(f"📊 Found {len(ptrs)} House PTR candidates to check (Textract will verify filing date and PTR content)")
                
            except Exception as parse_error:
                logger.error(f"❌ Error parsing House PTR directory: {parse_error}")
                import traceback
                logger.error(f"Traceback: {traceback.format_exc()}")
            
        except Exception as e:
            logger.error(f"❌ Error fetching House PTRs: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
        
        return ptrs
    
    def fetch_senate_ptrs(self, target_date: str) -> List[Dict[str, Any]]:
        """
        Fetch Senate PTRs for a specific date from Senate Ethics website
        
        Senate PTRs are published at: https://efdsearch.senate.gov/search/
        This site uses a search interface that requires form submission
        
        Args:
            target_date: Date in YYYY-MM-DD format
            
        Returns:
            List of PTR metadata dicts with url, s3_key, filing_date, etc.
        """
        logger.info(f"🏛️ Fetching Senate PTRs for date: {target_date}")
        
        ptrs = []
        
        try:
            # Parse target date for form submission
            target_date_obj = datetime.strptime(target_date, '%Y-%m-%d')
            
            # Format date for form (MM/DD/YYYY)
            date_str = target_date_obj.strftime('%m/%d/%Y')
            
            # Senate eFD search URL
            search_url = "https://efdsearch.senate.gov/search/"
            
            # First, GET the search page to get session cookies and any CSRF tokens
            logger.info(f"🔍 Accessing Senate PTR search page: {search_url}")
            response = self.session.get(search_url, timeout=30)
            response.raise_for_status()
            
            # Parse Set-Cookie headers manually to ensure we capture all cookies
            # requests.Session should do this automatically, but Django sessions can be tricky
            # requests stores multiple Set-Cookie headers under the same key
            set_cookie_header = response.headers.get('Set-Cookie', '')
            if set_cookie_header:
                # If multiple Set-Cookie headers exist, requests combines them with comma
                # But Django usually sends one Set-Cookie per cookie
                logger.info(f"📋 Set-Cookie header: {set_cookie_header[:200]}...")
            
                # Parse sessionid from Set-Cookie if present
                if 'sessionid=' in set_cookie_header:
                    # Extract sessionid value
                    sessionid_match = re.search(r'sessionid=([^;]+)', set_cookie_header)
                    if sessionid_match:
                        sessionid_value = sessionid_match.group(1)
                        # Manually set it in the session cookies if not already set
                        if not self.session.cookies.get('sessionid'):
                            from http.cookies import SimpleCookie
                            try:
                                cookie = SimpleCookie()
                                cookie.load(set_cookie_header)
                                for key, morsel in cookie.items():
                                    # Extract domain from cookie header
                                    domain = '.senate.gov'
                                    if 'Domain=' in set_cookie_header:
                                        domain_match = re.search(r'Domain=([^;]+)', set_cookie_header)
                                        if domain_match:
                                            domain = domain_match.group(1).strip()
                                    self.session.cookies.set(key, morsel.value, domain=domain)
                                logger.info(f"✅ Manually set sessionid cookie: {sessionid_value[:50]}...")
                            except Exception as e:
                                logger.warning(f"⚠️ Could not parse Set-Cookie header: {e}")
            
            # Log all cookies received from initial GET
            initial_cookies = list(self.session.cookies.keys())
            logger.info(f"📋 Cookies from initial GET (after parsing): {initial_cookies}")
            
            # Ensure we capture cookies (especially CSRF cookie and sessionid)
            csrf_cookie = self.session.cookies.get('csrftoken') or self.session.cookies.get('csrfmiddlewaretoken')
            sessionid_cookie = self.session.cookies.get('sessionid')
            
            if csrf_cookie:
                logger.info(f"✅ Found CSRF cookie: {csrf_cookie[:20]}...")
            else:
                logger.warning(f"⚠️ No CSRF cookie found from initial GET")
            
            if sessionid_cookie:
                logger.info(f"✅ Found sessionid cookie from initial GET: {sessionid_cookie[:50]}...")
            else:
                logger.warning(f"⚠️ No sessionid cookie found from initial GET - Django may create it on POST")
            
            html_content = response.text
            
            # Helper function to accept agreement if needed
            def accept_agreement_if_needed(current_html):
                """Accept the agreement form if present, return updated HTML"""
                if 'id="agreement_form"' in current_html or 'prohibition_agreement' in current_html:
                    logger.info("📋 Found agreement form - accepting terms...")
                    
                    # Extract CSRF token from agreement form
                    agreement_csrf_pattern = r'name=["\']csrfmiddlewaretoken["\'][^>]*value=["\']([^"\']+)["\']'
                    agreement_csrf_match = re.search(agreement_csrf_pattern, current_html, re.IGNORECASE)
                    
                    if agreement_csrf_match:
                        agreement_csrf = agreement_csrf_match.group(1)
                        
                        # Submit agreement form
                        agreement_data = {
                            'prohibition_agreement': '1',  # Check the checkbox
                            'csrfmiddlewaretoken': agreement_csrf
                        }
                        
                        logger.info(f"📋 Submitting agreement form...")
                        agreement_response = self.session.post(
                            search_url,
                            data=agreement_data,
                            headers={
                                'Content-Type': 'application/x-www-form-urlencoded',
                                'Referer': search_url,
                                'Origin': 'https://efdsearch.senate.gov',
                                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                                'X-CSRFToken': agreement_csrf
                            },
                            timeout=30,
                            allow_redirects=True
                        )
                        
                        # Check response - it might redirect (302/303) or return 200
                        if agreement_response.status_code in [200, 302, 303]:
                            # Follow redirects manually to ensure cookies are captured
                            final_url = agreement_response.url
                            logger.info(f"✅ Agreement POST response: status={agreement_response.status_code}, final_url={final_url}")
                            
                            # Parse Set-Cookie headers from agreement response to ensure sessionid is captured
                            agreement_set_cookie = agreement_response.headers.get('Set-Cookie', '')
                            if agreement_set_cookie:
                                logger.info(f"📋 Agreement Set-Cookie header: {agreement_set_cookie[:200]}...")
                                # Manually parse and set sessionid if present
                                if 'sessionid=' in agreement_set_cookie:
                                    from http.cookies import SimpleCookie
                                    try:
                                        cookie = SimpleCookie()
                                        cookie.load(agreement_set_cookie)
                                        for key, morsel in cookie.items():
                                            # Extract domain from cookie header or use default
                                            domain = '.senate.gov'
                                            if 'Domain=' in agreement_set_cookie:
                                                domain_match = re.search(r'Domain=([^;]+)', agreement_set_cookie)
                                                if domain_match:
                                                    domain = domain_match.group(1).strip()
                                            self.session.cookies.set(key, morsel.value, domain=domain)
                                            logger.info(f"✅ Set cookie from agreement response: {key}={morsel.value[:30]}...")
                                    except Exception as e:
                                        logger.warning(f"⚠️ Could not parse agreement Set-Cookie header: {e}")
                            
                            # Also check response.cookies (requests automatic parsing)
                            response_cookies = agreement_response.cookies
                            if response_cookies:
                                logger.info(f"📋 Cookies automatically parsed by requests: {list(response_cookies.keys())}")
                                # Update session cookies with response cookies
                                self.session.cookies.update(response_cookies)
                            
                            # Verify session cookies are set properly after update
                            sessionid_cookie = self.session.cookies.get('sessionid')
                            csrf_cookie = self.session.cookies.get('csrftoken') or self.session.cookies.get('csrfmiddlewaretoken')
                            
                            if sessionid_cookie:
                                logger.info(f"✅ Session ID cookie present: {sessionid_cookie[:50]}...")
                                logger.info(f"📋 Session cookie length: {len(sessionid_cookie)} chars")
                                # Decode to check for agreement (base64-encoded Django session)
                                try:
                                    import base64
                                    # Django sessions are base64-encoded
                                    decoded = base64.b64decode(sessionid_cookie.split(':')[0] + '==')
                                    if b'search_agreement' in decoded:
                                        logger.info("✅ Agreement found in session cookie!")
                                    else:
                                        logger.warning("⚠️ Agreement not found in session cookie")
                                except Exception as e:
                                    logger.debug(f"Could not decode session cookie: {e}")
                            else:
                                logger.warning(f"⚠️ Session ID cookie missing after agreement acceptance!")
                                logger.info(f"📋 Response headers: {dict(agreement_response.headers)}")
                            
                            if csrf_cookie:
                                logger.info(f"🔄 CSRF cookie present: {csrf_cookie[:20]}...")
                            
                            # Log all cookies for debugging
                            logger.info(f"📋 All session cookies after agreement: {list(self.session.cookies.keys())}")
                            
                            # Get final page content (after redirects if any)
                            if final_url != search_url:
                                # Follow redirect to get final content
                                final_response = self.session.get(final_url, timeout=30)
                                return final_response.text
                            else:
                                return agreement_response.text
                        else:
                            logger.warning(f"⚠️ Agreement submission returned status {agreement_response.status_code}")
                    else:
                        logger.warning("⚠️ Could not find CSRF token in agreement form")
                return current_html
            
            # Check if we need to accept the agreement first
            html_content = accept_agreement_if_needed(html_content)
            
            # Log first part of HTML to debug form structure
            logger.info(f"📄 HTML content preview (first 3000 chars): {html_content[:3000]}")
            
            # Verify we now have the search form (not the agreement form)
            if 'id="searchForm"' in html_content:
                logger.info("✅ Search form is now available")
            elif 'id="agreement_form"' in html_content:
                logger.warning("⚠️ Still seeing agreement form - agreement may not have been accepted properly")
            
            # Extract form action URL
            form_action = None
            form_match = re.search(r'<form[^>]*action=["\']([^"\']+)["\']', html_content, re.IGNORECASE)
            if form_match:
                form_action = form_match.group(1)
                if form_action.startswith('/'):
                    form_action = urljoin(search_url, form_action)
                elif not form_action.startswith('http'):
                    form_action = urljoin(search_url, form_action)
                logger.info(f"✅ Found form action: {form_action}")
            
            # Extract form method (GET or POST)
            form_method = 'POST'
            method_match = re.search(r'<form[^>]*method=["\']([^"\']+)["\']', html_content, re.IGNORECASE)
            if method_match:
                form_method = method_match.group(1).upper()
                logger.info(f"✅ Found form method: {form_method}")
            
            # Extract form field names from the actual HTML
            # Based on the actual form structure:
            # - Report type: name="report_type", value="11" for Periodic Transactions
            # - Date fields: name="submitted_start_date" and name="submitted_end_date"
            # - CSRF token: name="csrfmiddlewaretoken"
            
            # Extract CSRF token (required for Django forms)
            # IMPORTANT: Extract from search form AFTER agreement is accepted (token may change)
            csrf_token = None
            csrf_pattern = r'name=["\']csrfmiddlewaretoken["\'][^>]*value=["\']([^"\']+)["\']'
            csrf_match = re.search(csrf_pattern, html_content, re.IGNORECASE)
            if csrf_match:
                csrf_token = csrf_match.group(1)
                logger.info(f"✅ Found CSRF token from search form: {csrf_token[:20]}...")
            else:
                logger.warning("⚠️ Could not find CSRF token in search form - form submission may fail")
                
                # Fallback: try to use the cookie value
                if csrf_cookie:
                    logger.info("💡 Using CSRF cookie value as fallback")
                    csrf_token = csrf_cookie
            
            # Extract report type field name and value for Periodic Transactions
            report_type_field = 'report_type'
            report_type_value = None
            # Look for checkbox with value="11" and Periodic Transactions label
            ptr_checkbox_pattern = r'<input[^>]*name=["\']report_type["\'][^>]*value=["\'](\d+)["\'][^>]*>.*?Periodic.*?Transaction'
            ptr_match = re.search(ptr_checkbox_pattern, html_content, re.IGNORECASE | re.DOTALL)
            if ptr_match:
                report_type_value = ptr_match.group(1)
                logger.info(f"✅ Found Periodic Transactions checkbox value: {report_type_value}")
            else:
                # Fallback: look for checkbox with value that comes before "Periodic Transactions"
                # The HTML shows value="11" for Periodic Transactions
                report_type_value = '11'
                logger.info(f"💡 Using default Periodic Transactions value: {report_type_value}")
            
            # Date field names (from actual HTML)
            date_from_field = 'submitted_start_date'
            date_to_field = 'submitted_end_date'
            logger.info(f"✅ Using date fields: {date_from_field} and {date_to_field}")
            
            # First, submit the form to initialize the session/search state
            # This uses simple form data
            form_data = {}
            
            # Report type checkbox: name="report_type", value="11" for Periodic Transactions
            if report_type_value:
                form_data[report_type_field] = report_type_value
            else:
                form_data['report_type'] = '11'  # Periodic Transactions value
            
            # Date fields: name="submitted_start_date" and name="submitted_end_date"
            form_data[date_from_field] = date_str
            form_data[date_to_field] = date_str
            
            # For the AJAX endpoint, we need DataTables format with time appended to dates
            # Date format for AJAX: "10/30/2025 00:00:00" and "10/30/2025 23:59:59"
            ajax_date_start = f"{date_str} 00:00:00"
            ajax_date_end = f"{date_str} 23:59:59"
            
            # CSRF token: name="csrfmiddlewaretoken" (required for Django)
            if csrf_token:
                form_data['csrfmiddlewaretoken'] = csrf_token
            else:
                logger.error("❌ CSRF token is required but not found - form submission will fail")
                return ptrs
            
            # Set headers for form submission
            # Django requires X-CSRFToken header when submitting forms via AJAX/requests
            headers = {
                'Content-Type': 'application/x-www-form-urlencoded',
                'Referer': search_url,
                'Origin': 'https://efdsearch.senate.gov',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            }
            
            # Add CSRF token to headers (Django sometimes requires this)
            # Django CSRF protection can require both:
            # 1. csrfmiddlewaretoken in form data (already added)
            # 2. X-CSRFToken header (for AJAX/API requests)
            # 3. CSRF cookie (should be in session cookies already)
            if csrf_token:
                headers['X-CSRFToken'] = csrf_token
            # Also try using cookie value if available
            if csrf_cookie:
                headers['X-CSRFToken'] = csrf_cookie
            
            # Submit search form
            logger.info(f"🔍 Submitting search form for date range: {date_str} to {date_str}")
            logger.info(f"📋 Form data: {form_data}")
            
            # The actual workflow is:
            # 1. POST to /search/ (form submission)
            # 2. POST to /search/report/data/ (AJAX endpoint that returns JSON/HTML results)
            
            # Step 1: Submit the form (this sets up the search in the session)
            # Try POST to search endpoint
            search_endpoints = []
            if form_action:
                search_endpoints.append(form_action)
            search_endpoints.extend([
                search_url,
                f"{search_url}results/",
                f"{search_url}search/",
            ])
            
            form_submitted = False
            form_response_url = None  # Track where form submission redirects to
            
            for endpoint in search_endpoints:
                try:
                    logger.info(f"🔍 Trying POST to form endpoint: {endpoint}")
                    response = self.session.post(
                        endpoint,
                        data=form_data,
                        headers=headers,
                        timeout=30,
                        allow_redirects=False
                    )
                    
                    # Even if it redirects, that's OK - we'll call the AJAX endpoint next
                    if response.status_code in [200, 301, 302, 303, 307, 308]:
                        logger.info(f"✅ Form submitted (status: {response.status_code})")
                        
                        # If redirect, note where it goes (might give us a clue about the right endpoint)
                        if response.status_code in [301, 302, 303, 307, 308]:
                            redirect_location = response.headers.get('Location', '')
                            if redirect_location:
                                if redirect_location.startswith('/'):
                                    redirect_location = urljoin(endpoint, redirect_location)
                                logger.info(f"🔄 Form redirects to: {redirect_location}")
                                form_response_url = redirect_location
                                # Follow redirect to update session state
                                try:
                                    redirect_response = self.session.get(redirect_location, timeout=30)
                                    if redirect_response.status_code == 200:
                                        logger.info(f"✅ Followed redirect - session updated")
                                except Exception as e:
                                    logger.debug(f"⚠️ Could not follow redirect: {e}")
                        
                        form_submitted = True
                        break
                except Exception as e:
                    logger.debug(f"⚠️ Error POSTing to {endpoint}: {e}")
                    continue
            
            if not form_submitted:
                logger.warning("⚠️ Could not submit form - will still try AJAX endpoint")
            
            # Step 2: Call the AJAX endpoint to get actual results
            # Based on browser network logs, the AJAX endpoint is /search/report/data/
            # But let's try a few variations in case it's different
            ajax_endpoints = [
                f"{search_url}report/data/",  # Most likely based on network logs
                f"{search_url}results/",
                f"{search_url}data/",
                "https://efdsearch.senate.gov/search/report/data/",  # Absolute URL
            ]
            
            # If form redirected somewhere, maybe that's the endpoint base
            if form_response_url:
                base_url = form_response_url.rsplit('/', 1)[0] if '/' in form_response_url else form_response_url
                ajax_endpoints.insert(0, f"{base_url}/report/data/")
            
            logger.info(f"🔍 Trying AJAX endpoints: {ajax_endpoints[:3]}...")
            
            # Prepare AJAX headers (important for Django to recognize it as AJAX)
            ajax_headers = headers.copy()
            ajax_headers.update({
                'X-Requested-With': 'XMLHttpRequest',  # Important: tells Django this is AJAX
                'Accept': 'application/json, text/javascript, */*; q=0.01',
            })
            
            # Update CSRF token in header
            if csrf_token:
                ajax_headers['X-CSRFToken'] = csrf_token
            elif csrf_cookie:
                ajax_headers['X-CSRFToken'] = csrf_cookie
            
            # Try each AJAX endpoint until one works
            search_results_html = None
            ajax_worked = False
            
            def build_datatables_payload(draw_num: int, start: int = 0, length: int = 25) -> dict:
                """Build DataTables AJAX payload with pagination parameters"""
                return {
                    'draw': str(draw_num),
                    'columns[0][data]': '0',
                    'columns[0][name]': '',
                    'columns[0][searchable]': 'true',
                    'columns[0][orderable]': 'true',
                    'columns[0][search][value]': '',
                    'columns[0][search][regex]': 'false',
                    'columns[1][data]': '1',
                    'columns[1][name]': '',
                    'columns[1][searchable]': 'true',
                    'columns[1][orderable]': 'true',
                    'columns[1][search][value]': '',
                    'columns[1][search][regex]': 'false',
                    'columns[2][data]': '2',
                    'columns[2][name]': '',
                    'columns[2][searchable]': 'true',
                    'columns[2][orderable]': 'true',
                    'columns[2][search][value]': '',
                    'columns[2][search][regex]': 'false',
                    'columns[3][data]': '3',
                    'columns[3][name]': '',
                    'columns[3][searchable]': 'true',
                    'columns[3][orderable]': 'true',
                    'columns[3][search][value]': '',
                    'columns[3][search][regex]': 'false',
                    'columns[4][data]': '4',
                    'columns[4][name]': '',
                    'columns[4][searchable]': 'true',
                    'columns[4][orderable]': 'true',
                    'columns[4][search][value]': '',
                    'columns[4][search][regex]': 'false',
                    'order[0][column]': '1',
                    'order[0][dir]': 'asc',
                    'order[1][column]': '0',
                    'order[1][dir]': 'asc',
                    'start': str(start),
                    'length': str(length),
                    'search[value]': '',
                    'search[regex]': 'false',
                    'report_types': '[11]',  # Array as string: Periodic Transactions
                    'filer_types': '[]',  # Empty array
                    'submitted_start_date': ajax_date_start,  # "10/30/2025 00:00:00"
                    'submitted_end_date': ajax_date_end,  # "10/30/2025 23:59:59"
                    'candidate_state': '',
                    'senator_state': '',
                    'office_id': '',
                    'first_name': '',
                    'last_name': '',
                    'csrfmiddlewaretoken': csrf_token if csrf_token else csrf_cookie
                }
            
            # Collect all PTRs across all pages
            all_ptr_data_map = {}  # Map of view_url -> metadata (filer_name, filing_date, uuid, etc.)
            working_ajax_endpoint = None
            
            # First, find a working AJAX endpoint
            for ajax_endpoint in ajax_endpoints:
                try:
                    logger.info(f"📡 POSTing to AJAX endpoint: {ajax_endpoint}")
                    logger.info(f"📋 Using DataTables payload format")
                    logger.info(f"📋 Key params: report_types=[11], dates={ajax_date_start} to {ajax_date_end}")
                    logger.info(f"📋 Headers: X-Requested-With={ajax_headers.get('X-Requested-With')}, X-CSRFToken={ajax_headers.get('X-CSRFToken', '')[:20]}...")
                    
                    # Test with first page to find working endpoint
                    test_payload = build_datatables_payload(draw_num=1, start=0, length=25)
                    ajax_response = self.session.post(
                        ajax_endpoint,
                        data=test_payload,
                        headers=ajax_headers,
                        timeout=30
                    )
                    
                    logger.info(f"📥 Response status: {ajax_response.status_code}, Content-Type: {ajax_response.headers.get('Content-Type', 'unknown')}")
                    
                    if ajax_response.status_code == 200:
                        # AJAX endpoint returns JSON or HTML fragment
                        content_type = ajax_response.headers.get('Content-Type', '')
                        response_text = ajax_response.text
                        
                        logger.info(f"✅ AJAX response received (status: 200, content-type: {content_type}, length: {len(response_text)} chars)")
                        
                        # Check for maintenance/error pages
                        if 'Site Under Maintenance' in response_text or 'TEMPORARILY UNAVAILABLE' in response_text or 'maintenance' in response_text.lower():
                            logger.error("❌ Server returned maintenance page - site is under maintenance")
                            search_results_html = None
                        elif 'application/json' in content_type:
                            # Parse JSON response (DataTables format)
                            try:
                                import json
                                ajax_data = json.loads(response_text)
                                logger.info(f"📄 AJAX returned JSON: {json.dumps(ajax_data)[:500]}...")
                                
                                # DataTables response format: {draw, recordsTotal, recordsFiltered, data: [[...]], result: "ok"}
                                if 'result' in ajax_data and ajax_data.get('result') == 'ok':
                                    # Extract data array - each row contains [first_name, last_name, full_name, link_html, date]
                                    data_array = ajax_data.get('data', [])
                                    logger.info(f"✅ Found {len(data_array)} PTR records in JSON response")
                                    
                                    # Convert DataTables JSON response to HTML-like string for parsing
                                    # We'll parse the data array directly for PTR links
                                    if data_array:
                                        # The data is in JSON format, so we'll parse it directly
                                        # Store the JSON data so we can extract PTR links later
                                        search_results_html = response_text  # Store JSON for parsing
                                        logger.info(f"✅ Successfully retrieved {len(data_array)} PTR records")
                                    else:
                                        logger.info("ℹ️ No PTR records found for this date")
                                        search_results_html = response_text  # Still store for consistency
                                else:
                                    logger.warning(f"⚠️ JSON response has unexpected format or result != 'ok': {ajax_data.get('result', 'unknown')}")
                                    search_results_html = response_text
                            except json.JSONDecodeError as e:
                                logger.warning(f"⚠️ Failed to parse JSON response: {e}")
                                search_results_html = response_text
                        else:
                            # HTML response (shouldn't happen with DataTables endpoint)
                            search_results_html = response_text
                            logger.info(f"📄 AJAX returned HTML/text (unexpected)")
                    elif ajax_response.status_code == 503:
                        # Service Unavailable
                        logger.error(f"❌ AJAX endpoint returned 503 (Service Unavailable)")
                        response_text = ajax_response.text
                        if 'Site Under Maintenance' in response_text or 'TEMPORARILY UNAVAILABLE' in response_text:
                            logger.error("❌ Server is under maintenance - cannot fetch Senate PTRs at this time")
                        else:
                            logger.error(f"❌ Service unavailable (503) - response: {response_text[:500]}")
                        search_results_html = None
                    else:
                        logger.warning(f"⚠️ AJAX endpoint returned status {ajax_response.status_code}")
                        logger.warning(f"📄 Response preview: {ajax_response.text[:500]}")
                        
                        # Check if it's an error page
                        response_text = ajax_response.text
                        if 'Site Under Maintenance' in response_text or 'TEMPORARILY UNAVAILABLE' in response_text or 'error' in response_text.lower()[:500]:
                            logger.error("❌ Server returned error/maintenance page")
                            search_results_html = None
                        else:
                            # Fallback: try to use the response anyway (might be valid HTML error page with info)
                            search_results_html = response_text
                    
                    # If we got valid results from this endpoint, stop trying others
                    if search_results_html and ajax_response.status_code == 200 and 'Site Under Maintenance' not in (search_results_html or ''):
                        logger.info(f"✅ AJAX endpoint {ajax_endpoint} returned valid results!")
                        ajax_worked = True
                        working_ajax_endpoint = ajax_endpoint
                        break
                    elif search_results_html is None:
                        # This endpoint failed, try next one
                        logger.debug(f"⚠️ Endpoint {ajax_endpoint} didn't work, trying next...")
                        continue
                        
                except Exception as e:
                    logger.debug(f"⚠️ Error calling AJAX endpoint {ajax_endpoint}: {e}")
                    continue
            
            if not ajax_worked:
                logger.error("❌ All AJAX endpoints failed")
                search_results_html = None
            
            # If AJAX endpoint worked, fetch all pages with pagination
            if working_ajax_endpoint and ajax_worked:
                logger.info(f"✅ Using AJAX endpoint {working_ajax_endpoint} for pagination")
                
                # Fetch all pages by incrementing draw until no more results
                draw_num = 1
                page_size = 25
                start = 0
                total_records = None
                
                while True:
                    # Build payload for this page
                    page_payload = build_datatables_payload(draw_num=draw_num, start=start, length=page_size)
                    
                    logger.info(f"📄 Fetching page {draw_num} (start={start}, length={page_size})")
                    
                    try:
                        page_response = self.session.post(
                            working_ajax_endpoint,
                            data=page_payload,
                            headers=ajax_headers,
                            timeout=30
                        )
                        
                        if page_response.status_code != 200:
                            logger.warning(f"⚠️ Page {draw_num} returned status {page_response.status_code}, stopping pagination")
                            break
                        
                        try:
                            page_data = json.loads(page_response.text)
                            
                            if 'result' not in page_data or page_data.get('result') != 'ok':
                                logger.warning(f"⚠️ Page {draw_num} result != 'ok': {page_data.get('result')}, stopping pagination")
                                break
                            
                            data_array = page_data.get('data', [])
                            
                            if not data_array:
                                logger.info(f"✅ Page {draw_num} returned 0 records - end of pagination")
                                break
                            
                            # Update total records if available
                            if 'recordsTotal' in page_data:
                                total_records = page_data.get('recordsTotal')
                                logger.info(f"📊 Total records available: {total_records}")
                            
                            logger.info(f"✅ Page {draw_num}: Found {len(data_array)} PTR records")
                            
                            # Extract PTR metadata from this page
                            for row in data_array:
                                if len(row) >= 4:
                                    full_name = row[2] if len(row) > 2 else ''
                                    link_html = row[3] if len(row) > 3 else ''
                                    filing_date_str = row[4] if len(row) > 4 else ''
                                    
                                    # Extract UUID from link HTML
                                    uuid_match = re.search(r'/ptr/([a-f0-9-]+)', link_html, re.IGNORECASE)
                                    if uuid_match:
                                        uuid = uuid_match.group(1)
                                        base_url = "https://efdsearch.senate.gov"
                                        
                                        # Extract href path
                                        href_match = re.search(r'href=["\']([^"\']+)["\']', link_html, re.IGNORECASE)
                                        if href_match:
                                            href_path = href_match.group(1)
                                            if href_path.startswith('/'):
                                                view_url = f"{base_url}{href_path}"
                                            else:
                                                view_url = f"{base_url}/search/view/ptr/{uuid}/"
                                        else:
                                            view_url = f"{base_url}/search/view/ptr/{uuid}/"
                                        
                                        # Store metadata (avoid duplicates)
                                        if view_url not in all_ptr_data_map:
                                            all_ptr_data_map[view_url] = {
                                                'filer_name': full_name,
                                                'filing_date': filing_date_str,
                                                'uuid': uuid,
                                                'link_html': link_html
                                            }
                            
                            # Check if we've fetched all records
                            if total_records is not None and start + len(data_array) >= total_records:
                                logger.info(f"✅ Fetched all {total_records} records across {draw_num} pages")
                                break
                            
                            # Move to next page
                            draw_num += 1
                            start += page_size
                            
                            # Safety limit: stop after 100 pages (2500 records)
                            if draw_num > 100:
                                logger.warning(f"⚠️ Reached pagination limit (100 pages), stopping")
                                break
                            
                        except json.JSONDecodeError as e:
                            logger.warning(f"⚠️ Page {draw_num} JSON decode error: {e}, stopping pagination")
                            break
                            
                    except Exception as e:
                        logger.warning(f"⚠️ Error fetching page {draw_num}: {e}, stopping pagination")
                        break
                
                logger.info(f"✅ Collected {len(all_ptr_data_map)} unique PTR filings across all pages")
                
                # Now download HTML for each PTR and extract all transactions
                for view_url, metadata in all_ptr_data_map.items():
                    filer_name = metadata.get('filer_name', 'Unknown')
                    filing_date_str = metadata.get('filing_date', '')
                    uuid = metadata.get('uuid', '')
                    
                    # Parse filing date
                    filing_date = None
                    if filing_date_str:
                        try:
                            filing_date = datetime.strptime(filing_date_str, '%m/%d/%Y').date()
                        except Exception as e:
                            logger.debug(f"⚠️ Could not parse filing date '{filing_date_str}': {e}")
                            continue
                    
                    # Only process if filing date matches target date
                    if filing_date and filing_date != target_date_obj.date():
                        logger.debug(f"⏭️ Skipping PTR from {filing_date} (not {target_date_obj.date()})")
                        continue
                    
                    logger.info(f"📥 Downloading PTR HTML: {filer_name} - {view_url}")
                    
                    try:
                        # Download the PTR HTML page with session cookies
                        ptr_response = self.session.get(
                            view_url,
                            headers={
                                'Referer': search_url,
                                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                                'Accept-Language': 'en-US,en;q=0.5',
                                'Accept-Encoding': 'gzip, deflate, br',
                                'Connection': 'keep-alive',
                                'Upgrade-Insecure-Requests': '1'
                            },
                            timeout=30,
                            allow_redirects=True
                        )
                        
                        if ptr_response.status_code != 200:
                            logger.warning(f"⚠️ Failed to download PTR HTML: {view_url} (status: {ptr_response.status_code})")
                            continue
                        
                        ptr_html = ptr_response.text
                        
                        # Check if we got redirected to agreement or home page
                        if 'agreement_form' in ptr_html or '/search/home/' in ptr_response.url or 'Search Options' in ptr_html:
                            logger.warning(f"⚠️ PTR URL redirected to agreement/home page: {view_url}")
                            # Try accepting agreement again
                            if 'agreement_form' in ptr_html:
                                logger.info(f"🔄 Attempting to accept agreement for PTR: {view_url}")
                                # Extract CSRF token from agreement form
                                csrf_match = re.search(r'name=["\']csrfmiddlewaretoken["\'][^>]*value=["\']([^"\']+)["\']', ptr_html, re.IGNORECASE)
                                if csrf_match:
                                    agreement_csrf = csrf_match.group(1)
                                    # POST agreement acceptance
                                    agreement_response = self.session.post(
                                        view_url,
                                        data={'prohibition_agreement': '1', 'csrfmiddlewaretoken': agreement_csrf},
                                        headers={'X-CSRFToken': agreement_csrf, 'Referer': view_url},
                                        timeout=30,
                                        allow_redirects=True
                                    )
                                    if agreement_response.status_code == 200:
                                        ptr_html = agreement_response.text
                                        if 'agreement_form' in ptr_html:
                                            logger.warning(f"⚠️ Agreement acceptance failed for PTR: {view_url}")
                                            continue
                                else:
                                    logger.warning(f"⚠️ Could not find CSRF token in agreement form for: {view_url}")
                                    continue
                            else:
                                continue
                        
                        # Extract all transactions from this PTR HTML
                        transactions = self._extract_senate_ptr_transactions(ptr_html, filer_name, filing_date_str)
                        
                        logger.info(f"✅ Extracted {len(transactions)} transactions from {filer_name}'s PTR")
                        
                        # Create one entry per transaction, all sharing the same politician info
                        for transaction in transactions:
                            ptr_info = {
                                'url': view_url,  # Keep original URL for reference
                                'view_url': view_url,
                                'formType': 'senate_ptr',
                                'form_type': 'senate_ptr',
                                'source': 'senate',
                                'filingDate': filing_date_str if filing_date_str else date_str,
                                'filing_date': filing_date_str if filing_date_str else date_str,
                                'filer_name': filer_name,
                                'uuid': uuid,
                                # Transaction-specific fields (from extracted transaction)
                                'transactionDate': transaction.get('transactionDate'),
                                'owner': transaction.get('owner'),
                                'securitySymbol': transaction.get('ticker'),
                                'securityName': transaction.get('securityName'),
                                'assetType': transaction.get('assetType'),
                                'transactionType': transaction.get('order'),  # "Purchase", "Sale", etc.
                                'amount': transaction.get('amount'),
                                'amountMin': transaction.get('amountMin'),
                                'amountMax': transaction.get('amountMax'),
                                'comment': transaction.get('comment'),
                                # Store transactions array (single transaction per entry)
                                'transactions': [transaction]  # Single transaction per entry
                            }
                            
                            ptrs.append(ptr_info)
                            logger.debug(f"   ✅ Added transaction: {transaction.get('order')} - {transaction.get('securityName')} ({transaction.get('assetType')})")
                        
                        if not transactions:
                            logger.warning(f"⚠️ No transactions extracted from PTR: {view_url}")
                    
                    except Exception as e:
                        logger.error(f"❌ Error downloading/extracting from PTR {view_url}: {e}")
                        import traceback as tb
                        logger.error(f"Traceback: {tb.format_exc()}")
                        continue
                
                logger.info(f"✅ Successfully extracted {len(ptrs)} transactions from {len(all_ptr_data_map)} Senate PTR filings")
                return ptrs
            
            elif search_results_html is None:
                # Explicitly None means error/maintenance - don't try fallback
                logger.error("❌ AJAX endpoint failed - cannot fetch Senate PTRs")
                logger.error("💡 The Senate eFD search site may be under maintenance or experiencing issues")
                return ptrs
            else:
                logger.warning("⚠️ AJAX endpoint didn't return results - trying fallback method...")
                # Fallback: Try the old approach
                for endpoint in search_endpoints:
                    try:
                        logger.info(f"🔍 Trying POST to form endpoint: {endpoint}")
                        response = self.session.post(
                            endpoint,
                            data=form_data,
                            headers=headers,
                            timeout=30,
                            allow_redirects=False  # Handle redirects manually to inspect them
                        )
                        
                        # Check for redirect first
                        if response.status_code in [301, 302, 303, 307, 308]:
                            redirect_url = response.headers.get('Location')
                            if redirect_url:
                                if redirect_url.startswith('/'):
                                    redirect_url = urljoin(endpoint, redirect_url)
                                logger.info(f"🔄 Following redirect to: {redirect_url}")
                                
                                # If redirect goes to /search/home/, the form submission was likely rejected
                                if '/home/' in redirect_url or redirect_url.endswith('/home'):
                                    logger.warning("⚠️ Redirected to /home/ - form submission was rejected")
                                logger.warning("💡 Possible causes:")
                                logger.warning("   1. CSRF token mismatch (may have changed after agreement)")
                                logger.warning("   2. Form validation error (missing required fields)")
                                logger.warning("   3. Form requires JavaScript/AJAX to process")
                                logger.warning("   4. Session may have expired or agreement not properly stored")
                                
                                # Try one more time with fresh GET to get updated tokens
                                logger.info("🔄 Attempting to get fresh search form with updated session...")
                                fresh_response = self.session.get(search_url, timeout=30)
                                if fresh_response.status_code == 200:
                                    fresh_html = fresh_response.text
                                    
                                    # Accept agreement again if needed (session might have expired)
                                    fresh_html = accept_agreement_if_needed(fresh_html)
                                    
                                    # Check if we still have agreement form after accepting
                                    if 'id="agreement_form"' in fresh_html:
                                        logger.error("❌ Agreement acceptance failed - cannot proceed")
                                        return ptrs
                                    
                                    # Extract fresh CSRF token from search form
                                    fresh_csrf_match = re.search(csrf_pattern, fresh_html, re.IGNORECASE)
                                    if fresh_csrf_match:
                                        fresh_csrf = fresh_csrf_match.group(1)
                                        logger.info(f"💡 Fresh CSRF token: {fresh_csrf[:20]}...")
                                        # Update form_data with fresh token and retry once
                                        form_data['csrfmiddlewaretoken'] = fresh_csrf
                                        if fresh_csrf:
                                            headers['X-CSRFToken'] = fresh_csrf
                                        
                                        logger.info("🔄 Retrying form submission with fresh CSRF token...")
                                        retry_response = self.session.post(
                                            endpoint,
                                            data=form_data,
                                            headers=headers,
                                            timeout=30,
                                            allow_redirects=False
                                        )
                                        
                                        if retry_response.status_code == 200:
                                            search_results_html = retry_response.text
                                            logger.info("✅ Retry successful!")
                                            break
                                        elif retry_response.status_code in [301, 302, 303, 307, 308]:
                                            retry_redirect = retry_response.headers.get('Location')
                                            if retry_redirect and '/home/' not in retry_redirect:
                                                logger.info(f"✅ Retry redirect to different location: {retry_redirect}")
                                                # Follow this redirect
                                                if retry_redirect.startswith('/'):
                                                    retry_redirect = urljoin(endpoint, retry_redirect)
                                                retry_redirect_response = self.session.get(retry_redirect, timeout=30)
                                                if retry_redirect_response.status_code == 200:
                                                    search_results_html = retry_redirect_response.text
                                                    break
                                    else:
                                        logger.warning("⚠️ Could not extract fresh CSRF token")
                                
                                redirect_response = self.session.get(redirect_url, timeout=30, headers={
                                    'Referer': endpoint,
                                    'User-Agent': headers['User-Agent']
                                })
                                if redirect_response.status_code == 200:
                                    search_results_html = redirect_response.text
                                    logger.info(f"✅ Got results after redirect (length: {len(search_results_html)} chars)")
                                    # Check if this is actually results or still the form
                                    if 'Search Options' in search_results_html and '<form' in search_results_html:
                                        logger.warning("⚠️ Redirect led back to search form - submission may have failed")
                                        # Don't continue - still search this HTML in case results are embedded
                                    break
                                continue
                        
                        if response.status_code == 200:
                            search_results_html = response.text
                            logger.info(f"✅ Successfully submitted search form to {endpoint}")
                            # Check if we got the search form back (submission failed)
                            if 'Search Options' in search_results_html and 'id="searchForm"' in search_results_html:
                                logger.warning("⚠️ Response contains search form - form submission may have been rejected")
                                logger.warning("💡 This could mean: missing required fields, invalid CSRF token, or form validation failed")
                                # Check for error messages in the HTML
                                if 'error' in search_results_html.lower() or 'invalid' in search_results_html.lower():
                                    error_match = re.search(r'<div[^>]*class=["\'][^"\']*error[^"\']*["\'][^>]*>(.*?)</div>', search_results_html, re.IGNORECASE | re.DOTALL)
                                    if error_match:
                                        logger.error(f"❌ Form error: {error_match.group(1)[:200]}")
                                continue
                            # Log a sample of the response to see what we got
                            logger.debug(f"📄 Response preview (first 1000 chars): {search_results_html[:1000]}")
                            break
                        else:
                            logger.debug(f"⚠️ POST to {endpoint} returned status {response.status_code}")
                            logger.debug(f"📄 Response preview: {response.text[:500]}")
                    except Exception as e:
                        logger.debug(f"⚠️ Error POSTing to {endpoint}: {e}")
                        continue
            
            # Note: Django forms typically require POST, so we don't try GET as fallback
            
            if not search_results_html:
                logger.warning("⚠️ Could not retrieve search results page")
                logger.warning("💡 The form submission may have failed. Check form field names and submission method.")
                # Log the HTML form structure for debugging
                if html_content:
                    form_match = re.search(r'<form[^>]*>.*?</form>', html_content, re.IGNORECASE | re.DOTALL)
                    if form_match:
                        logger.debug(f"📋 Form HTML structure: {form_match.group(0)[:1000]}")
                return ptrs
            
            # Check if we got JSON response (DataTables format)
            import json
            try:
                # Try to parse as JSON first (DataTables response)
                json_data = json.loads(search_results_html)
                if 'result' in json_data and json_data.get('result') == 'ok' and 'data' in json_data:
                    logger.info(f"✅ Parsing DataTables JSON response with {len(json_data.get('data', []))} records")
                    
                    # Extract PTR links from JSON data array
                    # Each row is: [first_name, last_name, full_name, link_html, date]
                    data_array = json_data.get('data', [])
                    ptr_urls = set()
                    ptr_data_map = {}
                    
                    for row in data_array:
                        if len(row) >= 4:
                            # Row format: [first_name, last_name, full_name, link_html, date]
                            full_name = row[2] if len(row) > 2 else ''
                            link_html = row[3] if len(row) > 3 else ''
                            filing_date_str = row[4] if len(row) > 4 else ''
                            
                            # Extract UUID from link HTML: <a href="/search/view/ptr/c6456d94-2e65-4740-87e8-15f307c7e596/" ...
                            uuid_match = re.search(r'/ptr/([a-f0-9-]+)', link_html, re.IGNORECASE)
                            if uuid_match:
                                uuid = uuid_match.group(1)
                                full_url = f"https://efdsearch.senate.gov/search/view/ptr/{uuid}/"
                                ptr_urls.add(full_url)
                                
                                # Store metadata
                                ptr_data_map[full_url] = {
                                    'filer_name': full_name,
                                    'filing_date': filing_date_str,
                                    'uuid': uuid,
                                    'link_html': link_html  # Store link_html for later use
                                }
                                
                                logger.info(f"📄 Found PTR: {full_name} - {full_url}")
                            else:
                                logger.warning(f"⚠️ Could not extract UUID from link: {link_html[:100]}")
                    
                    logger.info(f"✅ Extracted {len(ptr_urls)} PTR URLs from JSON response")
                    
                    # For now, skip transaction extraction in fetcher (session cookie issues)
                    # The downloader Lambda will download the HTML with its own session,
                    # and the matcher will parse transactions from S3
                    # This avoids session management complexity in the fetcher
                    logger.info(f"💡 Skipping transaction extraction in fetcher - downloader will handle HTML download")
                    logger.info(f"💡 Matcher will parse transactions from S3 HTML files")
                    
                    # Process each PTR URL: Just extract metadata, no HTML download/parsing
                    for ptr_url in ptr_urls:
                        metadata = ptr_data_map.get(ptr_url, {})
                        filer_name = metadata.get('filer_name', 'Unknown')
                        filing_date_str = metadata.get('filing_date', '')
                        uuid = metadata.get('uuid', '')
                        link_html = metadata.get('link_html', '')  # Get link_html from metadata
                        
                        # Parse filing date
                        filing_date = None
                        if filing_date_str:
                            try:
                                # datetime is already imported at the top of the file
                                filing_date = datetime.strptime(filing_date_str, '%m/%d/%Y').date()
                            except Exception as e:
                                logger.debug(f"⚠️ Could not parse filing date '{filing_date_str}': {e}")
                                continue
                        
                        # Only process if filing date matches target date
                        if filing_date and filing_date != target_date_obj.date():
                            logger.debug(f"⏭️ Skipping PTR from {filing_date} (not {target_date_obj.date()})")
                            continue
                        
                        # Extract the href from the JSON data - it's a relative path like "/search/view/ptr/..."
                        # href format: "/search/view/ptr/c6456d94-2e65-4740-87e8-15f307c7e596/"
                        base_url = "https://efdsearch.senate.gov"
                        href_match = re.search(r'href=["\']([^"\']+)["\']', link_html, re.IGNORECASE)
                        if href_match:
                            href_path = href_match.group(1)
                            logger.info(f"📥 Extracted href path from JSON: {href_path}")
                            
                            # Build full URL by combining base URL with relative href
                            if href_path.startswith('/'):
                                view_url = f"{base_url}{href_path}"
                            elif href_path.startswith('http'):
                                view_url = href_path  # Already absolute
                            else:
                                # Relative path without leading slash - shouldn't happen but handle it
                                view_url = f"{base_url}/{href_path}"
                            logger.info(f"📥 Built full URL from href: {view_url}")
                        else:
                            # Fallback: construct URL from UUID if href extraction fails
                            view_url = f"{base_url}/search/view/ptr/{uuid}/"
                            logger.warning(f"⚠️ Could not extract href from link_html, using UUID fallback: {view_url}")
                        
                        # Skip downloading HTML in fetcher - let downloader handle it
                        # This avoids session cookie issues in the fetcher
                        # The downloader Lambda has its own session management and will download the HTML
                        # The matcher will parse transactions from the S3 HTML files
                        logger.info(f"📋 PTR URL ready for downloader: {view_url}")
                        logger.info(f"💡 Fetcher skipping HTML download/parsing - downloader will handle it")
                        
                        # No transactions extracted in fetcher - will be parsed by matcher from S3
                        transactions = []
                        
                        # All HTML download/parsing logic removed
                        # The downloader Lambda will handle downloading with its own session
                        # The matcher will parse transactions from S3 HTML files
                        
                        # No exception handling needed - just return empty transactions
                        
                        # Fetcher should NOT create s3_key - downloader will handle that
                        # Just return the metadata needed for downloading
                        ptr_info = {
                            'url': ptr_url,
                            'view_url': view_url,  # Store view_url (not print_url)
                            # NO s3_key - downloader will create it in YYYY-MM-DD format
                            'formType': 'senate_ptr',
                            'form_type': 'senate_ptr',
                            'source': 'senate',
                            'filingDate': filing_date_str if filing_date_str else date_str,
                            'filing_date': filing_date_str if filing_date_str else date_str,
                            'filer_name': filer_name,
                            'uuid': uuid,
                            'transactions': transactions  # Empty - will be parsed by matcher from S3
                        }
                        
                        ptrs.append(ptr_info)
                        logger.info(f"✅ Added Senate PTR: {filer_name} - {filing_date_str} (0 transactions - will be parsed by matcher)")
                    
                    logger.info(f"✅ Successfully extracted {len(ptrs)} Senate PTRs")
                    return ptrs
                    
            except (json.JSONDecodeError, ValueError):
                # Not JSON, try parsing as HTML (fallback)
                logger.info("📄 Response is not JSON - parsing as HTML...")
            
            # HTML parsing fallback code would go here if needed
            # For now, we rely on the JSON response method above
            logger.warning("⚠️ Search results were not in JSON format - returning empty list")
            return []
        
        except Exception as e:
            logger.error(f"❌ Error fetching Senate PTRs: {e}")
            import traceback as tb
            logger.error(f"Traceback: {tb.format_exc()}")
            return []
        
        except (json.JSONDecodeError, ValueError):
            # Not JSON, try parsing as HTML (fallback)
            logger.info("📄 Response is not JSON - parsing as HTML...")
            
            # Log search results page structure for debugging (HTML fallback)
            logger.info(f"📄 Search results page preview (first 5000 chars): {search_results_html[:5000]}")
            logger.info(f"📊 Full response length: {len(search_results_html)} characters")
            
            # Also log a sample from the middle and end to see if results are there
            if len(search_results_html) > 10000:
                mid_point = len(search_results_html) // 2
                logger.info(f"📄 Middle section (chars {mid_point-1000} to {mid_point+1000}): {search_results_html[mid_point-1000:mid_point+1000]}")
                logger.info(f"📄 End section (last 2000 chars): {search_results_html[-2000:]}")
            
            # Check if results might be in a specific section (HTML parsing)
            if 'view/ptr' in search_results_html.lower() or '/ptr/' in search_results_html.lower():
                # Find where PTR links appear
                ptr_positions = []
                for match in re.finditer(r'/ptr/([a-f0-9-]+)', search_results_html, re.IGNORECASE):
                    ptr_positions.append(match.start())
                
                if ptr_positions:
                    first_ptr_pos = ptr_positions[0]
                    logger.info(f"💡 Found PTR links starting at character position {first_ptr_pos}")
                    logger.info(f"📄 Context around first PTR (chars {max(0, first_ptr_pos-500)} to {first_ptr_pos+500}): {search_results_html[max(0, first_ptr_pos-500):first_ptr_pos+500]}")
            
            # Check if we got the search form back (which means submission failed)
            is_form_page = 'id="searchForm"' in search_results_html or ('Search Options' in search_results_html and '<form' in search_results_html)
            
            if is_form_page:
                logger.warning("⚠️ Got search form back - form submission was rejected or not processed")
                
                # Look for Django form validation errors
                error_patterns = [
                    r'<ul[^>]*class=["\'][^"\']*errorlist[^"\']*["\'][^>]*>(.*?)</ul>',
                    r'<div[^>]*class=["\'][^"\']*alert[^"\']*["\'][^>]*>(.*?)</div>',
                    r'<span[^>]*class=["\'][^"\']*error[^"\']*["\'][^>]*>(.*?)</span>',
                ]
                
                found_errors = []
                for pattern in error_patterns:
                    error_matches = re.finditer(pattern, search_results_html, re.IGNORECASE | re.DOTALL)
                    for match in error_matches:
                        error_text = re.sub(r'<[^>]+>', '', match.group(1)).strip()
                        if error_text:
                            found_errors.append(error_text[:200])
                
                if found_errors:
                    logger.error(f"❌ Form validation errors found: {found_errors}")
                else:
                    logger.info("💡 No Django validation errors found in HTML")
                
                # Check if the form values are still there (suggests validation failure)
                if date_str in search_results_html:
                    logger.info(f"✅ Date values are still in form: {date_str}")
                
                # Look deeper in the HTML for results that might be hidden or after the form
                # Sometimes results appear after the form or in a different section
                if 'view/ptr' in search_results_html.lower() or '/ptr/' in search_results_html.lower():
                    logger.info("💡 Found PTR links in response - results might be present!")
                else:
                    logger.warning("💡 No PTR links found in response")
                
                # Check if there's a results section we're missing
                # Look for DataTables initialization or results container
                if 'dataTables' in search_results_html.lower() or 'search-results' in search_results_html.lower():
                    logger.info("💡 Found DataTables or results container - results might be loaded dynamically")
                
                # Look for JavaScript that might handle form submission
                logger.info("💡 The form might require JavaScript/AJAX to process")
                
                # Since traditional POST returned form, try checking if results are elsewhere in page
                # Some sites render results on the same page after form submission
                logger.info("💡 Searching entire response for PTR links...")
            else:
                logger.info("✅ Response does not appear to be the search form - might have results!")
            
            # Check if we got an error message or empty results
            if 'no results' in search_results_html.lower() or 'no matches' in search_results_html.lower():
                logger.info("📋 Search returned no results (this might be correct if no PTRs were filed on this date)")
            elif 'error' in search_results_html.lower() and ('form' in search_results_html.lower() or 'invalid' in search_results_html.lower()):
                logger.warning("⚠️ Search may have returned an error - form submission might be incorrect")
            
            # Look for table with results
            if '<table' in search_results_html or '<tbody' in search_results_html:
                logger.info("✅ Found table structure in results - likely contains search results")
            else:
                logger.warning("⚠️ No table structure found in results - may not be the results page")
            
            # Parse search results HTML to find PTR links
            # The results are in a table with columns: FName, LName, Office, Report type, Date received/filed
            # Links are typically in the "Report type" column or filer name columns
            ptr_link_patterns = [
                r'href=["\']([^"\']*view/ptr/[^"\']+)["\']',  # Direct PTR view links
                r'href=["\']([^"\']*ptr/[^"\']+)["\']',  # PTR links
                r'https://efdsearch\.senate\.gov/search/view/ptr/([a-f0-9-]+)',  # UUID-based links
                r'/search/view/ptr/([a-f0-9-]+)/',  # UUID in path
            ]
            
            ptr_urls = set()  # Use set to avoid duplicates
            ptr_data_map = {}  # Map URL to metadata (filer name, etc.)
            
            # First, try to find links using patterns - search ENTIRE HTML response
            logger.info(f"📊 Searching entire response for PTR links (response length: {len(search_results_html)} chars)")
            
            for pattern in ptr_link_patterns:
                matches = re.finditer(pattern, search_results_html, re.IGNORECASE)
                for match in matches:
                    if match.groups():
                        # Pattern with capture group (UUID)
                        uuid = match.group(1)
                        # Clean UUID (remove trailing slash, query params, quotes)
                        uuid = uuid.split('/')[0].split('?')[0].split('"')[0].split("'")[0].strip()
                        if uuid and len(uuid) > 10:  # UUIDs are typically 36 chars with dashes
                            full_url = f"https://efdsearch.senate.gov/search/view/ptr/{uuid}/"
                            ptr_urls.add(full_url)
                            logger.debug(f"💡 Found PTR UUID: {uuid}")
                    else:
                        # Full URL pattern
                        link = match.group(0)
                        if link.startswith('/'):
                            full_url = f"https://efdsearch.senate.gov{link}"
                            ptr_urls.add(full_url)
                        elif link.startswith('http'):
                            full_url = link
                            ptr_urls.add(full_url)
                        else:
                            full_url = urljoin(search_url, link)
                            ptr_urls.add(full_url)
            
            logger.info(f"📋 Found {len(ptr_urls)} unique PTR URLs using link patterns")
            
            # Also try to parse the results table directly
            # Look for table rows with PTR data
            # The table might have structure like: <tr><td>Rick</td><td>Scott</td><td>...</td></tr>
            table_row_pattern = r'<tr[^>]*>.*?Periodic Transaction Report.*?</tr>'
            table_rows = re.finditer(table_row_pattern, search_results_html, re.IGNORECASE | re.DOTALL)
            
            for row_match in table_rows:
                row_html = row_match.group(0)
                
                # Extract UUID from any links in the row
                uuid_match = re.search(r'/ptr/([a-f0-9-]+)', row_html, re.IGNORECASE)
                if uuid_match:
                    uuid = uuid_match.group(1)
                    full_url = f"https://efdsearch.senate.gov/search/view/ptr/{uuid}/"
                    ptr_urls.add(full_url)
                    
                    # Try to extract filer name from table cells
                    # Look for name pattern in <td> tags
                    name_cells = re.finditer(r'<td[^>]*>([^<]+)</td>', row_html, re.IGNORECASE)
                    names = []
                    for cell_match in name_cells:
                        cell_text = cell_match.group(1).strip()
                        if cell_text and len(cell_text) > 2 and not cell_text.isdigit():
                            names.append(cell_text)
                    
                    if names and len(names) >= 2:
                        # First two non-empty cells are likely first and last name
                        filer_name = f"{names[0]} {names[1]}"
                        ptr_data_map[full_url] = {'filer_name': filer_name}
            
            logger.info(f"📋 Found {len(ptr_urls)} Senate PTR links in search results")
            
            # For each PTR URL, extract metadata and find the PDF download link
            for ptr_url in ptr_urls:
                try:
                    logger.info(f"🔍 Processing Senate PTR: {ptr_url}")
                    
                    # Access the PTR view page to get the download link
                    response = self.session.get(ptr_url, timeout=30)
                    response.raise_for_status()
                    
                    ptr_page_html = response.text
                    
                    # Look for PDF download link on the PTR page
                    # Common patterns: "Print", "Download PDF", "View PDF", etc.
                    pdf_link_patterns = [
                        r'href=["\']([^"\']*\.pdf[^"\']*)["\']',  # Direct PDF links
                        r'href=["\']([^"\']*print[^"\']*)["\']',  # Print links that generate PDFs
                        r'href=["\']([^"\']*download[^"\']*)["\']',  # Download links
                        r'data-pdf-url=["\']([^"\']+)["\']',  # Data attributes
                    ]
                    
                    pdf_url = None
                    
                    for pattern in pdf_link_patterns:
                        match = re.search(pattern, ptr_page_html, re.IGNORECASE)
                        if match:
                            link = match.group(1)
                            
                            # Convert relative URLs to absolute
                            if link.startswith('/'):
                                pdf_url = f"https://efdsearch.senate.gov{link}"
                            elif link.startswith('http'):
                                pdf_url = link
                            else:
                                pdf_url = urljoin(ptr_url, link)
                            
                            # Check if it's actually a PDF link
                            if '.pdf' in pdf_url.lower() or 'print' in pdf_url.lower():
                                break
                    
                    # If no direct PDF link found, the PTR page might have a print endpoint
                    # Try common print endpoints
                    if not pdf_url:
                        # Extract UUID from URL if present
                        uuid_match = re.search(r'/ptr/([a-f0-9-]+)', ptr_url, re.IGNORECASE)
                        if uuid_match:
                            uuid = uuid_match.group(1)
                            # Try common print endpoints
                            print_endpoints = [
                                f"https://efdsearch.senate.gov/search/view/ptr/{uuid}/print/",
                                f"https://efdsearch.senate.gov/search/ptr/{uuid}/print/",
                                f"https://efdsearch.senate.gov/search/print/ptr/{uuid}/",
                            ]
                            
                            for endpoint in print_endpoints:
                                # Test if endpoint exists (HEAD request)
                                try:
                                    test_response = self.session.head(endpoint, timeout=10, allow_redirects=True)
                                    if test_response.status_code == 200 or test_response.status_code == 302:
                                        pdf_url = endpoint
                                        break
                                except:
                                    continue
                    
                    # Extract filer name from the PTR page or use cached value from search results
                    filer_name = None
                    
                    # Check if we already have the filer name from search results
                    if ptr_url in ptr_data_map and 'filer_name' in ptr_data_map[ptr_url]:
                        filer_name = ptr_data_map[ptr_url]['filer_name']
                    else:
                        # Try to extract from PTR page
                        name_patterns = [
                            r'<h[1-6][^>]*>([^<]*The Honorable[^<]+)</h[1-6]>',  # Header with "The Honorable"
                            r'The Honorable\s+([A-Z][a-z]+(?:\s+[A-Z][a-z.]+)+)',  # "The Honorable First Last"
                            r'<td[^>]*>([A-Z][a-z]+\s+[A-Z][a-z]+)</td>',  # Table cell with name
                            r'<title>([^<]*The Honorable[^<]+)</title>',  # Title tag
                        ]
                        
                        for pattern in name_patterns:
                            match = re.search(pattern, ptr_page_html)
                            if match:
                                filer_name = match.group(1).strip()
                                # Clean up HTML tags and extra whitespace
                                filer_name = re.sub(r'<[^>]+>', '', filer_name)
                                filer_name = re.sub(r'\s+', ' ', filer_name).strip()
                                # Remove "The Honorable" title
                                filer_name = re.sub(r'\bThe Honorable\b', '', filer_name, flags=re.IGNORECASE).strip()
                                if filer_name:
                                    break
                    
                    # If no PDF URL found, try to construct it from the UUID
                    if not pdf_url and uuid_match:
                        uuid = uuid_match.group(1)
                        # Try direct PDF endpoint
                        pdf_url = f"https://efdsearch.senate.gov/search/view/ptr/{uuid}/print/"
                    
                    if pdf_url:
                        # Generate filename from URL or use UUID
                        if uuid_match:
                            filename = f"senate-ptr-{uuid_match.group(1)}.pdf"
                        else:
                            filename = f"senate-ptr-{target_date}.pdf"
                        
                        # Extract last name from filer name if available
                        if filer_name:
                            name_parts = filer_name.split()
                            if len(name_parts) >= 2:
                                last_name = name_parts[-1]
                                filename = f"senate-ptr-{last_name.lower()}-{target_date}.pdf"
                        
                        ptr_data = {
                            'filer_name': filer_name or 'Unknown',
                            'filing_date': target_date,
                            'form_type': 'senate_ptr',
                            'url': pdf_url,
                            's3_key': f"trades/{target_date}/senate/{filename}"
                        }
                        
                        ptrs.append(ptr_data)
                        logger.info(f"✅ Found Senate PTR: {filer_name or 'Unknown'} - {filename}")
                    else:
                        logger.warning(f"⚠️ Could not find PDF download URL for Senate PTR: {ptr_url}")
                        
                except Exception as ptr_error:
                    logger.error(f"❌ Error processing Senate PTR {ptr_url}: {ptr_error}")
                    import traceback
                    logger.debug(f"Traceback: {traceback.format_exc()}")
                    continue
            
            logger.info(f"📊 Found {len(ptrs)} Senate PTRs for {target_date}")
            
        except Exception as e:
            logger.error(f"❌ Error fetching Senate PTRs: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
        
        return ptrs
    
    def extract_filing_date_from_pdf(self, pdf_content: bytes, target_date: str) -> Optional[str]:
        """
        Use AWS Textract to extract filing date from PTR PDF
        
        Args:
            pdf_content: PDF file content as bytes
            target_date: Target date in YYYY-MM-DD format (for validation)
            
        Returns:
            Extracted filing date in YYYY-MM-DD format, or None if not found
        """
        try:
            textract_client = boto3.client('textract')
            
            logger.info(f"🔍 Using Textract to extract filing date from PTR PDF...")
            
            # Call Textract to extract text
            response = textract_client.detect_document_text(
                Document={'Bytes': pdf_content}
            )
            
            # Extract all text from Textract response
            text_lines = []
            for block in response.get('Blocks', []):
                if block.get('BlockType') == 'LINE':
                    text = block.get('Text', '').strip()
                    if text:
                        text_lines.append(text)
            
            full_text = ' '.join(text_lines)
            
            # First, verify this is actually a PTR document
            # Look for PTR-specific keywords
            ptr_keywords = [
                'periodic transaction report',
                'ptr',
                'stock act',
                'financial disclosure',
                'transaction report',
                'clerk of the house',
                'representative',
                'member of congress'
            ]
            
            text_lower = full_text.lower()
            has_ptr_content = any(keyword in text_lower for keyword in ptr_keywords)
            
            if not has_ptr_content:
                logger.warning(f"⚠️ Document does not appear to be a PTR (missing PTR-specific content)")
                return None
            
            # Look for date patterns in the extracted text
            # PTRs typically have dates like "Date Filed: MM/DD/YYYY" or "Filing Date: MM/DD/YYYY"
            date_patterns = [
                r'(?:Date Filed|Filing Date|Date|Report Date)[\s:]*(\d{1,2})[/-](\d{1,2})[/-](\d{4})',
                r'(\d{1,2})[/-](\d{1,2})[/-](\d{4})',  # Generic MM/DD/YYYY or MM-DD-YYYY
                r'(\d{4})[/-](\d{1,2})[/-](\d{1,2})',  # Generic YYYY/MM/DD or YYYY-MM-DD
            ]
            
            target_date_obj = datetime.strptime(target_date, '%Y-%m-%d')
            
            for pattern in date_patterns:
                matches = re.finditer(pattern, full_text, re.IGNORECASE)
                for match in matches:
                    try:
                        # Try to parse the date
                        date_str = match.group(0)
                        # Clean up the date string
                        date_str = re.sub(r'[^\d/-]', '', date_str)
                        
                        # Try different date formats
                        date_formats = [
                            '%m/%d/%Y',
                            '%m-%d-%Y',
                            '%Y/%m/%d',
                            '%Y-%m-%d',
                            '%d/%m/%Y',
                            '%d-%m-%Y',
                        ]
                        
                        parsed_date = None
                        for fmt in date_formats:
                            try:
                                parsed_date = datetime.strptime(date_str, fmt).date()
                                break
                            except ValueError:
                                continue
                        
                        if parsed_date:
                            # Check if this date matches the target date (or is within a few days)
                            date_diff = abs((parsed_date - target_date_obj.date()).days)
                            if date_diff <= 1:  # Allow 1 day tolerance
                                filing_date_str = parsed_date.strftime('%Y-%m-%d')
                                logger.info(f"✅ Extracted filing date: {filing_date_str} (target: {target_date})")
                                return filing_date_str
                            else:
                                logger.debug(f"📅 Found date {parsed_date} but doesn't match target {target_date}")
                    except Exception as date_parse_error:
                        logger.debug(f"⚠️ Could not parse date from match: {match.group(0)}: {date_parse_error}")
                        continue
            
            logger.warning(f"⚠️ Could not extract filing date from PTR PDF (target: {target_date})")
            return None
            
        except Exception as e:
            error_msg = str(e)
            # Handle unsupported document format (e.g., corrupted PDFs, non-PDF files)
            if 'UnsupportedDocumentException' in error_msg or 'UnsupportedDocumentFormat' in error_msg:
                logger.warning(f"⚠️ Document format not supported by Textract (likely not a valid PDF): {error_msg}")
                return None
            logger.error(f"❌ Error extracting filing date with Textract: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return None
    
    def download_ptr_file(self, url: str, s3_key: str, s3_bucket: str, s3_client, target_date: str = None) -> bool:
        """
        Download a PTR file from URL, verify filing date with Textract, and upload to S3
        
        Args:
            url: Source URL of the PTR PDF
            s3_key: Destination S3 key
            s3_bucket: S3 bucket name
            s3_client: Boto3 S3 client
            target_date: Target filing date (YYYY-MM-DD) - if provided, will filter by actual filing date
            
        Returns:
            True if successful and matches target date, False otherwise
        """
        try:
            logger.info(f"📥 Downloading PTR from {url}")
            
            # Download file
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            pdf_content = response.content
            
            # If target_date is provided, extract and verify filing date using Textract
            if target_date:
                extracted_date = self.extract_filing_date_from_pdf(pdf_content, target_date)
                if not extracted_date:
                    logger.warning(f"⚠️ PTR from {url} does not match target date {target_date}, skipping")
                    return False
                logger.info(f"✅ PTR filing date verified via Textract: {extracted_date} (target: {target_date})")
            
            # Upload to S3
            s3_client.put_object(
                Bucket=s3_bucket,
                Key=s3_key,
                Body=pdf_content,
                ContentType='application/pdf'
            )
            
            logger.info(f"✅ Uploaded PTR to S3: {s3_key}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error downloading/uploading PTR: {e}")
            return False
    
    def __del__(self):
        """Cleanup session"""
        if hasattr(self, 'session'):
            self.session.close()

