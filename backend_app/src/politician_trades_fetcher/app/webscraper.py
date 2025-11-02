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

logger = logging.getLogger()
logger.setLevel(logging.INFO)

class CongressionalPTRScraper:
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
            
            html_content = response.text
            
            # Log first part of HTML to debug form structure
            logger.info(f"📄 HTML content preview (first 3000 chars): {html_content[:3000]}")
            
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
            csrf_token = None
            csrf_pattern = r'name=["\']csrfmiddlewaretoken["\'][^>]*value=["\']([^"\']+)["\']'
            csrf_match = re.search(csrf_pattern, html_content, re.IGNORECASE)
            if csrf_match:
                csrf_token = csrf_match.group(1)
                logger.info(f"✅ Found CSRF token")
            else:
                logger.warning("⚠️ Could not find CSRF token - form submission may fail")
            
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
            
            # Prepare form data for POST request
            # Based on actual form structure from HTML
            form_data = {}
            
            # Report type checkbox: name="report_type", value="11" for Periodic Transactions
            if report_type_value:
                form_data[report_type_field] = report_type_value
            else:
                form_data['report_type'] = '11'  # Periodic Transactions value
            
            # Date fields: name="submitted_start_date" and name="submitted_end_date"
            form_data[date_from_field] = date_str
            form_data[date_to_field] = date_str
            
            # CSRF token: name="csrfmiddlewaretoken" (required for Django)
            if csrf_token:
                form_data['csrfmiddlewaretoken'] = csrf_token
            else:
                logger.error("❌ CSRF token is required but not found - form submission will fail")
                return ptrs
            
            # Set headers for form submission
            headers = {
                'Content-Type': 'application/x-www-form-urlencoded',
                'Referer': search_url,
                'Origin': 'https://efdsearch.senate.gov',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            }
            
            # Submit search form
            logger.info(f"🔍 Submitting search form for date range: {date_str} to {date_str}")
            logger.info(f"📋 Form data: {form_data}")
            
            # Try POST to search endpoint
            # Use form action if found, otherwise try common endpoints
            search_endpoints = []
            if form_action:
                search_endpoints.append(form_action)
            search_endpoints.extend([
                search_url,
                f"{search_url}results/",
                f"{search_url}search/",
                "https://efdsearch.senate.gov/search/results/",
            ])
            
            search_results_html = None
            
            for endpoint in search_endpoints:
                try:
                    logger.info(f"🔍 Trying POST to: {endpoint}")
                    response = self.session.post(
                        endpoint,
                        data=form_data,
                        headers=headers,
                        timeout=30,
                        allow_redirects=True
                    )
                    
                    if response.status_code == 200:
                        search_results_html = response.text
                        logger.info(f"✅ Successfully submitted search form to {endpoint}")
                        # Log a sample of the response to see what we got
                        logger.debug(f"📄 Response preview (first 1000 chars): {search_results_html[:1000]}")
                        break
                    elif response.status_code == 302 or response.status_code == 301:
                        # Redirect - follow it
                        redirect_url = response.headers.get('Location')
                        if redirect_url:
                            if redirect_url.startswith('/'):
                                redirect_url = urljoin(endpoint, redirect_url)
                            logger.info(f"🔄 Following redirect to: {redirect_url}")
                            redirect_response = self.session.get(redirect_url, timeout=30)
                            if redirect_response.status_code == 200:
                                search_results_html = redirect_response.text
                                logger.info(f"✅ Got results after redirect")
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
            
            # Log search results page structure for debugging
            logger.info(f"📄 Search results page preview (first 3000 chars): {search_results_html[:3000]}")
            
            # Check if we got the search form back (which means submission failed)
            if '<form' in search_results_html and 'Search' in search_results_html:
                logger.warning("⚠️ Got search form back - form submission may have failed or been rejected")
                logger.warning("💡 Response suggests form was not submitted correctly")
            
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
            
            # First, try to find links using patterns
            for pattern in ptr_link_patterns:
                matches = re.finditer(pattern, search_results_html, re.IGNORECASE)
                for match in matches:
                    if match.groups():
                        # Pattern with capture group (UUID)
                        uuid = match.group(1)
                        full_url = f"https://efdsearch.senate.gov/search/view/ptr/{uuid}/"
                    else:
                        # Full URL pattern
                        link = match.group(0)
                        if link.startswith('/'):
                            full_url = f"https://efdsearch.senate.gov{link}"
                        elif link.startswith('http'):
                            full_url = link
                        else:
                            full_url = urljoin(search_url, link)
                    
                    ptr_urls.add(full_url)
            
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

