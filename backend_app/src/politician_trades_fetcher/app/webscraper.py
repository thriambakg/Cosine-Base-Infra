"""
Web Scraper Helper Class for Congressional PTRs
Scrapes Senate and House financial disclosure forms (PTRs)
- Senate: efdsearch.senate.gov
- House: disclosures-clerk.house.gov
"""

import logging
import requests
from typing import List, Dict, Any
from datetime import datetime
import re
from urllib.parse import urljoin
import json
import traceback as tb

logger = logging.getLogger()
logger.setLevel(logging.INFO)

class CongressionalPTRScraper:
    
    """Scraper for Senate and House Periodic Transaction Reports (PTRs)"""
    
    # House Clerk base URL
    HOUSE_BASE_URL = "https://disclosures-clerk.house.gov"
    HOUSE_SEARCH_ENDPOINT = f"{HOUSE_BASE_URL}/FinancialDisclosure/ViewMemberSearchResult"
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36'
        })
    
    def fetch_senate_ptrs(self, target_date: str) -> List[Dict[str, Any]]:
        """
        Fetch Senate PTRs for a specific date from Senate Ethics website
        
        Senate PTRs are published at: https://efdsearch.senate.gov/search/
        This site uses a search interface that requires form submission
        
        Args:
            target_date: Date in YYYY-MM-DD format
            
        Returns:
            List of PTR metadata dicts with url, view_url, filing_date, filer_name, uuid, etc.
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
            
            # First, GET the search page to get session cookies and CSRF tokens
            logger.info(f"🔍 Accessing Senate PTR search page: {search_url}")
            response = self.session.get(search_url, timeout=30)
            response.raise_for_status()
            
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
                            'prohibition_agreement': '1',
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
                        
                        if agreement_response.status_code in [200, 302, 303]:
                            # Update session cookies from response
                            self.session.cookies.update(agreement_response.cookies)
                            return agreement_response.text
                        else:
                            logger.warning(f"⚠️ Agreement submission returned status {agreement_response.status_code}")
                    else:
                        logger.warning("⚠️ Could not find CSRF token in agreement form")
                return current_html
            
            # Check if we need to accept the agreement first
            html_content = accept_agreement_if_needed(html_content)
            
            # Extract CSRF token from search form (after agreement is accepted)
            csrf_token = None
            csrf_pattern = r'name=["\']csrfmiddlewaretoken["\'][^>]*value=["\']([^"\']+)["\']'
            csrf_match = re.search(csrf_pattern, html_content, re.IGNORECASE)
            if csrf_match:
                csrf_token = csrf_match.group(1)
                logger.info(f"✅ Found CSRF token: {csrf_token[:20]}...")
            else:
                csrf_cookie = self.session.cookies.get('csrftoken') or self.session.cookies.get('csrfmiddlewaretoken')
                if csrf_cookie:
                    csrf_token = csrf_cookie
                    logger.info("💡 Using CSRF cookie value")
                else:
                    logger.error("❌ CSRF token is required but not found")
                    return ptrs
            
            # For the AJAX endpoint, we need DataTables format with time appended to dates
            ajax_date_start = f"{date_str} 00:00:00"
            ajax_date_end = f"{date_str} 23:59:59"
            
            def build_datatables_payload(draw_num: int, start: int = 0, length: int = 25) -> dict:
                """Build DataTables AJAX payload with pagination parameters"""
                return {
                    'draw': str(draw_num),
                    'columns[0][data]': '0', 'columns[0][name]': '', 'columns[0][searchable]': 'true', 'columns[0][orderable]': 'true',
                    'columns[0][search][value]': '', 'columns[0][search][regex]': 'false',
                    'columns[1][data]': '1', 'columns[1][name]': '', 'columns[1][searchable]': 'true', 'columns[1][orderable]': 'true',
                    'columns[1][search][value]': '', 'columns[1][search][regex]': 'false',
                    'columns[2][data]': '2', 'columns[2][name]': '', 'columns[2][searchable]': 'true', 'columns[2][orderable]': 'true',
                    'columns[2][search][value]': '', 'columns[2][search][regex]': 'false',
                    'columns[3][data]': '3', 'columns[3][name]': '', 'columns[3][searchable]': 'true', 'columns[3][orderable]': 'true',
                    'columns[3][search][value]': '', 'columns[3][search][regex]': 'false',
                    'columns[4][data]': '4', 'columns[4][name]': '', 'columns[4][searchable]': 'true', 'columns[4][orderable]': 'true',
                    'columns[4][search][value]': '', 'columns[4][search][regex]': 'false',
                    'order[0][column]': '1', 'order[0][dir]': 'asc',
                    'order[1][column]': '0', 'order[1][dir]': 'asc',
                    'start': str(start), 'length': str(length),
                    'search[value]': '', 'search[regex]': 'false',
                    'report_types': '[11]',  # Periodic Transactions
                    'filer_types': '[]',
                    'submitted_start_date': ajax_date_start,
                    'submitted_end_date': ajax_date_end,
                    'candidate_state': '', 'senator_state': '', 'office_id': '',
                    'first_name': '', 'last_name': '',
                    'csrfmiddlewaretoken': csrf_token
                }
            
            # Prepare AJAX headers
            ajax_headers = {
                'Content-Type': 'application/x-www-form-urlencoded',
                'X-Requested-With': 'XMLHttpRequest',
                'X-CSRFToken': csrf_token,
                'Referer': search_url,
                'Origin': 'https://efdsearch.senate.gov',
                'Accept': 'application/json, text/javascript, */*; q=0.01'
            }
            
            # Try AJAX endpoints
            ajax_endpoints = [
                f"{search_url}report/data/",
                "https://efdsearch.senate.gov/search/report/data/",
                f"{search_url}results/",
                f"{search_url}data/",
            ]
            
            working_ajax_endpoint = None
            ajax_worked = False
            
            # First, find a working AJAX endpoint
            for ajax_endpoint in ajax_endpoints:
                try:
                    logger.info(f"📡 Testing AJAX endpoint: {ajax_endpoint}")
                    test_payload = build_datatables_payload(draw_num=1, start=0, length=25)
                    ajax_response = self.session.post(
                        ajax_endpoint,
                        data=test_payload,
                        headers=ajax_headers,
                        timeout=30
                    )
                    
                    if ajax_response.status_code == 200:
                        try:
                            response_data = json.loads(ajax_response.text)
                            if 'result' in response_data and response_data.get('result') == 'ok':
                                logger.info(f"✅ AJAX endpoint {ajax_endpoint} is working!")
                                ajax_worked = True
                                working_ajax_endpoint = ajax_endpoint
                                break
                        except json.JSONDecodeError:
                            continue
                except Exception as e:
                    logger.debug(f"⚠️ Error testing {ajax_endpoint}: {e}")
                    continue
            
            if not ajax_worked:
                logger.error("❌ All AJAX endpoints failed")
                return ptrs
            
            # Fetch all pages with pagination
            logger.info(f"✅ Using AJAX endpoint {working_ajax_endpoint} for pagination")
            
            all_ptr_data_map = {}  # Map of view_url -> metadata
            draw_num = 1
            page_size = 25
            start = 0
            total_records = None
            
            while True:
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
                        logger.warning(f"⚠️ Page {draw_num} returned status {page_response.status_code}, stopping")
                        break
                    
                    page_data = json.loads(page_response.text)
                    
                    if 'result' not in page_data or page_data.get('result') != 'ok':
                        logger.warning(f"⚠️ Page {draw_num} result != 'ok': {page_data.get('result')}, stopping")
                        break
                    
                    data_array = page_data.get('data', [])
                    
                    if not data_array:
                        logger.info(f"✅ Page {draw_num} returned 0 records - end of pagination")
                        break
                    
                    if 'recordsTotal' in page_data:
                        total_records = page_data.get('recordsTotal')
                        logger.info(f"📊 Total records available: {total_records}")
                    
                    logger.info(f"✅ Page {draw_num}: Found {len(data_array)} PTR records")
                    
                    # Extract PTR metadata from this page
                    for row in data_array:
                        if len(row) >= 4:
                            # Data structure:
                            # row[0] = First name (or first part of name)
                            # row[1] = Last name (or last part of name)  
                            # row[2] = Position (always "Senator" for Senate PTRs) - not used, we construct name from row[0] and row[1]
                            # row[3] = Link HTML
                            # row[4] = Filing date
                            
                            # Extract data from row - Senate PTRs only
                            first_name_part = ''
                            last_name_part = ''
                            link_html = ''
                            filing_date_str = ''
                            
                            if len(row) > 0 and row[0]:
                                first_name_part = str(row[0]).strip()
                            if len(row) > 1 and row[1]:
                                last_name_part = str(row[1]).strip()
                            if len(row) > 3 and row[3]:
                                link_html = str(row[3])
                            if len(row) > 4 and row[4]:
                                filing_date_str = str(row[4])
                            
                            # Simple name construction for Senate PTRs:
                            # Always construct as "Last, First (Senator)" from row[0] and row[1]
                            if first_name_part and last_name_part:
                                full_name = f"{last_name_part}, {first_name_part} (Senator)"
                            elif first_name_part or last_name_part:
                                # Fallback if only one part available
                                parts = f"{last_name_part} {first_name_part}".strip()
                                full_name = f"{parts} (Senator)" if parts else 'Unknown'
                            else:
                                full_name = 'Unknown'
                            
                            # Extract UUID from link HTML - support both /ptr/ and /paper/ URLs
                            # Some filings (e.g., amendments, certain report types) use /paper/ instead of /ptr/
                            uuid_match = re.search(r'/(?:ptr|paper)/([a-f0-9-]+)', link_html, re.IGNORECASE)
                            if uuid_match:
                                uuid = uuid_match.group(1)
                                base_url = "https://efdsearch.senate.gov"
                                
                                # Extract href path (preserves original path, whether /ptr/ or /paper/)
                                href_match = re.search(r'href=["\']([^"\']+)["\']', link_html, re.IGNORECASE)
                                if href_match:
                                    href_path = href_match.group(1)
                                    if href_path.startswith('/'):
                                        view_url = f"{base_url}{href_path}"
                                    else:
                                        # Determine URL type from href or default to /ptr/
                                        if '/paper/' in href_path.lower():
                                            view_url = f"{base_url}/search/view/paper/{uuid}/"
                                        else:
                                            view_url = f"{base_url}/search/view/ptr/{uuid}/"
                                else:
                                    # Default to /ptr/ if we can't determine from href
                                    view_url = f"{base_url}/search/view/ptr/{uuid}/"
                                
                                # Store metadata (avoid duplicates)
                                if view_url not in all_ptr_data_map:
                                    all_ptr_data_map[view_url] = {
                                        'filer_name': full_name,
                                        'filing_date': filing_date_str,
                                        'uuid': uuid,
                                        'link_html': link_html
                                    }
                            else:
                                logger.warning(f"⚠️ Could not extract UUID from link: {link_html[:100]}")
                    
                    # Check if we've fetched all records
                    if total_records is not None and start + len(data_array) >= total_records:
                        logger.info(f"✅ Fetched all {total_records} records across {draw_num} pages")
                        break
                    
                    # Move to next page
                    draw_num += 1
                    start += page_size
                    
                    # Safety limit
                    if draw_num > 100:
                        logger.warning(f"⚠️ Reached pagination limit (100 pages), stopping")
                        break
                    
                except json.JSONDecodeError as e:
                    logger.warning(f"⚠️ Page {draw_num} JSON decode error: {e}, stopping")
                    break
                except Exception as e:
                    logger.warning(f"⚠️ Error fetching page {draw_num}: {e}, stopping")
                    break
            
            logger.info(f"✅ Collected {len(all_ptr_data_map)} unique PTR filings across all pages")
            
            # Build output payload - just metadata, no HTML downloading
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
                
                # Build output payload (no s3_key - downloader will create it)
                ptr_info = {
                    'url': view_url,
                    'view_url': view_url,
                    'filingPageUrl': view_url,  # Filing page URL for Senate (view page)
                    'formType': 'senate_ptr',
                    'source': 'senate',
                    'filingDate': filing_date_str if filing_date_str else date_str,
                    'filer_name': filer_name,
                    'uuid': uuid,
                    'transactions': []  # Empty - will be parsed by matcher from S3
                }
                
                ptrs.append(ptr_info)
                logger.info(f"✅ Added Senate PTR: {filer_name} - {filing_date_str}")
            
            logger.info(f"✅ Successfully extracted {len(ptrs)} Senate PTRs")
            return ptrs
            
        except Exception as e:
            logger.error(f"❌ Error fetching Senate PTRs: {e}")
            logger.error(f"Traceback: {tb.format_exc()}")
            return []
    
    def _get_house_antiforgery_token(self) -> str:
        """Get the antiforgery token from the House search view endpoint"""
        # First, visit the main page to establish session
        main_page_url = f"{self.HOUSE_BASE_URL}/FinancialDisclosure"
        self.session.get(main_page_url, timeout=30)
        
        # Then fetch the search view which contains the form with the token
        search_view_url = f"{self.HOUSE_BASE_URL}/FinancialDisclosure/ViewSearch"
        
        view_headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "accept-language": "en-US,en;q=0.9",
            "referer": f"{self.HOUSE_BASE_URL}/FinancialDisclosure",
        }
        
        try:
            response = self.session.get(search_view_url, headers=view_headers, timeout=30)
            response.raise_for_status()
            
            html_content = response.text
            
            # Try multiple patterns to find the antiforgery token
            patterns = [
                r'<input[^>]*name=["\']__RequestVerificationToken["\'][^>]*value=["\']([^"\']+)["\']',
                r'<input[^>]*value=["\']([^"\']+)["\'][^>]*name=["\']__RequestVerificationToken["\']',
                r'__RequestVerificationToken["\']?\s*[:=]\s*["\']([^"\']+)["\']',
                r'name=["\']__RequestVerificationToken["\'][^>]*value=["\']([^"\']+)["\']',
                r'__RequestVerificationToken[^>]*value=["\']([A-Za-z0-9_-]+)',
                r'<input[^>]*type=["\']hidden["\'][^>]*name=["\']__RequestVerificationToken["\'][^>]*value=["\']([^"\']+)["\']',
            ]
            
            for pattern in patterns:
                token_match = re.search(pattern, html_content, re.IGNORECASE | re.DOTALL)
                if token_match:
                    return token_match.group(1)
            
            # If no pattern worked, try to find any input with RequestVerificationToken
            all_inputs = re.findall(r'<input[^>]*>', html_content, re.IGNORECASE)
            for input_tag in all_inputs:
                if '__RequestVerificationToken' in input_tag:
                    value_match = re.search(r'value=["\']([^"\']+)["\']', input_tag)
                    if value_match:
                        return value_match.group(1)
            
            logger.error("❌ Could not find antiforgery token in House search view")
            return None
            
        except Exception as e:
            logger.error(f"❌ Error fetching House search view: {e}")
            return None
    
    def _parse_house_name(self, name_text: str) -> Dict[str, str]:
        """
        Parse House representative name into components
        Examples:
        - "Aderholt, Hon.. Robert B." -> lname="Aderholt", fname="Robert", mname="B."
        - "Allen, Hon.. Richard W." -> lname="Allen", fname="Richard", mname="W."
        - "Pelosi, Nancy" -> lname="Pelosi", fname="Nancy", mname=""
        """
        # Remove "Hon.." prefix if present
        name_text = re.sub(r'^Hon\.\.?\s*', '', name_text, flags=re.IGNORECASE).strip()
        
        # Split by comma
        parts = [p.strip() for p in name_text.split(',')]
        
        if len(parts) >= 2:
            lname = parts[0]
            # First name and middle name are in parts[1]
            name_parts = parts[1].strip().split()
            if len(name_parts) >= 2:
                fname = name_parts[0]
                mname = ' '.join(name_parts[1:])
            elif len(name_parts) == 1:
                fname = name_parts[0]
                mname = ""
            else:
                fname = ""
                mname = ""
        elif len(parts) == 1:
            # No comma - try to split by space (last word is last name)
            name_parts = parts[0].strip().split()
            if len(name_parts) >= 2:
                lname = name_parts[-1]
                fname = name_parts[0]
                mname = ' '.join(name_parts[1:-1]) if len(name_parts) > 2 else ""
            else:
                lname = parts[0]
                fname = ""
                mname = ""
        else:
            lname = name_text
            fname = ""
            mname = ""
        
        return {
            'fname': fname,
            'mname': mname,
            'lname': lname
        }
    
    def fetch_house_ptrs(self, filing_year: str = None) -> List[Dict[str, Any]]:
        """
        Fetch House PTRs for a specific filing year from House Clerk website
        
        House PTRs are published at: https://disclosures-clerk.house.gov/FinancialDisclosure
        This site uses a search interface that requires antiforgery token
        
        Args:
            filing_year: Year in YYYY format (defaults to current year)
            
        Returns:
            List of PTR metadata dicts with url, view_url, filer_name, fname, mname, lname, uuid, etc.
        """
        if filing_year is None:
            filing_year = str(datetime.now().year)
        
        logger.info(f"🏛️ Fetching House PTRs for filing year: {filing_year}")
        
        ptrs = []
        
        try:
            # Get antiforgery token
            logger.info("🔍 Fetching antiforgery token...")
            token = self._get_house_antiforgery_token()
            
            if not token:
                logger.error("❌ Failed to get antiforgery token")
                return []
            
            logger.info(f"✅ Got antiforgery token: {token[:20]}...")
            
            # Prepare form data
            form_data = {
                "LastName": "",
                "FilingYear": filing_year,
                "State": "",
                "District": "",
                "__RequestVerificationToken": token
            }
            
            # Headers for search request
            headers = {
                "accept": "*/*",
                "accept-language": "en-US,en;q=0.9",
                "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
                "origin": self.HOUSE_BASE_URL,
                "referer": f"{self.HOUSE_BASE_URL}/FinancialDisclosure",
                "x-requested-with": "XMLHttpRequest",
            }
            
            logger.info(f"📤 Sending search request for year {filing_year}...")
            response = self.session.post(
                self.HOUSE_SEARCH_ENDPOINT,
                data=form_data,
                headers=headers,
                timeout=30
            )
            response.raise_for_status()
            
            html_content = response.text
            logger.info(f"✅ Search request successful (status: {response.status_code})")
            
            # Parse PTR links from HTML
            # Pattern: <a href="public_disc/ptr-pdfs/2025/20032062.pdf" target="_blank">Name</a>
            ptr_patterns = [
                r'<a\s+href="(public_disc/ptr-pdfs/[^"]+\.pdf)"[^>]*target="_blank"[^>]*>([^<]+)</a>',
                r'<a\s+href="(public_disc/ptr-pdfs/[^"]+\.pdf)"[^>]*>([^<]+)</a>',
                r'href="(public_disc/[^"]*ptr[^"]*\.pdf)"[^>]*>([^<]+)</a>',
            ]
            
            matches = []
            for pattern in ptr_patterns:
                matches = re.findall(pattern, html_content, re.IGNORECASE)
                if matches:
                    break
            
            if not matches:
                logger.warning("⚠️ No PTR links found in HTML response")
                return []
            
            logger.info(f"✅ Found {len(matches)} PTR links in results")
            
            # Process each match
            for relative_path, name_text in matches:
                try:
                    # Clean up name
                    name_text = name_text.strip()
                    
                    # Parse name components
                    name_parts = self._parse_house_name(name_text)
                    
                    # Build full URL
                    full_url = urljoin(self.HOUSE_BASE_URL + "/", relative_path)
                    
                    # Extract UUID from filename (without extension)
                    filename = relative_path.split('/')[-1]
                    uuid = filename.replace('.pdf', '')
                    
                    # Build filing page URL for House
                    # House filing page is the search results page with the specific filing
                    # We can construct it from the PDF URL or use a search result URL
                    # For now, use the PDF URL as the filing page (users can view/download from there)
                    # Alternatively, we could construct: f"{self.HOUSE_BASE_URL}/FinancialDisclosure/ViewMemberSearchResult?FilingYear={filing_year}"
                    # But the PDF URL is more direct and useful
                    filing_page_url = full_url  # PDF URL serves as the filing page for House
                    
                    # Build output payload (similar to Senate structure)
                    ptr_info = {
                        'url': full_url,
                        'filingPageUrl': filing_page_url,  # Filing page URL for House (PDF URL)
                        'formType': 'house_ptr',
                        'source': 'house',
                        'filer_name': name_text,
                        'fname': name_parts['fname'],
                        'mname': name_parts['mname'],
                        'lname': name_parts['lname'],
                        'uuid': uuid,
                        'relative_path': relative_path
                    }
                    
                    ptrs.append(ptr_info)
                    logger.info(f"✅ Added House PTR: {name_text} - {uuid}")
                    
                except Exception as e:
                    logger.warning(f"⚠️ Error processing House PTR match: {e}")
                    continue
            
            logger.info(f"✅ Successfully extracted {len(ptrs)} House PTRs")
            return ptrs
            
        except Exception as e:
            logger.error(f"❌ Error fetching House PTRs: {e}")
            logger.error(f"Traceback: {tb.format_exc()}")
            return []
    
    def __del__(self):
        """Cleanup session"""
        if hasattr(self, 'session'):
            self.session.close()
