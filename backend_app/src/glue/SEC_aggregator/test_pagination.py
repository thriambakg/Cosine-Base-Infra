"""
Test script for SEC browse-edgar API pagination
Tests the start parameter to verify pagination works correctly
"""

import requests
import re
from datetime import datetime

# Try BeautifulSoup, fallback to regex if not available
try:
    from bs4 import BeautifulSoup
    HAS_BS4 = True
except ImportError:
    HAS_BS4 = False
    print("⚠️ BeautifulSoup not available, will use regex parsing only")

SEC_BROWSE_EDGAR_URL = "https://www.sec.gov/cgi-bin/browse-edgar"
SEC_USER_AGENT = "Cosine Financial Platform contact@cosine.financial"

def test_api_call(start=0, count=100, form_type='4'):
    """
    Test the SEC browse-edgar API with pagination
    
    Args:
        start: Starting record number (0-based)
        count: Number of records per page
        form_type: Form type (3, 4, or 5)
    """
    print(f"\n{'='*80}")
    print(f"Testing SEC API: start={start}, count={count}, form_type={form_type}")
    print(f"{'='*80}")
    
    # Build URL exactly as in the Glue job
    url = f"{SEC_BROWSE_EDGAR_URL}?action=getcurrent&datea=&dateb=&company=&type={form_type}&SIC=&State=&Country=&CIK=&owner=only&accno=&start={start}&count={count}"
    
    print(f"URL: {url}")
    
    session = requests.Session()
    session.headers.update({
        'User-Agent': SEC_USER_AGENT,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-US,en;q=0.9',
        'Cache-Control': 'max-age=0',
        'Upgrade-Insecure-Requests': '1'
    })
    
    try:
        print(f"\n📥 Making request...")
        response = session.get(url, timeout=30)
        response.raise_for_status()
        
        print(f"✅ Response status: {response.status_code}")
        print(f"✅ Content-Type: {response.headers.get('Content-Type', 'unknown')}")
        print(f"✅ Content length: {len(response.text)} bytes")
        
        html_content = response.text
        
        # Try parsing with BeautifulSoup first (more robust)
        if HAS_BS4:
            print(f"\n🔍 Parsing HTML with BeautifulSoup...")
            try:
                soup = BeautifulSoup(html_content, 'html.parser')
                
                # Find all table rows
                rows = soup.find_all('tr')
                print(f"✅ Found {len(rows)} table rows")
                
                # Look for rows with archive links - these are the data rows
                # SEC table structure:
                # - Header row with column names
                # - Data rows that contain archive links and filing dates
                # - Filing Date is in column 5 (index 4) in YYYY-MM-DD format
                forms = []
                
                for row in rows:
                    # Check if this row has archive links (data row)
                    links = row.find_all('a', href=True)
                    has_archive_link = False
                    cik = None
                    accession_clean = None
                    
                    for link in links:
                        href = link.get('href', '')
                        # Debug: print first few links to see format
                        if len(forms) < 3:
                            print(f"      🔍 DEBUG: Found link href: {href}")
                        
                        match = re.search(r'/Archives/edgar/data/(\d+)/([^/"]+)/', href, re.IGNORECASE)
                        if match:
                            cik, accession_raw = match.groups()
                            # Debug: print extraction details
                            if len(forms) < 3:
                                print(f"      🔍 DEBUG: Extracted CIK={cik}, AccessionRaw={accession_raw}")
                            
                            # Keep the accession with dashes - don't remove them yet
                            # The accession from the link is already in the correct format (e.g., "0000740260-25-000279")
                            accession_clean = accession_raw.replace('/', '').strip()
                            
                            # If accession doesn't have dashes, it might be incomplete - try to find full accession
                            if '-' not in accession_clean and len(accession_clean) < 18:
                                # Look for full accession in the link text or nearby
                                link_text = link.get_text(strip=True)
                                # Try to find full accession number in the row
                                full_acc_match = re.search(r'(\d{10}-\d{2}-\d{6})', row.get_text())
                                if full_acc_match:
                                    accession_clean = full_acc_match.group(1).replace('-', '')
                                    if len(forms) < 3:
                                        print(f"      🔍 DEBUG: Found full accession in row: {full_acc_match.group(1)}")
                            
                            has_archive_link = True
                            break
                    
                    if not has_archive_link or not cik or not accession_clean:
                        continue
                    
                    # Extract dates from the data row
                    # Table structure: Form | Formats | Description | Accepted | Filing Date | File/Film No
                    cells = row.find_all('td')
                    filing_date = None
                    accepted_date = None
                    
                    # Column 4 (index 3) is the Accepted column
                    # Format: YYYY-MM-DD<br>HH:MM:SS (e.g., "2025-11-04<br>21:50:26")
                    if len(cells) >= 4:
                        accepted_cell = cells[3]
                        # Get raw HTML to preserve <br> structure
                        accepted_html = str(accepted_cell)
                        # Replace <br> tags with space to preserve structure
                        accepted_html = re.sub(r'<br[^>]*>', ' ', accepted_html, flags=re.IGNORECASE)
                        # Remove all other HTML tags
                        accepted_text = re.sub(r'<[^>]+>', '', accepted_html).strip()
                        # Clean up multiple spaces
                        accepted_text = re.sub(r'\s+', ' ', accepted_text)
                        
                        # Extract full timestamp: YYYY-MM-DD HH:MM:SS
                        timestamp_match = re.search(r'(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})', accepted_text)
                        if timestamp_match:
                            date_part = timestamp_match.group(1)
                            time_part = timestamp_match.group(2)
                            accepted_date = f"{date_part} {time_part}"  # Full timestamp: "2025-11-04 21:50:26"
                        else:
                            # Fallback: try without space separator (in case text was collapsed)
                            timestamp_match = re.search(r'(\d{4}-\d{2}-\d{2})(\d{2}:\d{2}:\d{2})', accepted_text)
                            if timestamp_match:
                                date_part = timestamp_match.group(1)
                                time_part = timestamp_match.group(2)
                                accepted_date = f"{date_part} {time_part}"
                            else:
                                # Fallback: just extract date if time not found
                                date_match = re.search(r'(\d{4}-\d{2}-\d{2})', accepted_text)
                                if date_match:
                                    accepted_date = date_match.group(1)
                    
                    # Column 5 (index 4) is the Filing Date column
                    # Format: YYYY-MM-DD (e.g., "2025-11-04")
                    if len(cells) >= 5:
                        filing_date_cell = cells[4]
                        cell_text = filing_date_cell.get_text(strip=True)
                        # Look for YYYY-MM-DD pattern (SEC standard format)
                        date_match = re.search(r'(\d{4}-\d{2}-\d{2})', cell_text)
                        if date_match:
                            filing_date = date_match.group(1)
                        else:
                            # Fallback: try MM/DD/YYYY pattern (shouldn't be needed but just in case)
                            date_match = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', cell_text)
                            if date_match:
                                filing_date = date_match.group(1)
                    
                    # Fallback: search entire row for YYYY-MM-DD
                    if not filing_date:
                        row_text = row.get_text()
                        date_match = re.search(r'(\d{4}-\d{2}-\d{2})', row_text)
                        if date_match:
                            filing_date = date_match.group(1)
                    
                    # Also extract the actual archive link URL for direct access
                    archive_url = None
                    for link in links:
                        href = link.get('href', '')
                        if '/Archives/edgar/data/' in href:
                            if href.startswith('/'):
                                archive_url = f"https://www.sec.gov{href}"
                            elif href.startswith('http'):
                                archive_url = href
                            else:
                                archive_url = f"https://www.sec.gov/{href}"
                            break
                    
                    forms.append({
                        'cik': cik,
                        'accession': accession_clean,
                        'filing_date': filing_date,
                        'accepted_date': accepted_date,
                        'archive_url': archive_url  # Store the actual link from browse-edgar
                    })
                
                print(f"\n📊 Extracted {len(forms)} forms from page")
                if forms:
                    print(f"\n📋 Sample forms (first 5):")
                    for i, form in enumerate(forms[:5], 1):
                        filing_date_str = form['filing_date'] if form['filing_date'] else "N/A"
                        accepted_date_str = form['accepted_date'] if form.get('accepted_date') else "N/A"
                        print(f"   {i}. CIK: {form['cik']}, Accession: {form['accession'][:20]}..., Filing Date: {filing_date_str}, Accepted: {accepted_date_str}")
                    
                    # Show date statistics
                    filing_dates_with_value = [f['filing_date'] for f in forms if f['filing_date']]
                    accepted_dates_with_value = [f.get('accepted_date') for f in forms if f.get('accepted_date')]
                    print(f"\n📅 Date extraction stats:")
                    print(f"   Filing dates: {len(filing_dates_with_value)}/{len(forms)} forms")
                    print(f"   Accepted dates: {len(accepted_dates_with_value)}/{len(forms)} forms")
                    if filing_dates_with_value:
                        unique_filing_dates = sorted(set(filing_dates_with_value))
                        print(f"   Unique filing dates: {unique_filing_dates[:10]}...")  # Show first 10
                    if accepted_dates_with_value:
                        unique_accepted_dates = sorted(set(accepted_dates_with_value))
                        print(f"   Unique accepted dates: {unique_accepted_dates[:10]}...")  # Show first 10
                
                return {
                    'success': True,
                    'forms_count': len(forms),
                    'forms': forms,
                    'html_length': len(html_content)
                }
            except Exception as parse_error:
                print(f"❌ BeautifulSoup parsing failed: {parse_error}")
                print(f"\n🔍 Falling back to regex parsing...")
        
        if not HAS_BS4:
            print(f"\n🔍 Using regex parsing (BeautifulSoup not available)...")
            
            # Fallback to regex parsing (as in Glue job)
            table_row_pattern = re.compile(
                r'<tr[^>]*>(.*?)</tr>',
                re.DOTALL | re.IGNORECASE
            )
            
            rows = table_row_pattern.findall(html_content)
            print(f"✅ Found {len(rows)} table rows (regex)")
            
            archive_link_pattern = re.compile(
                r'/Archives/edgar/data/(\d+)/([^/"]+)/',
                re.IGNORECASE
            )
            
            forms = []
            for row in rows:
                archive_matches = archive_link_pattern.findall(row)
                if archive_matches:
                    cik, accession_raw = archive_matches[0]
                    # Keep dashes initially - the accession from link is already formatted
                    accession_clean = accession_raw.replace('/', '').strip()
                    
                    # If accession doesn't have dashes and is incomplete, try to find full accession
                    if '-' not in accession_clean and len(accession_clean) < 18:
                        # Try to find full accession number in the row text
                        full_acc_match = re.search(r'(\d{10}-\d{2}-\d{6})', row)
                        if full_acc_match:
                            accession_clean = full_acc_match.group(1).replace('-', '')
                    
                    # Extract dates - SEC table structure
                    # Columns: Form | Formats | Description | Accepted | Filing Date | File/Film No
                    # Accepted is column 4 (index 3), Filing Date is column 5 (index 4)
                    td_pattern = re.compile(r'<td[^>]*>(.*?)</td>', re.DOTALL | re.IGNORECASE)
                    cells = td_pattern.findall(row)
                    
                    filing_date = None
                    accepted_date = None
                    
                    # Column 4 (index 3) is the Accepted column
                    # Format: YYYY-MM-DD<br>HH:MM:SS (e.g., "2025-11-04<br>21:50:26")
                    if len(cells) >= 4:
                        accepted_cell = cells[3]
                        # Clean HTML tags from cell (replacing <br> with space)
                        accepted_text = re.sub(r'<br[^>]*>', ' ', accepted_cell, flags=re.IGNORECASE)
                        accepted_text = re.sub(r'<[^>]+>', '', accepted_text).strip()
                        # Extract full timestamp: YYYY-MM-DD HH:MM:SS
                        timestamp_match = re.search(r'(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})', accepted_text)
                        if timestamp_match:
                            date_part = timestamp_match.group(1)
                            time_part = timestamp_match.group(2)
                            accepted_date = f"{date_part} {time_part}"  # Full timestamp: "2025-11-04 21:50:26"
                        else:
                            # Fallback: just extract date if time not found
                            date_match = re.search(r'(\d{4}-\d{2}-\d{2})', accepted_text)
                            if date_match:
                                accepted_date = date_match.group(1)
                    
                    # Column 5 (index 4) is the Filing Date column
                    if len(cells) >= 5:
                        filing_date_cell = cells[4]
                        # Clean HTML tags from cell
                        cell_text = re.sub(r'<[^>]+>', '', filing_date_cell).strip()
                        # Look for YYYY-MM-DD pattern (SEC standard format)
                        date_match = re.search(r'(\d{4}-\d{2}-\d{2})', cell_text)
                        if date_match:
                            filing_date = date_match.group(1)
                        else:
                            # Fallback: try MM/DD/YYYY pattern
                            date_match = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', cell_text)
                            if date_match:
                                filing_date = date_match.group(1)
                    
                    # Fallback: search entire row for YYYY-MM-DD
                    if not filing_date:
                        date_match = re.search(r'(\d{4}-\d{2}-\d{2})', row)
                        if date_match:
                            filing_date = date_match.group(1)
                        else:
                            # Fallback: MM/DD/YYYY
                            date_match = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', row)
                            filing_date = date_match.group(1) if date_match else None
                    
                    # Also extract the actual archive link URL for direct access
                    archive_url = None
                    href_match = re.search(r'href="([^"]*Archives/edgar/data/[^"]+)"', row, re.IGNORECASE)
                    if href_match:
                        href = href_match.group(1)
                        if href.startswith('/'):
                            archive_url = f"https://www.sec.gov{href}"
                        elif href.startswith('http'):
                            archive_url = href
                        else:
                            archive_url = f"https://www.sec.gov/{href}"
                    
                    forms.append({
                        'cik': cik,
                        'accession': accession_clean,
                        'filing_date': filing_date,
                        'accepted_date': accepted_date,
                        'archive_url': archive_url  # Store the actual link from browse-edgar
                    })
            
            print(f"\n📊 Extracted {len(forms)} forms from page (regex)")
            if forms:
                print(f"\n📋 Sample forms (first 5):")
                for i, form in enumerate(forms[:5], 1):
                    filing_date_str = form['filing_date'] if form['filing_date'] else "N/A"
                    accepted_date_str = form['accepted_date'] if form.get('accepted_date') else "N/A"
                    print(f"   {i}. CIK: {form['cik']}, Accession: {form['accession'][:20]}..., Filing Date: {filing_date_str}, Accepted: {accepted_date_str}")
                
                # Show date statistics
                filing_dates_with_value = [f['filing_date'] for f in forms if f['filing_date']]
                accepted_dates_with_value = [f.get('accepted_date') for f in forms if f.get('accepted_date')]
                print(f"\n📅 Date extraction stats:")
                print(f"   Filing dates: {len(filing_dates_with_value)}/{len(forms)} forms")
                print(f"   Accepted dates: {len(accepted_dates_with_value)}/{len(forms)} forms")
                if filing_dates_with_value:
                    unique_filing_dates = sorted(set(filing_dates_with_value))
                    print(f"   Unique filing dates: {unique_filing_dates[:10]}...")  # Show first 10
                if accepted_dates_with_value:
                    unique_accepted_dates = sorted(set(accepted_dates_with_value))
                    print(f"   Unique accepted dates: {unique_accepted_dates[:10]}...")  # Show first 10
            
            return {
                'success': True,
                'forms_count': len(forms),
                'forms': forms,
                'html_length': len(html_content)
            }
    
    except requests.exceptions.RequestException as e:
        print(f"❌ Request failed: {e}")
        return {
            'success': False,
            'error': str(e)
        }
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        return {
            'success': False,
            'error': str(e)
        }


def test_pagination_range(start=0, end=1000, step=100, form_type='4'):
    """
    Test multiple pages to verify pagination works
    """
    print(f"\n{'='*80}")
    print(f"Testing pagination: start={start}, end={end}, step={step}, form_type={form_type}")
    print(f"{'='*80}")
    
    results = []
    for page_start in range(start, end + 1, step):
        page_num = (page_start // step) + 1
        print(f"\n📄 Testing page {page_num} (start={page_start})...")
        
        result = test_api_call(start=page_start, count=step, form_type=form_type)
        results.append({
            'page': page_num,
            'start': page_start,
            'result': result
        })
        
        if not result.get('success'):
            print(f"❌ Page {page_num} failed, stopping pagination test")
            break
        
        if result.get('forms_count', 0) == 0:
            print(f"⚠️ Page {page_num} returned 0 forms, might be at end")
            # Continue to next page to verify
        
        # Small delay between requests
        import time
        time.sleep(0.2)
    
    # Summary
    print(f"\n{'='*80}")
    print(f"PAGINATION TEST SUMMARY")
    print(f"{'='*80}")
    total_forms = sum(r['result'].get('forms_count', 0) for r in results if r['result'].get('success'))
    print(f"Total pages tested: {len(results)}")
    print(f"Total forms found: {total_forms}")
    print(f"\nPer-page breakdown:")
    for r in results:
        if r['result'].get('success'):
            print(f"  Page {r['page']} (start={r['start']}): {r['result'].get('forms_count', 0)} forms")
        else:
            print(f"  Page {r['page']} (start={r['start']}): FAILED - {r['result'].get('error', 'unknown')}")


def download_sec_form_test(form_data: dict, output_dir: str = ".") -> bool:
    """
    Download SEC form matching the Glue job's download_sec_form logic
    
    Args:
        form_data: Form metadata with 'cik', 'accession', 'form_type', 'filing_date'
        output_dir: Directory to save the file
    
    Returns:
        True if successful, False otherwise
    """
    import os
    import re
    
    print(f"\n{'='*80}")
    print(f"📥 DOWNLOAD TEST: Starting download")
    print(f"{'='*80}")
    
    cik = form_data.get('cik', 'unknown')
    accession = form_data.get('accession', 'unknown')
    form_type = form_data.get('form_type', 'form4')
    filing_date = form_data.get('filing_date', 'unknown')
    
    print(f"   CIK: {cik}")
    print(f"   Accession: {accession}")
    print(f"   Form Type: {form_type}")
    print(f"   Filing Date: {filing_date}")
    
    if not all([cik, accession]) or cik == 'unknown' or accession == 'unknown':
        print(f"   ❌ Missing CIK/accession: CIK={cik}, Accession={accession}")
        return False
    
    # Format accession number (matching Lambda logic)
    # IMPORTANT: SEC URLs use accession WITHOUT dashes in the directory path
    # The dashes are only used in filenames, not in the URL path
    # Example: /Archives/edgar/data/1802974/000089914025001211/index.htm (no dashes in path)
    
    # Remove any dashes first to get clean number
    accession_clean = accession.replace('-', '').strip()
    
    # For URL path: use accession WITHOUT dashes
    accession_for_url = accession_clean
    
    # For filenames: use accession WITH dashes (if 18 digits)
    if len(accession_clean) == 18:
        accession_dashed = f"{accession_clean[:10]}-{accession_clean[10:12]}-{accession_clean[12:]}"
    elif len(accession_clean) > 0:
        accession_dashed = accession_clean
        print(f"   ⚠️ WARNING: Accession number length is {len(accession_clean)}, expected 18. Using as-is: {accession_dashed}")
    else:
        accession_dashed = accession
        print(f"   ⚠️ WARNING: Invalid accession number: {accession}")
    
    # Use accession WITHOUT dashes for URL path (matching Lambda)
    base_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_for_url}"
    print(f"   🔗 Base URL: {base_url}")
    
    # Create session with same headers as Glue job
    session = requests.Session()
    session.headers.update({
        'User-Agent': SEC_USER_AGENT,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-US,en;q=0.9',
        'Cache-Control': 'max-age=0',
        'Upgrade-Insecure-Requests': '1'
    })
    
    # Build list of URLs to try (matching Glue job exactly)
    urls_to_try = []
    
    # Priority 0: If we have the archive_url from browse-edgar, try accessing it directly first
    # This might be a direct link to the document or index page
    archive_url = form_data.get('archive_url')
    if archive_url:
        print(f"   🔗 Using archive URL from browse-edgar: {archive_url}")
        urls_to_try.append((archive_url, "archive_url_from_browse"))
        # Also try index.htm at that location
        if archive_url.endswith('/'):
            index_from_archive = f"{archive_url}index.htm"
        else:
            index_from_archive = f"{archive_url}/index.htm"
        urls_to_try.append((index_from_archive, "index_from_archive_url"))
    
    # Priority 1: Download index.htm to find actual document links
    index_url = f"{base_url}/index.htm"
    urls_to_try.append((index_url, "index.htm"))
    
    # Priority 2: Try known document file patterns (XML patterns)
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
    
    # Priority 3: Try .txt file
    txt_url = f"{base_url}/{accession_dashed}.txt"
    urls_to_try.append((txt_url, f"{accession_dashed}.txt"))
    
    print(f"\n   📋 Will try {len(urls_to_try)} URL patterns:")
    for idx, (url, name) in enumerate(urls_to_try[:5], 1):
        print(f"      {idx}. {name} -> {url}")
    if len(urls_to_try) > 5:
        print(f"      ... and {len(urls_to_try) - 5} more")
    
    file_content = None
    file_ext = None
    content_type = None
    failed_attempts = []
    
    for file_url, file_name in urls_to_try:
        try:
            print(f"\n   🔄 ATTEMPTING: {file_name}")
            print(f"      🔗 URL: {file_url}")
            
            response = session.get(file_url, timeout=30)
            
            print(f"      📥 RESPONSE: Status={response.status_code}, Size={len(response.content):,} bytes")
            
            if len(response.content) > 0:
                content_preview = response.content[:200].decode('utf-8', errors='ignore')
                print(f"      📄 Content Preview (first 200 chars): {content_preview}")
            
            if response.status_code == 200:
                file_content = response.content
                
                # Determine file extension and content type (matching Glue job logic)
                is_index_page = 'index' in file_name.lower() or file_url.endswith('index.htm') or file_url.endswith('index.html')
                
                if file_name.endswith('.htm') or file_name.endswith('.html') or is_index_page:
                    file_ext = 'html'
                    content_type = 'text/html'
                    
                    # Check if HTML contains document links we should follow
                    # This is the SEC index page that lists available document formats
                    if is_index_page:
                        try:
                            html_text = file_content.decode('utf-8', errors='ignore')
                            
                            print(f"      🔍 Parsing index page for document links...")
                            
                            # Parse the SEC index page table to find document links
                            # The table has rows with links like:
                            # <a href="/Archives/edgar/data/1641631/000149315225021146/xslF345X05/ownership.xml">ownership.html</a>
                            # <a href="/Archives/edgar/data/1641631/000149315225021146/ownership.xml">ownership.xml</a>
                            
                            doc_links = []
                            
                            # Strategy 1: Look for HTML links (highest priority - these are the rendered HTML forms)
                            # Pattern: links that contain "xslF345X05" or similar XSL transformation paths
                            # These typically return HTML even though the URL ends in .xml
                            # Example: <a href="/Archives/edgar/data/1802974/000089914025001211/xslF345X05/form4.xml">form4.html</a>
                            
                            # First, try to find all links in the document table
                            # Look for links where the href contains xsl and the link text contains html
                            table_link_pattern = r'<a\s+href="([^"]*)"[^>]*>([^<]+)</a>'
                            all_links = re.findall(table_link_pattern, html_text, re.IGNORECASE)
                            
                            print(f"      🔍 Found {len(all_links)} total links in page")
                            
                            for href, link_text in all_links:
                                href_lower = href.lower()
                                link_text_lower = link_text.lower()
                                
                                # Check if this is an XSL transformation link (returns HTML)
                                if 'xsl' in href_lower and '.xml' in href_lower:
                                    # This is likely an HTML rendering link
                                    doc_links.append(('html', href))
                                    print(f"      📋 Found HTML link (XSL): {href} (text: {link_text})")
                                elif 'html' in link_text_lower and '.xml' in href_lower:
                                    # Link text says HTML but href is XML - likely XSL transformation
                                    doc_links.append(('html', href))
                                    print(f"      📋 Found HTML link (text indicates HTML): {href} (text: {link_text})")
                                elif '.html' in href_lower:
                                    # Direct HTML link
                                    doc_links.append(('html', href))
                                    print(f"      📋 Found HTML link (direct): {href} (text: {link_text})")
                            
                            # Fallback: Use regex patterns if we didn't find links above
                            if not doc_links:
                                html_link_patterns = [
                                    r'href="([^"]*xslF345X05[^"]*\.xml[^"]*)"',  # Form 4 HTML (xslF345X05)
                                    r'href="([^"]*xslF345X03[^"]*\.xml[^"]*)"',  # Form 3 HTML (xslF345X03)
                                    r'href="([^"]*xslF345X04[^"]*\.xml[^"]*)"',  # Form 4 HTML (xslF345X04)
                                    r'href="([^"]*xsl[^"]*\.xml[^"]*)"',  # Any XSL transformation (returns HTML)
                                    r'href="([^"]*ownership\.html[^"]*)"',  # Direct HTML links
                                    r'href="([^"]*\.html[^"]*)"',  # Any HTML links
                                ]
                                
                                for pattern in html_link_patterns:
                                    matches = re.findall(pattern, html_text, re.IGNORECASE)
                                    for match in matches:
                                        # Extract the link text to see if it says "html"
                                        link_context = re.search(
                                            rf'href="{re.escape(match)}"[^>]*>([^<]+)</a>',
                                            html_text,
                                            re.IGNORECASE
                                        )
                                        if link_context:
                                            link_text = link_context.group(1).lower()
                                            if 'html' in link_text or 'xsl' in match.lower():
                                                doc_links.append(('html', match))
                                                print(f"      📋 Found HTML link (regex): {match} (text: {link_text})")
                            
                            # Strategy 2: Find XML links (fallback if no HTML found)
                            if not doc_links:
                                xml_pattern = r'href="([^"]*\.xml[^"]*)"'
                                xml_matches = re.findall(xml_pattern, html_text, re.IGNORECASE)
                                for match in xml_matches:
                                    # Skip XSL links (already added as HTML)
                                    if 'xsl' not in match.lower():
                                        doc_links.append(('xml', match))
                                        print(f"      📋 Found XML link: {match}")
                            
                            # Strategy 3: Look for .txt files (last resort)
                            if not doc_links:
                                txt_pattern = r'href="([^"]*\.txt[^"]*)"'
                                txt_matches = re.findall(txt_pattern, html_text, re.IGNORECASE)
                                for match in txt_matches:
                                    doc_links.append(('txt', match))
                                    print(f"      📋 Found TXT link: {match}")
                            
                            # Remove duplicates while preserving order
                            seen = set()
                            unique_doc_links = []
                            for link_type, link in doc_links:
                                if link not in seen:
                                    seen.add(link)
                                    unique_doc_links.append((link_type, link))
                            
                            # Sort: HTML links first, then XML, then TXT
                            def link_priority(item):
                                link_type, link = item
                                if link_type == 'html':
                                    return 0  # Highest priority
                                elif link_type == 'xml':
                                    return 1
                                else:
                                    return 2
                            
                            sorted_links = sorted(unique_doc_links, key=link_priority)
                            
                            print(f"      📋 Found {len(sorted_links)} document links, will try HTML first...")
                            
                            # Try each found link - IMPORTANT: We need to download the actual document, not save the index page
                            document_found = False
                            for link_type, doc_link in sorted_links[:10]:
                                if doc_link.startswith('/'):
                                    doc_link = f"https://www.sec.gov{doc_link}"
                                elif not doc_link.startswith('http'):
                                    doc_link = f"{base_url}/{doc_link}"
                                
                                if doc_link == file_url:
                                    continue
                                
                                print(f"      🔗 Trying {link_type.upper()} link: {doc_link}")
                                try:
                                    doc_response = session.get(doc_link, timeout=30)
                                    if doc_response.status_code == 200:
                                        doc_content = doc_response.content
                                        content_start = doc_content[:1000].lower()
                                        
                                        is_html = any(indicator in content_start for indicator in [
                                            b'<!doctype html',
                                            b'<html',
                                            b'<head>',
                                            b'<body>',
                                            b'<style',
                                            b'sec form 4',
                                            b'sec form 3',
                                            b'sec form 5',
                                            b'form 4',
                                            b'form 3',
                                            b'form 5',
                                        ])
                                        
                                        is_xml = (doc_content.startswith(b'<?xml') or 
                                                 b'<ownershipDocument' in doc_content or 
                                                 b'<document>' in doc_content or 
                                                 b'<edgarDocument' in doc_content)
                                        
                                        # Prefer HTML if both detected
                                        if is_html:
                                            # Replace file_content with the actual document
                                            file_content = doc_content
                                            file_ext = 'html'
                                            content_type = 'text/html'
                                            document_found = True
                                            print(f"      ✅ Found HTML document (will save this instead of index): {doc_link}")
                                            break
                                        elif is_xml and not is_html:
                                            file_content = doc_content
                                            file_ext = 'xml'
                                            content_type = 'application/xml'
                                            document_found = True
                                            print(f"      ✅ Found XML document: {doc_link}")
                                            break
                                        elif b'<sec-header' in content_start:
                                            file_content = doc_content
                                            file_ext = 'txt'
                                            content_type = 'text/plain'
                                            document_found = True
                                            print(f"      ✅ Found SGML header: {doc_link}")
                                            break
                                        else:
                                            file_content = doc_content
                                            file_ext = 'txt'
                                            content_type = 'text/plain'
                                            document_found = True
                                            print(f"      ✅ Found file (format unclear): {doc_link}")
                                            break
                                except Exception as doc_error:
                                    print(f"      ⚠️ Could not download document link {doc_link}: {doc_error}")
                                    continue
                            
                            if not document_found:
                                print(f"      ⚠️ Could not find any document links in index page, saving index page as fallback")
                        except Exception as html_parse_error:
                            print(f"      ⚠️ Could not parse HTML for document links: {html_parse_error}")
                            import traceback
                            print(f"      Traceback: {traceback.format_exc()}")
                
                elif file_name.endswith('.txt'):
                    content_lower = file_content.lower()
                    if b'<sec-header' in content_lower or b'<acceptance-datetime' in content_lower:
                        print(f"      ⚠️ Downloaded file is an SGML header, looking for actual document...")
                        doc_candidates = [
                            f"{accession_dashed}-primary-document.xml",
                            f"{accession_dashed}-primarydoc.xml",
                            "primary-document.xml",
                            "doc4.xml",
                            "doc1.xml",
                            "doc2.xml",
                            "doc3.xml",
                            f"{accession_dashed}.xml",
                        ]
                        found_doc = False
                        for doc_candidate in doc_candidates:
                            doc_url = f"{base_url}/{doc_candidate}"
                            try:
                                print(f"      🔍 Trying document candidate: {doc_url}")
                                doc_response = session.get(doc_url, timeout=30)
                                if doc_response.status_code == 200:
                                    doc_content = doc_response.content
                                    content_sample = doc_content[:100].lower()
                                    is_html = b'<html' in content_sample or b'<!doctype html' in content_sample
                                    is_xml = doc_content.startswith(b'<?xml') or b'<ownershipDocument' in doc_content or b'<document>' in doc_content
                                    
                                    if is_html:
                                        file_content = doc_content
                                        file_ext = 'html'
                                        content_type = 'text/html'
                                        print(f"      ✅ Found HTML document file: {doc_url}")
                                        found_doc = True
                                        break
                                    elif is_xml:
                                        file_content = doc_content
                                        file_ext = 'xml'
                                        content_type = 'application/xml'
                                        print(f"      ✅ Found XML document file: {doc_url}")
                                        found_doc = True
                                        break
                            except Exception as doc_error:
                                continue
                        if not found_doc:
                            file_ext = 'txt'
                            content_type = 'text/plain'
                    elif file_content.startswith(b'<?xml'):
                        file_ext = 'xml'
                        content_type = 'application/xml'
                    elif b'<ownershipDocument' in file_content or b'<document>' in file_content:
                        file_ext = 'xml'
                        content_type = 'application/xml'
                    elif b'<html' in content_lower or b'<!doctype html' in content_lower:
                        file_ext = 'txt'
                        content_type = 'text/html'
                    else:
                        file_ext = 'txt'
                        content_type = 'text/plain'
                elif file_name.endswith('.xml'):
                    content_sample = file_content[:100].lower()
                    if b'<html' in content_sample or b'<!doctype html' in content_sample:
                        file_ext = 'html'
                        content_type = 'text/html'
                    elif file_content.startswith(b'<?xml') or b'<ownershipDocument' in file_content or b'<document>' in file_content:
                        file_ext = 'xml'
                        content_type = 'application/xml'
                    else:
                        file_ext = 'txt'
                        content_type = 'text/plain'
                else:
                    if file_content.startswith(b'<?xml') or b'<ownershipDocument' in file_content or b'<document>' in file_content:
                        file_ext = 'xml'
                        content_type = 'application/xml'
                    elif b'<html' in file_content.lower():
                        file_ext = 'html'
                        content_type = 'text/html'
                    else:
                        file_ext = 'txt'
                        content_type = 'text/plain'
                
                if file_content:
                    print(f"      ✅ DOWNLOAD SUCCESS: URL={file_url}, Size={len(file_content):,} bytes, Ext={file_ext}")
                    break
            else:
                failed_attempts.append(f"{file_url} (HTTP {response.status_code})")
                print(f"      ⚠️ HTTP {response.status_code} for {file_url}, trying next option...")
        except requests.exceptions.Timeout as e:
            failed_attempts.append(f"{file_url} (Timeout)")
            print(f"      ⚠️ Timeout downloading {file_url}: {e}")
            continue
        except Exception as e:
            failed_attempts.append(f"{file_url} (Error: {str(e)})")
            print(f"      ⚠️ Could not download {file_url}: {e}")
            continue
    
    if not file_content:
        print(f"\n   ❌ DOWNLOAD FAILED: All {len(urls_to_try)} URLs failed")
        for attempt in failed_attempts:
            print(f"      - {attempt}")
        return False
    
    # Save file
    filename = f"{form_type}-{cik}-{filing_date}.{file_ext}"
    filepath = os.path.join(output_dir, filename)
    
    # Make output_dir absolute path for clarity
    abs_output_dir = os.path.abspath(output_dir)
    abs_filepath = os.path.abspath(filepath)
    
    try:
        # Ensure output directory exists
        os.makedirs(abs_output_dir, exist_ok=True)
        
        with open(abs_filepath, 'wb') as f:
            f.write(file_content)
        print(f"\n{'='*80}")
        print(f"✅ File saved successfully!")
        print(f"{'='*80}")
        print(f"   📁 Directory: {abs_output_dir}")
        print(f"   📄 Filename: {filename}")
        print(f"   📍 Full path: {abs_filepath}")
        print(f"   📊 Size: {len(file_content):,} bytes")
        print(f"   📋 Type: {content_type}")
        print(f"{'='*80}")
        return True
    except Exception as e:
        print(f"\n   ❌ Failed to save file: {e}")
        import traceback
        print(f"   Traceback: {traceback.format_exc()}")
        return False


if __name__ == "__main__":
    import sys
    
    # Test: Fetch second set of 100 forms (start=100)
    print("="*80)
    print("TEST: Fetching second set of 100 forms (start=100, count=100)")
    print("="*80)
    
    result = test_api_call(start=100, count=100, form_type='4')
    
    if result.get('success') and result.get('forms'):
        forms = result['forms']
        print(f"\n✅ Successfully fetched {len(forms)} forms")
        
        # Take first 10 forms
        first_10 = forms[:10]
        print(f"\n📋 First 10 forms from this page:")
        print("-"*80)
        for i, form in enumerate(first_10, 1):
            filing_date_str = form['filing_date'] if form['filing_date'] else "N/A"
            accepted_date_str = form['accepted_date'] if form.get('accepted_date') else "N/A"
            print(f"{i}. CIK: {form['cik']}, Accession: {form['accession'][:20]}..., "
                  f"Filing Date: {filing_date_str}, Accepted: {accepted_date_str}")
        
        # Download the first form
        if first_10:
            first_form = first_10[0]
            print(f"\n{'='*80}")
            print(f"DOWNLOADING FIRST FORM")
            print(f"{'='*80}")
            
            form_data = {
                'cik': first_form['cik'],
                'accession': first_form['accession'],
                'form_type': 'form4',  # Assuming Form 4
                'filing_date': first_form['filing_date'] or 'unknown',
                'archive_url': first_form.get('archive_url')  # Include the archive URL if available
            }
            
            if first_form.get('archive_url'):
                print(f"   📋 Archive URL from browse-edgar: {first_form['archive_url']}")
            
            success = download_sec_form_test(form_data, output_dir=".")
            
            if success:
                print(f"\n✅ Download test completed successfully!")
            else:
                print(f"\n❌ Download test failed!")
        else:
            print("\n⚠️ No forms to download")
    else:
        print(f"\n❌ Failed to fetch forms: {result.get('error', 'unknown error')}")
