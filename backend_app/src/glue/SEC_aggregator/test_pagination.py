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
                        match = re.search(r'/Archives/edgar/data/(\d+)/([^/"]+)/', href, re.IGNORECASE)
                        if match:
                            cik, accession_raw = match.groups()
                            accession_clean = accession_raw.replace('-', '').replace('/', '').strip()
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
                    
                    forms.append({
                        'cik': cik,
                        'accession': accession_clean,
                        'filing_date': filing_date,
                        'accepted_date': accepted_date
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
                    accession_clean = accession_raw.replace('-', '').replace('/', '').strip()
                    
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
                    
                    forms.append({
                        'cik': cik,
                        'accession': accession_clean,
                        'filing_date': filing_date,
                        'accepted_date': accepted_date
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


if __name__ == "__main__":
    import sys
    
    # Test single page first
    print("TEST 1: Single page test (start=0)")
    result1 = test_api_call(start=0, count=100, form_type='4')
    
    if result1.get('success'):
        print("\n✅ API call successful!")
        
        # Test higher page
        print("\n\nTEST 2: Higher page test (start=1000)")
        result2 = test_api_call(start=1000, count=100, form_type='4')
        
        if result2.get('success'):
            print("\n✅ Pagination working!")
            
            # Test pagination range
            if len(sys.argv) > 1 and sys.argv[1] == '--full':
                print("\n\nTEST 3: Full pagination test (multiple pages)")
                test_pagination_range(start=0, end=1000, step=100, form_type='4')
        else:
            print("\n⚠️ Higher page test failed, but first page worked")
    else:
        print("\n❌ API call failed - check error above")
