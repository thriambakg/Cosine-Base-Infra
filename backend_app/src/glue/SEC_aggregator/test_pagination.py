"""
Test script for SEC browse-edgar paginated API
Tests the pagination functionality with start parameter
"""

import requests
import re
from typing import List, Dict, Any

SEC_BASE_URL = "https://www.sec.gov"
SEC_BROWSE_EDGAR_URL = f"{SEC_BASE_URL}/cgi-bin/browse-edgar"
SEC_USER_AGENT = "Cosine Financial Platform contact@cosine.financial"

def test_paginated_api(form_type: str = '4', start: int = 0, count: int = 100):
    """
    Test the paginated SEC browse-edgar API
    
    Args:
        form_type: Form type ('3', '4', or '5')
        start: Starting index for pagination
        count: Number of results per page
    
    Returns:
        Response text or None if failed
    """
    try:
        # Build URL with pagination parameters
        params = {
            'action': 'getcurrent',
            'datea': '',
            'dateb': '',
            'company': '',
            'type': form_type,
            'SIC': '',
            'State': '',
            'Country': '',
            'CIK': '',
            'owner': 'only',
            'accno': '',
            'start': start,
            'count': count
        }
        
        # Build URL string
        url = f"{SEC_BROWSE_EDGAR_URL}?action=getcurrent&datea=&dateb=&company=&type={form_type}&SIC=&State=&Country=&CIK=&owner=only&accno=&start={start}&count={count}"
        
        print(f"📡 Testing API call:")
        print(f"   URL: {url}")
        print(f"   Form Type: {form_type}")
        print(f"   Start: {start}, Count: {count}")
        
        # Create session with headers
        session = requests.Session()
        session.headers.update({
            'User-Agent': SEC_USER_AGENT,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'max-age=0',
            'Upgrade-Insecure-Requests': '1'
        })
        
        # Make request
        response = session.get(url, timeout=30)
        response.raise_for_status()
        
        print(f"✅ Success! Status: {response.status_code}")
        print(f"   Content Length: {len(response.text)} bytes")
        print(f"   Content Type: {response.headers.get('Content-Type', 'N/A')}")
        
        # Try to extract some basic info from HTML
        html_content = response.text
        
        # Count potential filing links
        filing_link_pattern = re.compile(
            r'/Archives/edgar/data/\d+/[^/]+/',
            re.IGNORECASE
        )
        filing_links = filing_link_pattern.findall(html_content)
        unique_filings = set(filing_links)
        
        print(f"   Found {len(unique_filings)} unique filing links in response")
        
        # Show first few links as examples
        if unique_filings:
            print(f"   Sample filing links:")
            for i, link in enumerate(list(unique_filings)[:5], 1):
                print(f"      {i}. ...{link}")
        
        return response.text
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Request failed: {e}")
        return None
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        import traceback
        print(f"   Traceback: {traceback.format_exc()}")
        return None


def test_direct_url_invocation(form_type: str = '4', start: int = 0, count: int = 100):
    """
    Fallback: Test direct URL invocation (not using requests library)
    This would be used if the API call doesn't work with requests
    """
    import urllib.request
    import urllib.parse
    
    try:
        # Build URL
        params = {
            'action': 'getcurrent',
            'datea': '',
            'dateb': '',
            'company': '',
            'type': form_type,
            'SIC': '',
            'State': '',
            'Country': '',
            'CIK': '',
            'owner': 'only',
            'accno': '',
            'start': start,
            'count': count
        }
        
        url = f"{SEC_BROWSE_EDGAR_URL}?{urllib.parse.urlencode(params)}"
        
        print(f"📡 Testing direct URL invocation:")
        print(f"   URL: {url}")
        
        # Create request with headers
        req = urllib.request.Request(url)
        req.add_header('User-Agent', SEC_USER_AGENT)
        req.add_header('Accept', 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8')
        
        # Make request
        with urllib.request.urlopen(req, timeout=30) as response:
            content = response.read().decode('utf-8')
            print(f"✅ Success! Status: {response.status}")
            print(f"   Content Length: {len(content)} bytes")
            return content
            
    except Exception as e:
        print(f"❌ Direct URL invocation failed: {e}")
        import traceback
        print(f"   Traceback: {traceback.format_exc()}")
        return None


def test_multiple_pages(form_type: str = '4', max_pages: int = 3):
    """Test fetching multiple pages to verify pagination works"""
    print(f"\n🧪 Testing pagination across {max_pages} pages...")
    
    all_filings = set()
    count = 100
    
    for page in range(max_pages):
        start = page * count
        print(f"\n📄 Page {page + 1} (start={start}):")
        
        # Try requests first
        html_content = test_paginated_api(form_type, start, count)
        
        if not html_content:
            print(f"   ⚠️ Requests library failed, trying direct URL...")
            html_content = test_direct_url_invocation(form_type, start, count)
        
        if html_content:
            # Extract filing links
            filing_link_pattern = re.compile(
                r'/Archives/edgar/data/(\d+)/([^/]+)/',
                re.IGNORECASE
            )
            filing_matches = filing_link_pattern.findall(html_content)
            
            page_filings = set()
            for cik, accession in filing_matches:
                filing_id = f"{cik}/{accession}"
                page_filings.add(filing_id)
            
            print(f"   ✅ Found {len(page_filings)} filings on this page")
            all_filings.update(page_filings)
        else:
            print(f"   ❌ Failed to fetch page {page + 1}")
            break
    
    print(f"\n📊 Summary:")
    print(f"   Total unique filings across {max_pages} pages: {len(all_filings)}")
    print(f"   Expected: {max_pages * count} filings (if all pages full)")
    
    return len(all_filings)


if __name__ == "__main__":
    print("=" * 70)
    print("SEC Browse-Edgar Pagination Test")
    print("=" * 70)
    
    # Test single page
    print("\n1️⃣ Testing single page (start=0, count=100):")
    test_paginated_api('4', 0, 100)
    
    # Test second page
    print("\n2️⃣ Testing second page (start=100, count=100):")
    test_paginated_api('4', 100, 100)
    
    # Test tenth page (as per user's example)
    print("\n3️⃣ Testing tenth page (start=1000, count=100):")
    test_paginated_api('4', 1000, 100)
    
    # Test multiple pages
    print("\n4️⃣ Testing multiple pages:")
    total_filings = test_multiple_pages('4', max_pages=3)
    
    print("\n" + "=" * 70)
    print("✅ Test complete!")
    print("=" * 70)

