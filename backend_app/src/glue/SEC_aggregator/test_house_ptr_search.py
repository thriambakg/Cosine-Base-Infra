"""
Test script for House of Representatives PTR search and download

This script tests the House Clerk's Financial Disclosure search endpoint
and downloads the first PTR PDF from the results.
"""

import requests
import re
import os
from urllib.parse import urljoin
from datetime import datetime

# House Clerk base URL
BASE_URL = "https://disclosures-clerk.house.gov"
SEARCH_ENDPOINT = f"{BASE_URL}/FinancialDisclosure/ViewMemberSearchResult"

def get_antiforgery_token(session):
    """Get the antiforgery token from the search view endpoint"""
    # First, visit the main page to establish session
    main_page_url = f"{BASE_URL}/FinancialDisclosure"
    print(f"   📄 Visiting main page: {main_page_url}")
    session.get(main_page_url, timeout=30)
    
    # Then fetch the search view which contains the form with the token
    search_view_url = f"{BASE_URL}/FinancialDisclosure/ViewSearch"
    print(f"   📄 Fetching search view: {search_view_url}")
    
    # Headers for fetching the search view
    view_headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "accept-language": "en-US,en;q=0.9",
        "referer": f"{BASE_URL}/FinancialDisclosure",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
    }
    
    try:
        response = session.get(search_view_url, headers=view_headers, timeout=30)
        response.raise_for_status()
        
        html_content = response.text
        
        # Try multiple patterns to find the antiforgery token
        patterns = [
            # Pattern 1: Standard input field
            r'<input[^>]*name=["\']__RequestVerificationToken["\'][^>]*value=["\']([^"\']+)["\']',
            # Pattern 2: Input field with different quote style
            r'<input[^>]*value=["\']([^"\']+)["\'][^>]*name=["\']__RequestVerificationToken["\']',
            # Pattern 3: In script tag or data attribute
            r'__RequestVerificationToken["\']?\s*[:=]\s*["\']([^"\']+)["\']',
            # Pattern 4: In form data
            r'name=["\']__RequestVerificationToken["\'][^>]*value=["\']([^"\']+)["\']',
            # Pattern 5: More flexible - any input with the token name
            r'__RequestVerificationToken[^>]*value=["\']([A-Za-z0-9_-]+)',
            # Pattern 6: Look for hidden input fields
            r'<input[^>]*type=["\']hidden["\'][^>]*name=["\']__RequestVerificationToken["\'][^>]*value=["\']([^"\']+)["\']',
        ]
        
        for idx, pattern in enumerate(patterns, 1):
            token_match = re.search(pattern, html_content, re.IGNORECASE | re.DOTALL)
            if token_match:
                token = token_match.group(1)
                print(f"   ✅ Found token using pattern {idx}: {token[:30]}...")
                return token
        
        # If no pattern worked, try to find any input with RequestVerificationToken
        all_inputs = re.findall(r'<input[^>]*>', html_content, re.IGNORECASE)
        for input_tag in all_inputs:
            if '__RequestVerificationToken' in input_tag:
                print(f"   🔍 Found input tag with token: {input_tag[:200]}")
                # Try to extract value from this specific tag
                value_match = re.search(r'value=["\']([^"\']+)["\']', input_tag)
                if value_match:
                    token = value_match.group(1)
                    print(f"   ✅ Extracted token from input tag: {token[:30]}...")
                    return token
        
        # Debug: Save HTML to file for inspection
        debug_file = "house_search_view_debug.html"
        with open(debug_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        print(f"⚠️ Could not find antiforgery token in search view HTML")
        print(f"   Response length: {len(html_content)} chars")
        print(f"   Saved HTML to {debug_file} for inspection")
        print(f"   Searching for 'RequestVerificationToken' in HTML...")
        if '__RequestVerificationToken' in html_content:
            print(f"   ✅ Found 'RequestVerificationToken' string in HTML")
            # Find context around it
            idx = html_content.find('__RequestVerificationToken')
            context = html_content[max(0, idx-200):min(len(html_content), idx+200)]
            print(f"   Context: {context}")
        else:
            print(f"   ❌ 'RequestVerificationToken' not found in HTML")
            # Try to find any form tags
            forms = re.findall(r'<form[^>]*>(.*?)</form>', html_content, re.IGNORECASE | re.DOTALL)
            print(f"   Found {len(forms)} form(s) in HTML")
            if forms:
                print(f"   First form preview: {forms[0][:500]}")
        
        return None
        
    except Exception as e:
        print(f"❌ Error fetching search page: {e}")
        import traceback
        traceback.print_exc()
        return None

def search_house_ptrs(session, filing_year="2025", last_name="", state="", district=""):
    """Search for House PTRs using the search endpoint"""
    
    # Get antiforgery token first
    print("🔍 Fetching antiforgery token...")
    token = get_antiforgery_token(session)
    
    if not token:
        print("❌ Failed to get antiforgery token")
        return None
    
    print(f"✅ Got antiforgery token: {token[:20]}...")
    
    # Prepare form data
    form_data = {
        "LastName": last_name,
        "FilingYear": filing_year,
        "State": state,
        "District": district,
        "__RequestVerificationToken": token
    }
    
    # Headers matching the curl command
    headers = {
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9",
        "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
        "origin": BASE_URL,
        "referer": f"{BASE_URL}/FinancialDisclosure",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
        "x-requested-with": "XMLHttpRequest",
        "sec-ch-ua": '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin"
    }
    
    print(f"\n📤 Sending search request...")
    print(f"   Endpoint: {SEARCH_ENDPOINT}")
    print(f"   Filing Year: {filing_year}")
    print(f"   Last Name: {last_name or '(empty)'}")
    print(f"   State: {state or '(empty)'}")
    print(f"   District: {district or '(empty)'}")
    
    try:
        response = session.post(
            SEARCH_ENDPOINT,
            data=form_data,
            headers=headers,
            timeout=30
        )
        response.raise_for_status()
        
        print(f"✅ Search request successful (status: {response.status_code})")
        return response.text
        
    except Exception as e:
        print(f"❌ Error searching House PTRs: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"   Response status: {e.response.status_code}")
            print(f"   Response text: {e.response.text[:500]}")
        return None

def parse_search_results(html_content):
    """Parse the HTML response to extract PTR links"""
    
    print("\n🔍 Parsing search results...")
    
    # Pattern to match PTR links in the table
    # Format: <a href="public_disc/ptr-pdfs/2025/20032062.pdf" target="_blank">Name</a>
    ptr_pattern = r'<a\s+href="(public_disc/ptr-pdfs/[^"]+\.pdf)"[^>]*target="_blank"[^>]*>([^<]+)</a>'
    
    matches = re.findall(ptr_pattern, html_content, re.IGNORECASE)
    
    if not matches:
        # Try alternative pattern (without target="_blank")
        ptr_pattern_alt = r'<a\s+href="(public_disc/ptr-pdfs/[^"]+\.pdf)"[^>]*>([^<]+)</a>'
        matches = re.findall(ptr_pattern_alt, html_content, re.IGNORECASE)
    
    if not matches:
        # Try even more flexible pattern
        ptr_pattern_flex = r'href="(public_disc/[^"]*ptr[^"]*\.pdf)"[^>]*>([^<]+)</a>'
        matches = re.findall(ptr_pattern_flex, html_content, re.IGNORECASE)
    
    if matches:
        print(f"✅ Found {len(matches)} PTR links in results")
        return matches
    else:
        print("⚠️ No PTR links found in HTML response")
        print(f"   HTML preview (first 1000 chars):")
        print(f"   {html_content[:1000]}")
        return []

def download_house_ptr(session, relative_path, output_dir="downloads"):
    """Download a House PTR PDF from a relative path"""
    
    # Construct full URL
    full_url = urljoin(BASE_URL + "/", relative_path)
    
    print(f"\n📥 Downloading PTR from: {full_url}")
    
    try:
        response = session.get(full_url, timeout=30, stream=True)
        response.raise_for_status()
        
        # Extract filename from path
        filename = os.path.basename(relative_path)
        
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, filename)
        
        # Download file
        with open(output_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        file_size = os.path.getsize(output_path)
        print(f"✅ Downloaded: {output_path} ({file_size:,} bytes)")
        return output_path
        
    except Exception as e:
        print(f"❌ Error downloading PTR: {e}")
        return None

def main():
    """Main test function"""
    print("=" * 80)
    print("House of Representatives PTR Search Test")
    print("=" * 80)
    print()
    
    # Create a session to maintain cookies
    session = requests.Session()
    
    # Set User-Agent to match browser
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36'
    })
    
    # Optional: Set cookies from your curl command if needed
    # Uncomment and update if token extraction fails
    # session.cookies.set('_ga', 'GA1.1.2022243591.1759275480')
    # session.cookies.set('_ga_N512ZD1JJT', 'GS2.1.s1762826936$o1$g0$t1762826946$j50$l0$h0')
    # session.cookies.set('.AspNetCore.Antiforgery.irol_7OdSko', 'CfDJ8AxbK_aDyXFGjQqrw0t6FK1Q8rKCmkYcaYldL-eb0mhdHYGUMdqfX1_fShSO1PSXGA-QC3h27Ra1CJda_dNeMLq7FRPtLk53VZ1-fKoHue-2SgbEhzFIf3QTG4XSCbsbnAb_BDzxEhM5IRGW6NEgDfo')
    # session.cookies.set('_ga_3FFR6GSYFC', 'GS2.1.s1762826464$o7$g1$t1762827095$j57$l0$h0')
    
    # Search for PTRs (default: 2025, all members)
    filing_year = "2025"
    html_response = search_house_ptrs(session, filing_year=filing_year)
    
    if not html_response:
        print("❌ Failed to get search results")
        return
    
    # Parse results
    ptr_links = parse_search_results(html_response)
    
    if not ptr_links:
        print("❌ No PTR links found in search results")
        return
    
    # Display first few results
    print(f"\n📋 First {min(5, len(ptr_links))} results:")
    for idx, (relative_path, name) in enumerate(ptr_links[:5], 1):
        print(f"   {idx}. {name.strip()}")
        print(f"      Path: {relative_path}")
    
    # Download the first PTR
    print(f"\n" + "=" * 80)
    print("Downloading First PTR")
    print("=" * 80)
    
    first_relative_path, first_name = ptr_links[0]
    downloaded_file = download_house_ptr(session, first_relative_path)
    
    if downloaded_file:
        print(f"\n✅ Successfully downloaded first PTR:")
        print(f"   Name: {first_name.strip()}")
        print(f"   File: {downloaded_file}")
    else:
        print(f"\n❌ Failed to download first PTR")

if __name__ == "__main__":
    main()

