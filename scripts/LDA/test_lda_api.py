"""
Test script to fetch lobbying disclosures from LDA Senate API.
Tests API calls with no query parameters to see if it returns all records by default.
"""

import requests
import json
import time
from typing import Dict, Optional

# ============================================================================
# Configuration
# ============================================================================

API_BASE_URL = "https://lda.senate.gov/api/v1"
API_KEY = "88f3f8febf11c8321d9c64d67b0b1367b740f435"
RATE_LIMIT_DELAY = 0.5  # 120 calls per minute = 0.5 seconds between calls
REQUEST_TIMEOUT = 30

# ============================================================================
# Helper Functions
# ============================================================================

def create_session(api_key: str) -> requests.Session:
    """Create authenticated requests session"""
    session = requests.Session()
    session.headers.update({
        'Authorization': f'Token {api_key}',
        'Content-Type': 'application/json',
    })
    return session

def call_api(session: requests.Session, endpoint: str, params: Optional[Dict] = None) -> Dict:
    """Call LDA API endpoint with error handling"""
    url = f"{API_BASE_URL}{endpoint}"
    time.sleep(RATE_LIMIT_DELAY)
    
    print(f"\n🔍 Calling: {url}")
    if params:
        print(f"   Query params: {json.dumps(params, indent=2)}")
    else:
        print(f"   Query params: None (no filters)")
    
    response = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
    
    if response.status_code >= 400:
        print(f"   ❌ Error Details:")
        print(f"      Status: {response.status_code}")
        print(f"      URL: {response.url}")
        try:
            error_data = response.json()
            print(f"      Error response: {json.dumps(error_data, indent=2)}")
        except:
            print(f"      Error text: {response.text[:500]}")
        response.raise_for_status()
    
    data = response.json()
    print(f"   ✅ Success! Status: {response.status_code}")
    return data

# ============================================================================
# Main Test
# ============================================================================

def main():
    """Test API calls with no query parameters"""
    print("=" * 80)
    print("🧪 Testing LDA API with NO query parameters")
    print("=" * 80)
    
    session = create_session(API_KEY)
    
    # Test 1: Filings endpoint with no params
    print("\n" + "=" * 80)
    print("📋 TEST 1: /filings/ endpoint with NO query parameters")
    print("=" * 80)
    
    try:
        response = call_api(session, '/filings/', params=None)
        count = response.get('count', 0)
        results = response.get('results', [])
        next_page = response.get('next')
        previous_page = response.get('previous')
        
        print(f"\n📊 Response Summary:")
        print(f"   Total count: {count:,}")
        print(f"   Results in this page: {len(results)}")
        print(f"   Has next page: {next_page is not None}")
        print(f"   Has previous page: {previous_page is not None}")
        
        if results:
            print(f"\n📄 First result sample:")
            first_result = results[0]
            print(f"   Filing UUID: {first_result.get('filing_uuid', 'N/A')}")
            print(f"   Filing Type: {first_result.get('filing_type', 'N/A')}")
            print(f"   Posted Date: {first_result.get('filing_dt_posted', 'N/A')}")
            print(f"   Registrant: {first_result.get('registrant', {}).get('name', 'N/A') if isinstance(first_result.get('registrant'), dict) else 'N/A'}")
    except Exception as e:
        print(f"   ❌ Error: {str(e)[:500]}")
    
    # Test 2: Contributions endpoint with no params
    print("\n" + "=" * 80)
    print("📋 TEST 2: /contributions/ endpoint with NO query parameters")
    print("=" * 80)
    
    try:
        response = call_api(session, '/contributions/', params=None)
        count = response.get('count', 0)
        results = response.get('results', [])
        next_page = response.get('next')
        previous_page = response.get('previous')
        
        print(f"\n📊 Response Summary:")
        print(f"   Total count: {count:,}")
        print(f"   Results in this page: {len(results)}")
        print(f"   Has next page: {next_page is not None}")
        print(f"   Has previous page: {previous_page is not None}")
        
        if results:
            print(f"\n📄 First result sample:")
            first_result = results[0]
            print(f"   Contribution UUID: {first_result.get('contribution_uuid', 'N/A')}")
            print(f"   Posted Date: {first_result.get('filing_dt_posted', 'N/A')}")
            print(f"   Filer Type: {first_result.get('filer_type', 'N/A')}")
            registrant = first_result.get('registrant', {})
            if isinstance(registrant, dict):
                print(f"   Registrant: {registrant.get('name', 'N/A')}")
    except Exception as e:
        print(f"   ❌ Error: {str(e)[:500]}")
    
    # Test 3: Filings endpoint with only page_size (no date filters)
    print("\n" + "=" * 80)
    print("📋 TEST 3: /filings/ endpoint with ONLY page_size (no date filters)")
    print("=" * 80)
    
    try:
        response = call_api(session, '/filings/', params={'page_size': 25})
        count = response.get('count', 0)
        results = response.get('results', [])
        next_page = response.get('next')
        
        print(f"\n📊 Response Summary:")
        print(f"   Total count: {count:,}")
        print(f"   Results in this page: {len(results)}")
        print(f"   Has next page: {next_page is not None}")
        
        if results:
            print(f"\n📄 Date range in results:")
            dates = [r.get('filing_dt_posted') for r in results if r.get('filing_dt_posted')]
            if dates:
                print(f"   Earliest date: {min(dates)}")
                print(f"   Latest date: {max(dates)}")
    except Exception as e:
        print(f"   ❌ Error: {str(e)[:500]}")
    
    # Test 4: Contributions endpoint with only page_size (no date filters)
    print("\n" + "=" * 80)
    print("📋 TEST 4: /contributions/ endpoint with ONLY page_size (no date filters)")
    print("=" * 80)
    
    try:
        response = call_api(session, '/contributions/', params={'page_size': 25})
        count = response.get('count', 0)
        results = response.get('results', [])
        next_page = response.get('next')
        
        print(f"\n📊 Response Summary:")
        print(f"   Total count: {count:,}")
        print(f"   Results in this page: {len(results)}")
        print(f"   Has next page: {next_page is not None}")
        
        if results:
            print(f"\n📄 Date range in results:")
            dates = [r.get('filing_dt_posted') for r in results if r.get('filing_dt_posted')]
            if dates:
                print(f"   Earliest date: {min(dates)}")
                print(f"   Latest date: {max(dates)}")
    except Exception as e:
        print(f"   ❌ Error: {str(e)[:500]}")
    
    print("\n" + "=" * 80)
    print("✅ Testing complete!")
    print("=" * 80)

if __name__ == "__main__":
    main()
