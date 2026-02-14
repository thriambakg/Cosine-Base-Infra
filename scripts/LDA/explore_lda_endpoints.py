"""
Explore LDA API endpoints to identify which ones can be used for autocomplete values.
"""

import os
import requests
import json
import time
from typing import Dict, Optional

# ============================================================================
# Configuration
# ============================================================================

API_BASE_URL = "https://lda.senate.gov/api/v1"
# Set LDA_API_KEY env var (do not commit keys)
API_KEY = ""

REQUEST_TIMEOUT = 30
RATE_LIMIT_DELAY = 0.5

# ============================================================================
# Helper Functions
# ============================================================================

def create_session(api_key: str):
    """Create a requests session with Authorization header"""
    session = requests.Session()
    session.headers.update({
        'Authorization': f'Token {api_key}',
        'Accept': 'application/json',
    })
    return session

def call_api(session: requests.Session, endpoint: str, params: Optional[Dict] = None) -> Dict:
    """Call LDA API endpoint"""
    url = f"{API_BASE_URL}{endpoint}"
    time.sleep(RATE_LIMIT_DELAY)
    response = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.json()

def explore_endpoint(session: requests.Session, endpoint: str, description: str):
    """Explore an endpoint and show its structure"""
    print(f"\n{'='*80}")
    print(f"📋 {description}")
    print(f"{'='*80}")
    print(f"   Endpoint: {endpoint}")
    
    try:
        # Try without params first
        data = call_api(session, endpoint, params=None)
        
        # Check structure
        if isinstance(data, dict):
            if 'results' in data:
                # Paginated list
                results = data.get('results', [])
                count = data.get('count', 0)
                print(f"\n   ✅ Paginated list endpoint")
                print(f"      Total count: {count}")
                print(f"      Results on first page: {len(results)}")
                
                if results:
                    print(f"\n   📊 Sample result structure:")
                    sample = results[0]
                    if isinstance(sample, dict):
                        print(f"      Keys: {list(sample.keys())[:10]}...")  # Show first 10 keys
                        # Show a few key-value pairs
                        for key in list(sample.keys())[:5]:
                            value = sample.get(key)
                            if isinstance(value, (str, int, float, bool, type(None))):
                                print(f"      {key}: {value}")
                            elif isinstance(value, dict):
                                print(f"      {key}: {{dict with {len(value)} keys}}")
                            elif isinstance(value, list):
                                print(f"      {key}: [list with {len(value)} items]")
            elif 'count' in data or 'next' in data or 'previous' in data:
                # Paginated but no results key
                print(f"\n   ✅ Paginated endpoint (unusual structure)")
                print(f"      Keys: {list(data.keys())}")
            else:
                # Single object or other structure
                print(f"\n   ✅ Single object or constants endpoint")
                print(f"      Keys: {list(data.keys())[:20]}...")  # Show first 20 keys
                
                # If it's a constants endpoint, show values
                if 'results' not in data and len(data) < 50:
                    print(f"\n   📊 Content preview:")
                    for key, value in list(data.items())[:10]:
                        if isinstance(value, (str, int, float, bool, type(None))):
                            print(f"      {key}: {value}")
                        elif isinstance(value, list):
                            print(f"      {key}: [list with {len(value)} items]")
                            if value and len(value) < 5:
                                print(f"         Items: {value}")
                        elif isinstance(value, dict):
                            print(f"      {key}: {{dict}}")
        elif isinstance(data, list):
            print(f"\n   ✅ List endpoint")
            print(f"      Items: {len(data)}")
            if data:
                print(f"      Sample item: {data[0] if isinstance(data[0], (str, int, float)) else type(data[0])}")
        
        return data
        
    except Exception as e:
        print(f"   ❌ Error: {str(e)[:200]}")
        return None

# ============================================================================
# Main Exploration
# ============================================================================

def main():
    """Main exploration"""
    print("="*80)
    print("LDA API Endpoints Exploration")
    print("="*80)
    
    api_key = (os.environ.get("LDA_API_KEY") or API_KEY or "").strip()
    if not api_key:
        print("\n❌ ERROR: Set LDA_API_KEY env var (do not commit keys to the repo).")
        return
    
    session = create_session(api_key)
    
    # Get root endpoint to see all available endpoints
    print("\n" + "="*80)
    print("📋 Root Endpoint - Available Endpoints")
    print("="*80)
    try:
        root = call_api(session, '/', params=None)
        print("\n   Available endpoints:")
        for key, url in root.items():
            print(f"      {key}: {url}")
    except Exception as e:
        print(f"   ❌ Error: {str(e)[:200]}")
        return
    
    # Explore each endpoint
    endpoints_to_explore = [
        ('/registrants/', 'Registrants - For autocomplete (registrant names/IDs)'),
        ('/clients/', 'Clients - For autocomplete (client names/IDs)'),
        ('/lobbyists/', 'Lobbyists - For autocomplete (lobbyist names/IDs)'),
        ('/constants/filing/filingtypes/', 'Filing Types - Constants for dropdown'),
        ('/constants/filing/lobbyingactivityissues/', 'Lobbying Activity Issues - Constants for dropdown'),
    ]
    
    for endpoint, description in endpoints_to_explore:
        explore_endpoint(session, endpoint, description)
    
    # Check if there are more constants endpoints
    print(f"\n{'='*80}")
    print("🔍 Checking for additional constants endpoints...")
    print(f"{'='*80}")
    
    constants_endpoints = [
        '/constants/',
        '/constants/filing/',
        '/constants/contribution/',
    ]
    
    for endpoint in constants_endpoints:
        try:
            data = call_api(session, endpoint, params=None)
            if isinstance(data, dict):
                print(f"\n   {endpoint}:")
                for key in list(data.keys())[:10]:
                    print(f"      - {key}")
        except:
            pass
    
    print("\n" + "="*80)
    print("✅ Exploration complete!")
    print("="*80)

if __name__ == "__main__":
    main()

