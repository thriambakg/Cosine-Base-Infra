"""
Test script to determine if LDA API name search parameters support partial matching
(autocomplete) or only exact full name matches.
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

REQUEST_TIMEOUT = 30
RATE_LIMIT_DELAY = 0.5

# ============================================================================
# Helper Functions
# ============================================================================

def create_session():
    """Create a requests session with Authorization header"""
    session = requests.Session()
    session.headers.update({
        'Authorization': f'Token {API_KEY}',
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

def test_name_search(session: requests.Session, endpoint: str, param_name: str, test_queries: list, description: str):
    """Test name search with various query patterns"""
    print(f"\n{'='*80}")
    print(f"🔍 Testing: {description}")
    print(f"{'='*80}")
    print(f"   Endpoint: {endpoint}")
    print(f"   Parameter: {param_name}")
    
    for query in test_queries:
        print(f"\n   Test Query: '{query}'")
        try:
            params = {param_name: query, 'page_size': 5}
            results = call_api(session, endpoint, params=params)
            
            items = results.get('results', [])
            count = results.get('count', 0)
            
            print(f"      Results: {len(items)} items (total: {count})")
            
            if items:
                print(f"      Sample matches:")
                for i, item in enumerate(items[:3], 1):
                    # Extract name based on endpoint type
                    if 'registrant' in endpoint:
                        name = item.get('name', 'N/A')
                        print(f"         {i}. {name} (ID: {item.get('id', 'N/A')})")
                    elif 'client' in endpoint:
                        name = item.get('name', 'N/A')
                        print(f"         {i}. {name} (ID: {item.get('id', 'N/A')})")
                    elif 'lobbyist' in endpoint:
                        # Construct full name
                        name_parts = [
                            item.get('prefix_display', ''),
                            item.get('first_name', ''),
                            item.get('middle_name', ''),
                            item.get('last_name', ''),
                            item.get('suffix_display', '')
                        ]
                        name = ' '.join(filter(None, name_parts))
                        print(f"         {i}. {name} (ID: {item.get('id', 'N/A')})")
                
                # Check if query appears in results (partial match indicator)
                if 'lobbyist' in endpoint:
                    # For lobbyists, check if query appears in any name field
                    query_lower = query.lower()
                    matches = False
                    for item in items:
                        full_name = ' '.join(filter(None, [
                            item.get('prefix_display', ''),
                            item.get('first_name', ''),
                            item.get('middle_name', ''),
                            item.get('last_name', ''),
                            item.get('suffix_display', '')
                        ])).lower()
                        if query_lower in full_name:
                            matches = True
                            break
                    if matches:
                        print(f"      ✅ Partial match detected - supports autocomplete!")
                    else:
                        print(f"      ⚠️  Query not found in full name - checking individual fields...")
                        # Check individual fields
                        if any(query_lower in str(item.get('last_name', '')).lower() or 
                               query_lower in str(item.get('first_name', '')).lower() for item in items):
                            print(f"      ✅ Partial match in name fields - supports autocomplete!")
                        else:
                            print(f"      ⚠️  May require exact match or searches across name components")
                else:
                    # For registrants and clients, check name field
                    if any(query.lower() in str(item.get('name', '')).lower() for item in items):
                        print(f"      ✅ Partial match detected - supports autocomplete!")
                    else:
                        print(f"      ⚠️  Results don't contain query string - may be exact match only")
            else:
                print(f"      ❌ No results found")
                
        except Exception as e:
            print(f"      ❌ Error: {str(e)[:150]}")

# ============================================================================
# Main Tests
# ============================================================================

def main():
    """Main test execution"""
    print("="*80)
    print("LDA API Name Search - Partial Match vs Exact Match Test")
    print("="*80)
    
    if not API_KEY:
        print("\n❌ ERROR: Please set API_KEY at the top of this script")
        return
    
    session = create_session()
    
    # Test Registrants
    test_name_search(
        session,
        '/registrants/',
        'registrant_name',
        [
            'ACME',           # Partial - should match "ACME CORP", "ACME ASSOCIATES", etc.
            'Microsoft',      # Partial - should match "Microsoft Corporation", etc.
            'A-K',            # Partial - from earlier exploration
            'VAN SCOYOC',     # Partial - should match "VAN SCOYOC ASSOCIATES"
            'CHURCHILL GROUP' # Exact - from earlier exploration
        ],
        'Registrants Name Search'
    )
    
    # Test Clients
    test_name_search(
        session,
        '/clients/',
        'client_name',
        [
            'Microsoft',      # Partial - should match "Microsoft Corporation", etc.
            'BLUECROSS',     # Partial - from earlier exploration
            'AMERICAN',      # Partial - should match many "AMERICAN..." clients
            'FMC CORP',       # Exact - from earlier exploration
            'GENERAL MOTORS'  # Exact - from earlier exploration
        ],
        'Clients Name Search'
    )
    
    # Test Lobbyists
    test_name_search(
        session,
        '/lobbyists/',
        'lobbyist_name',
        [
            'Smith',         # Partial - should match many "Smith" last names
            'VAN SCOYOC',    # Partial - should match "H STEWART VAN SCOYOC", etc.
            'PARKER',        # Partial - from earlier exploration
            'WAYNE PARKER',  # More specific - should match "WAYNE PARKER"
            'KRISTIN'        # Partial - should match "KRISTIN ELIZABETH RUBIN"
        ],
        'Lobbyists Name Search'
    )
    
    print("\n" + "="*80)
    print("📊 Analysis Summary")
    print("="*80)
    print("\n   If partial queries (like 'ACME', 'Microsoft', 'Smith') return multiple")
    print("   results with those strings in the names, the API supports partial matching.")
    print("   If only exact matches are returned, the API requires full name matches.")
    print("\n   Check the results above to determine the search behavior.")
    print("="*80)

if __name__ == "__main__":
    main()

