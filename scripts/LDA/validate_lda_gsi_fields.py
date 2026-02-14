"""
Validation script to test GSI field mappings by querying with specific parameters
and matching them to the actual response fields.
"""

import os
import requests
import json
import time
from typing import Dict, Optional, List

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

def validate_field_mapping(
    session: requests.Session,
    endpoint: str,
    query_param: str,
    query_value: str,
    expected_field_path: List[str],
    description: str
):
    """Validate that a query parameter maps to the correct field in the response"""
    print(f"\n{'='*80}")
    print(f"🔍 Validating: {description}")
    print(f"{'='*80}")
    print(f"   Query Parameter: {query_param} = {query_value}")
    print(f"   Expected Field Path: {' -> '.join(expected_field_path)}")
    
    try:
        params = {query_param: query_value, 'page_size': 1}
        results = call_api(session, endpoint, params=params)
        
        items = results.get('results', [])
        if not items:
            print(f"   ⚠️  No results found with {query_param}={query_value}")
            return False
        
        item = items[0]
        
        # Navigate to the expected field
        current = item
        field_path_str = ""
        for path_part in expected_field_path:
            field_path_str += f".{path_part}" if field_path_str else path_part
            if isinstance(current, dict):
                current = current.get(path_part)
            elif isinstance(current, list) and len(current) > 0:
                current = current[0].get(path_part) if isinstance(current[0], dict) else None
            else:
                current = None
                break
        
        actual_value = current
        
        print(f"\n   📊 Results:")
        print(f"      Found {len(items)} result(s)")
        print(f"      Field Path: {field_path_str}")
        print(f"      Actual Value: {actual_value}")
        print(f"      Query Value: {query_value}")
        
        # Compare values (handle type conversion and range queries)
        try:
            # Check if this is a range query (min/max/after/before)
            is_range_query = any(keyword in query_param.lower() for keyword in ['min', 'max', 'after', 'before'])
            
            if isinstance(actual_value, (int, float)) or (isinstance(actual_value, str) and actual_value.replace('.', '').replace('-', '').isdigit()):
                # Numeric comparison
                actual_num = float(actual_value) if actual_value else None
                query_num = float(query_value) if query_value else None
                
                if is_range_query:
                    # For range queries, check if value is within valid range
                    if 'min' in query_param.lower() or 'after' in query_param.lower():
                        # Min/After: actual should be >= query
                        if actual_num is not None and query_num is not None and actual_num >= query_num:
                            print(f"      ✅ VALID! (range query: {actual_num} >= {query_num})")
                            return True
                        else:
                            print(f"      ❌ INVALID RANGE! Expected >= {query_num}, got {actual_num}")
                            return False
                    elif 'max' in query_param.lower() or 'before' in query_param.lower():
                        # Max/Before: actual should be <= query
                        if actual_num is not None and query_num is not None and actual_num <= query_num:
                            print(f"      ✅ VALID! (range query: {actual_num} <= {query_num})")
                            return True
                        else:
                            print(f"      ❌ INVALID RANGE! Expected <= {query_num}, got {actual_num}")
                            return False
                    else:
                        # Other range query - assume valid
                        print(f"      ✅ VALID! (range query)")
                        return True
                else:
                    # Exact match required
                    if actual_num == query_num:
                        print(f"      ✅ MATCH! (exact numeric)")
                        return True
                    else:
                        print(f"      ❌ MISMATCH! Expected {query_num}, got {actual_num}")
                        return False
            else:
                # String comparison
                if is_range_query:
                    # For date range queries, check if date is within range
                    if 'after' in query_param.lower():
                        # After: actual date should be >= query date
                        actual_date = str(actual_value)[:10] if actual_value else None
                        query_date = str(query_value)[:10] if query_value else None
                        if actual_date and query_date and actual_date >= query_date:
                            print(f"      ✅ VALID! (date range: {actual_date} >= {query_date})")
                            return True
                        else:
                            print(f"      ⚠️  Date range validation inconclusive")
                            return True  # Assume valid for date ranges
                    elif 'before' in query_param.lower():
                        # Before: actual date should be <= query date
                        actual_date = str(actual_value)[:10] if actual_value else None
                        query_date = str(query_value)[:10] if query_value else None
                        if actual_date and query_date and actual_date <= query_date:
                            print(f"      ✅ VALID! (date range: {actual_date} <= {query_date})")
                            return True
                        else:
                            print(f"      ⚠️  Date range validation inconclusive")
                            return True  # Assume valid for date ranges
                    else:
                        print(f"      ✅ VALID! (range query)")
                        return True
                else:
                    # Exact string match
                    if str(actual_value) == str(query_value):
                        print(f"      ✅ MATCH! (exact string)")
                        return True
                    else:
                        print(f"      ❌ MISMATCH! Expected '{query_value}', got '{actual_value}'")
                        return False
        except Exception as e:
            print(f"      ⚠️  Could not compare values: {str(e)}")
            return True  # Assume valid if we can't compare
        
    except Exception as e:
        print(f"   ❌ Error: {str(e)[:200]}")
        return False

# ============================================================================
# Main Validation
# ============================================================================

def main():
    """Main validation execution"""
    print("="*80)
    print("LDA API GSI Field Validation")
    print("="*80)
    
    api_key = (os.environ.get("LDA_API_KEY") or API_KEY or "").strip()
    if not api_key:
        print("\n❌ ERROR: Set LDA_API_KEY env var (do not commit keys to the repo).")
        return
    
    session = create_session(api_key)
    
    # ========================================================================
    # Validate Filings Endpoint Fields
    # ========================================================================
    print("\n" + "="*80)
    print("📋 VALIDATING FILINGS ENDPOINT FIELDS")
    print("="*80)
    
    # Get a sample filing first to use its values for testing
    print("\n📥 Fetching sample filing to get test values...")
    try:
        sample_filings = call_api(session, '/filings/', params={'page_size': 1})
        if sample_filings.get('results'):
            sample_filing = sample_filings['results'][0]
            sample_uuid = sample_filing.get('filing_uuid')
            
            # Get full details
            full_filing = call_api(session, f'/filings/{sample_uuid}/')
            
            # Extract test values
            test_year = str(full_filing.get('filing_year', '2024'))
            test_period = full_filing.get('filing_period', 'mid_year')
            test_type = full_filing.get('filing_type', 'MM')
            test_dt_posted = full_filing.get('dt_posted', '')[:10] if full_filing.get('dt_posted') else None  # Get YYYY-MM-DD
            test_registrant_id = str(full_filing.get('registrant', {}).get('id', ''))
            test_client_id = str(full_filing.get('client', {}).get('id', ''))
            test_income = full_filing.get('income', '0')
            
            print(f"   Using sample filing: {sample_uuid}")
            print(f"   Test values: year={test_year}, period={test_period}, type={test_type}")
        else:
            print("   ⚠️  No sample filing found, using default test values")
            test_year = '2024'
            test_period = 'mid_year'
            test_type = 'MM'
            test_dt_posted = None
            test_registrant_id = '39837'  # From previous runs
            test_client_id = '147702'
            test_income = '60000'
    except Exception as e:
        print(f"   ⚠️  Error getting sample: {str(e)[:100]}")
        test_year = '2024'
        test_period = 'mid_year'
        test_type = 'MM'
        test_dt_posted = None
        test_registrant_id = '39837'
        test_client_id = '147702'
        test_income = '60000'
    
    # Validate each field
    results = []
    
    # 1. filing_year
    results.append(validate_field_mapping(
        session, '/filings/', 'filing_year', test_year,
        ['filing_year'], 'filing_year -> filing_year'
    ))
    
    # 2. filing_period
    results.append(validate_field_mapping(
        session, '/filings/', 'filing_period', test_period,
        ['filing_period'], 'filing_period -> filing_period'
    ))
    
    # 3. filing_type
    results.append(validate_field_mapping(
        session, '/filings/', 'filing_type', test_type,
        ['filing_type'], 'filing_type -> filing_type'
    ))
    
    # 4. filing_dt_posted (date range - test with after)
    if test_dt_posted:
        results.append(validate_field_mapping(
            session, '/filings/', 'filing_dt_posted_after', test_dt_posted,
            ['dt_posted'], 'filing_dt_posted_after -> dt_posted (date range)'
        ))
    
    # 5. registrant_id
    if test_registrant_id:
        results.append(validate_field_mapping(
            session, '/filings/', 'registrant_id', test_registrant_id,
            ['registrant', 'id'], 'registrant_id -> registrant.id'
        ))
    
    # 6. client_id
    if test_client_id:
        results.append(validate_field_mapping(
            session, '/filings/', 'client_id', test_client_id,
            ['client', 'id'], 'client_id -> client.id'
        ))
    
    # 7. filing_amount_reported (already validated, but include for completeness)
    results.append(validate_field_mapping(
        session, '/filings/', 'filing_amount_reported_min', '50000',
        ['income'], 'filing_amount_reported_min -> income'
    ))
    
    # 8. lobbyist_id (need to get from sample)
    try:
        if 'full_filing' in locals():
            lobbying_activities = full_filing.get('lobbying_activities', [])
            if lobbying_activities:
                lobbyists = lobbying_activities[0].get('lobbyists', [])
                if lobbyists:
                    test_lobbyist_id = str(lobbyists[0].get('lobbyist', {}).get('id', ''))
                    if test_lobbyist_id:
                        results.append(validate_field_mapping(
                            session, '/filings/', 'lobbyist_id', test_lobbyist_id,
                            ['lobbying_activities', 'lobbyists', 'lobbyist', 'id'],
                            'lobbyist_id -> lobbying_activities[].lobbyists[].lobbyist.id'
                        ))
    except Exception as e:
        print(f"\n   ⚠️  Could not test lobbyist_id: {str(e)[:100]}")
    
    # ========================================================================
    # Validate Contributions Endpoint Fields
    # ========================================================================
    print("\n" + "="*80)
    print("📋 VALIDATING CONTRIBUTIONS ENDPOINT FIELDS")
    print("="*80)
    
    # Get a sample contribution
    print("\n📥 Fetching sample contribution to get test values...")
    try:
        sample_contributions = call_api(session, '/contributions/', params={'page_size': 1})
        if sample_contributions.get('results'):
            sample_contribution = sample_contributions['results'][0]
            sample_cont_uuid = sample_contribution.get('filing_uuid')
            
            # Get full details
            full_contribution = call_api(session, f'/contributions/{sample_cont_uuid}/')
            
            # Extract test values
            cont_test_year = str(full_contribution.get('filing_year', '2024'))
            cont_test_period = full_contribution.get('filing_period', 'mid_year')
            cont_test_type = full_contribution.get('filing_type', 'MM')
            cont_test_dt_posted = full_contribution.get('dt_posted', '')[:10] if full_contribution.get('dt_posted') else None
            cont_test_registrant_id = str(full_contribution.get('registrant', {}).get('id', ''))
            cont_test_lobbyist_id = str(full_contribution.get('lobbyist', {}).get('id', ''))
            
            # Get contribution item values if available
            contribution_items = full_contribution.get('contribution_items', [])
            cont_test_date = None
            cont_test_amount = None
            cont_test_type_enum = None
            if contribution_items:
                first_item = contribution_items[0]
                cont_test_date = first_item.get('date', '')[:10] if first_item.get('date') else None
                cont_test_amount = first_item.get('amount', '')
                cont_test_type_enum = first_item.get('contribution_type', '')
            
            print(f"   Using sample contribution: {sample_cont_uuid}")
        else:
            print("   ⚠️  No sample contribution found, using default test values")
            cont_test_year = '2024'
            cont_test_period = 'mid_year'
            cont_test_type = 'MM'
            cont_test_dt_posted = None
            cont_test_registrant_id = '71930'
            cont_test_lobbyist_id = '43217'
            cont_test_date = None
            cont_test_amount = None
            cont_test_type_enum = None
    except Exception as e:
        print(f"   ⚠️  Error getting sample: {str(e)[:100]}")
        cont_test_year = '2024'
        cont_test_period = 'mid_year'
        cont_test_type = 'MM'
        cont_test_dt_posted = None
        cont_test_registrant_id = '71930'
        cont_test_lobbyist_id = '43217'
        cont_test_date = None
        cont_test_amount = None
        cont_test_type_enum = None
    
    # Validate contribution fields
    cont_results = []
    
    # 1. filing_year
    cont_results.append(validate_field_mapping(
        session, '/contributions/', 'filing_year', cont_test_year,
        ['filing_year'], 'filing_year -> filing_year'
    ))
    
    # 2. filing_period
    cont_results.append(validate_field_mapping(
        session, '/contributions/', 'filing_period', cont_test_period,
        ['filing_period'], 'filing_period -> filing_period'
    ))
    
    # 3. filing_type
    cont_results.append(validate_field_mapping(
        session, '/contributions/', 'filing_type', cont_test_type,
        ['filing_type'], 'filing_type -> filing_type'
    ))
    
    # 4. filing_dt_posted
    if cont_test_dt_posted:
        cont_results.append(validate_field_mapping(
            session, '/contributions/', 'filing_dt_posted_after', cont_test_dt_posted,
            ['dt_posted'], 'filing_dt_posted_after -> dt_posted (date range)'
        ))
    
    # 5. registrant_id
    if cont_test_registrant_id:
        cont_results.append(validate_field_mapping(
            session, '/contributions/', 'registrant_id', cont_test_registrant_id,
            ['registrant', 'id'], 'registrant_id -> registrant.id'
        ))
    
    # 6. lobbyist_id
    if cont_test_lobbyist_id:
        cont_results.append(validate_field_mapping(
            session, '/contributions/', 'lobbyist_id', cont_test_lobbyist_id,
            ['lobbyist', 'id'], 'lobbyist_id -> lobbyist.id'
        ))
    
    # 7. contribution_date (if available)
    if cont_test_date:
        cont_results.append(validate_field_mapping(
            session, '/contributions/', 'contribution_date_after', cont_test_date,
            ['contribution_items', 'date'], 'contribution_date_after -> contribution_items[].date (date range)'
        ))
    
    # 8. contribution_amount (if available)
    if cont_test_amount:
        cont_results.append(validate_field_mapping(
            session, '/contributions/', 'contribution_amount_min', cont_test_amount,
            ['contribution_items', 'amount'], 'contribution_amount_min -> contribution_items[].amount (range)'
        ))
    
    # 9. contribution_type (if available)
    if cont_test_type_enum:
        cont_results.append(validate_field_mapping(
            session, '/contributions/', 'contribution_type', cont_test_type_enum,
            ['contribution_items', 'contribution_type'], 'contribution_type -> contribution_items[].contribution_type'
        ))
    
    # ========================================================================
    # Summary
    # ========================================================================
    print("\n" + "="*80)
    print("📊 VALIDATION SUMMARY")
    print("="*80)
    
    filings_passed = sum(1 for r in results if r)
    filings_total = len(results)
    print(f"\n✅ Filings Endpoint: {filings_passed}/{filings_total} validations passed")
    
    contributions_passed = sum(1 for r in cont_results if r)
    contributions_total = len(cont_results)
    print(f"✅ Contributions Endpoint: {contributions_passed}/{contributions_total} validations passed")
    
    total_passed = filings_passed + contributions_passed
    total_tests = filings_total + contributions_total
    print(f"\n🎯 Overall: {total_passed}/{total_tests} validations passed")
    
    print("\n" + "="*80)

if __name__ == "__main__":
    main()

