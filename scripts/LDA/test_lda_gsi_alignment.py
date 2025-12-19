"""
Test script to validate GSI field alignment for LDA filings and contributions.
Fetches one filing and one contribution, then prints all fields that should be indexed.
"""

import requests
import json
from typing import Dict, Optional
from pathlib import Path

# Configuration
API_BASE_URL = "https://lda.senate.gov/api/v1"
API_KEY = input("Enter your LDA API key: ").strip()

def create_session(api_key: str) -> requests.Session:
    """Create a requests session with Authorization header"""
    session = requests.Session()
    session.headers.update({
        'Authorization': f'Token {api_key}',
        'Accept': 'application/json',
    })
    return session

def call_api(session: requests.Session, endpoint: str, params: Optional[Dict] = None) -> Dict:
    """Call LDA API endpoint with error handling"""
    url = f"{API_BASE_URL}{endpoint}"
    response = session.get(url, params=params, timeout=30)
    
    if response.status_code >= 400:
        print(f"   🔍 Error Details:")
        print(f"      Status: {response.status_code}")
        print(f"      URL: {response.url}")
        try:
            error_data = response.json()
            print(f"      Error response: {json.dumps(error_data, indent=2)}")
        except:
            print(f"      Error text: {response.text[:500]}")
    
    response.raise_for_status()
    return response.json()

def print_gsi_fields_filing(filing: Dict):
    """Print GSI fields for a filing (LD-1 or LD-2)"""
    print(f"\n{'='*80}")
    print(f"📄 FILING - GSI Field Mapping")
    print(f"{'='*80}")
    
    print(f"\n🔑 Primary Key:")
    print(f"   PK: FILING#{filing.get('filing_uuid', 'N/A')}")
    print(f"   SK: FILING#{filing.get('filing_uuid', 'N/A')}")
    
    print(f"\n📊 GSI Fields:")
    
    # GSI1: Filing Year
    filing_year = filing.get('filing_year')
    dt_posted = filing.get('dt_posted', '')
    if filing_year:
        print(f"   ✅ GSI1PK: YEAR#{filing_year}")
        print(f"   ✅ GSI1SK: {dt_posted}")
    else:
        print(f"   ❌ GSI1PK: MISSING (filing_year)")
        print(f"   ❌ GSI1SK: MISSING (dt_posted)")
    
    # GSI2: Filing Period
    filing_period = filing.get('filing_period')
    if filing_period:
        print(f"   ✅ GSI2PK: PERIOD#{filing_period}")
        print(f"   ✅ GSI2SK: {dt_posted}")
    else:
        print(f"   ❌ GSI2PK: MISSING (filing_period)")
        print(f"   ❌ GSI2SK: MISSING (dt_posted)")
    
    # GSI3: Report Type
    report_type = filing.get('filing_type')
    if report_type:
        print(f"   ✅ GSI3PK: TYPE#{report_type}")
        print(f"   ✅ GSI3SK: {dt_posted}")
    else:
        print(f"   ❌ GSI3PK: MISSING (filing_type)")
        print(f"   ❌ GSI3SK: MISSING (dt_posted)")
    
    # GSI4: Registrant Name
    registrant = filing.get('registrant', {})
    registrant_name = registrant.get('name') if registrant else None
    if registrant_name:
        print(f"   ✅ GSI4PK: REGISTRANT#{registrant_name}")
        print(f"   ✅ GSI4SK: {dt_posted}")
    else:
        print(f"   ❌ GSI4PK: MISSING (registrant.name)")
        print(f"   ❌ GSI4SK: MISSING (dt_posted)")
    
    # GSI5: Client Name (FILINGS ONLY)
    client = filing.get('client', {})
    client_name = client.get('name') if client else None
    if client_name:
        print(f"   ✅ GSI5PK: CLIENT#{client_name}")
        print(f"   ✅ GSI5SK: {dt_posted}")
    else:
        print(f"   ⚠️  GSI5PK: OMIT (client.name not present - will be omitted, not NULL)")
        print(f"   ⚠️  GSI5SK: OMIT")
    
    # GSI6: Lobbyist Name
    lobbyist_name = None
    lobbying_activities = filing.get('lobbying_activities', [])
    if lobbying_activities:
        for activity in lobbying_activities:
            lobbyists = activity.get('lobbyists', [])
            if lobbyists:
                lobbyist = lobbyists[0].get('lobbyist', {})
                if lobbyist:
                    name_parts = [
                        lobbyist.get('prefix_display', ''),
                        lobbyist.get('first_name', ''),
                        lobbyist.get('middle_name', ''),
                        lobbyist.get('last_name', ''),
                        lobbyist.get('suffix_display', '')
                    ]
                    lobbyist_name = ' '.join(filter(None, name_parts))
                    break
    
    if lobbyist_name:
        print(f"   ✅ GSI6PK: LOBBYIST#{lobbyist_name}")
        print(f"   ✅ GSI6SK: {dt_posted}")
    else:
        print(f"   ⚠️  GSI6PK: OMIT (lobbyist_name not found - will be omitted, not NULL)")
        print(f"   ⚠️  GSI6SK: OMIT")
    
    # GSI7: Amount Reported (FILINGS ONLY)
    income = filing.get('income')
    if income is not None:
        try:
            amount = float(income)
            amount_bucket = int(amount / 10000) * 10000
            print(f"   ✅ GSI7PK: AMOUNT#{amount_bucket}")
            print(f"   ✅ GSI7SK: {amount} (Decimal/Number type)")
        except (ValueError, TypeError):
            print(f"   ⚠️  GSI7PK: OMIT (income={income} - cannot parse)")
            print(f"   ⚠️  GSI7SK: OMIT")
    else:
        print(f"   ⚠️  GSI7PK: OMIT (income is None - will be omitted, not NULL)")
        print(f"   ⚠️  GSI7SK: OMIT")
    
    print(f"\n{'='*80}\n")

def print_gsi_fields_contribution(contribution: Dict):
    """Print GSI fields for a contribution (LD-203)"""
    print(f"\n{'='*80}")
    print(f"📄 CONTRIBUTION - GSI Field Mapping")
    print(f"{'='*80}")
    
    print(f"\n🔑 Primary Key:")
    print(f"   PK: CONTRIBUTION#{contribution.get('filing_uuid', 'N/A')}")
    print(f"   SK: CONTRIBUTION#{contribution.get('filing_uuid', 'N/A')}")
    
    print(f"\n📊 GSI Fields:")
    
    # GSI1: Filing Year
    filing_year = contribution.get('filing_year')
    dt_posted = contribution.get('dt_posted', '')
    if filing_year:
        print(f"   ✅ GSI1PK: YEAR#{filing_year}")
        print(f"   ✅ GSI1SK: {dt_posted}")
    else:
        print(f"   ❌ GSI1PK: MISSING (filing_year)")
        print(f"   ❌ GSI1SK: MISSING (dt_posted)")
    
    # GSI2: Filing Period
    filing_period = contribution.get('filing_period')
    if filing_period:
        print(f"   ✅ GSI2PK: PERIOD#{filing_period}")
        print(f"   ✅ GSI2SK: {dt_posted}")
    else:
        print(f"   ❌ GSI2PK: MISSING (filing_period)")
        print(f"   ❌ GSI2SK: MISSING (dt_posted)")
    
    # GSI3: Report Type
    report_type = contribution.get('filing_type')
    if report_type:
        print(f"   ✅ GSI3PK: TYPE#{report_type}")
        print(f"   ✅ GSI3SK: {dt_posted}")
    else:
        print(f"   ❌ GSI3PK: MISSING (filing_type)")
        print(f"   ❌ GSI3SK: MISSING (dt_posted)")
    
    # GSI4: Registrant Name
    registrant = contribution.get('registrant', {})
    registrant_name = registrant.get('name') if registrant else None
    if registrant_name:
        print(f"   ✅ GSI4PK: REGISTRANT#{registrant_name}")
        print(f"   ✅ GSI4SK: {dt_posted}")
    else:
        print(f"   ❌ GSI4PK: MISSING (registrant.name)")
        print(f"   ❌ GSI4SK: MISSING (dt_posted)")
    
    # GSI5: Client Name (CONTRIBUTIONS - check if exists)
    client = contribution.get('client', {})
    client_name = client.get('name') if client else None
    if client_name:
        print(f"   ✅ GSI5PK: CLIENT#{client_name}")
        print(f"   ✅ GSI5SK: {dt_posted}")
    else:
        print(f"   ⚠️  GSI5PK: OMIT (client.name not present - will be omitted, not NULL)")
        print(f"   ⚠️  GSI5SK: OMIT")
    
    # GSI6: Lobbyist Name
    lobbyist = contribution.get('lobbyist', {})
    lobbyist_name = None
    if lobbyist:
        name_parts = [
            lobbyist.get('prefix_display', ''),
            lobbyist.get('first_name', ''),
            lobbyist.get('middle_name', ''),
            lobbyist.get('last_name', ''),
            lobbyist.get('suffix_display', '')
        ]
        lobbyist_name = ' '.join(filter(None, name_parts))
    
    if lobbyist_name:
        print(f"   ✅ GSI6PK: LOBBYIST#{lobbyist_name}")
        print(f"   ✅ GSI6SK: {dt_posted}")
    else:
        print(f"   ⚠️  GSI6PK: OMIT (lobbyist_name not found - will be omitted, not NULL)")
        print(f"   ⚠️  GSI6SK: OMIT")
    
    # GSI7: Amount Reported (NOT APPLICABLE TO CONTRIBUTIONS)
    print(f"   ⚠️  GSI7PK: OMIT (not applicable to contributions - will be omitted, not NULL)")
    print(f"   ⚠️  GSI7SK: OMIT")
    
    print(f"\n{'='*80}\n")

def main():
    """Main execution"""
    print("="*80)
    print("🧪 LDA GSI Field Alignment Test")
    print("="*80)
    
    # Create session
    session = create_session(API_KEY)
    
    # Fetch one filing
    print("\n📥 Fetching one filing...")
    try:
        filings_response = call_api(session, '/filings/', params={'page_size': 1})
        if filings_response.get('results'):
            filing_uuid = filings_response['results'][0].get('filing_uuid')
            print(f"   ✅ Found filing: {filing_uuid}")
            
            # Get full details
            full_filing = call_api(session, f'/filings/{filing_uuid}/')
            print_gsi_fields_filing(full_filing)
        else:
            print("   ❌ No filings found")
    except Exception as e:
        print(f"   ❌ Error fetching filing: {str(e)[:200]}")
    
    # Fetch one contribution
    print("\n📥 Fetching one contribution...")
    try:
        contributions_response = call_api(session, '/contributions/', params={'page_size': 1})
        if contributions_response.get('results'):
            contribution_uuid = contributions_response['results'][0].get('filing_uuid')
            print(f"   ✅ Found contribution: {contribution_uuid}")
            
            # Get full details
            full_contribution = call_api(session, f'/contributions/{contribution_uuid}/')
            print_gsi_fields_contribution(full_contribution)
        else:
            print("   ❌ No contributions found")
    except Exception as e:
        print(f"   ❌ Error fetching contribution: {str(e)[:200]}")
    
    print("\n" + "="*80)
    print("✅ Test complete!")
    print("="*80)
    print("\n📝 Notes:")
    print("   - ✅ = Field will be set in DynamoDB")
    print("   - ⚠️  = Field will be OMITTED (not set to NULL)")
    print("   - ❌ = Field is MISSING (should be present but isn't)")
    print("   - DynamoDB does not allow NULL values for GSI keys")
    print("   - Missing GSI keys should be omitted entirely, not set to None")

if __name__ == "__main__":
    main()

