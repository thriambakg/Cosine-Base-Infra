"""
Test script to fetch lobbying disclosures from LDA Senate API.
Retrieves full details for first 3 filings and contributions, outputs indexed values, and downloads documents.
"""

import requests
import json
import time
from typing import Dict, Optional
from pathlib import Path

# ============================================================================
# Configuration
# ============================================================================

API_BASE_URL = "https://lda.senate.gov/api/v1"
API_KEY = "88f3f8febf11c8321d9c64d67b0b1367b740f435"

REQUEST_TIMEOUT = 30
RATE_LIMIT_DELAY = 0.5

# Date range for filtering (optional - set to None to process all records)
# Format: YYYY-MM-DD
START_DATE = "2024-12-01"  # e.g., 
END_DATE = "2024-12-31"   # e.g., "2024-12-31"

# Script directory for downloads
SCRIPT_DIR = Path(__file__).parent
DOWNLOADS_DIR = SCRIPT_DIR / "lda_downloads"
DOWNLOADS_DIR.mkdir(exist_ok=True)

# ============================================================================
# Helper Functions
# ============================================================================

def create_session():
    """Create a requests session with Authorization header (most secure method)"""
    session = requests.Session()
    session.headers.update({
        'Authorization': f'Token {API_KEY}',
        'Accept': 'application/json',
    })
    return session

def call_api(session: requests.Session, endpoint: str, params: Optional[Dict] = None) -> Dict:
    """Call LDA API endpoint"""
    url = f"{API_BASE_URL}{endpoint}"
    
    time.sleep(RATE_LIMIT_DELAY)  # Rate limiting
    response = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
    
    # Show error details if request failed
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

def download_file(session: requests.Session, url: str, filepath: Path) -> bool:
    """Download a file from URL to filepath"""
    try:
        time.sleep(RATE_LIMIT_DELAY)
        response = session.get(url, timeout=REQUEST_TIMEOUT, stream=True)
        response.raise_for_status()
        
        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        return True
    except Exception as e:
        print(f"   ❌ Failed to download {url}: {str(e)[:200]}")
        return False

def print_filing_indexed_fields(filing: Dict):
    """Print indexed fields for a filing (LD-1 or LD-2)"""
    print(f"\n{'='*80}")
    print(f"📄 Filing - Indexed Fields")
    print(f"{'='*80}")
    
    # Primary Key
    print(f"\n🔑 Primary Key:")
    print(f"   filing_uuid: {filing.get('filing_uuid', 'N/A')}")
    
    # Search Fields
    print(f"\n🔍 Search Fields:")
    
    # Registrant
    registrant = filing.get('registrant', {})
    if registrant:
        print(f"   registrant_id: {registrant.get('id', 'N/A')}")
        print(f"   registrant_name: {registrant.get('name', 'N/A')}")
        print(f"   registrant_house_registrant_id: {registrant.get('house_registrant_id', 'N/A')}")
    
    # Client
    client = filing.get('client', {})
    if client:
        print(f"   client_id: {client.get('id', 'N/A')}")
        print(f"   client_name: {client.get('name', 'N/A')}")
        print(f"   client_client_id: {client.get('client_id', 'N/A')}")
    
    # Lobbyists
    lobbying_activities = filing.get('lobbying_activities', [])
    if lobbying_activities:
        print(f"\n   Lobbyists:")
        for activity in lobbying_activities[:3]:  # Show first 3
            lobbyists = activity.get('lobbyists', [])
            for lobbyist_info in lobbyists[:2]:  # Show first 2 per activity
                lobbyist = lobbyist_info.get('lobbyist', {})
                if lobbyist:
                    name_parts = [
                        lobbyist.get('prefix_display', ''),
                        lobbyist.get('first_name', ''),
                        lobbyist.get('middle_name', ''),
                        lobbyist.get('last_name', ''),
                        lobbyist.get('suffix_display', '')
                    ]
                    full_name = ' '.join(filter(None, name_parts))
                    print(f"      - {full_name} (ID: {lobbyist.get('id', 'N/A')})")
    
    # Report type
    print(f"\n   report_type: {filing.get('filing_type', 'N/A')}")
    print(f"   report_type_display: {filing.get('filing_type_display', 'N/A')}")
    
    # Filing period and year
    print(f"\n   filing_period: {filing.get('filing_period', 'N/A')}")
    print(f"   filing_period_display: {filing.get('filing_period_display', 'N/A')}")
    print(f"   filing_year: {filing.get('filing_year', 'N/A')}")
    
    # Posted date
    print(f"\n   dt_posted: {filing.get('dt_posted', 'N/A')}")
    
    # Amount fields
    print(f"\n   income: {filing.get('income', 'N/A')}")
    print(f"   expenses: {filing.get('expenses', 'N/A')}")
    print(f"   expenses_method: {filing.get('expenses_method', 'N/A')}")
    print(f"   expenses_method_display: {filing.get('expenses_method_display', 'N/A')}")
    
    # Amount reported index field (for LD-1 and LD-2)
    # This is typically the income field, which represents the amount reported
    income = filing.get('income')
    if income is not None:
        try:
            amount_reported = float(income) if income else None
            print(f"   amount_reported: {amount_reported}")  # Index field for searching
        except (ValueError, TypeError):
            print(f"   amount_reported: None (could not parse income: {income})")
    else:
        print(f"   amount_reported: None")
    
    # Affiliated organizations
    affiliated_orgs = filing.get('affiliated_organizations', [])
    if affiliated_orgs:
        print(f"\n   Affiliated Organizations ({len(affiliated_orgs)}):")
        for org in affiliated_orgs[:3]:  # Show first 3
            print(f"      - {org.get('name', 'N/A')} (Country: {org.get('country', 'N/A')})")
    
    # Foreign entities
    foreign_entities = filing.get('foreign_entities', [])
    if foreign_entities:
        print(f"\n   Foreign Entities ({len(foreign_entities)}):")
        for entity in foreign_entities[:3]:  # Show first 3
            print(f"      - {entity.get('name', 'N/A')} (Country: {entity.get('country', 'N/A')}, PPB: {entity.get('primary_place_of_business', 'N/A')})")
            print(f"        Owner Percentage: {entity.get('ownership_percentage', 'N/A')}")
    
    # Document URL
    doc_url = filing.get('filing_document_url')
    if doc_url:
        print(f"\n   filing_document_url: {doc_url}")
        print(f"   filing_document_content_type: {filing.get('filing_document_content_type', 'N/A')}")
    
    print(f"\n{'='*80}\n")

def print_contribution_indexed_fields(contribution: Dict):
    """Print indexed fields for a contribution report (LD-203)"""
    print(f"\n{'='*80}")
    print(f"📄 Contribution Report (LD-203) - Indexed Fields")
    print(f"{'='*80}")
    
    # Primary Key
    print(f"\n🔑 Primary Key:")
    print(f"   filing_uuid: {contribution.get('filing_uuid', 'N/A')}")
    
    # Search Fields
    print(f"\n🔍 Search Fields:")
    
    # Filer (Registrant)
    registrant = contribution.get('registrant', {})
    if registrant:
        print(f"   registrant_id: {registrant.get('id', 'N/A')}")
        print(f"   registrant_name: {registrant.get('name', 'N/A')}")
        print(f"   registrant_house_registrant_id: {registrant.get('house_registrant_id', 'N/A')}")
    
    # Lobbyist
    lobbyist = contribution.get('lobbyist', {})
    if lobbyist:
        name_parts = [
            lobbyist.get('prefix_display', ''),
            lobbyist.get('first_name', ''),
            lobbyist.get('middle_name', ''),
            lobbyist.get('last_name', ''),
            lobbyist.get('suffix_display', '')
        ]
        full_name = ' '.join(filter(None, name_parts))
        print(f"   lobbyist_name: {full_name}")
        print(f"   lobbyist_id: {lobbyist.get('id', 'N/A')}")
    
    # Report type
    print(f"\n   report_type: {contribution.get('filing_type', 'N/A')}")
    print(f"   report_type_display: {contribution.get('filing_type_display', 'N/A')}")
    
    # Filing period and year
    print(f"\n   filing_period: {contribution.get('filing_period', 'N/A')}")
    print(f"   filing_period_display: {contribution.get('filing_period_display', 'N/A')}")
    print(f"   filing_year: {contribution.get('filing_year', 'N/A')}")
    
    # Posted date
    print(f"\n   dt_posted: {contribution.get('dt_posted', 'N/A')}")
    
    # Filer type
    print(f"\n   filer_type: {contribution.get('filer_type', 'N/A')}")
    print(f"   filer_type_display: {contribution.get('filer_type_display', 'N/A')}")
    
    # Contribution items
    contribution_items = contribution.get('contribution_items', [])
    if contribution_items:
        print(f"\n   Contribution Items ({len(contribution_items)}):")
        total_amount = 0
        for item in contribution_items[:5]:  # Show first 5
            amount = item.get('amount', '0')
            try:
                total_amount += float(amount) if amount else 0
            except (ValueError, TypeError):
                pass
            
            print(f"      - Type: {item.get('contribution_type_display', 'N/A')}")
            print(f"        Contributor: {item.get('contributor_name', 'N/A')}")
            print(f"        Payee: {item.get('payee_name', 'N/A')}")
            print(f"        Honoree: {item.get('honoree_name', 'N/A')}")
            print(f"        Amount: ${amount}")
            print(f"        Date: {item.get('date', 'N/A')}")
        
        if len(contribution_items) > 5:
            print(f"      ... and {len(contribution_items) - 5} more items")
        
        print(f"\n   Total Contributions: ${total_amount:.2f}")
    else:
        print(f"\n   no_contributions: {contribution.get('no_contributions', 'N/A')}")
    
    # Document URL
    doc_url = contribution.get('filing_document_url')
    if doc_url:
        print(f"\n   filing_document_url: {doc_url}")
        print(f"   filing_document_content_type: {contribution.get('filing_document_content_type', 'N/A')}")
    
    print(f"\n{'='*80}\n")

# ============================================================================
# Main Script
# ============================================================================

def main():
    """Main execution"""
    print("="*80)
    print("LDA Senate API - Retrieve and Index Filings & Contributions")
    print("="*80)
    
    if not API_KEY:
        print("\n❌ ERROR: Please set API_KEY at the top of this script")
        return
    
    print(f"\n📁 Downloads Directory: {DOWNLOADS_DIR}")
    
    # Display date range if set
    if START_DATE or END_DATE:
        print(f"\n📅 Date Range Filter:")
        if START_DATE:
            print(f"   Start Date: {START_DATE}")
        if END_DATE:
            print(f"   End Date: {END_DATE}")
    else:
        print(f"\n📅 No date range filter - processing all records")
    
    # Create session with secure authentication
    session = create_session()
    
    # Test amount_reported filter to identify which field it corresponds to
    print("\n" + "="*80)
    print("🔍 Testing Amount Reported Filter")
    print("="*80)
    
    # Test 1: Filter for filings with amount_reported >= 50000
    print("\n   Test 1: Filtering for filings with amount_reported >= 50000")
    try:
        params = {
            'filing_amount_reported_min': '50000',
            'page_size': 5  # Get just a few results
        }
        test_filings = call_api(session, '/filings/', params=params)
        test_results = test_filings.get('results', [])
        print(f"   ✅ Found {len(test_results)} filings with amount_reported >= 50000")
        
        if test_results:
            print(f"\n   📊 Analyzing which field matches the filter:")
            for i, filing in enumerate(test_results[:3], 1):
                income = filing.get('income')
                expenses = filing.get('expenses')
                print(f"\n   Filing {i}:")
                print(f"      filing_uuid: {filing.get('filing_uuid', 'N/A')}")
                print(f"      income: {income}")
                print(f"      expenses: {expenses}")
                
                # Try to parse and compare
                try:
                    income_val = float(income) if income else None
                    expenses_val = float(expenses) if expenses else None
                    
                    if income_val and income_val >= 50000:
                        print(f"      ✅ income ({income_val}) >= 50000 - MATCHES FILTER")
                    elif income_val:
                        print(f"      ❌ income ({income_val}) < 50000 - DOES NOT MATCH")
                    
                    if expenses_val and expenses_val >= 50000:
                        print(f"      ✅ expenses ({expenses_val}) >= 50000 - MATCHES FILTER")
                    elif expenses_val:
                        print(f"      ❌ expenses ({expenses_val}) < 50000 - DOES NOT MATCH")
                except (ValueError, TypeError):
                    print(f"      ⚠️  Could not parse income/expenses as numbers")
    except Exception as e:
        print(f"   ❌ Error testing filter: {str(e)[:200]}")
    
    # Test 2: Filter for filings with amount_reported between 10000 and 100000
    print("\n   Test 2: Filtering for filings with amount_reported between 10000 and 100000")
    try:
        params = {
            'filing_amount_reported_min': '10000',
            'filing_amount_reported_max': '100000',
            'page_size': 5
        }
        # Add date range if provided
        if START_DATE:
            params['filing_dt_posted_after'] = START_DATE
        if END_DATE:
            params['filing_dt_posted_before'] = END_DATE
        test_filings2 = call_api(session, '/filings/', params=params)
        test_results2 = test_filings2.get('results', [])
        print(f"   ✅ Found {len(test_results2)} filings with amount_reported between 10000-100000")
        
        if test_results2:
            print(f"\n   📊 Analyzing which field matches the range filter:")
            for i, filing in enumerate(test_results2[:3], 1):
                income = filing.get('income')
                expenses = filing.get('expenses')
                print(f"\n   Filing {i}:")
                print(f"      filing_uuid: {filing.get('filing_uuid', 'N/A')}")
                print(f"      income: {income}")
                print(f"      expenses: {expenses}")
                
                try:
                    income_val = float(income) if income else None
                    expenses_val = float(expenses) if expenses else None
                    
                    if income_val and 10000 <= income_val <= 100000:
                        print(f"      ✅ income ({income_val}) is in range 10000-100000 - MATCHES FILTER")
                    elif income_val:
                        print(f"      ❌ income ({income_val}) is NOT in range - DOES NOT MATCH")
                    
                    if expenses_val and 10000 <= expenses_val <= 100000:
                        print(f"      ✅ expenses ({expenses_val}) is in range 10000-100000 - MATCHES FILTER")
                    elif expenses_val:
                        print(f"      ❌ expenses ({expenses_val}) is NOT in range - DOES NOT MATCH")
                except (ValueError, TypeError):
                    print(f"      ⚠️  Could not parse income/expenses as numbers")
    except Exception as e:
        print(f"   ❌ Error testing range filter: {str(e)[:200]}")
    
    # Fetch filings with date range filtering (if provided)
    print("\n" + "="*80)
    print("📋 Fetching Filings List")
    print("="*80)
    
    filings_params = {'page_size': 100}
    if START_DATE:
        filings_params['filing_dt_posted_after'] = START_DATE
        print(f"   📅 Filtering filings posted on or after: {START_DATE}")
    if END_DATE:
        filings_params['filing_dt_posted_before'] = END_DATE
        print(f"   📅 Filtering filings posted on or before: {END_DATE}")
    if not START_DATE and not END_DATE:
        print("   📅 No date filter - fetching all filings")
    
    try:
        filings_list = call_api(session, '/filings/', params=filings_params)
        filings_results = filings_list.get('results', [])
        total_count = filings_list.get('count', 0)
        print(f"✅ Found {len(filings_results)} filings on page 1 (total: {total_count})")
    except Exception as e:
        print(f"❌ Error fetching filings list: {str(e)[:200]}")
        return
    
    # Retrieve and process first 3 filings
    print("\n" + "="*80)
    print("📄 Retrieving Full Details for First 3 Filings")
    print("="*80)
    
    for i, filing_summary in enumerate(filings_results[:3], 1):
        filing_uuid = filing_summary.get('filing_uuid')
        if not filing_uuid:
            continue
        
        print(f"\n--- Filing {i}/3: {filing_uuid} ---")
        try:
            # Retrieve full filing details
            filing = call_api(session, f'/filings/{filing_uuid}/')
            
            # Debug: Print all top-level keys to see what fields are available
            if i == 1:  # Only for first filing to avoid clutter
                print(f"\n   🔍 Debug - All fields in filing response:")
                for key in sorted(filing.keys()):
                    value = filing[key]
                    if isinstance(value, (dict, list)):
                        print(f"      {key}: {type(value).__name__} (length: {len(value) if isinstance(value, list) else 'N/A'})")
                    else:
                        print(f"      {key}: {value}")
                print()
            
            # Print indexed fields
            print_filing_indexed_fields(filing)
            
            # Download document
            doc_url = filing.get('filing_document_url')
            if doc_url:
                filing_type = filing.get('filing_type', 'unknown')
                content_type = filing.get('filing_document_content_type', 'pdf')
                
                # Determine file extension
                if 'pdf' in content_type.lower():
                    ext = 'pdf'
                elif 'html' in content_type.lower():
                    ext = 'html'
                else:
                    ext = 'pdf'  # Default
                
                filename = f"filing_{filing_type}_{filing_uuid}.{ext}"
                filepath = DOWNLOADS_DIR / filename
                
                print(f"   📥 Downloading document: {filename}")
                if download_file(session, doc_url, filepath):
                    print(f"   ✅ Downloaded: {filepath}")
                else:
                    print(f"   ❌ Failed to download document")
            else:
                print(f"   ⚠️  No document URL available")
                
        except Exception as e:
            print(f"   ❌ Error retrieving filing {filing_uuid}: {str(e)[:200]}")
    
    # Fetch contributions with date range filtering (if provided)
    print("\n" + "="*80)
    print("📋 Fetching Contributions List")
    print("="*80)
    
    contributions_params = {'page_size': 100}
    if START_DATE:
        contributions_params['filing_dt_posted_after'] = START_DATE
        print(f"   📅 Filtering contributions posted on or after: {START_DATE}")
    if END_DATE:
        contributions_params['filing_dt_posted_before'] = END_DATE
        print(f"   📅 Filtering contributions posted on or before: {END_DATE}")
    if not START_DATE and not END_DATE:
        print("   📅 No date filter - fetching all contributions")
    
    try:
        contributions_list = call_api(session, '/contributions/', params=contributions_params)
        contributions_results = contributions_list.get('results', [])
        total_count = contributions_list.get('count', 0)
        print(f"✅ Found {len(contributions_results)} contributions on page 1 (total: {total_count})")
    except Exception as e:
        print(f"❌ Error fetching contributions list: {str(e)[:200]}")
        return
    
    # Retrieve and process first 3 contributions
    print("\n" + "="*80)
    print("📄 Retrieving Full Details for First 3 Contributions")
    print("="*80)
    
    for i, contribution_summary in enumerate(contributions_results[:3], 1):
        filing_uuid = contribution_summary.get('filing_uuid')
        if not filing_uuid:
            continue
        
        print(f"\n--- Contribution {i}/3: {filing_uuid} ---")
        try:
            # Retrieve full contribution details
            contribution = call_api(session, f'/contributions/{filing_uuid}/')
            
            # Print indexed fields
            print_contribution_indexed_fields(contribution)
            
            # Download document
            doc_url = contribution.get('filing_document_url')
            if doc_url:
                filing_type = contribution.get('filing_type', 'unknown')
                content_type = contribution.get('filing_document_content_type', 'pdf')
                
                # Determine file extension
                if 'pdf' in content_type.lower():
                    ext = 'pdf'
                elif 'html' in content_type.lower():
                    ext = 'html'
                else:
                    ext = 'pdf'  # Default
                
                filename = f"contribution_{filing_type}_{filing_uuid}.{ext}"
                filepath = DOWNLOADS_DIR / filename
                
                print(f"   📥 Downloading document: {filename}")
                if download_file(session, doc_url, filepath):
                    print(f"   ✅ Downloaded: {filepath}")
                else:
                    print(f"   ❌ Failed to download document")
            else:
                print(f"   ⚠️  No document URL available")
                
        except Exception as e:
            print(f"   ❌ Error retrieving contribution {filing_uuid}: {str(e)[:200]}")
    
    print("\n" + "="*80)
    print("✅ Test complete!")
    print(f"📁 Documents downloaded to: {DOWNLOADS_DIR}")
    print("="*80)

if __name__ == "__main__":
    main()
