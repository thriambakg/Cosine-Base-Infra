"""
Test script to check the amount of filings (awards/contracts) over the past 1 year
for the Department of Defense and the Department of War.

Usage:
    python test_agency_filings_count.py

Requirements:
    pip install requests

This script will:
1. Query the USAspending API for awards/contracts
2. Filter by Department of Defense and Department of War
3. Check counts over the past 2 months
4. Display detailed statistics
"""

import json
import requests
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional

# Configuration
USASPENDING_BASE_URL = "https://api.usaspending.gov"
USASPENDING_USER_AGENT = "Cosine Financial Platform Test Script (contact@cosine.financial)"

# Calculate date range (past 1 year)
end_date = datetime.now(timezone.utc).date()
start_date = end_date - timedelta(days=365)  # 1 year

print("=" * 80)
print("🧪 Testing Agency Filings Count (Past 1 Year)")
print("=" * 80)
print(f"📅 Date Range: {start_date} to {end_date} (1 year)")
print(f"🏛️  Agencies: Department of Defense, Department of War")
print(f"🌐 API: {USASPENDING_BASE_URL}")
print("=" * 80)

# Create session
session = requests.Session()
session.headers.update({
    'User-Agent': USASPENDING_USER_AGENT,
    'Accept': 'application/json',
    'Content-Type': 'application/json',
})

def query_award_count(agency_name: str, start_date: str, end_date: str) -> Dict[str, Any]:
    """Query USAspending API for award count by agency and date range using lightweight count endpoint"""
    print(f"\n📊 Querying awards for: {agency_name}...")
    
    # Use the lightweight count endpoint (faster than search endpoint)
    count_filters = {
        "agencies": [
            {
                "type": "awarding",
                "tier": "toptier",
                "name": agency_name
            },
            {
                "type": "funding",
                "tier": "toptier",
                "name": agency_name
            }
        ],
        "time_period": [
            {
                "start_date": start_date,
                "end_date": end_date
            }
        ]
    }
    
    request_body = {
        "filters": count_filters,
        "spending_level": "awards"  # Count awards, not transactions
    }
    
    print(f"📤 Request payload:")
    print(json.dumps(request_body, indent=2))
    
    try:
        response = session.post(
            f"{USASPENDING_BASE_URL}/api/v2/download/count/",
            json=request_body,
            timeout=60
        )
        response.raise_for_status()
        
        result = response.json()
        
        # Extract count from results
        calculated_count = result.get("calculated_count", 0)
        
        # Handle both int and string responses
        if isinstance(calculated_count, str):
            try:
                calculated_count = int(calculated_count)
            except (ValueError, TypeError):
                calculated_count = 0
        
        print(f"✅ Query successful")
        print(f"📊 Total awards: {calculated_count:,}")
        
        return {
            "agency_name": agency_name,
            "total_count": calculated_count,
            "date_range": {
                "start_date": start_date,
                "end_date": end_date
            }
        }
        
    except requests.exceptions.HTTPError as e:
        print(f"❌ HTTP Error: {e}")
        if e.response is not None:
            try:
                error_data = e.response.json()
                print(f"   Error details: {json.dumps(error_data, indent=2)}")
            except:
                print(f"   Error text: {e.response.text[:500]}")
        return {
            "agency_name": agency_name,
            "total_count": 0,
            "error": str(e),
            "date_range": {
                "start_date": start_date,
                "end_date": end_date
            }
        }
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return {
            "agency_name": agency_name,
            "total_count": 0,
            "error": str(e),
            "date_range": {
                "start_date": start_date,
                "end_date": end_date
            }
        }

def query_transaction_count(agency_name: str, start_date: str, end_date: str) -> Dict[str, Any]:
    """Query USAspending API for transaction count by agency and date range using lightweight count endpoint"""
    print(f"\n📊 Querying transactions for: {agency_name}...")
    
    # Use the lightweight count endpoint (faster than search endpoint)
    count_filters = {
        "agencies": [
            {
                "type": "awarding",
                "tier": "toptier",
                "name": agency_name
            },
            {
                "type": "funding",
                "tier": "toptier",
                "name": agency_name
            }
        ],
        "time_period": [
            {
                "start_date": start_date,
                "end_date": end_date
            }
        ]
    }
    
    request_body = {
        "filters": count_filters,
        "spending_level": "transactions"  # Count transactions, not awards
    }
    
    try:
        response = session.post(
            f"{USASPENDING_BASE_URL}/api/v2/download/count/",
            json=request_body,
            timeout=60
        )
        response.raise_for_status()
        
        result = response.json()
        calculated_count = result.get("calculated_count", 0)
        
        # Handle both int and string responses
        if isinstance(calculated_count, str):
            try:
                calculated_count = int(calculated_count)
            except (ValueError, TypeError):
                calculated_count = 0
        
        print(f"✅ Query successful")
        print(f"📊 Total transactions: {calculated_count:,}")
        
        return {
            "agency_name": agency_name,
            "total_count": calculated_count,
            "date_range": {
                "start_date": start_date,
                "end_date": end_date
            }
        }
        
    except Exception as e:
        print(f"❌ Error querying transactions: {e}")
        import traceback
        traceback.print_exc()
        return {
            "agency_name": agency_name,
            "total_count": 0,
            "error": str(e),
            "date_range": {
                "start_date": start_date,
                "end_date": end_date
            }
        }

def main():
    """Main test function"""
    agencies = [
        "Department of Defense",
        "Department of War"
    ]
    
    results = []
    
    for agency in agencies:
        print("\n" + "=" * 80)
        print(f"🏛️  Processing: {agency}")
        print("=" * 80)
        
        # Query award count
        award_result = query_award_count(
            agency,
            start_date.strftime('%Y-%m-%d'),
            end_date.strftime('%Y-%m-%d')
        )
        
        # Query transaction count
        transaction_result = query_transaction_count(
            agency,
            start_date.strftime('%Y-%m-%d'),
            end_date.strftime('%Y-%m-%d')
        )
        
        results.append({
            "agency": agency,
            "awards": award_result,
            "transactions": transaction_result
        })
    
    # Print summary
    print("\n" + "=" * 80)
    print("📊 SUMMARY REPORT")
    print("=" * 80)
    
    total_awards = 0
    total_transactions = 0
    
    for result in results:
        agency = result["agency"]
        award_count = result["awards"].get("total_count", 0)
        transaction_count = result["transactions"].get("total_count", 0)
        
        total_awards += award_count
        total_transactions += transaction_count
        
        print(f"\n🏛️  {agency}:")
        print(f"   📋 Awards: {award_count:,}")
        print(f"   📄 Transactions: {transaction_count:,}")
        
        if result["awards"].get("error"):
            print(f"   ⚠️  Award query error: {result['awards']['error']}")
        if result["transactions"].get("error"):
            print(f"   ⚠️  Transaction query error: {result['transactions']['error']}")
    
    print("\n" + "=" * 80)
    print("📈 TOTALS (Both Agencies Combined):")
    print(f"   📋 Total Awards: {total_awards:,}")
    print(f"   📄 Total Transactions: {total_transactions:,}")
    print("=" * 80)
    
    # Save results to JSON file
    output_file = "agency_filings_count_results.json"
    with open(output_file, 'w') as f:
        json.dump({
            "date_range": {
                "start_date": start_date.strftime('%Y-%m-%d'),
                "end_date": end_date.strftime('%Y-%m-%d'),
                "days": (end_date - start_date).days
            },
            "agencies": agencies,
            "results": results,
            "totals": {
                "total_awards": total_awards,
                "total_transactions": total_transactions
            }
        }, f, indent=2)
    
    print(f"\n💾 Results saved to: {output_file}")
    print("\n✅ Test Complete!")

if __name__ == "__main__":
    main()

