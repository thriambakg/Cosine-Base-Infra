"""
Test script to fetch legislative activity from Congress.gov API for the last 1 day.
Outputs all data to CSV files for analysis.

This script:
1. Gets current Congress number
2. Fetches all bills (HR, S, HJRES, SJRES, HCONRES, SCONRES, HRES, SRES) updated/introduced in last 1 day
3. Fetches all amendments (HAMDT, SAMDT) updated/submitted in last 1 day
4. For each item, fetches full details (sponsors, actions, committees, etc.)
5. Outputs flattened data to CSV files
"""

import requests
import csv
import json
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional
import time

# ============================================================================
# Configuration
# ============================================================================

API_BASE_URL = "https://api.congress.gov/v3"
API_KEY = "jVFi0sHwg2iolTUws0lSj5r0tHqfh98bcXnTiEAX"  # TODO: Paste your API key here

# Bill types to fetch
BILL_TYPES = ["HR", "S", "HJRES", "SJRES", "HCONRES", "SCONRES", "HRES", "SRES"]

# Amendment types to fetch
AMENDMENT_TYPES = ["HAMDT", "SAMDT"]

# Request settings
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
RETRY_DELAY = 2

# ============================================================================
# Helper Functions
# ============================================================================

def get_current_congress() -> int:
    """Get the current Congress number."""
    url = f"{API_BASE_URL}/congress"
    params = {"api_key": API_KEY, "format": "json"}
    
    try:
        response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        
        # Debug: print structure to understand it
        print(f"🔍 Congress API response structure: {type(data)}")
        if isinstance(data, dict):
            print(f"   Keys: {list(data.keys())}")
        
        # Handle different response structures
        # JSON format: {"congresses": {"item": [...]}} or {"congresses": [...]} or direct list
        congresses = None
        if isinstance(data, list):
            congresses = data
        elif "congresses" in data:
            congresses_data = data["congresses"]
            if isinstance(congresses_data, dict):
                # Try "item" key first
                if "item" in congresses_data:
                    congresses = congresses_data["item"]
                else:
                    # Might be direct array in a dict wrapper
                    congresses = [congresses_data] if not isinstance(congresses_data, list) else congresses_data
            elif isinstance(congresses_data, list):
                congresses = congresses_data
        
        if not congresses:
            # Fallback: calculate from current year
            current_year = datetime.now().year
            # Congress numbers increment every 2 years starting from 1789 (1st Congress)
            congress = ((current_year - 1789) // 2) + 1
            print(f"⚠️ Could not fetch Congress list, using calculated value: {congress}")
            return congress
        
        # Sort by number (descending) and get the first one
        sorted_congresses = sorted(congresses, key=lambda x: x.get("number", 0) if isinstance(x, dict) else (x if isinstance(x, (int, str)) else 0), reverse=True)
        if not sorted_congresses:
            raise ValueError("No congresses found in response")
        
        first_congress = sorted_congresses[0]
        if isinstance(first_congress, dict):
            current_congress = first_congress.get("number")
        elif isinstance(first_congress, (int, str)):
            current_congress = int(first_congress)
        else:
            raise ValueError(f"Unexpected congress format: {first_congress}")
        
        if current_congress is None:
            raise ValueError("Congress number is None")
        
        print(f"✅ Current Congress: {current_congress}")
        return int(current_congress)
    except Exception as e:
        print(f"⚠️ Error fetching current Congress: {e}. Using calculated value.")
        import traceback
        traceback.print_exc()
        # Fallback calculation
        current_year = datetime.now().year
        congress = ((current_year - 1789) // 2) + 1
        return congress


def make_api_request(url: str, params: Dict[str, Any], retries: int = MAX_RETRIES) -> Optional[Dict]:
    """Make API request with retry logic."""
    for attempt in range(retries):
        try:
            response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            if attempt < retries - 1:
                wait_time = RETRY_DELAY * (attempt + 1)
                print(f"⚠️ Request failed (attempt {attempt + 1}/{retries}): {e}. Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                print(f"❌ Request failed after {retries} attempts: {e}")
                return None
    return None


def flatten_dict(d: Dict, parent_key: str = '', sep: str = '_') -> Dict:
    """Flatten nested dictionary."""
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            # For lists, we'll handle them separately or join as string
            if len(v) == 0:
                items.append((new_key, None))
            elif isinstance(v[0], dict):
                # List of dicts - create indexed keys
                for i, item in enumerate(v):
                    if isinstance(item, dict):
                        items.extend(flatten_dict(item, f"{new_key}_{i}", sep=sep).items())
                    else:
                        items.append((f"{new_key}_{i}", item))
            else:
                # List of primitives - join as string
                items.append((new_key, '|'.join(str(x) for x in v)))
        else:
            items.append((new_key, v))
    return dict(items)


def fetch_bills(congress: int, bill_type: str, from_date: str, to_date: str) -> List[Dict]:
    """Fetch bills of a specific type updated/introduced in date range."""
    bills = []
    offset = 0
    limit = 250  # Max per API docs
    
    print(f"   📋 Fetching {bill_type} bills from Congress {congress}...")
    
    # Try with date filters first
    params_with_dates = {
        "api_key": API_KEY,
        "format": "json",
        "offset": offset,
        "limit": limit,
        "fromDateTime": from_date,
        "toDateTime": to_date
    }
    
    # If date filtering doesn't work, we'll fetch all and filter locally
    params_without_dates = {
        "api_key": API_KEY,
        "format": "json",
        "offset": offset,
        "limit": limit
    }
    
    use_date_filter = True
    start_datetime = datetime.fromisoformat(from_date.replace('Z', '+00:00'))
    end_datetime = datetime.fromisoformat(to_date.replace('Z', '+00:00'))
    
    while True:
        # Use lowercase bill type in URL (e.g., "hr" not "HR")
        url = f"{API_BASE_URL}/bill/{congress}/{bill_type.lower()}"
        params = params_with_dates if use_date_filter else params_without_dates
        params["offset"] = offset
        params["limit"] = limit
        
        data = make_api_request(url, params)
        if not data:
            # If date filtering fails, try without dates
            if use_date_filter:
                print(f"      ⚠️ Date filtering may not be supported, fetching all bills and filtering locally...")
                use_date_filter = False
                offset = 0
                continue
            break
        
        # Handle different JSON response structures
        # JSON format can be: {"bills": {"bill": [...]}} or {"bills": [...]} or just a list
        bill_list = []
        if isinstance(data, list):
            # Direct array response
            bill_list = data
        elif "bills" in data:
            bills_data = data["bills"]
            if isinstance(bills_data, dict):
                # {"bills": {"bill": [...]}}
                bill_list = bills_data.get("bill", [])
            elif isinstance(bills_data, list):
                # {"bills": [...]}
                bill_list = bills_data
        
        if not bill_list:
            break
        
        # If not using date filter, filter locally by updateDate or introducedDate
        if not use_date_filter:
            filtered_bills = []
            for bill in bill_list:
                # Check updateDateIncludingText or updateDate or introducedDate
                update_date_str = bill.get("updateDateIncludingText") or bill.get("updateDate") or bill.get("introducedDate")
                if update_date_str:
                    try:
                        # Parse date (could be YYYY-MM-DD or ISO format)
                        if 'T' in update_date_str:
                            bill_date = datetime.fromisoformat(update_date_str.replace('Z', '+00:00'))
                        else:
                            bill_date = datetime.fromisoformat(update_date_str + 'T00:00:00+00:00')
                        
                        if start_datetime <= bill_date <= end_datetime:
                            filtered_bills.append(bill)
                    except:
                        pass
            bill_list = filtered_bills
        
        bills.extend(bill_list)
        print(f"      ✅ Fetched {len(bill_list)} bills matching date range (total: {len(bills)})")
        
        # Check if there are more results
        # Pagination can be at different levels
        pagination = {}
        if isinstance(data, dict):
            if "bills" in data and isinstance(data["bills"], dict):
                pagination = data["bills"].get("pagination", {})
            elif "pagination" in data:
                pagination = data["pagination"]
        
        count = pagination.get("count", 0) if pagination else 0
        if count > 0 and offset + limit >= count:
            break
        
        offset += limit
        time.sleep(0.5)  # Rate limiting
    
    return bills


def fetch_bill_details(congress: int, bill_type: str, bill_number: int) -> Optional[Dict]:
    """Fetch full details for a specific bill."""
    url = f"{API_BASE_URL}/bill/{congress}/{bill_type.lower()}/{bill_number}"
    params = {
        "api_key": API_KEY,
        "format": "json"
    }
    
    data = make_api_request(url, params)
    if data:
        # Handle different response structures
        if isinstance(data, dict):
            return data.get("bill", data)  # If no "bill" key, return the whole dict
        return data
    return None


def fetch_amendments(congress: int, amendment_type: str, from_date: str, to_date: str) -> List[Dict]:
    """Fetch amendments of a specific type updated/submitted in date range."""
    amendments = []
    offset = 0
    limit = 250
    
    print(f"   📝 Fetching {amendment_type} amendments from Congress {congress}...")
    
    # Try with date filters first
    params_with_dates = {
        "api_key": API_KEY,
        "format": "json",
        "offset": offset,
        "limit": limit,
        "fromDateTime": from_date,
        "toDateTime": to_date
    }
    
    params_without_dates = {
        "api_key": API_KEY,
        "format": "json",
        "offset": offset,
        "limit": limit
    }
    
    use_date_filter = True
    start_datetime = datetime.fromisoformat(from_date.replace('Z', '+00:00'))
    end_datetime = datetime.fromisoformat(to_date.replace('Z', '+00:00'))
    
    while True:
        # Use lowercase amendment type in URL (e.g., "hamdt" not "HAMDT")
        url = f"{API_BASE_URL}/amendment/{congress}/{amendment_type.lower()}"
        params = params_with_dates if use_date_filter else params_without_dates
        params["offset"] = offset
        params["limit"] = limit
        
        data = make_api_request(url, params)
        if not data:
            # If date filtering fails, try without dates
            if use_date_filter:
                print(f"      ⚠️ Date filtering may not be supported, fetching all amendments and filtering locally...")
                use_date_filter = False
                offset = 0
                continue
            break
        
        # Handle different JSON response structures
        # JSON format can be: {"amendments": {"amendment": [...]}} or {"amendments": [...]} or just a list
        amendment_list = []
        if isinstance(data, list):
            # Direct array response
            amendment_list = data
        elif "amendments" in data:
            amendments_data = data["amendments"]
            if isinstance(amendments_data, dict):
                # {"amendments": {"amendment": [...]}}
                amendment_list = amendments_data.get("amendment", [])
            elif isinstance(amendments_data, list):
                # {"amendments": [...]}
                amendment_list = amendments_data
        
        if not amendment_list:
            break
        
        # If not using date filter, filter locally by updateDate or submittedDate
        if not use_date_filter:
            filtered_amendments = []
            for amendment in amendment_list:
                # Check updateDate or submittedDate
                update_date_str = amendment.get("updateDate") or amendment.get("submittedDate")
                if update_date_str:
                    try:
                        # Parse date (could be YYYY-MM-DD or ISO format)
                        if 'T' in update_date_str:
                            amendment_date = datetime.fromisoformat(update_date_str.replace('Z', '+00:00'))
                        else:
                            amendment_date = datetime.fromisoformat(update_date_str + 'T00:00:00+00:00')
                        
                        if start_datetime <= amendment_date <= end_datetime:
                            filtered_amendments.append(amendment)
                    except:
                        pass
            amendment_list = filtered_amendments
        
        amendments.extend(amendment_list)
        print(f"      ✅ Fetched {len(amendment_list)} amendments matching date range (total: {len(amendments)})")
        
        # Check if there are more results
        # Pagination can be at different levels
        pagination = {}
        if isinstance(data, dict):
            if "amendments" in data and isinstance(data["amendments"], dict):
                pagination = data["amendments"].get("pagination", {})
            elif "pagination" in data:
                pagination = data["pagination"]
        
        count = pagination.get("count", 0) if pagination else 0
        if count > 0 and offset + limit >= count:
            break
        
        offset += limit
        time.sleep(0.5)  # Rate limiting
    
    return amendments


def fetch_amendment_details(congress: int, amendment_type: str, amendment_number: int) -> Optional[Dict]:
    """Fetch full details for a specific amendment."""
    url = f"{API_BASE_URL}/amendment/{congress}/{amendment_type.lower()}/{amendment_number}"
    params = {
        "api_key": API_KEY,
        "format": "json"
    }
    
    data = make_api_request(url, params)
    if data:
        # Handle different response structures
        if isinstance(data, dict):
            return data.get("amendment", data)  # If no "amendment" key, return the whole dict
        return data
    return None


def fetch_summaries(congress: Optional[int] = None, bill_type: Optional[str] = None, from_date: str = None, to_date: str = None) -> List[Dict]:
    """
    Fetch bill summaries - BULK method for getting all legislative activity.
    
    This is the preferred method as it provides summaries for all bills updated in a date range.
    Supports filtering by congress and/or bill type.
    
    Args:
        congress: Optional congress number (e.g., 117)
        bill_type: Optional bill type (e.g., "hr", "s") - lowercase
        from_date: Start date in ISO format (e.g., "2022-08-04T04:02:00Z")
        to_date: End date in ISO format (e.g., "2022-09-30T04:03:00Z")
    
    Returns:
        List of summary records with bill information
    """
    summaries = []
    offset = 0
    limit = 250
    
    # Build URL based on parameters
    if congress and bill_type:
        url = f"{API_BASE_URL}/summaries/{congress}/{bill_type.lower()}"
        print(f"   📋 Fetching summaries for Congress {congress}, bill type {bill_type}...")
    elif congress:
        url = f"{API_BASE_URL}/summaries/{congress}"
        print(f"   📋 Fetching summaries for Congress {congress}...")
    else:
        url = f"{API_BASE_URL}/summaries"
        print(f"   📋 Fetching all summaries...")
    
    while True:
        params = {
            "api_key": API_KEY,
            "format": "json",
            "offset": offset,
            "limit": limit
        }
        
        # Add date filters if provided
        if from_date:
            params["fromDateTime"] = from_date
        if to_date:
            params["toDateTime"] = to_date
        
        # Add sort parameter
        params["sort"] = "updateDate+asc"
        
        data = make_api_request(url, params)
        if not data:
            break
        
        # Handle different JSON response structures
        summary_list = []
        if isinstance(data, list):
            summary_list = data
        elif "summaries" in data:
            summaries_data = data["summaries"]
            if isinstance(summaries_data, dict):
                # {"summaries": {"summary": [...]}}
                summary_list = summaries_data.get("summary", [])
            elif isinstance(summaries_data, list):
                # {"summaries": [...]}
                summary_list = summaries_data
        
        if not summary_list:
            break
        
        summaries.extend(summary_list)
        print(f"      ✅ Fetched {len(summary_list)} summaries (total: {len(summaries)})")
        
        # Check if there are more results
        pagination = {}
        if isinstance(data, dict):
            if "summaries" in data and isinstance(data["summaries"], dict):
                pagination = data["summaries"].get("pagination", {})
            elif "pagination" in data:
                pagination = data["pagination"]
        
        count = pagination.get("count", 0) if pagination else 0
        if count > 0 and offset + limit >= count:
            break
        
        offset += limit
        time.sleep(0.5)  # Rate limiting
    
    return summaries


def write_to_csv(data: List[Dict], filename: str):
    """Write data to CSV file."""
    if not data:
        print(f"   ⚠️ No data to write to {filename}")
        return
    
    # Get all unique keys from all records
    all_keys = set()
    for record in data:
        all_keys.update(record.keys())
    
    # Sort keys for consistent column order
    fieldnames = sorted(all_keys)
    
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(data)
    
    print(f"   ✅ Wrote {len(data)} records to {filename}")


# ============================================================================
# Main Execution
# ============================================================================

def main():
    if not API_KEY:
        print("❌ ERROR: Please set API_KEY at the top of this script")
        return
    
    print("=" * 80)
    print("Congress.gov API Test Script - Last 1 Day of Legislative Activity")
    print("=" * 80)
    
    # Calculate date range (last 1 day)
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=1)
    
    from_datetime = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    to_datetime = end_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    print(f"📅 Date Range: {start_date.strftime('%Y-%m-%d %H:%M:%S UTC')} to {end_date.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"   fromDateTime: {from_datetime}")
    print(f"   toDateTime: {to_datetime}")
    print()
    
    # Get current Congress
    congress = get_current_congress()
    print()
    
    # ========================================================================
    # Fetch Summaries (BULK METHOD - Preferred)
    # ========================================================================
    print("📋 Fetching Bill Summaries (Bulk Method)...")
    print("-" * 80)
    print("   ℹ️ Using /summaries endpoint for bulk activity retrieval")
    print()
    
    all_summaries = []
    
    # Option 1: Fetch all summaries for the congress (faster, single endpoint)
    print("   Option 1: Fetching all summaries for current Congress...")
    summaries = fetch_summaries(congress=congress, from_date=from_datetime, to_date=to_datetime)
    all_summaries.extend(summaries)
    print(f"   ✅ Fetched {len(summaries)} summaries from Congress {congress}")
    print()
    
    # Option 2: Fetch by bill type (if you want to separate by type)
    # Uncomment if you want separate files per bill type
    # for bill_type in BILL_TYPES:
    #     summaries = fetch_summaries(congress=congress, bill_type=bill_type, from_date=from_datetime, to_date=to_datetime)
    #     all_summaries.extend(summaries)
    
    print(f"✅ Total summaries fetched: {len(all_summaries)}")
    print()
    
    # ========================================================================
    # Fetch Bills (Individual Method - Fallback)
    # ========================================================================
    print("📋 Fetching Bills...")
    print("-" * 80)
    
    all_bills = []
    all_bill_details = []
    
    for bill_type in BILL_TYPES:
        bills = fetch_bills(congress, bill_type, from_datetime, to_datetime)
        all_bills.extend(bills)
        
        # Fetch full details for each bill
        print(f"   🔍 Fetching details for {len(bills)} {bill_type} bills...")
        for i, bill in enumerate(bills):
            bill_number = bill.get("number")
            if bill_number:
                details = fetch_bill_details(congress, bill_type, bill_number)
                if details:
                    # Add bill type and number to details
                    details["bill_type"] = bill_type
                    details["bill_number"] = bill_number
                    details["congress"] = congress
                    all_bill_details.append(flatten_dict(details))
                
                if (i + 1) % 10 == 0:
                    print(f"      ✅ Processed {i + 1}/{len(bills)} bills...")
                
                time.sleep(0.3)  # Rate limiting
        
        print()
    
    print(f"✅ Total bills fetched: {len(all_bills)}")
    print(f"✅ Total bill details fetched: {len(all_bill_details)}")
    print()
    
    # ========================================================================
    # Fetch Amendments
    # ========================================================================
    print("📝 Fetching Amendments...")
    print("-" * 80)
    
    all_amendments = []
    all_amendment_details = []
    
    for amendment_type in AMENDMENT_TYPES:
        amendments = fetch_amendments(congress, amendment_type, from_datetime, to_datetime)
        all_amendments.extend(amendments)
        
        # Fetch full details for each amendment
        print(f"   🔍 Fetching details for {len(amendments)} {amendment_type} amendments...")
        for i, amendment in enumerate(amendments):
            amendment_number = amendment.get("number")
            if amendment_number:
                details = fetch_amendment_details(congress, amendment_type, amendment_number)
                if details:
                    # Add amendment type and number to details
                    details["amendment_type"] = amendment_type
                    details["amendment_number"] = amendment_number
                    details["congress"] = congress
                    all_amendment_details.append(flatten_dict(details))
                
                if (i + 1) % 10 == 0:
                    print(f"      ✅ Processed {i + 1}/{len(amendments)} amendments...")
                
                time.sleep(0.3)  # Rate limiting
        
        print()
    
    print(f"✅ Total amendments fetched: {len(all_amendments)}")
    print(f"✅ Total amendment details fetched: {len(all_amendment_details)}")
    print()
    
    # ========================================================================
    # Write to CSV
    # ========================================================================
    print("💾 Writing data to CSV files...")
    print("-" * 80)
    
    # Write summaries (bulk method - preferred)
    flattened_summaries = [flatten_dict(summary) for summary in all_summaries]
    write_to_csv(flattened_summaries, "congress_summaries_bulk.csv")
    
    # Flatten bill list data (individual method)
    if all_bills:
        flattened_bills = [flatten_dict(bill) for bill in all_bills]
        write_to_csv(flattened_bills, "congress_bills_list.csv")
    if all_bill_details:
        write_to_csv(all_bill_details, "congress_bills_details.csv")
    
    # Flatten amendment list data
    if all_amendments:
        flattened_amendments = [flatten_dict(amendment) for amendment in all_amendments]
        write_to_csv(flattened_amendments, "congress_amendments_list.csv")
    if all_amendment_details:
        write_to_csv(all_amendment_details, "congress_amendments_details.csv")
    
    print()
    print("=" * 80)
    print("✅ Script completed successfully!")
    print("=" * 80)
    print()
    print("Generated files:")
    print("  - congress_summaries_bulk.csv (BULK summaries - preferred method)")
    if all_bills:
        print("  - congress_bills_list.csv (list-level bill data)")
    if all_bill_details:
        print("  - congress_bills_details.csv (full bill details)")
    if all_amendments:
        print("  - congress_amendments_list.csv (list-level amendment data)")
    if all_amendment_details:
        print("  - congress_amendments_details.csv (full amendment details)")
    print()
    print("💡 TIP: The summaries endpoint is the most efficient way to get bulk activity!")
    print("   It includes bill information and summaries in a single call.")
    print()


if __name__ == "__main__":
    main()






