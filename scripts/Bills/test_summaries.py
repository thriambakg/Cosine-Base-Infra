"""
Test script for Congress.gov /summaries API endpoint
Tests pagination, date filtering, and data extraction to ensure we get all needed fields
"""

import json
import requests
import time
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta, timezone

# ============================================================================
# Configuration - PASTE YOUR API KEY HERE
# ============================================================================
API_KEY = ""  # Replace with your actual API key
API_BASE_URL = "https://api.congress.gov/v3"
REQUEST_TIMEOUT = 30
MAX_RETRIES = 5
RETRY_DELAY = 2

# Test date range (default to past 200 days to get some results)
now = datetime.now(timezone.utc)
# Use a wider date range to ensure we get results
start_date = now - timedelta(days=2)
end_date = now

# Format as required by API: YYYY-MM-DDT00:00:00Z (set time to 00:00:00)
FROM_DATETIME = start_date.replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")
TO_DATETIME = end_date.replace(hour=23, minute=59, second=59, microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")

print(f"Test Date Range: {FROM_DATETIME} to {TO_DATETIME}")


def make_api_request(url: str, params: Dict[str, Any], api_key: str, retries: int = MAX_RETRIES) -> Optional[Dict]:
    """Make API request with retry logic and exponential backoff for rate limiting."""
    if "api_key" not in params:
        params["api_key"] = api_key
    
    for attempt in range(retries):
        try:
            print(f"   Attempt {attempt + 1}/{retries}: Making request to {url}")
            response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            
            # Handle 429 Too Many Requests with exponential backoff
            if response.status_code == 429:
                if attempt < retries - 1:
                    wait_time = max(5, (2 ** attempt) * RETRY_DELAY)
                    print(f"      ⚠️ Rate limited (429) - waiting {wait_time}s before retry")
                    time.sleep(wait_time)
                    continue
                else:
                    print(f"      ❌ Rate limited (429) after {retries} attempts")
                    return None
            
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            print(f"      ⚠️ HTTP Error: {e}")
            if attempt < retries - 1:
                wait_time = RETRY_DELAY * (attempt + 1)
                print(f"      Waiting {wait_time}s before retry...")
                time.sleep(wait_time)
            else:
                print(f"      ❌ Request failed after {retries} attempts")
                return None
        except requests.exceptions.RequestException as e:
            print(f"      ⚠️ Request Exception: {e}")
            if attempt < retries - 1:
                wait_time = RETRY_DELAY * (attempt + 1)
                print(f"      Waiting {wait_time}s before retry...")
                time.sleep(wait_time)
            else:
                print(f"      ❌ Request failed after {retries} attempts")
                return None
    return None


def fetch_summaries_paginated(
    from_datetime: str,
    to_datetime: str,
    api_key: str,
    congress: Optional[int] = None,
    bill_type: Optional[str] = None,
    limit: int = 250
) -> List[Dict]:
    """
    Fetch all summaries from the /summaries endpoint with pagination.
    
    Args:
        from_datetime: Start timestamp (YYYY-MM-DDT00:00:00Z)
        to_datetime: End timestamp (YYYY-MM-DDT00:00:00Z)
        api_key: Congress.gov API key
        congress: Optional congress number to filter
        bill_type: Optional bill type to filter (hr, s, etc.)
        limit: Maximum records per request (max 250)
    
    Returns:
        List of all summary records
    """
    all_summaries = []
    offset = 0
    
    # Build base URL
    if congress and bill_type:
        url = f"{API_BASE_URL}/summaries/{congress}/{bill_type}"
    elif congress:
        url = f"{API_BASE_URL}/summaries/{congress}"
    else:
        url = f"{API_BASE_URL}/summaries"
    
    print(f"\n📋 Fetching summaries from: {url}")
    print(f"   Date Range: {from_datetime} to {to_datetime}")
    print(f"   Limit per request: {limit}")
    
    while True:
        params = {
            "format": "json",
            "fromDateTime": from_datetime,
            "toDateTime": to_datetime,
            "offset": offset,
            "limit": limit,
            "sort": "updateDate+desc"  # Most recent first
        }
        
        print(f"\n   📄 Fetching page: offset={offset}, limit={limit}")
        
        data = make_api_request(url, params, api_key)
        if not data:
            print(f"   ❌ Failed to fetch summaries at offset {offset}")
            break
        
        # Debug: Print response structure
        if isinstance(data, dict):
            print(f"   🔍 Response structure: {list(data.keys())}")
        
        # Handle response structure
        # API returns: {"pagination": {...}, "request": {...}, "summaries": [...]}
        summaries = []
        if isinstance(data, dict) and "summaries" in data:
            summaries_data = data["summaries"]
            if isinstance(summaries_data, list):
                # summaries is a list - this is the correct structure
                summaries = summaries_data
            elif isinstance(summaries_data, dict) and "item" in summaries_data:
                # XML-style response with "item" wrapper
                item_data = summaries_data["item"]
                summaries = item_data if isinstance(item_data, list) else [item_data]
        elif isinstance(data, list):
            # Direct array response (unlikely but handle it)
            summaries = data
        
        # Check pagination count to provide better feedback
        pagination_count = None
        if isinstance(data, dict) and "pagination" in data:
            pagination = data.get("pagination", {})
            if isinstance(pagination, dict):
                pagination_count = pagination.get("count", 0)
        
        if not summaries:
            if pagination_count == 0:
                print(f"   ℹ️ Pagination shows count=0, no summaries in date range")
            else:
                print(f"   ⚠️ No summaries found in response")
                if isinstance(data, dict):
                    print(f"   Response keys: {list(data.keys())}")
                    if pagination_count is not None:
                        print(f"   Pagination count: {pagination_count}")
            break
        
        print(f"   ✅ Received {len(summaries)} summaries in this page")
        all_summaries.extend(summaries)
        
        # Check if we got fewer than limit (last page)
        if len(summaries) < limit:
            print(f"   ✅ Reached end of results (got {len(summaries)} < {limit})")
            break
        
        # Increment offset for next page
        offset += limit
        
        # Safety limit to prevent infinite loops
        if offset > 10000:  # Max 10,000 records
            print(f"   ⚠️ Reached safety limit of 10,000 records")
            break
        
        # Small delay between requests to be respectful
        time.sleep(0.5)
    
    print(f"\n✅ Total summaries fetched: {len(all_summaries)}")
    return all_summaries


def extract_bill_info_from_summary(summary: Dict) -> Dict[str, Any]:
    """
    Extract bill information from a summary record.
    This simulates what we need to extract to populate our DynamoDB table.
    
    Returns:
        Dictionary with bill_id and other key fields we need
    """
    bill_info = summary.get("bill", {})
    
    congress = bill_info.get("congress")
    bill_type = bill_info.get("type", "").upper()
    bill_number = bill_info.get("number")
    
    if not congress or not bill_type or not bill_number:
        return None
    
    bill_id = f"{congress}-{bill_type}-{bill_number}"
    
    # Extract key fields from summary
    extracted = {
        "bill_id": bill_id,
        "congress": congress,
        "bill_type": bill_type,
        "bill_number": int(bill_number) if str(bill_number).isdigit() else 0,
        "bill_title": bill_info.get("title", ""),
        "update_date": summary.get("updateDate", ""),
        "last_summary_update_date": summary.get("lastSummaryUpdateDate", ""),
        "action_date": summary.get("actionDate", ""),
        "action_desc": summary.get("actionDesc", ""),
        "current_chamber": summary.get("currentChamber", ""),
        "current_chamber_code": summary.get("currentChamberCode", ""),
        "version_code": summary.get("versionCode", ""),
        "summary_text": summary.get("text", ""),
        "bill_url": bill_info.get("url", ""),
        "update_date_including_text": bill_info.get("updateDateIncludingText", ""),
    }
    
    return extracted


def test_summaries_endpoint():
    """Test the summaries endpoint with pagination"""
    print("=" * 80)
    print("Congress.gov /summaries API Endpoint Test")
    print("=" * 80)
    print(f"API Key: {API_KEY[:10]}...{API_KEY[-10:] if len(API_KEY) > 20 else '***'}")
    print("")
    
    if API_KEY == "PASTE_YOUR_API_KEY_HERE":
        print("❌ ERROR: Please paste your API key in the API_KEY variable at the top of this script")
        return
    
    # Test 1: Fetch summaries for all bills (no filters)
    print("\n" + "=" * 80)
    print("TEST 1: Fetch summaries for all bills (no congress/billType filter)")
    print("=" * 80)
    all_summaries = fetch_summaries_paginated(FROM_DATETIME, TO_DATETIME, API_KEY)
    
    if not all_summaries:
        print("\n❌ No summaries returned")
        return
    
    print(f"\n📊 Summary Statistics:")
    print(f"   Total summaries: {len(all_summaries)}")
    
    # Extract bill IDs and check for duplicates
    bill_ids = set()
    for summary in all_summaries:
        extracted = extract_bill_info_from_summary(summary)
        if extracted:
            bill_ids.add(extracted["bill_id"])
    
    print(f"   Unique bills: {len(bill_ids)}")
    print(f"   Duplicate summaries: {len(all_summaries) - len(bill_ids)}")
    
    # Show sample data
    print(f"\n📄 Sample Summary (first 3):")
    for i, summary in enumerate(all_summaries[:3], 1):
        extracted = extract_bill_info_from_summary(summary)
        if extracted:
            print(f"\n   {i}. Bill ID: {extracted['bill_id']}")
            print(f"      Title: {extracted['bill_title'][:80]}...")
            print(f"      Update Date: {extracted['update_date']}")
            print(f"      Action: {extracted['action_desc']}")
            print(f"      Summary Text Length: {len(extracted['summary_text'])} chars")
    
    # Test 2: Check what fields we're missing vs what we need
    print(f"\n" + "=" * 80)
    print("TEST 2: Field Mapping Analysis")
    print("=" * 80)
    
    # Fields we need from the glue script (from parse_bill_xml)
    required_fields = [
        "bill_id", "search_index_sk", "congress", "bill_type", "bill_number",
        "bill_title", "bill_url", "introduced_date", "latest_action_date",
        "sponsor_bioguide_id", "sponsor_full_name", "sponsor_party", "sponsor_state",
        "cosponsor_count", "cosponsors_json", "action_count", "actions_json",
        "latest_action_text", "latest_action_type", "summary_count", "summaries_json",
        "policy_area", "bipartisan"
    ]
    
    print(f"\n📋 Fields we need in DynamoDB ({len(required_fields)} total):")
    for field in required_fields:
        print(f"   - {field}")
    
    print(f"\n📋 Fields available from /summaries endpoint:")
    sample_summary = all_summaries[0] if all_summaries else {}
    print(f"   Summary fields: {list(sample_summary.keys())}")
    if "bill" in sample_summary:
        print(f"   Bill fields: {list(sample_summary['bill'].keys())}")
    
    print(f"\n⚠️  NOTE: /summaries endpoint only provides summary data.")
    print(f"   We will need to fetch full bill details for each bill using:")
    print(f"   GET /bill/{{congress}}/{{billType}}/{{billNumber}}")
    print(f"   This will give us all the fields we need (actions, cosponsors, etc.)")
    
    # Test 3: Test with congress filter
    print(f"\n" + "=" * 80)
    print("TEST 3: Fetch summaries filtered by Congress (119)")
    print("=" * 80)
    congress_summaries = fetch_summaries_paginated(FROM_DATETIME, TO_DATETIME, API_KEY, congress=119)
    print(f"   ✅ Found {len(congress_summaries)} summaries for Congress 119")
    
    # Test 4: Test with congress and billType filter
    print(f"\n" + "=" * 80)
    print("TEST 4: Fetch summaries filtered by Congress (119) and Bill Type (hr)")
    print("=" * 80)
    filtered_summaries = fetch_summaries_paginated(FROM_DATETIME, TO_DATETIME, API_KEY, congress=119, bill_type="hr")
    print(f"   ✅ Found {len(filtered_summaries)} summaries for Congress 119, HR bills")
    
    print(f"\n" + "=" * 80)
    print("✅ Test Complete!")
    print("=" * 80)
    print(f"\n📊 Final Statistics:")
    print(f"   All bills: {len(all_summaries)} summaries, {len(bill_ids)} unique bills")
    print(f"   Congress 119 only: {len(congress_summaries)} summaries")
    print(f"   Congress 119, HR only: {len(filtered_summaries)} summaries")
    print(f"\n💡 Recommendation:")
    print(f"   Use /summaries to find updated bills, then fetch full details")
    print(f"   for each bill using /bill endpoint to get all required fields.")


if __name__ == "__main__":
    test_summaries_endpoint()

