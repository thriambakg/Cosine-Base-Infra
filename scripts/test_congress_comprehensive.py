"""
Comprehensive Congress.gov API Test Script
Fetches ALL data for each bill and creates one row per bill with:
- Bill basic info
- Primary sponsor (for sorting by proposer/party)
- All cosponsors
- Full action history
- All amendments with sponsors (who's amending it)
- All summaries (for text search)
- Subjects
- Key dates (for date sorting)

This creates a denormalized dataset perfect for:
- Sorting by proposer, party, dates
- Direct bill lookups
- Text search within bill descriptions
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
        
        # Handle different response structures
        congresses = None
        if isinstance(data, list):
            congresses = data
        elif "congresses" in data:
            congresses_data = data["congresses"]
            if isinstance(congresses_data, dict):
                congresses = congresses_data.get("item", [])
            elif isinstance(congresses_data, list):
                congresses = congresses_data
        
        if not congresses:
            current_year = datetime.now().year
            congress = ((current_year - 1789) // 2) + 1
            print(f"⚠️ Could not fetch Congress list, using calculated value: {congress}")
            return congress
        
        sorted_congresses = sorted(congresses, key=lambda x: x.get("number", 0) if isinstance(x, dict) else (x if isinstance(x, (int, str)) else 0), reverse=True)
        if not sorted_congresses:
            raise ValueError("No congresses found")
        
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
                print(f"      ⚠️ Request failed (attempt {attempt + 1}/{retries}): {str(e)[:100]}")
                time.sleep(wait_time)
            else:
                print(f"      ❌ Request failed after {retries} attempts: {str(e)[:100]}")
                return None
    return None


def fetch_bills_list(congress: int, bill_type: str, from_date: str, to_date: str) -> List[Dict]:
    """Fetch list of bills for a specific type and date range."""
    bills = []
    offset = 0
    limit = 250
    
    while True:
        url = f"{API_BASE_URL}/bill/{congress}/{bill_type.lower()}"
        params = {
            "api_key": API_KEY,
            "format": "json",
            "offset": offset,
            "limit": limit,
            "fromDateTime": from_date,
            "toDateTime": to_date
        }
        
        data = make_api_request(url, params)
        if not data:
            break
        
        # Handle different response structures
        bill_list = []
        if isinstance(data, list):
            bill_list = data
        elif "bills" in data:
            bills_data = data["bills"]
            if isinstance(bills_data, dict):
                bill_list = bills_data.get("bill", [])
            elif isinstance(bills_data, list):
                bill_list = bills_data
        
        if not bill_list:
            break
        
        bills.extend(bill_list)
        
        # Check pagination
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
        time.sleep(0.3)  # Rate limiting
    
    return bills


def fetch_bill_details(congress: int, bill_type: str, bill_number: int) -> Optional[Dict]:
    """Fetch full bill details."""
    url = f"{API_BASE_URL}/bill/{congress}/{bill_type.lower()}/{bill_number}"
    params = {"api_key": API_KEY, "format": "json"}
    
    data = make_api_request(url, params)
    if data and isinstance(data, dict):
        return data.get("bill", data)
    return data


def fetch_bill_actions(congress: int, bill_type: str, bill_number: int) -> List[Dict]:
    """Fetch all actions for a bill."""
    actions = []
    offset = 0
    limit = 250
    
    while True:
        url = f"{API_BASE_URL}/bill/{congress}/{bill_type.lower()}/{bill_number}/actions"
        params = {
            "api_key": API_KEY,
            "format": "json",
            "offset": offset,
            "limit": limit
        }
        
        data = make_api_request(url, params)
        if not data:
            break
        
        action_list = []
        if isinstance(data, list):
            action_list = data
        elif "actions" in data:
            actions_data = data["actions"]
            if isinstance(actions_data, dict):
                action_list = actions_data.get("item", [])
            elif isinstance(actions_data, list):
                action_list = actions_data
        
        if not action_list:
            break
        
        actions.extend(action_list)
        
        # Check pagination
        pagination = {}
        if isinstance(data, dict):
            if "actions" in data and isinstance(data["actions"], dict):
                pagination = data["actions"].get("pagination", {})
            elif "pagination" in data:
                pagination = data["pagination"]
        
        count = pagination.get("count", 0) if pagination else 0
        if count > 0 and offset + limit >= count:
            break
        
        offset += limit
        time.sleep(0.2)
    
    return actions


def fetch_bill_amendments(congress: int, bill_type: str, bill_number: int) -> List[Dict]:
    """Fetch all amendments for a bill."""
    amendments = []
    offset = 0
    limit = 250
    
    while True:
        url = f"{API_BASE_URL}/bill/{congress}/{bill_type.lower()}/{bill_number}/amendments"
        params = {
            "api_key": API_KEY,
            "format": "json",
            "offset": offset,
            "limit": limit
        }
        
        data = make_api_request(url, params)
        if not data:
            break
        
        amendment_list = []
        if isinstance(data, list):
            amendment_list = data
        elif "amendments" in data:
            amendments_data = data["amendments"]
            if isinstance(amendments_data, dict):
                amendment_list = amendments_data.get("amendment", [])
            elif isinstance(amendments_data, list):
                amendment_list = amendments_data
        
        if not amendment_list:
            break
        
        amendments.extend(amendment_list)
        
        # Check pagination
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
        time.sleep(0.2)
    
    return amendments


def fetch_bill_cosponsors(congress: int, bill_type: str, bill_number: int) -> List[Dict]:
    """Fetch all cosponsors for a bill."""
    cosponsors = []
    offset = 0
    limit = 250
    
    while True:
        url = f"{API_BASE_URL}/bill/{congress}/{bill_type.lower()}/{bill_number}/cosponsors"
        params = {
            "api_key": API_KEY,
            "format": "json",
            "offset": offset,
            "limit": limit
        }
        
        data = make_api_request(url, params)
        if not data:
            break
        
        cosponsor_list = []
        if isinstance(data, list):
            cosponsor_list = data
        elif "cosponsors" in data:
            cosponsors_data = data["cosponsors"]
            if isinstance(cosponsors_data, dict):
                cosponsor_list = cosponsors_data.get("item", [])
            elif isinstance(cosponsors_data, list):
                cosponsor_list = cosponsors_data
        
        if not cosponsor_list:
            break
        
        cosponsors.extend(cosponsor_list)
        
        # Check pagination
        pagination = {}
        if isinstance(data, dict):
            if "cosponsors" in data and isinstance(data["cosponsors"], dict):
                pagination = data["cosponsors"].get("pagination", {})
            elif "pagination" in data:
                pagination = data["pagination"]
        
        count = pagination.get("count", 0) if pagination else 0
        if count > 0 and offset + limit >= count:
            break
        
        offset += limit
        time.sleep(0.2)
    
    return cosponsors


def fetch_bill_summaries(congress: int, bill_type: str, bill_number: int) -> List[Dict]:
    """Fetch all summaries for a bill."""
    summaries = []
    offset = 0
    limit = 250
    
    while True:
        url = f"{API_BASE_URL}/bill/{congress}/{bill_type.lower()}/{bill_number}/summaries"
        params = {
            "api_key": API_KEY,
            "format": "json",
            "offset": offset,
            "limit": limit
        }
        
        data = make_api_request(url, params)
        if not data:
            break
        
        summary_list = []
        if isinstance(data, list):
            summary_list = data
        elif "summaries" in data:
            summaries_data = data["summaries"]
            if isinstance(summaries_data, dict):
                summary_list = summaries_data.get("summary", [])
            elif isinstance(summaries_data, list):
                summary_list = summaries_data
        
        if not summary_list:
            break
        
        summaries.extend(summary_list)
        
        # Check pagination
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
        time.sleep(0.2)
    
    return summaries


def fetch_bill_subjects(congress: int, bill_type: str, bill_number: int) -> Dict:
    """Fetch subjects for a bill."""
    url = f"{API_BASE_URL}/bill/{congress}/{bill_type.lower()}/{bill_number}/subjects"
    params = {"api_key": API_KEY, "format": "json"}
    
    data = make_api_request(url, params)
    if data and isinstance(data, dict):
        return data.get("subjects", {})
    return {}


def build_comprehensive_bill_record(bill: Dict, congress: int, bill_type: str) -> Dict:
    """
    Build a comprehensive bill record with all related data.
    One row per bill with all information flattened.
    """
    bill_number = bill.get("number")
    if not bill_number:
        return None
    
    print(f"      📋 Processing {bill_type} {bill_number}...")
    
    # Fetch all related data
    details = fetch_bill_details(congress, bill_type, bill_number)
    actions = fetch_bill_actions(congress, bill_type, bill_number)
    amendments = fetch_bill_amendments(congress, bill_type, bill_number)
    cosponsors = fetch_bill_cosponsors(congress, bill_type, bill_number)
    summaries = fetch_bill_summaries(congress, bill_type, bill_number)
    subjects = fetch_bill_subjects(congress, bill_type, bill_number)
    
    # Extract primary sponsor (first sponsor from details)
    primary_sponsor = {}
    if details:
        sponsors = details.get("sponsors", {})
        if isinstance(sponsors, dict):
            sponsor_items = sponsors.get("item", [])
            if isinstance(sponsor_items, list) and len(sponsor_items) > 0:
                primary_sponsor = sponsor_items[0]
            elif isinstance(sponsor_items, dict):
                primary_sponsor = sponsor_items
        elif isinstance(sponsors, list) and len(sponsors) > 0:
            primary_sponsor = sponsors[0]
    
    # Build comprehensive record
    record = {
        # Bill Basic Info
        "bill_id": f"{congress}-{bill_type}-{bill_number}",
        "congress": congress,
        "bill_type": bill_type,
        "bill_number": bill_number,
        "bill_title": bill.get("title") or (details.get("title") if details else ""),
        "bill_url": bill.get("url") or (details.get("url") if details else ""),
        
        # Dates (for sorting)
        "introduced_date": details.get("introducedDate") if details else bill.get("introducedDate", ""),
        "latest_action_date": bill.get("latestAction", {}).get("actionDate") if isinstance(bill.get("latestAction"), dict) else (details.get("latestAction", {}).get("actionDate") if details and isinstance(details.get("latestAction"), dict) else ""),
        "update_date": bill.get("updateDate", ""),
        "update_date_including_text": bill.get("updateDateIncludingText", ""),
        
        # Primary Sponsor (for sorting by proposer/party)
        "sponsor_bioguide_id": primary_sponsor.get("bioguideId", ""),
        "sponsor_full_name": primary_sponsor.get("fullName", ""),
        "sponsor_first_name": primary_sponsor.get("firstName", ""),
        "sponsor_last_name": primary_sponsor.get("lastName", ""),
        "sponsor_party": primary_sponsor.get("party", ""),  # For party sorting
        "sponsor_state": primary_sponsor.get("state", ""),
        "sponsor_district": primary_sponsor.get("district", ""),
        "sponsor_url": primary_sponsor.get("url", ""),
        
        # Cosponsors (comma-separated for easy viewing)
        "cosponsor_count": len(cosponsors),
        "cosponsor_names": "|".join([c.get("fullName", "") for c in cosponsors if c.get("fullName")]),
        "cosponsor_parties": "|".join([c.get("party", "") for c in cosponsors if c.get("party")]),
        "cosponsors_json": json.dumps(cosponsors) if cosponsors else "",
        
        # Actions (full history - JSON for detailed access)
        "action_count": len(actions),
        "actions_json": json.dumps(actions) if actions else "",
        "actions_summary": " | ".join([f"{a.get('actionDate', '')}: {a.get('text', '')[:100]}" for a in actions[:10]]) if actions else "",
        
        # Amendments (with sponsors - who's amending it)
        "amendment_count": len(amendments),
        "amendment_numbers": "|".join([str(a.get("number", "")) for a in amendments if a.get("number")]),
        "amendment_sponsors": "|".join([
            a.get("sponsors", {}).get("item", [{}])[0].get("fullName", "") if isinstance(a.get("sponsors"), dict) and a.get("sponsors", {}).get("item") else ""
            for a in amendments
        ]),
        "amendments_json": json.dumps(amendments) if amendments else "",
        
        # Summaries (for text search)
        "summary_count": len(summaries),
        "summary_text": " ".join([
            s.get("text", "").replace("<p>", " ").replace("</p>", " ").replace("<strong>", "").replace("</strong>", "")
            for s in summaries if s.get("text")
        ])[:5000],  # Limit to 5000 chars for CSV
        "summaries_json": json.dumps(summaries) if summaries else "",
        
        # Subjects
        "subjects_json": json.dumps(subjects) if subjects else "",
        "policy_area": "",
        "legislative_subjects": "",
        
        # Additional bill info
        "origin_chamber": bill.get("originChamber") or (details.get("originChamber") if details else ""),
        "origin_chamber_code": bill.get("originChamberCode") or (details.get("originChamberCode") if details else ""),
        "latest_action_text": bill.get("latestAction", {}).get("text", "") if isinstance(bill.get("latestAction"), dict) else (details.get("latestAction", {}).get("text", "") if details and isinstance(details.get("latestAction"), dict) else ""),
    }
    
    # Parse subjects (handle different response structures)
    if isinstance(subjects, dict):
        # Parse policy area
        policy_area_data = subjects.get("policyArea", {})
        if isinstance(policy_area_data, dict):
            record["policy_area"] = policy_area_data.get("name", "")
        elif isinstance(policy_area_data, str):
            record["policy_area"] = policy_area_data
        
        # Parse legislative subjects
        legislative_subjects_data = subjects.get("legislativeSubjects", {})
        subject_names = []
        if isinstance(legislative_subjects_data, dict):
            items = legislative_subjects_data.get("item", [])
            if isinstance(items, list):
                subject_names = [s.get("name", "") for s in items if isinstance(s, dict) and s.get("name")]
            elif isinstance(items, dict):
                subject_names = [items.get("name", "")] if items.get("name") else []
        elif isinstance(legislative_subjects_data, list):
            subject_names = [s.get("name", "") for s in legislative_subjects_data if isinstance(s, dict) and s.get("name")]
        
        record["legislative_subjects"] = "|".join(subject_names)
    
    return record


# ============================================================================
# Main Execution
# ============================================================================

def main():
    if not API_KEY:
        print("❌ ERROR: Please set API_KEY at the top of this script")
        return
    
    print("=" * 80)
    print("Congress.gov Comprehensive Bill Data Fetcher")
    print("=" * 80)
    
    # Calculate date range (last 1 day)
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=1)
    
    from_datetime = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    to_datetime = end_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    print(f"📅 Date Range: {start_date.strftime('%Y-%m-%d %H:%M:%S UTC')} to {end_date.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print()
    
    # Get current Congress
    congress = get_current_congress()
    print()
    
    # Fetch all bills
    print("📋 Fetching Bills...")
    print("-" * 80)
    
    all_bills = []
    for bill_type in BILL_TYPES:
        print(f"   📋 Fetching {bill_type} bills...")
        bills = fetch_bills_list(congress, bill_type, from_datetime, to_datetime)
        all_bills.extend(bills)
        print(f"      ✅ Found {len(bills)} {bill_type} bills")
    
    print(f"\n✅ Total bills found: {len(all_bills)}")
    print()
    
    # Build comprehensive records
    print("🔍 Building comprehensive bill records...")
    print("-" * 80)
    
    comprehensive_records = []
    for i, bill in enumerate(all_bills):
        bill_type = bill.get("type", "")
        if not bill_type:
            continue
        
        record = build_comprehensive_bill_record(bill, congress, bill_type)
        if record:
            comprehensive_records.append(record)
        
        if (i + 1) % 10 == 0:
            print(f"      ✅ Processed {i + 1}/{len(all_bills)} bills...")
        
        time.sleep(0.5)  # Rate limiting
    
    print(f"\n✅ Built {len(comprehensive_records)} comprehensive records")
    print()
    
    # Write to CSV
    print("💾 Writing to CSV...")
    print("-" * 80)
    
    if not comprehensive_records:
        print("   ⚠️ No records to write")
        return
    
    # Get all unique keys
    all_keys = set()
    for record in comprehensive_records:
        all_keys.update(record.keys())
    
    fieldnames = sorted(all_keys)
    
    filename = "congress_bills_comprehensive.csv"
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(comprehensive_records)
    
    print(f"   ✅ Wrote {len(comprehensive_records)} records to {filename}")
    print()
    print("=" * 80)
    print("✅ Script completed successfully!")
    print("=" * 80)
    print()
    print("📊 Data Structure:")
    print("   - One row per bill")
    print("   - Primary sponsor info (for sorting by proposer/party)")
    print("   - All cosponsors (comma-separated + JSON)")
    print("   - Full action history (JSON)")
    print("   - All amendments with sponsors (JSON)")
    print("   - All summaries (for text search)")
    print("   - Subjects and policy area")
    print("   - Key dates (for date sorting)")
    print()


if __name__ == "__main__":
    main()









