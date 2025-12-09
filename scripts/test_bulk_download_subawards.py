"""
Test script to verify bulk download CSV structure with sub-awards
Run locally to test how sub-awards are included in the bulk download CSV

Usage:
    python test_bulk_download_subawards.py

Requirements:
    pip install requests

This script will:
1. Initiate a bulk download request with sub_award_types included
2. Poll for download completion
3. Download and extract the CSV file
4. Analyze the CSV structure to determine:
   - If sub-awards are included in the CSV
   - How sub-awards are structured (separate rows, columns, etc.)
   - What columns indicate sub-award relationships
5. Print a detailed analysis report

The test uses a 1-day date range (yesterday) and filters for Department of Commerce to keep the download small and fast.
"""

import json
import time
import csv
import zipfile
import requests
from io import StringIO, BytesIO
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional

# Configuration
USASPENDING_BASE_URL = "https://api.usaspending.gov"
USASPENDING_USER_AGENT = "Cosine Financial Platform Test Script (contact@cosine.financial)"

# Test with a single day (yesterday)
end_date = datetime.now(timezone.utc).date()
start_date = end_date - timedelta(days=1)

print("=" * 80)
print("🧪 Testing Bulk Download with Sub-Awards")
print("=" * 80)
print(f"📅 Date Range: {start_date} to {end_date} (1 day)")
print(f"🏛️  Agency: Department of Commerce")
print(f"🌐 API: {USASPENDING_BASE_URL}")
print("=" * 80)

# Create session
session = requests.Session()
session.headers.update({
    'User-Agent': USASPENDING_USER_AGENT,
    'Accept': 'application/json',
    'Content-Type': 'application/json',
})

def initiate_bulk_download(start_date: str, end_date: str) -> Dict[str, Any]:
    """Initiate bulk download with sub-awards included for Department of Commerce"""
    print(f"\n📥 Step 1: Initiating bulk download for Department of Commerce...")
    
    bulk_filters = {
        "date_range": {
            "start_date": start_date,
            "end_date": end_date
        },
        "date_type": "action_date",
        "prime_award_types": [
            # Contract types
            "A", "B", "C", "D",
            # IDV types
            "IDV_A", "IDV_B", "IDV_B_A", "IDV_B_B", "IDV_B_C", "IDV_C", "IDV_D", "IDV_E",
            # Grant types
            "02", "03", "04", "05", "06", "07", "08", "09", "10", "11",
            # Other/Unknown
            "-1"
        ],
        "sub_award_types": ["grant", "procurement"],  # Include sub-awards
        "agencies": [
            {
                "type": "awarding",
                "tier": "toptier",
                "name": "Department of Commerce"
            },
            {
                "type": "funding",
                "tier": "toptier",
                "name": "Department of Commerce"
            }
        ]
    }
    
    request_body = {
        "filters": bulk_filters,
        "file_format": "csv"
    }
    
    print(f"📤 Request payload:")
    print(json.dumps(request_body, indent=2))
    
    response = session.post(
        f"{USASPENDING_BASE_URL}/api/v2/bulk_download/awards/",
        json=request_body,
        timeout=60
    )
    response.raise_for_status()
    
    result = response.json()
    file_name = result.get("file_name")
    file_url = result.get("file_url")
    
    print(f"✅ Download initiated: {file_name}")
    print(f"📁 File URL: {file_url}")
    
    return result

def poll_download_status(file_name: str, max_wait: int = 3600) -> Dict[str, Any]:
    """Poll download status until ready"""
    print(f"\n⏳ Step 2: Polling download status (max wait: {max_wait // 60} minutes)...")
    
    start_time = time.time()
    attempt = 0
    
    while time.time() - start_time < max_wait:
        attempt += 1
        elapsed = int(time.time() - start_time)
        
        response = session.get(
            f"{USASPENDING_BASE_URL}/api/v2/bulk_download/status/",
            params={"file_name": file_name},
            timeout=30
        )
        response.raise_for_status()
        
        status_info = response.json()
        status = status_info.get("status")
        
        if attempt % 5 == 0 or status in ["ready", "finished", "failed"]:
            print(f"  📊 Attempt {attempt}: status='{status}', elapsed={elapsed}s")
        
        if status == "ready" or status == "finished":
            file_url = status_info.get("file_url")
            if file_url:
                print(f"✅ Download ready!")
                return status_info
        elif status == "failed":
            raise Exception(f"Download failed: {status_info.get('message', 'Unknown error')}")
        
        time.sleep(30)
    
    raise Exception(f"Download timeout after {max_wait} seconds")

def download_and_analyze_csv(file_url: str) -> None:
    """Download CSV and analyze structure for sub-awards"""
    print(f"\n📥 Step 3: Downloading and analyzing CSV...")
    print(f"📁 URL: {file_url}")
    
    # Wait a bit for CDN propagation
    print("⏳ Waiting 30s for CDN propagation...")
    time.sleep(30)
    
    # Download file
    response = session.get(file_url, timeout=300, stream=True)
    response.raise_for_status()
    
    file_size_mb = len(response.content) / (1024 * 1024)
    print(f"✅ Downloaded: {file_size_mb:.2f} MB")
    
    # Check if ZIP
    is_zip = file_url.lower().endswith('.zip') or 'zip' in response.headers.get('Content-Type', '').lower()
    
    if is_zip:
        print("📦 Extracting ZIP file...")
        zip_content = BytesIO(response.content)
        
        with zipfile.ZipFile(zip_content, 'r') as zip_ref:
            file_list = zip_ref.namelist()
            print(f"📋 ZIP contains {len(file_list)} file(s):")
            for fname in file_list:
                print(f"   - {fname}")
            
            # Find CSV files
            csv_files = [f for f in file_list if f.lower().endswith('.csv')]
            if not csv_files:
                print("❌ No CSV files found in ZIP")
                return
            
            # Separate prime and sub-award files
            prime_files = [f for f in csv_files if 'subaward' not in f.lower()]
            subaward_files = [f for f in csv_files if 'subaward' in f.lower()]
            
            print(f"\n📄 Found {len(csv_files)} CSV file(s):")
            print(f"   - {len(prime_files)} Prime award file(s)")
            print(f"   - {len(subaward_files)} Sub-award file(s)")
            
            # Analyze prime awards first
            if prime_files:
                print(f"\n📊 Analyzing prime awards file: {prime_files[0]}")
                csv_content = zip_ref.read(prime_files[0]).decode('utf-8')
                analyze_csv_structure(csv_content, "Prime Awards")
            
            # Analyze sub-awards
            if subaward_files:
                for subaward_file in subaward_files:
                    print(f"\n📊 Analyzing sub-awards file: {subaward_file}")
                    csv_content = zip_ref.read(subaward_file).decode('utf-8')
                    analyze_csv_structure(csv_content, "Sub-Awards")
            else:
                print("\n⚠️ No sub-award files found in ZIP")
                return
    else:
        print("📄 Processing CSV file directly...")
        csv_content = response.text
        analyze_csv_structure(csv_content, "CSV File")

def analyze_csv_structure(csv_content: str, file_type: str = "CSV") -> None:
    """Analyze CSV structure for awards or sub-awards"""
    print(f"\n📊 Analyzing {file_type} CSV structure...")
    print("=" * 80)
    
    lines = csv_content.split('\n')
    print(f"📏 Total lines: {len(lines):,}")
    
    if len(lines) < 2:
        print("⚠️ CSV appears empty or has no data rows")
        return
    
    # Read header
    reader = csv.DictReader(StringIO(csv_content))
    headers = reader.fieldnames
    print(f"\n📋 CSV Headers ({len(headers)} columns):")
    for i, header in enumerate(headers, 1):
        print(f"   {i:3d}. {header}")
    
    # Check for sub-award related columns
    subaward_columns = [h for h in headers if 'sub' in h.lower() or 'parent' in h.lower()]
    if subaward_columns:
        print(f"\n🔍 Sub-award related columns found:")
        for col in subaward_columns:
            print(f"   - {col}")
    else:
        print(f"\n⚠️ No obvious sub-award related columns found")
    
    # Sample first few rows
    print(f"\n📝 Sample rows (first 5):")
    print("=" * 80)
    
    row_count = 0
    award_ids = set()
    parent_award_ids = set()
    subaward_rows = 0
    is_subaward_file = file_type == "Sub-Awards"
    
    for row in reader:
        row_count += 1
        
        # Get award ID (for sub-awards, this is the sub-award ID)
        award_id = (
            row.get('subaward_id') or
            row.get('contract_award_unique_key') or
            row.get('generated_unique_award_id') or
            row.get('award_id') or
            None
        )
        
        # Check for parent award ID (links sub-award to prime award)
        parent_award_id = (
            row.get('prime_award_id') or
            row.get('parent_award_id') or
            row.get('prime_award_unique_key') or
            row.get('parent_award_unique_key') or
            row.get('award_id_fain') or  # For assistance awards
            row.get('award_id_piid') or  # For contract awards
            None
        )
        
        if award_id:
            award_ids.add(award_id)
        
        if parent_award_id:
            parent_award_ids.add(parent_award_id)
        
        # For sub-award files, all rows are sub-awards
        if is_subaward_file:
            subaward_rows = row_count
        elif parent_award_id:
            subaward_rows += 1
        
        # Print first 5 rows with key fields
        if row_count <= 5:
            print(f"\nRow {row_count}:")
            print(f"  Sub-Award ID: {award_id}")
            print(f"  Parent/Prime Award ID: {parent_award_id}")
            print(f"  Award Type: {row.get('award_type', 'N/A')}")
            print(f"  Subaward Type: {row.get('subaward_type', 'N/A')}")
            print(f"  Subaward Amount: {row.get('subaward_amount', 'N/A')}")
            print(f"  Subaward Date: {row.get('subaward_date', 'N/A')}")
            print(f"  Description: {row.get('subaward_description', row.get('transaction_description', 'N/A'))[:100]}")
            
            # Show any columns with 'sub', 'parent', 'prime' in name
            for col in subaward_columns:
                if row.get(col):
                    print(f"  {col}: {row.get(col)}")
            
            # Show all non-empty columns for first row to understand structure
            if row_count == 1:
                print(f"\n  All non-empty columns in first row:")
                for key, value in row.items():
                    if value and value.strip():
                        print(f"    {key}: {str(value)[:80]}")
        
        # Limit analysis to first 1000 rows for speed
        if row_count >= 1000:
            break
    
    print("\n" + "=" * 80)
    print("📊 Analysis Summary:")
    print("=" * 80)
    print(f"  📏 Rows analyzed: {row_count:,}")
    print(f"  🆔 Unique award IDs: {len(award_ids):,}")
    print(f"  🔗 Rows with parent award ID: {subaward_rows:,}")
    print(f"  📦 Unique parent award IDs: {len(parent_award_ids):,}")
    
    if file_type == "Sub-Awards":
        # For sub-award files, all rows are sub-awards
        print(f"\n✅ SUB-AWARDS FILE ANALYZED!")
        print(f"   - {row_count:,} sub-award rows found")
        print(f"   - Sub-awards linked to {len(parent_award_ids):,} unique parent awards")
        if len(parent_award_ids) > 0:
            avg_subs = row_count / len(parent_award_ids)
            print(f"   - Average sub-awards per parent: {avg_subs:.2f}")
        print(f"\n💡 Recommendation: Parse sub-award CSV files separately and group by parent_award_id")
        print(f"   Then link sub-awards to their parent awards during indexing")
    elif subaward_rows > 0:
        print(f"\n✅ SUB-AWARDS DETECTED IN PRIME CSV!")
        print(f"   - {subaward_rows:,} rows appear to be sub-awards")
        print(f"   - Sub-awards are linked to {len(parent_award_ids):,} parent awards")
        print(f"\n💡 Recommendation: Parse sub-awards from CSV by grouping rows with parent_award_id")
    else:
        print(f"\n✅ PRIME AWARDS FILE ANALYZED")
        print(f"   - {row_count:,} prime award transaction rows")
        print(f"   - {len(award_ids):,} unique prime awards")
    
    # Note: Multiple CSV file handling is now done in download_and_analyze_csv

def main():
    """Main test function"""
    try:
        # Step 1: Initiate download
        download_info = initiate_bulk_download(
            start_date.strftime('%Y-%m-%d'),
            end_date.strftime('%Y-%m-%d')
        )
        file_name = download_info['file_name']
        
        # Step 2: Poll for completion
        status_info = poll_download_status(file_name)
        file_url = status_info['file_url']
        
        # Step 3: Download and analyze
        download_and_analyze_csv(file_url)
        
        print("\n" + "=" * 80)
        print("✅ Test Complete!")
        print("=" * 80)
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    main()


