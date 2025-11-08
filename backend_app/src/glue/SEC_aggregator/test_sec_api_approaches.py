"""
Test script to explore alternative SEC API approaches for fetching Forms 3, 4, 5.

This script tests:
1. SEC Daily Index Files (recommended for date-based fetching)
2. SEC Submissions API (data.sec.gov/submissions/CIK...json)
3. Compare with current browse-edgar approach
4. Downloads a sample form using the downloader logic

The daily index files are the recommended approach for fetching all forms filed on a specific date.
"""

import os
import re
import requests
import json
from datetime import datetime
from typing import List, Dict, Any, Optional
from html import unescape

# Import download logic from test_downloader_logic.py
import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from test_downloader_logic import download_sec_form_test

# SEC User Agent requirement
SEC_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 (Cosine Financial Platform; contact@cosine.financial)"

SEC_BASE_URL = "https://www.sec.gov"
SEC_DATA_BASE_URL = "https://data.sec.gov"

def get_session():
    """Create a requests session with proper SEC headers"""
    session = requests.Session()
    session.headers.update({
        'User-Agent': SEC_USER_AGENT,
        'Accept': 'application/json, text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Referer': 'https://www.sec.gov/',
    })
    return session


def test_daily_index_files(target_date: str = "2025-11-06") -> List[Dict[str, Any]]:
    """
    Test fetching forms from SEC Daily Index Files.
    
    Daily index files are available at:
    https://www.sec.gov/Archives/edgar/daily-index/{YEAR}/QTR{QUARTER}/master.{YYYYMMDD}.idx
    
    These files contain all filings for a specific date, including Forms 3, 4, 5.
    
    Args:
        target_date: Date in YYYY-MM-DD format
        
    Returns:
        List of form metadata dicts
    """
    print("=" * 80)
    print("TEST 1: SEC Daily Index Files")
    print("=" * 80)
    
    session = get_session()
    forms = []
    
    try:
        # Parse date
        date_obj = datetime.strptime(target_date, '%Y-%m-%d')
        year = date_obj.year
        quarter = (date_obj.month - 1) // 3 + 1
        date_str = date_obj.strftime('%Y%m%d')
        
        # Daily index file URL
        index_url = f"{SEC_BASE_URL}/Archives/edgar/daily-index/{year}/QTR{quarter}/master.{date_str}.idx"
        
        print(f"📅 Target Date: {target_date}")
        print(f"🔗 Index File URL: {index_url}")
        print()
        
        # Fetch the daily index file
        print("📥 Fetching daily index file...")
        response = session.get(index_url, timeout=30)
        
        print(f"   Status Code: {response.status_code}")
        print(f"   Response Size: {len(response.content):,} bytes")
        
        if response.status_code != 200:
            print(f"❌ Failed to fetch index file: {response.status_code}")
            if response.status_code == 404:
                print("   Note: Index file may not exist for this date (weekend/holiday)")
            return forms
        
        # Parse the index file
        # Format: CIK|Company Name|Form Type|Date Filed|File Name
        # Lines starting with "CIK" are headers, data lines follow
        content = response.text
        lines = content.split('\n')
        
        print(f"   Total lines in index: {len(lines)}")
        print()
        
        # Find the header line and data start
        header_found = False
        data_start_idx = 0
        
        for idx, line in enumerate(lines):
            if line.startswith('CIK|'):
                header_found = True
                data_start_idx = idx + 1
                print(f"📋 Header found at line {idx + 1}: {line[:100]}")
                break
        
        if not header_found:
            print("⚠️ No header line found in index file")
            return forms
        
        # Parse data lines
        print(f"📊 Parsing data lines (starting at line {data_start_idx + 1})...")
        print()
        
        form_types = ['3', '4', '5']
        forms_found = 0
        
        for line_num, line in enumerate(lines[data_start_idx:], start=data_start_idx + 1):
            if not line.strip():
                continue
            
            # Parse pipe-delimited format: CIK|Company Name|Form Type|Date Filed|File Name
            parts = line.split('|')
            if len(parts) < 5:
                continue
            
            try:
                cik = parts[0].strip()
                company_name = parts[1].strip()
                form_type_raw = parts[2].strip()
                date_filed = parts[3].strip()
                filename = parts[4].strip()
                
                # Check if this is a Form 3, 4, or 5
                form_match = re.search(r'(\d+)', form_type_raw)
                if form_match:
                    form_num = form_match.group(1)
                    if form_num in form_types:
                        # Check if date matches
                        if date_filed == target_date.replace('-', ''):
                            # Extract accession number from filename
                            # Format: {accession}-{form_type}.txt or {accession}-index.htm
                            accession_match = re.search(r'(\d{10}-\d{2}-\d{6})', filename)
                            accession = accession_match.group(1) if accession_match else None
                            
                            if accession:
                                form_data = {
                                    'cik': cik,
                                    'accession_number': accession.replace('-', ''),  # Store without dashes (matching downloader input format)
                                    'accession_number_dashed': accession,  # Keep dashed version for display
                                    'form_type': f'form{form_num}',
                                    'filing_date': target_date,
                                    'company_name': company_name,
                                    'filename': filename
                                }
                                forms.append(form_data)
                                forms_found += 1
                                
                                if forms_found <= 5:  # Show first 5
                                    print(f"   ✅ Form {form_num}: CIK={cik}, Company={company_name[:50]}, Accession={accession}")
            except Exception as e:
                continue
        
        print()
        print(f"✅ Found {len(forms)} Forms 3/4/5 for {target_date}")
        print(f"   Breakdown:")
        form_counts = {}
        for form in forms:
            form_type = form['form_type']
            form_counts[form_type] = form_counts.get(form_type, 0) + 1
        for form_type, count in sorted(form_counts.items()):
            print(f"      - {form_type}: {count}")
        
    except Exception as e:
        print(f"❌ Error fetching daily index: {e}")
        import traceback
        traceback.print_exc()
    
    return forms


def test_submissions_api(cik: str = "0001001250") -> List[Dict[str, Any]]:
    """
    Test fetching forms from SEC Submissions API (data.sec.gov).
    
    This API provides JSON data for a specific CIK's filing history.
    Note: This requires knowing the CIK in advance, so it's better for
    checking specific companies rather than finding all forms for a date.
    
    Args:
        cik: CIK number (with or without leading zeros)
        
    Returns:
        List of form metadata dicts
    """
    print("=" * 80)
    print("TEST 2: SEC Submissions API (data.sec.gov)")
    print("=" * 80)
    
    session = get_session()
    forms = []
    
    try:
        # Normalize CIK (pad with zeros to 10 digits)
        cik_clean = cik.lstrip('0')
        cik_padded = cik_clean.zfill(10)
        
        # Submissions API URL
        api_url = f"{SEC_DATA_BASE_URL}/submissions/CIK{cik_padded}.json"
        
        print(f"🔍 CIK: {cik} (normalized: {cik_padded})")
        print(f"🔗 API URL: {api_url}")
        print()
        
        # Fetch the submissions data
        print("📥 Fetching submissions data...")
        response = session.get(api_url, timeout=30)
        
        print(f"   Status Code: {response.status_code}")
        
        if response.status_code != 200:
            print(f"❌ Failed to fetch submissions: {response.status_code}")
            print(f"   Response: {response.text[:500]}")
            return forms
        
        data = response.json()
        
        # Extract company name
        company_name = data.get('name', 'Unknown')
        print(f"   Company: {company_name}")
        print()
        
        # Get recent filings
        recent_filings = data.get('filings', {}).get('recent', {})
        form_types_list = recent_filings.get('form', [])
        filing_dates = recent_filings.get('filingDate', [])
        accession_numbers = recent_filings.get('accessionNumber', [])
        
        print(f"📊 Total filings in recent history: {len(form_types_list)}")
        print()
        
        # Filter for Forms 3, 4, 5
        target_forms = ['3', '4', '5']
        forms_found = 0
        
        print("🔍 Searching for Forms 3, 4, 5...")
        for idx, form_type in enumerate(form_types_list):
            if form_type in target_forms:
                filing_date = filing_dates[idx] if idx < len(filing_dates) else None
                accession = accession_numbers[idx] if idx < len(accession_numbers) else None
                
                if filing_date and accession:
                    form_data = {
                        'cik': cik_padded,
                        'accession_number': accession.replace('-', ''),
                        'form_type': f'form{form_type}',
                        'filing_date': filing_date,
                        'company_name': company_name
                    }
                    forms.append(form_data)
                    forms_found += 1
                    
                    if forms_found <= 10:  # Show first 10
                        print(f"   ✅ Form {form_type}: Date={filing_date}, Accession={accession[:20]}...")
        
        print()
        print(f"✅ Found {len(forms)} Forms 3/4/5 for CIK {cik_padded}")
        
    except Exception as e:
        print(f"❌ Error fetching submissions: {e}")
        import traceback
        traceback.print_exc()
    
    return forms


def test_browse_edgar_comparison(target_date: str = "2025-11-06") -> List[Dict[str, Any]]:
    """
    Test the current browse-edgar approach for comparison.
    
    Args:
        target_date: Date in YYYY-MM-DD format
        
    Returns:
        List of form metadata dicts
    """
    print("=" * 80)
    print("TEST 3: Current browse-edgar Approach (for comparison)")
    print("=" * 80)
    
    session = get_session()
    forms = []
    
    try:
        # Test Form 4 (most common)
        form_type = '4'
        url = f"{SEC_BASE_URL}/cgi-bin/browse-edgar?action=getcurrent&datea=&dateb=&company=&type={form_type}&SIC=&State=&Country=&CIK=&owner=only&accno=&start=0&count=100"
        
        print(f"📅 Target Date: {target_date}")
        print(f"🔗 URL: {url}")
        print()
        
        print("📥 Fetching browse-edgar page...")
        response = session.get(url, timeout=30)
        
        print(f"   Status Code: {response.status_code}")
        print(f"   Response Size: {len(response.content):,} bytes")
        
        if response.status_code == 403:
            print("❌ Got 403 Forbidden - this is the issue we're trying to solve")
            return forms
        
        if response.status_code != 200:
            print(f"❌ Failed: {response.status_code}")
            return forms
        
        # Parse HTML (simplified - just count forms)
        html_content = response.text
        archive_links = re.findall(r'/Archives/edgar/data/(\d+)/([^/"]+)/', html_content, re.IGNORECASE)
        
        print(f"   Found {len(archive_links)} archive links in HTML")
        print()
        print("✅ browse-edgar approach works (if not blocked)")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    
    return forms


def main():
    """Run all tests"""
    print("\n" + "=" * 80)
    print("SEC API APPROACHES TEST SCRIPT")
    print("=" * 80)
    print()
    print("This script tests different approaches to fetch SEC Forms 3, 4, 5")
    print("to find the best alternative to the browse-edgar HTML scraping.")
    print()
    
    target_date = "2025-11-06"
    test_cik = "0001001250"  # Estee Lauder (from the form you showed)
    
    # Test 1: Daily Index Files (RECOMMENDED)
    daily_index_forms = test_daily_index_files(target_date)
    
    print()
    print()
    
    # Test 2: Submissions API (requires CIK)
    submissions_forms = test_submissions_api(test_cik)
    
    print()
    print()
    
    # Test 3: Current approach (for comparison)
    browse_edgar_forms = test_browse_edgar_comparison(target_date)
    
    print()
    print()
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Daily Index Files: {len(daily_index_forms)} forms found")
    print(f"Submissions API (CIK {test_cik}): {len(submissions_forms)} forms found")
    print(f"Browse-edgar: {'Blocked (403)' if not browse_edgar_forms else 'Works'}")
    print()
    print("RECOMMENDATION:")
    if daily_index_forms:
        print("✅ Use Daily Index Files - they provide all forms for a specific date")
        print("   and are less likely to be blocked than browse-edgar HTML scraping.")
    else:
        print("⚠️ Daily Index Files not available for this date (may be weekend/holiday)")
        print("   Try a weekday date or check if the file exists.")
    print()
    
    # Test 4: Download a sample form from daily index files using downloader logic
    if daily_index_forms:
        print("=" * 80)
        print("TEST 4: Download Sample Form from Daily Index Files")
        print("=" * 80)
        print()
        
        # Use the first form found (or you can pick a specific one)
        sample_form = daily_index_forms[0]
        print(f"📥 Downloading sample form:")
        print(f"   CIK: {sample_form['cik']}")
        print(f"   Accession: {sample_form.get('accession_number_dashed', sample_form['accession_number'])}")
        print(f"   Form Type: {sample_form['form_type']}")
        print(f"   Company: {sample_form['company_name']}")
        print(f"   Filing Date: {sample_form['filing_date']}")
        print()
        
        # Prepare form data in the format expected by download_sec_form_test
        # The downloader expects: cik, accessionNumber (without dashes), formType, filingDate
        download_form_data = {
            'cik': sample_form['cik'],
            'accessionNumber': sample_form['accession_number'],  # Without dashes (matching downloader input)
            'formType': sample_form['form_type'],
            'filingDate': sample_form['filing_date'],
            'filename': sample_form.get('filename', '')  # Optional
        }
        
        # Get output directory (same folder as script)
        script_dir = os.path.dirname(os.path.abspath(__file__))
        output_dir = script_dir
        
        try:
            filepath = download_sec_form_test(download_form_data, output_dir=output_dir)
            if filepath:
                print()
                print("=" * 80)
                print(f"✅ SUCCESS: Downloaded form to {filepath}")
                print("=" * 80)
        except Exception as e:
            print()
            print("=" * 80)
            print(f"❌ ERROR: Failed to download form: {e}")
            print("=" * 80)
            import traceback
            traceback.print_exc()
        print()


if __name__ == "__main__":
    main()

