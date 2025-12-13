"""
Test script to download bill text for a specific bill.
Tests the Congress.gov API bill text endpoint and downloads the XML/HTML file.

Usage:
    python test_bill_text_download.py

You will be prompted to enter your Congress.gov API key.
"""

import sys
import json
import requests
import os
from pathlib import Path

# Configuration
API_BASE_URL = "https://api.congress.gov/v3"
CONGRESS = 119
BILL_TYPE = "S"
BILL_NUMBER = 1670

# Get script directory (where this file is located)
SCRIPT_DIR = Path(__file__).parent

def get_api_key():
    """Get API key from user input"""
    api_key = input("Enter your Congress.gov API key: ").strip()
    if not api_key:
        raise ValueError("API key is required")
    return api_key

def fetch_bill_text_versions(congress: int, bill_type: str, bill_number: int, api_key: str):
    """Fetch all text versions available for a bill."""
    url = f"{API_BASE_URL}/bill/{congress}/{bill_type.lower()}/{bill_number}/text"
    params = {
        "format": "json",
        "api_key": api_key
    }
    
    print(f"📡 Fetching text versions from: {url}")
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()
    
    # Handle different response structures
    text_versions = []
    if isinstance(data, list):
        text_versions = data
    elif "textVersions" in data:
        versions_data = data["textVersions"]
        if isinstance(versions_data, dict):
            text_versions = versions_data.get("item", [])
        elif isinstance(versions_data, list):
            text_versions = versions_data
    
    return text_versions if isinstance(text_versions, list) else []

def download_bill_text_file(text_url: str, output_path: Path):
    """Download bill text file (XML/HTML) from Congress.gov."""
    print(f"📥 Downloading from: {text_url}")
    response = requests.get(text_url, timeout=60)
    response.raise_for_status()
    
    content = response.content
    
    # Determine file extension and content type based on content (similar to SEC logic)
    # Check first 1000 bytes for content type indicators
    content_start = content[:1000] if len(content) >= 1000 else content
    content_start_lower = content_start.lower()
    
    # Check for XML indicators first (XML declaration takes precedence)
    is_xml = (content.startswith(b'<?xml') or 
              b'<?xml version' in content_start_lower or
              b'<bill' in content_start_lower or
              b'<legis-body' in content_start_lower or
              b'<!DOCTYPE bill' in content_start_lower)
    
    # Check for HTML indicators (but XML takes precedence)
    is_html = (not is_xml and (
        b'<!doctype html' in content_start_lower or
        b'<html' in content_start_lower or
        b'<head>' in content_start_lower or
        b'<body>' in content_start_lower
    ))
    
    # Determine content type for display
    if is_xml:
        content_type = 'application/xml'
    elif is_html:
        content_type = 'text/html'
    else:
        content_type = 'application/xml'
    
    # Always save as HTML file
        file_ext = ".html"
    
    # Save to file
    output_file = output_path.with_suffix(file_ext)
    with open(output_file, 'wb') as f:
        f.write(content)
    
    print(f"✅ Downloaded {len(content):,} bytes to: {output_file} ({content_type})")
    return output_file

def main():
    print("=" * 80)
    print("Congress.gov Bill Text Download Test")
    print("=" * 80)
    print(f"Bill: {CONGRESS}-{BILL_TYPE}-{BILL_NUMBER}")
    print("")
    
    # Get API key
    try:
        api_key = get_api_key()
    except ValueError as e:
        print(f"❌ Error: {e}")
        return 1
    
    try:
        # Fetch text versions
        print("\n📋 Fetching available text versions...")
        text_versions = fetch_bill_text_versions(CONGRESS, BILL_TYPE, BILL_NUMBER, api_key)
        
        if not text_versions:
            print("❌ No text versions found for this bill")
            return 1
        
        print(f"✅ Found {len(text_versions)} text version(s):")
        for i, version in enumerate(text_versions, 1):
            version_type = version.get("type", "Unknown")
            date = version.get("date", "Unknown")
            print(f"   {i}. {version_type} (Date: {date})")
        
        # Find the "Introduced" version first, fallback to first available
        introduced_version = None
        for version in text_versions:
            version_type = version.get("type", "").lower()
            if "introduced" in version_type:
                introduced_version = version
                break
        
        selected_version = introduced_version if introduced_version else text_versions[0]
        version_type_name = selected_version.get("type", "unknown")
        
        print(f"\n📄 Selected version: {version_type_name}")
        
        # Get the Formatted Text (HTML) URL (preferred format)
        formats = selected_version.get("formats", {})
        text_url = None
        format_type = None
        
        # Handle different formats structures
        format_items = []
        if isinstance(formats, list):
            # formats is already a list
            format_items = formats
        elif isinstance(formats, dict):
            # formats is a dict, might have "item" key or be the list itself
            if "item" in formats:
                item_data = formats["item"]
                if isinstance(item_data, list):
                    format_items = item_data
                elif isinstance(item_data, dict):
                    # Single item wrapped in dict
                    format_items = [item_data]
            else:
                # Check if dict values are format items
                format_items = list(formats.values()) if formats else []
        
        if format_items:
            # Try Formatted Text (HTML) first
                for fmt_item in format_items:
                if isinstance(fmt_item, dict) and fmt_item.get("type") == "Formatted Text":
                        text_url = fmt_item.get("url")
                    format_type = "Formatted Text"
                        break
                
            # Fallback to other formats if HTML not available
                if not text_url and format_items:
                    fmt_item = format_items[0]
                if isinstance(fmt_item, dict):
                    text_url = fmt_item.get("url")
                    format_type = fmt_item.get("type", "Unknown")
        
        if not text_url:
            print("❌ No text URL found in formats")
            print(f"   Available formats: {json.dumps(formats, indent=2)}")
            return 1
        
        print(f"   Format: {format_type}")
        print(f"   URL: {text_url}")
        
        # Download the file
        print(f"\n💾 Downloading bill text...")
        output_filename = f"bill_{CONGRESS}_{BILL_TYPE}_{BILL_NUMBER}_{version_type_name.lower().replace(' ', '_')}"
        output_path = SCRIPT_DIR / output_filename
        
        downloaded_file = download_bill_text_file(text_url, output_path)
        
        print("")
        print("=" * 80)
        print("✅ Download completed successfully!")
        print(f"   File saved to: {downloaded_file}")
        print("=" * 80)
        
        return 0
        
    except requests.exceptions.RequestException as e:
        print(f"❌ HTTP Error: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"   Response status: {e.response.status_code}")
            print(f"   Response body: {e.response.text[:500]}")
        return 1
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)

