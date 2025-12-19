#!/usr/bin/env python3
"""
Fetch LDA constants from the API and save to separate JSON files.
This includes:
- General Issues (lobbying activity issue codes)
- Government Entities
- Contribution Item Types
- Lobbyist Prefixes
- Lobbyist Suffixes
- Filing Types

Usage:
    python fetch_general_issues_constants.py [--output-dir OUTPUT_DIR]

Each constant type will be saved to a separate file:
- general_issues_constants.json
- government_entities_constants.json
- contribution_item_types_constants.json
- lobbyist_prefixes_constants.json
- lobbyist_suffixes_constants.json
- filing_types_constants.json
"""

import json
import requests
import argparse
import sys
from pathlib import Path
from typing import List, Dict, Tuple

# LDA API Configuration
LDA_API_BASE_URL = "https://lda.senate.gov/api/v1"

# Endpoints for each constant type
ENDPOINTS = {
    "general_issues": f"{LDA_API_BASE_URL}/constants/filing/lobbyingactivityissues/",
    "government_entities": f"{LDA_API_BASE_URL}/constants/filing/governmententities/",
    "contribution_item_types": f"{LDA_API_BASE_URL}/constants/contribution/itemtypes/",
    "lobbyist_prefixes": f"{LDA_API_BASE_URL}/constants/lobbyist/prefixes/",
    "lobbyist_suffixes": f"{LDA_API_BASE_URL}/constants/lobbyist/suffixes/",
    "filing_types": f"{LDA_API_BASE_URL}/constants/filing/filingtypes/"
}

# Output file names
OUTPUT_FILES = {
    "general_issues": "general_issues_constants.json",
    "government_entities": "government_entities_constants.json",
    "contribution_item_types": "contribution_item_types_constants.json",
    "lobbyist_prefixes": "lobbyist_prefixes_constants.json",
    "lobbyist_suffixes": "lobbyist_suffixes_constants.json",
    "filing_types": "filing_types_constants.json"
}

def get_api_key() -> str:
    """Get API key from environment variable or prompt user"""
    import os
    api_key = os.environ.get('LDA_API_KEY')
    if not api_key:
        print("⚠️  LDA_API_KEY environment variable not set.")
        print("   Please set it or enter your API key when prompted.")
        api_key = input("Enter LDA API Key: ").strip()
    return api_key

def fetch_constants(api_key: str, endpoint: str, constant_type: str) -> List[Dict]:
    """Fetch constants from LDA API"""
    headers = {
        "Authorization": f"Token {api_key}",
        "Accept": "application/json"
    }
    
    print(f"📡 Fetching {constant_type} from: {endpoint}")
    
    try:
        response = requests.get(endpoint, headers=headers, timeout=30)
        response.raise_for_status()
        
        constants = response.json()
        print(f"✅ Successfully fetched {len(constants)} {constant_type}")
        return constants
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching {constant_type}: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"   Status Code: {e.response.status_code}")
            print(f"   Response: {e.response.text[:200]}")
        return None

def save_to_file(constants: List[Dict], output_file: Path, constant_type: str):
    """Save constants to JSON file"""
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(constants, f, indent=2, ensure_ascii=False)
    
    print(f"💾 Saved {len(constants)} {constant_type} to: {output_file}")

def print_sample(constants: List[Dict], constant_type: str):
    """Print sample entries from the constants list"""
    if not constants:
        return
    
    print(f"\n📋 Sample {constant_type} entries:")
    
    # Different display formats based on constant type
    if constant_type == "government_entities":
        # Government entities have 'id' and 'name'
        for item in constants[:5]:
            print(f"   ID {item.get('id', 'N/A')}: {item.get('name', 'N/A')}")
    else:
        # General issues and contribution item types have 'name' and 'value'
        for item in constants[:5]:
            print(f"   {item.get('value', 'N/A')}: {item.get('name', 'N/A')}")
    
    if len(constants) > 5:
        print(f"   ... and {len(constants) - 5} more")

def main():
    parser = argparse.ArgumentParser(
        description="Fetch LDA constants (General Issues, Government Entities, Contribution Item Types, Lobbyist Prefixes/Suffixes, Filing Types) for autocomplete"
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        default='.',
        help='Output directory for JSON files (default: current directory)'
    )
    
    args = parser.parse_args()
    output_dir = Path(args.output_dir)
    
    # Get API key
    api_key = get_api_key()
    
    # Fetch and save each constant type
    results = {}
    for constant_type, endpoint in ENDPOINTS.items():
        constants = fetch_constants(api_key, endpoint, constant_type)
        if constants is not None:
            output_file = output_dir / OUTPUT_FILES[constant_type]
            save_to_file(constants, output_file, constant_type)
            print_sample(constants, constant_type)
            results[constant_type] = constants
        print()  # Empty line between sections
    
    # Summary
    print("=" * 60)
    print("📊 Summary:")
    for constant_type, constants in results.items():
        if constants:
            print(f"   {constant_type}: {len(constants)} items → {OUTPUT_FILES[constant_type]}")
    print("=" * 60)

if __name__ == "__main__":
    main()

