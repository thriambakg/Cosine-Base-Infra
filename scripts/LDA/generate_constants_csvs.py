#!/usr/bin/env python3
"""
Generate CSV files from LDA constants JSON files for frontend autocomplete.

This script:
1. Reads JSON files from the constants directory
2. Converts each to CSV format
3. Uploads to S3

Usage:
    python generate_constants_csvs.py [--input-dir INPUT_DIR] [--s3-bucket BUCKET] [--s3-prefix PREFIX]
"""

import json
import csv
import argparse
import boto3
from pathlib import Path
from typing import Dict, List
from io import StringIO

# S3 client
s3_client = boto3.client('s3')

# Constants file mappings
CONSTANTS_FILES = {
    'general_issues': {
        'json': 'general_issues_constants.json',
        'csv': 'general_issues.csv',
        'columns': ['value', 'name']
    },
    'government_entities': {
        'json': 'government_entities_constants.json',
        'csv': 'government_entities.csv',
        'columns': ['id', 'name']
    },
    'contribution_item_types': {
        'json': 'contribution_item_types_constants.json',
        'csv': 'contribution_item_types.csv',
        'columns': ['value', 'name']
    },
    'filing_types': {
        'json': 'filing_types_constants.json',
        'csv': 'filing_types.csv',
        'columns': ['value', 'name']
    },
    'countries': {
        'json': 'countries_constants.json',
        'csv': 'countries.csv',
        'columns': ['value', 'name']
    }
}

def json_to_csv(data: List[Dict], columns: List[str]) -> str:
    """Convert JSON data to CSV string"""
    if not data:
        return ''
    
    csv_buffer = StringIO()
    writer = csv.DictWriter(csv_buffer, fieldnames=columns)
    writer.writeheader()
    
    for item in data:
        row = {col: item.get(col, '') for col in columns}
        writer.writerow(row)
    
    return csv_buffer.getvalue()

def process_constants_file(input_dir: Path, constant_name: str, s3_bucket: str, s3_prefix: str):
    """Process a single constants file"""
    config = CONSTANTS_FILES[constant_name]
    json_file = input_dir / config['json']
    csv_filename = config['csv']
    s3_key = f"{s3_prefix}/constants/{csv_filename}"
    
    if not json_file.exists():
        print(f"⚠️  {json_file} not found, skipping")
        return False
    
    try:
        # Read JSON
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Convert to CSV
        csv_content = json_to_csv(data, config['columns'])
        
        # Upload to S3
        if s3_bucket:
            s3_client.put_object(
                Bucket=s3_bucket,
                Key=s3_key,
                Body=csv_content.encode('utf-8'),
                ContentType='text/csv',
                CacheControl='max-age=3600'  # Cache for 1 hour
            )
            print(f"✅ Generated and uploaded {csv_filename} ({len(data)} rows) → s3://{s3_bucket}/{s3_key}")
        else:
            # Save locally
            csv_file = input_dir / csv_filename
            with open(csv_file, 'w', encoding='utf-8') as f:
                f.write(csv_content)
            print(f"✅ Generated {csv_filename} ({len(data)} rows) → {csv_file}")
        
        return True
    except Exception as e:
        print(f"❌ Error processing {constant_name}: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(
        description="Generate CSV files from LDA constants JSON files"
    )
    parser.add_argument(
        '--input-dir',
        type=str,
        default='.',
        help='Directory containing JSON files (default: current directory)'
    )
    parser.add_argument(
        '--s3-bucket',
        type=str,
        default=None,
        help='S3 bucket to upload CSVs (optional, if not provided saves locally)'
    )
    parser.add_argument(
        '--s3-prefix',
        type=str,
        default='lda-autocomplete',
        help='S3 key prefix (default: lda-autocomplete)'
    )
    
    args = parser.parse_args()
    input_dir = Path(args.input_dir)
    
    print("=" * 60)
    print("📊 Generating CSV files from LDA constants")
    print("=" * 60)
    
    results = {}
    for constant_name in CONSTANTS_FILES.keys():
        success = process_constants_file(
            input_dir,
            constant_name,
            args.s3_bucket,
            args.s3_prefix
        )
        results[constant_name] = success
    
    print("\n" + "=" * 60)
    print("📊 Summary:")
    for constant_name, success in results.items():
        status = "✅" if success else "❌"
        print(f"   {status} {constant_name}")
    print("=" * 60)

if __name__ == "__main__":
    main()

