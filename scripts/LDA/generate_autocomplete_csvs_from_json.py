"""
Generate autocomplete CSV files from JSON constants files
Reads the JSON files from the indexer app folder and creates single-column CSVs with names only
"""

import json
import csv
import os
from pathlib import Path

# Paths to JSON files in the indexer app folder
SCRIPT_DIR = Path(__file__).parent
INDEXER_APP_DIR = SCRIPT_DIR.parent.parent / "backend_app" / "src" / "LDA" / "lda_disclosures_indexer" / "app"

JSON_FILES = {
    "general_issues": INDEXER_APP_DIR / "general_issues_constants.json",
    "government_entities": INDEXER_APP_DIR / "government_entities_constants.json",
    "countries": INDEXER_APP_DIR / "countries_constants.json",
}

OUTPUT_DIR = SCRIPT_DIR / "generated_csvs"
OUTPUT_DIR.mkdir(exist_ok=True)


def generate_csv_from_json(json_path: Path, output_path: Path, name_field: str = "name"):
    """
    Generate a single-column CSV file with names from a JSON constants file
    
    Args:
        json_path: Path to the JSON file
        output_path: Path where the CSV should be saved
        name_field: Field name in JSON that contains the name (default: "name")
    """
    print(f"\n📖 Reading {json_path}...")
    
    if not json_path.exists():
        print(f"❌ File not found: {json_path}")
        return False
    
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        if not isinstance(data, list):
            print(f"❌ Expected JSON array, got {type(data)}")
            return False
        
        # Extract names
        names = []
        for item in data:
            if isinstance(item, dict):
                name = item.get(name_field, '')
                if name and str(name).strip():
                    names.append(str(name).strip())
        
        if not names:
            print(f"⚠️  No names found in {json_path}")
            return False
        
        # Sort names alphabetically
        names.sort(key=str.lower)
        
        # Write CSV
        print(f"📝 Writing {len(names)} names to {output_path}...")
        with open(output_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['value'])  # Header row
            for name in names:
                writer.writerow([name])
        
        print(f"✅ Successfully generated {output_path}")
        print(f"   Sample names (first 5): {names[:5]}")
        return True
        
    except json.JSONDecodeError as e:
        print(f"❌ JSON decode error: {e}")
        return False
    except Exception as e:
        print(f"❌ Error processing {json_path}: {e}")
        return False


def main():
    """Generate CSV files from JSON constants"""
    print("=" * 60)
    print("🚀 Generating Autocomplete CSV Files from JSON Constants")
    print("=" * 60)
    
    results = {}
    
    # Generate CSVs for each JSON file
    for constant_type, json_path in JSON_FILES.items():
        output_filename = f"{constant_type}.csv"
        output_path = OUTPUT_DIR / output_filename
        
        success = generate_csv_from_json(json_path, output_path)
        results[constant_type] = {
            'success': success,
            'output_path': output_path if success else None
        }
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 Summary")
    print("=" * 60)
    
    for constant_type, result in results.items():
        if result['success']:
            print(f"✅ {constant_type}: {result['output_path']}")
        else:
            print(f"❌ {constant_type}: Failed")
    
    print(f"\n📁 All CSV files saved to: {OUTPUT_DIR}")
    print("\n💡 Next steps:")
    print("   1. Review the generated CSV files")
    print("   2. Upload them to S3 bucket under 'lists/' prefix:")
    print("      - lists/general_issues.csv")
    print("      - lists/government_entities.csv")
    print("      - lists/countries.csv")
    print("   3. The autocomplete Lambda will read from these files")


if __name__ == "__main__":
    main()









