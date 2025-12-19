"""
Script to parse registrant column and contribution_items from LDA CSV export.
Analyzes if client data can be extracted from registrant, and parses contribution amounts.
"""

import csv
import json
import ast
from typing import Dict, List, Any, Optional
from pathlib import Path
from decimal import Decimal

def parse_dynamodb_native_format(value: Any) -> Any:
    """Parse DynamoDB native format (M=Map, S=String, N=Number, L=List, etc.)"""
    if isinstance(value, dict):
        # Check for DynamoDB type markers
        if 'M' in value:
            # Map (object)
            result = {}
            for k, v in value['M'].items():
                result[k] = parse_dynamodb_native_format(v)
            return result
        elif 'L' in value:
            # List
            return [parse_dynamodb_native_format(item) for item in value['L']]
        elif 'S' in value:
            # String
            return value['S']
        elif 'N' in value:
            # Number (return as string, caller can convert)
            return value['N']
        elif 'BOOL' in value:
            # Boolean
            return value['BOOL']
        elif 'NULL' in value:
            # Null
            return None
        else:
            # Regular dict, recurse
            return {k: parse_dynamodb_native_format(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [parse_dynamodb_native_format(item) for item in value]
    else:
        return value

def parse_dynamodb_value(value_str: str) -> Any:
    """Parse DynamoDB-exported value (handles nested JSON strings and native format)"""
    if not value_str or value_str == 'null' or value_str == '':
        return None
    
    # Try to parse as JSON first (for nested objects)
    try:
        parsed = json.loads(value_str)
        # Check if it's in DynamoDB native format
        if isinstance(parsed, (dict, list)):
            return parse_dynamodb_native_format(parsed)
        return parsed
    except (json.JSONDecodeError, ValueError):
        pass
    
    # Try to parse as Python literal (for lists, dicts)
    try:
        parsed = ast.literal_eval(value_str)
        if isinstance(parsed, (dict, list)):
            return parse_dynamodb_native_format(parsed)
        return parsed
    except (ValueError, SyntaxError):
        pass
    
    # Return as string
    return value_str

def read_csv_file(csv_path: str) -> List[Dict]:
    """Read CSV file and parse DynamoDB-exported values"""
    records = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Parse all values
            parsed_row = {}
            for key, value in row.items():
                parsed_row[key] = parse_dynamodb_value(value)
            records.append(parsed_row)
    return records

def analyze_registrant_column(records: List[Dict]) -> Dict[str, Any]:
    """Analyze registrant column to see if client data can be extracted"""
    analysis = {
        'total_records': len(records),
        'records_with_registrant': 0,
        'registrant_structure': {},
        'potential_client_fields': [],
        'sample_registrants': [],
    }
    
    for i, record in enumerate(records):
        registrant = record.get('registrant')
        
        if registrant and isinstance(registrant, dict):
            analysis['records_with_registrant'] += 1
            
            # Analyze structure
            for key, value in registrant.items():
                if key not in analysis['registrant_structure']:
                    analysis['registrant_structure'][key] = {
                        'type': type(value).__name__,
                        'sample_value': value,
                        'count': 0,
                    }
                analysis['registrant_structure'][key]['count'] += 1
            
            # Check if registrant has any client-related fields
            client_related_keys = [k for k in registrant.keys() if 'client' in k.lower()]
            if client_related_keys:
                analysis['potential_client_fields'].extend(client_related_keys)
            
            # Collect samples
            if len(analysis['sample_registrants']) < 5:
                analysis['sample_registrants'].append({
                    'record_index': i,
                    'record_type': 'FILING' if record.get('PK', '').startswith('FILING#') else 'CONTRIBUTION',
                    'registrant': registrant,
                    'has_client_column': record.get('client') is not None,
                    'client_in_record': record.get('client'),
                })
    
    # Remove duplicates from potential_client_fields
    analysis['potential_client_fields'] = list(set(analysis['potential_client_fields']))
    
    return analysis

def parse_contribution_items(records: List[Dict]) -> Dict[str, Any]:
    """Parse contribution_items to extract amounts and other details"""
    analysis = {
        'total_contributions': 0,
        'records_with_items': 0,
        'records_without_items': 0,
        'total_amount_calculated': Decimal('0'),
        'item_structure': {},
        'amount_statistics': {
            'min': None,
            'max': None,
            'total_items': 0,
        },
        'sample_items': [],
        'comparison_with_total': [],
    }
    
    for i, record in enumerate(records):
        if not record.get('PK', '').startswith('CONTRIBUTION#'):
            continue
        
        analysis['total_contributions'] += 1
        
        contribution_items = record.get('contribution_items')
        total_contribution_amount = record.get('total_contribution_amount')
        
        if contribution_items and isinstance(contribution_items, list) and len(contribution_items) > 0:
            analysis['records_with_items'] += 1
            
            record_total = Decimal('0')
            item_count = 0
            
            for item in contribution_items:
                # Handle DynamoDB native format: { "M": { ... } }
                if isinstance(item, dict) and 'M' in item:
                    item = item['M']
                
                if isinstance(item, dict):
                    # Analyze item structure
                    for key, value in item.items():
                        # Extract actual value if in DynamoDB format
                        actual_value = value
                        if isinstance(value, dict):
                            if 'S' in value:
                                actual_value = value['S']
                            elif 'N' in value:
                                actual_value = value['N']
                            elif 'BOOL' in value:
                                actual_value = value['BOOL']
                        
                        if key not in analysis['item_structure']:
                            analysis['item_structure'][key] = {
                                'type': type(actual_value).__name__,
                                'sample_value': actual_value,
                                'count': 0,
                            }
                        analysis['item_structure'][key]['count'] += 1
                    
                    # Extract amount (handle DynamoDB format)
                    amount_value = item.get('amount')
                    amount_str = None
                    
                    if amount_value:
                        # If it's a DynamoDB format string, extract the value
                        if isinstance(amount_value, dict):
                            if 'S' in amount_value:
                                amount_str = amount_value['S']
                            elif 'N' in amount_value:
                                amount_str = amount_value['N']
                        else:
                            amount_str = str(amount_value)
                        
                        if amount_str:
                            try:
                                amount = Decimal(str(amount_str))
                                record_total += amount
                                item_count += 1
                                analysis['amount_statistics']['total_items'] += 1
                                
                                # Update min/max
                                if analysis['amount_statistics']['min'] is None or amount < analysis['amount_statistics']['min']:
                                    analysis['amount_statistics']['min'] = amount
                                if analysis['amount_statistics']['max'] is None or amount > analysis['amount_statistics']['max']:
                                    analysis['amount_statistics']['max'] = amount
                            except (ValueError, TypeError) as e:
                                print(f"      ⚠️  Could not parse amount '{amount_str}': {e}")
                    
                    # Collect samples
                    if len(analysis['sample_items']) < 10:
                        parsed_amount = None
                        if amount_str:
                            try:
                                parsed_amount = Decimal(str(amount_str))
                            except (ValueError, TypeError):
                                pass
                        
                        analysis['sample_items'].append({
                            'record_index': i,
                            'item': item,
                            'parsed_amount': parsed_amount,
                            'raw_amount': amount_value,
                        })
            
            analysis['total_amount_calculated'] += record_total
            
            # Compare with stored total_contribution_amount
            if total_contribution_amount is not None:
                try:
                    stored_total = Decimal(str(total_contribution_amount))
                    if record_total != stored_total:
                        analysis['comparison_with_total'].append({
                            'record_index': i,
                            'calculated_total': float(record_total),
                            'stored_total': float(stored_total),
                            'difference': float(record_total - stored_total),
                        })
                except (ValueError, TypeError):
                    pass
        else:
            analysis['records_without_items'] += 1
            # Check if no_contributions flag is set
            if record.get('no_contributions'):
                analysis['records_without_items'] -= 1  # This is expected
    
    return analysis

def print_registrant_analysis(analysis: Dict[str, Any]):
    """Print registrant column analysis"""
    print("=" * 80)
    print("REGISTRANT COLUMN ANALYSIS")
    print("=" * 80)
    
    print(f"\n📊 Summary:")
    print(f"   Total Records: {analysis['total_records']}")
    print(f"   Records with Registrant: {analysis['records_with_registrant']}")
    
    print(f"\n🏗️  Registrant Structure:")
    for key, info in sorted(analysis['registrant_structure'].items()):
        print(f"   {key}:")
        print(f"      Type: {info['type']}")
        print(f"      Present in: {info['count']} records")
        if isinstance(info['sample_value'], (str, int, float, bool)) and len(str(info['sample_value'])) < 100:
            print(f"      Sample: {info['sample_value']}")
    
    if analysis['potential_client_fields']:
        print(f"\n🔍 Potential Client-Related Fields in Registrant:")
        for field in analysis['potential_client_fields']:
            print(f"   - {field}")
    else:
        print(f"\n⚠️  No client-related fields found in registrant structure")
    
    print(f"\n📋 Sample Registrants:")
    for sample in analysis['sample_registrants']:
        print(f"\n   Record {sample['record_index']} ({sample['record_type']}):")
        print(f"      Has separate client column: {sample['has_client_column']}")
        if sample['has_client_column']:
            client = sample['client_in_record']
            if isinstance(client, dict):
                print(f"      Client name: {client.get('name', 'N/A')}")
                print(f"      Client ID: {client.get('id', 'N/A')}")
        print(f"      Registrant keys: {list(sample['registrant'].keys())}")
        if 'name' in sample['registrant']:
            print(f"      Registrant name: {sample['registrant'].get('name', 'N/A')}")

def print_contribution_analysis(analysis: Dict[str, Any]):
    """Print contribution items analysis"""
    print("\n" + "=" * 80)
    print("CONTRIBUTION ITEMS ANALYSIS")
    print("=" * 80)
    
    print(f"\n📊 Summary:")
    print(f"   Total Contributions: {analysis['total_contributions']}")
    print(f"   Records with Items: {analysis['records_with_items']}")
    print(f"   Records without Items: {analysis['records_without_items']}")
    print(f"   Total Items Parsed: {analysis['amount_statistics']['total_items']}")
    print(f"   Total Amount Calculated: ${analysis['total_amount_calculated']:,.2f}")
    
    if analysis['amount_statistics']['min'] is not None:
        print(f"\n💰 Amount Statistics:")
        print(f"   Min Amount: ${analysis['amount_statistics']['min']:,.2f}")
        print(f"   Max Amount: ${analysis['amount_statistics']['max']:,.2f}")
        print(f"   Average: ${analysis['total_amount_calculated'] / analysis['amount_statistics']['total_items']:,.2f}" if analysis['amount_statistics']['total_items'] > 0 else "   Average: N/A")
    
    print(f"\n🏗️  Contribution Item Structure:")
    for key, info in sorted(analysis['item_structure'].items()):
        print(f"   {key}:")
        print(f"      Type: {info['type']}")
        print(f"      Present in: {info['count']} items")
        if isinstance(info['sample_value'], (str, int, float, bool)) and len(str(info['sample_value'])) < 100:
            print(f"      Sample: {info['sample_value']}")
    
    if analysis['comparison_with_total']:
        print(f"\n⚠️  Mismatches with stored total_contribution_amount:")
        for mismatch in analysis['comparison_with_total'][:5]:
            print(f"   Record {mismatch['record_index']}:")
            print(f"      Calculated: ${mismatch['calculated_total']:,.2f}")
            print(f"      Stored: ${mismatch['stored_total']:,.2f}")
            print(f"      Difference: ${mismatch['difference']:,.2f}")
    else:
        print(f"\n✅ All calculated totals match stored total_contribution_amount")
    
    print(f"\n📋 Sample Contribution Items:")
    for sample in analysis['sample_items'][:5]:
        print(f"\n   Record {sample['record_index']}:")
        item = sample['item']
        
        # Extract values (handle DynamoDB format)
        def get_value(key, default='N/A'):
            val = item.get(key, default)
            if isinstance(val, dict):
                if 'S' in val:
                    return val['S']
                elif 'N' in val:
                    return val['N']
                elif 'BOOL' in val:
                    return val['BOOL']
            return val if val != default else default
        
        print(f"      Amount: ${sample['parsed_amount']:,.2f}" if sample['parsed_amount'] else f"      Amount: N/A (raw: {sample.get('raw_amount', 'N/A')})")
        print(f"      Type: {get_value('contribution_type_display')}")
        print(f"      Contributor: {get_value('contributor_name')}")
        print(f"      Payee: {get_value('payee_name')}")
        print(f"      Honoree: {get_value('honoree_name')}")
        print(f"      Date: {get_value('date')}")

def main():
    """Main execution"""
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python parse_registrant_and_contributions.py <csv_file_path>")
        print("Example: python parse_registrant_and_contributions.py ../selected\\(6\\).csv")
        sys.exit(1)
    
    csv_path = sys.argv[1]
    
    if not Path(csv_path).exists():
        print(f"❌ Error: CSV file not found: {csv_path}")
        sys.exit(1)
    
    print(f"📖 Reading CSV file: {csv_path}")
    try:
        records = read_csv_file(csv_path)
        print(f"✅ Loaded {len(records)} records")
    except Exception as e:
        print(f"❌ Error reading CSV: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    print(f"\n🔍 Analyzing registrant column...")
    registrant_analysis = analyze_registrant_column(records)
    print_registrant_analysis(registrant_analysis)
    
    print(f"\n🔍 Analyzing contribution items...")
    contribution_analysis = parse_contribution_items(records)
    print_contribution_analysis(contribution_analysis)
    
    print("\n" + "=" * 80)
    print("Analysis Complete")
    print("=" * 80)

if __name__ == "__main__":
    main()

