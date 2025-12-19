"""
Analyze CSV to understand relationships between registrant, lobbyist, client, and contribution_items.
Answers:
1. Are registrant and lobbyist the same thing?
2. Do contribution_items contain client information?
3. What fields can be merged/unified?
"""

import csv
import json
import ast
from typing import Dict, List, Any, Set
from pathlib import Path
from collections import defaultdict

def parse_dynamodb_native_format(value: Any) -> Any:
    """Parse DynamoDB native format"""
    if isinstance(value, dict):
        if 'M' in value:
            result = {}
            for k, v in value['M'].items():
                result[k] = parse_dynamodb_native_format(v)
            return result
        elif 'L' in value:
            return [parse_dynamodb_native_format(item) for item in value['L']]
        elif 'S' in value:
            return value['S']
        elif 'N' in value:
            return value['N']
        elif 'BOOL' in value:
            return value['BOOL']
        elif 'NULL' in value:
            return None
        else:
            return {k: parse_dynamodb_native_format(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [parse_dynamodb_native_format(item) for item in value]
    else:
        return value

def parse_dynamodb_value(value_str: str) -> Any:
    """Parse DynamoDB-exported value"""
    if not value_str or value_str == 'null' or value_str == '':
        return None
    
    try:
        parsed = json.loads(value_str)
        if isinstance(parsed, (dict, list)):
            return parse_dynamodb_native_format(parsed)
        return parsed
    except (json.JSONDecodeError, ValueError):
        pass
    
    try:
        parsed = ast.literal_eval(value_str)
        if isinstance(parsed, (dict, list)):
            return parse_dynamodb_native_format(parsed)
        return parsed
    except (ValueError, SyntaxError):
        pass
    
    return value_str

def extract_lobbyist_name(lobbyist_obj: Dict) -> str:
    """Extract full name from lobbyist object"""
    if not isinstance(lobbyist_obj, dict):
        return ""
    
    # Handle DynamoDB format
    def get_val(key):
        val = lobbyist_obj.get(key)
        if isinstance(val, dict):
            if 'S' in val:
                return val['S']
            elif 'N' in val:
                return val['N']
        return val or ""
    
    name_parts = [
        get_val('prefix_display'),
        get_val('first_name'),
        get_val('middle_name'),
        get_val('last_name'),
        get_val('suffix_display')
    ]
    return ' '.join(filter(None, name_parts))

def analyze_relationships(csv_path: str):
    """Analyze relationships in the CSV"""
    records = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            parsed_row = {}
            for key, value in row.items():
                parsed_row[key] = parse_dynamodb_value(value)
            records.append(parsed_row)
    
    analysis = {
        'registrant_vs_lobbyist': {
            'same_name_count': 0,
            'different_name_count': 0,
            'samples': [],
        },
        'contribution_items_client_info': {
            'has_client_fields': False,
            'client_related_fields': [],
            'samples': [],
        },
        'field_merge_opportunities': {
            'registrant_fields': set(),
            'client_fields': set(),
            'lobbyist_fields': set(),
        },
        'summary': {
            'total_records': len(records),
            'filings': 0,
            'contributions': 0,
        }
    }
    
    for i, record in enumerate(records):
        record_type = 'FILING' if record.get('PK', '').startswith('FILING#') else 'CONTRIBUTION'
        if record_type == 'FILING':
            analysis['summary']['filings'] += 1
        else:
            analysis['summary']['contributions'] += 1
        
        # 1. Compare registrant vs lobbyist
        registrant = record.get('registrant')
        registrant_name = None
        if isinstance(registrant, dict):
            registrant_name = registrant.get('name')
            if isinstance(registrant_name, dict) and 'S' in registrant_name:
                registrant_name = registrant_name['S']
        
        lobbyist_name = None
        if record_type == 'FILING':
            # Extract from lobbying_activities
            lobbying_activities = record.get('lobbying_activities', [])
            if lobbying_activities:
                for activity in lobbying_activities:
                    if isinstance(activity, dict) and 'M' in activity:
                        activity = activity['M']
                    lobbyists = activity.get('lobbyists', [])
                    if lobbyists:
                        for lobbyist_info in lobbyists:
                            if isinstance(lobbyist_info, dict) and 'M' in lobbyist_info:
                                lobbyist_info = lobbyist_info['M']
                            lobbyist_obj = lobbyist_info.get('lobbyist', {})
                            if isinstance(lobbyist_obj, dict) and 'M' in lobbyist_obj:
                                lobbyist_obj = lobbyist_obj['M']
                            if lobbyist_obj:
                                lobbyist_name = extract_lobbyist_name(lobbyist_obj)
                                break
                        if lobbyist_name:
                            break
        else:
            # Direct lobbyist object
            lobbyist = record.get('lobbyist')
            if isinstance(lobbyist, dict):
                if 'M' in lobbyist:
                    lobbyist = lobbyist['M']
                lobbyist_name = extract_lobbyist_name(lobbyist)
        
        if registrant_name and lobbyist_name:
            if registrant_name.upper() == lobbyist_name.upper():
                analysis['registrant_vs_lobbyist']['same_name_count'] += 1
            else:
                analysis['registrant_vs_lobbyist']['different_name_count'] += 1
            
            if len(analysis['registrant_vs_lobbyist']['samples']) < 5:
                analysis['registrant_vs_lobbyist']['samples'].append({
                    'record_index': i,
                    'record_type': record_type,
                    'registrant_name': registrant_name,
                    'lobbyist_name': lobbyist_name,
                    'same': registrant_name.upper() == lobbyist_name.upper(),
                })
        
        # 2. Check contribution_items for client info
        if record_type == 'CONTRIBUTION':
            contribution_items = record.get('contribution_items', [])
            if contribution_items:
                for item in contribution_items:
                    if isinstance(item, dict) and 'M' in item:
                        item = item['M']
                    
                    # Check for client-related fields
                    item_keys = list(item.keys()) if isinstance(item, dict) else []
                    client_related = [k for k in item_keys if 'client' in k.lower()]
                    
                    if client_related:
                        analysis['contribution_items_client_info']['has_client_fields'] = True
                        analysis['contribution_items_client_info']['client_related_fields'].extend(client_related)
                    
                    if len(analysis['contribution_items_client_info']['samples']) < 3:
                        analysis['contribution_items_client_info']['samples'].append({
                            'record_index': i,
                            'item_keys': item_keys,
                            'item_sample': {k: str(v)[:100] for k, v in list(item.items())[:5]} if isinstance(item, dict) else str(item)[:200],
                        })
        
        # 3. Collect all field names for merge opportunities
        if registrant and isinstance(registrant, dict):
            if 'M' in registrant:
                registrant = registrant['M']
            analysis['field_merge_opportunities']['registrant_fields'].update(registrant.keys())
        
        client = record.get('client')
        if client and isinstance(client, dict):
            if 'M' in client:
                client = client['M']
            analysis['field_merge_opportunities']['client_fields'].update(client.keys())
        
        if record_type == 'CONTRIBUTION':
            lobbyist = record.get('lobbyist')
            if lobbyist and isinstance(lobbyist, dict):
                if 'M' in lobbyist:
                    lobbyist = lobbyist['M']
                analysis['field_merge_opportunities']['lobbyist_fields'].update(lobbyist.keys())
    
    # Remove duplicates
    analysis['contribution_items_client_info']['client_related_fields'] = list(set(
        analysis['contribution_items_client_info']['client_related_fields']
    ))
    
    return analysis, records

def print_analysis(analysis: Dict, records: List[Dict]):
    """Print analysis results"""
    print("=" * 80)
    print("LDA RELATIONSHIP ANALYSIS")
    print("=" * 80)
    
    print(f"\n📊 Summary:")
    print(f"   Total Records: {analysis['summary']['total_records']}")
    print(f"   Filings: {analysis['summary']['filings']}")
    print(f"   Contributions: {analysis['summary']['contributions']}")
    
    print(f"\n🔍 REGISTRANT vs LOBBYIST:")
    print(f"   Same Name: {analysis['registrant_vs_lobbyist']['same_name_count']}")
    print(f"   Different Name: {analysis['registrant_vs_lobbyist']['different_name_count']}")
    
    if analysis['registrant_vs_lobbyist']['samples']:
        print(f"\n   Samples:")
        for sample in analysis['registrant_vs_lobbyist']['samples']:
            same_str = "✅ SAME" if sample['same'] else "❌ DIFFERENT"
            print(f"      {sample['record_type']} #{sample['record_index']}: {same_str}")
            print(f"         Registrant: {sample['registrant_name']}")
            print(f"         Lobbyist: {sample['lobbyist_name']}")
    
    print(f"\n🔍 CONTRIBUTION ITEMS - Client Information:")
    print(f"   Has Client Fields: {analysis['contribution_items_client_info']['has_client_fields']}")
    if analysis['contribution_items_client_info']['client_related_fields']:
        print(f"   Client-Related Fields Found: {analysis['contribution_items_client_info']['client_related_fields']}")
    else:
        print(f"   No client-related fields found in contribution_items")
    
    if analysis['contribution_items_client_info']['samples']:
        print(f"\n   Sample Contribution Items:")
        for sample in analysis['contribution_items_client_info']['samples']:
            print(f"      Record #{sample['record_index']}:")
            print(f"         Keys: {sample['item_keys']}")
            print(f"         Sample values: {sample['item_sample']}")
    
    print(f"\n🔍 FIELD MERGE OPPORTUNITIES:")
    print(f"\n   Registrant Fields ({len(analysis['field_merge_opportunities']['registrant_fields'])}):")
    for field in sorted(analysis['field_merge_opportunities']['registrant_fields']):
        print(f"      - {field}")
    
    print(f"\n   Client Fields ({len(analysis['field_merge_opportunities']['client_fields'])}):")
    for field in sorted(analysis['field_merge_opportunities']['client_fields']):
        print(f"      - {field}")
    
    print(f"\n   Lobbyist Fields ({len(analysis['field_merge_opportunities']['lobbyist_fields'])}):")
    for field in sorted(analysis['field_merge_opportunities']['lobbyist_fields']):
        print(f"      - {field}")
    
    # Find overlapping fields
    registrant_set = analysis['field_merge_opportunities']['registrant_fields']
    client_set = analysis['field_merge_opportunities']['client_fields']
    lobbyist_set = analysis['field_merge_opportunities']['lobbyist_fields']
    
    all_common = registrant_set & client_set & lobbyist_set
    registrant_client_common = registrant_set & client_set
    registrant_lobbyist_common = registrant_set & lobbyist_set
    
    print(f"\n   Overlapping Fields:")
    if all_common:
        print(f"      All three share: {sorted(all_common)}")
    if registrant_client_common - all_common:
        print(f"      Registrant & Client: {sorted(registrant_client_common - all_common)}")
    if registrant_lobbyist_common - all_common:
        print(f"      Registrant & Lobbyist: {sorted(registrant_lobbyist_common - all_common)}")
    
    print("\n" + "=" * 80)

def main():
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python analyze_relationships.py <csv_file_path>")
        sys.exit(1)
    
    csv_path = sys.argv[1]
    
    if not Path(csv_path).exists():
        print(f"❌ Error: CSV file not found: {csv_path}")
        sys.exit(1)
    
    print(f"📖 Analyzing CSV: {csv_path}")
    analysis, records = analyze_relationships(csv_path)
    print_analysis(analysis, records)

if __name__ == "__main__":
    main()

