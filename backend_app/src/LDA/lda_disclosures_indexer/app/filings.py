"""
Filings processing module for LDA indexer Lambda
Processes a single page of filings in parallel (25 workers per page)
"""

import json
from typing import Dict, Optional
from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor, as_completed
from shared_utils import (
    get_api_key, create_session, call_api, download_document,
    merge_address_fields, create_unified_entity, send_autocomplete_value,
    save_parameter_filing_mapping,
    filings_table, PAC_QUEUE_URL
)

def extract_indexed_fields_filing(filing: Dict) -> Dict:
    """Extract indexed fields for a filing (LD-1 or LD-2)"""
    indexed = {}
    
    # Primary Key
    indexed['filing_uuid'] = filing.get('filing_uuid')
    
    # Registrant
    registrant = filing.get('registrant', {})
    if registrant:
        indexed['registrant_id'] = registrant.get('id')
        indexed['registrant_name'] = registrant.get('name')
        indexed['registrant_house_registrant_id'] = registrant.get('house_registrant_id')
    
    # Client
    client = filing.get('client', {})
    if client:
        indexed['client_id'] = client.get('id')
        indexed['client_name'] = client.get('name')
        indexed['client_client_id'] = client.get('client_id')
    
    # Lobbyists, General Issue Code, and Government Entity ID
    lobbying_activities = filing.get('lobbying_activities', [])
    all_general_issue_codes = []
    all_government_entity_ids = []
    all_lobbyist_names = []
    
    if lobbying_activities:
        for activity in lobbying_activities:
            # Extract all general issue codes (no first element indexing)
            general_issue_code = activity.get('general_issue_code')
            if general_issue_code:
                if general_issue_code not in all_general_issue_codes:
                    all_general_issue_codes.append(general_issue_code)
            
            # Extract all government entity IDs (no first element indexing)
            government_entities = activity.get('government_entities', [])
            if government_entities:
                for entity in government_entities:
                    entity_id = entity.get('id')
                    if entity_id:
                        try:
                            entity_id_int = int(entity_id)
                            if entity_id_int not in all_government_entity_ids:
                                all_government_entity_ids.append(entity_id_int)
                        except (ValueError, TypeError):
                            pass
            
            # Extract all lobbyists (no first element indexing)
            lobbyists = activity.get('lobbyists', [])
            if lobbyists:
                for lobbyist_obj in lobbyists:
                    lobbyist = lobbyist_obj.get('lobbyist', {})
                    if lobbyist:
                        name_parts = [
                            lobbyist.get('prefix_display', ''),
                            lobbyist.get('first_name', ''),
                            lobbyist.get('middle_name', ''),
                            lobbyist.get('last_name', ''),
                            lobbyist.get('suffix_display', '')
                        ]
                        lobbyist_name = ' '.join(filter(None, name_parts))
                        if lobbyist_name and lobbyist_name not in all_lobbyist_names:
                            all_lobbyist_names.append(lobbyist_name)
    
    # Store all codes as lists for parameter-filings table mapping
    if all_general_issue_codes:
        indexed['all_general_issue_codes'] = all_general_issue_codes
    if all_government_entity_ids:
        indexed['all_government_entity_ids'] = all_government_entity_ids
    if all_lobbyist_names:
        indexed['all_lobbyist_names'] = all_lobbyist_names
    
    # Foreign Entities
    foreign_entities = filing.get('foreign_entities', [])
    foreign_countries = set()
    
    if foreign_entities:
        for entity in foreign_entities:
            country_code = entity.get('country') or entity.get('ppb_country')
            if country_code and country_code != 'US':
                foreign_countries.add(country_code)
    
    indexed['is_foreign'] = 1 if len(foreign_countries) > 0 else 0
    if foreign_countries:
        indexed['foreign_countries'] = sorted(list(foreign_countries))
    
    # Report type
    indexed['report_type'] = filing.get('filing_type')
    indexed['report_type_display'] = filing.get('filing_type_display')
    
    # Filing period and year
    indexed['filing_period'] = filing.get('filing_period')
    indexed['filing_period_display'] = filing.get('filing_period_display')
    filing_year = filing.get('filing_year')
    if filing_year:
        try:
            indexed['filing_year'] = int(filing_year)
        except (ValueError, TypeError):
            indexed['filing_year'] = filing_year
    
    # Posted date
    indexed['dt_posted'] = filing.get('dt_posted')
    
    # Amount fields
    income = filing.get('income')
    if income is not None:
        try:
            indexed['amount_reported'] = Decimal(str(income))
        except (ValueError, TypeError):
            pass
    
    expenses = filing.get('expenses')
    if expenses is not None:
        try:
            indexed['expenses'] = Decimal(str(expenses))
        except (ValueError, TypeError):
            indexed['expenses'] = expenses
    else:
        indexed['expenses'] = None
    
    indexed['expenses_method'] = filing.get('expenses_method')
    
    # Extract state
    state = None
    if filing.get('address'):
        state = filing['address'].get('state')
    if not state and registrant:
        if registrant.get('address'):
            state = registrant['address'].get('state')
        if not state:
            state = registrant.get('state')
    indexed['state'] = state
    
    return indexed

def save_filing_to_dynamodb(filing: Dict, indexed_fields: Dict, s3_key: Optional[str] = None):
    """Save filing to DynamoDB with all fields and indexed GSI fields"""
    try:
        item = json.loads(json.dumps(filing), parse_float=Decimal)
        item.update(indexed_fields)
        
        if 'expenses' in indexed_fields and indexed_fields['expenses'] is not None:
            item['expenses'] = indexed_fields['expenses']
        
        if s3_key:
            item['s3_key'] = s3_key
        
        # Merge address fields
        registrant = item.get('registrant')
        unified_address = merge_address_fields(item, registrant)
        if unified_address:
            item['address'] = unified_address
        
        # Remove redundant address fields
        for field in ['address_1', 'address_2', 'zip', 'registrant_address_1', 'registrant_address_2',
                     'registrant_city', 'registrant_state', 'registrant_zip', 'registrant_country']:
            item.pop(field, None)
        
        # Create unified entity field
        client = item.get('client')
        lobbyist = None
        lobbying_activities = item.get('lobbying_activities', [])
        if lobbying_activities:
            for activity in lobbying_activities:
                lobbyists = activity.get('lobbyists', [])
                if lobbyists:
                    lobbyist = lobbyists[0].get('lobbyist', {})
                    break
        unified_entity = create_unified_entity(client, lobbyist)
        if unified_entity:
            item['entity'] = unified_entity
        
        # Set null values for contribution-specific fields
        item['contribution_items'] = None
        item['no_contributions'] = None
        item.pop('filer_type', None)
        item.pop('filer_type_display', None)
        item.pop('pac', None)
        
        # Set primary key
        item['PK'] = f"FILING#{item['filing_uuid']}"
        item['SK'] = f"FILING#{item['filing_uuid']}"
        
        # Handle GSI fields - only set if values exist
        if indexed_fields.get('registrant_name'):
            send_autocomplete_value('registrant_name', indexed_fields['registrant_name'])
        else:
            item.pop('registrant_name', None)
        
        if not indexed_fields.get('client_name'):
            item.pop('client_name', None)
        else:
            send_autocomplete_value('client_name', indexed_fields['client_name'])
        
        if not indexed_fields.get('lobbyist_name'):
            item.pop('lobbyist_name', None)
        else:
            send_autocomplete_value('lobbyist_name', indexed_fields['lobbyist_name'])
        
        if indexed_fields.get('amount_reported'):
            amount = indexed_fields['amount_reported']
            amount_bucket = int(float(amount) / 10000) * 10000
            item['amount_bucket'] = Decimal(str(amount_bucket))
        else:
            item.pop('amount_reported', None)
            item.pop('amount_bucket', None)
        
        if not indexed_fields.get('state'):
            item.pop('state', None)
        
        # Remove first-element indexed fields (now only in parameter-filings table)
        item.pop('general_issue_code', None)
        item.pop('general_issue_code_display', None)
        item.pop('government_entity_id', None)
        item.pop('lobbyist_name', None)
        item.pop('lobbyist_id', None)
        
        item.pop('contribution_item_type', None)
        item.pop('all_contribution_item_types', None)
        
        if indexed_fields.get('is_foreign') != 1:
            item.pop('is_foreign', None)
        else:
            item['is_foreign'] = 1
        
        # Save to DynamoDB
        filings_table.put_item(Item=item)
        
        # Save parameter-filing mappings for all array values
        filing_uuid = item['filing_uuid']
        dt_posted = indexed_fields.get('dt_posted')
        
        # Save parameter-filing mappings only for: General Issue Codes, PACs, and Foreign Entities
        # (Other fields like government entities and lobbyists have GSIs and are fast queries)
        
        # Save all general issue codes
        all_general_issue_codes = indexed_fields.get('all_general_issue_codes', [])
        if all_general_issue_codes:
            for issue_code in all_general_issue_codes:
                if issue_code:
                    # Save parameter-filing mapping
                    # Note: General issue codes use static CSV in frontend, no autocomplete queue needed
                    save_parameter_filing_mapping(
                        parameter_type='GENERAL_ISSUE',
                        parameter_value=issue_code,
                        filing_uuid=filing_uuid,
                        filing_type='FILING',
                        dt_posted=dt_posted
                    )
        
        # Save all foreign countries
        foreign_countries = indexed_fields.get('foreign_countries', [])
        if foreign_countries:
            for country_code in foreign_countries:
                if country_code:
                    save_parameter_filing_mapping(
                        parameter_type='FOREIGN_COUNTRY',
                        parameter_value=country_code,
                        filing_uuid=filing_uuid,
                        filing_type='FILING',
                        dt_posted=dt_posted
                    )
        
    except Exception as e:
        print(f"❌ Error saving filing to DynamoDB: {str(e)[:200]}")
        raise

def process_single_filing(session, filing: Dict) -> bool:
    """Process a single filing (download document, save to DynamoDB)"""
    filing_uuid = filing.get('filing_uuid')
    if not filing_uuid:
        return False
    
    try:
        filing_type = filing.get('filing_type', '')
        indexed_fields = extract_indexed_fields_filing(filing)
        
        # Download document if available
        s3_key = None
        doc_url = filing.get('filing_document_url')
        if doc_url:
            content_type = filing.get('filing_document_content_type', 'pdf')
            ext = 'pdf' if 'pdf' in content_type.lower() else 'html'
            s3_key = f"filings/{filing_type}/{filing_uuid}.{ext}"
            download_document(session, doc_url, s3_key)
        
        # Save to DynamoDB
        save_filing_to_dynamodb(filing, indexed_fields, s3_key=s3_key)
        return True
        
    except Exception as e:
        print(f"   ❌ Error processing filing {filing_uuid}: {str(e)[:200]}")
        return False

def process_filings_page(page: int, start_date: Optional[str], end_date: Optional[str]) -> Dict:
    """
    Process a single page of filings with 25 parallel workers
    
    Returns:
    {
        'processed_count': 25,
        'success': True
    }
    """
    print(f"📄 Processing filings page {page}...")
    
    # Get API key and create session
    api_key = get_api_key()
    session = create_session(api_key)
    
    # Build params
    base_params = {'page_size': 25}
    if not start_date and not end_date:
        base_params['filing_dt_posted_after'] = '2000-01-01'
    else:
        if start_date:
            base_params['filing_dt_posted_after'] = start_date
        if end_date:
            base_params['filing_dt_posted_before'] = end_date
    
    # Fetch the page
    response = call_api(session, 'filings', params={**base_params, 'page': page})
    results = response.get('results', [])
    
    if not results:
        print(f"   ⚠️ No results on page {page}")
        return {'processed_count': 0, 'success': True}
    
    print(f"   Found {len(results)} filings on page {page}")
    
    # Process items in parallel (25 workers per page)
    # Each item: API call, document download, DynamoDB write
    processed_count = 0
    failed_count = 0
    max_workers = 25  # Process all items on page in parallel
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all filing processing tasks
        future_to_filing = {
            executor.submit(process_single_filing, session, filing): filing
            for filing in results
        }
        
        # Collect results as they complete
        for future in as_completed(future_to_filing):
            filing = future_to_filing[future]
            filing_uuid = filing.get('filing_uuid', 'unknown')
            try:
                if future.result():
                    processed_count += 1
                else:
                    failed_count += 1
            except Exception as e:
                failed_count += 1
                print(f"   ❌ Exception processing filing {filing_uuid}: {str(e)[:200]}")
    
    print(f"✅ Page {page} complete: Processed {processed_count}/{len(results)} filings ({failed_count} failed)")
    
    return {
        'processed_count': processed_count,
        'success': True
    }

