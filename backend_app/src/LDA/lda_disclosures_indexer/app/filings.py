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
    save_parameter_filing_mapping, save_search_index_item, get_government_entity_name, get_general_issue_name, get_country_name,
    clean_value, filings_table, PAC_QUEUE_URL
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
            
            # Extract all government entity IDs from government_entities array
            # Handle both direct array format and DynamoDB format: [{"N": "62"}, {"N": "64"}]
            government_entities = activity.get('government_entities', [])
            if government_entities:
                for entity_item in government_entities:
                    # Handle DynamoDB format: {"N": "62"} or nested structure
                    entity_id = None
                    if isinstance(entity_item, dict):
                        # Check for DynamoDB number format: {"N": "62"}
                        if 'N' in entity_item:
                            entity_id = entity_item['N']
                        else:
                            # Check for nested entity structure
                            entity = entity_item.get('entity', entity_item)
                            if isinstance(entity, dict) and 'M' in entity:
                                entity = entity['M']
                            entity_id = entity.get('id')
                            if isinstance(entity_id, dict) and 'N' in entity_id:
                                entity_id = entity_id['N']
                    elif isinstance(entity_item, (int, str)):
                        entity_id = entity_item
                    
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
                for lobbyist_item in lobbyists:
                    # Handle both formats: direct string or nested object
                    if isinstance(lobbyist_item, str):
                        lobbyist_name = lobbyist_item.strip()
                    elif isinstance(lobbyist_item, dict):
                        # Check if it's a DynamoDB format string: {"S": "NAME"}
                        if 'S' in lobbyist_item:
                            lobbyist_name = lobbyist_item['S'].strip()
                        else:
                            # Nested lobbyist object
                            lobbyist = lobbyist_item.get('lobbyist', lobbyist_item)
                            name_parts = [
                                lobbyist.get('prefix_display', ''),
                                lobbyist.get('first_name', ''),
                                lobbyist.get('middle_name', ''),
                                lobbyist.get('last_name', ''),
                                lobbyist.get('suffix_display', '')
                            ]
                            lobbyist_name = ' '.join(filter(None, name_parts))
                    else:
                        lobbyist_name = str(lobbyist_item).strip()
                    
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
        for entity_item in foreign_entities:
            # Handle nested entity structure: entity can be in 'entity' field (DynamoDB format) or directly in the item
            entity = entity_item.get('entity', entity_item)
            
            # Handle DynamoDB format: if entity is a dict with 'M' key, extract the map
            if isinstance(entity, dict) and 'M' in entity:
                entity = entity['M']
            
            # Extract country code (handle DynamoDB 'S' format)
            country_code = entity.get('country') or entity.get('ppb_country')
            if isinstance(country_code, dict) and 'S' in country_code:
                country_code = country_code['S']
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
        
        # Set item_type for GSI
        item['item_type'] = 'FILING'
        
        # Handle GSI fields - only set if values exist
        # Clean values before storing to ensure uniform field names (no commas)
        if indexed_fields.get('registrant_name'):
            cleaned_registrant = clean_value(indexed_fields['registrant_name'])
            if cleaned_registrant:
                item['registrant_name'] = cleaned_registrant
                send_autocomplete_value('registrant_name', cleaned_registrant)
            else:
                item.pop('registrant_name', None)
        else:
            item.pop('registrant_name', None)
        
        if indexed_fields.get('client_name'):
            cleaned_client = clean_value(indexed_fields['client_name'])
            if cleaned_client:
                item['client_name'] = cleaned_client
                send_autocomplete_value('client_name', cleaned_client)
            else:
                item.pop('client_name', None)
        else:
            item.pop('client_name', None)
        
        # Remove single lobbyist_name field (now using parameter-filing mappings for all lobbyists)
        item.pop('lobbyist_name', None)
        
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
        
        # Remove arrays that are now only in parameter-filing mappings
        item.pop('all_general_issue_codes', None)
        item.pop('foreign_countries', None)
        item.pop('all_government_entity_ids', None)
        
        if indexed_fields.get('is_foreign') != 1:
            item.pop('is_foreign', None)
        else:
            item['is_foreign'] = 1
        
        # Save to DynamoDB
        filings_table.put_item(Item=item)
        
        # Save parameter-filing mappings for all array values
        filing_uuid = item['filing_uuid']
        dt_posted = indexed_fields.get('dt_posted')
        
        # Save parameter-filing mappings for: General Issue Codes, Government Entities, PACs, Foreign Entities, and Lobbyists
        
        # Save all general issue codes (mapped to names)
        all_general_issue_codes = indexed_fields.get('all_general_issue_codes', [])
        if all_general_issue_codes:
            for issue_code in all_general_issue_codes:
                if issue_code:
                    # Map issue code to name using constants file
                    issue_name = get_general_issue_name(issue_code)
                    if issue_name:
                        # Save parameter-filing mapping with full name, not abbreviation
                        save_parameter_filing_mapping(
                            parameter_type='GENERAL_ISSUE',
                            parameter_value=issue_name,  # Store name, not code
                            filing_uuid=filing_uuid,
                            filing_type='FILING',
                            dt_posted=dt_posted
                        )
        
        # Save all government entity IDs (mapped to names)
        all_government_entity_ids = indexed_fields.get('all_government_entity_ids', [])
        if all_government_entity_ids:
            for entity_id in all_government_entity_ids:
                if entity_id:
                    # Map entity ID to name using constants file
                    entity_name = get_government_entity_name(entity_id)
                    if entity_name:
                        save_parameter_filing_mapping(
                            parameter_type='GOVERNMENT_ENTITY',
                            parameter_value=entity_name,  # Store name, not ID
                            filing_uuid=filing_uuid,
                            filing_type='FILING',
                            dt_posted=dt_posted
                        )
        
        # Save all foreign countries (mapped to names)
        foreign_countries = indexed_fields.get('foreign_countries', [])
        if foreign_countries:
            for country_code in foreign_countries:
                if country_code:
                    # Map country code to name using constants file
                    country_name = get_country_name(country_code)
                    if country_name:
                        # Save parameter-filing mapping with full name, not code
                        save_parameter_filing_mapping(
                            parameter_type='FOREIGN_COUNTRY',
                            parameter_value=country_name,  # Store name, not code
                            filing_uuid=filing_uuid,
                            filing_type='FILING',
                            dt_posted=dt_posted
                        )
        
        # Save all lobbyist names
        all_lobbyist_names = indexed_fields.get('all_lobbyist_names', [])
        if all_lobbyist_names:
            for lobbyist_name in all_lobbyist_names:
                if lobbyist_name:
                    # Clean value before storing to ensure uniform field names (no commas)
                    cleaned_lobbyist = clean_value(lobbyist_name)
                    if cleaned_lobbyist:
                        # Send to autocomplete queue for autocomplete file generation
                        send_autocomplete_value('lobbyist_name', cleaned_lobbyist)
                        # Save parameter-filing mapping
                        save_parameter_filing_mapping(
                            parameter_type='LOBBYIST',
                            parameter_value=cleaned_lobbyist,
                            filing_uuid=filing_uuid,
                            filing_type='FILING',
                            dt_posted=dt_posted
                        )
        
        # Save materialized search index items for efficient querying and pagination
        entity_pk = f"FILING#{filing_uuid}"
        
        # Save registrant search index
        if indexed_fields.get('registrant_name'):
            cleaned_registrant = clean_value(indexed_fields['registrant_name'])
            if cleaned_registrant:
                save_search_index_item(
                    search_type='REGISTRANT',
                    search_value=cleaned_registrant,
                    entity_pk=entity_pk,
                    entity_type='FILING',
                    dt_posted=dt_posted
                )
        
        # Save client search index
        if indexed_fields.get('client_name'):
            cleaned_client = clean_value(indexed_fields['client_name'])
            if cleaned_client:
                save_search_index_item(
                    search_type='CLIENT',
                    search_value=cleaned_client,
                    entity_pk=entity_pk,
                    entity_type='FILING',
                    dt_posted=dt_posted
                )
        
        # Save lobbyist search indexes
        if all_lobbyist_names:
            for lobbyist_name in all_lobbyist_names:
                if lobbyist_name:
                    cleaned_lobbyist = clean_value(lobbyist_name)
                    if cleaned_lobbyist:
                        save_search_index_item(
                            search_type='LOBBYIST',
                            search_value=cleaned_lobbyist,
                            entity_pk=entity_pk,
                            entity_type='FILING',
                            dt_posted=dt_posted
                        )
        
        # Save general issue search indexes
        if all_general_issue_codes:
            for issue_code in all_general_issue_codes:
                if issue_code:
                    issue_name = get_general_issue_name(issue_code)
                    if issue_name:
                        save_search_index_item(
                            search_type='GENERAL_ISSUE',
                            search_value=issue_name,
                            entity_pk=entity_pk,
                            entity_type='FILING',
                            dt_posted=dt_posted
                        )
        
        # Save government entity search indexes
        if all_government_entity_ids:
            for entity_id in all_government_entity_ids:
                if entity_id:
                    entity_name = get_government_entity_name(entity_id)
                    if entity_name:
                        save_search_index_item(
                            search_type='GOVERNMENT_ENTITY',
                            search_value=entity_name,
                            entity_pk=entity_pk,
                            entity_type='FILING',
                            dt_posted=dt_posted
                        )
        
        # Save foreign country search indexes
        if foreign_countries:
            for country_code in foreign_countries:
                if country_code:
                    country_name = get_country_name(country_code)
                    if country_name:
                        save_search_index_item(
                            search_type='FOREIGN_COUNTRY',
                            search_value=country_name,
                            entity_pk=entity_pk,
                            entity_type='FILING',
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

