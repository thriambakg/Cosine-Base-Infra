"""
Contributions processing module for LDA indexer Lambda
Processes a single page of contributions in parallel (25 workers per page)
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

def extract_indexed_fields_contribution(contribution: Dict) -> Dict:
    """Extract indexed fields for a contribution report (LD-203)"""
    indexed = {}
    
    # Primary Key
    indexed['filing_uuid'] = contribution.get('filing_uuid')
    
    # Registrant
    registrant = contribution.get('registrant', {})
    if registrant:
        indexed['registrant_id'] = registrant.get('id')
        indexed['registrant_name'] = registrant.get('name')
        indexed['registrant_house_registrant_id'] = registrant.get('house_registrant_id')
    
    # Client
    client = contribution.get('client', {})
    if client:
        indexed['client_id'] = client.get('id')
        indexed['client_name'] = client.get('name')
        indexed['client_client_id'] = client.get('client_id')
    
    # Lobbyist
    lobbyist = contribution.get('lobbyist', {})
    if lobbyist:
        name_parts = [
            lobbyist.get('prefix_display', ''),
            lobbyist.get('first_name', ''),
            lobbyist.get('middle_name', ''),
            lobbyist.get('last_name', ''),
            lobbyist.get('suffix_display', '')
        ]
        indexed['lobbyist_name'] = ' '.join(filter(None, name_parts))
        indexed['lobbyist_id'] = lobbyist.get('id')
    
    # Report type
    indexed['report_type'] = contribution.get('filing_type')
    indexed['report_type_display'] = contribution.get('filing_type_display')
    
    # Filing period and year
    indexed['filing_period'] = contribution.get('filing_period')
    indexed['filing_period_display'] = contribution.get('filing_period_display')
    filing_year = contribution.get('filing_year')
    if filing_year:
        try:
            indexed['filing_year'] = int(filing_year)
        except (ValueError, TypeError):
            indexed['filing_year'] = filing_year
    
    # Posted date
    indexed['dt_posted'] = contribution.get('dt_posted')
    
    # Filer type
    indexed['filer_type'] = contribution.get('filer_type')
    indexed['filer_type_display'] = contribution.get('filer_type_display')
    
    # Calculate total contribution amount from contribution_items
    contribution_items = contribution.get('contribution_items', [])
    total_amount = Decimal('0')
    all_contribution_item_types = []
    
    if contribution_items:
        for item in contribution_items:
            contribution_type = item.get('contribution_type')
            if contribution_type:
                if 'contribution_item_type' not in indexed:
                    indexed['contribution_item_type'] = contribution_type
                if contribution_type not in all_contribution_item_types:
                    all_contribution_item_types.append(contribution_type)
            
            amount_str = item.get('amount')
            if amount_str:
                try:
                    total_amount += Decimal(str(amount_str))
                except (ValueError, TypeError):
                    pass
    
    indexed['amount_reported'] = total_amount if total_amount > 0 else None
    if all_contribution_item_types:
        indexed['all_contribution_item_types'] = all_contribution_item_types
    
    # Extract state
    state = None
    if contribution.get('address'):
        state = contribution['address'].get('state')
    if not state and registrant:
        if registrant.get('address'):
            state = registrant['address'].get('state')
        if not state:
            state = registrant.get('state')
    indexed['state'] = state
    
    # General Issue Codes (from lobbying_activities if present)
    lobbying_activities = contribution.get('lobbying_activities', [])
    all_general_issue_codes = []
    
    if lobbying_activities:
        for activity in lobbying_activities:
            general_issue_code = activity.get('general_issue_code')
            if general_issue_code:
                if general_issue_code not in all_general_issue_codes:
                    all_general_issue_codes.append(general_issue_code)
    
    if all_general_issue_codes:
        indexed['all_general_issue_codes'] = all_general_issue_codes
    
    # Foreign Entities
    foreign_entities = contribution.get('foreign_entities', [])
    foreign_countries = set()
    
    if foreign_entities:
        for entity in foreign_entities:
            country_code = entity.get('country') or entity.get('ppb_country')
            if country_code and country_code != 'US':
                foreign_countries.add(country_code)
    
    indexed['is_foreign'] = 1 if len(foreign_countries) > 0 else 0
    if foreign_countries:
        indexed['foreign_countries'] = sorted(list(foreign_countries))
    
    return indexed

def save_contribution_to_dynamodb(contribution: Dict, indexed_fields: Dict, s3_key: Optional[str] = None):
    """Save contribution to DynamoDB with all fields and indexed GSI fields"""
    try:
        item = json.loads(json.dumps(contribution), parse_float=Decimal)
        item.update(indexed_fields)
        
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
        lobbyist = item.get('lobbyist')
        unified_entity = create_unified_entity(client, lobbyist)
        if unified_entity:
            item['entity'] = unified_entity
        
        # Check for PACs
        pacs = contribution.get('pacs', [])
        indexed_fields['pac'] = 1 if (pacs and len(pacs) > 0) else 0
        item['pac'] = indexed_fields['pac']
        
        # Send PAC names to SQS and save parameter mappings
        if pacs:
            for pac in pacs:
                if isinstance(pac, dict):
                    pac_name = pac.get('name') or pac.get('S')
                elif isinstance(pac, str):
                    pac_name = pac
                else:
                    pac_name = str(pac)
                
                if pac_name:
                    send_autocomplete_value('pac_name', pac_name)
                    # Save parameter-filing mapping for each PAC
                    save_parameter_filing_mapping(
                        parameter_type='PAC',
                        parameter_value=pac_name,
                        filing_uuid=item['filing_uuid'],
                        filing_type='CONTRIBUTION',
                        dt_posted=indexed_fields.get('dt_posted')
                    )
        
        # Save parameter-filing mappings for General Issue Codes and Foreign Countries
        filing_uuid = item['filing_uuid']
        dt_posted = indexed_fields.get('dt_posted')
        
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
                        filing_type='CONTRIBUTION',
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
                        filing_type='CONTRIBUTION',
                        dt_posted=dt_posted
                    )
        
        # Set null values for filing-specific fields
        original_client = contribution.get('client')
        if not original_client:
            item['client'] = None
            item['client_id'] = None
            item['client_name'] = None
            item['client_client_id'] = None
        
        item['income'] = None
        item['expenses'] = None
        item['expenses_method'] = None
        item['expenses_method_display'] = None
        item['lobbying_activities'] = None
        item['termination_date'] = None
        item.pop('general_issue_code', None)
        item.pop('general_issue_code_display', None)
        
        # Set primary key
        item['PK'] = f"CONTRIBUTION#{item['filing_uuid']}"
        item['SK'] = f"CONTRIBUTION#{item['filing_uuid']}"
        
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
        
        if not indexed_fields.get('filer_type'):
            item.pop('filer_type', None)
            item.pop('filer_type_display', None)
        
        if not indexed_fields.get('contribution_item_type'):
            item.pop('contribution_item_type', None)
        
        item.pop('all_contribution_item_types', None)
        item.pop('government_entity_id', None)
        item.pop('all_government_entity_ids', None)
        
        # Remove arrays that are now only in parameter-filing mappings
        item.pop('all_general_issue_codes', None)
        item.pop('foreign_countries', None)
        
        # Keep is_foreign and pac as boolean flags for GSI queries
        if indexed_fields.get('is_foreign') != 1:
            item.pop('is_foreign', None)
        else:
            item['is_foreign'] = 1
        
        if indexed_fields.get('pac') != 1:
            item.pop('pac', None)
        else:
            item['pac'] = 1
        
        # Save to DynamoDB
        filings_table.put_item(Item=item)
        
    except Exception as e:
        print(f"❌ Error saving contribution to DynamoDB: {str(e)[:200]}")
        raise

def process_single_contribution(session, contribution: Dict) -> bool:
    """Process a single contribution (download document, save to DynamoDB)"""
    filing_uuid = contribution.get('filing_uuid')
    if not filing_uuid:
        return False
    
    try:
        filing_type = contribution.get('filing_type', 'unknown')
        indexed_fields = extract_indexed_fields_contribution(contribution)
        
        # Download document if available
        s3_key = None
        doc_url = contribution.get('filing_document_url')
        if doc_url:
            content_type = contribution.get('filing_document_content_type', 'pdf')
            ext = 'pdf' if 'pdf' in content_type.lower() else 'html'
            s3_key = f"contributions/{filing_type}/{filing_uuid}.{ext}"
            download_document(session, doc_url, s3_key)
        
        # Save to DynamoDB
        save_contribution_to_dynamodb(contribution, indexed_fields, s3_key=s3_key)
        return True
        
    except Exception as e:
        print(f"   ❌ Error processing contribution {filing_uuid}: {str(e)[:200]}")
        return False

def process_contributions_page(page: int, start_date: Optional[str], end_date: Optional[str]) -> Dict:
    """
    Process a single page of contributions with 25 parallel workers
    
    Returns:
    {
        'processed_count': 25,
        'success': True
    }
    """
    print(f"📄 Processing contributions page {page}...")
    
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
    response = call_api(session, 'contributions', params={**base_params, 'page': page})
    results = response.get('results', [])
    
    if not results:
        print(f"   ⚠️ No results on page {page}")
        return {'processed_count': 0, 'success': True}
    
    print(f"   Found {len(results)} contributions on page {page}")
    
    # Process items in parallel (25 workers per page)
    # Each item: API call, document download, DynamoDB write
    processed_count = 0
    failed_count = 0
    max_workers = 25  # Process all items on page in parallel
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all contribution processing tasks
        future_to_contribution = {
            executor.submit(process_single_contribution, session, contribution): contribution
            for contribution in results
        }
        
        # Collect results as they complete
        for future in as_completed(future_to_contribution):
            contribution = future_to_contribution[future]
            contribution_uuid = contribution.get('filing_uuid', 'unknown')
            try:
                if future.result():
                    processed_count += 1
                else:
                    failed_count += 1
            except Exception as e:
                failed_count += 1
                print(f"   ❌ Exception processing contribution {contribution_uuid}: {str(e)[:200]}")
    
    print(f"✅ Page {page} complete: Processed {processed_count}/{len(results)} contributions ({failed_count} failed)")
    
    return {
        'processed_count': processed_count,
        'success': True
    }

