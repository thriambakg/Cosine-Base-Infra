"""
Test script for SEC EDGAR Search API
Interactive script that queries user for all search parameters available on SEC search page
Uses browser automation to interact with the actual SEC search page and extract real URLs
"""

import requests
import json
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import time
import re

# Try to import Selenium for browser automation
try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    from selenium.common.exceptions import TimeoutException, NoSuchElementException
    SELENIUM_AVAILABLE = True
    
    # Try to import webdriver-manager for automatic ChromeDriver management
    try:
        from webdriver_manager.chrome import ChromeDriverManager
        from selenium.webdriver.chrome.service import Service as ChromeService
        WEBDRIVER_MANAGER_AVAILABLE = True
    except ImportError:
        WEBDRIVER_MANAGER_AVAILABLE = False
        # ChromeDriver must be in PATH or specified manually
except ImportError:
    SELENIUM_AVAILABLE = False
    WEBDRIVER_MANAGER_AVAILABLE = False
    print("⚠️  Selenium not available. Install with: pip install selenium")
    print("   Browser automation features will be disabled.")

# SEC API configuration
SEC_BASE_URL = "https://www.sec.gov"
SEC_SEARCH_URL = "https://www.sec.gov/edgar/search"
SEC_DATA_URL = "https://data.sec.gov"
SEC_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 (Cosine Financial Platform; contact@cosine.financial)"

# Limit results to 10
MAX_RESULTS = 10

def create_session():
    """Create a requests session with proper headers"""
    session = requests.Session()
    session.headers.update({
        'User-Agent': SEC_USER_AGENT,
        'Accept': 'application/json, text/html, application/xhtml+xml, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Referer': 'https://www.sec.gov/',
    })
    return session

def get_company_search_preview(search_term: str) -> List[Dict[str, Any]]:
    """
    Get search preview/autocomplete results from SEC search-index API
    
    Args:
        search_term: Company name or partial name to search
    
    Returns:
        List of matching companies with name, CIK, ticker
    """
    session = create_session()
    
    try:
        # SEC search-index API endpoint
        url = f"https://efts.sec.gov/LATEST/search-index"
        params = {
            'keysTyped': search_term
        }
        
        time.sleep(0.1)  # Rate limiting
        response = session.get(url, params=params, timeout=10)
        response.raise_for_status()
        
        # The API returns JSON with company matches
        data = response.json()
        
        # Parse the response - structure may vary
        results = []
        
        # Debug: Check response structure
        # print(f"   DEBUG: API response type: {type(data)}")
        # if isinstance(data, dict):
        #     print(f"   DEBUG: API response keys: {list(data.keys())}")
        
        if isinstance(data, list):
            # If it's a list, use it directly
            for item in data:
                if isinstance(item, dict):
                    # Try various field name variations
                    name = (item.get('name') or item.get('entityName') or 
                           item.get('entity') or item.get('title') or '')
                    cik = (item.get('cik') or item.get('CIK') or 
                          item.get('cik_str') or '')
                    ticker = (item.get('ticker') or item.get('symbol') or 
                             item.get('tickerSymbol') or '')
                    
                    if name or cik:  # Only add if we have at least name or CIK
                        results.append({
                            'name': str(name),
                            'cik': str(cik) if cik else '',
                            'ticker': str(ticker) if ticker else ''
                        })
        elif isinstance(data, dict):
            # If it's a dict, look for common keys
            if 'hits' in data:
                hits = data['hits']
                if isinstance(hits, list):
                    for hit in hits:
                        if isinstance(hit, dict):
                            source = hit.get('_source', hit)
                            name = (source.get('name') or source.get('entityName') or 
                                   source.get('entity') or '')
                            cik = (source.get('cik') or source.get('CIK') or 
                                  source.get('cik_str') or '')
                            ticker = (source.get('ticker') or source.get('symbol') or 
                                     source.get('tickerSymbol') or '')
                            
                            if name or cik:
                                results.append({
                                    'name': str(name),
                                    'cik': str(cik) if cik else '',
                                    'ticker': str(ticker) if ticker else ''
                                })
            elif 'results' in data:
                for item in data['results']:
                    if isinstance(item, dict):
                        name = (item.get('name') or item.get('entityName') or 
                               item.get('entity') or '')
                        cik = (item.get('cik') or item.get('CIK') or 
                              item.get('cik_str') or '')
                        ticker = (item.get('ticker') or item.get('symbol') or 
                                 item.get('tickerSymbol') or '')
                        
                        if name or cik:
                            results.append({
                                'name': str(name),
                                'cik': str(cik) if cik else '',
                                'ticker': str(ticker) if ticker else ''
                            })
            elif 'companies' in data:
                # Alternative structure
                for item in data['companies']:
                    if isinstance(item, dict):
                        name = (item.get('name') or item.get('entityName') or '')
                        cik = (item.get('cik') or item.get('CIK') or '')
                        ticker = (item.get('ticker') or item.get('symbol') or '')
                        
                        if name or cik:
                            results.append({
                                'name': str(name),
                                'cik': str(cik) if cik else '',
                                'ticker': str(ticker) if ticker else ''
                            })
            else:
                # Try to extract from top-level keys
                name = (data.get('name') or data.get('entityName') or 
                       data.get('entity') or '')
                cik = (data.get('cik') or data.get('CIK') or 
                      data.get('cik_str') or '')
                ticker = (data.get('ticker') or data.get('symbol') or 
                         data.get('tickerSymbol') or '')
                
                if name or cik:
                    results.append({
                        'name': str(name),
                        'cik': str(cik) if cik else '',
                        'ticker': str(ticker) if ticker else ''
                    })
        
        # Limit to first 10 results
        return results[:10]
        
    except Exception as e:
        # If preview fails, return empty list (will use original input)
        # Silently fail - user can still proceed with their input
        return []

def get_user_input(prompt: str, default: str = "", allow_blank: bool = True) -> str:
    """Get user input with optional default and blank handling"""
    if default:
        full_prompt = f"{prompt} [{default}]: "
    else:
        full_prompt = f"{prompt}: "
    
    value = input(full_prompt).strip()
    
    if not value:
        if default:
            return default
        elif allow_blank:
            return ""
        else:
            return get_user_input(prompt, default, allow_blank)
    
    return value

def get_search_parameters() -> Dict[str, Any]:
    """Interactively query user for all SEC search page parameters"""
    print("=" * 80)
    print("SEC EDGAR Search - Enter Search Parameters")
    print("=" * 80)
    print()
    print("Press Enter to leave blank (blank values will be treated as empty)")
    print()
    
    params = {}
    
    # Document word or phrase (keywords)
    print("1. Document word or phrase")
    print("   Keywords to search for in filing documents")
    params['keywords'] = get_user_input("   Enter keywords", allow_blank=True)
    print()
    
    # Company name, ticker, CIK number or individual's name
    print("2. Company name, ticker, CIK number or individual's name")
    print("   Example: WATERS CORP /DE/ (WAT) (CIK 0001000697)")
    entity_input = get_user_input("   Enter company/ticker/CIK/name", allow_blank=True)
    
    # Try to parse CIK from input (look for CIK pattern)
    if entity_input:
        import re
        cik_match = re.search(r'CIK\s*(\d+)', entity_input, re.IGNORECASE)
        if cik_match:
            params['cik'] = cik_match.group(1)
        else:
            # Check if it's just a CIK number
            if entity_input.isdigit():
                params['cik'] = entity_input
            else:
                # Check if it's a ticker (usually 1-5 uppercase letters)
                ticker_match = re.match(r'^([A-Z]{1,5})$', entity_input.strip())
                if ticker_match:
                    params['ticker'] = entity_input.strip()
                else:
                    # It's likely a company name - try to get search preview
                    print(f"   🔍 Searching for matches...")
                    preview_results = get_company_search_preview(entity_input)
                    if preview_results and len(preview_results) > 1:
                        print()
                        print("   📋 Multiple results found. Please select one:")
                        print()
                        for i, result in enumerate(preview_results, 1):
                            name = result.get('name', 'N/A')
                            cik = result.get('cik', 'N/A')
                            ticker = result.get('ticker', 'N/A')
                            print(f"   {i}. {name}")
                            if ticker and ticker != 'N/A':
                                print(f"      Ticker: {ticker}, CIK: {cik}")
                            else:
                                print(f"      CIK: {cik}")
                        
                        print()
                        print(f"   Enter number (1-{len(preview_results)}) or press Enter to use original input:")
                        selection = input("   Selection: ").strip()
                        
                        if selection.isdigit():
                            idx = int(selection) - 1
                            if 0 <= idx < len(preview_results):
                                selected = preview_results[idx]
                                params['cik'] = selected.get('cik', '')
                                params['entityName'] = selected.get('name', entity_input)
                                if selected.get('ticker'):
                                    params['ticker'] = selected.get('ticker')
                                print(f"   ✓ Selected: {selected.get('name', 'N/A')} (CIK: {selected.get('cik', 'N/A')})")
                            else:
                                params['entityName'] = entity_input
                        else:
                            params['entityName'] = entity_input
                    elif preview_results and len(preview_results) == 1:
                        # Only one result, use it automatically
                        result = preview_results[0]
                        params['cik'] = result.get('cik', '')
                        params['entityName'] = result.get('name', entity_input)
                        if result.get('ticker'):
                            params['ticker'] = result.get('ticker')
                        print(f"   ✓ Found: {result.get('name', 'N/A')} (CIK: {result.get('cik', 'N/A')})")
                    else:
                        # No preview results or error, use original input
                        params['entityName'] = entity_input
    print()
    
    # Filing category (browse filing types)
    print("3. Filing category (Browse filing types)")
    print("   Common forms: 3, 4, 5, 10-K, 10-Q, 8-K, etc.")
    print("   Enter multiple forms separated by commas (e.g., 3,4,5 or 10-K,10-Q)")
    form_types_input = get_user_input("   Enter form types", allow_blank=True)
    if form_types_input:
        params['formTypes'] = [ft.strip() for ft in form_types_input.split(',') if ft.strip()]
    print()
    
    # Filed date range
    print("4. Filed date range")
    print("   Format: YYYY-MM-DD (e.g., 2025-11-01)")
    print("   Default: All (since 2001)")
    
    date_from = get_user_input("   Filed from (YYYY-MM-DD)", default="2001-01-01", allow_blank=True)
    if date_from:
        params['dateFrom'] = date_from
    
    date_to = get_user_input("   Filed to (YYYY-MM-DD)", default=datetime.now().strftime("%Y-%m-%d"), allow_blank=True)
    if date_to:
        params['dateTo'] = date_to
    print()
    
    # Principal executive office in (location)
    print("5. Principal executive office in")
    print("   Enter state, city, or location")
    params['principalOffice'] = get_user_input("   Enter location", allow_blank=True)
    print()
    
    # Additional filters
    print("6. Additional Filters")
    print("   Reporting for (entity name)")
    params['reportingFor'] = get_user_input("   Enter reporting for entity", allow_blank=True)
    
    print("   Located (state/city)")
    params['located'] = get_user_input("   Enter located location", allow_blank=True)
    
    print("   Incorporated (state)")
    params['incorporated'] = get_user_input("   Enter incorporated state", allow_blank=True)
    
    print("   File number")
    params['fileNumber'] = get_user_input("   Enter file number", allow_blank=True)
    
    print("   Film number")
    params['filmNumber'] = get_user_input("   Enter film number", allow_blank=True)
    print()
    
    # Column selection
    print("7. Column Selection")
    print("   Available columns: Filed, Reporting for, CIK, Located, Incorporated, File number, Film number")
    print("   Enter columns to show (comma-separated, or leave blank for all)")
    columns_input = get_user_input("   Enter columns", allow_blank=True)
    if columns_input:
        params['columns'] = [col.strip() for col in columns_input.split(',') if col.strip()]
    print()
    
    return params

def search_by_cik_submissions(cik: str, form_types: Optional[List[str]] = None, 
                              start_date: Optional[str] = None, 
                              end_date: Optional[str] = None,
                              keywords: Optional[str] = None) -> Dict[str, Any]:
    """
    Search using SEC Submissions API (official API)
    
    Args:
        cik: Company CIK (10 digits, with or without leading zeros)
        form_types: Optional list of form types to filter
        start_date: Optional start date (YYYY-MM-DD)
        end_date: Optional end date (YYYY-MM-DD)
        keywords: Optional keywords (not supported by this API, but kept for compatibility)
    
    Returns:
        Dict with company info and filings (limited to MAX_RESULTS)
    """
    session = create_session()
    
    # Pad CIK to 10 digits
    cik_padded = str(cik).zfill(10)
    url = f"{SEC_DATA_URL}/submissions/CIK{cik_padded}.json"
    
    print(f"🔍 Querying Submissions API: {url}")
    print(f"   CIK: {cik_padded}")
    
    try:
        time.sleep(0.1)  # Rate limiting
        response = session.get(url, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        # Extract company info
        company_info = {
            'cik': data.get('cik', ''),
            'name': data.get('name', ''),
            'ticker': data.get('ticker', ''),
            'exchanges': data.get('exchanges', []),
            'sic': data.get('sic', ''),
            'sicDescription': data.get('sicDescription', ''),
        }
        
        # Get filings
        filings_data = data.get('filings', {}).get('recent', {})
        form_list = filings_data.get('form', [])
        filing_date_list = filings_data.get('filingDate', [])
        report_date_list = filings_data.get('reportDate', [])
        accession_list = filings_data.get('accessionNumber', [])
        file_number_list = filings_data.get('fileNumber', [])
        
        # Combine into structured list
        all_filings = []
        for i in range(len(form_list)):
            filing = {
                'form': form_list[i] if i < len(form_list) else None,
                'filingDate': filing_date_list[i] if i < len(filing_date_list) else None,
                'reportDate': report_date_list[i] if i < len(report_date_list) else None,
                'accessionNumber': accession_list[i] if i < len(accession_list) else None,
                'fileNumber': file_number_list[i] if i < len(file_number_list) else None,
            }
            all_filings.append(filing)
        
        # Apply filters
        filtered_filings = all_filings
        if form_types:
            filtered_filings = [f for f in filtered_filings 
                              if f['form'] and any(ft in f['form'].upper() for ft in form_types)]
        
        if start_date:
            filtered_filings = [f for f in filtered_filings 
                              if f['filingDate'] and f['filingDate'] >= start_date]
        
        if end_date:
            filtered_filings = [f for f in filtered_filings 
                              if f['filingDate'] and f['filingDate'] <= end_date]
        
        # Limit to MAX_RESULTS
        limited_filings = filtered_filings[:MAX_RESULTS]
        
        return {
            'success': True,
            'company': company_info,
            'total_filings': len(all_filings),
            'filtered_filings': len(filtered_filings),
            'filings': limited_filings,
        }
        
    except requests.exceptions.RequestException as e:
        return {
            'success': False,
            'error': str(e),
            'error_type': type(e).__name__
        }


def search_edgar_with_browser(query_params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Use browser automation to interact with the actual SEC EDGAR search page
    and extract real document URLs and filing page URLs
    
    Args:
        query_params: Dict with search parameters
    
    Returns:
        Dict with search results including actual URLs from SEC page
    """
    if not SELENIUM_AVAILABLE:
        return {
            'success': False,
            'error': 'Selenium not available. Install with: pip install selenium'
        }
    
    print("🌐 Opening browser to interact with SEC search page...")
    
    # Setup Chrome options
    chrome_options = Options()
    chrome_options.add_argument('--headless')  # Run in background
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    chrome_options.add_argument(f'user-agent={SEC_USER_AGENT}')
    
    driver = None
    try:
        # Initialize Chrome driver
        # Use webdriver-manager if available, otherwise assume ChromeDriver is in PATH
        if WEBDRIVER_MANAGER_AVAILABLE:
            service = ChromeService(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=chrome_options)
        else:
            # Try to use ChromeDriver from PATH
            driver = webdriver.Chrome(options=chrome_options)
        driver.set_page_load_timeout(30)
        
        # Navigate to SEC search page
        print(f"   Navigating to {SEC_SEARCH_URL}...")
        driver.get(SEC_SEARCH_URL)
        time.sleep(2)  # Wait for page to load
        
        # Fill in search form fields
        wait = WebDriverWait(driver, 10)
        
        # 1. Document word or phrase (keywords)
        if query_params.get('keywords'):
            try:
                keywords_field = wait.until(
                    EC.presence_of_element_located((By.NAME, "keywords"))
                )
                keywords_field.clear()
                keywords_field.send_keys(query_params['keywords'])
                print(f"   ✓ Entered keywords: {query_params['keywords']}")
            except (TimeoutException, NoSuchElementException):
                print("   ⚠️  Keywords field not found")
        
        # 2. Company name, ticker, CIK
        entity_value = None
        if query_params.get('entityName'):
            entity_value = query_params['entityName']
        elif query_params.get('cik'):
            entity_value = query_params['cik']
        elif query_params.get('ticker'):
            entity_value = query_params['ticker']
        
        if entity_value:
            try:
                # Try different possible field names
                entity_field = None
                for field_name in ['entityName', 'company', 'cik', 'ticker', 'search']:
                    try:
                        entity_field = driver.find_element(By.NAME, field_name)
                        break
                    except NoSuchElementException:
                        continue
                
                if not entity_field:
                    # Try by ID or placeholder text
                    try:
                        entity_field = driver.find_element(By.CSS_SELECTOR, 
                            "input[placeholder*='company'], input[placeholder*='CIK'], input[placeholder*='ticker']")
                    except NoSuchElementException:
                        pass
                
                if entity_field:
                    entity_field.clear()
                    entity_field.send_keys(entity_value)
                    print(f"   ✓ Entered entity: {entity_value}")
                else:
                    print("   ⚠️  Entity field not found")
            except Exception as e:
                print(f"   ⚠️  Could not enter entity: {e}")
        
        # 3. Form types
        if query_params.get('formTypes'):
            try:
                # Try to find form type selector
                form_types_str = ','.join(query_params['formTypes'])
                form_field = None
                for field_name in ['formType', 'form', 'category']:
                    try:
                        form_field = driver.find_element(By.NAME, field_name)
                        break
                    except NoSuchElementException:
                        continue
                
                if form_field:
                    form_field.clear()
                    form_field.send_keys(form_types_str)
                    print(f"   ✓ Entered form types: {form_types_str}")
            except Exception as e:
                print(f"   ⚠️  Could not enter form types: {e}")
        
        # 4. Date range
        if query_params.get('dateFrom'):
            try:
                date_from_field = driver.find_element(By.NAME, "dateFrom")
                date_from_field.clear()
                # Convert YYYY-MM-DD to MM/DD/YYYY
                try:
                    date_obj = datetime.strptime(query_params['dateFrom'], '%Y-%m-%d')
                    date_from_str = date_obj.strftime('%m/%d/%Y')
                except ValueError:
                    date_from_str = query_params['dateFrom']
                date_from_field.send_keys(date_from_str)
                print(f"   ✓ Entered date from: {date_from_str}")
            except NoSuchElementException:
                print("   ⚠️  Date from field not found")
        
        if query_params.get('dateTo'):
            try:
                date_to_field = driver.find_element(By.NAME, "dateTo")
                date_to_field.clear()
                try:
                    date_obj = datetime.strptime(query_params['dateTo'], '%Y-%m-%d')
                    date_to_str = date_obj.strftime('%m/%d/%Y')
                except ValueError:
                    date_to_str = query_params['dateTo']
                date_to_field.send_keys(date_to_str)
                print(f"   ✓ Entered date to: {date_to_str}")
            except NoSuchElementException:
                print("   ⚠️  Date to field not found")
        
        # 5. Principal office
        if query_params.get('principalOffice'):
            try:
                office_field = driver.find_element(By.NAME, "principalOffice")
                office_field.clear()
                office_field.send_keys(query_params['principalOffice'])
                print(f"   ✓ Entered principal office: {query_params['principalOffice']}")
            except NoSuchElementException:
                print("   ⚠️  Principal office field not found")
        
        # 6. Additional filters
        if query_params.get('reportingFor'):
            try:
                reporting_field = driver.find_element(By.NAME, "reportingFor")
                reporting_field.clear()
                reporting_field.send_keys(query_params['reportingFor'])
                print(f"   ✓ Entered reporting for: {query_params['reportingFor']}")
            except NoSuchElementException:
                # Try alternative field names
                try:
                    reporting_field = driver.find_element(By.NAME, "reporting")
                    reporting_field.clear()
                    reporting_field.send_keys(query_params['reportingFor'])
                    print(f"   ✓ Entered reporting for: {query_params['reportingFor']}")
                except NoSuchElementException:
                    print("   ⚠️  Reporting for field not found")
        
        if query_params.get('located'):
            try:
                located_field = driver.find_element(By.NAME, "located")
                located_field.clear()
                located_field.send_keys(query_params['located'])
                print(f"   ✓ Entered located: {query_params['located']}")
            except NoSuchElementException:
                print("   ⚠️  Located field not found")
        
        if query_params.get('incorporated'):
            try:
                incorporated_field = driver.find_element(By.NAME, "incorporated")
                incorporated_field.clear()
                incorporated_field.send_keys(query_params['incorporated'])
                print(f"   ✓ Entered incorporated: {query_params['incorporated']}")
            except NoSuchElementException:
                print("   ⚠️  Incorporated field not found")
        
        if query_params.get('fileNumber'):
            try:
                file_number_field = driver.find_element(By.NAME, "fileNumber")
                file_number_field.clear()
                file_number_field.send_keys(query_params['fileNumber'])
                print(f"   ✓ Entered file number: {query_params['fileNumber']}")
            except NoSuchElementException:
                print("   ⚠️  File number field not found")
        
        if query_params.get('filmNumber'):
            try:
                film_number_field = driver.find_element(By.NAME, "filmNumber")
                film_number_field.clear()
                film_number_field.send_keys(query_params['filmNumber'])
                print(f"   ✓ Entered film number: {query_params['filmNumber']}")
            except NoSuchElementException:
                print("   ⚠️  Film number field not found")
        
        # 7. Column selection (if supported by the page)
        if query_params.get('columns'):
            try:
                # Try to find column selection controls (checkboxes or dropdowns)
                # This might be in a settings/options menu or as checkboxes
                columns_to_show = query_params['columns']
                print(f"   ✓ Column selection requested: {', '.join(columns_to_show)}")
                # Note: Column selection might require clicking on UI elements
                # This would need to be implemented based on the actual SEC page structure
            except Exception as e:
                print(f"   ⚠️  Could not set column selection: {e}")
        
        # Submit the form
        print("   Submitting search form...")
        try:
            # Try to find submit button
            submit_button = None
            for button_text in ['Search', 'Submit', 'Find']:
                try:
                    submit_button = driver.find_element(By.XPATH, 
                        f"//button[contains(text(), '{button_text}')] | //input[@type='submit' and contains(@value, '{button_text}')]")
                    break
                except NoSuchElementException:
                    continue
            
            if submit_button:
                submit_button.click()
            else:
                # Try pressing Enter on the form
                from selenium.webdriver.common.keys import Keys
                driver.find_element(By.TAG_NAME, "body").send_keys(Keys.RETURN)
            
            # Wait for results to load
            print("   Waiting for results...")
            time.sleep(3)
            
            # Get current URL (might have changed after search)
            current_url = driver.current_url
            print(f"   Current page: {current_url}")
            
            # Extract results from the page
            results = []
            
            # Try to find result elements - SEC search results are typically in tables or divs
            try:
                # Wait a bit more for results to render
                time.sleep(2)
                
                # Multiple selectors to try for finding results
                result_selectors = [
                    "tr[data-row-key]",  # Ant Design table rows
                    ".result-row",
                    ".filing-item",
                    "table tbody tr",
                    ".search-result",
                    "[class*='result']",
                    "[class*='filing']",
                    "tbody tr"
                ]
                
                result_elements = []
                for selector in result_selectors:
                    try:
                        elements = driver.find_elements(By.CSS_SELECTOR, selector)
                        if elements and len(elements) > 0:
                            result_elements = elements
                            print(f"   ✓ Found results using selector: {selector}")
                            break
                    except:
                        continue
                
                # If no results found, try getting all links on the page
                if not result_elements:
                    print("   ⚠️  No result containers found, extracting all links...")
                    all_links = driver.find_elements(By.TAG_NAME, "a")
                    # Filter for SEC filing links
                    filing_links = [link for link in all_links 
                                  if link.get_attribute('href') and 
                                  ('edgar' in link.get_attribute('href').lower() or 
                                   'sec.gov' in link.get_attribute('href').lower())]
                    if filing_links:
                        result_elements = filing_links[:MAX_RESULTS]
                
                for idx, element in enumerate(result_elements[:MAX_RESULTS]):
                    try:
                        result_data = {}
                        
                        # Extract text content
                        text = element.text
                        if text:
                            result_data['text'] = text
                        
                        # Extract filing page URL first
                        links = element.find_elements(By.TAG_NAME, "a")
                        filing_page_url = None
                        
                        for link in links:
                            href = link.get_attribute('href')
                            if not href:
                                continue
                            
                            href_lower = href.lower()
                            
                            # Find filing page (index.htm)
                            if any(pattern in href_lower for pattern in ['/Archives/edgar/data/', '/cgi-bin/viewer']):
                                if 'index' in href_lower or 'index.htm' in href_lower:
                                    filing_page_url = href
                                    break
                        
                        # If element itself is a link to filing page
                        if not filing_page_url and element.tag_name == 'a':
                            href = element.get_attribute('href')
                            if href and ('index' in href.lower() or '/Archives/edgar/data/' in href.lower()):
                                filing_page_url = href
                        
                        # Store filing page URL
                        if filing_page_url:
                            result_data['filingPageUrl'] = filing_page_url
                        
                        # Try to extract form type, date, company from text
                        if text:
                            # Extract form type (e.g., "Form 4", "10-K", "4")
                            form_patterns = [
                                r'Form\s+([A-Z0-9-]+)',
                                r'\b([A-Z0-9-]+)\s+\(Form',
                                r'^([A-Z0-9-]+)\s+',
                            ]
                            for pattern in form_patterns:
                                form_match = re.search(pattern, text, re.IGNORECASE)
                                if form_match:
                                    result_data['form'] = form_match.group(1).strip()
                                    break
                            
                            # Extract date (multiple formats)
                            date_patterns = [
                                r'(\d{1,2}/\d{1,2}/\d{4})',  # MM/DD/YYYY
                                r'(\d{4}-\d{2}-\d{2})',      # YYYY-MM-DD
                                r'(\d{2}/\d{2}/\d{2})',      # MM/DD/YY
                            ]
                            for pattern in date_patterns:
                                date_match = re.search(pattern, text)
                                if date_match:
                                    result_data['filingDate'] = date_match.group(1)
                                    break
                            
                            # Extract company name (if present)
                            company_match = re.search(r'([A-Z][A-Za-z0-9\s&.,/]+(?:Inc|Corp|LLC|Ltd|Company))', text)
                            if company_match:
                                result_data['company'] = company_match.group(1).strip()
                        
                        if result_data:
                            results.append(result_data)
                    except Exception as e:
                        print(f"   ⚠️  Error extracting result {idx}: {e}")
                        continue
                
                print(f"   ✓ Extracted {len(results)} results with URLs")
                
            except Exception as e:
                print(f"   ⚠️  Error finding results: {e}")
                # Get page source as fallback
                page_source = driver.page_source
                result_data = {
                    'html_preview': page_source[:2000],
                    'url': current_url
                }
                results.append(result_data)
            
            return {
                'success': True,
                'method': 'browser_automation',
                'url': current_url,
                'results': results,
                'total_found': len(results)
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Error submitting form: {str(e)}'
            }
    
    except Exception as e:
        return {
            'success': False,
            'error': f'Browser automation error: {str(e)}'
        }
    finally:
        if driver:
            driver.quit()
            print("   ✓ Browser closed")

def display_results(results: Dict[str, Any], search_params: Dict[str, Any]):
    """Display search results in a formatted way"""
    print()
    print("=" * 80)
    print("SEARCH RESULTS")
    print("=" * 80)
    print()
    
    # Try Browser Automation first (most reliable for getting real URLs)
    if SELENIUM_AVAILABLE:
        print("📋 Method 1: Browser Automation (SEC Search Page)")
        print("-" * 80)
        browser_result = search_edgar_with_browser(search_params)
        
        if browser_result.get('success'):
            print(f"✅ Success!")
            print(f"   Search URL: {browser_result.get('url', 'N/A')}")
            print(f"   Results found: {browser_result.get('total_found', 0)}")
            print()
            
            results_list = browser_result.get('results', [])
            if results_list:
                print("   Filings with URLs:")
                for i, result in enumerate(results_list, 1):
                    form = result.get('form', 'N/A')
                    filing_date = result.get('filingDate', 'N/A')
                    document_url = result.get('documentUrl', '')
                    filing_page_url = result.get('filingPageUrl', '')
                    
                    print(f"   {i}. Form {form} - Filed: {filing_date}")
                    if filing_page_url:
                        print(f"      📋 Filing Page URL: {filing_page_url}")
                    if result.get('text'):
                        # Show preview of text
                        text_preview = result.get('text', '')[:100]
                        if len(result.get('text', '')) > 100:
                            text_preview += "..."
                        print(f"      Preview: {text_preview}")
                    print()
            else:
                print("   No results found or could not extract results from page.")
        else:
            print(f"❌ Error: {browser_result.get('error')}")
        print()
    
    # Try Submissions API if we have a CIK
    if search_params.get('cik'):
        print("📋 Method 2: Submissions API (Official)")
        print("-" * 80)
        result1 = search_by_cik_submissions(
            cik=search_params['cik'],
            form_types=search_params.get('formTypes'),
            start_date=search_params.get('dateFrom'),
            end_date=search_params.get('dateTo'),
            keywords=search_params.get('keywords')
        )
        
        if result1.get('success'):
            company = result1.get('company', {})
            print(f"✅ Success!")
            print(f"   Company: {company.get('name', 'N/A')}")
            print(f"   Ticker: {company.get('ticker', 'N/A')}")
            print(f"   CIK: {company.get('cik', 'N/A')}")
            print(f"   Total filings found: {result1.get('filtered_filings', 0)}")
            print(f"   Showing: {len(result1.get('filings', []))} (limited to {MAX_RESULTS})")
            print()
            
            filings = result1.get('filings', [])
            if filings:
                print("   Filings:")
                for i, filing in enumerate(filings, 1):
                    cik = company.get('cik', search_params['cik'])
                    accession = filing.get('accessionNumber', '')
                    
                    # Construct filing page URL from CIK and accession
                    filing_page_url = None
                    if cik and accession:
                        cik_padded = str(cik).zfill(10)
                        accession_clean = accession.replace('-', '')
                        if len(accession_clean) >= 12:
                            accession_dashed = f"{accession_clean[:10]}-{accession_clean[10:12]}-{accession_clean[12:]}"
                            base_url = f"{SEC_BASE_URL}/Archives/edgar/data/{cik_padded}/{accession_dashed}"
                            filing_page_url = f"{base_url}/{accession_dashed}-index.htm"
                    
                    print(f"   {i}. Form {filing.get('form', 'N/A')} - Filed: {filing.get('filingDate', 'N/A')}")
                    print(f"      Accession: {filing.get('accessionNumber', 'N/A')}")
                    if filing_page_url:
                        print(f"      📋 Filing Page: {filing_page_url}")
            else:
                print("   No filings found matching criteria.")
        else:
            print(f"❌ Error: {result1.get('error')}")
        print()
    

def main():
    """Main interactive function"""
    print("=" * 80)
    print("SEC EDGAR Search API Test Script")
    print("=" * 80)
    print()
    print("This script allows you to search SEC EDGAR filings with all parameters")
    print("available on the SEC search webpage.")
    print()
    print(f"Results will be limited to {MAX_RESULTS} entries.")
    print()
    
    if SELENIUM_AVAILABLE:
        print("✅ Browser automation enabled - will extract real URLs from SEC page")
        if WEBDRIVER_MANAGER_AVAILABLE:
            print("✅ ChromeDriver manager available - will auto-download ChromeDriver if needed")
        else:
            print("⚠️  ChromeDriver manager not available - ensure ChromeDriver is in PATH")
            print("   Or install webdriver-manager: pip install webdriver-manager")
    else:
        print("⚠️  Browser automation disabled - install selenium for full functionality")
        print("   Install with: pip install selenium")
        print("   Also requires ChromeDriver: https://chromedriver.chromium.org/")
        print("   Or install webdriver-manager: pip install webdriver-manager (auto-downloads ChromeDriver)")
    print()
    
    # Get search parameters from user
    search_params = get_search_parameters()
    
    # Display summary
    print()
    print("=" * 80)
    print("Search Summary")
    print("=" * 80)
    print(f"Keywords: {search_params.get('keywords', '(blank)')}")
    print(f"Entity/CIK/Ticker: {search_params.get('entityName') or search_params.get('cik') or search_params.get('ticker') or '(blank)'}")
    print(f"Form Types: {', '.join(search_params.get('formTypes', [])) if search_params.get('formTypes') else '(blank)'}")
    print(f"Date From: {search_params.get('dateFrom', '(blank)')}")
    print(f"Date To: {search_params.get('dateTo', '(blank)')}")
    print(f"Principal Office: {search_params.get('principalOffice', '(blank)')}")
    print(f"Reporting For: {search_params.get('reportingFor', '(blank)')}")
    print(f"Located: {search_params.get('located', '(blank)')}")
    print(f"Incorporated: {search_params.get('incorporated', '(blank)')}")
    print(f"File Number: {search_params.get('fileNumber', '(blank)')}")
    print(f"Film Number: {search_params.get('filmNumber', '(blank)')}")
    print(f"Columns: {', '.join(search_params.get('columns', [])) if search_params.get('columns') else '(all)'}")
    print()
    
    # Confirm before searching
    confirm = input("Proceed with search? (y/n) [y]: ").strip().lower()
    if confirm and confirm != 'y':
        print("Search cancelled.")
        return
    
    # Perform search and display results
    display_results({}, search_params)
    
    print("=" * 80)
    print("Search Complete")
    print("=" * 80)

if __name__ == "__main__":
    main()
