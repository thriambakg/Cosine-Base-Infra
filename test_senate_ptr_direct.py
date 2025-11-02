#!/usr/bin/env python3
"""
Test script to directly GET a Senate PTR URL with proper headers and cookies.
This simulates what happens when clicking a PTR link from search results.
"""
import requests

def test_senate_ptr_direct():
    """Test direct GET to Senate PTR URL with browser-like headers"""
    
    # The PTR URL
    url = "https://efdsearch.senate.gov/search/view/ptr/c6456d94-2e65-4740-87e8-15f307c7e596/"
    
    # Cookies from browser (key cookies: sessionid contains search_agreement flag)
    cookies = {
        's_tslv': '1759275710553',
        'AMCV_345E01D16312552B0A495FAC%40AdobeOrg': '179643557%7CMCIDTS%7C20362%7CMCMID%7C73196593459830584430151924312595268447%7CMCAAMLH-1759880510%7C7%7CMCAAMB-1759880510%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1759282910s%7CNONE%7CvVersion%7C5.5.0',
        'csrftoken': 'fOLLhElm7ROy7HgmYeR5eL6wx37NuJxm',
        'sessionid': 'gAWVGAAAAAAAAAB9lIwQc2VhcmNoX2FncmVlbWVudJSIcy4:1vFdgD:BY9cl7E9zZ63sLlOGTfZW9ZFyuE3nf6DBCk3IB7slPE',
        '33a5c6d97f299a223cb6fc3925909ef7': 'de79d4bd6a12d1ee9fc9665a340a8b9c'
    }
    
    # Headers from browser network tab
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-encoding': 'gzip, deflate, br, zstd',
        'accept-language': 'en-US,en;q=0.9',
        'connection': 'keep-alive',
        'host': 'efdsearch.senate.gov',
        'sec-ch-ua': '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'none',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36'
    }
    
    print("=" * 80)
    print("Testing Senate PTR Direct GET Request")
    print("=" * 80)
    print(f"\nURL: {url}")
    print(f"\nCookies being sent:")
    for name, value in cookies.items():
        print(f"  {name}: {value[:80]}...")
    
    print(f"\nKey Headers:")
    print(f"  User-Agent: {headers['user-agent'][:80]}...")
    print(f"  Accept: {headers['accept'][:80]}...")
    
    print("\n" + "=" * 80)
    print("Making GET request...")
    print("=" * 80)
    
    try:
        response = requests.get(
            url,
            cookies=cookies,
            headers=headers,
            timeout=30,
            allow_redirects=True
        )
        
        print(f"\n✅ Response Status: {response.status_code}")
        print(f"✅ Final URL: {response.url}")
        print(f"✅ Content Length: {len(response.content)} bytes")
        print(f"✅ Content Type: {response.headers.get('Content-Type', 'N/A')}")
        
        # Check response headers
        print(f"\nResponse Headers:")
        for key, value in response.headers.items():
            if key.lower() in ['content-type', 'content-length', 'location', 'set-cookie']:
                print(f"  {key}: {value[:100]}{'...' if len(str(value)) > 100 else ''}")
        
        # Check response content
        response_text = response.text
        print(f"\nResponse Content Analysis:")
        print(f"  Total length: {len(response_text)} characters")
        print(f"  Contains 'Transactions': {'Transactions' in response_text}")
        print(f"  Contains 'table-striped': {'table-striped' in response_text}")
        print(f"  Contains 'Periodic Transaction Report': {'Periodic Transaction Report' in response_text}")
        print(f"  Contains 'agreement_form': {'agreement_form' in response_text}")
        print(f"  Contains 'Get Access': {'Get Access' in response_text}")
        print(f"  Contains '<table': {response_text.count('<table')} table tags found")
        print(f"  Contains '<tbody': {response_text.count('<tbody')} tbody tags found")
        
        # Show preview of content
        print(f"\nFirst 500 characters of response:")
        print("-" * 80)
        print(response_text[:500])
        print("-" * 80)
        
        # Check for transactions table
        if 'Transactions' in response_text and 'table-striped' in response_text:
            print("\n✅ SUCCESS: Found transaction table in response!")
            
            # Try to extract transaction count
            import re
            transaction_count_match = re.search(r'\((\d+)\s+transaction', response_text, re.IGNORECASE)
            if transaction_count_match:
                print(f"   Found {transaction_count_match.group(1)} transaction(s) in the report")
        elif 'agreement_form' in response_text or 'Get Access' in response_text:
            print("\n⚠️ WARNING: Response still contains agreement form")
        else:
            print("\n⚠️ WARNING: Unexpected response content")
        
        # Output full response content
        print("\n" + "=" * 80)
        print("FULL RESPONSE CONTENT:")
        print("=" * 80)
        print(response_text)
        print("=" * 80)
        
    except requests.exceptions.RequestException as e:
        print(f"\n❌ Error making request: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True

if __name__ == "__main__":
    success = test_senate_ptr_direct()
    print("\n" + "=" * 80)
    if success:
        print("Test completed")
    else:
        print("Test failed")
    print("=" * 80)

