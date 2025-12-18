"""
Test script to parse full addresses from SEC Forms 3, 4, and 5.
Extracts: Street lines + City, State, Zip
"""
import re
from html import unescape

def extract_full_address(html_content):
    """
    Extract the complete address from SEC form HTML.
    Returns: Full address string with all street lines + city, state, zip
    """
    address_parts = []
    
    # Step 1: Extract all street lines from the table before (Street) label
    # The street address is in a table that appears before the (Street) label
    # Structure: ... name table ... <hr> ... (Last)/(First)/(Middle) table ... street table ... <hr> ... (Street)
    
    # Find the section between the name link and (Street) label
    name_to_street_section = re.search(
        r'<a[^>]*href="[^"]*cgi-bin/browse-edgar[^"]*CIK=\d+[^"]*">[^<]+</a>.*?</table>(.*?)<hr[^>]*>\s*<span[^>]*>\(Street\)',
        html_content, re.IGNORECASE | re.DOTALL
    )
    
    if name_to_street_section:
        section = name_to_street_section.group(1)
        # Find all tables in this section
        all_tables = list(re.finditer(
            r'<table[^>]*border="0"[^>]*width="100%"[^>]*>(.*?)</table>',
            section, re.IGNORECASE | re.DOTALL
        ))
        
        # Find the last table that contains FormData (this should be the street table)
        # Skip tables that contain (Last), (First), (Middle) labels
        street_table_content = None
        for table_match in reversed(all_tables):  # Start from the last table
            table_content = table_match.group(1)
            # Check if this table has FormData and is not the name/Last/First/Middle table
            if 'FormData' in table_content and '(Last)' not in table_content and '(First)' not in table_content:
                street_table_content = table_content
                break
        
        if street_table_content:
            # Extract all street lines from this table
            street_lines = re.findall(
                r'<tr><td><span[^>]*class="FormData"[^>]*>([^<]*)</span></td></tr>',
                street_table_content, re.IGNORECASE | re.DOTALL
            )
            for street_line in street_lines:
                street = unescape(street_line).strip()
                if street:  # Only add non-empty street lines
                    address_parts.append(street)
    
    # Step 2: Extract City, State, Zip from table after (Street) label
    city_state_zip_patterns = [
        # Pattern: (Street) ... </span><table><tr><td><span class="FormData">CITY</span></td><td><span class="FormData">STATE</span></td><td><span class="FormData">ZIP</span></td></tr></table>
        r'\(Street\)[^<]*</span><table[^>]*border="0"[^>]*width="100%"[^>]*>.*?<tr>.*?<td[^>]*width="33%"[^>]*><span[^>]*class="FormData"[^>]*>([^<]+)</span></td>.*?<td[^>]*width="33%"[^>]*><span[^>]*class="FormData"[^>]*>([^<]+)</span></td>.*?<td[^>]*width="33%"[^>]*><span[^>]*class="FormData"[^>]*>([^<]+)</span></td>.*?</tr>.*?</table>',
        # More flexible pattern with </span> tag
        r'\(Street\)[^<]*</span><table[^>]*>.*?<tr>.*?<td[^>]*><span[^>]*class="FormData"[^>]*>([^<]+)</span></td>.*?<td[^>]*><span[^>]*class="FormData"[^>]*>([^<]+)</span></td>.*?<td[^>]*><span[^>]*class="FormData"[^>]*>([^<]+)</span></td>.*?</tr>.*?</table>',
        # Pattern: Look for table between (Street) and (City) labels
        r'\(Street\)[^<]*</span><table[^>]*>.*?<tr>.*?<td[^>]*><span[^>]*class="FormData"[^>]*>([^<]+)</span></td>.*?<td[^>]*><span[^>]*class="FormData"[^>]*>([^<]+)</span></td>.*?<td[^>]*><span[^>]*class="FormData"[^>]*>([^<]+)</span></td>.*?</tr>.*?</table>[^<]*<hr[^>]*>[^<]*\(City\)',
        # Fallback: Pattern without requiring </span>
        r'\(Street\)[^<]*<table[^>]*border="0"[^>]*width="100%"[^>]*>.*?<tr>.*?<td[^>]*width="33%"[^>]*><span[^>]*class="FormData"[^>]*>([^<]+)</span></td>.*?<td[^>]*width="33%"[^>]*><span[^>]*class="FormData"[^>]*>([^<]+)</span></td>.*?<td[^>]*width="33%"[^>]*><span[^>]*class="FormData"[^>]*>([^<]+)</span></td>.*?</tr>.*?</table>',
        # Fallback: More flexible pattern without width attributes
        r'\(Street\)[^<]*<table[^>]*>.*?<tr>.*?<td[^>]*><span[^>]*class="FormData"[^>]*>([^<]+)</span></td>.*?<td[^>]*><span[^>]*class="FormData"[^>]*>([^<]+)</span></td>.*?<td[^>]*><span[^>]*class="FormData"[^>]*>([^<]+)</span></td>.*?</tr>.*?</table>',
    ]
    
    csv_match = None
    for pattern in city_state_zip_patterns:
        csv_match = re.search(pattern, html_content, re.IGNORECASE | re.DOTALL)
        if csv_match:
            break
    
    if csv_match:
        city = unescape(csv_match.group(1)).strip()
        state = unescape(csv_match.group(2)).strip()
        zip_code = unescape(csv_match.group(3)).strip()
        
        # Add city, state, zip to address parts
        if city:
            address_parts.append(city)
        if state:
            address_parts.append(state)
        if zip_code:
            address_parts.append(zip_code)
    
    # Combine all address parts with commas
    full_address = ", ".join(address_parts) if address_parts else None
    
    return full_address, address_parts

def test_form(file_path, form_type):
    """Test address extraction on a specific form file."""
    print(f"\n{'='*70}")
    print(f"Testing {form_type}: {file_path}")
    print('='*70)
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        full_address, address_parts = extract_full_address(html_content)
        
        print(f"\nAddress Parts Extracted:")
        for i, part in enumerate(address_parts, 1):
            print(f"  {i}. {part}")
        
        print(f"\nFull Address:")
        print(f"  {full_address}")
        
        return full_address
        
    except FileNotFoundError:
        print(f"  ERROR: File not found: {file_path}")
        return None
    except Exception as e:
        print(f"  ERROR: {str(e)}")
        return None

if __name__ == "__main__":
    # Test Form 3
    form3_path = r'c:\Users\Thriambak\Downloads\form3-2089676-0000945621-25-001040-2025-11-07.html'
    test_form(form3_path, "Form 3")
    
    # Test Form 4
    form4_path = r'c:\Users\Thriambak\Downloads\form4-1897057-0001193125-25-274204-2025-11-10.html'
    test_form(form4_path, "Form 4")
    
    print(f"\n{'='*70}")
    print("Summary:")
    print("="*70)
    print("Form 3 Expected: 140 LAKESIDE AVENUE, SUITE 100, SEATTLE, WA, 98122")
    print("Form 4 Expected: C/O TEVA PHARMACEUTICAL INDUSTRIES LTD., 124 DVORA HANEVI'A ST., TEL AVIV, L3, 6944020")

