"""
Test script to mimic the download_and_store_sec_form method from the downloader Lambda.
This script downloads SEC forms and stores them locally in the glue folder for testing.
"""

import os
import re
import requests
from typing import Dict, Any, Optional

# SEC User Agent requirement
SEC_USER_AGENT = os.environ.get('SEC_USER_AGENT', 'Company Name admin@company.com')


def download_sec_form_test(form_data: Dict[str, Any], output_dir: str = ".") -> Optional[str]:
    """
    Download SEC form file and store locally (mimics download_and_store_sec_form from Lambda)
    
    Args:
        form_data: Form metadata (CIK, accession number, form type, filename)
        output_dir: Directory to save the file (default: current directory)
        
    Returns:
        File path if successful, None otherwise
    """
    try:
        # Construct SEC filing URL
        # Format: https://www.sec.gov/Archives/edgar/data/{CIK}/{accession_no}/{filename}
        
        # Support both field name variations (matching Lambda input)
        cik = form_data.get('cik')
        accession = form_data.get('accessionNumber') or form_data.get('accession_number') or form_data.get('accession')
        form_type = form_data.get('formType') or form_data.get('form_type')
        filename = form_data.get('filename')
        
        if not all([cik, accession]):
            error_msg = f"Missing required fields (CIK/accession) for form download: {form_data}"
            print(f"❌ {error_msg}")
            raise ValueError(error_msg)
        
        # Construct accession number with dashes (format: 0001234567-12-345678)
        # Accession numbers are 18 digits, formatted as 10-2-6
        # IMPORTANT: Based on working Lambda logs, SEC uses accession WITH dashes in the directory path
        # The index file is at: {accession-with-dashes}/{accession-with-dashes}-index.htm
        # Example: /Archives/edgar/data/1509282/0001509282-25-000007/0001509282-25-000007-index.htm
        
        # Remove any dashes first to get clean number
        accession_clean = accession.replace('-', '').strip()
        
        # For URL path: use accession WITH dashes (matching working Lambda behavior)
        if len(accession_clean) == 18:
            accession_dashed = f"{accession_clean[:10]}-{accession_clean[10:12]}-{accession_clean[12:]}"
        else:
            accession_dashed = accession_clean
        
        # Build base URL - use accession WITH dashes in path (matching working Lambda)
        base_url = f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_dashed}"
        
        session = requests.Session()
        session.headers.update({'User-Agent': SEC_USER_AGENT})
        
        # Try to download the file
        file_content = None
        file_ext = None
        content_type = None
        
        # Build list of URLs to try (matching Lambda logic exactly)
        # According to SEC EDGAR structure:
        # - Forms are stored at: /Archives/edgar/data/{CIK}/{ACCESSION}/
        # - index.htm contains links to all documents in the filing
        # - The actual XML document may be:
        #   1. {accession}-primary-document.xml
        #   2. doc4.xml or doc{N}.xml (numbered documents)
        #   3. {accession}.txt (but this often contains SGML header + XML)
        # - We should parse index.htm first to find the correct document link
        
        urls_to_try = []
        
        # Priority 1: Download index.htm to find actual document links
        # Based on working Lambda: index file is at {accession-with-dashes}-index.htm
        index_url = f"{base_url}/{accession_dashed}-index.htm"
        urls_to_try.append((index_url, f"{accession_dashed}-index.htm"))
        # Also try plain index.htm as fallback
        index_url_fallback = f"{base_url}/index.htm"
        urls_to_try.append((index_url_fallback, "index.htm"))
        
        # Priority 2: Try known document file patterns (if index.htm fails)
        doc_urls = [
            f"{accession_dashed}-primary-document.xml",
            f"{accession_dashed}-primarydoc.xml",
            "primary-document.xml",
            "doc4.xml",  # Common for Form 4
            "doc1.xml",
            f"{accession_dashed}.xml",
        ]
        for doc_name in doc_urls:
            doc_url = f"{base_url}/{doc_name}"
            urls_to_try.append((doc_url, doc_name))
        
        # Priority 3: Try .txt file (often contains SGML + XML, needs parsing)
        txt_url = f"{base_url}/{accession_dashed}.txt"
        urls_to_try.append((txt_url, f"{accession_dashed}.txt"))
        
        # Priority 4: If RSS feed provided a specific filename, try it
        if filename and filename not in [doc for _, doc in urls_to_try]:
            file_url = f"{base_url}/{filename}"
            urls_to_try.append((file_url, filename))
        
        failed_attempts = []  # Store failed attempts with status codes
        
        print(f"\n{'='*80}")
        print(f"📥 DOWNLOAD TEST: Starting download (mimicking Lambda logic)")
        print(f"{'='*80}")
        print(f"   CIK: {cik}")
        print(f"   Accession: {accession}")
        print(f"   Form Type: {form_type}")
        print(f"   Base URL: {base_url}")
        print(f"   Will try {len(urls_to_try)} URL patterns")
        print(f"{'='*80}\n")
        
        for file_url, file_name in urls_to_try:
            try:
                print(f"🔄 ATTEMPTING: {file_name}")
                print(f"   🔗 URL: {file_url}")
                response = session.get(file_url, timeout=30)
                
                print(f"   📥 RESPONSE: Status={response.status_code}, Size={len(response.content):,} bytes")
                
                if response.status_code == 200:
                    file_content = response.content
                    
                    # Determine file extension and content type (matching Lambda logic)
                    is_index_page = 'index' in file_name.lower() or file_url.endswith('index.htm') or file_url.endswith('index.html')
                    
                    if file_name.endswith('.htm') or file_name.endswith('.html') or is_index_page:
                        # For HTML files (including index pages), try to find the primary document link
                        file_ext = 'html'
                        content_type = 'text/html'
                        
                        # Check if HTML contains document links we should follow
                        # This is the SEC index page that lists available document formats
                        if is_index_page:
                            try:
                                html_text = file_content.decode('utf-8', errors='ignore')
                                
                                print(f"   🔍 Parsing index page for document links...")
                                
                                # Parse the SEC index page table to find document links
                                # The table has rows with links like:
                                # <a href="/Archives/edgar/data/1802974/000089914025001211/xslF345X05/form4.xml">form4.html</a>
                                # <a href="/Archives/edgar/data/1802974/000089914025001211/form4.xml">form4.xml</a>
                                
                                doc_links = []
                                
                                # Match Lambda's exact strategy for finding document links
                                # Strategy 1: Find all .xml file links (prioritize these over .txt)
                                xml_pattern = r'href="([^"]*\.xml[^"]*)"'
                                xml_matches = re.findall(xml_pattern, html_text, re.IGNORECASE)
                                doc_links.extend(xml_matches)
                                print(f"   🔍 Found {len(xml_matches)} XML links in page")
                                
                                # Strategy 2: Look for primary document patterns (highest priority)
                                primary_patterns = [
                                    r'href="([^"]*primary[_-]?document[^"]*\.xml[^"]*)"',
                                    r'href="([^"]*primarydoc[^"]*\.xml[^"]*)"',
                                    r'href="([^"]*document[^"]*\.xml[^"]*)"',
                                    # Common SEC naming: doc4.xml for Form 4
                                    r'href="([^"]*doc\d+\.xml[^"]*)"',
                                ]
                                primary_links = []
                                for pattern in primary_patterns:
                                    matches = re.findall(pattern, html_text, re.IGNORECASE)
                                    primary_links.extend(matches)
                                # Prepend primary links to prioritize them
                                doc_links = primary_links + [link for link in doc_links if link not in primary_links]
                                
                                # Strategy 3: If no XML found, look for .txt files (last resort, contains SGML+XML)
                                if not doc_links:
                                    txt_pattern = r'href="([^"]*\.txt[^"]*)"'
                                    txt_matches = re.findall(txt_pattern, html_text, re.IGNORECASE)
                                    doc_links.extend(txt_matches)
                                    print(f"   🔍 Found {len(txt_matches)} TXT links (fallback)")
                                
                                # Strategy 4: Look for links with accession number
                                if not doc_links:
                                    acc_pattern = rf'href="([^"]*{re.escape(accession_dashed)}[^"]*)"'
                                    acc_matches = re.findall(acc_pattern, html_text, re.IGNORECASE)
                                    doc_links.extend(acc_matches)
                                    print(f"   🔍 Found {len(acc_matches)} links with accession number")
                                
                                # Remove duplicates while preserving order
                                seen = set()
                                unique_doc_links = []
                                for link in doc_links:
                                    if link not in seen:
                                        seen.add(link)
                                        unique_doc_links.append(link)
                                
                                # Sort: XML files first, then others (matching Lambda logic)
                                def link_priority(link):
                                    if link.endswith('.xml'):
                                        return 0  # Highest priority
                                    elif 'primary' in link.lower() or 'document' in link.lower():
                                        return 1
                                    elif 'doc' in link.lower():
                                        return 2
                                    else:
                                        return 3
                                
                                sorted_links = sorted(unique_doc_links, key=link_priority)
                                
                                print(f"   📋 Found {len(sorted_links)} document links, will try XML first...")
                                
                                # Try each found link - IMPORTANT: We need to download the actual document, not save the index page
                                document_found = False
                                for doc_link in sorted_links[:10]:  # Try up to 10 links (matching Lambda)
                                    # Handle relative URLs (matching Lambda logic exactly)
                                    if doc_link.startswith('/'):
                                        doc_link = f"https://www.sec.gov{doc_link}"
                                    elif not doc_link.startswith('http'):
                                        doc_link = f"{base_url}/{doc_link}"
                                    
                                    # Skip if it's the same URL we just tried
                                    if doc_link == file_url:
                                        continue
                                    
                                    print(f"      🔗 Trying link: {doc_link}")
                                    try:
                                        doc_response = session.get(doc_link, timeout=30)
                                        if doc_response.status_code == 200:
                                            doc_content = doc_response.content
                                            content_start = doc_content[:1000].lower()
                                            
                                            is_html = any(indicator in content_start for indicator in [
                                                b'<!doctype html',
                                                b'<html',
                                                b'<head>',
                                                b'<body>',
                                                b'<style',
                                                b'sec form 4',
                                                b'sec form 3',
                                                b'sec form 5',
                                                b'form 4',
                                                b'form 3',
                                                b'form 5',
                                            ])
                                            
                                            is_xml = (doc_content.startswith(b'<?xml') or 
                                                     b'<ownershipDocument' in doc_content or 
                                                     b'<document>' in doc_content or 
                                                     b'<edgarDocument' in doc_content)
                                            
                                            # Prefer HTML if both detected
                                            if is_html:
                                                # Replace file_content with the actual document
                                                file_content = doc_content
                                                file_ext = 'html'
                                                content_type = 'text/html'
                                                document_found = True
                                                print(f"      ✅ Found HTML document (will save this instead of index): {doc_link}")
                                                break
                                            elif is_xml and not is_html:
                                                file_content = doc_content
                                                file_ext = 'xml'
                                                content_type = 'application/xml'
                                                document_found = True
                                                print(f"      ✅ Found XML document: {doc_link}")
                                                break
                                            elif b'<sec-header' in content_start:
                                                file_content = doc_content
                                                file_ext = 'txt'
                                                content_type = 'text/plain'
                                                document_found = True
                                                print(f"      ✅ Found SGML header: {doc_link}")
                                                break
                                            else:
                                                file_content = doc_content
                                                file_ext = 'txt'
                                                content_type = 'text/plain'
                                                document_found = True
                                                print(f"      ✅ Found file (format unclear): {doc_link}")
                                                break
                                    except Exception as doc_error:
                                        print(f"      ⚠️ Could not download document link {doc_link}: {doc_error}")
                                        continue
                                
                                if not document_found:
                                    print(f"      ⚠️ Could not find any document links in index page, saving index page as fallback")
                            except Exception as html_parse_error:
                                print(f"      ⚠️ Could not parse HTML for document links: {html_parse_error}")
                                import traceback
                                print(f"      Traceback: {traceback.format_exc()}")
                    
                    elif file_name.endswith('.txt'):
                        content_lower = file_content.lower()
                        if b'<sec-header' in content_lower or b'<acceptance-datetime' in content_lower:
                            print(f"      ⚠️ Downloaded file is an SGML header, looking for actual document...")
                            # Try to find the actual document file (matching Lambda logic)
                            doc_candidates = [
                                f"{accession_dashed}-primary-document.xml",
                                f"{accession_dashed}-primarydoc.xml",
                                "primary-document.xml",
                                "doc4.xml",
                                "doc1.xml",
                                "doc2.xml",
                                "doc3.xml",
                                f"{accession_dashed}.xml",
                            ]
                            
                            found_doc = False
                            for doc_candidate in doc_candidates:
                                doc_url = f"{base_url}/{doc_candidate}"
                                try:
                                    print(f"      🔍 Trying document candidate: {doc_url}")
                                    doc_response = session.get(doc_url, timeout=30)
                                    if doc_response.status_code == 200:
                                        doc_content = doc_response.content
                                        content_sample = doc_content[:100].lower()
                                        is_html = b'<html' in content_sample or b'<!doctype html' in content_sample
                                        is_xml = doc_content.startswith(b'<?xml') or b'<ownershipDocument' in doc_content or b'<document>' in doc_content
                                        
                                        if is_xml:
                                            file_content = doc_content
                                            file_ext = 'xml'
                                            content_type = 'application/xml'
                                            print(f"      ✅ Found XML document file: {doc_url}")
                                            found_doc = True
                                            break
                                        elif is_html:
                                            file_content = doc_content
                                            file_ext = 'html'
                                            content_type = 'text/html'
                                            print(f"      ✅ Found HTML document file: {doc_url}")
                                            found_doc = True
                                            break
                                        else:
                                            file_content = doc_content
                                            file_ext = 'txt'
                                            content_type = 'text/plain'
                                            print(f"      ✅ Found document file (unknown format): {doc_url}")
                                            found_doc = True
                                            break
                                except Exception as doc_error:
                                    continue
                            
                            if not found_doc:
                                print(f"      📄 No separate document file found, accepting SGML header for processing")
                                file_ext = 'txt'
                                content_type = 'text/plain'
                        elif file_content.startswith(b'<?xml'):
                            file_ext = 'xml'
                            content_type = 'application/xml'
                        elif b'<ownershipDocument' in file_content or b'<document>' in file_content or b'<XBRL>' in file_content:
                            file_ext = 'xml'
                            content_type = 'application/xml'
                        elif b'<html' in content_lower or b'<!doctype html' in content_lower:
                            file_ext = 'txt'  # Store as-is, matcher will need to handle HTML
                            content_type = 'text/html'
                        else:
                            file_ext = 'txt'
                            content_type = 'text/plain'
                    elif file_name.endswith('.xml'):
                        # Detect actual content type, not just extension
                        content_sample = file_content[:100].lower()
                        if b'<html' in content_sample or b'<!doctype html' in content_sample:
                            file_ext = 'html'
                            content_type = 'text/html'
                            print(f"      📄 .xml file contains HTML content, treating as HTML")
                        elif file_content.startswith(b'<?xml') or b'<ownershipDocument' in file_content or b'<document>' in file_content:
                            file_ext = 'xml'
                            content_type = 'application/xml'
                        else:
                            file_ext = 'txt'
                            content_type = 'text/plain'
                            print(f"      📄 .xml file format unclear, storing as text")
                    elif file_name.endswith('.pdf'):
                        file_ext = 'pdf'
                        content_type = 'application/pdf'
                    else:
                        # Try to determine from content
                        if file_content.startswith(b'<?xml') or b'<ownershipDocument' in file_content or b'<document>' in file_content:
                            file_ext = 'xml'
                            content_type = 'application/xml'
                        elif b'<html' in file_content.lower():
                            file_ext = 'html'
                            content_type = 'text/html'
                            print(f"      📄 Detected HTML content")
                        else:
                            file_ext = 'txt'
                            content_type = 'text/plain'
                    
                    # Accept whatever content we got
                    if file_content:
                        print(f"   ✅ Successfully downloaded: {file_url} ({len(file_content):,} bytes, type: {file_ext})")
                        break
                else:
                    failed_attempts.append(f"{file_url} (HTTP {response.status_code})")
                    print(f"   ⚠️ HTTP {response.status_code} for {file_url}, trying next option...")
                    
            except requests.exceptions.Timeout as e:
                failed_attempts.append(f"{file_url} (Timeout)")
                print(f"   ⚠️ Timeout downloading {file_url}: {e}")
                continue
            except requests.exceptions.RequestException as e:
                failed_attempts.append(f"{file_url} (Error: {str(e)})")
                print(f"   ⚠️ Could not download {file_url}: {e}")
                continue
        
        if not file_content:
            # Log more details about what we tried
            attempted_urls = [url for url, _ in urls_to_try]
            error_msg = f"Could not download form - all URLs failed: {failed_attempts}"
            print(f"\n❌ {error_msg}")
            print(f"   Attempted URLs: {attempted_urls}")
            print(f"   Failed attempts: {failed_attempts}")
            raise Exception(error_msg)
        
        # Generate filename (matching Lambda S3 key structure)
        filing_date = form_data.get('filingDate') or form_data.get('filing_date', 'unknown')
        filename = f"{form_type}-{cik}-{filing_date}.{file_ext}"
        filepath = os.path.join(output_dir, filename)
        
        # Save file
        with open(filepath, 'wb') as f:
            f.write(file_content)
        
        print(f"\n✅ File saved: {filepath}")
        print(f"   Size: {len(file_content):,} bytes")
        print(f"   Type: {content_type}")
        return filepath
        
    except Exception as e:
        print(f"\n❌ Error downloading/storing SEC form: {e}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        raise


if __name__ == "__main__":
    import sys
    
    # Use the EXACT input payload from the working Lambda logs
    # Input: {"formType": "form4", "cik": "1509282", "accessionNumber": "000150928225000007", "filename": "0001509282-25-000007-index.htm", "filingDate": "2025-10-31"}
    test_form = {
        'cik': '1509282',
        'accessionNumber': '000150928225000007',  # No dashes in input
        'formType': 'form4',
        'filingDate': '2025-10-31',
        'filename': '0001509282-25-000007-index.htm'  # Optional, but matches logs
    }
    
    # Get output directory (glue folder)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(script_dir, '..')  # Go up one level to glue folder
    
    print("="*80)
    print("TEST: Downloading SEC form using EXACT Lambda input payload")
    print("="*80)
    print(f"Form: {test_form}")
    print(f"Output directory: {output_dir}")
    print("="*80)
    print("\nExpected behavior from logs:")
    print("  1. Try: https://www.sec.gov/Archives/edgar/data/1509282/0001509282-25-000007/index.htm (404)")
    print("  2. Try: https://www.sec.gov/Archives/edgar/data/1509282/0001509282-25-000007/0001509282-25-000007-index.htm (200)")
    print("  3. Find link: https://www.sec.gov/Archives/edgar/data/1350653/000150928225000007/xslF345X05/form4-10312025_061002.xml")
    print("  4. Download HTML document")
    print("="*80)
    
    try:
        filepath = download_sec_form_test(test_form, output_dir=output_dir)
        if filepath:
            print(f"\n✅ Success! File saved to: {filepath}")
        else:
            print(f"\n❌ Failed to download form")
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        sys.exit(1)

