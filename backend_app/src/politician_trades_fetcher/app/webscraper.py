"""
Web Scraper Helper Class for Congressional PTRs
Scrapes House and Senate financial disclosure forms (PTRs) from public websites
"""

import logging
import requests
import time
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import re
from urllib.parse import urljoin, urlparse
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

class CongressionalPTRScraper:
    """Scraper for Congressional Periodic Transaction Reports (PTRs)"""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
    
    def fetch_house_ptrs(self, target_date: str) -> List[Dict[str, Any]]:
        """
        Fetch House PTRs for a specific date from Clerk of House website
        
        House PTRs are published at: https://clerk.house.gov/public_disc/ptr-pdfs/{year}/
        Files are typically named: {lastname}-{firstname}-{date}.pdf or similar patterns
        
        Args:
            target_date: Date in YYYY-MM-DD format
            
        Returns:
            List of PTR metadata dicts with url, s3_key, filing_date, etc.
        """
        logger.info(f"🏛️ Fetching House PTRs for date: {target_date}")
        
        ptrs = []
        try:
            # House Clerk's website structure
            # URL: https://clerk.house.gov/public_disc/ptr-pdfs/{year}/
            year = target_date[:4]
            base_url = f"https://clerk.house.gov/public_disc/ptr-pdfs/{year}/"
            
            # Try to fetch the directory listing page
            # House Clerk site may have an index page or directory listing
            try:
                # Try common index page names
                index_urls = [
                    base_url,  # Direct directory access
                    f"{base_url}index.html",
                    f"{base_url}index.htm",
                ]
                
                html_content = None
                for index_url in index_urls:
                    try:
                        logger.info(f"🔍 Trying to access House PTR directory: {index_url}")
                        response = self.session.get(index_url, timeout=30)
                        if response.status_code == 200:
                            html_content = response.text
                            logger.info(f"✅ Successfully accessed directory listing")
                            break
                    except Exception as e:
                        logger.debug(f"⚠️ Could not access {index_url}: {e}")
                        continue
                
                if not html_content:
                    logger.warning(f"⚠️ Could not access House PTR directory for {year}")
                    return ptrs
                
                # Find all PDF links
                pdf_pattern = r'href=["\']([^"\']*\.pdf[^"\']*)["\']'
                pdf_matches = re.findall(pdf_pattern, html_content, re.IGNORECASE)
                
                logger.info(f"📋 Found {len(pdf_matches)} PDF links in directory")
                
                # Filter out clearly non-PTR documents
                # Non-PTR indicators: generic filenames like "statistics", "terms", "guide", etc.
                non_ptr_indicators = [
                    'statistics', 'terms_of_service', 'terms', 'guide', 'user', 'manual',
                    'index', 'readme', 'help', 'faq', 'about', 'contact', 'privacy',
                    'duplicate', 'olm', 'ttd', 'oal', 'scsoal', 'artificial-intelligence'
                ]
                
                # Download candidate PTRs - Textract will verify dates and PTR content
                for pdf_link in pdf_matches:
                    # Convert relative URLs to absolute
                    if pdf_link.startswith('/'):
                        pdf_url = f"https://clerk.house.gov{pdf_link}"
                    elif pdf_link.startswith('http'):
                        pdf_url = pdf_link
                    else:
                        pdf_url = urljoin(base_url, pdf_link)
                    
                    # Extract filename
                    filename = pdf_link.split('/')[-1].lower()
                    
                    # Skip obviously non-PTR files
                    is_likely_non_ptr = any(indicator in filename for indicator in non_ptr_indicators)
                    if is_likely_non_ptr:
                        logger.debug(f"⏭️ Skipping likely non-PTR file: {filename}")
                        continue
                    
                    # Extract filer name from filename if possible
                    filer_name = filename.replace('.pdf', '').replace('-', ' ').title()
                    
                    ptr_data = {
                        'filer_name': filer_name,
                        'filing_date': target_date,  # Will be verified/updated by Textract
                        'form_type': 'house_ptr',
                        'url': pdf_url,
                        's3_key': f"trades/{target_date}/house/{filename}"
                    }
                    ptrs.append(ptr_data)
                    logger.info(f"📄 Queueing House PTR candidate for download and verification: {filename}")
                
                logger.info(f"📊 Found {len(ptrs)} House PTR candidates to check (Textract will verify filing date and PTR content)")
                
            except Exception as parse_error:
                logger.error(f"❌ Error parsing House PTR directory: {parse_error}")
                import traceback
                logger.error(f"Traceback: {traceback.format_exc()}")
            
        except Exception as e:
            logger.error(f"❌ Error fetching House PTRs: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
        
        return ptrs
    
    def fetch_senate_ptrs(self, target_date: str) -> List[Dict[str, Any]]:
        """
        Fetch Senate PTRs for a specific date from Senate Ethics website
        
        Senate PTRs are published at: https://efdsearch.senate.gov/search/
        This site uses a search interface that requires form submission
        
        NOTE: Senate PTRs are accessed through a search interface that requires:
        1. Authenticated session (may need to handle cookies/CSRF)
        2. Form submission with date range
        3. Parsing search results
        
        For now, this returns an empty list. Senate PTRs should be downloaded
        manually or accessed through their API if available.
        
        Args:
            target_date: Date in YYYY-MM-DD format
            
        Returns:
            List of PTR metadata dicts with url, s3_key, filing_date, etc.
        """
        logger.info(f"🏛️ Fetching Senate PTRs for date: {target_date}")
        
        ptrs = []
        
        # TODO: Implement Senate PTR fetching
        # Senate eFD search requires:
        # 1. Form submission with date range
        # 2. Authentication/session management
        # 3. Result parsing
        
        logger.warning("⚠️ Senate PTR fetching not yet implemented - requires form submission and session handling")
        logger.info("💡 Senate PTRs can be accessed at: https://efdsearch.senate.gov/search/")
        logger.info("💡 PTRs filed on a specific date can be searched by date range")
        
        # Placeholder implementation
        try:
                
                # For now, try to find links to PDFs with date in URL/filename
                pdf_pattern = r'href=["\']([^"\']*\.pdf[^"\']*)["\']'
                pdf_matches = re.findall(pdf_pattern, html_content, re.IGNORECASE)
                
                logger.info(f"📋 Found {len(pdf_matches)} PDF links on Senate search page")
                
                # Check if PDFs match the target date
                date_formats_in_filename = [
                    target_date.replace('-', ''),  # YYYYMMDD
                    target_date,  # YYYY-MM-DD
                    target_date[5:7] + '-' + target_date[8:10] + '-' + target_date[0:4],  # MM-DD-YYYY
                ]
                
                for pdf_link in pdf_matches:
                    # Convert relative URLs to absolute
                    if pdf_link.startswith('/'):
                        pdf_url = f"https://efdsearch.senate.gov{pdf_link}"
                    elif pdf_link.startswith('http'):
                        pdf_url = pdf_link
                    else:
                        pdf_url = urljoin(search_url, pdf_link)
                    
                    # Check if filename contains target date
                    filename_matches_date = any(date_format in pdf_link for date_format in date_formats_in_filename)
                    
                    if filename_matches_date:
                        filename = pdf_link.split('/')[-1]
                        filer_name = filename.replace('.pdf', '').replace('-', ' ').title()
                        
                        ptr_data = {
                            'filer_name': filer_name,
                            'filing_date': target_date,
                            'form_type': 'senate_ptr',
                            'url': pdf_url,
                            's3_key': f"trades/{target_date}/senate/{filename}"
                        }
                        ptrs.append(ptr_data)
                        logger.info(f"✅ Found Senate PTR: {filename}")
                
                # If we can't find PDFs directly, we might need to use the search form
                # This requires more complex form submission
                if len(ptrs) == 0:
                    logger.warning("⚠️ Senate PTR scraping requires search form submission - basic implementation may not find all PTRs")
                    logger.info("💡 Consider using Senate PTR API or official data feed if available")
                
            except Exception as search_error:
                logger.error(f"❌ Error accessing Senate PTR search: {search_error}")
                import traceback
                logger.error(f"Traceback: {traceback.format_exc()}")
            
        except Exception as e:
            logger.error(f"❌ Error fetching Senate PTRs: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
        
        return ptrs
    
    def extract_filing_date_from_pdf(self, pdf_content: bytes, target_date: str) -> Optional[str]:
        """
        Use AWS Textract to extract filing date from PTR PDF
        
        Args:
            pdf_content: PDF file content as bytes
            target_date: Target date in YYYY-MM-DD format (for validation)
            
        Returns:
            Extracted filing date in YYYY-MM-DD format, or None if not found
        """
        try:
            textract_client = boto3.client('textract')
            
            logger.info(f"🔍 Using Textract to extract filing date from PTR PDF...")
            
            # Call Textract to extract text
            response = textract_client.detect_document_text(
                Document={'Bytes': pdf_content}
            )
            
            # Extract all text from Textract response
            text_lines = []
            for block in response.get('Blocks', []):
                if block.get('BlockType') == 'LINE':
                    text = block.get('Text', '').strip()
                    if text:
                        text_lines.append(text)
            
            full_text = ' '.join(text_lines)
            
            # First, verify this is actually a PTR document
            # Look for PTR-specific keywords
            ptr_keywords = [
                'periodic transaction report',
                'ptr',
                'stock act',
                'financial disclosure',
                'transaction report',
                'clerk of the house',
                'representative',
                'member of congress'
            ]
            
            text_lower = full_text.lower()
            has_ptr_content = any(keyword in text_lower for keyword in ptr_keywords)
            
            if not has_ptr_content:
                logger.warning(f"⚠️ Document does not appear to be a PTR (missing PTR-specific content)")
                return None
            
            # Look for date patterns in the extracted text
            # PTRs typically have dates like "Date Filed: MM/DD/YYYY" or "Filing Date: MM/DD/YYYY"
            date_patterns = [
                r'(?:Date Filed|Filing Date|Date|Report Date)[\s:]*(\d{1,2})[/-](\d{1,2})[/-](\d{4})',
                r'(\d{1,2})[/-](\d{1,2})[/-](\d{4})',  # Generic MM/DD/YYYY or MM-DD-YYYY
                r'(\d{4})[/-](\d{1,2})[/-](\d{1,2})',  # Generic YYYY/MM/DD or YYYY-MM-DD
            ]
            
            target_date_obj = datetime.strptime(target_date, '%Y-%m-%d')
            
            for pattern in date_patterns:
                matches = re.finditer(pattern, full_text, re.IGNORECASE)
                for match in matches:
                    try:
                        # Try to parse the date
                        date_str = match.group(0)
                        # Clean up the date string
                        date_str = re.sub(r'[^\d/-]', '', date_str)
                        
                        # Try different date formats
                        date_formats = [
                            '%m/%d/%Y',
                            '%m-%d-%Y',
                            '%Y/%m/%d',
                            '%Y-%m-%d',
                            '%d/%m/%Y',
                            '%d-%m-%Y',
                        ]
                        
                        parsed_date = None
                        for fmt in date_formats:
                            try:
                                parsed_date = datetime.strptime(date_str, fmt).date()
                                break
                            except ValueError:
                                continue
                        
                        if parsed_date:
                            # Check if this date matches the target date (or is within a few days)
                            date_diff = abs((parsed_date - target_date_obj.date()).days)
                            if date_diff <= 1:  # Allow 1 day tolerance
                                filing_date_str = parsed_date.strftime('%Y-%m-%d')
                                logger.info(f"✅ Extracted filing date: {filing_date_str} (target: {target_date})")
                                return filing_date_str
                            else:
                                logger.debug(f"📅 Found date {parsed_date} but doesn't match target {target_date}")
                    except Exception as date_parse_error:
                        logger.debug(f"⚠️ Could not parse date from match: {match.group(0)}: {date_parse_error}")
                        continue
            
            logger.warning(f"⚠️ Could not extract filing date from PTR PDF (target: {target_date})")
            return None
            
        except Exception as e:
            error_msg = str(e)
            # Handle unsupported document format (e.g., corrupted PDFs, non-PDF files)
            if 'UnsupportedDocumentException' in error_msg or 'UnsupportedDocumentFormat' in error_msg:
                logger.warning(f"⚠️ Document format not supported by Textract (likely not a valid PDF): {error_msg}")
                return None
            logger.error(f"❌ Error extracting filing date with Textract: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return None
    
    def download_ptr_file(self, url: str, s3_key: str, s3_bucket: str, s3_client, target_date: str = None) -> bool:
        """
        Download a PTR file from URL, verify filing date with Textract, and upload to S3
        
        Args:
            url: Source URL of the PTR PDF
            s3_key: Destination S3 key
            s3_bucket: S3 bucket name
            s3_client: Boto3 S3 client
            target_date: Target filing date (YYYY-MM-DD) - if provided, will filter by actual filing date
            
        Returns:
            True if successful and matches target date, False otherwise
        """
        try:
            logger.info(f"📥 Downloading PTR from {url}")
            
            # Download file
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            pdf_content = response.content
            
            # If target_date is provided, extract and verify filing date using Textract
            if target_date:
                extracted_date = self.extract_filing_date_from_pdf(pdf_content, target_date)
                if not extracted_date:
                    logger.warning(f"⚠️ PTR from {url} does not match target date {target_date}, skipping")
                    return False
                logger.info(f"✅ PTR filing date verified via Textract: {extracted_date} (target: {target_date})")
            
            # Upload to S3
            s3_client.put_object(
                Bucket=s3_bucket,
                Key=s3_key,
                Body=pdf_content,
                ContentType='application/pdf'
            )
            
            logger.info(f"✅ Uploaded PTR to S3: {s3_key}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error downloading/uploading PTR: {e}")
            return False
    
    def __del__(self):
        """Cleanup session"""
        if hasattr(self, 'session'):
            self.session.close()

