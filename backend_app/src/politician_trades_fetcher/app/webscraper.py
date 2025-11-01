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
                
                # Parse HTML to find PDF links that match the target date
                # Look for links to PDF files
                # Date patterns in filenames: YYYYMMDD, YYYY-MM-DD, MM-DD-YYYY
                date_formats_in_filename = [
                    target_date.replace('-', ''),  # YYYYMMDD
                    target_date,  # YYYY-MM-DD
                    target_date[5:7] + '-' + target_date[8:10] + '-' + target_date[0:4],  # MM-DD-YYYY
                ]
                
                # Find all PDF links
                pdf_pattern = r'href=["\']([^"\']*\.pdf[^"\']*)["\']'
                pdf_matches = re.findall(pdf_pattern, html_content, re.IGNORECASE)
                
                logger.info(f"📋 Found {len(pdf_matches)} PDF links in directory")
                
                for pdf_link in pdf_matches:
                    # Convert relative URLs to absolute
                    if pdf_link.startswith('/'):
                        pdf_url = f"https://clerk.house.gov{pdf_link}"
                    elif pdf_link.startswith('http'):
                        pdf_url = pdf_link
                    else:
                        pdf_url = urljoin(base_url, pdf_link)
                    
                    # Check if filename contains target date
                    filename_matches_date = any(date_format in pdf_link for date_format in date_formats_in_filename)
                    
                    if filename_matches_date:
                        # Extract filer name from filename if possible
                        filename = pdf_link.split('/')[-1]
                        filer_name = filename.replace('.pdf', '').replace('-', ' ').title()
                        
                        ptr_data = {
                            'filer_name': filer_name,
                            'filing_date': target_date,
                            'form_type': 'house_ptr',
                            'url': pdf_url,
                            's3_key': f"trades/{target_date}/house/{filename}"
                        }
                        ptrs.append(ptr_data)
                        logger.info(f"✅ Found House PTR: {filename}")
                
                logger.info(f"📊 Found {len(ptrs)} House PTRs for {target_date}")
                
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
        
        Args:
            target_date: Date in YYYY-MM-DD format
            
        Returns:
            List of PTR metadata dicts with url, s3_key, filing_date, etc.
        """
        logger.info(f"🏛️ Fetching Senate PTRs for date: {target_date}")
        
        ptrs = []
        try:
            # Senate Ethics Financial Disclosure search
            # URL: https://efdsearch.senate.gov/search/
            search_url = "https://efdsearch.senate.gov/search/"
            
            # Parse target date
            target_date_obj = datetime.strptime(target_date, '%Y-%m-%d')
            
            # Senate search requires form submission with date range
            # For a specific date, we'll search that date plus/minus 1 day to catch filings
            # that might be dated slightly differently
            date_start = (target_date_obj - timedelta(days=1)).strftime('%m/%d/%Y')
            date_end = (target_date_obj + timedelta(days=1)).strftime('%m/%d/%Y')
            
            try:
                # First, GET the search page to get any CSRF tokens or session cookies
                logger.info(f"🔍 Accessing Senate PTR search page: {search_url}")
                response = self.session.get(search_url, timeout=30)
                response.raise_for_status()
                
                # Parse the search form to extract necessary fields
                html_content = response.text
                
                # Look for form action and any hidden fields
                # Senate site may require specific form submission
                
                # Try to find PTR links in the page or submit search
                # This is a simplified approach - actual implementation may need:
                # 1. Form submission with proper POST data
                # 2. Handling pagination
                # 3. Parsing search results
                
                # For now, try to find links to PDFs with date in URL/filename
                pdf_pattern = r'href=["\']([^"\']*\.pdf[^"\']*)["\']'
                pdf_matches = re.findall(pdf_pattern, html_content, re.IGNORECASE)
                
                logger.info(f"📋 Found {len(pdf_matches)} PDF links on Senate search page")
                
                # Alternative: Try to construct common PTR URL patterns
                # Senate may publish at a known structure
                year = target_date[:4]
                
                # Common Senate PTR URL patterns (may vary)
                common_patterns = [
                    f"https://efdsearch.senate.gov/search/view/{year}/ptr/",
                    f"https://efdsearch.senate.gov/public/view/{year}/ptr/",
                ]
                
                for pattern_url in common_patterns:
                    try:
                        logger.debug(f"🔍 Trying Senate PTR pattern: {pattern_url}")
                        # This would need directory listing or API access
                    except:
                        continue
                
                # If we can't find PDFs directly, we might need to use the search form
                # This requires more complex form submission
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
    
    def download_ptr_file(self, url: str, s3_key: str, s3_bucket: str, s3_client) -> bool:
        """
        Download a PTR file from URL and upload to S3
        
        Args:
            url: Source URL of the PTR PDF
            s3_key: Destination S3 key
            s3_bucket: S3 bucket name
            s3_client: Boto3 S3 client
            
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"📥 Downloading PTR from {url}")
            
            # Download file
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            
            # Upload to S3
            s3_client.put_object(
                Bucket=s3_bucket,
                Key=s3_key,
                Body=response.content,
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

