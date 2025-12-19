import boto3
import os
import json
import logging
import re
import csv
from typing import List, Dict, Any, Set

logger = logging.getLogger()
logger.setLevel(logging.INFO)

class EnhancedKeywordExtractor:
    """Enhanced keyword extraction using real ticker data and Amazon Comprehend"""
    
    def __init__(self, region_name='us-east-1'):
        self.comprehend = boto3.client('comprehend', region_name=region_name)
        self.max_chars_per_request = 5000  # Comprehend limit
        
        # Load ticker data
        self.ticker_to_company = {}
        self.company_to_ticker = {}
        self.all_tickers = set()
        self.all_companies = set()
        
        self._load_ticker_data()
    
    def _load_ticker_data(self):
        """Load ticker and company data from CSV and text files"""
        try:
            # Load from Nasdaq CSV file
            nasdaq_path = os.path.join(os.path.dirname(__file__), 'nasdaq-listed.csv')
            if os.path.exists(nasdaq_path):
                with open(nasdaq_path, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        ticker = row['Symbol'].strip().upper()
                        company = row['Security Name'].strip()
                        
                        # Clean company name for matching
                        clean_company = self._clean_company_name(company)
                        
                        self.ticker_to_company[ticker] = company
                        self.company_to_ticker[clean_company] = ticker
                        self.all_tickers.add(ticker)
                        self.all_companies.add(clean_company)
            
            # Load from NYSE CSV file
            nyse_path = os.path.join(os.path.dirname(__file__), 'nyse-listed.csv')
            if os.path.exists(nyse_path):
                with open(nyse_path, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        ticker = row['ACT Symbol'].strip().upper()
                        company = row['Company Name'].strip()
                        
                        # Clean company name for matching
                        clean_company = self._clean_company_name(company)
                        
                        self.ticker_to_company[ticker] = company
                        self.company_to_ticker[clean_company] = ticker
                        self.all_tickers.add(ticker)
                        self.all_companies.add(clean_company)
            
            # Load additional tickers from text file
            ticker_path = os.path.join(os.path.dirname(__file__), 'nasdaq_tickers.txt')
            if os.path.exists(ticker_path):
                with open(ticker_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        ticker = line.strip().upper()
                        if ticker:
                            self.all_tickers.add(ticker)
            
            # Load additional companies from text file
            company_path = os.path.join(os.path.dirname(__file__), 'nasdaq_companies.txt')
            if os.path.exists(company_path):
                with open(company_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        company = line.strip()
                        if company:
                            clean_company = self._clean_company_name(company)
                            self.all_companies.add(clean_company)
            
            logger.info(f"Loaded {len(self.all_tickers)} tickers and {len(self.all_companies)} companies")
            
        except Exception as e:
            logger.error(f"Error loading ticker data: {str(e)}")
    
    def _clean_company_name(self, company_name: str) -> str:
        """Clean company name for better matching"""
        # Remove common suffixes
        suffixes_to_remove = [
            'inc.', 'inc', 'corp.', 'corp', 'corporation', 'llc', 'ltd.', 'ltd', 'limited',
            'co.', 'co', 'company', 'group', 'holdings', 'technologies', 'systems',
            'solutions', 'motors', 'airlines', 'bank', 'financial', 'capital'
        ]
        
        clean_name = company_name.lower().strip()
        
        # Remove suffixes
        for suffix in suffixes_to_remove:
            clean_name = re.sub(rf'\b{suffix}\b', '', clean_name).strip()
        
        # Remove extra spaces and punctuation
        clean_name = re.sub(r'[^\w\s]', ' ', clean_name)
        clean_name = re.sub(r'\s+', ' ', clean_name).strip()
        
        return clean_name
    
    def extract_tickers_from_text(self, text: str) -> List[str]:
        """Extract ticker symbols from text using loaded ticker data"""
        if not text:
            return []
        
        found_tickers = set()
        
        # Look for ticker matches (1-5 uppercase letters) - be more selective
        ticker_pattern = r'\b[A-Z]{1,5}\b'
        potential_tickers = re.findall(ticker_pattern, text)
        
        for ticker in potential_tickers:
            # Only include if it's a real ticker AND it's likely to be a ticker in context
            if ticker in self.all_tickers:
                # Additional filtering: avoid very short tickers unless they're common
                if len(ticker) >= 3 or ticker in ['BA', 'GE', 'GM', 'AT', 'T']:
                    found_tickers.add(ticker.lower())
        
        return list(found_tickers)
    
    def extract_companies_from_text(self, text: str) -> List[str]:
        """Extract company names from text using loaded company data"""
        if not text:
            return []
        
        found_companies = set()
        text_lower = text.lower()
        
        # Check for company name matches (exact and partial)
        for company in self.all_companies:
            if len(company) > 3:  # Avoid very short matches
                # Exact match
                if company in text_lower:
                    found_companies.add(company)
                    
                    # Also add the ticker for this company
                    if company in self.company_to_ticker:
                        found_companies.add(self.company_to_ticker[company].lower())
                
                # Partial match - only for well-known company names
                else:
                    company_words = company.split()
                    # Only match if the first significant word (usually the company name) appears in text
                    if company_words and len(company_words[0]) > 3:
                        first_word = company_words[0]
                        if first_word in text_lower:
                            # Only add if it's a reasonable company name length
                            if len(first_word) <= 15:  # Avoid very long matches
                                found_companies.add(first_word)
                                
                                # Also add the ticker for this company
                                if company in self.company_to_ticker:
                                    found_companies.add(self.company_to_ticker[company].lower())
        
        # Note: NYSE and Nasdaq data is now loaded from actual CSV files
        # No need for hardcoded mappings since we have comprehensive real data
        
        return list(found_companies)
    
    def extract_keyphrases_from_text(self, text: str, min_confidence: float = 0.8) -> List[str]:
        """Extract key phrases from text using Amazon Comprehend"""
        if not text or len(text.strip()) < 10:
            return []
        
        try:
            # Truncate text if too long
            text_to_analyze = text[:self.max_chars_per_request]
            
            # Call Comprehend API
            response = self.comprehend.detect_key_phrases(
                Text=text_to_analyze,
                LanguageCode='en'
            )
            
            # Extract key phrases with confidence filtering
            keyphrases = []
            for phrase in response.get('KeyPhrases', []):
                if phrase.get('Score', 0) >= min_confidence:
                    keyphrases.append(phrase.get('Text', '').lower())
            
            # Remove duplicates and return
            unique_keyphrases = list(dict.fromkeys(keyphrases))
            logger.info(f"Extracted {len(unique_keyphrases)} key phrases from text")
            
            return unique_keyphrases
            
        except Exception as e:
            logger.error(f"Error extracting key phrases with Comprehend: {str(e)}")
            return []
    
    def extract_entities_from_text(self, text: str, min_confidence: float = 0.7) -> List[str]:
        """Extract entities from text using Amazon Comprehend"""
        if not text or len(text.strip()) < 10:
            return []
        
        try:
            # Truncate text if too long
            text_to_analyze = text[:self.max_chars_per_request]
            
            # Call Comprehend API
            response = self.comprehend.detect_entities(
                Text=text_to_analyze,
                LanguageCode='en'
            )
            
            # Extract relevant entities
            entities = []
            relevant_types = ['ORGANIZATION', 'PERSON', 'LOCATION', 'MONEY', 'EVENT']
            
            for entity in response.get('Entities', []):
                if (entity.get('Score', 0) >= min_confidence and 
                    entity.get('Type') in relevant_types):
                    entities.append(entity.get('Text', '').lower())
            
            # Remove duplicates and return
            unique_entities = list(dict.fromkeys(entities))
            logger.info(f"Extracted {len(unique_entities)} entities from text")
            
            return unique_entities
            
        except Exception as e:
            logger.error(f"Error extracting entities with Comprehend: {str(e)}")
            return []
    
    def extract_key_terms_from_phrases(self, phrases: List[str]) -> List[str]:
        """Extract individual key terms from longer phrases"""
        key_terms = set()
        
        for phrase in phrases:
            # Split phrase into words
            words = phrase.split()
            
            # Add individual words (2+ characters)
            for word in words:
                clean_word = re.sub(r'[^\w]', '', word.lower())
                if len(clean_word) >= 2:
                    key_terms.add(clean_word)
            
            # Add the full phrase if it's not too long
            if len(phrase) <= 25:  # Reasonable phrase length
                key_terms.add(phrase)
        
        return list(key_terms)
    
    def extract_comprehensive_keywords(self, title: str, description: str = "") -> List[str]:
        """
        Extract comprehensive keywords using enhanced approach:
        1. Real ticker data extraction (from Nasdaq files)
        2. Company name extraction (from Nasdaq files)
        3. Amazon Comprehend (AI-based key phrases and entities)
        4. Key term extraction from phrases
        
        Args:
            title: Article title
            description: Article description
            
        Returns:
            Combined list of searchable keywords
        """
        all_keywords = []
        
        # Combine title and description
        full_text = f"{title}. {description}".strip()
        
        # 1. Extract tickers using real data
        tickers = self.extract_tickers_from_text(full_text)
        all_keywords.extend(tickers)
        logger.info(f"Extracted {len(tickers)} tickers: {tickers}")
        
        # 2. Extract company names using real data
        companies = self.extract_companies_from_text(full_text)
        all_keywords.extend(companies)
        logger.info(f"Extracted {len(companies)} companies: {companies}")
        
        # 3. Extract key phrases using Comprehend
        keyphrases = self.extract_keyphrases_from_text(full_text)
        all_keywords.extend(keyphrases)
        
        # 4. Extract entities using Comprehend
        entities = self.extract_entities_from_text(full_text)
        all_keywords.extend(entities)
        
        # 5. Extract individual terms from phrases
        key_terms = self.extract_key_terms_from_phrases(keyphrases + entities)
        all_keywords.extend(key_terms)
        
        # Remove duplicates while preserving order
        unique_keywords = list(dict.fromkeys(all_keywords))
        
        # Filter out very short or common words
        common_words = {
            'the', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by',
            'a', 'an', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has',
            'had', 'do', 'does', 'did', 'will', 'would', 'could', 'should', 'may', 'might',
            'can', 'this', 'that', 'these', 'those', 'i', 'you', 'he', 'she', 'it', 'we', 'they'
        }
        
        # Filter keywords, but allow short ticker symbols (2+ characters)
        filtered_keywords = []
        for kw in unique_keywords:
            # Allow ticker symbols (2+ chars) and longer keywords (3+ chars)
            # Check both lowercase and uppercase versions for tickers
            is_ticker = (len(kw) >= 2 and (kw in self.all_tickers or kw.upper() in self.all_tickers))
            is_valid_keyword = (len(kw) > 2 and kw not in common_words)
            
            if (is_ticker or is_valid_keyword) and len(kw) <= 50:
                filtered_keywords.append(kw)
        
        # Prioritize tickers and companies (more likely to be searched)
        priority_keywords = []
        regular_keywords = []
        
        for kw in filtered_keywords:
            if (kw in tickers or kw in companies or len(kw) <= 10):
                priority_keywords.append(kw)
            else:
                regular_keywords.append(kw)
        
        # Return prioritized keywords (tickers/companies first, then others)
        final_keywords = priority_keywords + regular_keywords
        
        # Return top 20 keywords
        result = final_keywords[:20]
        logger.info(f"Final keywords ({len(result)}): {result}")
        
        return result

def test_enhanced_extraction():
    """Test the enhanced keyword extraction"""
    
    extractor = EnhancedKeywordExtractor()
    
    # Test with the Boeing article
    test_title = "Boeing Settles Wrongful Death Lawsuit Over Former Worker's Suicide Amid Whistleblower Retaliation Claims"
    test_description = "Boeing has reached a $50,000 settlement in a wrongful death lawsuit filed by the family of former employee John Barnett, who died by suicide after raising safety concerns about the company's production practices. The settlement was announced Friday in U.S. District Court in South Carolina."
    
    print(f"🧪 Testing Enhanced Keyword Extraction...")
    print(f"📰 Title: {test_title}")
    print(f"📝 Description: {test_description}")
    print(f"=" * 80)
    
    keywords = extractor.extract_comprehensive_keywords(test_title, test_description)
    
    print(f"🔑 Final extracted keywords: {keywords}")
    print(f"📊 Total keywords: {len(keywords)}")
    
    # Check if core entities are captured
    expected_entities = ['boeing', 'ba']  # Boeing company name and ticker
    found_entities = [kw for kw in keywords if kw in expected_entities]
    
    print(f"🎯 Expected core entities: {expected_entities}")
    print(f"✅ Found core entities: {found_entities}")
    
    if found_entities:
        print(f"✅ SUCCESS: Core entities captured!")
    else:
        print(f"❌ ISSUE: Core entities not captured properly")
    
    # Test with a few more examples
    test_cases = [
        {
            "title": "Apple Reports Record Quarterly Earnings",
            "description": "Apple Inc. announced record-breaking quarterly earnings with strong iPhone sales."
        },
        {
            "title": "Tesla Stock Surges on New Model Y Announcement",
            "description": "Tesla Motors saw its stock price increase significantly following the announcement."
        }
    ]
    
    print(f"\n🧪 Testing additional cases...")
    for i, case in enumerate(test_cases, 1):
        print(f"\n--- Test Case {i} ---")
        keywords = extractor.extract_comprehensive_keywords(case['title'], case['description'])
        print(f"Keywords: {keywords}")
    
    return keywords

if __name__ == "__main__":
    test_enhanced_extraction()
