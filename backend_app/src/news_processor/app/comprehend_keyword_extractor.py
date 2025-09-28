import boto3
import os
import json
import logging
from typing import List, Dict, Any

logger = logging.getLogger()
logger.setLevel(logging.INFO)

class ComprehendKeywordExtractor:
    """Extract keywords using Amazon Comprehend for better accuracy"""
    
    def __init__(self, region_name='us-east-1'):
        self.comprehend = boto3.client('comprehend', region_name=region_name)
        self.max_chars_per_request = 5000  # Comprehend limit
    
    def extract_keyphrases_from_text(self, text: str, min_confidence: float = 0.8) -> List[str]:
        """
        Extract key phrases from text using Amazon Comprehend
        
        Args:
            text: Input text to analyze
            min_confidence: Minimum confidence score for key phrases (0.0-1.0)
            
        Returns:
            List of extracted key phrases
        """
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
        """
        Extract entities from text using Amazon Comprehend
        
        Args:
            text: Input text to analyze
            min_confidence: Minimum confidence score for entities (0.0-1.0)
            
        Returns:
            List of extracted entities
        """
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
    
    def extract_comprehensive_keywords(self, title: str, description: str = "") -> List[str]:
        """
        Extract comprehensive keywords from title and description
        
        Args:
            title: Article title
            description: Article description
            
        Returns:
            Combined list of key phrases and entities
        """
        all_keywords = []
        
        # Combine title and description
        full_text = f"{title}. {description}".strip()
        
        # Extract key phrases
        keyphrases = self.extract_keyphrases_from_text(full_text)
        all_keywords.extend(keyphrases)
        
        # Extract entities
        entities = self.extract_entities_from_text(full_text)
        all_keywords.extend(entities)
        
        # Remove duplicates while preserving order
        unique_keywords = list(dict.fromkeys(all_keywords))
        
        # Filter out very short or common words
        filtered_keywords = [
            kw for kw in unique_keywords 
            if len(kw) > 2 and kw not in ['the', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by']
        ]
        
        # Return top 15 most relevant keywords
        return filtered_keywords[:15]

def test_comprehend_extraction():
    """Test the Comprehend keyword extraction"""
    
    extractor = ComprehendKeywordExtractor()
    
    # Test with your problematic title
    test_title = "New York residents scramble after experiencing massive energy rate hikes: 'Our bill will be almost $600 more'"
    test_description = "Residents across New York state are facing unprecedented electricity rate increases, with some bills doubling overnight. The New York State Energy Commission announced new rate structures affecting millions of customers."
    
    print(f"🧪 Testing Comprehend extraction...")
    print(f"📰 Title: {test_title}")
    print(f"📝 Description: {test_description}")
    
    keywords = extractor.extract_comprehensive_keywords(test_title, test_description)
    
    print(f"🔑 Extracted keywords: {keywords}")
    print(f"📊 Total keywords: {len(keywords)}")
    
    return keywords

if __name__ == "__main__":
    test_comprehend_extraction()
