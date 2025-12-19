import re
import unicodedata
from typing import List, Set, Dict
import logging

logger = logging.getLogger(__name__)

class DeterministicTokenizer:
    """
    Deterministic tokenizer for news articles that ensures consistent, 
    lossless tokenization without AI dependency.
    """
    
    def __init__(self):
        # Curated allowlist of important short terms that should always be indexed
        self.short_term_allowlist = {
            'ai', 'uk', 'us', 'eu', 'ipo', 'ev', 'g7', 'g20', 'qe', 'qe2', 
            'lvmh', 'xai', 'ceo', 'cfo', 'cto', 'ipo', 'm&a', 'pe', 'vc',
            'nyse', 'nasdaq', 'sec', 'fed', 'cpi', 'gdp', 'pmi', 'un', 'eu',
            'brexit', 'covid', 'covid-19', 'ai', 'ml', 'iot', 'api', 'sdk',
            'saas', 'paas', 'iaas', 'b2b', 'b2c', 'c2c', 'fintech', 'deeptech'
        }
        
        # Synonym/alias mapping for better recall
        self.synonyms = {
            'ai': ['artificial intelligence', 'machine intelligence'],
            'artificial intelligence': ['ai'],
            'us': ['united states', 'usa', 'america'],
            'united states': ['us', 'usa', 'america'],
            'uk': ['united kingdom', 'britain', 'england'],
            'united kingdom': ['uk', 'britain', 'england'],
            'eu': ['european union', 'europe'],
            'european union': ['eu', 'europe'],
            'ev': ['electric vehicle', 'electric vehicles'],
            'electric vehicle': ['ev'],
            'electric vehicles': ['ev'],
            'ipo': ['initial public offering'],
            'initial public offering': ['ipo'],
            'm&a': ['mergers and acquisitions', 'merger', 'acquisition'],
            'mergers and acquisitions': ['m&a'],
            'ceo': ['chief executive officer'],
            'cfo': ['chief financial officer'],
            'cto': ['chief technology officer'],
            'covid-19': ['covid', 'coronavirus'],
            'covid': ['covid-19', 'coronavirus'],
            'coronavirus': ['covid', 'covid-19']
        }
        
        # Common financial/tech terms that should be preserved as-is
        self.preserve_terms = {
            's&p', 's&p500', 'nasdaq', 'nyse', 'dow', 'ftse', 'dax', 'nikkei',
            'etf', 'etfs', 'reit', 'reits', 'crypto', 'bitcoin', 'ethereum',
            'tesla', 'apple', 'microsoft', 'google', 'amazon', 'meta', 'nvidia',
            'amd', 'intel', 'ibm', 'oracle', 'salesforce', 'adobe', 'netflix'
        }
    
    def normalize_text(self, text: str) -> str:
        """
        Normalize text for consistent tokenization:
        - Convert to lowercase
        - Unicode normalization
        - Handle special characters
        """
        if not text:
            return ""
        
        # Unicode normalization
        text = unicodedata.normalize('NFKC', text)
        
        # Convert to lowercase
        text = text.lower()
        
        # Handle curly quotes and special characters
        text = text.replace('"', '"').replace('"', '"')
        text = text.replace(''', "'").replace(''', "'")
        text = text.replace('–', '-').replace('—', '-')
        
        return text
    
    def handle_possessives(self, text: str) -> str:
        """
        Handle possessives by creating both possessive and non-possessive versions.
        e.g., "Walmart's" -> "walmart's" and "walmart"
        """
        # Find possessives and create variations
        possessive_pattern = r"(\w+)'s\b"
        variations = []
        
        # Original text
        variations.append(text)
        
        # Remove possessives
        without_possessive = re.sub(possessive_pattern, r'\1', text)
        if without_possessive != text:
            variations.append(without_possessive)
        
        # Keep possessives but also add non-possessive version
        possessive_with_variation = re.sub(possessive_pattern, r"\1's \1", text)
        if possessive_with_variation != text:
            variations.append(possessive_with_variation)
        
        return " ".join(variations)
    
    def split_text(self, text: str) -> List[str]:
        """
        Split text into tokens using whitespace, hyphens, and punctuation.
        Preserve important compound terms.
        """
        # Handle possessives first
        text = self.handle_possessives(text)
        
        # Split on whitespace, hyphens, and most punctuation
        # Keep important punctuation for compound terms
        tokens = re.findall(r'\b\w+(?:[-\.]\w+)*\b', text)
        
        # Also split on hyphens to get individual components
        hyphen_tokens = []
        for token in tokens:
            if '-' in token:
                hyphen_tokens.extend(token.split('-'))
            hyphen_tokens.append(token)
        
        return hyphen_tokens
    
    def filter_tokens(self, tokens: List[str]) -> List[str]:
        """
        Filter tokens based on length and importance.
        Keep short terms from allowlist, longer terms, and important compound terms.
        """
        filtered = []
        
        for token in tokens:
            # Clean the token
            token = token.strip('.,!?;:"()[]{}')
            
            # Skip empty tokens
            if not token:
                continue
            
            # Always keep if in allowlist
            if token in self.short_term_allowlist:
                filtered.append(token)
                continue
            
            # Always keep if in preserve terms
            if token in self.preserve_terms:
                filtered.append(token)
                continue
            
            # Keep tokens of length 3 or more
            if len(token) >= 3:
                filtered.append(token)
                continue
            
            # Keep 2-character tokens if they look like tickers (all caps or mixed case)
            if len(token) == 2 and any(c.isupper() for c in token):
                filtered.append(token)
                continue
        
        return filtered
    
    def expand_synonyms(self, tokens: List[str]) -> List[str]:
        """
        Expand tokens with their synonyms for better recall.
        """
        expanded = set(tokens)  # Use set to avoid duplicates
        
        for token in tokens:
            if token in self.synonyms:
                # Add synonyms
                expanded.update(self.synonyms[token])
                
                # Also add synonyms of synonyms (bidirectional)
                for synonym in self.synonyms[token]:
                    if synonym in self.synonyms:
                        expanded.update(self.synonyms[synonym])
        
        return list(expanded)
    
    def generate_phrasegrams(self, tokens: List[str], max_length: int = 3) -> List[str]:
        """
        Generate n-gram phrases from tokens for better phrase matching.
        """
        phrases = []
        
        for n in range(2, max_length + 1):
            for i in range(len(tokens) - n + 1):
                phrase = " ".join(tokens[i:i+n])
                phrases.append(phrase)
        
        return phrases
    
    def tokenize_article(self, title: str, description: str = "", max_tokens: int = 100) -> Dict[str, List[str]]:
        """
        Main tokenization function for news articles.
        
        Returns:
        {
            'tokens': List[str],           # Individual tokens
            'phrases': List[str],          # N-gram phrases
            'expanded_tokens': List[str]   # Tokens with synonyms
        }
        """
        # Combine title and description
        full_text = f"{title} {description}".strip()
        
        if not full_text:
            return {'tokens': [], 'phrases': [], 'expanded_tokens': []}
        
        # Normalize text
        normalized_text = self.normalize_text(full_text)
        
        # Split into tokens
        raw_tokens = self.split_text(normalized_text)
        
        # Filter tokens
        filtered_tokens = self.filter_tokens(raw_tokens)
        
        # Remove duplicates while preserving order
        unique_tokens = list(dict.fromkeys(filtered_tokens))
        
        # Limit to max_tokens to avoid overly large records
        unique_tokens = unique_tokens[:max_tokens]
        
        # Generate phrases
        phrases = self.generate_phrasegrams(unique_tokens, max_length=3)
        
        # Expand with synonyms
        expanded_tokens = self.expand_synonyms(unique_tokens)
        expanded_tokens = list(dict.fromkeys(expanded_tokens))  # Remove duplicates
        
        return {
            'tokens': unique_tokens,
            'phrases': phrases,
            'expanded_tokens': expanded_tokens
        }
    
    def create_search_tokens(self, query: str) -> List[str]:
        """
        Create search tokens from user query using the same tokenization logic.
        This ensures search terms are processed the same way as indexed terms.
        """
        if not query:
            return []
        
        # Use the same tokenization process
        result = self.tokenize_article(query)
        
        # Return both tokens and phrases for comprehensive search
        search_terms = result['tokens'] + result['phrases']
        
        # Remove duplicates
        return list(dict.fromkeys(search_terms))

# Global instance for reuse
tokenizer = DeterministicTokenizer()
