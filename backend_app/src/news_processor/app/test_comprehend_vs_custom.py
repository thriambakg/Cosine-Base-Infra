#!/usr/bin/env python3
"""
Test script to compare Amazon Comprehend vs custom keyword extraction
"""

import os
import sys
from comprehend_keyword_extractor import ComprehendKeywordExtractor

def test_keyword_extraction_comparison():
    """Compare Comprehend vs custom extraction"""
    
    # Your problematic example
    test_cases = [
        {
            "title": "New York residents scramble after experiencing massive energy rate hikes: 'Our bill will be almost $600 more'",
            "description": "Residents across New York state are facing unprecedented electricity rate increases, with some bills doubling overnight. The New York State Energy Commission announced new rate structures affecting millions of customers.",
            "old_keywords": ["alm", "bl", "cing", "energy", "erie", "expe", "hike", "ke", "le", "mass", "electric", "ge", "ib", "on", "os", "oss", "th", "tri", "u", "electricity costs", "new york", "electric bills", "grid", "times union", "residents", "clean energy", "national grid", "electric bill"]
        },
        {
            "title": "Tesla Stock Surges 15% After Strong Q3 Earnings Beat and EV Market Expansion",
            "description": "Tesla Inc. reported better-than-expected quarterly earnings, beating analyst estimates by a wide margin. The company's electric vehicle sales continued to grow, driven by strong demand in China and Europe.",
            "old_keywords": ["tesla", "stock", "earnings", "ev", "automotive"]
        },
        {
            "title": "Federal Reserve Raises Interest Rates by 0.25% Amid Inflation Concerns",
            "description": "The Federal Reserve announced a quarter-point interest rate increase in response to persistent inflation pressures affecting the economy.",
            "old_keywords": ["federal", "reserve", "interest", "rates", "fed"]
        }
    ]
    
    print("🔬 Comparing Keyword Extraction Methods\n")
    
    # Initialize Comprehend extractor
    try:
        extractor = ComprehendKeywordExtractor()
        print("✅ Amazon Comprehend initialized successfully\n")
    except Exception as e:
        print(f"❌ Error initializing Comprehend: {str(e)}")
        print("💡 Make sure you have AWS credentials configured\n")
        return
    
    for i, case in enumerate(test_cases, 1):
        print(f"📰 Test Case {i}: {case['title'][:60]}...")
        print(f"📝 Description: {case['description'][:80]}...")
        
        # Extract with Comprehend
        try:
            new_keywords = extractor.extract_comprehensive_keywords(
                case['title'], 
                case['description']
            )
            
            print(f"\n🔍 OLD Custom Keywords ({len(case['old_keywords'])}):")
            print(f"   {case['old_keywords']}")
            
            print(f"\n🚀 NEW Comprehend Keywords ({len(new_keywords)}):")
            print(f"   {new_keywords}")
            
            # Analysis
            print(f"\n📊 Analysis:")
            print(f"   - Old method: {len(case['old_keywords'])} keywords (many fragments)")
            print(f"   - New method: {len(new_keywords)} keywords (meaningful phrases)")
            print(f"   - Quality improvement: {'✅ Much better' if len(new_keywords) < len(case['old_keywords']) else '⚠️ Similar count'}")
            
            # Show problematic old keywords
            problematic_old = [kw for kw in case['old_keywords'] if len(kw) <= 3 and kw not in new_keywords]
            if problematic_old:
                print(f"   - Problematic fragments removed: {problematic_old}")
            
        except Exception as e:
            print(f"❌ Error with Comprehend: {str(e)}")
        
        print("\n" + "="*80 + "\n")

if __name__ == "__main__":
    test_keyword_extraction_comparison()
