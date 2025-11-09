"""
Test script for OpenSearch domain connectivity and indexing
Tests:
1. Connection to OpenSearch domain
2. Index creation (if needed)
3. Document indexing (test document)
4. Document search/retrieval (test document)
5. Basic query capabilities
6. Real document queries (excludes test documents)
7. Full-text search on HTML content (simulates AI agent queries)
8. Filtering by ticker symbol
9. Filtering by issuer name
10. Filtering by politician flag
11. Date range queries
12. Complex multi-filter queries (combining form type, date range, and full-text search)
"""

import json
import sys
import argparse
from datetime import datetime
from typing import Dict, Any, Optional

import boto3
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
import urllib3

# Disable SSL warnings for testing
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class OpenSearchTester:
    def __init__(self, endpoint: str, region: str, index_name: str = "sec-filings"):
        """
        Initialize OpenSearch tester
        
        Args:
            endpoint: OpenSearch domain endpoint (e.g., cosine-sec-staging-xxxxx.us-east-1.es.amazonaws.com)
            region: AWS region (e.g., us-east-1)
            index_name: Index name to use for testing
        """
        self.endpoint = endpoint
        self.region = region
        self.index_name = index_name
        self.base_url = f"https://{endpoint}"
        self.session = boto3.Session()
        self.credentials = self.session.get_credentials()
        
    def _make_request(self, method: str, path: str, data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Make an authenticated request to OpenSearch
        
        Args:
            method: HTTP method (GET, POST, PUT, DELETE)
            path: API path (e.g., /_cluster/health)
            data: Optional request body data
            
        Returns:
            Response data as dict
        """
        url = f"{self.base_url}{path}"
        headers = {'Content-Type': 'application/json'}
        
        # Prepare request body
        body = json.dumps(data) if data else None
        
        # Create AWS request
        request = AWSRequest(method=method, url=url, data=body, headers=headers)
        
        # Sign request with SigV4
        SigV4Auth(self.credentials, 'es', self.region).add_auth(request)
        
        # Make request
        http = urllib3.PoolManager()
        response = http.request(
            method,
            url,
            body=body,
            headers=dict(request.headers)
        )
        
        # Parse response
        if response.data:
            try:
                return json.loads(response.data.decode('utf-8'))
            except json.JSONDecodeError:
                return {'raw_response': response.data.decode('utf-8')}
        return {}
    
    def test_connection(self) -> bool:
        """Test connection to OpenSearch domain"""
        print("=" * 80)
        print("Test 1: Connection Test")
        print("=" * 80)
        
        try:
            response = self._make_request('GET', '/_cluster/health')
            
            if 'status' in response:
                print(f"[OK] Connection successful!")
                print(f"   Cluster Status: {response.get('status', 'unknown')}")
                print(f"   Cluster Name: {response.get('cluster_name', 'unknown')}")
                print(f"   Number of Nodes: {response.get('number_of_nodes', 'unknown')}")
                print(f"   Active Shards: {response.get('active_shards', 'unknown')}")
                return True
            else:
                print(f"[ERROR] Connection failed: Unexpected response")
                print(f"   Response: {json.dumps(response, indent=2)}")
                return False
                
        except Exception as e:
            print(f"[ERROR] Connection failed: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def test_index_exists(self) -> bool:
        """Check if index exists"""
        print("\n" + "=" * 80)
        print("Test 2: Index Existence Check")
        print("=" * 80)
        
        try:
            response = self._make_request('GET', f'/{self.index_name}')
            
            if 'error' in response:
                if response.get('error', {}).get('type') == 'index_not_found_exception':
                    print(f"[INFO] Index '{self.index_name}' does not exist yet")
                    return False
                else:
                    print(f"[ERROR] Error checking index: {response.get('error', {})}")
                    return False
            else:
                print(f"[OK] Index '{self.index_name}' exists")
                if 'settings' in response:
                    print(f"   Number of Shards: {response.get('settings', {}).get('index', {}).get('number_of_shards', 'unknown')}")
                    print(f"   Number of Replicas: {response.get('settings', {}).get('index', {}).get('number_of_replicas', 'unknown')}")
                return True
                
        except Exception as e:
            print(f"[ERROR] Error checking index: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def create_index(self) -> bool:
        """Create index with mapping"""
        print("\n" + "=" * 80)
        print("Test 3: Create Index")
        print("=" * 80)
        
        # Index mapping based on SEC form structure
        mapping = {
            "mappings": {
                "properties": {
                    "tradeId": {"type": "keyword"},
                    "formType": {"type": "keyword"},
                    "reportingPersonName": {
                        "type": "text",
                        "fields": {
                            "keyword": {"type": "keyword"}
                        }
                    },
                    "issuerName": {
                        "type": "text",
                        "fields": {
                            "keyword": {"type": "keyword"}
                        }
                    },
                    "tickerSymbol": {"type": "keyword"},
                    "relationship": {"type": "keyword"},
                    "relationshipTypes": {"type": "text"},
                    "relationshipAdditionalText": {"type": "text"},
                    "filingType": {"type": "keyword"},
                    "eventDate": {"type": "date"},
                    "reportingDate": {"type": "date"},
                    "address": {"type": "text"},
                    "signatureName": {"type": "text"},
                    "politician": {"type": "boolean"},
                    "formS3Key": {"type": "keyword"},
                    "amended": {"type": "boolean"},
                    "amendment": {"type": "boolean"},
                    "amendedTradeId": {"type": "keyword"},
                    "htmlContent": {"type": "text"},  # Full HTML content for full-text search
                    "@timestamp": {"type": "date"}
                }
            }
        }
        
        try:
            response = self._make_request('PUT', f'/{self.index_name}', data=mapping)
            
            if 'acknowledged' in response and response.get('acknowledged'):
                print(f"[OK] Index '{self.index_name}' created successfully")
                print(f"   Acknowledged: {response.get('acknowledged')}")
                print(f"   Index: {response.get('index', self.index_name)}")
                return True
            elif 'error' in response:
                error_type = response.get('error', {}).get('type', 'unknown')
                if error_type == 'resource_already_exists_exception':
                    print(f"[INFO] Index '{self.index_name}' already exists")
                    return True
                else:
                    print(f"[ERROR] Error creating index: {response.get('error', {})}")
                    return False
            else:
                print(f"[ERROR] Unexpected response: {json.dumps(response, indent=2)}")
                return False
                
        except Exception as e:
            print(f"[ERROR] Error creating index: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def index_test_document(self) -> Optional[str]:
        """Index a test document"""
        print("\n" + "=" * 80)
        print("Test 4: Index Test Document")
        print("=" * 80)
        
        # Create a test document similar to what Glue job would index
        test_doc = {
            "tradeId": "test_sec_form4_123456_000123456789012345_20251108",
            "formType": "form4",
            "reportingPersonName": "Test Person",
            "issuerName": "Test Company Inc.",
            "tickerSymbol": "TEST",
            "relationship": "Director",
            "relationshipTypes": "Director",
            "filingType": "individual",
            "eventDate": "2025-11-08",
            "reportingDate": "2025-11-08",
            "address": "123 Test Street, Test City, ST 12345",
            "signatureName": "Test Person",
            "politician": 0,  # Use 0/1 instead of boolean to match DynamoDB/Glue job format
            "formS3Key": "trades/2025-11-08/sec/test-form.html",
            "amended": False,
            "amendment": False,
            "htmlContent": "<html><body>Test SEC Form 4 content. This is a test document for OpenSearch indexing.</body></html>",
            "@timestamp": datetime.now().isoformat()
        }
        
        doc_id = test_doc['tradeId']
        
        try:
            response = self._make_request('PUT', f'/{self.index_name}/_doc/{doc_id}', data=test_doc)
            
            if response.get('result') in ['created', 'updated']:
                print(f"[OK] Test document indexed successfully")
                print(f"   Document ID: {doc_id}")
                print(f"   Result: {response.get('result')}")
                print(f"   Version: {response.get('_version', 'N/A')}")
                return doc_id
            else:
                print(f"[ERROR] Unexpected response: {json.dumps(response, indent=2)}")
                return None
                
        except Exception as e:
            print(f"[ERROR] Error indexing document: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def search_test_document(self, doc_id: str) -> bool:
        """Search for the test document"""
        print("\n" + "=" * 80)
        print("Test 5: Search Test Document")
        print("=" * 80)
        
        # Search by document ID
        try:
            response = self._make_request('GET', f'/{self.index_name}/_doc/{doc_id}')
            
            if 'found' in response and response.get('found'):
                print(f"[OK] Document found by ID")
                print(f"   Document ID: {response.get('_id')}")
                print(f"   Source: {json.dumps(response.get('_source', {}), indent=2)}")
                return True
            else:
                print(f"[ERROR] Document not found")
                print(f"   Response: {json.dumps(response, indent=2)}")
                return False
                
        except Exception as e:
            print(f"[ERROR] Error searching document: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def test_search_query(self) -> bool:
        """Test search query capabilities"""
        print("\n" + "=" * 80)
        print("Test 6: Search Query Test")
        print("=" * 80)
        
        # Test a simple search query
        query = {
            "query": {
                "match_all": {}
            },
            "size": 5
        }
        
        try:
            response = self._make_request('POST', f'/{self.index_name}/_search', data=query)
            
            if 'hits' in response:
                total = response.get('hits', {}).get('total', {})
                if isinstance(total, dict):
                    total_count = total.get('value', 0)
                else:
                    total_count = total
                
                print(f"[OK] Search query successful")
                print(f"   Total documents: {total_count}")
                print(f"   Returned: {len(response.get('hits', {}).get('hits', []))}")
                
                # Show first few results
                hits = response.get('hits', {}).get('hits', [])
                if hits:
                    print(f"\n   Sample results:")
                    for i, hit in enumerate(hits[:3], 1):
                        source = hit.get('_source', {})
                        print(f"   {i}. {source.get('tradeId', 'N/A')} - {source.get('reportingPersonName', 'N/A')}")
                
                return True
            else:
                print(f"[ERROR] Unexpected response: {json.dumps(response, indent=2)}")
                return False
                
        except Exception as e:
            print(f"[ERROR] Error executing search query: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def test_filtered_search(self) -> bool:
        """Test filtered search (e.g., by form type)"""
        print("\n" + "=" * 80)
        print("Test 7: Filtered Search Test")
        print("=" * 80)
        
        # Search for form4 documents
        query = {
            "query": {
                "term": {
                    "formType": "form4"
                }
            },
            "size": 10
        }
        
        try:
            response = self._make_request('POST', f'/{self.index_name}/_search', data=query)
            
            if 'hits' in response:
                total = response.get('hits', {}).get('total', {})
                if isinstance(total, dict):
                    total_count = total.get('value', 0)
                else:
                    total_count = total
                
                print(f"[OK] Filtered search successful")
                print(f"   Form Type: form4")
                print(f"   Total matches: {total_count}")
                print(f"   Returned: {len(response.get('hits', {}).get('hits', []))}")
                return True
            else:
                print(f"[ERROR] Unexpected response: {json.dumps(response, indent=2)}")
                return False
                
        except Exception as e:
            print(f"[ERROR] Error executing filtered search: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def cleanup_test_document(self, doc_id: str) -> bool:
        """Delete the test document"""
        print("\n" + "=" * 80)
        print("Test 8: Cleanup Test Document")
        print("=" * 80)
        
        try:
            response = self._make_request('DELETE', f'/{self.index_name}/_doc/{doc_id}')
            
            if response.get('result') == 'deleted':
                print(f"[OK] Test document deleted successfully")
                print(f"   Document ID: {doc_id}")
                return True
            else:
                print(f"[INFO] Document may not exist or already deleted")
                print(f"   Response: {json.dumps(response, indent=2)}")
                return True  # Not a failure if already deleted
                
        except Exception as e:
            print(f"[WARNING] Error deleting test document (non-critical): {e}")
            return True  # Non-critical
    
    def test_query_real_documents(self) -> bool:
        """Query real documents from the index (excluding test documents)"""
        print("\n" + "=" * 80)
        print("Test 9: Query Real Documents")
        print("=" * 80)
        
        # Query for real documents (exclude test documents)
        query = {
            "query": {
                "bool": {
                    "must_not": [
                        {"prefix": {"tradeId": "test_"}}
                    ]
                }
            },
            "size": 10,
            "sort": [{"@timestamp": {"order": "desc"}}]
        }
        
        try:
            response = self._make_request('POST', f'/{self.index_name}/_search', data=query)
            
            if 'hits' in response:
                total = response.get('hits', {}).get('total', {})
                if isinstance(total, dict):
                    total_count = total.get('value', 0)
                else:
                    total_count = total
                
                hits = response.get('hits', {}).get('hits', [])
                
                print(f"[OK] Query for real documents successful")
                print(f"   Total real documents: {total_count}")
                print(f"   Returned: {len(hits)}")
                
                if hits:
                    print(f"\n   Sample real documents:")
                    for i, hit in enumerate(hits[:5], 1):
                        source = hit.get('_source', {})
                        print(f"\n   {i}. Trade ID: {source.get('tradeId', 'N/A')}")
                        print(f"      Form Type: {source.get('formType', 'N/A')}")
                        print(f"      Reporting Person: {source.get('reportingPersonName', 'N/A')}")
                        print(f"      Issuer: {source.get('issuerName', 'N/A')}")
                        print(f"      Ticker: {source.get('tickerSymbol', 'N/A')}")
                        print(f"      Relationship: {source.get('relationship', 'N/A')}")
                        print(f"      Event Date: {source.get('eventDate', 'N/A')}")
                        print(f"      Reporting Date: {source.get('reportingDate', 'N/A')}")
                        print(f"      Politician: {source.get('politician', 'N/A')}")
                        print(f"      S3 Key: {source.get('formS3Key', 'N/A')}")
                else:
                    print(f"\n   [INFO] No real documents found in index")
                    print(f"   This is expected if the Glue job hasn't indexed any real SEC forms yet")
                
                return True
            else:
                print(f"[ERROR] Unexpected response: {json.dumps(response, indent=2)}")
                return False
                
        except Exception as e:
            print(f"[ERROR] Error querying real documents: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def test_fulltext_search_html_content(self) -> bool:
        """Test full-text search on htmlContent field"""
        print("\n" + "=" * 80)
        print("Test 10: Full-Text Search on HTML Content")
        print("=" * 80)
        
        # Search for documents containing specific terms in HTML content
        # This simulates what the AI agent would do
        search_terms = ["Class A Common Stock", "Director", "beneficial ownership"]
        
        for term in search_terms:
            query = {
                "query": {
                    "bool": {
                        "must": [
                            {"match": {"htmlContent": term}}
                        ],
                        "must_not": [
                            {"prefix": {"tradeId": "test_"}}
                        ]
                    }
                },
                "size": 5,
                "_source": ["tradeId", "formType", "reportingPersonName", "issuerName", "tickerSymbol", "eventDate"]
            }
            
            try:
                response = self._make_request('POST', f'/{self.index_name}/_search', data=query)
                
                if 'hits' in response:
                    total = response.get('hits', {}).get('total', {})
                    if isinstance(total, dict):
                        total_count = total.get('value', 0)
                    else:
                        total_count = total
                    
                    hits = response.get('hits', {}).get('hits', [])
                    
                    print(f"\n   Search term: '{term}'")
                    print(f"   Matches: {total_count}")
                    print(f"   Returned: {len(hits)}")
                    
                    if hits:
                        print(f"   Sample results:")
                        for i, hit in enumerate(hits[:3], 1):
                            source = hit.get('_source', {})
                            score = hit.get('_score', 0)
                            print(f"      {i}. {source.get('reportingPersonName', 'N/A')} - {source.get('issuerName', 'N/A')} ({source.get('tickerSymbol', 'N/A')}) [Score: {score:.2f}]")
                    else:
                        print(f"      No results found")
                else:
                    print(f"[ERROR] Unexpected response for term '{term}': {json.dumps(response, indent=2)}")
                    return False
                    
            except Exception as e:
                print(f"[ERROR] Error searching for term '{term}': {e}")
                import traceback
                traceback.print_exc()
                return False
        
        print(f"\n[OK] Full-text search on HTML content completed")
        return True
    
    def test_filter_by_ticker(self) -> bool:
        """Test filtering by ticker symbol"""
        print("\n" + "=" * 80)
        print("Test 11: Filter by Ticker Symbol")
        print("=" * 80)
        
        # First, get documents to find unique tickers (since aggregations may not work on text fields)
        query = {
            "size": 100,  # Get more documents to find unique tickers
            "query": {
                "bool": {
                    "must": [
                        {"exists": {"field": "tickerSymbol"}}
                    ],
                    "must_not": [
                        {"prefix": {"tradeId": "test_"}}
                    ]
                }
            },
            "_source": ["tickerSymbol"]
        }
        
        try:
            response = self._make_request('POST', f'/{self.index_name}/_search', data=query)
            
            if 'hits' in response:
                hits = response.get('hits', {}).get('hits', [])
                
                # Extract unique tickers from results
                tickers = set()
                for hit in hits:
                    source = hit.get('_source', {})
                    ticker = source.get('tickerSymbol')
                    if ticker:
                        tickers.add(ticker)
                
                if tickers:
                    ticker_list = sorted(list(tickers))
                    test_ticker = ticker_list[0]
                    
                    print(f"[OK] Found {len(tickers)} unique tickers in index: {', '.join(ticker_list)}")
                    print(f"   Testing with ticker: {test_ticker}")
                    
                    # Query documents for this ticker using match query (works with text fields)
                    ticker_query = {
                        "query": {
                            "bool": {
                                "must": [
                                    {"match": {"tickerSymbol": test_ticker}}
                                ],
                                "must_not": [
                                    {"prefix": {"tradeId": "test_"}}
                                ]
                            }
                        },
                        "size": 5
                    }
                    
                    ticker_response = self._make_request('POST', f'/{self.index_name}/_search', data=ticker_query)
                    
                    if 'hits' in ticker_response:
                        ticker_hits = ticker_response.get('hits', {}).get('hits', [])
                        total = ticker_response.get('hits', {}).get('total', {})
                        if isinstance(total, dict):
                            total_count = total.get('value', 0)
                        else:
                            total_count = total
                        
                        print(f"   Documents for {test_ticker}: {total_count}")
                        print(f"\n   Sample documents for {test_ticker}:")
                        for i, hit in enumerate(ticker_hits[:3], 1):
                            source = hit.get('_source', {})
                            print(f"      {i}. {source.get('reportingPersonName', 'N/A')} - {source.get('issuerName', 'N/A')} on {source.get('eventDate', 'N/A')}")
                    
                    return True
                else:
                    print(f"[INFO] No tickers found in index")
                    print(f"   This is expected if the Glue job hasn't indexed any real SEC forms yet")
                    return True
            else:
                print(f"[ERROR] Unexpected response: {json.dumps(response, indent=2)}")
                return False
                
        except Exception as e:
            print(f"[ERROR] Error filtering by ticker: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def test_filter_by_issuer(self) -> bool:
        """Test filtering by issuer name"""
        print("\n" + "=" * 80)
        print("Test 12: Filter by Issuer Name")
        print("=" * 80)
        
        # Get unique issuers
        query = {
            "size": 0,
            "aggs": {
                "unique_issuers": {
                    "terms": {
                        "field": "issuerName.keyword",
                        "size": 10
                    }
                }
            },
            "query": {
                "bool": {
                    "must_not": [
                        {"prefix": {"tradeId": "test_"}}
                    ]
                }
            }
        }
        
        try:
            response = self._make_request('POST', f'/{self.index_name}/_search', data=query)
            
            if 'aggregations' in response:
                issuers = response.get('aggregations', {}).get('unique_issuers', {}).get('buckets', [])
                
                if issuers:
                    test_issuer = issuers[0].get('key')
                    issuer_count = issuers[0].get('doc_count', 0)
                    
                    print(f"[OK] Found {len(issuers)} unique issuers in index")
                    print(f"   Testing with issuer: {test_issuer} ({issuer_count} documents)")
                    
                    # Query documents for this issuer
                    issuer_query = {
                        "query": {
                            "bool": {
                                "must": [
                                    {"match": {"issuerName": test_issuer}}
                                ],
                                "must_not": [
                                    {"prefix": {"tradeId": "test_"}}
                                ]
                            }
                        },
                        "size": 5
                    }
                    
                    issuer_response = self._make_request('POST', f'/{self.index_name}/_search', data=issuer_query)
                    
                    if 'hits' in issuer_response:
                        hits = issuer_response.get('hits', {}).get('hits', [])
                        print(f"\n   Sample documents for {test_issuer}:")
                        for i, hit in enumerate(hits[:3], 1):
                            source = hit.get('_source', {})
                            print(f"      {i}. {source.get('reportingPersonName', 'N/A')} - {source.get('tickerSymbol', 'N/A')} on {source.get('eventDate', 'N/A')}")
                    
                    return True
                else:
                    print(f"[INFO] No issuers found in index")
                    return True
            else:
                print(f"[ERROR] Unexpected response: {json.dumps(response, indent=2)}")
                return False
                
        except Exception as e:
            print(f"[ERROR] Error filtering by issuer: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def test_filter_by_politician(self) -> bool:
        """Test filtering by politician flag"""
        print("\n" + "=" * 80)
        print("Test 13: Filter by Politician Flag")
        print("=" * 80)
        
        # Query for politician filings
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"politician": 1}}
                    ],
                    "must_not": [
                        {"prefix": {"tradeId": "test_"}}
                    ]
                }
            },
            "size": 10,
            "sort": [{"eventDate": {"order": "desc"}}]
        }
        
        try:
            response = self._make_request('POST', f'/{self.index_name}/_search', data=query)
            
            if 'hits' in response:
                total = response.get('hits', {}).get('total', {})
                if isinstance(total, dict):
                    total_count = total.get('value', 0)
                else:
                    total_count = total
                
                hits = response.get('hits', {}).get('hits', [])
                
                print(f"[OK] Politician filter query successful")
                print(f"   Total politician filings: {total_count}")
                print(f"   Returned: {len(hits)}")
                
                if hits:
                    print(f"\n   Sample politician filings:")
                    for i, hit in enumerate(hits[:5], 1):
                        source = hit.get('_source', {})
                        print(f"      {i}. {source.get('reportingPersonName', 'N/A')} - {source.get('issuerName', 'N/A')} ({source.get('tickerSymbol', 'N/A')}) on {source.get('eventDate', 'N/A')}")
                else:
                    print(f"\n   [INFO] No politician filings found")
                
                return True
            else:
                print(f"[ERROR] Unexpected response: {json.dumps(response, indent=2)}")
                return False
                
        except Exception as e:
            print(f"[ERROR] Error filtering by politician: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def test_date_range_query(self) -> bool:
        """Test date range queries"""
        print("\n" + "=" * 80)
        print("Test 14: Date Range Query")
        print("=" * 80)
        
        # Query for recent documents (last 30 days)
        from datetime import datetime, timedelta
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        
        query = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "eventDate": {
                                    "gte": start_date.strftime('%Y-%m-%d'),
                                    "lte": end_date.strftime('%Y-%m-%d')
                                }
                            }
                        }
                    ],
                    "must_not": [
                        {"prefix": {"tradeId": "test_"}}
                    ]
                }
            },
            "size": 10,
            "sort": [{"eventDate": {"order": "desc"}}]
        }
        
        try:
            response = self._make_request('POST', f'/{self.index_name}/_search', data=query)
            
            if 'hits' in response:
                total = response.get('hits', {}).get('total', {})
                if isinstance(total, dict):
                    total_count = total.get('value', 0)
                else:
                    total_count = total
                
                hits = response.get('hits', {}).get('hits', [])
                
                print(f"[OK] Date range query successful")
                print(f"   Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
                print(f"   Total documents: {total_count}")
                print(f"   Returned: {len(hits)}")
                
                if hits:
                    print(f"\n   Sample recent documents:")
                    for i, hit in enumerate(hits[:5], 1):
                        source = hit.get('_source', {})
                        print(f"      {i}. {source.get('eventDate', 'N/A')} - {source.get('reportingPersonName', 'N/A')} - {source.get('issuerName', 'N/A')} ({source.get('tickerSymbol', 'N/A')})")
                else:
                    print(f"\n   [INFO] No documents found in date range")
                
                return True
            else:
                print(f"[ERROR] Unexpected response: {json.dumps(response, indent=2)}")
                return False
                
        except Exception as e:
            print(f"[ERROR] Error executing date range query: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def test_complex_query(self) -> bool:
        """Test a complex query combining multiple filters (simulating AI agent queries)"""
        print("\n" + "=" * 80)
        print("Test 15: Complex Multi-Filter Query")
        print("=" * 80)
        
        # Complex query: Form4, recent date, with full-text search
        from datetime import datetime, timedelta
        end_date = datetime.now()
        start_date = end_date - timedelta(days=60)
        
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"formType": "form4"}},
                        {
                            "range": {
                                "eventDate": {
                                    "gte": start_date.strftime('%Y-%m-%d'),
                                    "lte": end_date.strftime('%Y-%m-%d')
                                }
                            }
                        },
                        {
                            "match": {
                                "htmlContent": {
                                    "query": "Director",
                                    "operator": "or"
                                }
                            }
                        }
                    ],
                    "must_not": [
                        {"prefix": {"tradeId": "test_"}}
                    ]
                }
            },
            "size": 10,
            "sort": [{"eventDate": {"order": "desc"}}],
            "_source": ["tradeId", "formType", "reportingPersonName", "issuerName", "tickerSymbol", "relationship", "eventDate", "formS3Key"]
        }
        
        try:
            response = self._make_request('POST', f'/{self.index_name}/_search', data=query)
            
            if 'hits' in response:
                total = response.get('hits', {}).get('total', {})
                if isinstance(total, dict):
                    total_count = total.get('value', 0)
                else:
                    total_count = total
                
                hits = response.get('hits', {}).get('hits', [])
                
                print(f"[OK] Complex query successful")
                print(f"   Query: Form4 + Date Range ({start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}) + Full-text search ('Director')")
                print(f"   Total matches: {total_count}")
                print(f"   Returned: {len(hits)}")
                
                if hits:
                    print(f"\n   Sample results:")
                    for i, hit in enumerate(hits[:5], 1):
                        source = hit.get('_source', {})
                        score = hit.get('_score', 0)
                        print(f"\n      {i}. Score: {score:.2f}")
                        print(f"         Trade ID: {source.get('tradeId', 'N/A')}")
                        print(f"         Person: {source.get('reportingPersonName', 'N/A')}")
                        print(f"         Issuer: {source.get('issuerName', 'N/A')} ({source.get('tickerSymbol', 'N/A')})")
                        print(f"         Relationship: {source.get('relationship', 'N/A')}")
                        print(f"         Event Date: {source.get('eventDate', 'N/A')}")
                        print(f"         S3 Key: {source.get('formS3Key', 'N/A')}")
                else:
                    print(f"\n   [INFO] No documents matched complex query")
                
                return True
            else:
                print(f"[ERROR] Unexpected response: {json.dumps(response, indent=2)}")
                return False
                
        except Exception as e:
            print(f"[ERROR] Error executing complex query: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def run_all_tests(self, cleanup: bool = True, test_real_data: bool = True, skip_test_doc: bool = False) -> Dict[str, bool]:
        """
        Run all tests
        
        Args:
            cleanup: Whether to cleanup test document after tests
            test_real_data: Whether to run tests on real data from the index
            skip_test_doc: Whether to skip test document creation/cleanup tests
        """
        print("\n" + "=" * 80)
        print("OpenSearch Test Suite")
        print("=" * 80)
        print(f"Endpoint: {self.endpoint}")
        print(f"Region: {self.region}")
        print(f"Index: {self.index_name}")
        print("=" * 80)
        
        results = {}
        
        # Test 1: Connection
        results['connection'] = self.test_connection()
        if not results['connection']:
            print("\n[ERROR] Connection failed. Cannot proceed with other tests.")
            return results
        
        # Test 2: Check index
        index_exists = self.test_index_exists()
        
        # Test 3: Create index if needed
        if not index_exists:
            results['create_index'] = self.create_index()
        else:
            results['create_index'] = True
        
        # Test document tests (optional)
        doc_id = None
        if not skip_test_doc:
            # Test 4: Index test document
            doc_id = self.index_test_document()
            results['index_document'] = doc_id is not None
            
            if doc_id:
                # Test 5: Search test document
                results['search_document'] = self.search_test_document(doc_id)
                
                # Test 6: Search query
                results['search_query'] = self.test_search_query()
                
                # Test 7: Filtered search
                results['filtered_search'] = self.test_filtered_search()
                
                # Test 8: Cleanup
                if cleanup:
                    results['cleanup'] = self.cleanup_test_document(doc_id)
        else:
            # Skip test document tests but still do basic search
            results['index_document'] = True  # Skip
            results['search_document'] = True  # Skip
            results['search_query'] = self.test_search_query()
            results['filtered_search'] = self.test_filtered_search()
        
        # Real data query tests
        if test_real_data:
            # Test 9: Query real documents
            results['query_real_documents'] = self.test_query_real_documents()
            
            # Test 10: Full-text search on HTML content
            results['fulltext_search'] = self.test_fulltext_search_html_content()
            
            # Test 11: Filter by ticker
            results['filter_by_ticker'] = self.test_filter_by_ticker()
            
            # Test 12: Filter by issuer
            results['filter_by_issuer'] = self.test_filter_by_issuer()
            
            # Test 13: Filter by politician
            results['filter_by_politician'] = self.test_filter_by_politician()
            
            # Test 14: Date range query
            results['date_range_query'] = self.test_date_range_query()
            
            # Test 15: Complex multi-filter query
            results['complex_query'] = self.test_complex_query()
        
        # Summary
        print("\n" + "=" * 80)
        print("Test Summary")
        print("=" * 80)
        for test_name, passed in results.items():
            status = "[PASS]" if passed else "[FAIL]"
            print(f"   {status}: {test_name}")
        
        total = len(results)
        passed = sum(1 for v in results.values() if v)
        print(f"\n   Total: {passed}/{total} tests passed")
        print("=" * 80)
        
        return results


def main():
    parser = argparse.ArgumentParser(description='Test OpenSearch domain connectivity and functionality')
    parser.add_argument('--endpoint', required=True, help='OpenSearch domain endpoint (e.g., cosine-sec-staging-xxxxx.us-east-1.es.amazonaws.com)')
    parser.add_argument('--region', required=True, help='AWS region (e.g., us-east-1)')
    parser.add_argument('--index', default='sec-filings', help='Index name (default: sec-filings)')
    parser.add_argument('--no-cleanup', action='store_true', help='Do not delete test document after tests')
    parser.add_argument('--skip-test-doc', action='store_true', help='Skip test document creation/cleanup tests')
    parser.add_argument('--real-data-only', action='store_true', help='Only run real data query tests (implies --skip-test-doc)')
    
    args = parser.parse_args()
    
    tester = OpenSearchTester(args.endpoint, args.region, args.index)
    
    # Determine test options
    skip_test_doc = args.skip_test_doc or args.real_data_only
    test_real_data = True  # Always test real data unless explicitly disabled
    
    results = tester.run_all_tests(
        cleanup=not args.no_cleanup,
        test_real_data=test_real_data,
        skip_test_doc=skip_test_doc
    )
    
    # Exit with error code if any test failed
    if not all(results.values()):
        sys.exit(1)


if __name__ == '__main__':
    main()
