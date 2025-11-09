"""
Test script for OpenSearch domain connectivity and indexing
Tests:
1. Connection to OpenSearch domain
2. Index creation (if needed)
3. Document indexing
4. Document search/retrieval
5. Query capabilities
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
                    "nonDerivativeSecurities": {"type": "text"},
                    "derivativeSecurities": {"type": "text"},
                    "misc": {"type": "text"},
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
            "nonDerivativeSecurities": json.dumps([{
                "securityName": "Common Stock",
                "shares": "1000",
                "pricePerShare": "$10.00"
            }]),
            "derivativeSecurities": json.dumps([]),
            "misc": json.dumps({"remarks": "Test document"}),
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
    
    def run_all_tests(self, cleanup: bool = True) -> Dict[str, bool]:
        """Run all tests"""
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
    
    args = parser.parse_args()
    
    tester = OpenSearchTester(args.endpoint, args.region, args.index)
    results = tester.run_all_tests(cleanup=not args.no_cleanup)
    
    # Exit with error code if any test failed
    if not all(results.values()):
        sys.exit(1)


if __name__ == '__main__':
    main()
