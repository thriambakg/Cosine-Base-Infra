# News API Deployment Guide

This guide explains how to deploy the news search API infrastructure and connect it to the frontend.

## 🏗️ Infrastructure Components

### 1. News Search Lambda Function
- **Location**: `backend_app/src/news_search/app/`
- **Runtime**: Python 3.11
- **Purpose**: Processes complex query expressions and searches DynamoDB
- **Timeout**: 30 seconds
- **Memory**: 512 MB

### 2. API Gateway
- **Endpoint**: `POST /news/search`
- **CORS**: Enabled for frontend access
- **Integration**: Lambda proxy integration
- **Logging**: CloudWatch logs enabled

### 3. DynamoDB Integration
- **Table**: News table with GSI indexes
- **GSI1**: Category-based search
- **GSI2**: Sentiment-based search  
- **GSI3**: AI tag-based search
- **GSI4**: Keywords-based search
- **GSI5**: Source-based search

## 🚀 Deployment Steps

### 1. Deploy Infrastructure
```bash
cd Cosine-Base-Infra/terraform
terraform plan -var-file="environments/production.auto.tfvars"
terraform apply -var-file="environments/production.auto.tfvars"
```

### 2. Get API Gateway URL
After deployment, get the API Gateway URL:
```bash
terraform output news_api_gateway_url
```

### 3. Update Frontend Configuration
Set the environment variable in your frontend:
```bash
# In your frontend .env file
REACT_APP_NEWS_API_URL=https://your-api-gateway-url.amazonaws.com/prod/news/search
```

## 🔍 API Usage

### Request Format
```json
{
  "query": {
    "keywords": {
      "type": "expression",
      "children": [
        {
          "type": "term",
          "field": "keywords",
          "value": "Tesla",
          "operator": "AND"
        },
        {
          "type": "term",
          "field": "keywords",
          "value": "earnings"
        }
      ]
    },
    "sources": {
      "type": "term",
      "field": "source_name",
      "value": "Financial Times"
    },
    "categories": null,
    "countries": null
  },
  "dateRange": "24h",
  "limit": 50,
  "offset": 0
}
```

### Response Format
```json
{
  "articles": [
    {
      "id": "article_123",
      "title": "Tesla Reports Strong Q4 Earnings",
      "description": "Tesla exceeded expectations...",
      "source_url": "https://example.com/article",
      "source_name": "Financial Times",
      "published_date": "2024-01-15T10:30:00Z",
      "keywords": "Tesla,earnings,automotive",
      "category": "Technology",
      "sentiment": "positive",
      "country": "US"
    }
  ],
  "total": 25,
  "limit": 50,
  "offset": 0,
  "query": { /* original query */ },
  "timestamp": "2024-01-15T10:30:00Z"
}
```

## 🧪 Testing

### 1. Test API Endpoint
```bash
curl -X POST https://your-api-gateway-url.amazonaws.com/prod/news/search \
  -H "Content-Type: application/json" \
  -d '{
    "query": {
      "keywords": {
        "type": "term",
        "field": "keywords",
        "value": "Tesla"
      }
    },
    "dateRange": "24h",
    "limit": 10
  }'
```

### 2. Test Frontend Integration
1. Open browser developer tools
2. Create a news tile with filters
3. Check console logs for API payload and response
4. Verify articles are displayed correctly

## 🔧 Query Logic

### Expression Types
- **term**: Single keyword/source/category/country
- **group**: Parentheses for complex logic
- **expression**: Multiple terms with operators

### Operators
- **AND**: All conditions must match
- **OR**: Any condition can match

### Examples
```json
// Simple keyword search
"keywords": {
  "type": "term",
  "field": "keywords", 
  "value": "Tesla"
}

// Complex expression: (Tesla AND earnings) OR Apple
"keywords": {
  "type": "expression",
  "children": [
    {
      "type": "group",
      "children": {
        "type": "expression",
        "children": [
          {"type": "term", "field": "keywords", "value": "Tesla", "operator": "AND"},
          {"type": "term", "field": "keywords", "value": "earnings"}
        ]
      },
      "operator": "OR"
    },
    {"type": "term", "field": "keywords", "value": "Apple"}
  ]
}
```

## 📊 Monitoring

### CloudWatch Logs
- **API Gateway**: `/aws/apigateway/cosine-news-api-{env}`
- **Lambda**: `/aws/lambda/cosine-news-search-{env}`

### Key Metrics
- API Gateway: Request count, latency, error rate
- Lambda: Duration, errors, throttles
- DynamoDB: Read/write capacity, throttles

## 🚨 Troubleshooting

### Common Issues

1. **CORS Errors**
   - Check API Gateway CORS configuration
   - Verify frontend URL is allowed

2. **Lambda Timeout**
   - Increase timeout in Terraform
   - Optimize DynamoDB queries

3. **No Results**
   - Check DynamoDB table has data
   - Verify query structure matches data format
   - Check date range filters

4. **Permission Errors**
   - Verify Lambda has DynamoDB access
   - Check IAM policies

### Debug Steps
1. Check CloudWatch logs for errors
2. Test API endpoint directly with curl
3. Verify DynamoDB table structure
4. Check frontend console logs

## 🔄 Updates

### Deploying Changes
1. Update Lambda function code
2. Run `terraform apply` to update infrastructure
3. Test API endpoint
4. Update frontend if needed

### Rollback
1. Revert Terraform changes
2. Run `terraform apply` to rollback
3. Update frontend configuration if needed
