# WAF Module

## Overview
This module creates a security-compliant AWS WAFv2 Web ACL with basic protection rules. The module is designed to provide essential security for API Gateway endpoints and other web applications, ensuring compliance with security best practices.

## Security Features
- **Rate Limiting**: Configurable rate limiting to prevent DDoS attacks
- **IP Reputation**: AWS managed IP reputation list blocking
- **Core Rule Set**: AWS managed common rule set for OWASP protection
- **CloudWatch Logging**: Comprehensive request logging for monitoring
- **KMS Encryption**: Optional encryption for log data

## Usage

### Basic WAF for API Gateway
```hcl
module "api_waf" {
  source = "./modules/waf"
  
  web_acl_name = "api-gateway-protection"
  scope        = "REGIONAL"
  rate_limit   = 2000
  
  tags = {
    Environment = "production"
    Purpose     = "api-protection"
  }
}

# Use with API Gateway
module "api_gateway" {
  source = "./modules/api-gateway"
  
  api_name        = "my-api"
  waf_web_acl_arn = module.api_waf.web_acl_arn
  
  # ... other configuration
}
```

### WAF for CloudFront Distribution
```hcl
module "cloudfront_waf" {
  source = "./modules/waf"
  
  web_acl_name = "cloudfront-protection"
  scope        = "CLOUDFRONT"
  rate_limit   = 5000
  
  enable_logging      = true
  log_retention_days  = 30
  kms_key_arn        = module.kms.key_arn
  
  tags = {
    Environment = "production"
    Purpose     = "cdn-protection"
  }
}
```

### Custom Rate Limiting
```hcl
module "strict_waf" {
  source = "./modules/waf"
  
  web_acl_name = "strict-api-protection"
  rate_limit   = 100  # Very strict rate limiting
  
  tags = {
    Environment = "production"
    Security    = "high"
  }
}
```

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|----------|
| web_acl_name | Name of the WAF Web ACL | `string` | n/a | yes |
| scope | Scope of the WAF (CLOUDFRONT or REGIONAL) | `string` | `"REGIONAL"` | no |
| rate_limit | Rate limit per 5 minutes from single IP | `number` | `2000` | no |
| tags | Tags to apply to WAF resources | `map(string)` | `{}` | no |
| enable_logging | Whether to enable WAF logging | `bool` | `true` | no |
| log_retention_days | Days to retain WAF logs | `number` | `365` | no |
| kms_key_arn | ARN of KMS key for log encryption | `string` | `null` | no |

## Outputs

| Name | Description |
|------|-------------|
| web_acl_arn | ARN of the WAF Web ACL |
| web_acl_id | ID of the WAF Web ACL |
| web_acl_name | Name of the WAF Web ACL |
| log_group_name | Name of the CloudWatch log group for WAF logs |
| log_group_arn | ARN of the CloudWatch log group for WAF logs |

## Security Rules Included

### 1. Rate Limiting Rule
- **Purpose**: Prevents DDoS and brute force attacks
- **Action**: Block requests exceeding the rate limit
- **Scope**: Per source IP address
- **Default**: 2000 requests per 5 minutes

### 2. IP Reputation List
- **Purpose**: Blocks requests from known malicious IP addresses
- **Source**: AWS Managed Rules - Amazon IP Reputation List
- **Action**: Block all requests from reputation-based bad IPs
- **Updates**: Automatically maintained by AWS

### 3. Core Rule Set
- **Purpose**: Protection against OWASP Top 10 vulnerabilities
- **Source**: AWS Managed Rules - Common Rule Set
- **Coverage**: SQL injection, XSS, path traversal, etc.
- **Action**: Block malicious requests

## Monitoring and Logging

### CloudWatch Metrics
- **Request Count**: Total requests processed
- **Blocked Requests**: Requests blocked by each rule
- **Rate Limit Hits**: Requests exceeding rate limits
- **Rule Matches**: Detailed breakdown by rule type

### Log Analysis
- **Request Details**: IP, user agent, headers, payload
- **Block Reasons**: Which rule triggered the block
- **Geographic Data**: Country and region information
- **Response Actions**: Allow, block, count decisions

## Cost Considerations

### WAF Pricing Components
- **Web ACL**: $1.00 per month per Web ACL
- **Rules**: $0.60 per month per rule (3 rules = $1.80)
- **Requests**: $0.60 per 1 million requests
- **Logging**: CloudWatch Logs standard pricing

### Cost Optimization Tips
- Adjust `rate_limit` based on legitimate traffic patterns
- Consider disabling logging in development environments
- Use shorter log retention for cost savings in non-production

## Best Practices

### Rate Limiting
- Monitor legitimate traffic patterns before setting strict limits
- Consider burst traffic for marketing campaigns or events
- Implement allow-lists for trusted sources if needed

### Rule Management
- Start with AWS managed rules before custom rules
- Monitor false positives and adjust as needed
- Regularly review WAF metrics and logs

### Integration
- Always use WAF with API Gateway for public endpoints
- Consider multiple WAFs for different application tiers
- Implement proper error handling for blocked requests

## Compliance Features

### Security Standards
- **CKV2_AWS_29**: API Gateway WAF protection compliance
- **Logging**: Comprehensive request logging for audit trails
- **Encryption**: Optional KMS encryption for sensitive log data

### Operational Excellence
- **Monitoring**: Built-in CloudWatch metrics and alarms
- **Automation**: Fully managed rule updates from AWS
- **Scalability**: Automatic scaling for traffic volume

## Dependencies
- AWS Provider >= 5.0
- CloudWatch module (for log group management)
- KMS module (if using encryption)

## Notes
- WAF rules are evaluated in priority order (1, 2, 3)
- REGIONAL scope is for API Gateway, ALB, AppSync
- CLOUDFRONT scope is for CloudFront distributions only
- Rate limiting uses a 5-minute window with IP-based aggregation
- Managed rules are automatically updated by AWS
