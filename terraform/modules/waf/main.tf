# WAF Module - Basic Security Protection
# modules/waf/main.tf

# Basic WAF Web ACL for API Gateway protection
resource "aws_wafv2_web_acl" "this" {
  name  = var.web_acl_name
  scope = var.scope # CLOUDFRONT or REGIONAL

  default_action {
    allow {}
  }

  # Rate limiting rule
  rule {
    name     = "RateLimitRule"
    priority = 1

    action {
      block {}
    }

    statement {
      rate_based_statement {
        limit              = var.rate_limit
        aggregate_key_type = "IP"
      }
    }

    visibility_config {
      cloudwatch_metrics_enabled = true
      metric_name                = "${var.web_acl_name}-RateLimit"
      sampled_requests_enabled   = true
    }
  }

  # IP Reputation rule (AWS Managed)
  rule {
    name     = "AWS-AWSManagedRulesAmazonIpReputationList"
    priority = 2

    override_action {
      none {}
    }

    statement {
      managed_rule_group_statement {
        name        = "AWSManagedRulesAmazonIpReputationList"
        vendor_name = "AWS"
      }
    }

    visibility_config {
      cloudwatch_metrics_enabled = true
      metric_name                = "${var.web_acl_name}-IpReputation"
      sampled_requests_enabled   = true
    }
  }

  # Core Rule Set (AWS Managed)
  rule {
    name     = "AWS-AWSManagedRulesCommonRuleSet"
    priority = 3

    override_action {
      none {}
    }

    statement {
      managed_rule_group_statement {
        name        = "AWSManagedRulesCommonRuleSet"
        vendor_name = "AWS"
      }
    }

    visibility_config {
      cloudwatch_metrics_enabled = true
      metric_name                = "${var.web_acl_name}-CommonRuleSet"
      sampled_requests_enabled   = true
    }
  }

  # CKV_AWS_192: Log4j vulnerability protection (CVE-2021-44228)
  rule {
    name     = "AWS-AWSManagedRulesKnownBadInputsRuleSet"
    priority = 4

    override_action {
      none {}
    }

    statement {
      managed_rule_group_statement {
        name        = "AWSManagedRulesKnownBadInputsRuleSet"
        vendor_name = "AWS"
      }
    }

    visibility_config {
      cloudwatch_metrics_enabled = true
      metric_name                = "${var.web_acl_name}-Log4jProtection"
      sampled_requests_enabled   = true
    }
  }

  tags = merge(var.tags, {
    Name = var.web_acl_name
    Type = "WAFv2WebACL"
  })

  visibility_config {
    cloudwatch_metrics_enabled = true
    metric_name                = var.web_acl_name
    sampled_requests_enabled   = true
  }
}

# CloudWatch log group for WAF logs
resource "aws_cloudwatch_log_group" "waf_logs" {
  count             = var.enable_logging ? 1 : 0
  name              = "/aws/wafv2/${var.web_acl_name}"
  retention_in_days = var.log_retention_days
  kms_key_id        = var.kms_key_arn

  tags = merge(var.tags, {
    Name = "${var.web_acl_name}-logs"
    Type = "LogGroup"
  })
}

# WAF logging configuration
resource "aws_wafv2_web_acl_logging_configuration" "this" {
  count                   = var.enable_logging ? 1 : 0
  resource_arn            = aws_wafv2_web_acl.this.arn
  log_destination_configs = [aws_cloudwatch_log_group.waf_logs[0].arn]

  redacted_fields {
    single_header {
      name = "authorization"
    }
  }

  redacted_fields {
    single_header {
      name = "cookie"
    }
  }
}
