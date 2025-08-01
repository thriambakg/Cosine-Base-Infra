# API Gateway Module - Security Compliant by Default
# modules/api-gateway/main.tf

# REST API Gateway
resource "aws_api_gateway_rest_api" "this" {
  name        = var.api_name
  description = var.api_description

  endpoint_configuration {
    types            = [var.endpoint_type]
    vpc_endpoint_ids = var.endpoint_type == "PRIVATE" ? var.vpc_endpoint_ids : null
  }

  # Disable execute-api endpoint if using custom domain
  disable_execute_api_endpoint = var.disable_execute_api_endpoint

  # Binary media types
  binary_media_types = var.binary_media_types

  # Minimum compression size
  minimum_compression_size = var.minimum_compression_size

  # API key source
  api_key_source = var.api_key_source

  # Policy for PRIVATE endpoints
  policy = var.endpoint_type == "PRIVATE" ? jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = "*"
        Action    = "execute-api:Invoke"
        Resource  = "*"
        Condition = {
          StringEquals = {
            "aws:SourceVpce" = var.vpc_endpoint_ids
          }
        }
      }
    ]
  }) : null

  tags = merge(var.tags, {
    Name = var.api_name
    Type = "APIGateway"
  })

  # CKV_AWS_237: Create before destroy lifecycle rule
  lifecycle {
    create_before_destroy = true
  }
}

# CloudWatch log group for API Gateway
resource "aws_cloudwatch_log_group" "api_gateway" {
  name              = "/aws/apigateway/${var.api_name}"
  retention_in_days = var.log_retention_days
  kms_key_id        = var.cloudwatch_kms_key_arn

  tags = merge(var.tags, {
    Name = "/aws/apigateway/${var.api_name}"
    Type = "LogGroup"
  })
}

# API Gateway Account (for CloudWatch logging)
resource "aws_api_gateway_account" "this" {
  count               = var.create_api_gateway_account ? 1 : 0
  cloudwatch_role_arn = aws_iam_role.api_gateway_cloudwatch[0].arn

  depends_on = [aws_iam_role_policy_attachment.api_gateway_cloudwatch]
}

# IAM role for API Gateway CloudWatch logging
resource "aws_iam_role" "api_gateway_cloudwatch" {
  count = var.create_api_gateway_account ? 1 : 0
  name  = "${var.api_name}-api-gateway-cloudwatch-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "apigateway.amazonaws.com"
        }
      }
    ]
  })

  tags = var.tags
}

# IAM policy attachment for API Gateway CloudWatch logging
resource "aws_iam_role_policy_attachment" "api_gateway_cloudwatch" {
  count      = var.create_api_gateway_account ? 1 : 0
  role       = aws_iam_role.api_gateway_cloudwatch[0].name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonAPIGatewayPushToCloudWatchLogs"
}

# API Gateway deployment
resource "aws_api_gateway_deployment" "this" {
  count       = var.create_deployment ? 1 : 0
  rest_api_id = aws_api_gateway_rest_api.this.id

  triggers = {
    redeployment = sha1(jsonencode([
      aws_api_gateway_rest_api.this.body,
      var.deployment_triggers
    ]))
  }

  lifecycle {
    create_before_destroy = true
  }

  depends_on = [
    aws_api_gateway_rest_api.this
  ]
}

# API Gateway stage
resource "aws_api_gateway_stage" "this" {
  count         = var.create_deployment ? 1 : 0
  deployment_id = aws_api_gateway_deployment.this[0].id
  rest_api_id   = aws_api_gateway_rest_api.this.id
  stage_name    = var.stage_name

  # X-Ray tracing
  xray_tracing_enabled = var.xray_tracing_enabled

  # Access logging - CKV2_AWS_4: Ensure appropriate logging level
  dynamic "access_log_settings" {
    for_each = var.enable_access_logging ? [1] : []
    content {
      destination_arn = aws_cloudwatch_log_group.api_gateway.arn
      format = jsonencode({
        requestId      = "$context.requestId"
        ip             = "$context.identity.sourceIp"
        caller         = "$context.identity.caller"
        user           = "$context.identity.user"
        requestTime    = "$context.requestTime"
        httpMethod     = "$context.httpMethod"
        resourcePath   = "$context.resourcePath"
        status         = "$context.status"
        protocol       = "$context.protocol"
        responseLength = "$context.responseLength"
        errorMessage   = "$context.error.message"
        errorType      = "$context.error.messageString"
      })
    }
  }

  # Cache settings - CKV_AWS_120: Ensure caching is enabled
  cache_cluster_enabled = var.cache_cluster_enabled
  cache_cluster_size    = var.cache_cluster_enabled ? var.cache_cluster_size : null

  # CKV2_AWS_51: Client certificate authentication
  client_certificate_id = var.client_certificate_id

  tags = merge(var.tags, {
    Name  = "${var.api_name}-${var.stage_name}"
    Stage = var.stage_name
  })

  # CKV2_AWS_29: Lifecycle rule to enforce WAF protection for public APIs
  lifecycle {
    precondition {
      condition = (
        var.endpoint_type == "PRIVATE" ||
        (var.endpoint_type != "PRIVATE" && var.waf_web_acl_arn != null && var.waf_web_acl_arn != "")
      )
      error_message = "CKV2_AWS_29: Public API Gateway stages MUST be protected by WAF for security compliance. You must provide a valid waf_web_acl_arn for non-PRIVATE endpoints. Current endpoint_type: ${var.endpoint_type}"
    }

    # CKV2_AWS_4: Ensure logging is properly configured
    precondition {
      condition = (
        var.enable_access_logging == true &&
        contains(["ERROR", "INFO"], var.logging_level)
      )
      error_message = "CKV2_AWS_4: API Gateway stage must have access logging enabled and appropriate logging level (ERROR or INFO) for compliance."
    }
  }
}

# Method settings for the stage - CKV2_AWS_4 compliance
resource "aws_api_gateway_method_settings" "this" {
  count       = var.create_deployment ? 1 : 0
  rest_api_id = aws_api_gateway_rest_api.this.id
  stage_name  = aws_api_gateway_stage.this[0].stage_name
  method_path = "*/*"

  settings {
    # CKV2_AWS_4: Enable detailed CloudWatch metrics (required for compliance)
    metrics_enabled = var.detailed_metrics_enabled

    # CKV2_AWS_4: Data trace logging configuration
    data_trace_enabled = var.data_trace_enabled

    # CKV2_AWS_4: Logging level must be ERROR or INFO (never OFF)
    logging_level = var.logging_level

    # Throttling settings
    throttling_rate_limit  = var.throttling_rate_limit
    throttling_burst_limit = var.throttling_burst_limit

    # Caching settings
    caching_enabled      = var.caching_enabled
    cache_ttl_in_seconds = var.cache_ttl_in_seconds
    cache_data_encrypted = var.cache_data_encrypted

    # Request validation
    require_authorization_for_cache_control    = var.require_authorization_for_cache_control
    unauthorized_cache_control_header_strategy = var.unauthorized_cache_control_header_strategy
  }
}

# CKV2_AWS_29: WAF Web ACL Association (mandatory for public APIs)
# This resource ensures public API stages are always protected by WAF
resource "aws_wafv2_web_acl_association" "this" {
  count        = var.create_deployment && var.endpoint_type != "PRIVATE" ? 1 : 0
  resource_arn = aws_api_gateway_stage.this[0].arn
  web_acl_arn  = var.waf_web_acl_arn

  # Ensure WAF ARN is provided for public endpoints
  depends_on = [aws_api_gateway_stage.this]
}

# Additional compliance validation resource for CKV2_AWS_29
resource "terraform_data" "waf_compliance_validation" {
  count = var.create_deployment && var.endpoint_type != "PRIVATE" ? 1 : 0

  lifecycle {
    precondition {
      condition     = var.waf_web_acl_arn != null && var.waf_web_acl_arn != ""
      error_message = "CKV2_AWS_29: Public API Gateway stages require WAF protection. WAF Web ACL ARN must be provided for ${var.endpoint_type} endpoints."
    }
  }
}

# Additional compliance validation resource for CKV2_AWS_4
resource "terraform_data" "logging_compliance_validation" {
  count = var.create_deployment ? 1 : 0

  lifecycle {
    precondition {
      condition = (
        var.enable_access_logging == true &&
        contains(["ERROR", "INFO"], var.logging_level) &&
        var.detailed_metrics_enabled == true
      )
      error_message = "CKV2_AWS_4: API Gateway stage must have appropriate logging configuration. Requirements: enable_access_logging=true, logging_level must be ERROR or INFO (not OFF), detailed_metrics_enabled=true."
    }
  }
}

# Custom domain name (if provided)
resource "aws_api_gateway_domain_name" "this" {
  count           = var.domain_name != null ? 1 : 0
  domain_name     = var.domain_name
  certificate_arn = var.certificate_arn

  endpoint_configuration {
    types = [var.domain_endpoint_type]
  }

  security_policy = var.security_policy

  tags = merge(var.tags, {
    Name = var.domain_name
  })
}

# Base path mapping for custom domain
resource "aws_api_gateway_base_path_mapping" "this" {
  count       = var.domain_name != null && var.create_deployment ? 1 : 0
  api_id      = aws_api_gateway_rest_api.this.id
  stage_name  = aws_api_gateway_stage.this[0].stage_name
  domain_name = aws_api_gateway_domain_name.this[0].domain_name
  base_path   = var.base_path
}

# API Gateway Usage Plan (if API keys are used)
resource "aws_api_gateway_usage_plan" "this" {
  count       = var.create_usage_plan ? 1 : 0
  name        = "${var.api_name}-usage-plan"
  description = "Usage plan for ${var.api_name}"

  api_stages {
    api_id = aws_api_gateway_rest_api.this.id
    stage  = var.create_deployment ? aws_api_gateway_stage.this[0].stage_name : var.stage_name
  }

  # Quota settings
  dynamic "quota_settings" {
    for_each = var.quota_settings != null ? [var.quota_settings] : []
    content {
      limit  = quota_settings.value.limit
      period = quota_settings.value.period
      offset = quota_settings.value.offset
    }
  }

  # Throttle settings
  dynamic "throttle_settings" {
    for_each = var.throttle_settings != null ? [var.throttle_settings] : []
    content {
      rate_limit  = throttle_settings.value.rate_limit
      burst_limit = throttle_settings.value.burst_limit
    }
  }

  tags = var.tags
}

# API Keys
resource "aws_api_gateway_api_key" "this" {
  for_each = var.api_keys

  name        = each.key
  description = each.value.description
  enabled     = each.value.enabled
  value       = each.value.value

  tags = merge(var.tags, {
    Name = each.key
  })
}

# Usage plan key associations
resource "aws_api_gateway_usage_plan_key" "this" {
  for_each = var.create_usage_plan ? var.api_keys : {}

  key_id        = aws_api_gateway_api_key.this[each.key].id
  key_type      = "API_KEY"
  usage_plan_id = aws_api_gateway_usage_plan.this[0].id
}
