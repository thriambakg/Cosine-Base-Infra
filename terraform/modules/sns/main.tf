# SNS Module - Security Compliant by Default
# modules/sns/main.tf

# Data sources
data "aws_caller_identity" "current" {}

# SNS Topic
resource "aws_sns_topic" "this" {
  name              = var.topic_name
  display_name      = var.display_name
  kms_master_key_id = var.kms_key_arn

  # Enable server-side encryption
  tags = merge(var.tags, {
    Name    = var.topic_name
    Type    = "SNSTopic"
    Purpose = var.purpose
  })
}

# SNS Topic Policy
resource "aws_sns_topic_policy" "this" {
  arn = aws_sns_topic.this.arn

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = concat([
      {
        Sid    = "DefaultPolicy"
        Effect = "Allow"
        Principal = {
          AWS = "*"
        }
        Action = [
          "SNS:GetTopicAttributes",
          "SNS:SetTopicAttributes",
          "SNS:AddPermission",
          "SNS:RemovePermission",
          "SNS:DeleteTopic",
          "SNS:Subscribe",
          "SNS:ListSubscriptionsByTopic",
          "SNS:Publish"
        ]
        Resource = aws_sns_topic.this.arn
        Condition = {
          StringEquals = {
            "AWS:SourceOwner" = data.aws_caller_identity.current.account_id
          }
        }
      }],
      var.allow_s3_publish ? [{
        Sid    = "AllowS3Publish"
        Effect = "Allow"
        Principal = {
          Service = "s3.amazonaws.com"
        }
        Action   = "SNS:Publish"
        Resource = aws_sns_topic.this.arn
        Condition = var.s3_bucket_arns != null ? {
          ArnLike = {
            "aws:SourceArn" = [
              var.s3_bucket_arns[0],
              "${var.s3_bucket_arns[0]}/*"
            ]
          }
        } : {}
      }] : [],
      var.allow_cloudwatch_publish ? [{
        Sid    = "AllowCloudWatchPublish"
        Effect = "Allow"
        Principal = {
          Service = "cloudwatch.amazonaws.com"
        }
        Action   = "SNS:Publish"
        Resource = aws_sns_topic.this.arn
      }] : [],
      var.allow_lambda_publish ? [{
        Sid    = "AllowLambdaPublish"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
        Action   = "SNS:Publish"
        Resource = aws_sns_topic.this.arn
      }] : []
    )
  })
}

# Email subscriptions
resource "aws_sns_topic_subscription" "email" {
  for_each = toset(var.email_addresses)

  topic_arn = aws_sns_topic.this.arn
  protocol  = "email"
  endpoint  = each.value
}

# SMS subscriptions
resource "aws_sns_topic_subscription" "sms" {
  for_each = toset(var.phone_numbers)

  topic_arn = aws_sns_topic.this.arn
  protocol  = "sms"
  endpoint  = each.value
}

# SQS subscriptions
resource "aws_sns_topic_subscription" "sqs" {
  for_each = var.sqs_subscriptions

  topic_arn = aws_sns_topic.this.arn
  protocol  = "sqs"
  endpoint  = each.value.queue_arn

  # Filter policy if provided
  filter_policy = each.value.filter_policy != null ? jsonencode(each.value.filter_policy) : null
}

# Lambda subscriptions
resource "aws_sns_topic_subscription" "lambda" {
  for_each = toset(var.lambda_function_arns)

  topic_arn = aws_sns_topic.this.arn
  protocol  = "lambda"
  endpoint  = each.value
}

# HTTP/HTTPS subscriptions
resource "aws_sns_topic_subscription" "http" {
  for_each = toset(var.http_endpoints)

  topic_arn = aws_sns_topic.this.arn
  protocol  = "http"
  endpoint  = each.value
}

resource "aws_sns_topic_subscription" "https" {
  for_each = toset(var.https_endpoints)

  topic_arn = aws_sns_topic.this.arn
  protocol  = "https"
  endpoint  = each.value
}

