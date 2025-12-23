# LDA Autocomplete Terraform Resources

## Resources to Add to `main.tf`

### 1. SQS Queue for Autocomplete Strings

```hcl
# SQS Queue for LDA Autocomplete Strings
module "lda_autocomplete_queue" {
  source = "./modules/sqs"

  project_name = var.project_name
  environment  = var.environment
  queue_name   = "lda-autocomplete-strings"
  purpose      = "LDA Autocomplete String Processing"

  message_retention_seconds  = 1209600 # 14 days
  visibility_timeout_seconds = 60      # 1 minute (Lambda timeout is 60s)
  max_receive_count          = 3
  enable_dlq                 = true

  kms_key_id = module.kms.main_key_id

  tags = var.common_tags
}
```

### 2. Lambda Function for Autocomplete Processing

```hcl
# Lambda Function for LDA Autocomplete String Processor
module "lda_autocomplete_processor_lambda" {
  source = "./modules/lambda"

  function_name = "${var.project_name}-lda-autocomplete-processor-${var.environment}"
  description   = "Processes autocomplete strings from SQS, deduplicates, and builds sorted CSV in S3"
  runtime       = "python3.11"
  handler       = "lambda_function.lambda_handler"
  timeout       = 60  # 1 minute
  memory_size   = 512

  source_dir = "${path.module}/../backend_app/src/lda_autocomplete_processor/app"

  environment_variables = {
    S3_BUCKET_NAME = module.lda_disclosures_s3.bucket_id
    S3_KEY_PREFIX  = "lda-autocomplete"
  }

  layers = [
    module.core_layer.layer_arn
  ]

  additional_policy_arns = [
    module.lda_autocomplete_queue.sqs_access_policy_arn,
    module.kms.kms_access_policy_arn,
    aws_iam_policy.lambda_lda_autocomplete_s3_policy.arn
  ]

  tags = var.common_tags

  depends_on = [
    module.lda_autocomplete_queue,
    module.lda_disclosures_s3
  ]
}
```

### 3. S3 IAM Policy for Lambda

```hcl
# IAM Policy for Lambda to access LDA autocomplete S3 bucket
resource "aws_iam_policy" "lambda_lda_autocomplete_s3_policy" {
  name        = "${var.project_name}-lambda-lda-autocomplete-s3-${var.environment}"
  description = "Allows Lambda to read/write LDA autocomplete files in S3"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = "${module.lda_disclosures_s3.bucket_arn}/lda-autocomplete/*"
      },
      {
        Effect = "Allow"
        Action = [
          "s3:ListBucket"
        ]
        Resource = module.lda_disclosures_s3.bucket_arn
        Condition = {
          StringLike = {
            "s3:prefix" = "lda-autocomplete/*"
          }
        }
      }
    ]
  })

  tags = var.common_tags
}
```

### 4. SQS Event Source Mapping

```hcl
# SQS Event Source Mapping for Lambda
resource "aws_lambda_event_source_mapping" "lda_autocomplete_sqs_trigger" {
  event_source_arn                   = module.lda_autocomplete_queue.queue_arn
  function_name                      = module.lda_autocomplete_processor_lambda.function_arn
  batch_size                         = 10 # Process up to 10 messages per invocation
  maximum_batching_window_in_seconds = 3  # Wait up to 3 seconds to collect more messages
  enabled                            = true

  depends_on = [
    module.lda_autocomplete_queue,
    module.lda_autocomplete_processor_lambda
  ]
}
```

### 5. IAM Policy for Glue Job to Send to SQS

```hcl
# IAM Policy for Glue Job to send autocomplete strings to SQS
resource "aws_iam_policy" "glue_lda_autocomplete_sqs_policy" {
  name        = "${var.project_name}-glue-lda-autocomplete-sqs-${var.environment}"
  description = "Allows Glue job to send autocomplete strings to SQS"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "sqs:SendMessage",
          "sqs:GetQueueAttributes"
        ]
        Resource = [
          module.lda_autocomplete_queue.queue_arn
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "kms:Decrypt",
          "kms:GenerateDataKey",
          "kms:DescribeKey"
        ]
        Resource = [
          module.kms.main_key_arn
        ]
      }
    ]
  })

  tags = var.common_tags
}

# Attach SQS policy to Glue job role
resource "aws_iam_role_policy_attachment" "glue_lda_autocomplete_sqs" {
  role       = module.lda_disclosures_glue_job.role_name
  policy_arn = aws_iam_policy.glue_lda_autocomplete_sqs_policy.arn

  depends_on = [
    module.lda_disclosures_glue_job,
    aws_iam_policy.glue_lda_autocomplete_sqs_policy
  ]
}
```

### 6. Update Glue Job Arguments in Step Function

Add to the Glue job arguments in the Step Function definition:

```hcl
"--AUTOCOMPLETE_SQS_URL.$" = "$.AUTOCOMPLETE_SQS_URL"
```

Or set it directly:

```hcl
"--AUTOCOMPLETE_SQS_URL" = module.lda_autocomplete_queue.queue_url
```

## File Locations

- Lambda function: `Cosine-Base-Infra/backend_app/src/lda_autocomplete_processor/app/lambda_function.py`
- CSV generator script: `Cosine-Base-Infra/scripts/LDA/generate_constants_csvs.py`
- Glue script modifications: `Cosine-Base-Infra/backend_app/src/glue/lda_disclosures/glue_script.py`

## S3 Structure

```
s3://lda-disclosures-bucket/
└── lda-autocomplete/
    ├── constants/
    │   ├── general_issues.csv
    │   ├── government_entities.csv
    │   ├── contribution_item_types.csv
    │   └── filing_types.csv
    └── autocomplete_strings.csv
```





