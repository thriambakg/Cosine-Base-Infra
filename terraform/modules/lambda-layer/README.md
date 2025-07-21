# Lambda Layer Module

This module creates a Lambda layer with shared Python dependencies for the Cosine project.

## Features

- Python dependencies for stock analysis (yfinance, numpy, pandas)
- Compatible with Python 3.11 and 3.12 Lambda runtimes
- Automated dependency installation using virtual environment
- Platform-specific package installation for Linux x86_64 (Lambda environment)
- Comprehensive error handling and logging

## Dependencies Included

- **yfinance**: Yahoo Finance data retrieval
- **numpy**: Numerical computing
- **pandas**: Data manipulation and analysis
- **requests**: HTTP library
- **beautifulsoup4**: HTML/XML parsing
- **lxml**: XML processing
- **multitasking**: Concurrent processing
- **frozendict**: Immutable dictionaries
- **pytz**: Timezone handling

## Usage

```hcl
module "shared_layer" {
  source = "./modules/lambda-layer"
  
  project_name = "cosine"
  environment  = "staging"
  
  tags = {
    Project     = "cosine"
    Environment = "staging"
    ManagedBy   = "terraform"
  }
}
```

## Requirements

| Name | Version |
|------|---------|
| terraform | >= 1.0 |
| aws | ~> 5.0 |

## Resources

| Name | Type |
|------|------|
| aws_lambda_layer_version.shared_dependencies | resource |
| null_resource.pip_install | resource |
| data.archive_file.layer_zip | data source |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| project_name | Name of the project | `string` | n/a | yes |
| environment | Environment (development, staging, production) | `string` | n/a | yes |
| tags | Common tags to apply to all resources | `map(string)` | `{}` | no |
| compatible_runtimes | List of compatible Lambda runtimes | `list(string)` | `["python3.11", "python3.12"]` | no |
| layer_description | Description for the Lambda layer | `string` | `"Shared dependencies for Lambda functions"` | no |

## Outputs

| Name | Description |
|------|-------------|
| layer_arn | ARN of the Lambda layer |
| layer_version | Version of the Lambda layer |
| layer_name | Name of the Lambda layer |
| compatible_runtimes | Compatible runtimes for the Lambda layer |

## Installation Process

The module uses a PowerShell script (`install-layer-deps.ps1`) that:

1. Creates a Python virtual environment
2. Installs dependencies with platform-specific targeting for AWS Lambda
3. Falls back to general installation if platform-specific fails
4. Packages dependencies into the correct layer structure
5. Cleans up temporary files

## Notes

- Dependencies are installed for Linux x86_64 platform (AWS Lambda environment)
- The layer is compatible with both Python 3.11 and 3.12
- Virtual environment ensures clean dependency installation
- Comprehensive logging helps with troubleshooting
