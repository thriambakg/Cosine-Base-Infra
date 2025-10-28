# Lambda Layer Module

This module creates AWS Lambda layers for shared Python dependencies and code, organized into logical groups to stay under the 64MB per layer limit.

## Architecture

The module uses a **dynamic multi-layer approach** that automatically discovers layers based on requirements files and source code:

### Dependency-Based Layers
- **Core Layer** (`core-dependencies.txt`) - Essential packages (requests, boto3, etc.)
- **Financial Layer** (`financial-dependencies.txt`) - Financial data processing (yfinance, numpy, pandas)
- **AI Core Layer** (`ai-core-dependencies.txt`) - Essential AI packages (aiohttp, pyjwt, tenacity, etc.)
- **Strands Layer** (`strands-dependencies.txt`) - Heavy AI agent framework (strands-agents, strands-agents-tools)
- **Utility Layer** (`utility-dependencies.txt`) - Optional utilities (pillow, sympy, rich, etc.)

### Code-Based Layers
- **Shared Code Layer** (`shared-code-dependencies.txt`) - Shared Python code modules (lambda_invocation, etc.)

**Dynamic Discovery**: The system automatically discovers all layers by scanning for `*-dependencies.txt` files in the `layer-definitions/` directory.

## Files

### Build Scripts
- `build-layer.sh` - Enhanced build script that handles both dependencies and source code
- `build-all-layers.sh` - Wrapper script to build all layers or specific layers

### Requirements Files
- `layer-definitions/core-dependencies.txt` - Core dependencies
- `layer-definitions/financial-dependencies.txt` - Financial dependencies  
- `layer-definitions/ai-dependencies.txt` - AI dependencies
- `layer-definitions/utility-dependencies.txt` - Utility dependencies
- `layer-definitions/shared-code-dependencies.txt` - Shared code layer (no external deps)

### Terraform Files
- `main.tf` - Module configuration
- `variables.tf` - Input variables
- `outputs.tf` - Output values

## Usage

### Building Layers Locally

```bash
# Build all layers
./build-all-layers.sh

# Build specific layers
./build-all-layers.sh core financial shared-code

# Build individual dependency-based layer
./build-layer.sh core

# Build individual code-based layer with source directory
./build-layer.sh shared-code ../../Cosine2.0/backend_app/src/shared_layers
```

### Using in Terraform

#### Dependency-Based Layer
```hcl
module "lambda_layer_core" {
  source = "./modules/lambda-layer"
  
  project_name        = var.project_name
  environment         = var.environment
  requirements_file   = "core-dependencies.txt"
  layer_name_suffix   = "core-deps"
  layer_description   = "Core dependencies layer"
  compatible_runtimes = ["python3.11"]
  python_command      = "python3.11"
}
```

#### Code-Based Layer
```hcl
module "lambda_layer_shared_code" {
  source = "./modules/lambda-layer"
  
  project_name        = var.project_name
  environment         = var.environment
  requirements_file   = "shared-code-dependencies.txt"
  layer_name_suffix   = "shared-code"
  layer_description   = "Shared code modules layer"
  source_directory    = "../../Cosine2.0/backend_app/src/shared_layers"
  compatible_runtimes = ["python3.11"]
  python_command      = "python3.11"
}
```

### Using Layers in Lambda Functions

```hcl
resource "aws_lambda_function" "my_function" {
  # ... other configuration ...
  
  layers = [
    module.lambda_layer_core.layer_arn,
    module.lambda_layer_financial.layer_arn,
    module.lambda_layer_ai.layer_arn,
    module.lambda_layer_utility.layer_arn,
  ]
}
```

## Adding/Removing Layers

### To Add a New Layer:

1. Create new requirements file: `layer-definitions/new-layer-dependencies.txt`
2. Add module call in main `main.tf` with the layer name
3. Update outputs in `outputs.tf`
4. **That's it!** The system will automatically discover and build the new layer

### To Remove a Layer:

1. Remove requirements file from `layer-definitions/`
2. Remove module call from main `main.tf`
3. Remove from outputs in `outputs.tf`
4. **That's it!** The system will automatically stop building the removed layer

## Benefits

- ✅ **No size limitations** - each layer under 64MB
- ✅ **All dependencies preserved** - no functionality lost
- ✅ **Zero additional cost** - Lambda layers are FREE
- ✅ **Logical organization** - easy to understand and maintain
- ✅ **Flexible usage** - use only the layers you need
- ✅ **Independent updates** - update one layer without affecting others
- ✅ **Dynamic layer discovery** - automatically finds and builds all layers
- ✅ **Modular build system** - easy to add/remove layers
- ✅ **Robust error handling** - graceful fallback for package installation issues
- ✅ **Flexible version constraints** - uses compatible version ranges instead of exact pins
- ✅ **Zero maintenance** - no need to update hardcoded layer lists

## Layer Limits

- **Maximum layers per function**: 5
- **Maximum size per layer**: 64MB (unzipped)
- **Maximum total size**: 250MB (all layers combined)
- **Maximum function size**: 50MB (zipped)

This module ensures all layers stay well under these limits while providing all necessary dependencies.

## Troubleshooting

### Common Issues

#### "No matching distribution found" Error
If you see errors like `ERROR: No matching distribution found for numpy==1.26.4`, this means the exact version isn't available for the target platform.

**Solution:** The requirements files now use flexible version constraints (e.g., `numpy>=1.24.0,<2.0.0`) instead of exact pins. If you still encounter issues, try:
1. Using even more flexible constraints (e.g., `numpy>=1.20.0`)
2. Removing version constraints entirely for problematic packages
3. Checking if the package has Linux-compatible wheels

#### "Zip file structure invalid" Error
This usually happens when there are permission issues or existing zip files.

**Solution:** The build script now automatically removes existing zip files before creating new ones. If you still encounter issues:
1. Ensure you have write permissions in the directory
2. Close any applications that might have the zip file open
3. Run the build script from a clean directory

#### Layer Size Exceeds 64MB
If a layer is too large, the build will fail with a clear error message.

**Solution:**
1. Check the largest directories shown in the error output
2. Remove unnecessary dependencies from the requirements file
3. Consider splitting the layer into multiple smaller layers
4. Use the cleanup options in the build script

### Debug Mode

To see more detailed output during the build process, you can modify the build script to remove the `-q` flag from the zip command, or add `set -x` at the beginning of the script to see all commands being executed.
