#!/bin/bash

# Build script for Lambda Layer with all dependencies
# This script creates a layer package with all the heavy dependencies

set -e

# Default Python command (can be overridden)
# Try different Python commands in order of preference
if [ -z "$PYTHON_CMD" ]; then
    if command -v python3.11 >/dev/null 2>&1; then
        PYTHON_CMD="python3.11"
    elif command -v python3 >/dev/null 2>&1; then
        PYTHON_CMD="python3"
    elif command -v python >/dev/null 2>&1; then
        PYTHON_CMD="python"
    else
        echo "Error: No Python command found. Please install Python or set PYTHON_CMD environment variable."
        exit 1
    fi
fi

echo "Building Lambda Layer with dependencies..."
echo "Using Python command: $PYTHON_CMD"

# Verify Python command works
echo "Verifying Python installation..."
$PYTHON_CMD --version
if [ $? -ne 0 ]; then
    echo "Error: Python command '$PYTHON_CMD' failed. Available Python commands:"
    which python3.11 python3 python || echo "No Python commands found in PATH"
    exit 1
fi

# Clean up any existing build artifacts
echo "Cleaning up existing build artifacts..."
rm -rf python/
rm -f layer.zip

# Create a minimal placeholder zip file for Terraform validation
echo "Creating placeholder layer.zip for Terraform validation..."
mkdir -p python
echo "# Placeholder for Terraform validation" > python/placeholder.txt
zip -q layer.zip python/placeholder.txt
rm -rf python/

# Create python directory structure
echo "Creating python directory structure..."
mkdir -p python/

# Install dependencies into the python directory
echo "Installing dependencies using $PYTHON_CMD..."
echo "Using requirements file: layer-definitions/${REQUIREMENTS_FILE:-chat-agent-dependencies.txt}"
$PYTHON_CMD -m pip install -r "layer-definitions/${REQUIREMENTS_FILE:-chat-agent-dependencies.txt}" -t python/ --no-user

if [ $? -ne 0 ]; then
    echo "Error: Failed to install dependencies"
    exit 1
fi

# Remove unnecessary files to reduce size
echo "Cleaning up unnecessary files..."

# Remove __pycache__ directories
find python/ -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true

# Remove compiled Python files (keep .so files for Linux Lambda)
find python/ -name "*.pyc" -delete 2>/dev/null || true
find python/ -name "*.pyo" -delete 2>/dev/null || true
find python/ -name "*.pyd" -delete 2>/dev/null || true

# Remove unnecessary distribution info files (keep OpenTelemetry one)
find python/ -name "*.dist-info" -type d -not -name "opentelemetry_api-*" -exec rm -rf {} + 2>/dev/null || true
find python/ -name "*.egg-info" -type d -exec rm -rf {} + 2>/dev/null || true

# Remove macOS and Windows shared libraries (keep Linux .so files for Lambda)
find python/ -name "*.dylib" -delete 2>/dev/null || true
find python/ -name "*.dll" -delete 2>/dev/null || true

# Remove test directories and documentation
find python/ -type d -name "test*" -exec rm -rf {} + 2>/dev/null || true
find python/ -type d -name "tests" -exec rm -rf {} + 2>/dev/null || true
find python/ -type d -name "doc" -exec rm -rf {} + 2>/dev/null || true
find python/ -type d -name "docs" -exec rm -rf {} + 2>/dev/null || true
find python/ -name "*.md" -delete 2>/dev/null || true
find python/ -name "*.rst" -delete 2>/dev/null || true

# Remove most .txt files but preserve important ones
find python/ -name "*.txt" -not -path "*/opentelemetry_api-*/entry_points.txt" -delete 2>/dev/null || true

# Fix OpenTelemetry entry points issue (must be done AFTER cleanup)
echo "Fixing OpenTelemetry entry points..."
ENTRY_POINTS_PATH="python/opentelemetry_api-1.36.0.dist-info/entry_points.txt"
if [ -d "python/opentelemetry_api-1.36.0.dist-info" ]; then
    cat > "$ENTRY_POINTS_PATH" << 'EOF'
[opentelemetry_context]
contextvars_context = opentelemetry.context.contextvars_context:ContextVarsRuntimeContext
EOF
    echo "Created OpenTelemetry entry points file"
else
    echo "OpenTelemetry distribution directory not found"
fi

# Create the layer zip file
echo "Creating layer zip file..."
zip -r layer.zip python/ -q

# Get the size of the layer
LAYER_SIZE=$(du -h layer.zip | cut -f1)
LAYER_SIZE_MB=$(du -m layer.zip | cut -f1)
echo "Layer created successfully: layer.zip (${LAYER_SIZE})"

# Verify the layer structure
echo "Verifying layer structure..."
echo "First 10 entries in layer:"
unzip -l layer.zip | head -12 | tail -10

# Verify critical files exist
echo "Checking for critical files..."
if unzip -l layer.zip | grep -q "python/opentelemetry_api-.*/entry_points.txt"; then
    echo "✓ OpenTelemetry entry_points.txt found"
else
    echo "✗ OpenTelemetry entry_points.txt missing"
fi

if unzip -l layer.zip | grep -q "python/pydantic_core/"; then
    echo "✓ pydantic_core found"
else
    echo "✗ pydantic_core missing"
fi

# Check if layer is within size limits
if [ $LAYER_SIZE_MB -gt 250 ]; then
    echo "Warning: Layer size (${LAYER_SIZE_MB}MB) exceeds AWS Lambda layer limit of 250MB"
    echo "Consider removing unnecessary dependencies or splitting into multiple layers"
fi

echo "Lambda layer build completed successfully!"
