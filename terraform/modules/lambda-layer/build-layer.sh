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
echo "Using requirements file: layer-definitions/${REQUIREMENTS_FILE:-chat-agent-dependencies-smart.txt}"
$PYTHON_CMD -m pip install -r "layer-definitions/${REQUIREMENTS_FILE:-chat-agent-dependencies-smart.txt}" -t python/ --no-user

if [ $? -ne 0 ]; then
    echo "Error: Failed to install dependencies"
    exit 1
fi

# Show package sizes to help identify large dependencies
echo "Package sizes after installation:"
du -h python/ | sort -hr | head -15

echo ""
echo "Analyzing largest packages for optimization opportunities..."
echo "Top 10 largest packages:"
du -h python/ | sort -hr | head -10 | while read size path; do
    package_name=$(basename "$path")
    echo "  $size - $package_name"
done

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

# Smart cleanup to reduce layer size while preserving functionality
echo "Performing smart cleanup to reduce layer size..."

# Remove package installation artifacts (these are not needed at runtime)
find python/ -name "*.whl" -delete 2>/dev/null || true
find python/ -name "*.tar.gz" -delete 2>/dev/null || true
find python/ -name "*.zip" -delete 2>/dev/null || true

# Remove documentation and example files (not needed at runtime)
find python/ -name "*example*" -type f -delete 2>/dev/null || true
find python/ -name "*sample*" -type f -delete 2>/dev/null || true
find python/ -name "*demo*" -type f -delete 2>/dev/null || true
find python/ -name "*test*" -type f -delete 2>/dev/null || true

# Remove documentation files
find python/ -name "LICENSE*" -delete 2>/dev/null || true
find python/ -name "CHANGELOG*" -delete 2>/dev/null || true
find python/ -name "HISTORY*" -delete 2>/dev/null || true
find python/ -name "NEWS*" -delete 2>/dev/null || true
find python/ -name "README*" -delete 2>/dev/null || true

# Remove large data files that might be included (but keep small config files)
find python/ -name "*.json" -size +100k -delete 2>/dev/null || true
find python/ -name "*.xml" -size +100k -delete 2>/dev/null || true
find python/ -name "*.csv" -size +100k -delete 2>/dev/null || true

# Remove distribution metadata (but keep essential package info)
find python/ -type d -name "*.dist-info" -exec rm -rf {} + 2>/dev/null || true
find python/ -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true

# Fix OpenTelemetry entry points issue (must be done BEFORE removing dist-info)
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

# Remove OpenTelemetry distribution info but keep the entry points file
# Only remove the dist-info directory, not the entire opentelemetry package
find python/ -type d -name "opentelemetry*" -name "*.dist-info" -exec rm -rf {} + 2>/dev/null || true

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
LAYER_SIZE_BYTES=$(stat -c%s layer.zip 2>/dev/null || echo 0)
MAX_SIZE_BYTES=67108864  # 64MB limit for Lambda layers

if [ $LAYER_SIZE_BYTES -gt $MAX_SIZE_BYTES ]; then
    echo "❌ ERROR: Layer size (${LAYER_SIZE_MB}MB / ${LAYER_SIZE_BYTES} bytes) exceeds AWS Lambda layer limit of 64MB"
    echo "Current size: ${LAYER_SIZE_MB}MB"
    echo "Maximum allowed: 64MB"
    echo ""
    echo "To reduce layer size, consider:"
    echo "1. Removing unnecessary dependencies from requirements.txt"
    echo "2. Splitting into multiple layers"
    echo "3. Using lighter alternatives for heavy packages"
    echo ""
    echo "Largest directories in the layer:"
    du -h python/ | sort -hr | head -10
    exit 1
elif [ $LAYER_SIZE_MB -gt 50 ]; then
    echo "⚠️  Warning: Layer size (${LAYER_SIZE_MB}MB) is getting close to the 64MB limit"
    echo "Consider optimizing dependencies to reduce size"
fi

echo "Lambda layer build completed successfully!"
