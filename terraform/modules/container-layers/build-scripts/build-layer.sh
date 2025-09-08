#!/bin/bash
# Container-based Lambda Layer Builder Script
# Builds individual layers with no size restrictions

set -e

LAYER_NAME="${LAYER_NAME}"
REQUIREMENTS_FILE="layer-definitions/${REQUIREMENTS_FILE}"
OUTPUT_FILE="layer-${LAYER_NAME}.zip"
S3_BUCKET="${S3_BUCKET_NAME}"
S3_KEY="layers/${OUTPUT_FILE}"

echo "[INFO] Building Lambda layer: $LAYER_NAME"
echo "[INFO] Requirements file: $REQUIREMENTS_FILE"
echo "[INFO] Output file: $OUTPUT_FILE"

# Check if requirements file exists
if [ ! -f "$REQUIREMENTS_FILE" ]; then
    echo "[ERROR] Requirements file not found: $REQUIREMENTS_FILE"
    exit 1
fi

# Clean up any existing build artifacts
echo "[INFO] Cleaning up existing build artifacts..."
rm -rf python/
rm -f "layer-*.zip"

# Create python directory structure
echo "[INFO] Creating Python directory structure..."
mkdir -p python/

# Install dependencies
echo "[INFO] Installing dependencies from $REQUIREMENTS_FILE..."
pip install --no-cache-dir --target python/ -r "$REQUIREMENTS_FILE"

# Verify installation
echo "[INFO] Verifying package installation..."
if [ -d "python/" ] && [ "$(ls -A python/)" ]; then
    echo "[SUCCESS] Packages installed successfully"
    ls -la python/ | head -10
else
    echo "[ERROR] No packages found in python/ directory"
    exit 1
fi

# Clean up unnecessary files
echo "[INFO] Cleaning up unnecessary files..."
find python/ -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
find python/ -name "*.pyc" -delete 2>/dev/null || true
find python/ -name "*.pyo" -delete 2>/dev/null || true
find python/ -name "*.pyd" -delete 2>/dev/null || true
find python/ -name "*.so" -not -path "*/lib/*" -delete 2>/dev/null || true

# Remove test files and documentation
find python/ -name "test*" -type d -exec rm -rf {} + 2>/dev/null || true
find python/ -name "tests" -type d -exec rm -rf {} + 2>/dev/null || true
find python/ -name "*.md" -delete 2>/dev/null || true
find python/ -name "*.txt" -not -name "*.dist-info" -delete 2>/dev/null || true

# Create layer zip file
echo "[INFO] Creating layer zip file: $OUTPUT_FILE"
cd python/
zip -r "../$OUTPUT_FILE" . -q
cd ..

# Verify zip file
if [ -f "$OUTPUT_FILE" ]; then
    ZIP_SIZE=$(stat -c%s "$OUTPUT_FILE" 2>/dev/null || stat -f%z "$OUTPUT_FILE" 2>/dev/null || echo "unknown")
    echo "[SUCCESS] Layer created successfully: $OUTPUT_FILE ($ZIP_SIZE bytes)"
else
    echo "[ERROR] Failed to create layer zip file"
    exit 1
fi

# Upload to S3
echo "[INFO] Uploading layer to S3..."
aws s3 cp "$OUTPUT_FILE" "s3://$S3_BUCKET/$S3_KEY"

if [ $? -eq 0 ]; then
    echo "[SUCCESS] Layer uploaded to S3: s3://$S3_BUCKET/$S3_KEY"
else
    echo "[ERROR] Failed to upload layer to S3"
    exit 1
fi

echo "[SUCCESS] Lambda layer '$LAYER_NAME' build completed successfully!"
echo "[SUCCESS] Layer file: $OUTPUT_FILE"
echo "[SUCCESS] S3 location: s3://$S3_BUCKET/$S3_KEY"
echo "[SUCCESS] Ready for Terraform deployment"
