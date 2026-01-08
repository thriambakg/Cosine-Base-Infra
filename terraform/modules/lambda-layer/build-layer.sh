#!/bin/bash

# Enhanced Lambda Layer Builder
# Usage: ./build-layer.sh [LAYER_NAME] [SOURCE_DIR]
# Example: ./build-layer.sh financial
# Example: ./build-layer.sh shared-code ../shared_layers

set -e

# Configuration
LAYER_NAME=${1:-"financial"}
SOURCE_DIR=${2:-""}
REQUIREMENTS_FILE="layer-definitions/${LAYER_NAME}-dependencies.txt"
OUTPUT_FILE="layer-${LAYER_NAME}.zip"

echo "🔨 Building Lambda layer: ${LAYER_NAME}"

# Clean up any existing build
rm -rf python/
rm -f ${OUTPUT_FILE}

# Create python directory
mkdir -p python/

# Handle requirements file (if exists)
if [ -f "${REQUIREMENTS_FILE}" ]; then
    echo "📦 Installing dependencies from ${REQUIREMENTS_FILE}..."
    
    # Install dependencies with Linux compatibility
    # Note: When using platform/python-version constraints, must use --only-binary=:all:
    pip install -r "${REQUIREMENTS_FILE}" -t python/ \
        --platform manylinux2014_x86_64 \
        --implementation cp \
        --python-version 3.11 \
        --only-binary=:all: \
        --no-cache-dir
    
    echo "🧹 Cleaning up unnecessary dependency files..."
    
    # Remove unnecessary files to reduce layer size
    find python/ -name "*.pyc" -delete 2>/dev/null || true
    find python/ -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
    # Keep *.dist-info to preserve package metadata needed by importlib
    find python/ -name "tests" -type d -exec rm -rf {} + 2>/dev/null || true
    find python/ -name "test_*" -delete 2>/dev/null || true
    find python/ -name "*_test.py" -delete 2>/dev/null || true
else
    echo "⚠️ No requirements file found: ${REQUIREMENTS_FILE}"
fi

# Handle source code (if provided)
if [ -n "${SOURCE_DIR}" ] && [ -d "${SOURCE_DIR}" ]; then
    echo "📁 Copying source code from ${SOURCE_DIR}..."
    
    # Copy source files to python directory
    cp -r "${SOURCE_DIR}"/* python/ 2>/dev/null || true
    
    echo "🧹 Cleaning up source code files..."
    
    # Remove unnecessary files from source code
    find python/ -name "*.pyc" -delete 2>/dev/null || true
    find python/ -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
    find python/ -name "*.pyo" -delete 2>/dev/null || true
    find python/ -name "*.pyd" -delete 2>/dev/null || true
    find python/ -name ".DS_Store" -delete 2>/dev/null || true
    find python/ -name "Thumbs.db" -delete 2>/dev/null || true
else
    echo "⚠️ No source directory provided or directory not found: ${SOURCE_DIR}"
fi

echo "🗜️ Creating layer zip file..."

# Create the layer zip (zip the python directory, not its contents)
zip -r "${OUTPUT_FILE}" python/

# Clean up
rm -rf python/

echo "✅ Layer created successfully: ${OUTPUT_FILE}"
echo "📊 Layer size: $(du -h ${OUTPUT_FILE} | cut -f1)"