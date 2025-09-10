#!/bin/bash

# Simple Lambda Layer Builder
# Usage: ./build-layer.sh [LAYER_NAME]
# Example: ./build-layer.sh financial

set -e

# Configuration
LAYER_NAME=${1:-"financial"}
REQUIREMENTS_FILE="layer-definitions/${LAYER_NAME}-dependencies.txt"
OUTPUT_FILE="layer-${LAYER_NAME}.zip"

echo "🔨 Building Lambda layer: ${LAYER_NAME}"

# Check if requirements file exists
if [ ! -f "${REQUIREMENTS_FILE}" ]; then
    echo "❌ Requirements file not found: ${REQUIREMENTS_FILE}"
    exit 1
fi

# Clean up any existing build
rm -rf python/
rm -f ${OUTPUT_FILE}

# Create python directory
mkdir -p python/

echo "📦 Installing dependencies from ${REQUIREMENTS_FILE}..."

# Install dependencies with Linux compatibility
pip install -r "${REQUIREMENTS_FILE}" -t python/ \
    --platform manylinux2014_x86_64 \
    --implementation cp \
    --python-version 3.11 \
    --only-binary=:all: \
    --no-cache-dir

echo "🗜️ Creating layer zip file..."

# Create the layer zip
cd python/
zip -r "../${OUTPUT_FILE}" .
cd ..

# Clean up
rm -rf python/

echo "✅ Layer created successfully: ${OUTPUT_FILE}"
echo "📊 Layer size: $(du -h ${OUTPUT_FILE} | cut -f1)"