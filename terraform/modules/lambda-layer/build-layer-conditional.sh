#!/bin/bash

# Conditional build script for Lambda Layer
# This script checks if the layer already exists and is valid before building

set -e

echo "Checking if layer.zip exists and is valid..."

# Check if layer.zip exists and is not empty
if [ -f layer.zip ] && [ -s layer.zip ]; then
    # Check if the file size is reasonable (at least 100 bytes to avoid placeholder files)
    LAYER_SIZE=$(stat -c%s layer.zip 2>/dev/null || echo 0)
    if [ $LAYER_SIZE -gt 100 ]; then
        echo "Layer already exists and is valid (${LAYER_SIZE} bytes), skipping build"
        exit 0
    else
        echo "Layer exists but is too small (${LAYER_SIZE} bytes), rebuilding..."
    fi
else
    echo "Layer does not exist or is empty, building..."
fi

echo "Building layer..."
./build-layer.sh
