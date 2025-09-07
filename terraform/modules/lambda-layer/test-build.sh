#!/bin/bash

# Test script to verify the build process works correctly
# This script tests building a single layer to ensure everything is working

set -e

echo "🧪 Testing Lambda Layer Build Process"
echo "======================================"

# Test building the core layer (smallest and most essential)
echo "Testing core layer build..."
./build-layer.sh core

# Verify the layer was created
if [ -f "layer-core.zip" ]; then
    size=$(du -h "layer-core.zip" | cut -f1)
    echo "✅ Core layer created successfully: layer-core.zip (${size})"
    
    # Show layer contents
    echo "Layer contents:"
    unzip -l "layer-core.zip" | head -10
    
    # Clean up
    rm -f "layer-core.zip"
    rm -rf "python"
    echo "✅ Test completed successfully"
else
    echo "❌ Core layer was not created"
    exit 1
fi

echo ""
echo "🎉 All tests passed! The build process is working correctly."
