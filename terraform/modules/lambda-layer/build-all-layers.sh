#!/bin/bash

# Build All Layers Script
# This script builds all layers or specific layers as requested
# Usage: ./build-all-layers.sh [layer1] [layer2] ...
# Example: ./build-all-layers.sh                    # Build all layers
# Example: ./build-all-layers.sh core financial     # Build only core and financial

set -e  # Exit on any error

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Dynamically discover available layers
AVAILABLE_LAYERS=()
for req_file in layer-definitions/*-dependencies.txt; do
  if [ -f "$req_file" ]; then
    layer_name=$(basename "$req_file" -dependencies.txt)
    AVAILABLE_LAYERS+=("$layer_name")
  fi
done

# Also check for other naming patterns
for req_file in layer-definitions/*dependencies.txt; do
  if [ -f "$req_file" ]; then
    filename=$(basename "$req_file" .txt)
    if [[ $filename == *"-dependencies" ]]; then
      layer_name=${filename%-dependencies}
    elif [[ $filename == *"dependencies" ]]; then
      layer_name=${filename%dependencies}
    fi
    if [[ ! " ${AVAILABLE_LAYERS[@]} " =~ " ${layer_name} " ]]; then
      AVAILABLE_LAYERS+=("$layer_name")
    fi
  fi
done

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

# Show discovered layers
print_status "Discovered ${#AVAILABLE_LAYERS[@]} available layers: ${AVAILABLE_LAYERS[*]}"

# Determine which layers to build
if [ $# -eq 0 ]; then
    # Build all layers
    LAYERS_TO_BUILD=("${AVAILABLE_LAYERS[@]}")
    print_status "Building all layers: ${LAYERS_TO_BUILD[*]}"
else
    # Build specified layers
    LAYERS_TO_BUILD=("$@")
    print_status "Building specified layers: ${LAYERS_TO_BUILD[*]}"
fi

# Validate layer names
for layer in "${LAYERS_TO_BUILD[@]}"; do
    if [[ ! " ${AVAILABLE_LAYERS[@]} " =~ " ${layer} " ]]; then
        echo "Error: Unknown layer '${layer}'"
        echo "Available layers: ${AVAILABLE_LAYERS[*]}"
        exit 1
    fi
done

# Make build script executable
chmod +x build-layer.sh

# Build each layer
for layer in "${LAYERS_TO_BUILD[@]}"; do
    print_status "Building layer: ${layer}"
    ./build-layer.sh "${layer}"
    print_success "Layer '${layer}' built successfully"
    echo ""
done

print_success "All requested layers built successfully!"
print_status "Layer files created:"
for layer in "${LAYERS_TO_BUILD[@]}"; do
    if [ -f "layer-${layer}.zip" ]; then
        compressed_size=$(du -h "layer-${layer}.zip" | cut -f1)
        compressed_mb=$(du -m "layer-${layer}.zip" | cut -f1)
        echo "  - layer-${layer}.zip (${compressed_size} compressed)"
    fi
done
