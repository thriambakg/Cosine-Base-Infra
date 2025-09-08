#!/bin/bash
# Container-based Lambda Layer Builder - Build All Layers
# Dynamically discovers and builds all layers

set -e

echo "[INFO] Starting container-based Lambda layer build process..."

# Set environment variables
export S3_BUCKET_NAME="${S3_BUCKET_NAME}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"

echo "[INFO] Environment:"
echo "  S3_BUCKET_NAME: $S3_BUCKET_NAME"
echo "  AWS_DEFAULT_REGION: $AWS_DEFAULT_REGION"

# Discover available layers
echo "[INFO] Discovering available layers..."
AVAILABLE_LAYERS=()

for req_file in layer-definitions/*-dependencies.txt; do
    if [ -f "$req_file" ]; then
        layer_name=$(basename "$req_file" -dependencies.txt)
        AVAILABLE_LAYERS+=("$layer_name")
        echo "[INFO] Found layer: $layer_name"
    fi
done

if [ ${#AVAILABLE_LAYERS[@]} -eq 0 ]; then
    echo "[ERROR] No layer definition files found in layer-definitions/"
    exit 1
fi

echo "[INFO] Discovered ${#AVAILABLE_LAYERS[@]} layers: ${AVAILABLE_LAYERS[*]}"

# Build all layers
echo "[INFO] Building all layers..."
for layer in "${AVAILABLE_LAYERS[@]}"; do
    echo "[INFO] Building layer: $layer"
    ./build-scripts/build-layer.sh "$layer"
    
    if [ $? -eq 0 ]; then
        echo "[SUCCESS] Layer '$layer' built successfully"
    else
        echo "[ERROR] Failed to build layer '$layer'"
        exit 1
    fi
done

echo "[SUCCESS] All layers built successfully!"
echo "[INFO] Layer summary:"
for layer in "${AVAILABLE_LAYERS[@]}"; do
    if [ -f "layer-${layer}.zip" ]; then
        size=$(stat -c%s "layer-${layer}.zip" 2>/dev/null || stat -f%z "layer-${layer}.zip" 2>/dev/null || echo "unknown")
        echo "  - $layer: layer-${layer}.zip ($size bytes)"
    fi
done

echo "[SUCCESS] Container-based layer build process completed!"
echo "[SUCCESS] All layers are ready for Terraform deployment"
