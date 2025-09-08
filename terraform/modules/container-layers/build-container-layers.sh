#!/bin/bash
# Container-based Lambda Layer Builder Script
# This script builds and pushes container images, then runs them to build layers
# Integrates with existing GitHub Actions workflow

set -e

# Configuration
PROJECT_NAME="${PROJECT_NAME:-cosine}"
ENVIRONMENT="${ENVIRONMENT:-production}"
AWS_REGION="${AWS_REGION:-us-east-1}"
ECR_REGISTRY="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"
ECR_REPOSITORY="${PROJECT_NAME}-layer-builder-${ENVIRONMENT}"
S3_BUCKET_NAME="${PROJECT_NAME}-layer-artifacts-${ENVIRONMENT}"

echo "[INFO] Starting container-based Lambda layer build process..."
echo "[INFO] Configuration:"
echo "  PROJECT_NAME: $PROJECT_NAME"
echo "  ENVIRONMENT: $ENVIRONMENT"
echo "  AWS_REGION: $AWS_REGION"
echo "  ECR_REGISTRY: $ECR_REGISTRY"
echo "  ECR_REPOSITORY: $ECR_REPOSITORY"
echo "  S3_BUCKET_NAME: $S3_BUCKET_NAME"

# Check required environment variables
if [ -z "$AWS_ACCOUNT_ID" ]; then
    echo "[ERROR] AWS_ACCOUNT_ID environment variable is required"
    exit 1
fi

# Login to ECR
echo "[INFO] Logging in to Amazon ECR..."
aws ecr get-login-password --region $AWS_REGION | docker login --username AWS --password-stdin $ECR_REGISTRY

# Build and push container image
echo "[INFO] Building container image..."
cd terraform/modules/container-layers
docker build -t $ECR_REGISTRY/$ECR_REPOSITORY:latest .

echo "[INFO] Pushing container image to ECR..."
docker push $ECR_REGISTRY/$ECR_REPOSITORY:latest

# Run container to build layers
echo "[INFO] Running container to build layers..."
docker run --rm \
    -e AWS_DEFAULT_REGION=$AWS_REGION \
    -e S3_BUCKET_NAME=$S3_BUCKET_NAME \
    -v $(pwd):/app \
    $ECR_REGISTRY/$ECR_REPOSITORY:latest

# Verify layer artifacts
echo "[INFO] Verifying layer artifacts..."
if [ -d "layer-*.zip" ]; then
    echo "[SUCCESS] Layer artifacts created successfully"
    ls -la layer-*.zip
else
    echo "[ERROR] No layer artifacts found"
    exit 1
fi

# Verify S3 upload
echo "[INFO] Verifying layer artifacts in S3..."
aws s3 ls s3://$S3_BUCKET_NAME/layers/ --recursive

echo "[SUCCESS] Container-based layer build process completed!"
echo "[SUCCESS] All layers are ready for Terraform deployment"
