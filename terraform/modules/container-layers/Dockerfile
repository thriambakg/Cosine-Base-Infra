# Container-based Lambda Layer Builder
# This container builds Lambda layers and uploads them to S3/ECR
# Runs only during deployment, not during Lambda execution

FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    zip \
    curl \
    awscli \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN groupadd -r layerbuilder && useradd -r -g layerbuilder layerbuilder

# Set working directory
WORKDIR /app

# Copy layer definitions and build scripts
COPY layer-definitions/ ./layer-definitions/
COPY build-scripts/ ./build-scripts/
COPY requirements.txt .

# Install Python dependencies for layer building
RUN pip install --no-cache-dir -r requirements.txt

# Make build scripts executable
RUN chmod +x build-scripts/*.sh

# Change ownership to non-root user
RUN chown -R layerbuilder:layerbuilder /app

# Ensure build scripts are still executable after ownership change
RUN chmod +x build-scripts/*.sh

# Switch to non-root user
USER layerbuilder

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV AWS_DEFAULT_REGION=us-east-1

# Default command - build individual layer based on environment variables
# Add debugging and ensure script exists
CMD ["sh", "-c", "echo 'Container started' && ls -la build-scripts/ && file build-scripts/build-layer.sh && ./build-scripts/build-layer.sh"]
