# Container-based Lambda Layer Builder
# This container builds Lambda layers and uploads them to S3/ECR
# Runs only during deployment, not during Lambda execution

FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    zip \
    curl \
    awscli \
    && rm -rf /var/lib/apt/lists/*

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

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV AWS_DEFAULT_REGION=us-east-1

# Default command
CMD ["./build-scripts/build-all-layers.sh"]
