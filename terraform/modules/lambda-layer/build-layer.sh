#!/bin/bash

# Modular Lambda Layer Builder
# This script can build any layer individually by specifying the layer name
# Usage: ./build-layer.sh [LAYER_NAME]
# Example: ./build-layer.sh core
# Example: ./build-layer.sh financial
# Example: ./build-layer.sh ai
# Example: ./build-layer.sh utility
# Version: 2.9 - DOCKER FIX: Added NumPy shared library cleanup

set -e  # Exit on any error

# Configuration
LAYER_NAME=${1:-"core"}
REQUIREMENTS_FILE="${LAYER_NAME}-dependencies.txt"
LAYER_FILE="layer-${LAYER_NAME}.zip"
PYTHON_CMD=${PYTHON_CMD:-"python3"}

# Check for alternative naming patterns
if [ ! -f "layer-definitions/${REQUIREMENTS_FILE}" ]; then
    # Try other naming patterns
    for pattern in "${LAYER_NAME}dependencies.txt" "${LAYER_NAME}-core-dependencies.txt"; do
        if [ -f "layer-definitions/${pattern}" ]; then
            REQUIREMENTS_FILE="${pattern}"
            break
        fi
    done
fi

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Validate inputs
if [ ! -f "layer-definitions/${REQUIREMENTS_FILE}" ]; then
    print_error "Requirements file not found: layer-definitions/${REQUIREMENTS_FILE}"
    print_error "Available layers: core, financial, ai, utility"
    exit 1
fi

print_status "Building Lambda layer: ${LAYER_NAME}"
print_status "Requirements file: ${REQUIREMENTS_FILE}"
print_status "Output file: ${LAYER_FILE}"

# Detect Python command
if command -v python3.11 >/dev/null 2>&1; then
    PYTHON_CMD="python3.11"
elif command -v python3 >/dev/null 2>&1; then
    PYTHON_CMD="python3"
elif command -v python >/dev/null 2>&1; then
    PYTHON_CMD="python"
else
    print_error "Python not found. Please install Python 3.11 or later."
    exit 1
fi

print_status "Using Python command: ${PYTHON_CMD}"

# Verify Python version
PYTHON_VERSION=$(${PYTHON_CMD} --version 2>&1 | cut -d' ' -f2)
print_status "Python version: ${PYTHON_VERSION}"

# Remove any existing layer file to ensure clean creation
if [ -f "${LAYER_FILE}" ]; then
    print_status "Removing existing layer file: ${LAYER_FILE}"
    rm -f "${LAYER_FILE}"
fi

# Clean up any existing python directory
if [ -d "python" ]; then
    print_status "Cleaning up existing python directory..."
    rm -rf python
fi

# Create python directory
mkdir -p python

# Install dependencies
print_status "Installing dependencies from ${REQUIREMENTS_FILE}..."
print_status "Using pip install with Linux compatibility flags..."

# Special handling for financial layer with NumPy
if [ "${LAYER_NAME}" = "financial" ]; then
    print_status "Installing financial layer with Docker-based AWS Lambda compatibility..."
    
    # Use Docker to build in exact AWS Lambda environment
    if command -v docker >/dev/null 2>&1; then
        print_status "Using Docker to build in AWS Lambda-compatible environment..."
        
        # Create a temporary Dockerfile for building the layer
        cat > Dockerfile.layer << 'EOF'
FROM public.ecr.aws/lambda/python:3.11

# Create output directory
RUN mkdir -p /output

# Install dependencies in the exact Lambda environment
COPY layer-definitions/financial-dependencies.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt -t /tmp/python/ --no-cache-dir --force-reinstall

# Clean up any source directory conflicts
RUN find /tmp/python/ -name "*.egg-info" -type d -exec rm -rf {} + 2>/dev/null || true
RUN find /tmp/python/ -name "dist" -type d -exec rm -rf {} + 2>/dev/null || true
RUN find /tmp/python/ -name "build" -type d -exec rm -rf {} + 2>/dev/null || true
RUN find /tmp/python/ -name "setup.py" -delete 2>/dev/null || true
RUN find /tmp/python/ -name "pyproject.toml" -delete 2>/dev/null || true

# Clean up NumPy shared libraries that cause Lambda import issues
RUN find /tmp/python/ -name "libopenblas*" -delete 2>/dev/null || true
RUN find /tmp/python/ -name "libgfortran*" -delete 2>/dev/null || true
RUN find /tmp/python/ -name "libquadmath*" -delete 2>/dev/null || true
RUN find /tmp/python/ -name "liblapack*" -delete 2>/dev/null || true
RUN find /tmp/python/ -name "libblas*" -delete 2>/dev/null || true
RUN find /tmp/python/ -name "*.so" -path "*/numpy/*" -delete 2>/dev/null || true
RUN find /tmp/python/ -name "*.so" -path "*/scipy/*" -delete 2>/dev/null || true

# Copy the clean python directory contents to output/python
RUN mkdir -p /output/python && cp -r /tmp/python/* /output/python/
EOF
        
        # Build the layer using Docker
        docker build -f Dockerfile.layer -t lambda-layer-builder .
        
        # Extract the python directory from the Docker container
        docker create --name temp-container lambda-layer-builder
        docker cp temp-container:/output/python/. ./python/
        docker rm temp-container
        
        # Clean up
        rm -f Dockerfile.layer
        docker rmi lambda-layer-builder 2>/dev/null || true
        
        print_success "Financial layer built using Docker in AWS Lambda environment"
    else
        print_warning "Docker not available, falling back to local build..."
        
        # Clean up any existing numpy directories to avoid source directory conflicts
        print_status "Cleaning up any existing NumPy installations..."
        rm -rf python/numpy* 2>/dev/null || true
        rm -rf python/pandas* 2>/dev/null || true
        rm -rf python/scipy* 2>/dev/null || true
        
        # Additional cleanup to prevent source directory conflicts
        find python/ -name "*numpy*" -type d -exec rm -rf {} + 2>/dev/null || true
        find python/ -name "*pandas*" -type d -exec rm -rf {} + 2>/dev/null || true
        find python/ -name "*scipy*" -type d -exec rm -rf {} + 2>/dev/null || true
    
    # Install NumPy with a completely different approach to avoid source directory conflicts
    print_status "Installing NumPy with Lambda-compatible settings..."
    
    # Try installing NumPy with AWS Lambda-compatible platform flags
    if ! ${PYTHON_CMD} -m pip install "numpy==1.24.4" -t python/ --platform manylinux2014_x86_64 --implementation cp --python-version 3.11 --only-binary=:all: --upgrade --no-cache-dir --force-reinstall --no-deps; then
        print_warning "NumPy installation with manylinux2014_x86_64 failed, trying linux_x86_64..."
        if ! ${PYTHON_CMD} -m pip install "numpy==1.24.4" -t python/ --platform linux_x86_64 --implementation cp --python-version 3.11 --only-binary=:all: --upgrade --no-cache-dir --force-reinstall --no-deps; then
            print_warning "NumPy installation with platform flags failed, trying without platform constraints..."
            if ! ${PYTHON_CMD} -m pip install "numpy==1.24.4" -t python/ --upgrade --no-cache-dir --force-reinstall --no-deps; then
                print_error "NumPy installation failed completely. Trying with more flexible version constraints..."
                ${PYTHON_CMD} -m pip install "numpy==1.24.4" -t python/ --upgrade --no-cache-dir --force-reinstall --no-deps
            fi
        fi
    fi
    
    # Install NumPy dependencies separately
    print_status "Installing NumPy dependencies..."
    ${PYTHON_CMD} -m pip install "python-dateutil>=2.8.2" -t python/ --upgrade --no-cache-dir --force-reinstall
    
    # Install other financial dependencies
    print_status "Installing other financial dependencies..."
    if ! ${PYTHON_CMD} -m pip install "pandas>=2.0.0,<2.1.0" "scipy>=1.10.0,<1.11.0" "yfinance>=0.2.18" -t python/ --platform manylinux2014_x86_64 --implementation cp --python-version 3.11 --only-binary=:all: --upgrade --no-cache-dir --force-reinstall; then
        print_warning "Financial dependencies installation with manylinux2014_x86_64 failed, trying linux_x86_64..."
        if ! ${PYTHON_CMD} -m pip install "pandas>=2.0.0,<2.1.0" "scipy>=1.10.0,<1.11.0" "yfinance>=0.2.18" -t python/ --platform linux_x86_64 --implementation cp --python-version 3.11 --only-binary=:all: --upgrade --no-cache-dir --force-reinstall; then
            print_warning "Financial dependencies installation with platform flags failed, trying without..."
            ${PYTHON_CMD} -m pip install "pandas>=2.0.0,<2.1.0" "scipy>=1.10.0,<1.11.0" "yfinance>=0.2.18" -t python/ --upgrade --no-cache-dir --force-reinstall
        fi
    fi
    
        print_success "Financial dependencies installed with Python 3.11 compatibility fixes"
    fi
else
    # Standard installation for other layers
    if ! ${PYTHON_CMD} -m pip install -r "layer-definitions/${REQUIREMENTS_FILE}" -t python/ --platform manylinux2014_x86_64 --implementation cp --python-version 3.11 --only-binary=:all: --upgrade --no-cache-dir; then
        print_error "Failed to install dependencies. Trying with more flexible options..."
        
        # Try again without platform restrictions for packages that might not have Linux wheels
        if ! ${PYTHON_CMD} -m pip install -r "layer-definitions/${REQUIREMENTS_FILE}" -t python/ --upgrade --no-cache-dir; then
            print_error "Failed to install dependencies even with flexible options."
            print_error "Please check your requirements file: layer-definitions/${REQUIREMENTS_FILE}"
            print_error "Consider using more flexible version constraints (e.g., >=1.0.0 instead of ==1.0.0)"
            exit 1
        else
            print_warning "Dependencies installed with flexible options (may not be Linux-optimized)"
        fi
    else
        print_success "Dependencies installed successfully with Linux compatibility"
    fi
fi

# Verify critical packages were installed
print_status "Verifying package installation..."
if [ ! -d "python" ] || [ -z "$(ls -A python)" ]; then
    print_error "No packages were installed. Check your requirements file."
    exit 1
fi

# Show installed packages for debugging
print_status "Installed packages:"
ls python/ | head -10
if [ $(ls python/ | wc -l) -gt 10 ]; then
    echo "... and $(($(ls python/ | wc -l) - 10)) more packages"
fi

# Fix NumPy for Lambda compatibility (financial layer only)
if [ "${LAYER_NAME}" = "financial" ]; then
    print_status "Applying NumPy Lambda compatibility fixes..."
    
    # Remove problematic shared libraries that cause import errors
    find python/ -name "libopenblas*.so*" -delete 2>/dev/null || true
    find python/ -name "libgfortran*.so*" -delete 2>/dev/null || true
    find python/ -name "libquadmath*.so*" -delete 2>/dev/null || true
    find python/ -name "liblapack*.so*" -delete 2>/dev/null || true
    find python/ -name "libblas*.so*" -delete 2>/dev/null || true
    
    # Fix NumPy source directory conflicts
    if [ -d "python/numpy" ]; then
        print_status "Fixing NumPy source directory structure..."
        
        # Remove ALL source files that could cause conflicts
        find python/numpy/ -name "*.py" -path "*/tests/*" -delete 2>/dev/null || true
        find python/numpy/ -name "*.py" -path "*/doc/*" -delete 2>/dev/null || true
        find python/numpy/ -name "*.py" -path "*/benchmarks/*" -delete 2>/dev/null || true
        find python/numpy/ -name "*.py" -path "*/f2py/*" -delete 2>/dev/null || true
        find python/numpy/ -name "*.py" -path "*/distutils/*" -delete 2>/dev/null || true
        find python/numpy/ -name "setup.py" -delete 2>/dev/null || true
        find python/numpy/ -name "pyproject.toml" -delete 2>/dev/null || true
        find python/numpy/ -name "setup.cfg" -delete 2>/dev/null || true
        find python/numpy/ -name "MANIFEST.in" -delete 2>/dev/null || true
        find python/numpy/ -name "*.c" -delete 2>/dev/null || true
        find python/numpy/ -name "*.h" -delete 2>/dev/null || true
        find python/numpy/ -name "*.f" -delete 2>/dev/null || true
        find python/numpy/ -name "*.f90" -delete 2>/dev/null || true
        
        # Remove entire problematic directories
        rm -rf python/numpy/tests 2>/dev/null || true
        rm -rf python/numpy/doc 2>/dev/null || true
        rm -rf python/numpy/benchmarks 2>/dev/null || true
        rm -rf python/numpy/f2py 2>/dev/null || true
        rm -rf python/numpy/distutils 2>/dev/null || true
        rm -rf python/numpy/ma/tests 2>/dev/null || true
        rm -rf python/numpy/fft/tests 2>/dev/null || true
        rm -rf python/numpy/linalg/tests 2>/dev/null || true
        rm -rf python/numpy/random/tests 2>/dev/null || true
        
        # Remove NumPy's internal shared libraries that aren't needed in Lambda
        find python/numpy/ -name "*.so" -not -path "*/core/*" -delete 2>/dev/null || true
        # Keep only essential NumPy core files
        find python/numpy/core/ -name "*.so" -not -name "*multiarray*" -not -name "*umath*" -delete 2>/dev/null || true
        
        # Remove any remaining build artifacts
        find python/numpy/ -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
        find python/numpy/ -name "*.pyc" -delete 2>/dev/null || true
        find python/numpy/ -name "*.pyo" -delete 2>/dev/null || true
        
        # CRITICAL: Remove any files that NumPy uses to detect source directory
        find python/numpy/ -name "*.egg-info" -type d -exec rm -rf {} + 2>/dev/null || true
        find python/numpy/ -name "dist" -type d -exec rm -rf {} + 2>/dev/null || true
        find python/numpy/ -name "build" -type d -exec rm -rf {} + 2>/dev/null || true
        find python/numpy/ -name ".git" -type d -exec rm -rf {} + 2>/dev/null || true
        find python/numpy/ -name ".gitignore" -delete 2>/dev/null || true
        find python/numpy/ -name "*.git*" -delete 2>/dev/null || true
        
        # Remove any remaining source detection files
        find python/numpy/ -name "PKG-INFO" -delete 2>/dev/null || true
        find python/numpy/ -name "SOURCES.txt" -delete 2>/dev/null || true
        find python/numpy/ -name "dependency_links.txt" -delete 2>/dev/null || true
        find python/numpy/ -name "top_level.txt" -delete 2>/dev/null || true
        
        # Ensure NumPy has proper __init__.py files
        if [ ! -f "python/numpy/__init__.py" ]; then
            echo "# NumPy package" > python/numpy/__init__.py
        fi
        
        # Create a minimal __init__.py for core if it doesn't exist
        if [ ! -f "python/numpy/core/__init__.py" ]; then
            echo "# NumPy core package" > python/numpy/core/__init__.py
        fi
        
        # Create a minimal __init__.py for all subdirectories to prevent import issues
        find python/numpy/ -type d -exec sh -c 'if [ ! -f "$1/__init__.py" ]; then echo "# Auto-generated" > "$1/__init__.py"; fi' _ {} \;
    fi
    
    # Remove SciPy shared libraries that might cause issues
    if [ -d "python/scipy" ]; then
        find python/scipy/ -name "*.so" -delete 2>/dev/null || true
    fi
    
    # Remove any remaining source directories that might cause conflicts
    find python/ -name "*.egg-info" -type d -exec rm -rf {} + 2>/dev/null || true
    find python/ -name "dist" -type d -exec rm -rf {} + 2>/dev/null || true
    find python/ -name "build" -type d -exec rm -rf {} + 2>/dev/null || true
    
    print_success "NumPy Lambda compatibility fixes applied"
fi

# Clean up unnecessary files to reduce layer size
print_status "Cleaning up unnecessary files..."

# Remove .whl, .tar.gz, .zip files
find python/ -name "*.whl" -delete
find python/ -name "*.tar.gz" -delete
find python/ -name "*.zip" -delete

# Remove example, sample, demo files
find python/ -name "*example*" -type d -exec rm -rf {} + 2>/dev/null || true
find python/ -name "*sample*" -type d -exec rm -rf {} + 2>/dev/null || true
find python/ -name "*demo*" -type d -exec rm -rf {} + 2>/dev/null || true
find python/ -name "*test*" -type d -exec rm -rf {} + 2>/dev/null || true

# Remove license, changelog, history, readme files
find python/ -name "LICENSE*" -delete
find python/ -name "CHANGELOG*" -delete
find python/ -name "HISTORY*" -delete
find python/ -name "README*" -delete
find python/ -name "*.md" -delete
find python/ -name "*.txt" -not -name "entry_points.txt" -delete

# Remove large data files
find python/ -name "*.json" -size +100k -delete
find python/ -name "*.xml" -size +100k -delete
find python/ -name "*.csv" -size +100k -delete

# Linux-specific cleanup
if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    print_status "Performing Linux-specific cleanup..."
    
    # Remove Windows-specific files
    find python/ -name "*.dll" -delete
    find python/ -name "*.exe" -delete
    find python/ -name "*.pyd" -delete
    
    # Clean up numpy.libs (contains duplicate libraries)
    if [ -d "python/numpy.libs" ]; then
        rm -rf python/numpy.libs
    fi
    
    # Clean up sympy (very large, often not needed in production)
    if [ -d "python/sympy" ]; then
        find python/sympy -name "*.py" -size +50k -delete
    fi
    
    # Clean up botocore data
    if [ -d "python/botocore/data" ]; then
        find python/botocore/data -name "*.json" -size +10k -delete
    fi
    
    # Clean up cryptography (remove unnecessary files)
    if [ -d "python/cryptography" ]; then
        find python/cryptography -name "*.c" -delete
        find python/cryptography -name "*.h" -delete
    fi
fi

# Handle OpenTelemetry entry points (only for strands layer)
if [ "${LAYER_NAME}" = "strands" ]; then
    print_status "Setting up OpenTelemetry entry points..."
    
    # Find opentelemetry_api directory
    OPENTELEMETRY_DIR=$(find python/ -name "opentelemetry_api-*" -type d | head -1)
    if [ -n "$OPENTELEMETRY_DIR" ]; then
        # Create entry_points.txt for OpenTelemetry
        cat > "${OPENTELEMETRY_DIR}/entry_points.txt" << 'EOF'
[opentelemetry_context]
contextvars_context = opentelemetry.context.contextvars_context:ContextVarsRuntimeContext
EOF
        print_success "Created OpenTelemetry entry_points.txt"
    else
        print_warning "OpenTelemetry not found in Strands layer - skipping entry points setup"
    fi
fi

# Remove .dist-info directories (but preserve opentelemetry_api for strands layer)
if [ "${LAYER_NAME}" = "strands" ]; then
    find python/ -name "*.dist-info" -not -path "*/opentelemetry_api-*" -exec rm -rf {} + 2>/dev/null || true
else
    find python/ -name "*.dist-info" -exec rm -rf {} + 2>/dev/null || true
fi

# Remove .egg-info directories
find python/ -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true

# Create the layer zip file
print_status "Creating layer zip file: ${LAYER_FILE}"

# Ensure python directory exists and has content
if [ ! -d "python" ] || [ -z "$(ls -A python)" ]; then
    print_error "Python directory is empty or doesn't exist. Cannot create layer."
    exit 1
fi

# Create zip file with verbose output for debugging
if ! zip -r "${LAYER_FILE}" python/ -q; then
    print_error "Failed to create zip file: ${LAYER_FILE}"
    print_error "Checking python directory contents:"
    ls -la python/ | head -10
    exit 1
fi

# Verify zip file was created successfully
if [ ! -f "${LAYER_FILE}" ]; then
    print_error "Zip file was not created: ${LAYER_FILE}"
    exit 1
fi

# Get the size of the layer
LAYER_SIZE=$(du -h "${LAYER_FILE}" | cut -f1)
LAYER_SIZE_MB=$(du -m "${LAYER_FILE}" | cut -f1)
UNCOMPRESSED_SIZE_MB=$(du -sm python/ | cut -f1)
print_success "Layer created successfully: ${LAYER_FILE} (${LAYER_SIZE})"
print_status "Compressed size: ${LAYER_SIZE_MB}MB, Uncompressed size: ${UNCOMPRESSED_SIZE_MB}MB"

# Verify the layer structure
print_status "Verifying layer structure..."
echo "First 10 entries in layer:"
unzip -l "${LAYER_FILE}" | head -12 | tail -10

# Verify critical files exist
print_status "Checking for critical files..."
if unzip -l "${LAYER_FILE}" | grep -q "python/"; then
    print_success "✓ Python packages found"
else
    print_error "✗ No Python packages found"
    exit 1
fi

# Check if layer is within size limits (AWS checks uncompressed size)
UNCOMPRESSED_SIZE_BYTES=$((UNCOMPRESSED_SIZE_MB * 1024 * 1024))
MAX_SIZE_BYTES=262144000  # 250MB limit for Lambda layers (uncompressed)

if [ $UNCOMPRESSED_SIZE_BYTES -gt $MAX_SIZE_BYTES ]; then
    print_error "Layer uncompressed size (${UNCOMPRESSED_SIZE_MB}MB / ${UNCOMPRESSED_SIZE_BYTES} bytes) exceeds AWS Lambda layer limit of 250MB"
    print_error "Current uncompressed size: ${UNCOMPRESSED_SIZE_MB}MB"
    print_error "Maximum allowed: 250MB"
    echo ""
    print_error "To reduce layer size, consider:"
    print_error "1. Removing unnecessary dependencies from requirements.txt"
    print_error "2. Splitting into multiple layers"
    print_error "3. Using lighter alternatives for heavy packages"
    echo ""
    print_error "Largest directories in the layer:"
    du -h python/ | sort -hr | head -10
    exit 1
elif [ $UNCOMPRESSED_SIZE_MB -gt 200 ]; then
    print_warning "Layer uncompressed size (${UNCOMPRESSED_SIZE_MB}MB) is getting close to the 250MB limit"
    print_warning "Consider optimizing dependencies to reduce size"
fi

# Create a placeholder file for Terraform validation (if needed)
# This ensures Terraform can validate even if the zip file doesn't exist yet
PLACEHOLDER_FILE="${LAYER_FILE}.placeholder"
if [ ! -f "${LAYER_FILE}" ] && [ ! -f "${PLACEHOLDER_FILE}" ]; then
    print_status "Creating placeholder file for Terraform validation..."
    touch "${PLACEHOLDER_FILE}"
fi

print_success "Lambda layer '${LAYER_NAME}' build completed successfully!"
print_success "Layer file: ${LAYER_FILE} (${LAYER_SIZE})"
print_success "Ready for Terraform deployment"