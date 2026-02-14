#!/usr/bin/env bash
# Build a single zip of Python libraries for all Glue jobs and optionally upload to S3.
#
# Uses backend_app/src/glue/requirements.txt (shared). One zip, one S3 key: glue-libs.zip.
# Any Glue job that needs extra libs uses --extra-py-files s3://bucket/glue-libs.zip.
#
# Usage:
#   ./build_glue_libs.sh
#   ./build_glue_libs.sh s3://your-glue-scripts-bucket/glue-libs.zip
#
# Add or remove packages in backend_app/src/glue/requirements.txt and re-run.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
REQUIREMENTS="${REPO_ROOT}/backend_app/src/glue/requirements.txt"
BUILD_DIR="${REPO_ROOT}/backend_app/src/glue/.glue_libs_build"
OUTPUT_ZIP="${REPO_ROOT}/backend_app/src/glue/glue-libs.zip"

if [[ ! -f "$REQUIREMENTS" ]]; then
  echo "Missing requirements.txt at $REQUIREMENTS"
  exit 1
fi

echo "Building Glue libs from $REQUIREMENTS ..."
rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_DIR"

pip install --target "$BUILD_DIR" --no-compile -r "$REQUIREMENTS"

rm -f "$OUTPUT_ZIP"
(cd "$BUILD_DIR" && zip -rq "$OUTPUT_ZIP" .)
rm -rf "$BUILD_DIR"

echo "Created $OUTPUT_ZIP ($(du -h "$OUTPUT_ZIP" | cut -f1))"

if [[ -n "${1:-}" ]]; then
  S3_PATH="$1"
  echo "Uploading to $S3_PATH ..."
  aws s3 cp "$OUTPUT_ZIP" "$S3_PATH"
  echo "Done. Set Glue job --extra-py-files to: $S3_PATH"
fi
