#!/usr/bin/env bash
# Delete all object versions and delete markers so a versioned bucket can be emptied/destroyed.
set -euo pipefail

BUCKET="${1:?bucket name required}"

if ! aws s3api head-bucket --bucket "$BUCKET" 2>/dev/null; then
  echo "Bucket $BUCKET does not exist or is inaccessible; skipping"
  exit 0
fi

echo "Emptying all versions from s3://$BUCKET"
while true; do
  json=$(aws s3api list-object-versions --bucket "$BUCKET" --max-keys 1000 --output json)
  objects=$(echo "$json" | jq -c '{Objects: (((.Versions // []) + (.DeleteMarkers // [])) | map({Key:.Key, VersionId:.VersionId}) | .[0:1000]), Quiet: true}')
  count=$(echo "$objects" | jq '.Objects | length')
  if [ "$count" -eq 0 ]; then
    echo "Bucket empty: $BUCKET"
    break
  fi
  echo "Deleting $count object versions from $BUCKET"
  aws s3api delete-objects --bucket "$BUCKET" --delete "$objects" >/dev/null
done
