#!/usr/bin/env bash
set -euo pipefail

PROFILE="${AWS_PROFILE:-dev}"
STACK_NAME="serverless-etl-s3-to-iceberg"
TARGET="${1:-test-data.parquet}"

DATA_BUCKET=$(aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" \
  --query "Stacks[0].Outputs[?OutputKey=='DataBucketName'].OutputValue" \
  --output text \
  --profile "$PROFILE")

upload_one() {
  local file="$1"
  local ts base stem ext key
  ts=$(date -u +%Y%m%dT%H%M%SZ)
  base="$(basename "$file")"
  stem="${base%.*}"
  ext="${base##*.}"
  # Unique suffix (timestamp + short random) so parallel uploads never collide.
  key="feed/${stem}-${ts}-${RANDOM}.${ext}"
  echo "  ${file} -> s3://${DATA_BUCKET}/${key}"
  aws s3 cp "$file" "s3://${DATA_BUCKET}/${key}" --profile "$PROFILE"
}

if [[ -d "$TARGET" ]]; then
  echo "Uploading all *.parquet in ${TARGET}/ ..."
  shopt -s nullglob
  files=("$TARGET"/*.parquet)
  if [[ ${#files[@]} -eq 0 ]]; then
    echo "No .parquet files found in ${TARGET}/" >&2
    exit 1
  fi
  for f in "${files[@]}"; do
    upload_one "$f"
  done
  echo "Uploaded ${#files[@]} file(s). Pipeline triggered."
else
  upload_one "$TARGET"
  echo "Pipeline triggered. Monitor execution in the Step Functions console."
fi
