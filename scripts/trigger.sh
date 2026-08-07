#!/usr/bin/env bash
set -euo pipefail

PROFILE="${AWS_PROFILE:-dev}"
STACK_NAME="serverless-etl-s3-to-iceberg"
FILE="${1:-test-data.parquet}"

DATA_BUCKET=$(aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" \
  --query "Stacks[0].Outputs[?OutputKey=='DataBucketName'].OutputValue" \
  --output text \
  --profile "$PROFILE")

echo "Uploading ${FILE} to s3://${DATA_BUCKET}/feed/..."
aws s3 cp "$FILE" "s3://${DATA_BUCKET}/feed/$(basename "$FILE")" --profile "$PROFILE"
echo "Pipeline triggered. Monitor execution in the Step Functions console."
