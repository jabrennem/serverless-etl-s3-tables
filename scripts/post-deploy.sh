#!/usr/bin/env bash
set -euo pipefail

PROFILE="${AWS_PROFILE:-dev}"
STACK_NAME="serverless-etl-s3-to-iceberg"
TABLE_BUCKET_NAME="serverless-etl-table-bucket"

echo "Fetching stack outputs..."
DATA_BUCKET=$(aws cloudformation describe-stacks \
  --stack-name "$STACK_NAME" \
  --query "Stacks[0].Outputs[?OutputKey=='DataBucketName'].OutputValue" \
  --output text \
  --profile "$PROFILE")

echo "Uploading PySpark script..."
aws s3 cp emr/load_data.py "s3://${DATA_BUCKET}/emr/load_data.py" --profile "$PROFILE"

echo "Applying Lake Formation grants..."
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text --profile "$PROFILE")
EMR_ROLE=$(aws cloudformation describe-stack-resource \
  --stack-name "$STACK_NAME" \
  --logical-resource-id EmrAppRole \
  --query 'StackResourceDetail.PhysicalResourceId' \
  --output text \
  --profile "$PROFILE")
EMR_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${EMR_ROLE}"
CATALOG_ID="${ACCOUNT_ID}:s3tablescatalog/${TABLE_BUCKET_NAME}"

# Catalog-level
aws lakeformation grant-permissions \
  --principal "DataLakePrincipalIdentifier=${EMR_ROLE_ARN}" \
  --resource "{\"Catalog\":{\"Id\":\"${CATALOG_ID}\"}}" \
  --permissions '["CREATE_DATABASE", "DESCRIBE"]' \
  --profile "$PROFILE" 2>/dev/null || true

# Database-level (default namespace)
aws lakeformation grant-permissions \
  --principal "DataLakePrincipalIdentifier=${EMR_ROLE_ARN}" \
  --resource "{\"Database\":{\"CatalogId\":\"${CATALOG_ID}\",\"Name\":\"default\"}}" \
  --permissions '["DESCRIBE", "ALTER", "CREATE_TABLE"]' \
  --profile "$PROFILE" 2>/dev/null || true

# Table-level (all tables in default namespace)
aws lakeformation grant-permissions \
  --principal "DataLakePrincipalIdentifier=${EMR_ROLE_ARN}" \
  --resource "{\"Table\":{\"CatalogId\":\"${CATALOG_ID}\",\"DatabaseName\":\"default\",\"TableWildcard\":{}}}" \
  --permissions '["SELECT", "INSERT", "DESCRIBE", "ALTER", "DROP"]' \
  --profile "$PROFILE" 2>/dev/null || true

echo "Done. Data bucket: ${DATA_BUCKET}"
