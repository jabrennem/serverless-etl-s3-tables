#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "Usage: $0 [--profile <aws-profile>]"
  exit 1
}

PROFILE="${AWS_PROFILE:-default}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile)
      PROFILE="$2"
      shift 2
      ;;
    *)
      usage
      ;;
  esac
done

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

# Iceberg tables to grant on (must match the TableName values in template.yaml).
TABLES=(customers test_data)

# Catalog-level
aws lakeformation grant-permissions \
  --principal "DataLakePrincipalIdentifier=${EMR_ROLE_ARN}" \
  --resource "{\"Catalog\":{\"Id\":\"${CATALOG_ID}\"}}" \
  --permissions '["CREATE_DATABASE", "DESCRIBE"]' \
  --profile "$PROFILE"

# Database-level (default namespace)
aws lakeformation grant-permissions \
  --principal "DataLakePrincipalIdentifier=${EMR_ROLE_ARN}" \
  --resource "{\"Database\":{\"CatalogId\":\"${CATALOG_ID}\",\"Name\":\"default\"}}" \
  --permissions '["DESCRIBE", "ALTER", "CREATE_TABLE"]' \
  --profile "$PROFILE"

# Table-level, granted by explicit name. ALL (SUPER) is required for EMR to
# WRITE S3 Tables data — INSERT alone is insufficient for the Iceberg commit's
# read-modify-write against the underlying S3 storage, and it surfaces as a raw
# S3 403 on the write/abort path. A TableWildcard grant does not reliably
# register against the S3 Tables federated catalog, so grant per table.
for T in "${TABLES[@]}"; do
  echo "  granting ALL on table ${T}"
  aws lakeformation grant-permissions \
    --principal "DataLakePrincipalIdentifier=${EMR_ROLE_ARN}" \
    --resource "{\"Table\":{\"CatalogId\":\"${CATALOG_ID}\",\"DatabaseName\":\"default\",\"Name\":\"${T}\"}}" \
    --permissions '["ALL"]' \
    --profile "$PROFILE"
done

echo "Done. Data bucket: ${DATA_BUCKET}"
