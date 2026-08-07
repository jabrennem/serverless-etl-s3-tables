# aws-sam-serverless-etl

AWS SAM Application of a serverless ETL data ingestion pipeline

# Requirements 

* Python 3.13

## Data Layer

- S3 Bucket
    - /feed - files 
    - /iceberg - iceberg tables 

## Compute Layer

- Lambda
    - Validates the input
- EMR Serverless App
    - Runs python script

## Orchestration Layer

- State Machine
    - Validates input with the lambda
    - Runs EMR Serverless job
- Event bridge rule S3 Object Created
    - Triggers the state machine to run when a parquet file is uploaded with a feed/ prefix.

# Build and Deploy

Deploy the SAM App

```bash
# build and deploy
PROFILE='dev'
STACK_NAME='serverless-etl-s3-to-iceberg'
sam build
sam deploy

# copy emr script
aws cloudformation describe-stacks \
  --stack-name $STACK_NAME \
  --query "Stacks[0].Outputs" \
  --output json > stack-outputs.json \
  --profile $PROFILE
DATA_BUCKET=$(jq -r '.[] | select(.OutputKey=="DataBucketName") | .OutputValue' stack-outputs.json)
aws s3 cp --profile $PROFILE emr/load_data.py "s3://${DATA_BUCKET}/emr/load_data.py"

# upload a test parquet to trigger state machine.
aws s3 cp --profile $PROFILE test-data.parquet s3://${DATA_BUCKET}/feed/test-data.parquet
```