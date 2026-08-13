#!/bin/bash
REGION="ap-south-1"
BUCKET="ssa-kessler-bipul"
ACCOUNT_ID="315466292382"
ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/ssa-lambda-role"

deploy_lambda() {
  NAME=$1
  ZIP=$2

  echo "Deploying $NAME..."

  # Check if exists — update or create
  if aws lambda get-function --function-name $NAME --region $REGION 2>/dev/null; then
    aws lambda update-function-code \
      --function-name $NAME \
      --zip-file fileb://$ZIP \
      --region $REGION
  else
    aws lambda create-function \
      --function-name $NAME \
      --runtime python3.11 \
      --role $ROLE_ARN \
      --handler lambda_function.lambda_handler \
      --zip-file fileb://$ZIP \
      --timeout 60 \
      --memory-size 256 \
      --environment "Variables={S3_BUCKET=$BUCKET}" \
      --region $REGION
  fi

  echo "$NAME deployed."
}

deploy_lambda "ssa-extract-celestrak" "lambda_celestrak/function.zip"
deploy_lambda "ssa-extract-nasa-neows" "lambda_nasa/function.zip"
deploy_lambda "ssa-extract-noaa-goes"  "lambda_noaa/function.zip"

echo "All 3 Lambdas deployed."
