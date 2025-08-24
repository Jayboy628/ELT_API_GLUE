#!/bin/bash
set -euo pipefail

# -------------------------
# Configuration
# -------------------------
# Vars you already compute
ENV="dev"
PREFIX="lead"
REGION="us-east-1"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
S3_BUCKET="${PREFIX}-elt-${ACCOUNT_ID}-${ENV}"
API_SECRET_NAME="api_key_closeio-${ENV}"   # adjust if yours differs

# Resources
S3_BUCKET="${PREFIX}-elt-${ACCOUNT_ID}-${ENV}"
GLUE_JOB_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${PREFIX}-glue-job-role-${ENV}"

# Close secret naming (matches 02_secrets.yml defaults)
SECRET_BASENAME="api_key_closeio"
SECRET_NAME="${SECRET_BASENAME}-${ENV}"

# Local paths
CLOUDFORMATION_DIR="cloudformation"
SCRIPT_DIR="scripts"

# CFN stack names
IAM_STACK="iam-stack"
SECRETS_STACK="secrets-stack"
GLUE_STACK="glue-stack"

# Required env: Close API key
: "${CLOSE_API_KEY:?Set CLOSE_API_KEY environment variable before running}"

echo "Region:         ${REGION}"
echo "Account:        ${ACCOUNT_ID}"
echo "Env:            ${ENV}"
echo "Bucket:         ${S3_BUCKET}"
echo "Glue Role ARN:  ${GLUE_JOB_ROLE_ARN}"
echo "Secret Name:    ${SECRET_NAME}"
echo

# -------------------------
# Ensure S3 bucket exists
# -------------------------
# echo "Checking S3 bucket s3://${S3_BUCKET}..."
# if aws s3api head-bucket --bucket "${S3_BUCKET}" 2>/dev/null; then
#   echo "Bucket exists."
# else
#   echo "Creating bucket..."
#   # us-east-1 does not need LocationConstraint
#   if [[ "${REGION}" == "us-east-1" ]]; then
#     aws s3api create-bucket --bucket "${S3_BUCKET}"
#   else
#     aws s3api create-bucket --bucket "${S3_BUCKET}" \
#       --create-bucket-configuration LocationConstraint="${REGION}"
#   fi
# fi
# echo

# -------------------------
# Deploy IAM stack
# -------------------------
echo "Deploying ${IAM_STACK}..."
echo "Deploying iam-stack..."
aws cloudformation deploy \
  --stack-name "${IAM_STACK}" \
  --template-file "${CLOUDFORMATION_DIR}/01_iam.yml" \
  --capabilities CAPABILITY_NAMED_IAM \
  --no-fail-on-empty-changeset \
  --parameter-overrides \
    Environment="${ENV}" \
    Prefix="${PREFIX}" \
    ExistingBucketName="${S3_BUCKET}" \
    ApiSecretName="${API_SECRET_NAME}" \
  --region "${REGION}"

# -------------------------
# Deploy Secrets stack (inject API key)
# -------------------------
echo "Deploying ${SECRETS_STACK}..."
aws cloudformation deploy \
  --stack-name "${SECRETS_STACK}" \
  --template-file "${CLOUDFORMATION_DIR}/02_secrets.yml" \
  --capabilities CAPABILITY_NAMED_IAM \
  --no-fail-on-empty-changeset \
  --parameter-overrides \
      Environment="${ENV}" \
      Prefix="${PREFIX}" \
      SecretName="${SECRET_BASENAME}" \
      ApiKeyValue="${CLOSE_API_KEY}" \
  --region "${REGION}"
echo

# -------------------------
# Upload Glue scripts
# -------------------------
echo "Uploading Glue scripts to s3://${S3_BUCKET}/scripts/ ..."
aws s3 cp "${SCRIPT_DIR}/" "s3://${S3_BUCKET}/scripts/" --recursive
echo

# -------------------------
# Deploy Glue Jobs stack
# -------------------------
# 03_gluejobs.yml (as provided) accepts both a secret ARN import OR a secret NAME.
# We'll pass the NAME explicitly to avoid cross-stack import coupling.
echo "Deploying ${GLUE_STACK}..."
aws cloudformation deploy \
  --stack-name "${GLUE_STACK}" \
  --template-file "${CLOUDFORMATION_DIR}/03_gluejobs.yml" \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides \
      GlueJobRoleArn="${GLUE_JOB_ROLE_ARN}" \
      S3Bucket="${S3_BUCKET}" \
      Environment="${ENV}" \
      Prefix="${PREFIX}" \
      ApiKeySecretName="${SECRET_NAME}" \
      # Optional: override output prefix (else CFN default is fine)
      S3OutputPrefix="s3://${S3_BUCKET}/landing/closeio/" \
      # Optional: override API base
      ApiBaseUrl="https://api.close.com/api/v1" \
      # Optional: job defaults (comment out to use CFN defaults)
      OutputFormat="parquet" \
      PageSize="200" \
      MaxPages="500" \
      FetchLeadDetails="false" \
      LeadLimit="0" \
  --region "${REGION}"
echo

echo "All stacks deployed successfully!"
