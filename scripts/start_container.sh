#!/bin/bash
#
# Copyright 2023-2024 Amazon.com, Inc. or its affiliates.
#

# Specify the profile you want to use (replace 'default' with your profile name if needed)
AWS_PROFILE="default"

# Extract AWS credentials from the credentials file
AWS_ACCESS_KEY_ID=$(awk -v profile="[$AWS_PROFILE]" -F ' *= *' '$1 == profile {flag=1} flag && $1 == "aws_access_key_id" {print $2; exit}' ~/.aws/credentials)
AWS_SECRET_ACCESS_KEY=$(awk -v profile="[$AWS_PROFILE]" -F ' *= *' '$1 == profile {flag=1} flag && $1 == "aws_secret_access_key" {print $2; exit}' ~/.aws/credentials)
AWS_SESSION_TOKEN=$(awk -v profile="[$AWS_PROFILE]" -F ' *= *' '$1 == profile {flag=1} flag && $1 == "aws_session_token" {print $2; exit}' ~/.aws/credentials)
AWS_DEFAULT_REGION=$(awk -v profile="[$AWS_PROFILE]" -F ' *= *' '$1 == profile {flag=1} flag && $1 == "region" {print $2; exit}' ~/.aws/config)

if [ -z "$AWS_ACCESS_KEY_ID" ] || [ -z "$AWS_SECRET_ACCESS_KEY" ]; then
  echo "Failed to extract AWS credentials."
  exit 1
fi

# Export the variables to be used in the Docker Compose environment
export AWS_ACCESS_KEY_ID
export AWS_SECRET_ACCESS_KEY
export AWS_SESSION_TOKEN
export AWS_DEFAULT_REGION

echo "${AWS_DEFAULT_REGION}"

# If the AWS_SESSION_TOKEN is not found, it will be set as an empty string
# You can handle this based on your requirement
if [ -z "$AWS_SESSION_TOKEN" ]; then
  unset AWS_SESSION_TOKEN
fi

# Print the variables to verify (optional)
echo "AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID"
echo "AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY"
echo "AWS_SESSION_TOKEN=$AWS_SESSION_TOKEN"
echo "AWS_DEFAULT_REGION=$AWS_DEFAULT_REGION"

## You can now run your docker-compose command
docker-compose -f docker-compose.yml up -d --build
