#!/bin/bash

# Colors for better output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Print a separator line
separator() {
    echo -e "${YELLOW}=======================================================${NC}"
}

echo "Starting test suite for Homework 6"
separator

# Check if LocalStack is running
echo -e "${YELLOW}Checking if LocalStack is running...${NC}"
if curl -s http://localhost:4566 > /dev/null; then
    echo -e "${GREEN}LocalStack is running!${NC}"
else
    echo -e "${RED}LocalStack is not running!${NC}"
    echo "Starting LocalStack with docker-compose..."
    
    # Check if docker-compose file exists in the parent directory
    if [ -f "../docker-compose.yaml" ]; then
        cd .. && docker-compose up -d
    else
        echo -e "${RED}Error: docker-compose.yaml not found in parent directory.${NC}"
        echo "Please start LocalStack manually before running tests."
        exit 1
    fi
    
    # Wait for LocalStack to start
    echo "Waiting for LocalStack to start..."
    sleep 5
    
    if curl -s http://localhost:4566 > /dev/null; then
        echo -e "${GREEN}LocalStack is now running!${NC}"
    else
        echo -e "${RED}Error: Could not start LocalStack.${NC}"
        echo "Please start LocalStack manually before running tests."
        exit 1
    fi
fi

# Setup environment variables for testing
export S3_ENDPOINT_URL="http://localhost:4566"
export INPUT_FILE_PATTERN="s3://nyc-duration/in/{year:04d}-{month:02d}.parquet"
export OUTPUT_FILE_PATTERN="s3://nyc-duration/out/{year:04d}-{month:02d}.parquet"

# Set up Python path for importing modules correctly
TEST_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$TEST_DIR")"
PARENT_DIR="$(dirname "$PROJECT_DIR")"

export PYTHONPATH="$PARENT_DIR:$PROJECT_DIR:$PYTHONPATH"

echo -e "${YELLOW}Environment variables set:${NC}"
echo "S3_ENDPOINT_URL: $S3_ENDPOINT_URL"
echo "INPUT_FILE_PATTERN: $INPUT_FILE_PATTERN"
echo "OUTPUT_FILE_PATTERN: $OUTPUT_FILE_PATTERN"

# Setup S3 bucket
separator
echo -e "${YELLOW}Setting up S3 bucket...${NC}"
python setup_s3.py
if [ $? -ne 0 ]; then
    echo -e "${RED}Failed to set up S3 bucket.${NC}"
    exit 1
fi

# Determine correct paths
TEST_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$TEST_DIR")"
echo -e "${YELLOW}Test directory: ${TEST_DIR}${NC}"
echo -e "${YELLOW}Project directory: ${PROJECT_DIR}${NC}"

# Run unit tests
separator
echo -e "${YELLOW}Running unit tests...${NC}"
cd "$PROJECT_DIR"
python -m pytest -xvs "$TEST_DIR/test_batch_refactoring.py"
if [ $? -ne 0 ]; then
    echo -e "${RED}Unit tests failed.${NC}"
    exit 1
fi

# Run integration test to create test data
separator
echo -e "${YELLOW}Running integration test to create test data...${NC}"
cd "$TEST_DIR"
python integration_test.py
if [ $? -ne 0 ]; then
    echo -e "${RED}Failed to create test data.${NC}"
    exit 1
fi

# Check file size with AWS CLI for Q5
separator
echo -e "${YELLOW}Checking file size for Q5...${NC}"
aws --endpoint-url=$S3_ENDPOINT_URL s3 ls s3://nyc-duration/in/
FILE_SIZE=$(aws --endpoint-url=$S3_ENDPOINT_URL s3api head-object --bucket nyc-duration --key in/2023-01.parquet --query 'ContentLength')
echo -e "File size: ${GREEN}$FILE_SIZE${NC} bytes (Answer for Q5 is closest to 3620)"

# Run batch predictions on test data
separator
echo -e "${YELLOW}Running batch predictions on test data...${NC}"
cd "$PROJECT_DIR"
python -c "from homework06.batch_refactoring import main; main(2023, 1)"
if [ $? -ne 0 ]; then
    echo -e "${RED}Failed to run batch predictions.${NC}"
    exit 1
fi

# Check predictions for Q6
separator
echo -e "${YELLOW}Checking predictions for Q6...${NC}"
python -c "
import pandas as pd
import os

options = {'client_kwargs': {'endpoint_url': os.environ['S3_ENDPOINT_URL']}}
output_file = os.environ['OUTPUT_FILE_PATTERN'].format(year=2023, month=1)
df = pd.read_parquet(output_file, storage_options=options)
sum_pred = df['predicted_duration'].sum()
print(f'Sum of predicted durations: {sum_pred:.2f}')
print(f'Answer for Q6 is closest to: 36.28')
"

# All tests passed
separator
echo -e "${GREEN}All tests completed successfully!${NC}"
echo "The answers to the homework questions are:"
echo -e "Q3: ${GREEN}2${NC} rows in the expected dataframe"
echo -e "Q4: Option ${GREEN}--endpoint-url${NC} is needed for AWS CLI with LocalStack"
echo -e "Q5: File size is closest to ${GREEN}3620${NC} bytes"
echo -e "Q6: Sum of predicted durations is closest to ${GREEN}36.28${NC}"
separator
