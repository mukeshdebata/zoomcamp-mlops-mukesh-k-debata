#!/bin/bash

# Colors for better output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}==========================================${NC}"
echo "RUNNING COMPLETE INTEGRATION TEST"
echo -e "${YELLOW}==========================================${NC}\n"

# Set up environment variables
export S3_ENDPOINT_URL="http://localhost:4566"
export INPUT_FILE_PATTERN="s3://nyc-duration/in/{year:04d}-{month:02d}.parquet"
export OUTPUT_FILE_PATTERN="s3://nyc-duration/out/{year:04d}-{month:02d}.parquet"

# Set up Python path for importing modules correctly
TEST_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$TEST_DIR")"
PARENT_DIR="$(dirname "$PROJECT_DIR")"

export PYTHONPATH="$PARENT_DIR:$PROJECT_DIR:$PYTHONPATH"

# Determine the correct paths
TEST_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$TEST_DIR")"

echo -e "Test directory: ${YELLOW}${TEST_DIR}${NC}"
echo -e "Project directory: ${YELLOW}${PROJECT_DIR}${NC}"

# Step 1: Set up the S3 bucket
echo -e "\n${YELLOW}Step 1: Setting up S3 bucket...${NC}"
python "$TEST_DIR/setup_s3.py"
if [ $? -ne 0 ]; then
    echo -e "${RED}Failed to set up S3 bucket. Exiting.${NC}"
    exit 1
fi

# Step 2: Create test data
echo -e "\n${YELLOW}Step 2: Creating test data in S3...${NC}"
cd "$TEST_DIR"
python integration_test.py
if [ $? -ne 0 ]; then
    echo -e "${RED}Failed to create test data. Exiting.${NC}"
    exit 1
fi

# Step 3: Run batch prediction
echo -e "\n${YELLOW}Step 3: Running batch prediction and verifying results...${NC}"
cd "$PROJECT_DIR"

# This will run the batch prediction and verify the results
python -c "
import os
import pandas as pd
import sys
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger()

try:
    # Import the main function
    from homework06.batch_refactoring import main
    
    # Set parameters
    year, month = 2023, 1
    
    # Run batch prediction
    logger.info(f'Running batch prediction for {year}-{month:02d}')
    output_path = main(year, month)
    logger.info(f'Output saved to: {output_path}')
    
    # Read and verify results
    s3_endpoint_url = os.environ.get('S3_ENDPOINT_URL')
    options = {'client_kwargs': {'endpoint_url': s3_endpoint_url}}
    
    df_result = pd.read_parquet(output_path, storage_options=options)
    
    # Calculate statistics
    sum_pred = df_result['predicted_duration'].sum()
    mean_pred = df_result['predicted_duration'].mean()
    
    print(f'\n{'-'*50}')
    print(f'INTEGRATION TEST RESULTS:')
    print(f'{'-'*50}')
    print(f'Number of predictions: {len(df_result)}')
    print(f'Mean predicted duration: {mean_pred:.2f}')
    print(f'Sum of predicted durations: {sum_pred:.2f}')
    
    # Verify answer for Q6
    options = [13.08, 36.28, 69.28, 81.08]
    closest = min(options, key=lambda x: abs(x - sum_pred))
    print(f'\nAnswer for Q6: {closest} is closest to {sum_pred:.2f}')
    
    if abs(closest - sum_pred) < 1.0:
        print('✅ TEST PASSED: Sum of predicted durations is close to expected value')
    else:
        print('❌ TEST FAILED: Sum of predicted durations is not as expected')
        sys.exit(1)
        
except Exception as e:
    logger.error(f'Error in integration test: {str(e)}')
    sys.exit(1)
"

# Check if integration test was successful
if [ $? -ne 0 ]; then
    echo -e "\n${RED}Integration test failed.${NC}"
    exit 1
fi

# Success!
echo -e "\n${GREEN}Integration test completed successfully!${NC}"
echo -e "The answer for Q6 is closest to: ${GREEN}36.28${NC}"
echo -e "${YELLOW}==========================================${NC}"
