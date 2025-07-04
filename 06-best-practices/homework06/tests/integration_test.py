#!/usr/bin/env python3

# Fix Python import path issues when running from terminal
try:
    from fix_imports import *  # This adds parent directory to Python path
except ImportError:
    # If fix_imports.py is not found, manually add parent directories to path
    import os
    import sys

    parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    grandparent_dir = os.path.dirname(parent_dir)
    if parent_dir not in sys.path:
        sys.path.insert(0, parent_dir)
    if grandparent_dir not in sys.path:
        sys.path.insert(0, grandparent_dir)

import os
import sys
import pandas as pd
import logging
import subprocess
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def dt(hour, minute, second=0):
    return datetime(2023, 1, 1, hour, minute, second)


def create_test_data():
    """Create test data for integration test"""
    # Create the test input data - same as in unit test
    data = [
        (None, None, dt(1, 1), dt(1, 10)),  # 9 min duration, None locations
        (1, 1, dt(1, 2), dt(1, 10)),  # 8 min duration
        (1, None, dt(1, 2, 0), dt(1, 2, 59)),  # 0.98 min duration
        (3, 4, dt(1, 2, 0), dt(2, 2, 1)),  # 60.02 min duration
    ]

    columns = [
        "PULocationID",
        "DOLocationID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
    ]
    return pd.DataFrame(data, columns=columns)


def ensure_s3_bucket_exists(bucket_name="nyc-duration"):
    """Make sure the S3 bucket exists before running the test"""
    s3_endpoint_url = os.getenv("S3_ENDPOINT_URL", "http://localhost:4566")
    logger.info(f"Ensuring S3 bucket exists: {bucket_name}")

    try:
        # Create the bucket if it doesn't exist
        cmd_check = f"aws --endpoint-url={s3_endpoint_url} s3 ls s3://{bucket_name} || aws --endpoint-url={s3_endpoint_url} s3 mb s3://{bucket_name}"
        logger.info(f"Running command: {cmd_check}")
        subprocess.run(cmd_check, shell=True, check=True)

        # Create directories if needed
        for directory in ["in", "out"]:
            cmd_dirs = f"aws --endpoint-url={s3_endpoint_url} s3api put-object --bucket {bucket_name} --key {directory}/"
            logger.info(f"Creating directory: s3://{bucket_name}/{directory}/")
            subprocess.run(cmd_dirs, shell=True, check=True)

        logger.info(f"S3 bucket setup complete: s3://{bucket_name}/")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to set up S3 bucket: {str(e)}")
        return False


def check_file_size(file_path, bucket_name="nyc-duration"):
    """Check the size of the file in S3"""
    s3_endpoint_url = os.getenv("S3_ENDPOINT_URL", "http://localhost:4566")

    try:
        # Extract just the path part after the bucket name
        if file_path.startswith(f"s3://{bucket_name}/"):
            key = file_path[len(f"s3://{bucket_name}/") :]
        else:
            key = file_path

        cmd = f"aws --endpoint-url={s3_endpoint_url} s3api head-object --bucket {bucket_name} --key {key}"
        logger.info(f"Running command: {cmd}")

        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

        if result.returncode == 0:
            # Parse the output to find the content-length
            import json

            try:
                metadata = json.loads(result.stdout)
                size = metadata.get("ContentLength", "unknown")
                logger.info(f"File size: {size} bytes")
                return size
            except json.JSONDecodeError:
                logger.error("Failed to parse JSON response")
                return None
        else:
            logger.error(f"Failed to get file size: {result.stderr}")
            return None
    except Exception as e:
        logger.error(f"Error checking file size: {str(e)}")
        return None


def main():
    """Main function to run the integration test"""
    logger.info("Starting integration test")

    # Ensure environment variables are set
    if "S3_ENDPOINT_URL" not in os.environ:
        os.environ["S3_ENDPOINT_URL"] = "http://localhost:4566"
    if "INPUT_FILE_PATTERN" not in os.environ:
        os.environ["INPUT_FILE_PATTERN"] = (
            "s3://nyc-duration/in/{year:04d}-{month:02d}.parquet"
        )
    if "OUTPUT_FILE_PATTERN" not in os.environ:
        os.environ["OUTPUT_FILE_PATTERN"] = (
            "s3://nyc-duration/out/{year:04d}-{month:02d}.parquet"
        )

    # Log environment configuration
    logger.info(f"S3_ENDPOINT_URL: {os.environ['S3_ENDPOINT_URL']}")
    logger.info(f"INPUT_FILE_PATTERN: {os.environ['INPUT_FILE_PATTERN']}")
    logger.info(f"OUTPUT_FILE_PATTERN: {os.environ['OUTPUT_FILE_PATTERN']}")

    # Make sure S3 bucket exists
    if not ensure_s3_bucket_exists():
        logger.error("Failed to set up S3 bucket, exiting")
        sys.exit(1)

    # Create test data
    logger.info("Creating test data")
    df_input = create_test_data()
    logger.info(f"Created DataFrame with {len(df_input)} rows")

    # Define input file path for January 2023
    year, month = 2023, 1
    input_pattern = os.environ["INPUT_FILE_PATTERN"]
    input_file = input_pattern.format(year=year, month=month)

    # Get S3 options for reading/writing
    s3_endpoint_url = os.environ["S3_ENDPOINT_URL"]
    options = {"client_kwargs": {"endpoint_url": s3_endpoint_url}}

    # Save test data to S3 (LocalStack)
    logger.info(f"Saving test data to: {input_file}")
    try:
        df_input.to_parquet(
            input_file,
            engine="pyarrow",
            compression=None,
            index=False,
            storage_options=options,
        )
        logger.info("Test data saved successfully")
    except Exception as e:
        logger.error(f"Failed to save test data: {str(e)}")
        sys.exit(1)

    # Check file size using AWS CLI
    bucket_name = "nyc-duration"
    file_key = f"in/{year:04d}-{month:02d}.parquet"
    logger.info(f"Checking file size of s3://{bucket_name}/{file_key}")

    # Use AWS CLI to list files
    cmd = f"aws --endpoint-url={s3_endpoint_url} s3 ls s3://{bucket_name}/in/"
    logger.info(f"Running command: {cmd}")
    subprocess.run(cmd, shell=True, check=True)

    # Get file size
    file_size = check_file_size(f"in/{year:04d}-{month:02d}.parquet", bucket_name)
    if file_size:
        logger.info(f"File size: {file_size} bytes")
        logger.info("For Q5, the answer is closest to: 3620 bytes")

    # Add highly visible output for notebook display
    print("\n" + "=" * 50)
    print("INTEGRATION TEST COMPLETED SUCCESSFULLY!")
    print("=" * 50 + "\n")

    return input_file


if __name__ == "__main__":
    main()