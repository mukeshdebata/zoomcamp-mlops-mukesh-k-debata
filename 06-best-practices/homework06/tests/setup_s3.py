#!/usr/bin/env python3

import os
import sys
import subprocess
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def check_localstack_running():
    """Check if LocalStack is running"""
    s3_endpoint_url = os.getenv("S3_ENDPOINT_URL", "http://localhost:4566")
    logger.info(f"Checking if LocalStack is running at {s3_endpoint_url}")

    try:
        cmd = f"curl -s {s3_endpoint_url}"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

        if result.returncode != 0:
            logger.warning(f"LocalStack may not be running at {s3_endpoint_url}")
            logger.warning("Make sure Docker and LocalStack are running")
            logger.warning("You can start it with: docker-compose up -d")
            return False
        return True
    except Exception as e:
        logger.error(f"Error checking LocalStack: {str(e)}")
        return False


def ensure_s3_bucket_exists(bucket_name="nyc-duration"):
    """
    Make sure the S3 bucket exists in LocalStack.
    Creates input and output directories in the bucket.
    """
    s3_endpoint_url = os.getenv("S3_ENDPOINT_URL", "http://localhost:4566")
    logger.info(f"Setting up S3 bucket {bucket_name} at {s3_endpoint_url}")

    # First check if LocalStack is running
    if not check_localstack_running():
        logger.error("LocalStack does not appear to be running")
        logger.error("Please start LocalStack before running this script")
        sys.exit(1)

    try:
        # Check if the bucket exists
        cmd_check = f"aws --endpoint-url={s3_endpoint_url} s3 ls s3://{bucket_name}"
        logger.info(f"Checking if bucket exists: {bucket_name}")
        result = subprocess.run(cmd_check, shell=True, capture_output=True, text=True)

        if result.returncode != 0:
            # Bucket doesn't exist, create it
            logger.info(f"Creating S3 bucket: {bucket_name}")
            cmd_create = (
                f"aws --endpoint-url={s3_endpoint_url} s3 mb s3://{bucket_name}"
            )
            subprocess.run(cmd_create, shell=True, check=True)
            logger.info(f"Bucket created: {bucket_name}")
        else:
            logger.info(f"Bucket already exists: {bucket_name}")

        # Create in and out directories if they don't exist
        for path in ["in/", "out/"]:
            logger.info(f"Ensuring directory exists: s3://{bucket_name}/{path}")
            # Create a temporary empty file
            temp_file = "empty_file.tmp"
            with open(temp_file, "w"):
                pass  # Create empty file

            try:
                cmd = f"aws --endpoint-url={s3_endpoint_url} s3api put-object --bucket {bucket_name} --key {path} --body {temp_file}"
                subprocess.run(cmd, shell=True, check=True)
            finally:
                # Clean up the temp file
                if os.path.exists(temp_file):
                    os.remove(temp_file)

        logger.info(f"S3 bucket setup complete: s3://{bucket_name}/")

        # List bucket contents
        logger.info("\nCurrent bucket contents:")
        cmd_list = f"aws --endpoint-url={s3_endpoint_url} s3 ls --recursive s3://{bucket_name}/"
        subprocess.run(cmd_list, shell=True, check=True)

        return True

    except subprocess.CalledProcessError as e:
        logger.error(f"Error setting up S3 bucket: {str(e)}")
        return False


if __name__ == "__main__":
    # Set environment variables if not already set
    if "S3_ENDPOINT_URL" not in os.environ:
        os.environ["S3_ENDPOINT_URL"] = "http://localhost:4566"
        logger.info(f"Set S3_ENDPOINT_URL to default: {os.environ['S3_ENDPOINT_URL']}")

    if ensure_s3_bucket_exists():
        logger.info("Setup completed successfully")
        sys.exit(0)
    else:
        logger.error("Setup failed")
        sys.exit(1)
