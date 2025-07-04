# Fix Python import path issues when running from terminal
try:
    from fix_imports import *  # This adds parent directory to Python path
except ImportError:
    pass

import pandas as pd
import logging
from datetime import datetime
import sys
import os

# Dynamically adjust imports based on where the script is run from
try:
    from homework06.batch_refactoring import (
        prepare_data,
        get_input_path,
        get_output_path,
    )
except ImportError:
    # Try relative import if running from tests directory
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from batch_refactoring import prepare_data, get_input_path, get_output_path

# Configure logging for tests
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def dt(hour, minute, second=0):
    return datetime(2023, 1, 1, hour, minute, second)


def test_prepare_data():
    """Test the prepare_data function"""
    logger.info("Starting test_prepare_data")

    # Create the test input data
    data = [
        (None, None, dt(1, 1), dt(1, 10)),  # 9 min duration, None locations
        (1, 1, dt(1, 2), dt(1, 10)),  # 8 min duration
        (
            1,
            None,
            dt(1, 2, 0),
            dt(1, 2, 59),
        ),  # 0.98 min duration - should be filtered out
        (3, 4, dt(1, 2, 0), dt(2, 2, 1)),  # 60.02 min duration - should be filtered out
    ]

    columns = [
        "PULocationID",
        "DOLocationID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
    ]
    df = pd.DataFrame(data, columns=columns)
    logger.info(f"Created test DataFrame with {len(df)} rows")

    # Apply the prepare_data function
    categorical = ["PULocationID", "DOLocationID"]
    result = prepare_data(df, categorical)

    # Log the result
    logger.info(f"DataFrame after prepare_data has {len(result)} rows")
    for index, row in result.iterrows():
        logger.info(
            f"Row {index}: Duration={row['duration']:.2f}, PULocationID={row['PULocationID']}, DOLocationID={row['DOLocationID']}"
        )

    # Check that the result has the expected number of rows
    assert len(result) == 2, f"Expected 2 rows, got {len(result)}"
    logger.info("Row count assertion passed")

    # Check that the categorical columns were properly converted
    assert result["PULocationID"].tolist() == ["-1", "1"]
    assert result["DOLocationID"].tolist() == ["-1", "1"]
    logger.info("Categorical column conversion assertion passed")

    # Check the duration calculations
    assert result["duration"].tolist() == [9.0, 8.0]
    logger.info("Duration calculation assertion passed")

    logger.info("test_prepare_data completed successfully")


def test_path_functions():
    """Test the path handling functions"""
    logger.info("Starting test_path_functions")

    # Test path patterns
    input_path = get_input_path(2023, 3)
    logger.info(f"Default input path: {input_path}")

    # Check if input path is valid for current environment
    # Either it's a local path with yellow_tripdata or an S3 path
    if "s3://" in input_path:
        assert "in/2023-03.parquet" in input_path
        logger.info("Using S3 path format for input")
    else:
        assert "yellow_tripdata_2023-03.parquet" in input_path
        logger.info("Using local path format for input")

    output_path = get_output_path(2023, 3)
    logger.info(f"Default output path: {output_path}")

    # Check if output path is valid for current environment
    if "s3://" in output_path:
        assert "out/2023-03.parquet" in output_path
        logger.info("Using S3 path format for output")
    else:
        assert "taxi_type=yellow_year=2023_month=03.parquet" in output_path
        logger.info("Using local path format for output")

    # Test with environment variables (mock test)
    import os

    try:
        # Save original values
        original_input = os.environ.get("INPUT_FILE_PATTERN")
        original_output = os.environ.get("OUTPUT_FILE_PATTERN")

        # Set test values
        os.environ["INPUT_FILE_PATTERN"] = "test-input-{year:04d}-{month:02d}.parquet"
        os.environ["OUTPUT_FILE_PATTERN"] = "test-output-{year:04d}-{month:02d}.parquet"

        # Test with custom patterns
        input_path = get_input_path(2023, 3)
        logger.info(f"Custom input path: {input_path}")
        assert input_path == "test-input-2023-03.parquet"

        output_path = get_output_path(2023, 3)
        logger.info(f"Custom output path: {output_path}")
        assert output_path == "test-output-2023-03.parquet"

    finally:
        # Restore original values
        if original_input:
            os.environ["INPUT_FILE_PATTERN"] = original_input
        else:
            del os.environ["INPUT_FILE_PATTERN"]

        if original_output:
            os.environ["OUTPUT_FILE_PATTERN"] = original_output
        else:
            del os.environ["OUTPUT_FILE_PATTERN"]

    logger.info("test_path_functions completed successfully")
