import os
import pandas as pd
import sys
from pathlib import Path

# Add the parent directory to sys.path to import from the parent package
sys.path.insert(0, str(Path(__file__).parent.parent.parent))


def test_batch_prediction():
    """Integration test for batch prediction

    1. Run the integration_test.py to prepare the test data
    2. Run the batch script for the same year/month
    3. Check the output and verify predictions
    """
    # Configure test environment
    os.environ["INPUT_FILE_PATTERN"] = (
        "s3://nyc-duration/in/{year:04d}-{month:02d}.parquet"
    )
    os.environ["OUTPUT_FILE_PATTERN"] = (
        "s3://nyc-duration/out/{year:04d}-{month:02d}.parquet"
    )
    os.environ["S3_ENDPOINT_URL"] = "http://localhost:4566"

    # Set up parameters
    year, month = 2023, 1
    s3_endpoint_url = os.environ["S3_ENDPOINT_URL"]

    # Configure S3 options for LocalStack
    options = {"client_kwargs": {"endpoint_url": s3_endpoint_url}}

    # Import main function and run it
    from homework06.batch_refactoring import main

    main(year, month)

    # Define output path
    output_pattern = os.environ["OUTPUT_FILE_PATTERN"]
    output_file = output_pattern.format(year=year, month=month)

    # Read the output file
    df_result = pd.read_parquet(output_file, storage_options=options)
    print(f"Successfully read output from {output_file}")
    print(df_result)

    # Calculate sum of predicted durations
    sum_pred = df_result["predicted_duration"].sum()
    print(f"Sum of predicted durations: {sum_pred}")

    # We're looking for a sum close to one of these values:
    # 13.08, 36.28, 69.28, 81.08
    return sum_pred


if __name__ == "__main__":
    sum_predicted = test_batch_prediction()
    print("\n" + "=" * 50)
    print(f"INTEGRATION TEST RESULT: Sum of predicted durations = {sum_predicted:.2f}")
    print("=" * 50 + "\n")

    # Compare with Q6 options
    options = [13.08, 36.28, 69.28, 81.08]
    closest = min(options, key=lambda x: abs(x - sum_predicted))

    print(f"The closest option from Q6 is: {closest}")