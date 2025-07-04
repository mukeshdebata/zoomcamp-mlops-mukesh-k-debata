# Best Practices for MLOps

This module covers best practices for Machine Learning Operations (MLOps), focusing on code reliability, testing, and automation.

## Class Notes

### Topics Covered

1. **Testing the Code**
   - Unit tests with pytest
   - Integration tests with docker-compose
   - Testing cloud services with LocalStack

2. **Code Quality**
   - Linting and formatting
   - Git pre-commit hooks

3. **Development Tools**
   - Makefiles and make
   - Simplifying complex workflows

4. **Environment Management**
   - Staging vs. production environments
   - Infrastructure as Code

5. **Continuous Integration/Continuous Delivery**
   - CI/CD pipelines
   - GitHub Actions

### Key Code Snippets

#### Docker Operations

```bash
# Building Docker images
docker build -t stream-model-duration:v2 .

# Running Docker containers
docker run -it --rm \
    -p 8080:8080 \
    -e PREDICTIONS_STREAM_NAME="ride_predictions" \
    -e RUN_ID="e1efc53e9bd149078b0c12aeaa6365df" \
    -e TEST_RUN="True" \
    -e AWS_DEFAULT_REGION="eu-west-1" \
    stream-model-duration:v2
```

#### Working with LocalStack

```bash
# List streams
aws --endpoint-url=http://localhost:4566 \
    kinesis list-streams

# Create stream
aws --endpoint-url=http://localhost:4566 \
    kinesis create-stream \
    --stream-name ride_predictions \
    --shard-count 1
```

#### Make Commands

```bash
# Without make
isort .
black .
pylint --recursive=y .
pytest tests/

# With make
make quality_checks
make test
make setup
```

## Homework Solution: Improving Code Reliability with Tests

### Overview

The homework focused on improving the reliability of a batch prediction model by implementing unit and integration tests. We refactored the code to make it more testable, added unit tests for the data preprocessing logic, and created integration tests using LocalStack to simulate S3 interactions.

### Step-by-Step Solution

#### Q1: Refactoring

We refactored the code to eliminate global variables and improve modularity:

1. Created a `main` function with `year` and `month` parameters
2. Moved the code (except `read_data`) inside the `main` function
3. Made `categorical` a parameter for `read_data`
4. Added the Python idiom `if __name__ == "__main__":` to make the script both executable and importable

#### Q2: Installing pytest

1. Added pytest to development dependencies with `uv add --dev pytest`
2. Created a `tests` directory
3. Added a `test_batch_refactoring.py` file for tests
4. Added an `__init__.py` file to make the directory a proper Python package

The `__init__.py` file was crucial for enabling imports from parent directories.

#### Q3: Writing First Unit Test

1. Split the `read_data` function into separate I/O and transformation parts
2. Created a `prepare_data` function for transformations
3. Wrote a unit test using this test data:

```python
data = [
    (None, None, dt(1, 1), dt(1, 10)),
    (1, 1, dt(1, 2), dt(1, 10)),
    (1, None, dt(1, 2, 0), dt(1, 2, 59)),
    (3, 4, dt(1, 2, 0), dt(2, 2, 1)),      
]
```

4. Verified that the expected dataframe has 2 rows (not 1, 3, or 4) as 2 rows were filtered out:
   - One with duration < 1 minute (0.98 min)
   - One with duration > 60 minutes (60.02 min)

#### Q4: Mocking S3 with LocalStack

1. Created a `docker-compose.yaml` file with LocalStack configured for S3
2. Learned that the proper option for AWS CLI with LocalStack is `--endpoint-url`
3. Made input and output paths configurable via environment variables:
   ```bash
   export INPUT_FILE_PATTERN="s3://nyc-duration/in/{year:04d}-{month:02d}.parquet"
   export OUTPUT_FILE_PATTERN="s3://nyc-duration/out/{year:04d}-{month:02d}.parquet"
   ```
4. Modified the `read_data` function to use the S3 endpoint URL when available

#### Q5: Creating Test Data

1. Created an `integration_test.py` script
2. Used the dataframe from Q3 as test data for January 2023
3. Saved it to LocalStack S3 using the specified method
4. Found that the size of the test file was closest to 3620 bytes

#### Q6: Finishing the Integration Test

1. Created a `save_data` function similar to `read_data`
2. Ran the batch prediction for January 2023
3. Read the results and verified the sum of predicted durations
4. Confirmed that the sum is closest to 36.28

### Running the Tests

A shell script was created to run all the tests in sequence:

1. Start LocalStack
2. Run unit tests
3. Run integration tests
4. Verify the results

### Lessons Learned

1. **Code Structure**: Proper code structure with functions and clear separation of concerns makes testing easier
2. **Testing Methodology**: Unit tests for isolated components, integration tests for interactions
3. **Mocking External Services**: LocalStack provides a way to test AWS services locally
4. **Environment Configuration**: Using environment variables for configuration makes tests more flexible
5. **Validation**: Comprehensive tests give confidence in code reliability

The homework demonstrated the importance of testing in MLOps pipelines and provided practical experience in applying software engineering best practices to ML code.
