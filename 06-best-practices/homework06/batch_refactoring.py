import sys
import os
import pickle
import pandas as pd
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def prepare_data(df, categorical):
    """
    Apply transformations to the dataframe:
    - Calculate trip duration in minutes
    - Filter trips between 1 and 60 minutes
    - Convert categorical columns to strings with -1 for missing values
    """
    df = df.copy()
    df["duration"] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df["duration"] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical] = df[categorical].fillna(-1).astype("int").astype("str")

    return df


def read_data(filename, categorical):
    """
    Read data from parquet file and prepare it using the prepare_data function
    If S3_ENDPOINT_URL is set, use it for reading from localstack
    """
    S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL")

    logger.info(f"Reading data from {filename}")

    if S3_ENDPOINT_URL and filename.startswith("s3://"):
        options = {"client_kwargs": {"endpoint_url": S3_ENDPOINT_URL}}
        logger.info(f"Using S3 endpoint URL: {S3_ENDPOINT_URL}")
        df = pd.read_parquet(filename, storage_options=options)
    else:
        df = pd.read_parquet(filename)

    logger.info(f"Read {len(df)} records from {filename}")

    prepared_df = prepare_data(df, categorical)
    logger.info(f"After preparation: {len(prepared_df)} records remaining")

    return prepared_df


def get_input_path(year, month):
    default_input_pattern = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet"
    input_pattern = os.getenv("INPUT_FILE_PATTERN", default_input_pattern)
    return input_pattern.format(year=year, month=month)


def get_output_path(year, month):
    default_output_pattern = (
        "homework06/data/taxi_type=yellow_year={year:04d}_month={month:02d}.parquet"
    )
    output_pattern = os.getenv("OUTPUT_FILE_PATTERN", default_output_pattern)
    return output_pattern.format(year=year, month=month)


def main(year, month):
    logger.info(f"Starting prediction for year={year}, month={month}")

    input_file = get_input_path(year, month)
    output_file = get_output_path(year, month)

    logger.info(f"Input file: {input_file}")
    logger.info(f"Output file: {output_file}")

    # Create the data directory if it doesn't exist and we're saving locally
    if not output_file.startswith("s3://"):
        os.makedirs(os.path.dirname(output_file), exist_ok=True)

    logger.info("Loading model...")
    # Get the directory where this script is located
    current_dir = os.path.dirname(os.path.abspath(__file__))
    model_path = os.path.join(current_dir, "model.bin")

    # Try different paths if the model is not found
    if not os.path.exists(model_path):
        alternative_paths = [
            os.path.join(current_dir, "model.bin"),
            os.path.join(current_dir, "homework06", "model.bin"),
            os.path.join(os.path.dirname(current_dir), "homework06", "model.bin"),
            "homework06/model.bin",
        ]

        for path in alternative_paths:
            logger.info(f"Trying model path: {path}")
            if os.path.exists(path):
                model_path = path
                logger.info(f"Found model at: {model_path}")
                break

    try:
        with open(model_path, "rb") as f_in:
            dv, lr = pickle.load(f_in)
        logger.info(f"Model loaded successfully from {model_path}")
    except FileNotFoundError:
        logger.error(f"Model file not found at {model_path}")
        logger.info("Current directory: " + os.getcwd())
        logger.info("Directory contents: " + str(os.listdir(os.getcwd())))
        raise

    categorical = ["PULocationID", "DOLocationID"]

    df = read_data(input_file, categorical)
    df["ride_id"] = f"{year:04d}/{month:02d}_" + df.index.astype("str")

    logger.info("Transforming features...")
    dicts = df[categorical].to_dict(orient="records")
    X_val = dv.transform(dicts)

    logger.info("Making predictions...")
    y_pred = lr.predict(X_val)

    logger.info(f"\nPredicted mean duration: {y_pred.mean():.2f}")
    logger.info(f"\nPredicted sum duration: {y_pred.sum():.2f}\n")

    df_result = pd.DataFrame()
    df_result["ride_id"] = df["ride_id"]
    df_result["predicted_duration"] = y_pred

    # Check if we need to use S3 endpoint URL for writing
    S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL")
    logger.info(f"Saving results to {output_file}")

    if S3_ENDPOINT_URL and output_file.startswith("s3://"):
        options = {"client_kwargs": {"endpoint_url": S3_ENDPOINT_URL}}
        logger.info(f"Using S3 endpoint URL for saving: {S3_ENDPOINT_URL}")
        df_result.to_parquet(
            output_file, engine="pyarrow", index=False, storage_options=options
        )
    else:
        df_result.to_parquet(output_file, engine="pyarrow", index=False)

    logger.info(f"Results saved successfully to {output_file}")
    logger.info(f"Total predictions: {len(df_result)}")

    return output_file


if __name__ == "__main__":
    year = int(sys.argv[1])
    month = int(sys.argv[2])
    main(year, month)