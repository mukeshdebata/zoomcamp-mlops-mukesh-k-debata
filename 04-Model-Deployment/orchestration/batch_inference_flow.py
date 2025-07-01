import os
import pandas as pd
import pickle
from datetime import datetime
import boto3
from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner


@task
def download_data(year, month):
    """Download the dataset"""
    print(f"Downloading data for {year}-{month:02d}")

    # Use the NYC TLC data URL
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"

    os.makedirs("data", exist_ok=True)
    local_file = f"data/yellow_tripdata_{year}-{month:02d}.parquet"

    # Download the file if it doesn't exist
    if not os.path.exists(local_file):
        os.system(f"wget {url} -O {local_file}")

    return local_file


@task
def preprocess_data(file_path):
    """Preprocess the dataset"""
    print("Preprocessing data")

    df = pd.read_parquet(file_path)

    # Feature engineering
    df["duration"] = (
        df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    ).dt.total_seconds() / 60

    # Filter outliers
    df = df[(df.duration >= 1) & (df.duration <= 60)]

    # Feature creation
    categorical = ["PULocationID", "DOLocationID"]
    df[categorical] = df[categorical].astype(str)

    # Extract datetime features
    df["pickup_hour"] = df.tpep_pickup_datetime.dt.hour
    df["pickup_day"] = df.tpep_pickup_datetime.dt.day
    df["pickup_month"] = df.tpep_pickup_datetime.dt.month
    df["pickup_weekday"] = df.tpep_pickup_datetime.dt.weekday

    # Convert to dictionaries for DictVectorizer
    dicts = df[
        categorical
        + [
            "pickup_hour",
            "pickup_day",
            "pickup_month",
            "pickup_weekday",
            "trip_distance",
        ]
    ].to_dict(orient="records")

    return dicts, df


@task
def load_model(model_path):
    """Load the trained model and DictVectorizer"""
    print(f"Loading model from {model_path}")

    with open(model_path, "rb") as f_in:
        model = pickle.load(f_in)

    # If separate DictVectorizer file exists
    dv_path = "dv.pkl"
    if os.path.exists(dv_path):
        with open(dv_path, "rb") as f_in:
            dv = pickle.load(f_in)
    else:
        # Assume model is a tuple of (model, dv)
        dv = None
        if isinstance(model, tuple) and len(model) == 2:
            model, dv = model

    return model, dv


@task
def run_inference(model, dv, features):
    """Run inference on the preprocessed data"""
    print("Running inference")

    if dv is not None:
        X = dv.transform(features)
    else:
        X = features  # Assume features are already transformed

    y_pred = model.predict(X)

    return y_pred


@task
def process_results(df, predictions, output_file):
    """Process and save the results"""
    print(f"Processing results and saving to {output_file}")

    # Create results dataframe
    results_df = pd.DataFrame()
    results_df["ride_id"] = df.index
    results_df["predicted_duration"] = predictions
    results_df["actual_duration"] = df["duration"]
    results_df["pickup_datetime"] = df["tpep_pickup_datetime"]
    results_df["dropoff_datetime"] = df["tpep_dropoff_datetime"]
    results_df["PULocationID"] = df["PULocationID"]
    results_df["DOLocationID"] = df["DOLocationID"]

    # Calculate mean duration
    mean_duration = predictions.mean()
    print(f"Mean predicted duration: {mean_duration:.2f} minutes")

    # Save to parquet
    results_df.to_parquet(output_file, engine="pyarrow")

    return output_file, mean_duration


@task
def upload_to_cloud(file_path, cloud_provider="s3", **kwargs):
    """Upload the results to cloud storage"""
    print(f"Uploading results to {cloud_provider}")

    if cloud_provider.lower() == "s3":
        return upload_to_s3(file_path, **kwargs)
    elif cloud_provider.lower() == "gcs":
        return upload_to_gcs(file_path, **kwargs)
    elif cloud_provider.lower() == "azure":
        return upload_to_azure(file_path, **kwargs)
    else:
        print(f"Cloud provider {cloud_provider} not supported")
        return False


def upload_to_s3(file_path, bucket_name, object_name=None):
    """Upload a file to S3"""
    if object_name is None:
        object_name = os.path.basename(file_path)

    try:
        s3_client = boto3.client("s3")
        s3_client.upload_file(file_path, bucket_name, object_name)
        print(f"Successfully uploaded {file_path} to {bucket_name}/{object_name}")
        return True
    except Exception as e:
        print(f"Error uploading to S3: {e}")
        return False


def upload_to_gcs(file_path, bucket_name, blob_name=None):
    """Upload a file to GCS"""
    try:
        from google.cloud import storage

        if blob_name is None:
            blob_name = os.path.basename(file_path)

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        blob.upload_from_filename(file_path)
        print(f"Successfully uploaded {file_path} to gs://{bucket_name}/{blob_name}")
        return True
    except ImportError:
        print(
            "Google Cloud Storage library not installed. Run: pip install google-cloud-storage"
        )
        return False
    except Exception as e:
        print(f"Error uploading to GCS: {e}")
        return False


def upload_to_azure(file_path, container_name, blob_name=None):
    """Upload a file to Azure Blob Storage"""
    try:
        from azure.storage.blob import BlobServiceClient

        if blob_name is None:
            blob_name = os.path.basename(file_path)

        connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")

        blob_service_client = BlobServiceClient.from_connection_string(
            connection_string
        )
        blob_client = blob_service_client.get_blob_client(
            container=container_name, blob=blob_name
        )

        with open(file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)

        print(
            f"Successfully uploaded {file_path} to Azure container {container_name} as {blob_name}"
        )
        return True
    except ImportError:
        print(
            "Azure Blob Storage library not installed. Run: pip install azure-storage-blob"
        )
        return False
    except Exception as e:
        print(f"Error uploading to Azure: {e}")
        return False


@task
def send_notification(mean_duration, success=True):
    """Send notification about job completion"""
    if success:
        print(
            f"✅ Batch inference completed successfully! Mean duration: {mean_duration:.2f} minutes"
        )
    else:
        print(f"❌ Batch inference failed!")

    # You could add email, Slack, or other notification methods here
    return success


@flow(task_runner=SequentialTaskRunner())
def batch_inference(
    year: int = 2023,
    month: int = 4,
    model_path: str = "../models/model.bin",
    output_file: str = None,
    upload_to_cloud_storage: bool = False,
    cloud_provider: str = "s3",
    bucket_name: str = "taxi-duration-predictions",
):
    """Main batch inference workflow"""
    # Generate output filename with timestamp
    if output_file is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = (
            f"../output/result_yellow_tripdata_{year}-{month:02d}_{timestamp}.parquet"
        )

    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    # Execute tasks in sequence
    data_file = download_data(year, month)
    features, df = preprocess_data(data_file)
    model, dv = load_model(model_path)
    predictions = run_inference(model, dv, features)
    result_file, mean_duration = process_results(df, predictions, output_file)

    # Upload to cloud (optional)
    if upload_to_cloud_storage:
        upload_success = upload_to_cloud(
            result_file, cloud_provider=cloud_provider, bucket_name=bucket_name
        )
    else:
        upload_success = True

    # Send notification
    send_notification(mean_duration, success=upload_success)

    return mean_duration


if __name__ == "__main__":
    batch_inference()
