from batch_inference_flow import batch_inference
from prefect.deployments import Deployment
from prefect.orion.schemas.schedules import CronSchedule

# Scheduled deployment
deployment = Deployment.build_from_flow(
    flow=batch_inference,
    name="taxi-duration-batch-inference",
    schedule=CronSchedule(
        cron="0 0 1 * *",  # Run at midnight on the 1st of every month
        timezone="UTC",
    ),
    parameters={
        "year": 2023,
        "month": 4,
        "model_path": "../models/model.bin",
        "upload_to_cloud_storage": True,
        "cloud_provider": "s3",
        "bucket_name": "taxi-duration-predictions",
    },
    tags=["taxi", "batch-inference", "production"],
)

if __name__ == "__main__":
    deployment.apply()
