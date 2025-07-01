import datetime
import time
import random
import logging
import uuid
import pytz
import pandas as pd
import io
import psycopg
import joblib

from prefect import task, flow

from evidently import Report
from evidently import DataDefinition
from evidently import Dataset
from evidently.metrics.column_statistics import QuantileValue, ValueDrift

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s]: %(message)s"
)

SEND_TIMEOUT = 10
CONNECTION_STRING = "host=localhost port=5432 user=postgres password=example"
CONNECTION_STRING_DB = CONNECTION_STRING + " dbname=test"
rand = random.Random()

create_table_statement = """
drop table if exists dummy_metrics;
create table dummy_metrics(
	timestamp timestamp,
	quantile_fare float,
	pred_drift float
)
"""

reference_data = pd.read_parquet("data/reference.parquet")
with open("models/lin_reg.bin", "rb") as f_in:
    model = joblib.load(f_in)

raw_data = pd.read_parquet("data/green_tripdata_2024-03.parquet")

begin = datetime.datetime(2024, 3, 1, 0, 0)
num_features = ["passenger_count", "trip_distance", "fare_amount", "total_amount"]
cat_features = ["PULocationID", "DOLocationID"]
data_definition = DataDefinition(
    numerical_columns=num_features + ["prediction"],
    categorical_columns=cat_features,
)

report = Report(
    metrics=[
        QuantileValue(column="fare_amount", quantile=0.5),
        ValueDrift(column="prediction"),
    ],
    include_tests=True,
)


@task
def prep_db():
    with psycopg.connect(CONNECTION_STRING, autocommit=True) as conn:
        res = conn.execute("SELECT 1 FROM pg_database WHERE datname='test'")
        if len(res.fetchall()) == 0:
            conn.execute("create database test;")
        with psycopg.connect(CONNECTION_STRING_DB) as conn:
            conn.execute(create_table_statement)


@task
def calculate_metrics_postgresql(i):
    current_data = raw_data[
        (raw_data.lpep_pickup_datetime >= (begin + datetime.timedelta(i)))
        & (raw_data.lpep_pickup_datetime < (begin + datetime.timedelta(i + 1)))
    ]

    # current_data.fillna(0, inplace=True)
    current_data["prediction"] = model.predict(
        current_data[num_features + cat_features].fillna(0)
    )

    current_dataset = Dataset.from_pandas(current_data, data_definition=data_definition)
    reference_dataset = Dataset.from_pandas(
        reference_data, data_definition=data_definition
    )

    run = report.run(reference_data=reference_dataset, current_data=current_dataset)

    result = run.dict()

    quantile_fare = result["metrics"][0]["value"]
    pred_drift = result["metrics"][1]["value"]

    with psycopg.connect(CONNECTION_STRING_DB, autocommit=True) as conn:
        with conn.cursor() as curr:
            curr.execute(
                "insert into dummy_metrics(timestamp, quantile_fare, pred_drift) values (%s, %s, %s)",
                (begin + datetime.timedelta(i), quantile_fare, pred_drift),
            )


@flow
def batch_monitoring_backfill():
    prep_db()
    last_send = datetime.datetime.now() - datetime.timedelta(seconds=10)
    for i in range(0, 30):
        calculate_metrics_postgresql(i)

        new_send = datetime.datetime.now()
        seconds_elapsed = (new_send - last_send).total_seconds()
        if seconds_elapsed < SEND_TIMEOUT:
            time.sleep(SEND_TIMEOUT - seconds_elapsed)
        while last_send < new_send:
            last_send = last_send + datetime.timedelta(seconds=10)
        logging.info("data sent")


if __name__ == "__main__":
    batch_monitoring_backfill()
