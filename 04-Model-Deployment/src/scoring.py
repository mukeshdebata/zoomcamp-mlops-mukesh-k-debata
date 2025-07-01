import pickle
import pandas as pd
import sys
import warnings

warnings.filterwarnings("ignore")


def read_data(filename, categorical):
    df = pd.read_parquet(filename)

    df["duration"] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df["duration"] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical] = df[categorical].fillna(-1).astype("int").astype("str")

    return df


def apply_model(model_file, data_file):
    with open(model_file, "rb") as f_in:
        dv, model = pickle.load(f_in)

    categorical = ["PULocationID", "DOLocationID"]

    df = read_data(filename=data_file, categorical=categorical)

    dicts = df[categorical].to_dict(orient="records")

    X_val = dv.transform(dicts)
    y_pred = model.predict(X_val)

    print(f"The mean predicted duration is {round(y_pred.mean(), 3)} minutes")

    return df, y_pred


def run(model_file, year, month):
    df, y_pred = apply_model(
        model_file=model_file,
        data_file=f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month}.parquet",
    )

    year_df = df["tpep_pickup_datetime"].dt.year.astype(str).str.zfill(4)
    month_df = df["tpep_pickup_datetime"].dt.month.astype(str).str.zfill(2)

    df["ride_id"] = year_df + "/" + month_df + "_" + df.index.astype(str)

    df_result = df[["ride_id"]].copy()
    df_result["duration"] = y_pred

    output_file = f"output/result_yellow_tripdata_{year}-{month}.parquet"
    df_result.to_parquet(output_file, engine="pyarrow", compression=None, index=False)
    return


if __name__ == "__main__":
    year = sys.argv[1]
    month = sys.argv[2]

    run(model_file="model.bin", year=year, month=month)