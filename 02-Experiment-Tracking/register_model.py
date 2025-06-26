import os
import pickle
import click
import mlflow

from mlflow.entities import ViewType
from mlflow.tracking import MlflowClient
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error

HPO_EXPERIMENT_NAME = "random-forest-hyperopt"
EXPERIMENT_NAME = "random-forest-best-models"
RF_PARAMS = ['max_depth', 'n_estimators', 'min_samples_split', 'min_samples_leaf', 'random_state']

mlflow.set_tracking_uri("http://127.0.0.1:5000")
mlflow.sklearn.autolog()


def load_pickle(filename):
    """Load a pickle file"""
    with open(filename, "rb") as f_in:
        return pickle.load(f_in)


def restore_experiment(client, experiment_name):
    """Restore the experiment if it is in a 'deleted' state"""
    experiment = client.get_experiment_by_name(experiment_name)
    if experiment and experiment.lifecycle_stage == "deleted":
        client.restore_experiment(experiment.experiment_id)
        print(f"Experiment '{experiment_name}' has been restored.")
    return experiment


def train_and_log_model(data_path, params):
    """Train a RandomForest model and log the training details"""
    X_train, y_train = load_pickle(os.path.join(data_path, "train.pkl"))
    X_val, y_val = load_pickle(os.path.join(data_path, "val.pkl"))
    X_test, y_test = load_pickle(os.path.join(data_path, "test.pkl"))

    with mlflow.start_run():
        for param in RF_PARAMS:
            params[param] = int(params[param])  # Convert string params to integers where applicable

        rf = RandomForestRegressor(**params)
        rf.fit(X_train, y_train)

        # Evaluate model on the validation and test sets
        val_rmse = mean_squared_error(y_val, rf.predict(X_val), squared=False)
        mlflow.log_metric("val_rmse", val_rmse)
        test_rmse = mean_squared_error(y_test, rf.predict(X_test), squared=False)
        mlflow.log_metric("test_rmse", test_rmse)


@click.command()
@click.option(
    "--data_path",
    default="./output",
    help="Location where the processed NYC taxi trip data was saved"
)
@click.option(
    "--top_n",
    default=5,
    type=int,
    help="Number of top models that need to be evaluated to decide which one to promote"
)
def run_register_model(data_path: str, top_n: int):
    """Retrieve, evaluate, and register the best model"""

    client = MlflowClient()

    # Restore experiments if they are deleted
    hpo_experiment = restore_experiment(client, HPO_EXPERIMENT_NAME)
    if hpo_experiment is None:
        raise ValueError(f"Experiment '{HPO_EXPERIMENT_NAME}' does not exist.")

    best_models_experiment = restore_experiment(client, EXPERIMENT_NAME)
    if best_models_experiment is None:
        # Create the experiment if it does not exist
        experiment_id = client.create_experiment(EXPERIMENT_NAME)
        print(f"Experiment '{EXPERIMENT_NAME}' has been created with ID {experiment_id}.")

    # Retrieve the top_n model runs and log the models
    runs = client.search_runs(
        experiment_ids=hpo_experiment.experiment_id,
        run_view_type=ViewType.ACTIVE_ONLY,
        max_results=top_n,
        order_by=["metrics.rmse ASC"]
    )
    for run in runs:
        print(f"Training model with parameters from run {run.info.run_id}")
        train_and_log_model(data_path=data_path, params=run.data.params)

    # Select the model with the lowest test RMSE
    best_run = client.search_runs(
        experiment_ids=best_models_experiment.experiment_id,
        run_view_type=ViewType.ACTIVE_ONLY,
        max_results=top_n,
        order_by=["metrics.test_rmse ASC"]
    )[0]

    # Register the best model
    run_id = best_run.info.run_id
    model_uri = f"runs:/{run_id}/model"
    registered_model_name = "rf-best-model"

    print(f"Registering model from run {run_id} to model registry with name '{registered_model_name}'")
    mlflow.register_model(model_uri, name=registered_model_name)


if __name__ == '__main__':
    run_register_model()