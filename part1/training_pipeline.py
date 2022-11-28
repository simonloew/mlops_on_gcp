from google.cloud import aiplatform as aip
from kfp.v2.dsl import (
    Artifact,
    Dataset,
    Input,
    Model,
    Output,
    ClassificationMetrics,
    component,
    pipeline,
)
from kfp.v2 import compiler

from google_cloud_pipeline_components.v1.endpoint import EndpointCreateOp, ModelDeployOp
from google_cloud_pipeline_components.v1.model import ModelUploadOp


PROJECT_ID = "XXX"  # TODO: Replace it with your own project id
REGION = "europe-west1"
SERVICE_ACCOUNT = f"vertex-ai-service-account@{PROJECT_ID}.iam.gserviceaccount.com"
BUCKET = f"training_data_{PROJECT_ID}"

pipeline_root_path = f"gs://{BUCKET}/pipeline-output/"


@component(
    base_image="europe-docker.pkg.dev/vertex-ai/prediction/sklearn-cpu.1-0:latest",
)
def data_download(
    data_url: str,
    split_date: str,
    dataset_train: Output[Dataset],
    dataset_test: Output[Dataset],
):
    import pandas as pd

    import logging

    logging.warn("Import file:", data_url)

    data = pd.read_csv(data_url)

    cancelled = (data["Cancelled"] > 0) | (data["Diverted"] > 0)
    completed_flights = data[~cancelled]

    training_data = completed_flights[["DepDelay", "TaxiOut", "Distance"]]
    # Consider flights that arrive more than 15 min late as delayed
    training_data["target"] = completed_flights["ArrDelay"] > 15

    test_data = training_data[completed_flights["FlightDate"] >= split_date]
    training_data = training_data[completed_flights["FlightDate"] < split_date]

    training_data.to_csv(dataset_train.path, index=False)
    test_data.to_csv(dataset_test.path, index=False)


@component(
    base_image="europe-docker.pkg.dev/vertex-ai/prediction/sklearn-cpu.1-0:latest",
)
def model_train(
    dataset: Input[Dataset],
    model: Output[Artifact],
):
    import pandas as pd
    import pickle
    from sklearn.pipeline import Pipeline
    from sklearn.impute import SimpleImputer
    from sklearn.preprocessing import StandardScaler
    from sklearn.linear_model import LogisticRegression

    data = pd.read_csv(dataset.path)
    X = data.drop(columns=["target"])
    y = data["target"]

    model_pipeline = Pipeline(
        [
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
            ("clf", LogisticRegression(random_state=42)),
        ]
    )

    model_pipeline.fit(X, y)

    model.metadata["framework"] = "scikit-learn"
    model.metadata["containerSpec"] = {
        "imageUri": "europe-docker.pkg.dev/vertex-ai/prediction/sklearn-cpu.1-0:latest"
    }

    file_name = model.path + "/model.pkl"
    import pathlib

    pathlib.Path(model.path).mkdir()
    with open(file_name, "wb") as file:
        pickle.dump(model_pipeline, file)


@component(
    base_image="europe-docker.pkg.dev/vertex-ai/prediction/sklearn-cpu.1-0:latest",
)
def model_evaluate(
    test_set: Input[Dataset],
    model: Input[Model],
    metrics: Output[ClassificationMetrics],
):
    import pandas as pd
    import pickle
    from sklearn.metrics import roc_curve, confusion_matrix, accuracy_score

    data = pd.read_csv(test_set.path)[:1000]
    file_name = model.path + "/model.pkl"
    with open(file_name, "rb") as file:
        model_pipeline = pickle.load(file)

    X = data.drop(columns=["target"])
    y = data.target
    y_pred = model_pipeline.predict(X)

    y_scores = model_pipeline.predict_proba(X)[:, 1]
    fpr, tpr, thresholds = roc_curve(y_true=y, y_score=y_scores, pos_label=True)
    metrics.log_roc_curve(fpr.tolist(), tpr.tolist(), thresholds.tolist())

    metrics.log_confusion_matrix(
        ["False", "True"],
        confusion_matrix(y, y_pred).tolist(),
    )


# Define the workflow of the pipeline.
@pipeline(name="gcp-mlops-v0", pipeline_root=pipeline_root_path)
def pipeline(
    training_data_url: str = f"gs://{BUCKET}/data/2021/2021-12.csv",
    test_split_date: str = "2021-12-20",
):
    data_op = data_download(
        data_url=training_data_url,
        split_date=test_split_date
    )

    from google_cloud_pipeline_components.experimental.custom_job.utils import (
        create_custom_training_job_op_from_component,
    )

    custom_job_distributed_training_op = create_custom_training_job_op_from_component(
        model_train, replica_count=1
    )

    model_train_op = custom_job_distributed_training_op(
        dataset=data_op.outputs["dataset_train"],
        project=PROJECT_ID,
        location=REGION,
    )

    model_evaluate_op = model_evaluate(
        test_set=data_op.outputs["dataset_test"],
        model=model_train_op.outputs["model"],
    )

    model_upload_op = ModelUploadOp(
        project=PROJECT_ID,
        location=REGION,
        display_name="flight-delay-model",
        unmanaged_container_model=model_train_op.outputs["model"],
    ).after(model_evaluate_op)

    endpoint_create_op = EndpointCreateOp(
        project=PROJECT_ID,
        location=REGION,
        display_name="flight-delay-endpoint",
    )

    ModelDeployOp(
        endpoint=endpoint_create_op.outputs["endpoint"],
        model=model_upload_op.outputs["model"],
        deployed_model_display_name="flight-delay-model",
        dedicated_resources_machine_type="n1-standard-4",
        dedicated_resources_min_replica_count=1,
        dedicated_resources_max_replica_count=1,
    )


compiler.Compiler().compile(pipeline_func=pipeline, package_path="gcp-mlops-v0.json")

aip.init(project=PROJECT_ID, staging_bucket=BUCKET, location=REGION)

job = aip.PipelineJob(
    display_name="gcp-mlops-v0",
    template_path="gcp-mlops-v0.json",
    pipeline_root=pipeline_root_path,
)

job.run(service_account=SERVICE_ACCOUNT)
