# Apache Beam / Dataflow Feature Pipeline

To run this code we need a new virtual environment and install Apache Beam alongside the `feature_pipeline` package:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

We can then run the code on GCP using the Dataflow runner:

```bash
export BUCKET="your-bucket-name"
export PROJECT_ID="project-id"

python ./main.py \
    --input=gs://${BUCKET}/data/2021/2021-12.csv \
    --output-flights=gs://${BUCKET}/features/flight_features/ \
    --output-airports=gs://${BUCKET}/features/airport_features/ \
    --output-read-instances=gs://${BUCKET}/features/read_instances/ \
    --runner=DataflowRunner \
    --project=${PROJECT_ID} \
    --region=europe-west1 \
    --staging_location=gs://${BUCKET}/beam_staging \
    --temp_location=gs://${BUCKET}/beam_tmp \
    --job_name=flight-batch-features \
    --setup_file ./setup.py
```

Check the related blog post["MLOps on GCP - Part 2: Using the Vertex AI Feature Store with DataFlow and Apache Beam"](https://aiinpractice.com/gcp-mlops-vertex-ai-feature-store/) for a full tutorial.
