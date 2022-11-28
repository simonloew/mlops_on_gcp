#! /bin/bash

export PROJECT_ID=XXX # TODO: Replace with your PROJECT ID
export USER_NAME="xxx@xxx.com" # TODO: Replace with your GCP user name

gcloud services enable compute.googleapis.com \
                       containerregistry.googleapis.com \
                       aiplatform.googleapis.com \
                       cloudbuild.googleapis.com \
                       cloudfunctions.googleapis.com \
                       --project=${PROJECT_ID}

gcloud storage buckets create gs://training_data_${PROJECT_ID} \
    --project=${PROJECT_ID} \
    --location="europe-west1"

gcloud iam service-accounts create vertex-ai-service-account \
    --description="VertexAI Service Account" \
    --display-name="vertex-ai-service-account" \
    --project=${PROJECT_ID}

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
    --member="serviceAccount:vertex-ai-service-account@${PROJECT_ID}.iam.gserviceaccount.com" \
    --role="roles/aiplatform.user"

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
    --member="serviceAccount:vertex-ai-service-account@${PROJECT_ID}.iam.gserviceaccount.com" \
    --role="roles/storage.objectViewer"

gcloud iam service-accounts add-iam-policy-binding \
    vertex-ai-service-account@${PROJECT_ID}.iam.gserviceaccount.com \
    --member="user:${USER_NAME}" \
    --role="roles/iam.serviceAccountUser"

gsutil iam ch \
    serviceAccount:vertex-ai-service-account@${PROJECT_ID}.iam.gserviceaccount.com:roles/storage.objectCreator,objectViewer,objectAdmin \
    gs://training_data_${PROJECT_ID}

