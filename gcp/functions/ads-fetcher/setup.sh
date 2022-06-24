enable_api() {
  gcloud services enable run.googleapis.com # required for Gen2 GCF
  gcloud services enable artifactregistry.googleapis.com # required for Gen2 GCF
  gcloud services enable cloudresourcemanager.googleapis.com
  gcloud services enable iamcredentials.googleapis.com
  gcloud services enable cloudbuild.googleapis.com
  gcloud services enable bigquery.googleapis.com
}

#enable_api()

PROJECT_ID=$(gcloud config get-value project 2> /dev/null)
PROJECT_NUMBER=$(gcloud projects describe $PROJECT_ID --format="csv(projectNumber)" | tail -n 1)
# Gen1:
SERVICE_ACCOUNT=$PROJECT_ID@appspot.gserviceaccount.com
# Gen2:
#SERVICE_ACCOUNT=$PROJECT_NUMBER-compute@developer.gserviceaccount.com

# for Gen1:
# Grant the default service account with the Service Account Token Creator role so it could create GCS signed urls
gcloud projects add-iam-policy-binding $PROJECT_ID --member=serviceAccount:$SERVICE_ACCOUNT --role=roles/iam.serviceAccountTokenCreator

# Grant the default service account with read permissions on Cloud Storage
gcloud projects add-iam-policy-binding $PROJECT_ID --member=serviceAccount:$SERVICE_ACCOUNT --role=roles/storage.objectViewer

# Grant the default service account with admin permissions in BigQuery
gcloud projects add-iam-policy-binding $PROJECT_ID --member=serviceAccount:$SERVICE_ACCOUNT --role=roles/bigquery.admin
