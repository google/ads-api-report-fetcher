# Cloud Function deployment script
# NOTE: it's an example, adjust it to your needs

gcloud beta functions deploy ads-fetcher \
  --trigger-http \
  --entry-point=main \
  --runtime=nodejs16 \
  --timeout=540s \
  --memory=512MB \
  --region=us-central1 \
  --allow-unauthenticated \
  --source=.

#  --env-vars-file .env.yaml
# --timeout=540s
#For GCF 1st gen functions, cannot be more than 540s.
#For GCF 2nd gen functions, cannot be more than 3600s. 
