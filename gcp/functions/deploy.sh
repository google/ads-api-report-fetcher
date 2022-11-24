#!/bin/bash
#
# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#------------------------------------------------------------------------------
#
# Cloud Function deployment script
# it's assumed it'll be called in the function's source folder
# where a google-ads.yaml located (with your auth info for accessing Ads API)
# NOTE: it's an example, adjust it to your needs
#------------------------------------------------------------------------------

FUNCTION_NAME=gaarf
REGION=us-central1
MEMORY=1024MB

while :; do
    case $1 in
  -n|--name)
      shift
      FUNCTION_NAME=$1
      ;;
  -r|--region)
      shift
      REGION=$1
      ;;
  -m|--memory)
      shift
      MEMORY=$1
      ;;
  *)
      break
    esac
  shift
done

# NOTE:
#For GCF 1st gen functions, cannot be more than 540s.
#For GCF 2nd gen functions, cannot be more than 3600s.

gcloud functions delete $FUNCTION_NAME --gen2 --region=$REGION --quiet

gcloud functions deploy $FUNCTION_NAME \
  --trigger-http \
  --entry-point=main \
  --runtime=nodejs16 \
  --timeout=3600s \
  --memory=$MEMORY \
  --region=$REGION \
  --quiet \
  --gen2 \
  --source=.

exitcode=$?
if [ $exitcode -ne 0 ]; then
  echo 'Breaking script as gcloud command failed'
  exit $exitcode
fi

# If you need to increase memory about 2GB use this (gcloud functions deploy fails with memory sizes above 2GB):
#gcloud run services update $FUNCTION_NAME --region $REGION --cpu 1 --memory=2048Mi --no-cpu-throttling

gcloud functions delete $FUNCTION_NAME-getcids --gen2 --region=$REGION --quiet

gcloud functions deploy $FUNCTION_NAME-getcids \
  --trigger-http \
  --entry-point=main_getcids \
  --runtime=nodejs16 \
  --memory=512MB \
  --timeout=3600s \
  --region=$REGION \
  --quiet \
  --gen2 \
  --source=.

exitcode=$?
if [ $exitcode -ne 0 ]; then
  echo 'Breaking script as gcloud command failed'
  exit $exitcode
fi

gcloud functions delete $FUNCTION_NAME-bq --gen2 --region=$REGION --quiet

gcloud functions deploy $FUNCTION_NAME-bq \
  --trigger-http \
  --entry-point=main_bq \
  --runtime=nodejs16 \
  --memory=512MB \
  --timeout=3600s \
  --region=$REGION \
  --quiet \
  --gen2 \
  --source=.

exitcode=$?
if [ $exitcode -ne 0 ]; then
  echo 'Breaking script as gcloud command failed'
  exit $exitcode
fi

gcloud functions delete $FUNCTION_NAME-bq-view --gen2 --region=$REGION --quiet

gcloud functions deploy $FUNCTION_NAME-bq-view \
  --trigger-http \
  --entry-point=main_bq_view \
  --runtime=nodejs16 \
  --memory=512MB \
  --timeout=3600s \
  --region=$REGION \
  --quiet \
  --gen2 \
  --source=.


#  --env-vars-file .env.yaml
#  --allow-unauthenticated \
#  --timeout=540s
