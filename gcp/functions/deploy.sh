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

function redeploy_cf() {
  local deployable_function=$1
  local entry_point=$2
  local memory=$3
  if [ ! $memory ]; then
    memory='512MB' # default memory for CF Gen2 usually not enough to build them, with 512 it works fine
  fi
  # it's ok if it fails:
  gcloud functions delete $deployable_function --gen2 --region=$REGION --quiet

  gcloud functions deploy $deployable_function \
      --trigger-http \
      --entry-point=$entry_point \
      --runtime=nodejs16 \
      --timeout=3600s \
      --memory=$memory \
      --region=$REGION \
      --quiet \
      --gen2 \
      --source=.

  exitcode=$?
  if [ $exitcode -ne 0 ]; then
    echo 'Breaking script as gcloud command failed'
    exit $exitcode
  fi
}

# NOTE:
#For GCF 1st gen functions, cannot be more than 540s.
#For GCF 2nd gen functions, cannot be more than 3600s.

redeploy_cf $FUNCTION_NAME main $MEMORY

# If you need to increase memory about 2GB use this (gcloud functions deploy fails with memory sizes above 2GB):
#gcloud run services update $FUNCTION_NAME --region $REGION --cpu 1 --memory=2048Mi --no-cpu-throttling

redeploy_cf $FUNCTION_NAME-getcids main_getcids

redeploy_cf $FUNCTION_NAME-bq main_bq

redeploy_cf $FUNCTION_NAME-bq-view main_bq_view


#  --env-vars-file .env.yaml
#  --allow-unauthenticated \
#  --timeout=540s
