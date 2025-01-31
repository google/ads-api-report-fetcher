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
CYAN='\033[0;36m'
RED='\033[0;31m'
NC='\033[0m'

SCRIPT_PATH=$(readlink -f "$0" | xargs dirname)
pushd $SCRIPT_PATH > /dev/null

FUNCTION_NAME=gaarf
REGION=
MEMORY=1024MB
RETRIES=10
MAX_INSTANCES=

while :; do
    case $1 in
  -n|--name)
      shift
      FUNCTION_NAME=$1
      ;;
  -r|--region)
      shift
      REGION=--region=$1
      ;;
  -m|--memory)
      shift
      MEMORY=$1
      ;;
  -mc|--memory-getcids)
      shift
      MEMORY_GETCIDS=$1
      ;;
  --no-retry)
      NO_RETRY=true
      ;;
  --retries)
      shift
      RETRIES=$1
      ;;
  --max-instances)
      shift
      MAX_INSTANCES=--max-instances=$1
      ;;
  --service-account)
      shift
      SERVICE_ACCOUNT=--service-account=$1
      ;;
  --use-secret-manager)
      USE_SECRET_MANAGER=true
      ;;
  *)
      break
    esac
  shift
done

statusfile=$(mktemp)  # a file for saving exitcode from gcloud commands
logfile=$(mktemp)     # a file for saving output from gcloud commands
function execute_deploy() {
  local deployable_function=$1
  local entry_point=$2
  local memory=$3
  local set_secret
  if [[ $USE_SECRET_MANAGER ]]; then
    set_secret="--set-secrets DEVELOPER_TOKEN=google-ads-dev-token:latest"
  fi

  gcloud functions deploy $deployable_function \
      --trigger-http \
      --entry-point=$entry_point \
      --runtime=nodejs20 \
      --timeout=3600s \
      --memory=$memory \
      $REGION \
      --quiet \
      --gen2 \
      $MAX_INSTANCES \
      $SERVICE_ACCOUNT \
      $set_secret \
      --source=.
  echo $? > $statusfile
}

function redeploy_cf() {
  local deployable_function=$1
  local entry_point=$2
  local memory=$3
  if [ ! $memory ]; then
    memory='512MB' # default memory for CF Gen2 usually not enough to build them, with 512 it works fine
  fi
  # it's ok if it fails:
  gcloud functions delete $deployable_function --gen2 $REGION --quiet 2> /dev/null

  echo -e "${CYAN}Deploying $deployable_function Cloud Function${NC}"
  if [[ $NO_RETRY ]]; then
    execute_deploy $deployable_function $entry_point $memory
    exitcode=$?
    if [ $exitcode -ne 0 ]; then
      echo 'Breaking script as gcloud command failed'
      exit $exitcode
    fi
  else
    # deploy with retrying on error
    for ((i=1; i<=RETRIES; i++)); do
      execute_deploy $deployable_function $entry_point $memory 2>&1 | tee $logfile
      output=$(cat $logfile)
      rm $logfile
      exitcode=$(cat $statusfile)
      rm $statusfile
      if [[ $exitcode -eq 0 ]]; then
        echo -e "${CYAN}Deployment is successful${NC}"
        break
      else
        if [[ $output == *"OperationError: code=7, message=Unable to retrieve the repository metadata"* ]]; then
          if [[ $i -eq $RETRIES ]]; then
            echo -e "${RED}Breaking script as maximum number of retries ($RETRIES) exceeded${NC}"
            exit $exitcode
          fi
          echo -e "${CAYN}Retrying the deployment ($i)...${NC}"
          continue
        else
          echo -e "${RED}Breaking script as gcloud command failed${NC}"
          exit $exitcode
        fi
      fi
      sleep 10s
    done
  fi
}

redeploy_cf $FUNCTION_NAME main $MEMORY

redeploy_cf $FUNCTION_NAME-getcids main_getcids $MEMORY_GETCIDS

redeploy_cf $FUNCTION_NAME-bq main_bq

redeploy_cf $FUNCTION_NAME-bq-view main_bq_view

popd > /dev/null
