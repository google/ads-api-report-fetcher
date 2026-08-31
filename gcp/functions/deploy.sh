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

# When we build and deploy functions we need to use 
# the exact same version of gaarf that is in the repository,
# regardless of its version and what's published on npm.
# So we build a local tar package and reference it explicitly in package.json.
function reference_npm_package() {
  # Build and pack the package
  cd ../../js
  npm i --production
  npm run build
  npm pack --pack-destination ../gcp/functions
  cd ..

  # Temporarily update the dependency to use tarball
  cd gcp/functions
  TARBALL=$(ls *.tgz | head -1)
  npm pkg set "dependencies.google-ads-api-report-fetcher=file:./$TARBALL"
  # NOTE: if you're building in dev time and tar-package is updated
  #       but version hasn't changed you should delete package-lock.json
  #       Because npm caches it and reuses the cache even if the file content
  #       changes but the version number stays the same
  npm install
}

function clear_npm_package() {
  # Clean up (optional - restore workspace reference)
  npm pkg set "dependencies.google-ads-api-report-fetcher=*"
  rm *.tgz
}

function execute_deploy() {
  local deployable_function=$1
  local entry_point=$2
  local memory=$3
  local statusfile=$4
  local set_secret
  if [[ $USE_SECRET_MANAGER ]]; then
    set_secret="--set-secrets DEVELOPER_TOKEN=google-ads-dev-token:latest"
  fi

  # we provide GAARF_SCHEMA_DIR envvar to Function for storing Ads json schemas
  local PROJECT_ID=$(gcloud config get-value project 2> /dev/null)
  local set_env_vars="--set-env-vars GAARF_SCHEMA_DIR=gs://${PROJECT_ID}/gaarf/schemas"

  gcloud functions deploy $deployable_function \
      --trigger-http \
      --ingress-settings=internal-and-gclb \
      --entry-point=$entry_point \
      --runtime=nodejs22 \
      --timeout=3600s \
      --memory=$memory \
      $REGION \
      --quiet \
      --gen2 \
      $MAX_INSTANCES \
      $SERVICE_ACCOUNT \
      $set_secret \
      $set_env_vars \
      --source=.
  echo $? > "$statusfile"
}

function redeploy_cf() {
  local deployable_function=$1
  local entry_point=$2
  local memory=$3
  if [ ! "$memory" ]; then
    memory='512MB' # default memory for CF Gen2 usually not enough to build them, with 512 it works fine
  fi

  local statusfile=$(mktemp)
  local logfile=$(mktemp)
  local exitcode=0

  echo -e "${CYAN}Deploying $deployable_function Cloud Function${NC}"

  if [[ $NO_RETRY ]]; then
    execute_deploy $deployable_function $entry_point $memory "$statusfile"
    exitcode=$(cat "$statusfile")
    if [ $exitcode -ne 0 ]; then
      echo -e "${RED}Deploying $deployable_function failed. Attempting fallback: delete and redeploy.${NC}"
      gcloud functions delete $deployable_function --gen2 $REGION --quiet 2> /dev/null
      execute_deploy $deployable_function $entry_point $memory "$statusfile"
      exitcode=$(cat "$statusfile")
      if [ $exitcode -ne 0 ]; then
        echo -e "${RED}Breaking script as gcloud command failed for $deployable_function${NC}"
        rm -f "$statusfile" "$logfile"
        exit $exitcode
      fi
    fi
  else
    # deploy with retrying on error
    for ((i=1; i<=RETRIES; i++)); do
      execute_deploy $deployable_function $entry_point $memory "$statusfile" 2>&1 | tee "$logfile"
      output=$(cat "$logfile")
      exitcode=$(cat "$statusfile")

      if [[ $exitcode -eq 0 ]]; then
        echo -e "${CYAN}Deployment of $deployable_function is successful${NC}"
        break
      else
        if [[ $output == *"OperationError: code=7, message=Unable to retrieve the repository metadata"* ]]; then
          if [[ $i -eq $RETRIES ]]; then
            echo -e "${RED}Breaking script as maximum number of retries ($RETRIES) exceeded for $deployable_function${NC}"
            rm -f "$statusfile" "$logfile"
            exit $exitcode
          fi
          echo -e "${CYAN}Retrying the deployment of $deployable_function ($i)...${NC}"
          sleep 10s
          continue
        else
          # Fallback: if it failed for another reason, try deleting it and redeploying from scratch
          echo -e "${RED}Deployment of $deployable_function failed. Attempting fallback: delete and redeploy.${NC}"
          gcloud functions delete $deployable_function --gen2 $REGION --quiet 2> /dev/null

          execute_deploy $deployable_function $entry_point $memory "$statusfile" 2>&1 | tee "$logfile"
          exitcode=$(cat "$statusfile")
          if [[ $exitcode -eq 0 ]]; then
             echo -e "${CYAN}Deployment of $deployable_function is successful (after delete fallback)${NC}"
             break
          fi

          echo -e "${RED}Breaking script as gcloud command failed for $deployable_function even after delete${NC}"
          rm -f "$statusfile" "$logfile"
          exit $exitcode
        fi
      fi
    done
  fi
  rm -f "$statusfile" "$logfile"
}

reference_npm_package

redeploy_cf $FUNCTION_NAME main $MEMORY &
PID1=$!

redeploy_cf $FUNCTION_NAME-getcids main_getcids $MEMORY_GETCIDS &
PID2=$!

redeploy_cf $FUNCTION_NAME-bq main_bq &
PID3=$!

redeploy_cf $FUNCTION_NAME-bq-view main_bq_view &
PID4=$!

FAIL=0
wait $PID1 || FAIL=1
wait $PID2 || FAIL=1
wait $PID3 || FAIL=1
wait $PID4 || FAIL=1

clear_npm_package

if [ $FAIL -ne 0 ]; then
  echo -e "${RED}One or more deployments failed.${NC}"
  popd > /dev/null
  exit 1
fi

popd > /dev/null
