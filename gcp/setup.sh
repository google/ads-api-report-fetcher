#!/bin/bash
#
# Copyright 2025 Google LLC
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
CYAN='\033[0;36m'
RED='\033[0;31m'
NC='\033[0m'

SETTING_FILE="./settings.ini"
SCRIPT_PATH=$(readlink -f "$0" | xargs dirname)
SETTING_FILE="${SCRIPT_PATH}/settings.ini"

# changing the cwd to the script's containing folder so all paths inside can be local to it
# (important as the script can be called via absolute path and as a nested path)
pushd $SCRIPT_PATH > /dev/null

args=()
started=0
# pack all arguments into args array (except --settings)
for ((i=1; i<=$#; i++)); do
  if [[ ${!i} == --* ]]; then started=1; fi
  if [ ${!i} = "--settings" ]; then
    ((i++))
    SETTING_FILE=${!i}
  elif [ $started = 1 ]; then
    args+=("${!i}")
  fi
done
echo "Using settings from $SETTING_FILE"

PROJECT_ID=$(gcloud config get-value project 2> /dev/null)
PROJECT_NUMBER=$(gcloud projects describe $PROJECT_ID --format="csv(projectNumber)" | tail -n 1)
# be default Cloud Worflows run under the Compute Engine default service account:
SERVICE_ACCOUNT=$(git config -f $SETTING_FILE common.service-account || echo $PROJECT_NUMBER-compute@developer.gserviceaccount.com)
NAME=$(git config -f $SETTING_FILE common.name || echo "gaarf")
REGION=$(git config -f $SETTING_FILE common.region || echo "europe-west1")
WORKFLOW_NAME=$NAME-wf

enable_apis() {
  # Track if compute API was newly enabled
  local compute_enabled=false
  # Check if compute API is already enabled
  if ! gcloud services list --enabled --filter="name:compute.googleapis.com" | grep -q compute.googleapis.com; then
    echo "Enabling Compute Engine API..."
    gcloud services enable compute.googleapis.com
    compute_enabled=true
  fi

  # apis for workflow
  gcloud services enable workflows.googleapis.com
  gcloud services enable workflowexecutions.googleapis.com
  gcloud services enable cloudscheduler.googleapis.com
  # apis for cloud functions
  gcloud services enable artifactregistry.googleapis.com # required for Gen2 GCF
  gcloud services enable run.googleapis.com # required for Gen2 GCF
  gcloud services enable cloudresourcemanager.googleapis.com
  gcloud services enable iamcredentials.googleapis.com
  gcloud services enable cloudbuild.googleapis.com
  gcloud services enable bigquery.googleapis.com
  gcloud services enable cloudfunctions.googleapis.com
  gcloud services enable secretmanager.googleapis.com
  gcloud services enable googleads.googleapis.com

  if [ "$compute_enabled" = "true" ]; then
    wait_for_service_account
    return $?
  fi
}

wait_for_service_account() {
  local max_attempts=15
  local attempt=1

  while [ $attempt -le $max_attempts ]; do
    if gcloud iam service-accounts describe $SERVICE_ACCOUNT &>/dev/null; then
      echo -e "${CYAN}Service account $SERVICE_ACCOUNT is ready!${NC}"
      return 0
    else
      echo "Attempt $attempt: Service account $SERVICE_ACCOUNT not ready yet, waiting..."
      sleep 10
      attempt=$((attempt+1))
    fi
  done

  echo -e "${RED}Service account didn't become available after multiple attempts.${NC}"
  return 1
}

check_service_account() {
  echo "Checking if service account $SERVICE_ACCOUNT exists..."
  if ! gcloud iam service-accounts describe $SERVICE_ACCOUNT &>/dev/null; then
    echo "Service account $SERVICE_ACCOUNT doesn't exist or you don't have permission to access it."
    exit 1
  fi
}

set_iam_permissions() {
  local disable_grants
  disable_grants=$(_get_arg_value "--disable-grants" "$@")
  if [[ "$disable_grants" == "true" ]]; then
    echo -e "${CYAN}Skipping setting up IAM permissions...${NC}"
    return
  fi

  echo -e "${CYAN}Setting up IAM permissions...${NC}"
  declare -ar ROLES=(
    # Permissions for Workflow:
    # Cloud Storage
    roles/storage.admin
    # Cloud Functions gen2 (CF)
    roles/run.invoker
    # Cloud Logging
    roles/logging.logWriter
    # view permissions (cloudfunctions.functions.get) on CF
    roles/cloudfunctions.viewer
    #execute permissions on Cloud Workflow (this is for Scheduler which also runs under the default GCE SA)
    roles/workflows.invoker
    # to publish pubsub messages
    roles/pubsub.admin
    roles/cloudfunctions.serviceAgent
    roles/bigquery.admin
    # For deploying Gen2 CF 'artifactregistry.repositories.list' and 'artifactregistry.repositories.get' permissions are required
    roles/artifactregistry.repoAdmin
    roles/iam.serviceAccountUser
    roles/secretmanager.secretAccessor
  )
  for role in "${ROLES[@]}"
  do
    gcloud projects add-iam-policy-binding $PROJECT_ID \
      --member=serviceAccount:$SERVICE_ACCOUNT \
      --role=$role \
      --condition=None
  done
}


create_gcs() {
  echo -e "${CYAN}Creating GCS...${NC}"
  if ! gcloud storage ls gs://${PROJECT_ID} > /dev/null 2> /dev/null; then
    gcloud storage buckets create gs://${PROJECT_ID} --location ${GCS_REGION} --uniform-bucket-level-access
  fi
}


create_wf_completion_topic() {
  # create completion topic
  TOPIC=$(git config -f $SETTING_FILE workflows.topic || echo "gaarf_wf_completed")
  TOPIC_EXISTS=$(gcloud pubsub topics list --filter="name.scope(topic):'$TOPIC'" --format="get(name)")
  if [[ ! -n $TOPIC_EXISTS ]]; then
    gcloud pubsub topics create $TOPIC
  fi
}


create_secret() {
  local SECRET_NAME
  SECRET_NAME=$(_get_arg_value "--secret" "$@")
  local SECRET_VALUE
  SECRET_VALUE=$(_get_arg_value "--value" "$@")
  if [[ ! -n $SECRET_NAME ]]; then
    echo -e "${RED}Please provide a secret name via --secret argument${NC}"
    return 1
  fi
  if [[ ! -n $SECRET_VALUE ]]; then
    echo -e "${RED}Please provide a secret value via --value argument${NC}"
    return 1
  fi
  if gcloud secrets describe $SECRET_NAME >/dev/null 2>&1; then
      # Secret exists - add new version
      echo -n "$SECRET_VALUE" | gcloud secrets versions add $SECRET_NAME --data-file=-
  else
      # Secret doesn't exist - create new
      echo -n "$SECRET_VALUE" | gcloud secrets create $SECRET_NAME --data-file=-
  fi
}

deploy_functions() {
  USE_SM=$(git config -f $SETTING_FILE functions.use-secret-manager || echo false)
  if [[ "$USE_SM" == "true" ]]; then
    USE_SM="--use-secret-manager"
  else
    USE_SM=""
  fi
  CF_MEMORY=$(git config -f $SETTING_FILE functions.memory)
  if [[ -n $CF_MEMORY ]]; then
    CF_MEMORY="--memory $CF_MEMORY"
  fi

  MEMORY_GETCIDS=$(git config -f $SETTING_FILE functions.memory-getcids)
  if [[ -n $MEMORY_GETCIDS ]]; then
    MEMORY_GETCIDS="--memory-getcids $MEMORY_GETCIDS"
  fi
  ./functions/deploy.sh --name $NAME $CF_MEMORY $MEMORY_GETCIDS --region $REGION --service-account $SERVICE_ACCOUNT $USE_SM
  return $?
}


enable_debug_logging() {
  gcloud functions deploy $NAME --update-env-vars LOG_LEVEL=debug --region $REGION --source ./functions --runtime=nodejs22
  gcloud functions deploy $NAME-getcids --update-env-vars LOG_LEVEL=debug --region $REGION --source ./functions --runtime=nodejs22
  gcloud functions deploy $NAME-bq --update-env-vars LOG_LEVEL=debug --region $REGION --source ./functions --runtime=nodejs22
  gcloud functions deploy $NAME-bq-view --update-env-vars LOG_LEVEL=debug --region $REGION --source ./functions --runtime=nodejs22
}


deploy_wf() {
  ./workflow/deploy.sh --name $WORKFLOW_NAME --region $REGION --service-account $SERVICE_ACCOUNT
  return $?
}

_get_arg_value() {
  local arg_name=$1
  shift
  for ((i=1; i<=$#; i++)); do
    if [ ${!i} = "$arg_name" ]; then
      # Check if this is the last argument or if next argument starts with - or --
      ((next=i+1))
      if [ $next -gt $# ] || [[ "${!next}" == -* ]]; then
        # This is a flag without value
        echo "true"
        return 0
      else
        # This is a value argument
        echo ${!next}
        return 0
      fi
    fi
  done
  return 1
}

_get_data() {
  # we'll get a path to data json file with job's args from --data cli argument
  local DATA_FILE
  DATA_FILE=$(_get_arg_value "--data" "$@")
  data=$(<${DATA_FILE})
  echo $data
}

schedule_wf() {
  JOB_NAME=$WORKFLOW_NAME
  SCHEDULE=$(git config -f $SETTING_FILE scheduler.schedule || echo "0 0 * * *")
  SCHEDULE_TZ=$(git config -f $SETTING_FILE scheduler.schedule-timezone|| echo "Etc/UTC")

  data=$(_get_data $@)
  escaped_data="$(echo "$data" | sed 's/"/\\"/g')"

  JOB_EXISTS=$(gcloud scheduler jobs list --location=$REGION --format="value(ID)" --filter="ID:'$JOB_NAME'")
  if [[ -n $JOB_EXISTS ]]; then
    gcloud scheduler jobs delete $JOB_NAME --location $REGION --quiet
  fi

  # run the job daily at midnight
  gcloud scheduler jobs create http $JOB_NAME \
    --schedule="$SCHEDULE" \
    --uri="https://workflowexecutions.googleapis.com/v1/projects/$PROJECT_ID/locations/$REGION/workflows/$WORKFLOW_NAME/executions"   --location=$REGION \
    --message-body="{\"argument\": \"$escaped_data\"}" \
    --oauth-service-account-email="$SERVICE_ACCOUNT" \
    --time-zone="$SCHEDULE_TZ"
  return $?
}


run_wf() {
  data=$(_get_data $@)
  state=$(gcloud workflows run $WORKFLOW_NAME --location=$REGION --data="$data" --format="get(state)")
  if [[ $state == 'FAILED' ]]; then
    echo -e "${RED}Execution failed${NC}"
    exit 1
  else
    echo 'Execution succeeded'
    exit 0
  fi
}

run_job() {
  gcloud scheduler jobs run $WORKFLOW_NAME --location=$REGION
}

delete_all() {
  echo "Deleting Functions"
  # delete functions
  gcloud functions delete $NAME --region $REGION --quiet 2> /dev/null
  gcloud functions delete $NAME-getcids --region $REGION --quiet 2> /dev/null
  gcloud functions delete $NAME-bq --region $REGION --quiet 2> /dev/null
  gcloud functions delete $NAME-bq-view --region $REGION --quiet 2> /dev/null
  # delete workflows
  echo "Deleting Workflows"
  gcloud workflows delete $WORKFLOW_NAME --location=$REGION --quiet 2> /dev/null
  gcloud workflows delete $WORKFLOW_NAME-ads --location=$REGION --quiet 2> /dev/null
  # delete schedule job
  echo "Deleting Scheduler Job"
  gcloud scheduler jobs delete $WORKFLOW_NAME --location $REGION --quiet 2> /dev/null
}


deploy_all() {
  enable_apis || return $?
  set_iam_permissions $@ || return $?
  deploy_functions || return $?
  create_wf_completion_topic || return $?
  deploy_wf || return $?
}


_list_functions() {
  # list all functions in this file not starting with "_"
  declare -F | awk '{print $3}' | grep -v "^_"
}


if [[ $# -eq 0 ]]; then
  echo "USAGE: $0 target1 target2 ... [--settings /path/to/settings.ini]"
  echo "  where supported targets:"
  _list_functions
else
  for i in "$@"; do
    if [[ $i == --* ]]; then
      break
    fi
    if declare -F "$i" > /dev/null; then
      "$i" ${args[@]}
      exitcode=$?
      if [ $exitcode -ne 0 ]; then
        echo -e "${RED}Breaking script as command '$i' failed${NC}"
        exit $exitcode
      fi
    else
      echo -e "${RED}Function '$i' does not exist.${NC}"
      exit -1
    fi
  done
fi

popd > /dev/null
