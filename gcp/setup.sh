#!/bin/bash
#
# Copyright 2024 Google LLC
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
SETTING_FILE="./settings.ini"
SCRIPT_PATH=$(readlink -f "$0" | xargs dirname)
SETTING_FILE="${SCRIPT_PATH}/settings.ini"

# changing the cwd to the script's contining folder so all pathes inside can be local to it
# (important as the script can be called via absolute path and as a nested path)
pushd $SCRIPT_PATH > /dev/null

args=()
started=0
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
  gcloud services enable compute.googleapis.com
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
  gcloud services enable googleads.googleapis.com
}


set_iam_permissions() {
  # Permissions for Workflow:
  # grant the default service account with read permissions on Cloud Storage
  gcloud projects add-iam-policy-binding $PROJECT_ID --member=serviceAccount:$SERVICE_ACCOUNT --role=roles/storage.objectViewer
  # grant the default service account with execute permissions on Cloud Functions gen2 (CF)
  gcloud projects add-iam-policy-binding $PROJECT_ID --member=serviceAccount:$SERVICE_ACCOUNT --role=roles/run.invoker
  # grant the default service account with write permissions on Cloud Logging
  gcloud projects add-iam-policy-binding $PROJECT_ID --member=serviceAccount:$SERVICE_ACCOUNT --role=roles/logging.logWriter
  # grant the default service account with view permissions (cloudfunctions.functions.get) on CF
  gcloud projects add-iam-policy-binding $PROJECT_ID --member=serviceAccount:$SERVICE_ACCOUNT --role=roles/cloudfunctions.viewer
  # grant the default service account with execute permissions on Cloud Workflow (this is for Scheduler which also runs under the default GCE SA)
  gcloud projects add-iam-policy-binding $PROJECT_ID --member=serviceAccount:$SERVICE_ACCOUNT --role roles/workflows.invoker
  # grant the default service account with permissions on to publish pubsub messages
  gcloud projects add-iam-policy-binding $PROJECT_ID --member=serviceAccount:$SERVICE_ACCOUNT --role roles/pubsub.admin #topics.publish
  # Permissions for Cloud Functions:
  # Required for calling Cloud Function Gen2
  gcloud projects add-iam-policy-binding $PROJECT_ID --member=serviceAccount:$SERVICE_ACCOUNT --role=roles/cloudfunctions.serviceAgent
  # Grant the default service account with read permissions on Cloud Storage
  gcloud projects add-iam-policy-binding $PROJECT_ID --member=serviceAccount:$SERVICE_ACCOUNT --role=roles/storage.objectViewer
  # Grant the default service account with admin permissions in BigQuery
  gcloud projects add-iam-policy-binding $PROJECT_ID --member=serviceAccount:$SERVICE_ACCOUNT --role=roles/bigquery.admin
  # For deploying Gen2 CF 'artifactregistry.repositories.list' and 'artifactregistry.repositories.get' permissions are required
  gcloud projects add-iam-policy-binding $PROJECT_ID --member=serviceAccount:$SERVICE_ACCOUNT --role=roles/artifactregistry.repoAdmin
}


create_wf_completion_topic() {
  # create completion topic
  TOPIC=$(git config -f $SETTING_FILE workflows.topic || echo "gaarf_wf_completed")
  TOPIC_EXISTS=$(gcloud pubsub topics list --filter="name.scope(topic):'$TOPIC'" --format="get(name)")
  if [[ ! -n $TOPIC_EXISTS ]]; then
    gcloud pubsub topics create $TOPIC
  fi
}


deploy_functions() {
  CF_MEMORY=$(git config -f $SETTING_FILE functions.memory)
  if [[ -n $CF_MEMORY ]]; then
    CF_MEMORY="--memory $CF_MEMORY"
  fi

  ./functions/deploy.sh --name $NAME $CF_MEMORY --region $REGION --service-account $SERVICE_ACCOUNT
}


enable_debug_logging() {
  gcloud functions deploy $NAME --update-env-vars LOG_LEVEL=debug --region $REGION
  gcloud functions deploy $NAME-getcids --update-env-vars LOG_LEVEL=debug --region $REGION 
  gcloud functions deploy $NAME-bq --update-env-vars LOG_LEVEL=debug --region $REGION 
  gcloud functions deploy $NAME-bq-view --update-env-vars LOG_LEVEL=debug --region $REGION
}


deploy_wf() {
  ./workflow/deploy.sh --name $WORKFLOW_NAME --region $REGION --service-account $SERVICE_ACCOUNT
}


_get_data() {
  # we'll get a path to data json file with job's args from --data cli argument
  local DATA_FILE
  for ((i=1; i<=$#; i++)); do
    if [ ${!i} = "--data" ]; then
      ((i++))
      DATA_FILE=${!i}
      break
    fi
  done
  data=$(<${DATA_FILE})
  echo $data
}

schedule_wf() {
  JOB_NAME=$WORKFLOW_NAME
  SCHEDULE=$(git config -f $SETTING_FILE scheduler.schedule || echo "0 0 * * *")
  SCHEDULE_TZ=$(git config -f $SETTING_FILE scheduler.schedule-timezone|| echo "Etc/UTC")

  data=$(_get_data $@)

  JOB_EXISTS=$(gcloud scheduler jobs list --location=$REGION --format="value(ID)" --filter="ID:'$JOB_NAME'")
  if [[ -n $JOB_EXISTS ]]; then
    gcloud scheduler jobs delete $JOB_NAME --location $REGION --quiet
  fi
  
  # run the job daily at midnight
  gcloud scheduler jobs create http $JOB_NAME \
    --schedule="$SCHEDULE" \
    --uri="https://workflowexecutions.googleapis.com/v1/projects/$PROJECT_ID/locations/$REGION/workflows/$WORKFLOW_NAME/executions"   --location=$REGION \
    --message-body="{\"argument\": \"$data\"}" \
    --oauth-service-account-email="$SERVICE_ACCOUNT" \
    --time-zone="$SCHEDULE_TZ"
}


run_wf() {
  data=$(_get_data $@)
  state=$(gcloud workflows run $WORKFLOW_NAME --location=$REGION --data="$data" --format="get(state)")
  if [[ $state == 'FAILED' ]]; then
    echo 'Execution failed'
    exit 1
  else
    echo 'Execution succeeded'
    exit 0
  fi
}


delete_all() {
  echo "Deleting Functions"
  # delete functions
  gcloud functions delete $NAME --region $REGION --quiet 2> /dev/null
  gcloud functions delete $NAME-getcids --region $REGION --quiet 2> /dev/null
  gcloud functions delete $NAME-bq --region $REGION --quiet 2> /dev/null
  gcloud functions delete $NAME-bq-view --region $REGION --quiet 2> /dev/null
  # delete workflows
  echo "Deleting Worflows"
  gcloud workflows delete $WORKFLOW_NAME --location=$REGION --quiet 2> /dev/null
  gcloud workflows delete $WORKFLOW_NAME-ads --location=$REGION --quiet 2> /dev/null
  # delete schedule job
  echo "Deleting Scheduler Job"
  gcloud scheduler jobs delete $WORKFLOW_NAME --location $REGION --quiet 2> /dev/null
}


deploy_all() {
  enable_apis
  set_iam_permissions
  deploy_functions
  create_wf_completion_topic
  deploy_wf
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
        echo "Breaking script as command '$i' failed"
        exit $exitcode
      fi
    else
      echo -e "\033[0;31mFunction '$i' does not exist.\033[0m"
    fi
  done
fi

popd > /dev/null
