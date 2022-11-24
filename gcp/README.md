# Gaarf Cloud Workflow

Here you can find additional components for running Gaarf tool in Google Cloud.

There are the following components provided:
* Cloud Functions (in [functions](functions) folder) - two CFs that you can use for running gaarf with scripts located on GCS
* Cloud Workflow (in [workflow](workflow) folder) - a Cloud Workflow that orchestrates enumerating scripts on GCS and calling CFs with each of them
* Interactive generator that creates all required components and shell scrips for you

So for using Cloud Workflow you need to deploy the workflow and cloud functions. But you can use cloud functions independently if you need.

## Table of content
 - [Installation](#installation)
 - [Cloud Functions](#cloud-functions)
      - [Configuration](#configuration)
      - [Parameters](#functions-parameters)
 - [Cloud Workflow](#cloud-workflow)
      - [Parameters](#workflow-parameters)
      - [Deployment](#deployment)

## Installation

The easiest way to initialize a Google Cloud infrastructure for automated running of your queries is to use our interactive generator [create-gaarf-wf](https://www.npmjs.com/package/create-gaarf-wf).

```shell
npm init gaarf-wf
```
To better understand what the generator creates for you we recommend to read the following sections.

## Cloud Functions

We provide several cloud functions that correlate to cli tools: gaarf and gaarf-bq. Default name of main function is 'gaarf'
and other ones' names based on it with suffixes. You can change the base name during deployment by supplying a `-n` parameter for `deploy.sh` and/or `setup.sh`.

To deploy cloud functions you need to execute `setup.sh` which enables required APIs, sets up permissions and does actual deployment of functions using `deploy.sh`. So later on if something changed in functions' code or Ads API settings you can redeploy by executing `deploy.sh`.

Additionally you can customize region (`-r`) and memory (`-m`).

There are following functions provided:
* gaarf-getcids - fetches and returns customer account ids (CIDs) for further processing
* gaarf - the main function for executing Ads queries, correlates to the `gaarf` cli tool
* gaarf-bq-view - function for creating unified views for all customer-based tables in BQ with data from Ads API
* gaarf-bq - function for executing post-processing queries in BigQuery, correlates to the `gaarf-bq` cli tool

### Configuration
Please note that you need to copy your `google-ads.yaml` somewhere where functions (gaarf and gaarf-getcids) can find it. Preferably onto Cloud Storage. Then you should provide `ads_config_path` query argument or `ADS_CONFIG` environment variable with a path to the Ads config (e.g. gs://myproject/path/to/google-ads.yaml). As a last resort the function will search for `google-ads.yaml` locally, so we can just copy your config to function' folder before deployment.
Alternatively you can provide all configuration values for Ads API via environment variables (you can supply env vars via `--env-vars-file env.yaml` cli option for gcloud tool - you'll need to adapt the `deploy.sh` for this):
* DEVELOPER_TOKEN,
* LOGIN_CUSTOMER_ID
* CLIENT_ID
* CLIENT_SECRET
* REFRESH_TOKEN

### Functions Parameters
> Normally your functions are called by Cloud Workflow and you don't need to bother about their parameters' format.

> Note that actual functions names are chosen on deployment. Here they are mentioned under default ones.

#### gaarf
Body:
* `script` - an object with two fields `query` and `name`, where `query` contains an Ads query text (as alternative to providing it via the `script_path` query argument) and `name` is a name (normally base file name would use) used for target table with data retuned by the script
* `macro` - an object with macros for ads queries

Query string:
* `ads_config_path` - a path to Ads config, e.g. gs://bucket/path/to/google-ads.yaml, if not passed then a local file will be tried to used (should be deployed with the function)
* `script_path` - a path to Ads query file, currently only GCS paths are supported, e.g. gs://bucket/path/to/file, it's mandatory if script/name were not provided via body
* `single_customer` - true/false, pass true to prevent fetching child accounts for a given customer id (`customer_id`)
* `customer_id` - customer id (CID), without '-', can be specified in google-ads.yaml as well, if so then can be omitted
* `bq_project_id` - BigQuery project id for output
* `bq_dataset` - BiQuery dataset id for output
* `bq_dataset_location` - BigQuery dataset location

Returns:
* A map of CID (customer account id) to row counts.

#### gaarf-bq
Body:
* `script` - an object with two fields `query` and `name`, where `query` contains an Ads query text (as alternative to providing it via the `script_path` query argument) and `name` is a name (normally base file name would use) used for target table with data retuned by the script
* `macro` - an object with macros for queries
* `sql` - an object with sql parameters for queries

Query string:
* `project_id` - a GCP project id
* `dataset_location` - BigQuery dataset location

Returns:
* an object with `rowCount` field with a row count if the script returned data, otherwise (the script is a DDL like create view) response is empty

#### gaarf-getcids
Query string:
* `ads_config_path` - a path to Ads config, same as for gaarf
* `customer_id` - customer id (CID), without '-', can be specified in google-ads.yaml as well, if so then can be omitted
* `customer_ids_query` - custom Ads query to filter customer accounts expanded from `customer_id`, same as same-name argument for gaarf cli tool. Query's first column should be a customer id (CID)

Body:
* `customer_ids_query` - same as QueryString's argument as alternative

Returns:
* Array of CIDs (customer account ids) for a given account


#### gaarf-bq-view
Query string:
* `project_id` - a GCP project id
* `dataset` - BiQuery dataset id
* `dataset_location` - BigQuery dataset location
* `script_path` - a path to Ads query file to get table id from (file base name will be used)
* `table` - table id, not needed if `script_path` is specified


## Cloud Workflow

To deploy workflow for gaarf you can use `gcp/workflow/setup.sh` script. It accepts parameters:
* `-n` or `--name` - name of the workflow, by default it will be `gaarf-wf`
* `-l` or `--location` - location region for workflow, be default it will be `us-central1`

As soon as you deployed your workflow (you can use the same workflow for many projects by the way) you can call it directly (via `gcloud workflows run`) or create a Scheduler job for calling it regularly.

For both methods we'll need to provide parameters for workflow.

Executing workflow directly:
```shell
gcloud workflows run gaarf-wf \
  --data='{
  "cloud_function":"gaarf",
  ...
  }'
```

Create a Scheduler job:
```shell
PROJECT_ID=$(gcloud config get-value project 2> /dev/null)
PROJECT_NUMBER=$(gcloud projects describe $PROJECT_ID --format="csv(projectNumber)" | tail -n 1)
# be default Cloud Worflows run under the Compute Engine default service account:
SERVICE_ACCOUNT=$PROJECT_NUMBER-compute@developer.gserviceaccount.com

REGION=us-central1
WORKFLOW_NAME=gaarf-wf
JOB_NAME=$WORKFLOW_NAME

data='{
  \"cloud_function\":\"gaarf\",
  ...
  }'

# you might need to delte job first:
#gcloud scheduler jobs delete $JOB_NAME --location $REGION

# daily at midnight
gcloud scheduler jobs create http $JOB_NAME \
  --schedule="0 8 * * *" \
  --uri="https://workflowexecutions.googleapis.com/v1/projects/$PROJECT_ID/locations/$REGION/workflows/$WORKFLOW_NAME/executions" \
  --location=$REGION \
  --message-body="{\"argument\": \"$data\"}" \
  --oauth-service-account-email="$SERVICE_ACCOUNT"
```

Please notice the escaping of quotes for job's argument.

### Workflow Parameters

* `cloud_function` - name for gaarf cloud function (by default `gaarf` but could be customized during deployment)
* `gcs_bucket` - GCS bucket name where queries are stored, by default your GCP project id
* `ads_queries_path` - relative GCS path for ads queries, e.g. "gaarf/ads-queries/" (then workflow will fetch all files from gs://your_bucket/gaarf/adds-queries/*) (required)
* `bq_queries_path` - relative GCS path for BigQuery queries, e.g. "gaarf/bq-queries" (required)
* `dataset` - BigQuery dataset id for writing results of ads queries (required)
* `bq_dataset_location` - BigQuery dataset location, e.g. "europe", by default "us"
* `cid` - Ads customer id, can be either a MCC or child account, without dashes (required)
* `ads_config_path` - a full GCS path to your google-ads.yaml config, e.g. "gs://MYPROJECT/path/to/google-ads.yaml" (required)
* `ads_macro` - an object with macro for Ads queries, see the root [README](../README.md)
* `bq_macro` - an object with macro for BigQuery queries, see the root [README](../README.md)
* `bq_sql` - an object with sql parameters for BigQuery queries


### Deployment
For deployment we recommend using our interactive generator [create-gaarf-wf](https://www.npmjs.com/package/create-gaarf-wf).
It greatly simplifies deployment. As a result of running it creates a bunch of shell scripts that reviewed here.

Common deployment scripts are:
* deploy-scripts.sh - copy queries and google-ads.yaml to GCS
* deploy-wf.sh - deploy cloud functions and cloud workflow
* schedule-wf.sh - create a schedule job with parameters

For deployment of all components you can use the [setup.sh](setup.sh) script with an argument with a name of your project.
Providing you supplied a name "myproject" the script will deploy functions with base name "myproject" ("myproject-bq", "myproject-getcids", "myproject-bq-view") and a workflow "myproject-wf". You can always customize names and other settings for components by adjusting `setup.sh` scripts in components' folders.


#### deploy-scripts.sh
```shell
PROJECT_ID=$(gcloud config get-value project 2> /dev/null)
if [[ "PROJECT_ID" = '' ]]
then
  echo -e "${RED}There is no activate project. Activate one with gcloud config set project${NC}"
  exit
fi

GCS_BUCKET=gs://${PROJECT_ID}
GCS_BASE_PATH=$GCS_BUCKET/gaarf
# create a GCS bucket (one time):
#gsutil mb -b on $GCS_BUCKET

gsutil -m cp google-ads.yaml $GCS_BASE_PATH/google-ads.yaml

gsutil rm -r $GCS_BASE_PATH/ads-queries
gsutil -m cp -R ./ads-queries/* $GCS_BASE_PATH/ads-queries/

gsutil rm -r $GCS_BASE_PATH/bq-queries
gsutil -m cp -R ./bq-queries/* $GCS_BASE_PATH/bq-queries/
```

#### deploy-wf.sh
```shell
cd ./ads-api-fetcher/gcp/functions
./setup.sh -n gaarf
cd ../workflow
./setup.sh -n gaarf-wf
```

#### schedule-wf.sh
```shell
PROJECT_ID=$(gcloud config get-value project 2> /dev/null)
PROJECT_NUMBER=$(gcloud projects describe $PROJECT_ID --format="csv(projectNumber)" | tail -n 1)
# be default Cloud Worflows run under the Compute Engine default service account:
SERVICE_ACCOUNT=$PROJECT_NUMBER-compute@developer.gserviceaccount.com

REGION=us-central1
WORKFLOW_NAME=gaarf-wf
JOB_NAME=$WORKFLOW_NAME

data='{
  \"cloud_function\":\"gaarf\",
  \"cloud_function_bq\":\"gaarf-bq\",
  \"gcs_bucket\":\"'"$PROJECT_ID"'\",
  \"ads_queries_path\":\"gaarf/ads-queries/\",
  \"bq_queries_path\":\"gaarf/bq-queries/\",
  \"dataset\":\"gaarf_ads\",
  \"cid\":\"3717154796\",
  \"ads_config_path\":\"gs://'"$PROJECT_ID"'/gaarf/google-ads.yaml\",
  \"bq_dataset_location\": \"europe\",
  \"ads_macro\":{ \"start_date\": \"2022-01-01\", \"end_date\": \":YYYYMMDD\" },
  \"bq_macro\": {\"ads_ds\": \"gaarf_ads\", \"ds_dst\": \"gaarf\"},
  \"bq_sql\": {}
  }'

gcloud scheduler jobs delete $JOB_NAME --location $REGION

# daily at midnight
gcloud scheduler jobs create http $JOB_NAME \
  --schedule="0 0 * * *" \
  --uri="https://workflowexecutions.googleapis.com/v1/projects/$PROJECT_ID/locations/$REGION/workflows/$WORKFLOW_NAME/executions" \
  --location=$REGION \
  --message-body="{\"argument\": \"$data\"}" \
  --oauth-service-account-email="$SERVICE_ACCOUNT"

#  --time-zone="TIME_ZONE"
# for time zone use names from: https://en.wikipedia.org/wiki/List_of_tz_database_time_zones
# e.g. "Etc/GMT+3"
```
