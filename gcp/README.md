# Gaarf Cloud Workflow

Here you can find additional components for running Gaarf tool in Google Cloud.

There are the following components provided:
* Cloud Functions (in [functions](functions) folder) - CFs that you can use for running gaarf with queries located on GCS
* Cloud Workflow (in [workflow](workflow) folder) - a Cloud Workflow that orchestrates enumerating queries on GCS and calling CFs with each of them
* Interactive generator that creates all required components and shell scrips for you

So for using Cloud Workflow you need to deploy the workflow and cloud functions. But you can use cloud functions independently if you need.

## Table of content
 - [Installation](#installation)
 - [Cloud Workflow](#cloud-workflow)
      - [Parameters](#workflow-parameters)
 - [Cloud Functions](#cloud-functions)
      - [Parameters](#functions-parameters)
 - [Deployment](#deployment)

## Installation

The easiest way to initialize a Google Cloud infrastructure for automated running of your queries is to use our interactive generator [create-gaarf-wf](https://www.npmjs.com/package/create-gaarf-wf).

```shell
npm init gaarf-wf@latest
```
To better understand what the generator creates for you we recommend to read the following sections.

>NOTE: if you're installing on Google Cloud Shell then it's worth cleaning npm cache first to make sure you're using the latest versions:
>`rm -rf ~/.npm/`


## Cloud Workflow

To deploy workflow for gaarf you can use `gcp/workflow/setup.sh` script. It accepts parameters:
* `-n` or `--name` - name of the workflow, by default it will be `gaarf-wf`
* `-l` or `--location` - location region for workflow, be default it will be `us-central1`

As soon as you deployed your workflow (you can use the same workflow for many projects by the way) you can call it directly (via `gcloud workflows run`) or create a Scheduler job for calling it regularly.

For both methods you'll need to provide parameters for workflow.

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

# you might need to delete job first:
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
* `bq_dataset_location` - BigQuery dataset location, e.g. "europe", by default "us" (optional)
* `cid` - Ads customer id, can be either a MCC or child account, without dashes (required), or a list of CIDs comma separated (required)
* `customer_ids_query` - a path to a file with GAQL query that refines for which accounts to execute scripts (optional)
* `customer_ids_batchsize` - a batch size for customer ids (cids), if not specified accounts will be processed by 1000 accounts (see gaarf-getids CF)
* `customer_ids_offset` - an offset in resulting list of accounts if you need to implemented an external batching - i.e. execute workflow only for a subset of accounts from specified seed account(s). It differs from internal batching where accounts processed by batches to workaround the maximum steps limitation of Cloud Workflows (100K runtime steps).
* `ads_config_path` - a full GCS path to your google-ads.yaml config, e.g. "gs://MYPROJECT/path/to/google-ads.yaml" (required)
* `ads_macro` - an object with macro for Ads queries, see the root [README](../README.md) (optional)
* `bq_macro` - an object with macro for BigQuery queries, see the root [README](../README.md) (optional)
* `bq_sql` - an object with sql parameters for BigQuery queries, see the root [README](../README.md) (optional)
* `bq_writer_options` - additional options (as object) for BqWriter, see the root [README](../README.md) (optional)
* `concurrency_limit` - a custom concurrency level to use instead of the default one (20) - it's a number of concurrent threads for parallel loop over scripts and over accounts. For example, if you specify 5 then there will be 5 parallel executions of scripts for each of them there will be 5 parallel running CF executions, so in total in a moment there will be 5*5=25 parallel executions of the CF. (optional)
* `workflow_ads_id` - a workflow id for child Ads workflow, by default it's the parent name with the '-ads' suffix (optional)
* `disable_strict_views` - an option to pass to gaarf-bq-view CF to disable adding the WHERE condition with a list of accounts for views (optional)


## Cloud Functions

We provide several cloud functions that correlate to cli tools: gaarf, gaarf-getcids, gaarf-bq and gaarf-bq-view. Default name of main function is 'gaarf' and other ones' names based on it with suffixes. You can change the base name during deployment by supplying a `--name`/`-n` parameter for `deploy.sh` and/or `setup.sh`.

To deploy cloud functions you need to execute `setup.sh` which enables required APIs, sets up permissions and does actual deployment of functions using `deploy.sh`. If later on something changes in functions' code or Ads API settings you can redeploy by executing `deploy.sh`.

Additionally you can customize region (`-r`) and memory (`-m`).

There are following functions provided:
* gaarf-getcids - fetches and returns customer account ids (CIDs) for further processing
* gaarf - the main function for executing Ads queries, correlates to the `gaarf` cli tool
* gaarf-bq-view - function for creating unified views for all customer-based tables in BQ with data from Ads API
* gaarf-bq - function for executing post-processing queries in BigQuery, correlates to the `gaarf-bq` cli tool


### Functions Parameters
Please note that you need to copy your `google-ads.yaml` somewhere where functions (gaarf and gaarf-getcids) can find it. Preferably onto Cloud Storage. Then you should provide `ads_config_path` query argument or `ADS_CONFIG` environment variable with a path to the Ads config (e.g. gs://myproject/path/to/google-ads.yaml). As a last resort the function will search for `google-ads.yaml` locally, so we can just copy your config to function' folder before deployment.  
Alternatively you can provide all configuration values for Ads API via environment variables (you can supply env vars via `--env-vars-file env.yaml` cli option for gcloud tool - you'll need to adapt the `deploy.sh` for this):
* DEVELOPER_TOKEN,
* LOGIN_CUSTOMER_ID
* CLIENT_ID
* CLIENT_SECRET
* REFRESH_TOKEN

> Normally your functions are called by Cloud Workflows workflow and you don't need to bother about their parameters'.

> Note that actual functions names are chosen on deployment. Here they are mentioned under the default ones.

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
* `customer_ids_batchsize` - a size of batches into which account ids list will be split. 
* `customer_ids_offset` - an offset in the customer ids list resulted from the seed CIDs and optional query in `customer_ids_query`, it allows to implement an external batching. 
* `flatten` - flatten the list of customer ids. If `customer_ids_offset` is provided then the list will be a subset of CIDs otherwise it will be the whole list of accounts, ignoring batching (regadless of the customer_ids_batchsize's value)
Body:
* `customer_ids_query` - same as QueryString's argument as alternative

Returns:
if no `flatten` specifiedd then the CF returns an object:
* accounts - array of arrays with CIDs - i.e. CIDs grouped in batches, but if customer_ids_offset provided then there will be only one batch
  e.g. [1, 2, ..., 10_000] => [ [1, 2, ..., 5_000], [5_001, 10_000] ]
* batchCount - number of batches (items in root array)
* batchSize - number of ids per batch (items in nested arrays)

If `flatten` provided then respose is simply an array with CIDs


#### gaarf-bq-view
Query string:
* `project_id` - a GCP project id
* `dataset` - BiQuery dataset id
* `dataset_location` - BigQuery dataset location
* `script_path` - a path to Ads query file to get table id from (file base name will be used)
* `table` - table id, not needed if `script_path` is specified


## Deployment
For deployment we recommend using our interactive generator [create-gaarf-wf](https://www.npmjs.com/package/create-gaarf-wf).
It greatly simplifies deployment. As a result it creates a bunch of shell scripts that reviewed here.

Common deployment scripts are:
* `deploy-queries.sh` - copy queries and google-ads.yaml to GCS (where they will be picked up by the workflow during execution)
* `deploy-wf.sh` - deploy Cloud functions and Cloud workflow
* `schedule-wf.sh` - create a schedule job with parameters
* `run-wf.sh` - execute workflow synchronously with the same parameters as the job

For deployment of all components you can use the [setup.sh](setup.sh) script with an argument with a name of your project.
Providing you supplied a name "myproject" the script will deploy functions with base name "myproject" ("myproject-bq", "myproject-getcids", "myproject-bq-view") and a workflow "myproject-wf". You can always customize names and other settings for components by adjusting `setup.sh` scripts in components' folders.

