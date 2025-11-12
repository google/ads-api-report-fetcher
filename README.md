Google Ads API Report Fetcher (gaarf)

[![npm](https://img.shields.io/npm/v/google-ads-api-report-fetcher)](https://www.npmjs.com/package/google-ads-api-report-fetcher)
[![Downloads npm](https://img.shields.io/npm/dw/google-ads-api-report-fetcher?logo=npm)](https://www.npmjs.com/package/google-ads-api-report-fetcher)
[![PyPI](https://img.shields.io/pypi/v/google-ads-api-report-fetcher?logo=pypi&logoColor=white&style=flat-square)](https://pypi.org/project/google-ads-api-report-fetcher/)
[![Downloads PyPI](https://img.shields.io/pypi/dw/google-ads-api-report-fetcher?logo=pypi)](https://pypi.org/project/google-ads-api-report-fetcher/)
[![GitHub Workflow CI](https://img.shields.io/github/actions/workflow/status/google/ads-api-report-fetcher/pytest.yaml?branch=main&label=pytest&logo=python&logoColor=white&style=flat-square)](https://github.com/google/ads-api-report-fetcher/actions/workflows/pytest.yaml?branch=main)


## Table of content

 - [Overview](#overview)
 - [Getting started](#getting-started)
 - [Writing Queries](#writing-queries)
 - [Running gaarf](#running-gaarf)
     - [Options](#options)
     - [Postprocessing](#postprocessing)
 - [Expressions and Macros](#expressions-and-macros)
     - [Dynamic dates](#dynamic-dates)
 - [Docker](#docker)
 - [Gaarf Cloud Workflow](#gaarf-cloud-workflow)
 - [Differencies in Python and NodeJS versions](#differencies-in-python-and-nodejs-versions)


## Overview

Google Ads API Report Fetcher (`gaarf`) simplifies running [Google Ads API Reports](https://developers.google.com/google-ads/api/fields/v9/overview)
by separating logic of writing [GAQL](https://developers.google.com/google-ads/api/docs/query/overview)-like query from executing it and saving results.\
The library allows you to define GAQL queries alongside aliases and custom extractors and specify where the results of such query should be stored.
You can find example queries in [examples](examples) folder.
Based on such a query the library constructs the correct GAQL query, automatically extract all necessary fields from schema
and transform them into a structure suitable for writing data.


## Getting started

Google Ads API Report Fetcher has two versions - Python and Node.js.
Please explore the relevant section to install and run the tool:

* [Getting started with gaarf in Python](py/README.md)
* [Getting started with gaarf in Node.js](js/README.md)

Both versions have similar command line arguments and query syntax.


## Writing Queries

Google Ads API Report Fetcher provides an extended syntax on writing GAQL queries.\
Please refer to [How to write queries](docs/how-to-write-queries.md) section to learn the query syntax.


## Running gaarf

If `gaarf` is installed globally you can run it with the following command.

```shell
gaarf <queries> [options]
```

### Options
The required positional arguments are a list of files or a text that contain Ads queries (GAQL).
On *nix OSes you can use a glob pattern, e.g. `./ads-queries/**/*.sql`.
A file path can be not only a local path but also a GCS file (gs://).

> If you run the tool on a *nix OS then your shell (like zsh/bash) probably
> supports file names expansion (see [bash](https://www.gnu.org/software/bash/manual/html_node/Filename-Expansion.html),
> [zsh](https://zsh.sourceforge.io/Doc/Release/Expansion.html), 14.8 Filename Generation).
> And so it does expansion of glob pattern (file mask) into a list of files.

Currently only the NodeJS version supports wildcards natively (without glob expansion by the OS).
* `gaarf ads-queries/*.sql` - using OS wildcards expansion
* `gaarf "ads-queries/*.sql"` - using native wildcards (i.e. passing wildcard into the tool)

Instead of passing files you can pass commands (currently supported only by NodeJs versions):
* `validate` - validate Ads credentials (supplied via `--ads-config`).
* `account-tree` - print an account structure, given an account id (via `--account`) it outputs all children account as a hierarchy.

Options:
* `ads-config` - a path to yaml file with config for Google Ads,
               by default assuming 'google-ads.yaml' in the current folder
* `account` - Ads account id, aka customer id, it can contain multiple ids separated with comma, also can be specified in google-ads.yaml as 'customer-id' (as string or list)
* `input` - input type - where queries are coming from. Supports the following values:
  * `file` - (default) local or remote (GCS, S3, Azure, etc.) files
  * `console` - data are read from standard input
* `output` - output type, supports the following values:
  * `bq` or `bigquery` - write data to BigQuery
  * `console` - write data to standard output
  * `csv` - write data to CSV files
  * `json` - writes data to JSON files
  * `sqldb` - writes data to a database supported by SQL Alchemy (Python only)
  * `sheet` - writes data to a Google Sheets (Python only)
* `loglevel` - logging level: 'debug', 'verbose', 'info', 'warn', 'error'
* `skip-constants` - do not execute scripts for constant resources (e.g. language_constant) (*NodeJS version only*)
* `dump-query` - outputs query text to console after resolving all macros and expressions (*NodeJS version only*), loglevel should be not less than 'verbose'
* `customer-ids-query` - GAQL query that specifies for which accounts you need to run `gaarf`. Must contains **customer.id** as the first column in SELECT statement with all the filtering logic going to WHERE statement.
  `account` argument must be a MCC account id (or accounts) in this case.

  >Example usage: `gaarf <queries> --account=123456 --customer-ids-query='SELECT customer.id FROM campaign WHERE campaign.advertising_channel_type="SEARCH"'`

* `customer-ids-query-file` - the same as `customer-ids-query` but the query is coming from a file (can be a GCS path).

  >Example usage: `gaarf <queries> --account=123456 --customer-ids-query-file=/path/to/query.sql

* `disable-account-expansion` - disable MCC account expansion into child accounts (useful when you need to execute a query at MCC level or for speeding up if you provided leaf accounts).
  By default Gaarf does account expansion (even with `customer-ids-query`).

* `parallel-accounts` - how one query is processed for multiple accounts: in parallel (true) or sequentially (false). By default - in parallel.
* `parallel-queries` - how to process queries files: all in parallel (true) or sequentially (false). By default - in parallel (*Python version only*, for NodeJS - always sequentially)
* `parallel-threshold` - a number, maximum number of parallel queries.

Options specific for CSV writer:
* `csv.output-path` - output folder where csv files will be created
* `csv.array-separator` - a separator symbol for joining arrays as strings, by default '|'.
* `csv.file-per-customer` - create a CSV file per customer (default: false) (*NodeJS version only*)
* `csv.quote` - wrap values in quotes (default: false) (*NodeJS version only*)

Options specific for BigQuery writer:
* `bq.project` - GCP project id
* `bq.dataset` - BigQuery dataset id where tables with output data will be created
* `bq.location` - BigQuery [locations](https://cloud.google.com/bigquery/docs/locations)
* `bq.table-template`  - template for tables names, `{script}` references script base name, plus you can use [expressions](#expressions-and-macros) (*NodeJS version only*)
* `bq.dump-schema` - flag that enable dumping json files with schemas for tables (*NodeJS version only*)
* `bq.no-union-view` - flag that disables creation of "union" view that combines all customer tables (*NodeJS version only*)
* `bq.array-handling` - arrays handling method: "arrays" (default) - store arrays as arrays (repeated fields), "strings" - store arrays as strings (items combined via a separator, e.g. "item1|item2").
* `bq.array-separator` - a separator symbol for joining arrays as strings, by default '|'.
* `bq.key-file-path` - a SA key file path for BigQuery authentication (by default application default credentials will be used) (*NodeJS version only*)
* `bq.insert-method` - a method of loading data into tables: `load` - using loadTable method (default), `insert` - using insertAll method. loadTable loads tables from temp json files, while 'insertAll' accumulate rows in memory and insert them in batches. (*NodeJS version only*)
* `bq.output-path` - a path to location where temporal json files will be created, used to load BQ tables (for insert-method=load). It can be GCS location (gs://) (*NodeJS version only*)

Options specific for Console writer:
* `console.transpose` - whenever and how to transpose (switch rows and columns) result tables in output:
`auto` (default) - transpose only if table does not fit into terminal window, `always` - transpose all the time, `never` - never transpose (*NodeJS version only*).
* `console.page_size` - maximum rows count to output per each script (aliases: `page-size`, `maxrows`)
* `console.format` - output format: "json" or "table" (*Python version only).

Options specific for SqlAlchemy writer (*Python version only*):
* `sqldb.connection-string` to specify where to write the data (see [more](https://docs.sqlalchemy.org/en/14/core/engines.html))
* `sqldb.if-exists` - specify how to behave if the table already exists (see [more](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_sql.html))

Options specific for Sheet writer (*Python version only*):
* `sheet.spreadsheet-url` - optional URL of spreadsheet where data should be saved; if not provided a new spreadsheet will be created.
* `sheet.share-with` - with whom the newly created spreadsheet should be shared.
* `sheet.credentials-file` - path to service account used to write data. More at [gspread authentication](https://docs.gspread.org/en/v5.10.0/oauth2.html)
* `sheet.is-append` - whether data in the sheet should be overwritten (default) or appended.

Options specific for JSON writer:
* `json.output-path` - output folder where json files will be created
* `json.file-per-customer` - create a CSV file per customer (default: false) (*NodeJS version only*)
* `json.format` - output format: "json" (JSON) or "jsonl" (JSON Lines)
* `json.value-format` - value format - representation of values: "arrays" (values as arrays), "objects" (values as objects), "raw" (raw output)

####  Query specific options

If your query contains macros, templates, or sql  you need to pass `--macro.`, `--template.`, or `--sql.` CLI flags to `gaarf`.
Learn more about each of those in [How to write queries](docs/how-to-write-queries.md) document:
* [Macros](docs/how-to-write-queries.md#macros)
* [Templates](docs/how-to-write-queries.md#templates)
* [Sql](docs/how-to-write-queries.md#sql)


By default `gaarf` expect a list of files, but with `--input=console` option you can provide query(-ies) directly from console:

```
gaarf "select customer.id from customer" 'SELECT campaign.id FROM campaign WHERE campaign.advertising_channel_type="SEARCH"' \
  --input=console --output=console
```

(*NodeJS version only*) You can use all types of outputs and might want to specify queries names (will be used for output files/tables),
to do it prepend a query with "some_name:". E.g.:
```
gaarf "customer:select customer.id from customer" --input=console --output=bq
```

For NodeJS version any of arguments can be specified via environment variable which name starts with "GAARF_" (e.g. GAARF_ACCOUNT).


### Postprocessing

Once reports have been fetched you might use `gaarf-bq` or `gaarf-sql` (utilities that installed alongside with `gaarf`) to run queries in BigQuery or any other DB supposed by SqlAlchemy based on collected data in there.
Essentially it's a simple tool for executing queries from files, optionally creating tables for query results.


```shell
gaarf-bq <files> [options]
gaarf-sql <files> [options]
```

If your query contains macros, templates, or sql  you need to pass `--macro.`, `--template.`, or `--sql.` CLI flags to to `gaarf-bq` or `gaarf-sql`.
Lear more about each of those in [How to write queries](docs/how-to-write-queries.md) document:
* [Macros](docs/how-to-write-queries.md#macros)
* [Templates](docs/how-to-write-queries.md#templates)
* [Sql](docs/how-to-write-queries.md#sql)

The tool assumes that scripts you provide are DDL, i.e. contains statements like create table or create view.

In general it's recommended to separate tables with data from Ads API and final tables/views created by your post-processing queries.

**BigQuery specific options:**

* `project` - GCP project id
* `dataset-location` - BigQuery [locations](https://cloud.google.com/bigquery/docs/locations) for newly created dataset(s)

So it's likely that your final tables will be in a separate dataset (or datasets). To allow the tool to create those datasets for you, make sure that macro for your datasets contains the word "dataset".
In that case `gaarf-bq` will check that dataset exists and create it if not.


For example:
```
CREATE OR REPLACE TABLE `{dst_dataset}.my_dashboard_table` AS
SELECT * FROM {ads_ds}.{campaign}
```
In this case `gaarf-bq` will check for existence of a dataset specified as 'dst_dataset' macro.

**SqlAlchemy specific options [Python only]:**
* `connection-string` - specific connection to the selected DB (see [more](https://docs.sqlalchemy.org/en/14/core/engines.html))

Connection string can be:
* raw text (i.e. `sqlite:///gaarf.db`)
* parameterized string relying on environmental variables
(i.e. `postgresql+psycopg2://{GAARF_USER}:{GAARF_PASSWORD}@{GAARF_DB_HOST}:{GAARF_DB_PORT}/{GAARF_DB_NAME}`).

If the connection string relies on parameters, please export them:

```
export GAARF_USER=test
export GAARF_PASSWORD=test
export GAARF_DB_HOST=test
export GAARF_DB_PORT=12345
export GAARF_DB_NAME=test
```

### Dynamic dates
Macro values can contain a special syntax for dynamic dates. If a macro value starts with *:YYYY* it will be processed
as a dynamic expression to calculate a date based on the current date.
The syntax is: `:PATTERN-N`,
where N is a number of days/months/years and PATTERN is one of the following:
* *:YYYY* - current year, `:YYYY-1` - one year ago
* *:YYYYMM* - current month, `:YYYYMM-2` - two months ago
* *:YYYYMMDD* - current date, `:YYYYMMDD-7` - 7 days ago

Example with providing values for macro start_date and end_date (that can be used in queries as date range) as
a range from 1 month ago to yesterday:
```
gaarf google_ads_queries/*.sql --ads-config=google-ads.yaml \
  --account=1234567890 --output=bq \
  --macro.start_date=:YYYYMM-1 \
  --macro.end_date=:YYYYMMDD-1 \
```
So if today is 2022-07-29 then start_date will be '2022-06-29' (minus one month) and
end_date will be '2022-07-28' (minus one day).


> NOTE: dynamic date macro (:YYYY) can be defined as expressions as well (e.g. `${today()-1}` instead of ':YYYYMMDD-1')
> so they are two alternatives. But with expressions you won't need to provide any arguments.
> With expressions we'll have easier deployment (no arguments needed) but
> with dynamic date macro more flexibility if you need to provide different values (sometimes dynamic, sometimes fixed).


If you need to get the current date value in your query you can use a special macro `date_iso` that both versions support.
In runtime you can assume that its value is always provided with the current date in the YYYYMMDD format.
But you can override it via arguments if needed (e.g. `--macro.date_iso=:YYYYMMDD-1`).


## Docker

You can run Gaarf as a Docker container.

```
export GAARF_ACCOUNT=123456
docker run  \
  -v $HOME/google-ads.yaml:/root/google-ads.yaml \
  ghcr.io/google/gaarf-py:latest \
  gaarf "SELECT customer.id AS account_id FROM customer" \
  --input=console --output=console \
  --account=$GAARF_ACCOUNT --ads_config=/root/google-ads.yaml
```

### Build a container image
The repository contains sample `Dockerfile`'s for both versions ([Node](js/Dockerfile)/[Python](py/Dockerfile))
that you can use to build a Docker image.

If you cloned the repo then you can just run `docker build` (see below) inside it (in js/py folders) with the local [Dockerfile](js/Dockerfile).
Otherwise you can just download `Dockerfile` into an empty folder:
```
curl -L https://raw.githubusercontent.com/google/ads-api-report-fetcher/main/js/Dockerfile > Dockerfile
```

Sample Dockerfile's don't depend on sources, they install gaarf from registries for each platform (npm and PyPi).
To build an image with name 'gaarf' (the name is up to you but you'll use it to run a container later) run
the following command in a folder with `Dockerfile`:
```
sudo docker build . -t gaarf
```
Now you can run a container from this image.

### Run a container
For running a container you'll need the same parameters as you would provide for running it in command line
(a list of ads scripts and a Ads API config and other parameters) and authentication for Google Cloud if you need to write data to BigQuery.
The latter is achievable via declaring `GOOGLE_APPLICATION_CREDENTIALS` environment variable with a path to a service account key file.

You can either embed all them into the image on build or supply them in runtime when you run a container.

The aforementioned `Dockerfile` assumes the following:
* You will provide a list of ads script files
* Application Default Credentials is set with a service account key file as `/app/service_account.json`

So you can map your local files onto these paths so that Gaarf inside a container will find them.
Or copy them before building, so they will be embedded into the image.

This is an example of running Gaarf (Node version) with mapping local files, assuming you have `.gaarfrc` and `service_account.json` in the current folder:
```
sudo docker run --mount type=bind,source="$(pwd)/.gaarfrc",target=/app/.gaarfrc \
  --mount type=bind,source="$(pwd)/ads-scripts",target=/app/ads-scripts \
  --mount type=bind,source="$(pwd)/service_account.json",target=/app/service_account.json \
  gaarf ./ads-scripts/*.sql
```
Here we mapped local `.gaarfrc` with with all parameters (alternatively you can pass them explicitly in command line),
mapped a local service_account.json file with SA keys for authenticating in BigQuery, mapped a local folder "ads-scripts"
with all Ads scripts that we're passing by wildcard mask (it'll be expanded to a list of files by your shell).


## Gaarf Cloud Workflow
Inside [gcp](gcp) folder you can find code for deploying Gaarf to Google Cloud. There are the following components provided:
* Cloud Function (in [gcp/functions](gcp/functions) folder) - two CFs that you can use for running gaarf with scripts located on GCS
* Cloud Workflow (in [gcp/workflow](gcp/workflow) folder) - a Cloud Workflow that orchestrates enumeration scripts on GCS and calling CFs

Please see the [README](gcp/README.md) there for all information.


## Differences in Python and NodeJS versions

### Query syntax and features
There are differences in supported features for queries.

NodeJS-only features:
* expressions (${...})
* functions

### Output BigQuery structure
There are differences in how tools process Ads queries.
Python version sends queries to Ads API and parses the result. From the result it creates a BigQuery schema. That's becasue tables in BQ are created only when a query retuned some data.
NodeJS on the contrary parses queries and initializes BigQuery schema before execution. So that it creates BQ tables regardless of the results.

There are differences in BigQuery table structures as well.
Python version creates one table per script. While NodeJS creates a table per script per customer and then creates a view to combine all customer tables.
For example, you have a query campaign.sql. As a result you'll get a querable source 'campaign' in BigQuery in any way. But for Python version it'll be a table.
For NodeJS it'll be a view like `create view dataset.campaign as select * from campaign_* when _TABLE_PREFIX in (cid1,cid2)`, where cid1, cid2 are customer ids you supplied.

From Ads API we can get arrays, structs and arrays of arrays or structs. In Python version all arrays will be degraded to string with "|" separator.
In NodeJS version the result by default will be a repeated field (array) but can be degrated to string with separator via the `bq.array-handling` option.
If values of an array from Ads API are also arrays or structs, they will be converted to JSON.

### API support
Python version supports any API version (currently available).
While as NodeJS parses query structure it supports only one particular version (it's printed when you run `gaarf` without arguments).


## Disclaimer
This is not an officially supported Google product.
