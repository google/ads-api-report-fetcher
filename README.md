# Google Ads API Reports Fetcher (gaarf)

## Overview

Google Ads API Reports Fetcher (`gaarf`) simplifies running [Google Ads API Reports](https://developers.google.com/google-ads/api/fields/v9/overview)
by separating logic of writing [GAQL](https://developers.google.com/google-ads/api/docs/query/overview)-like query from executing it and saving results.\
The library allows you to define GAQL query alonside aliases and custom extractors and specify where the results of such query should be stored. You can find example queries in [examples](examples) folder. Based on this query the library fill extract the correct GAQL query, automatically extract all necessary fields from returned results and transform them into the structure suitable for writing data.


## Getting started

Ads API Reports Fetcher has two versions - Python and Node.js.
Please explore the relevant section to install and run the tool:

* [Getting started with gaarf in Python](py/README.md)
* [Getting started with gaarf in Node.js](js/README.md)

## Writing Queries

Google Ads API Reports Fetcher provides an extended syntax on writing GAQL queries.\
Please refer to [How to write queries](docs/how-to-write-queries.md) section to learn the query syntax.

## Running gaarf

If `gaarf` is installed globally you can run it with the following command.

```shell
gaarf <files> [options]
```

### Options
The required positional arguments are a list of files with Ads queries (GAQL).
On *nix OSes you can use a glob pattern, e.g. `./ads-queries/**/*.sql`.

> If you run the tool on a *nix OS then your shell (like zsh/bash) probably
> supports file names expansion (see [bash](https://www.gnu.org/software/bash/manual/html_node/Filename-Expansion.html),
> [zsh](https://zsh.sourceforge.io/Doc/Release/Expansion.html), 14.8 Filename Generation).
> And so it does expansion of glob pattern (file mask) into a list of files.

Options:
* `ads-config` - a path to yaml file with config for Google Ads,
               by default assuming 'google-ads.yaml' in the current folder
* `account` - ads account id, aka customer id, also can be specified in google-ads.yaml as 'customer-id'
* `output` - output type,
           values:
           * `csv` - write data to CSV files
           * `bq` or `bigquery` - write data to BigQuery

Options specific for CSV writer:
* `csv.destination-folder` - output folder where csv files will be created

Options specific for BigQuery writer:
* `bq.project` - GCP project id
* `bq.dataset` - BigQuery dataset id where tables with output data will be created
* `bq.table-template`  - template for tables names, `{script}` references script name (*JS version only*)
* `bq.dump-schema` - flag that enable dumping json files with schemas for tables (*JS version only*)

All parameters whose names start with the `sql.` prefix are passed to queries as params object.
For example if we pass parameters: `--sql.start_date=2021-12-01 --sql.end_date=2022-02-28`
then inside sql we can use `start_date` and `end_date`:
```sql
    AND segments.date >= "{start_date}"
    AND segments.date <= "{end_date}"
```

### Postprocessing

(*Python version only*) Once report have been fetched you might use `gaarf-postprocess` (utility that is installed with `gaarf`) to run queries in BigQuery based on collected data 

```shell
gaarf-postprocess <files> [options]
```

Options:
* `bq.project` - GCP project id
* `bq.dataset` - BigQuery dataset id where tables with output data will be created


## Disclaimer
This is not an officially supported Google product.

Copyright 2022 Google LLC. This solution, including any related sample code or data, is made available on an “as is,” “as available,” and “with all faults” basis, solely for illustrative purposes, and without warranty or representation of any kind. This solution is experimental, unsupported and provided solely for your convenience. Your use of it is subject to your agreements with Google, as applicable, and may constitute a beta feature as defined under those agreements. To the extent that you make any data available to Google in connection with your use of the solution, you represent and warrant that you have all necessary and appropriate rights, consents and permissions to permit Google to use and process that data. By using any portion of this solution, you acknowledge, assume and accept all risks, known and unknown, associated with its usage, including with respect to your deployment of any portion of this solution in your systems, or usage in connection with your business, if at all.

