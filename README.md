# Google Ads API Report Fetcher (gaarf)

## Overview

Google Ads API Report Fetcher (`gaarf`) simplifies running [Google Ads API Reports](https://developers.google.com/google-ads/api/fields/v9/overview)
by separating logic of writing [GAQL](https://developers.google.com/google-ads/api/docs/query/overview)-like query from executing it and saving results.\
The library allows you to define GAQL queries alonside aliases and custom extractors and specify where the results of such query should be stored.
You can find example queries in [examples](examples) folder.
Based on such a query the library fill extract the correct GAQL query, automatically extract all necessary fields from schema
and transform them into a structure suitable for writing data.

Currently the tool supports two types of output: CSV files and BigQuery tables.


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
* `account` - Ads account id, aka customer id, also can be specified in google-ads.yaml as 'customer-id'
* `output` - output type,
           values:
           * `csv` - write data to CSV files
           * `bq` or `bigquery` - write data to BigQuery

Options specific for CSV writer:
* `csv.destination-folder` - output folder where csv files will be created

Options specific for BigQuery writer:
* `bq.project` - GCP project id
* `bq.dataset` - BigQuery dataset id where tables with output data will be created
* `bq.table-template`  - template for tables names, `{script}` references script base name (*JS version only*)
* `bq.dump-schema` - flag that enable dumping json files with schemas for tables (*JS version only*)

All parameters whose names start with the `macro.` prefix are passed to queries as params object.
For example if we pass parameters: `--macro.start_date=2021-12-01 --macro.end_date=2022-02-28`
then inside sql we can use `start_date` and `end_date` parameters in curly brackets:
```sql
    AND segments.date >= "{start_date}"
    AND segments.date <= "{end_date}"
```

Full example:
```
gaarf google_ads_queries/*.sql --ads-config=google-ads.yaml \
  --account=1234567890 --output=bq \
  --macro.start_date=2021-12-01 \
  --macro.end_date=2022-02-28 \
  --bq.project=my_project \
  --bq.dataset=my_dataset
```

### Postprocessing

Once reports have been fetched you might use `gaarf-bq` (utility that installed alonside with `gaarf`) to run queries in BigQuery based on collected data in there.
Essensially it's a simple tool for executing BigQuery queries from files, optionally creating tables for query results.


```shell
gaarf-bq <files> [options]
```

Options:
* `project` - GCP project id
* `target` - a target dataset to create a table with query's result, if omitted query's result aren't inserted anywhere
* `sql.*` - named SQL parameters to be used in queries as `@param`. E.g. a parameter 'date' supplied via cli as `--sql.date=2022-06-01` can be used in query as `@date` in query.
* `macro.*` - macro parameters to substitute into queries as `{param}`. E.g. a parameter 'dataset' supplied via cli as `--macro.dataset=myds` can be used as `{dataset}` in query's text.

There are prefined macros that can be used in queries without passing via command line:
* `date_iso` - replaced onto current date in YYYYMMDD format

Basically there're two main use-cases: with passing `target` parameter and without. If a target supplied it should be
a dataset name (either existing or non-existing one) where a table for each script will be created (the name of the table will be the script file base name).
So you can write a select script that extracts data from other BigQuery tables and the results will be put into a new table.
If a target isn't supplied than no table will be created. It's useful if your script contains DDL statements (e.g. create or replace view).

There are two type of parameters that you can pass to a script: macro and sql-parameter. First one is just a substitution in script text.
For example:
```
SELECT *
FROM {ds-src}.{table-src}
```
Here `ds-src` and `table-src` are macros that can be supplied as:
```
gaarf-bq --macro.table-src=table1 --macro.ds-src=dataset1
```

You can also use normal sql type parameters with `sql` argument:
```
SELECT *
FROM {ds-src}.{table-src}
WHERE name LIKE @name
```
and to execute:
`gaarf-bq --macro.table-src=table1 --macro.ds-src=dataset1 --sql.name='myname%'`

it will create a script to run in BQ:
```
SELECT *
FROM dataset1.table1
WHERE name LIKE @name
```

ATTENTION: passing macros into sql query is vulnerable to sql-injection so be very careful where you're taking values from.

## Disclaimer
This is not an officially supported Google product.

Copyright 2022 Google LLC. This solution, including any related sample code or data, is made available on an “as is,” “as available,” and “with all faults” basis, solely for illustrative purposes, and without warranty or representation of any kind. This solution is experimental, unsupported and provided solely for your convenience. Your use of it is subject to your agreements with Google, as applicable, and may constitute a beta feature as defined under those agreements. To the extent that you make any data available to Google in connection with your use of the solution, you represent and warrant that you have all necessary and appropriate rights, consents and permissions to permit Google to use and process that data. By using any portion of this solution, you acknowledge, assume and accept all risks, known and unknown, associated with its usage, including with respect to your deployment of any portion of this solution in your systems, or usage in connection with your business, if at all.

