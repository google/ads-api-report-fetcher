# Changelog
## 1.6.2 - 2022-09-07
* Fixes for ConsoleWrite (`--output=console`)

## 1.6.1 - 2022-08-24
* improved diagnostics for parsing queries when a select expression accesses unknown/deprecated properties

## 1.6.0 - 2022-08-24
* migrated onto Google Ads API v11 (google-ads-api@11)
* gaarf-bq: added support for `dataset-location` argument

## 1.5.0 - 2022-08-23
* support loading google-ads.yaml (`ads-config` argument) from GCS
                                                                                 2
## 1.4.2 - 2022-08-18
* support for ':YYYYMMDD-N' macro ('dynamic dates macro')
* added `dump-query` cli argument
* support yaml files and envvars as sources of cli arguments (for both tools) (`--config` argument)

## 1.4.1 - 2022-07-27
* ConsoleWriter (`--output=console`) - output query results to stdout, supports transposing (if a table doesn't fit in terminal window it's transposed and splitted onto several horizontal tables) which can be tuned via cli args
* all console output replaced with logging, setting log-level via cli args

## 1.4.0 - 2022-07-22
* support expressions (`${...}`) (based on [mathjs](https://mathjs.org) library) in queries (both Ads and BigQuery)
* breaking: removed `date_iso` built-in macro (easily can be replaced by `${today()}`)
