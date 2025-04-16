# Changelog

## 3.1.1 - 2025-04-17

- Fix: added `override:true` while importing user functions into mathjs
- added debug logging for raw objects from API response via env var GAARF_DUMP_API_ROW

## 3.1.0 - 2025-04-08

- support Google Ads API v19 (updated google-ads-api to v19)

## 3.0.1 - 2025-02-13

- Fix: fixed gaarf-bq cli entrypoint

## 3.0.0 - 2025-02-05

- support authentication under service accounts
- GAQL parser based on Peggy grammar (https://peggyjs.org/online.html) - support any form of comments and whitespaces
- reworked parsing of virtual column expressions (with mathjs):
  - compatibility with resource indexes and nested fields, e.g. the following are examples that didn't work and work now:
    - `'http://' + ad_group_ad.ad.final_urls[1]` - failed previously
    - any other expressions contained '~' and ':' in strings failed
    - `(change_event.new_resource:campaign.target_cpa.target_cpa_micros) / 1000000` - nested field customizers in expressions
  - support method calls (e.g. `(metrics.clicks / metrics.impressions).toFixed(2)` or `campaign.name.split('.').pop()`)
  - support classic function invocation instead of former ':$' syntax (`campaign_criterion.ad_schedule.day_of_week:$formatDay`), 
now it can be `formatDay(campaign_criterion.ad_schedule.day_of_week)` (functions are still should be defined in FUNCTIONS sections at the bottom)
  - full compatibility with RestApiClient - all fields in result in camelCase are decoded to snake_case

## 2.13.0 - 2025-01-30

- migrated to ESM modules

## 2.12.2 - 2024-10-29

- resource index customizer ("~0") support not only string fields but also structs with predefined fields (name/text/assset/value),
  automatically extract their values

## 2.12.1 - 2024-10-04

- added filtering ouf of hidden accounts while expanding MCC

## 2.12 - 2024-09-13

- support Google Ads API v17.1 (updated google-ads-api to v17.1)

## 2.11.1 - 2024-08-14

- Fix: `FileWriterBase`: disabled resumable upload for streaming to GCS, increased maxRetries - to overcome sporadic failures of GCS API on high load

## 2.11 - 2024-08-12

- `BigQueryWriter` and all file-based writers (`CsvWriter`/`JsonWriter`) support `outputPath` option with paths on GCS. Via cli it's passed as `--bq.output-path`, `--csv.output-path`, `--json.output-path`.
- All writers implemented streaming to files (including paths on GCS). It's especially useful in Gaarf-WF where gaarf Cloud Function streams data to files on GCS.
- added REST API support (along with existing gRPC). Now there're two clients: `GoogleAdsRpcClient` (renamed existing GoogleAdsClient) and `GoogleAdsRestClient` (new one). A client is chosen via `--api` option: `--api=rest` (REST) or `--api=rpc` (gRCP) - default. For the REST client a API verson can be provided via `--api-version`.
- `AdsQueryExecutor`: introduced a new method executeQueryAndParseToObjects to returns values as objects, using a new mode for `AdsRowParser.parseRow`.
- diag: support DUMP_MEMORY envvar to output current memory usage to log

## 2.10 - 2024-05-09

- added a new writer - JsonWirter (`--output=json`)
- added `account-tree` cli command to show account info with subaccounts (`gaarf account-tree <options>`)

## 2.9 - 2024-03-04

- support Google Ads API v16 (updated google-ads-api to v16)

## 2.8 - 2023-11-29

- files with glob wildcards are now supported (`*` and `**`), so gaarf can process files by mask without relying on shell's wildcard expansion
- Fix: console-writer: fixed hanging in some cases, prettified output for arrays
- Fix: cli: aliases for `loglevel` argument weren't handled correctly for initializing logger
- ads-api-client: added additional error codes from grpc to handle for retrying (RESOURCE_EXHAUSTED, DEADLINE_EXCEEDED)
- console-writer: by default it limits output to 1000 rows

## 2.7 - 2023-11-06

- added 'validate' cli command for validating ads credentials: `gaarf validate --ads-config=/path/to/google-ads.yaml`
- added silent mode: `--loglevel=off`

## 2.6 - 2023-11-03

- support for `--input=console` cli argument allows to supply queries in command file, e.g. `gaarf "select campaign.id from campaign" --input=console`

## 2.5 - 2023-10-04

- support for builtin resources
- support for the `*` selector in Ads queries - it expands to all scalar primitive and enum fields of a resource
- bq-writer: Fixed: writer failed with 'dataset not found' error if dataset exists in a different location than specified
- console-writer: Fixed: writer went to the infinite recursion if the only one column didn't fit into console width after transposing

## 2.4 - 2023-09-22

- support Google Ads API v14.1 (updated google-ads-api to v14.1)

## 2.3 - 2023-09-07

- Improved error handling and logging of errors, added retrying on internal errors from the API (for sync and async methods both)

## 2.2 - 2023-07-27

- added `current_date`, `current_datetime` macros additionally to `date_iso` for Python-version compatibility

## 2.1 - 2023-07-26

- Fix: [bq-writer] integer fields got string type if bq.array-handling=strings was specified

## 2.0 - 2023-07-11

- breaking: gaarf CLI tool supports multiple CIDs in `account` argument.
  `GoogleAdsApiClient` constructor and `loadAdsConfigFromFile` don't take customer_id anymore,
  but `GoogleAdsApiClient`'s methods `getCustomerIds`, `executeQuery` and `executeQueryStream` on the contrary now require a customer_id.
- Templates support (via https://mozilla.github.io/nunjucks/)
- AdsQueryExecutor: limit level of parallelism (be default there's a threshold to issue simultaneous parallel queries)
- CsvWriter: support `file-per-customer` argument - split output file by customer

## 1.15 - 2023-05-18

- added support for `date_iso` macro (to be compatible with the Python version)
- added global executable aliases `gaarf-node` and `gaarf-node-bq`

## 1.14 - 2023-04-26

- support Google Ads API v13 (updated google-ads-api to v13)

## 1.13 - 2023-04-17

- bq-writer: added `array-handling`, `array-separator` options - array can be represented either as arrays (default) or string (as in Python version)
- csv-writer: added `array-separator` option + similar object to bq-writer for array representation (previously JSON.stringify used)

## 1.12 - 2023-03-31

- Added `disable-account-expansion` flag to disable MCC account expansion into child accounts (useful when you need to execute a query at MCC level)
- (released as 1.11.9) enhanced logging support for running in Google Cloud (while running as part Gaarf Workflow all log entries have trace id from the root request)

## 1.11 - 2023-02-11

- Support for expressions in columns ("virtual columns")
- Support for block comments in ads queries (/_.._/)

## 1.10.2 - 2023-02-08

- Cutting off of resource name from autogenerated column aliases

## 1.10.1 - 2023-01-11

- Fix: fixed running for MCC but without login-customer-id in google-ads.yaml (account-id from args will be used as login-customer-id)
- ConsoleWriter: added `page-size` argument (limits maximum row count per script)
- ConsoleWriter: added `destination` alias for `destination-folder` argument

## 1.10 - 2022-12-14

- support Google Ads API v12 (updated google-ads-api to v12)

## 1.9.0 - 2022-11-21

- AdsQueryExecutor: introduced `getCustomerIds` method to fetch customer ids with a custom query to additionally filter cids extracted from a root MCC
  (previous implementation inside AdsApiClient was not correct)
- BigQueryExecutor: introduced `createUnifiedView` method for creating views for combining per-account tables (used by a new CF)
- Fix: BigQueryExecutor and BigQueryWriter: pass `datasetLocation` to BigQuery ctor
- Fix: winston logger format string didn't output fractional seconds

## 1.8.0 - 2022-11-10

- Massive rewrite of transfering data from Google Ads API to BigQuery using streaming

## 1.7.3 - 2022-10-21

- [breaking change] function `loadAdsConfigYaml` was renamed to `loadAdsConfigFromFile`
- Fix: @types-packages moved from devDependencies to dependencies to fix ts compilation in projects using gaarf-js as a ts-library

## 1.7.2 - 2022-09-13

- support Google Ads API v11.1 (updated google-ads-api to v11)

## 1.7.1 - 2022-09-09

- Fix: fixed running in Cloud Function environment

## 1.7.0 - 2022-09-09

- BigQueryWrite: changed data load method (loadTable instead of insertAll), which results in a significant speed up
- gaarf-bq: check existence and create datasets from macro those names contain 'dataset'; removed 'target' argument
- Fix: remove coloring for logging in non-TTY environments

## 1.6.2 - 2022-09-07

- Fixes for ConsoleWrite (`--output=console`)

## 1.6.1 - 2022-08-24

- improved diagnostics for parsing queries when a select expression accesses unknown/deprecated properties

## 1.6.0 - 2022-08-24

- migrated onto Google Ads API v11 (google-ads-api@11)
- gaarf-bq: added support for `dataset-location` argument

## 1.5.0 - 2022-08-23

- support loading google-ads.yaml (`ads-config` argument) from GCS
  2

## 1.4.2 - 2022-08-18

- support for ':YYYYMMDD-N' macro ('dynamic dates macro')
- added `dump-query` cli argument
- support yaml files and envvars as sources of cli arguments (for both tools) (`--config` argument)

## 1.4.1 - 2022-07-27

- ConsoleWriter (`--output=console`) - output query results to stdout, supports transposing (if a table doesn't fit in terminal window it's transposed and splitted onto several horizontal tables) which can be tuned via cli args
- all console output replaced with logging, setting log-level via cli args

## 1.4.0 - 2022-07-22

- support expressions (`${...}`) (based on [mathjs](https://mathjs.org) library) in queries (both Ads and BigQuery)
- breaking: removed `date_iso` built-in macro (easily can be replaced by `${today()}`)
