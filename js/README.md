# Google Ads API Report Fetcher (gaarf)
Node.js version of Google Ads API Report Fetcher tool a.k.a. `gaarf`.
Please see the full documentation in the root [README](../README.md).

## Table of content

 - [Overview](#overview)
 - [Command Line](#command-line)
      - [Install globally](#install-globally)
      - [Running from folder](#running-from-folder)
      - [Config files](#config-files)
          - [Ads API config](#ads-api-config)
 - [Library](#library)
 - [Development](#development)


## Overview
>You need [Node.js](https://nodejs.org/) to run the tool.
v16 should be sufficient.

## Command Line
### Install globally
```shell
npm i ads-api-report-fetcher -g
```
then you can run the tool with `gaarf` and `gaarf-bq` commands:
```shell
gaarf <files> [options]
```

Documentation on available options see in the root [README.md](../README.md).

### Running from folder
If you cloned the repo into "ads-api-fetcher" folder, then
run `npm i --production` in ads-api-fetcher/js folder,
after than we can run the tool directly:
```shell
ads-api-fetcher/js/gaarf <files> [options]
```
or
```shell
node ads-api-fetcher/js/dist/cli.js <files> [options]
```


### Config files
Besides passing options explicitly (see the root [README.me](../README.md) for
full description) you can use config files.
By default the tool will try to find `.gaarfrc` starting from the current folder
up to the root. If found, options from that file will be used if they weren't
supplied via command line.

Example of `.gaarfrc`:
```json
{
 "ads-config": ".config/google-ads.yaml",
 "output": "bq",
 "csv.destination-folder": "output",
 "macro": {
   "start_date": "2022-01-01",
   "end_date": "2022-02-10"
 },
 "account": 1234567890,
 "bq.project": "myproject",
 "bq.dataset": "mydataset",
 "bq.dump-schema": true
}
```
Please note that options with nested values, like 'bq.project', can be specified
either as objects (see "macro") or as flatten names ("bq.project").

Besides an implicitly used .rc-files you can specify a config file explicitly
via `--config` option. In that case options from `--config` file will be merge
with a .rc file if one exists. Via `--config` option you can also provide a YAML
file (as alternative to JSON) with a similar structure:
`gaarf <files> --config=gaarf.yaml`

Example of a yaml config:
```yaml
ads-config: .config/google-ads.yaml
output: bq
csv.destination-folder: output
macro:
  start_date: 2022-01-01
  end_date: :YYYYMMDD
account: 1234567890
bq.project: myproject
bq.dataset: mydataset
```

Similarly a config file can be provided for the gaarf-bq tool:
```
gaarf-bq bq-queries/*.sql --config=gaarf-bq.yaml
```
(again it can be either YAML or JSON)


#### Ads API config
There are two mechanisms for supplying Ads API configuration (developer token, etc ).
Either via a separated yaml-file whose name is set in `ads-config` argument or
via separated CLI arguments starting `ads.*` (e.g. `--ads.client_id`) or
in a config file (`ads` object):
```json
{
 "ads": {
   "client_id": "...",
   "developer_token": ".."
 },
 "output": "bq",
}
```
Such a yaml-file is a standard way to configure Ads API Python client -
see [example](https://github.com/googleads/google-ads-python/blob/HEAD/google-ads.yaml).

If neither `ads-config` argument nor `ads.*` arguments were provider then the tool will
search for a local file "google-ads.yaml" and if it exists it will be used.

See more help with `--help` option.


## Library
How to use Gaarf as a library in your own code.
```ts
import {AdsQueryExecutor} from 'ads-api-report-fetcher';

let client = new GoogleAdsApiClient('google-ads.yaml');
let customers = await client.getCustomerIds();
let writer = new CsvWriter('.tmp');
let executor = new AdsQueryExecutor(client);
let params = {};
for (let scriptPath of scriptPaths) {
  let queryText = fs.readFileSync(scriptPath.trim(), 'utf-8');
  let scriptName = path.basename(scriptPath).split('.sql')[0];
  await executor.execute(scriptName, queryText, customers, params, writer);
}
```

If you need to process results from queries (and not just rely on a writer) then
use `executeGen` method (it's a async generator):
```ts
  let results = await executor
    .executeGen(scriptName, queryText, customers, params, writer);
  for await (let res of results) {
    //res.rows - array of rows for one customer
  }
```

Alternatively you can incorporate `keepData` option of the `BigQueryWriter`:
```ts
  let writer =
      new BigQueryWriter(projectId, dataset, {keepData: true});
  await executor.execute(scriptName, queryText, customers, macroParams, writer);
```
thanks to keepData you will be able access the `writer.rowsByCustomer` property
with a map of customer id to rows with data.


# Development
## Run typescript directly
```
node -r ./node_modules/ts-node/register src/cli.ts ...
```

