# Google Ads API Report Fetcher (gaarf)

Node.js version of Google Ads API Report Fetcher tool a.k.a. `gaarf`.
Please see the full documentation in the root [README](https://github.com/google/ads-api-report-fetcher/README.md).

<p align="center">
  <a href="https://developers.google.com/google-ads/api/docs/release-notes">
    <img src="https://img.shields.io/badge/google%20ads-REST%20schemas-009688.svg?style=flat-square"/>
  </a>
  <a href="https://www.npmjs.com/package/google-ads-api-report-fetcher">
    <img src="https://img.shields.io/npm/v/google-ads-api-report-fetcher.svg?style=flat-square" />
  </a>
  <a>
    <img src="https://img.shields.io/npm/dm/google-ads-api-report-fetcher.svg?style=flat-square" />
  </a>
  <a href="https://github.com/google/gts">
    <img src="https://img.shields.io/badge/code%20style-google-blueviolet.svg"  />
  </a>
</p>

## Table of content

- [Overview](#overview)
- [Command Line](#command-line)
  - [Install globally](#install-globally)
  - [Running from folder](#running-from-folder)
  - [Config files](#config-files)
    - [Ads API config](#ads-api-config)
- [Library](#library)
  - [Basic usage with a writer](#basic-usage-with-a-writer)
  - [Streaming / programmatic row processing](#streaming--programmatic-row-processing)
  - [Single query and customer execution](#single-query-and-customer-execution)
  - [Macros and template parameters](#macros-and-template-parameters)
- [Query Syntax & Repeated Fields](#query-syntax--repeated-fields)
- [Flavors](#flavors)
  - [Google Apps Script (`gaarf-appsscript`)](#google-apps-script-gaarf-appsscript)
  - [Web / Browser (`gaarf-web`)](#web--browser-gaarf-web)
- [Development](#development)
- [License](#license)
- [Disclaimer](#disclaimer)

## Overview

> You need [Node.js](https://nodejs.org/) to run the tool.
> Node.js >= 18 is recommended.

## Command Line

### Install globally

```shell
npm i google-ads-api-report-fetcher -g
```

then you can run the tool with `gaarf` and `gaarf-bq` commands:

```shell
gaarf <files> [options]
```

If you have both versions installed (Python and Node) you can run the Node-version explicitly via `gaarf-node` alias.

Documentation on available options see in the root [README.md](../README.md).

### Running from folder

If you cloned the repo into "ads-api-fetcher" folder, then
run `npm i` and `npm run build` in `ads-api-fetcher/js` folder,
after that you can run the tool directly:

```shell
ads-api-fetcher/js/gaarf <files> [options]
```

or

```shell
node ads-api-fetcher/js/dist/cli.js <files> [options]
```

### Config files

Besides passing options explicitly (see the root [README.md](../README.md) for
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
either as objects (see "macro") or as flattened names ("bq.project").

Besides an implicitly used .rc-file you can specify a config file explicitly
via `--config` option. In that case options from `--config` file will be merged
with a .rc file if one exists. Via `--config` option you can also provide a YAML
file (as alternative to JSON) with a similar structure:
`gaarf <files> --config=gaarf.yaml`

Example of a YAML config:

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

```shell
gaarf-bq bq-queries/*.sql --config=gaarf-bq.yaml
```

(again it can be either YAML or JSON)

#### Ads API config

There are two mechanisms for supplying Ads API configuration (OAuth credentials, etc.).
Either via a separate YAML file whose name is set in `ads-config` argument or
via separate CLI arguments starting `ads.*` (e.g. `--ads.client_id`) or
in a config file (`ads` object):

```json
{
  "ads": {
    "client_id": "...",
    "client_secret": "..."
  },
  "output": "bq"
}
```

Such a YAML file is a standard way to configure the Ads API client -
see [example](https://github.com/googleads/google-ads-python/blob/HEAD/google-ads.yaml).

If neither `ads-config` argument nor `ads.*` arguments were provided, the tool will
search for a local file `google-ads.yaml` and if it exists it will be used.

See more help with `--help` option.

## Library

How to use Gaarf as a library in your own Node.js / TypeScript code.

Gaarf communicates directly with Google Ads API REST endpoints and utilizes Google Ads REST Discovery schemas. It can load schemas dynamically from the Discovery API or use bundled schemas, giving you the flexibility to call any supported Google Ads API version without updating or maintaining specialized client libraries.

`GoogleAdsApiClient` expects an object with Ads API access settings (`GoogleAdsApiConfig`) and an optional API version. You can configure settings manually or load them from a YAML or JSON file (e.g., `google-ads.yaml`) using the `loadAdsConfigFromFile` function.

### Basic usage with a writer

Execute queries across one or more accounts and pipe output directly to a writer (such as `CsvWriter`, `JsonWriter`, or `BigQueryWriter`):

```ts
import fs from 'fs';
import {
  GoogleAdsApiClient,
  AdsQueryExecutor,
  loadAdsConfigFromFile,
  getCustomerIds,
  CsvWriter,
} from 'google-ads-api-report-fetcher';

// Load credentials from YAML or JSON (e.g. google-ads.yaml)
const adsConfig = await loadAdsConfigFromFile('google-ads.yaml');
const client = new GoogleAdsApiClient(adsConfig);

// If customer_id is an MCC, getCustomerIds expands it to all child account IDs
const seedCid = adsConfig.customer_id;
const customers = await getCustomerIds(client, seedCid);

const executor = new AdsQueryExecutor(client);
const writer = new CsvWriter({outputPath: './output'});

const queryText = `
  SELECT
    campaign.id,
    campaign.name,
    metrics.clicks,
    metrics.impressions,
    metrics.clicks / metrics.impressions AS ctr
  FROM campaign
  WHERE segments.date DURING LAST_30_DAYS
`;

// Execute query for all accounts and write CSV files
await executor.execute('campaign_report', queryText, customers, {}, writer);
```

### Streaming / programmatic row processing

If you need to process results directly in memory rather than piping to a file writer, use `executeGen` (an async generator yielding results per account):

```ts
const results = executor.executeGen(
  'campaign_report',
  queryText,
  customers,
);

for await (const res of results) {
  console.log(`Account: ${res.customerId}, Rows count: ${res.rowCount}`);
  console.log(`Columns: ${res.query.columnNames.join(', ')}`);

  // res.rows is a 2D array of parsed values (with evaluated customizers, virtual columns, etc.)
  for (const row of res.rows!) {
    console.log(row);
  }
}
```

### Single query and customer execution

To execute a query for a single customer account and retrieve results as structured objects or arrays:

```ts
// Parse query with optional macros or template parameters
const query = await executor.parseQuery(queryText, 'campaign_report');

// Option A: Get parsed rows as an array of structured objects ({ [column]: value })
const objectResult = await executor.executeQueryAndParseToObjects(query, '1234567890');
for (const row of objectResult.rows!) {
  console.log(row); // e.g. { campaign_id: '123', campaign_name: 'Search Campaign', ctr: 0.05 }
}

// Option B: Get raw QueryResult with 2D parsed rows and raw API rows
const queryResult = await executor.executeOne(query, '1234567890');
console.log(queryResult.rows);
```

### Macros and template parameters

Gaarf supports macro placeholders (`{macro_name}`) and Nunjucks template expressions directly in your queries:

```ts
const queryText = `
  SELECT
    campaign.id,
    campaign.name
  FROM campaign
  WHERE segments.date >= '{start_date}' AND segments.date <= '{end_date}'
  {% if campaign_type %}
    AND campaign.advertising_channel_type = '{campaign_type}'
  {% endif %}
`;

const params = {
  macros: {
    start_date: '2026-01-01',
    end_date: '2026-01-31',
  },
  templateParams: {
    campaign_type: 'SEARCH',
  },
};

const query = await executor.parseQuery(queryText, 'templated_report', params);
const result = await executor.executeQueryAndParseToObjects(query, '1234567890');
```

## Query Syntax & Repeated Fields

Gaarf extends standard Google Ads GAQL with customizers (such as nested field extraction `:`, resource index extraction `~N`, and custom functions), virtual columns (via Math.js), and advanced handling for **repeated (array) fields**.

> [!NOTE]
> Extended query syntax and array operations are supported in **JavaScript versions of Gaarf** (Node.js, Google Apps Script, and Web).

See the comprehensive guide in [How to write queries](../docs/how-to-write-queries.md) for full syntax details and examples.

## Flavors

Gaarf is available in multiple flavors tailored for different JavaScript/TypeScript runtimes:

### Google Apps Script (`gaarf-appsscript`)

Gaarf has a dedicated build for [Google Apps Script](https://developers.google.com/apps-script) located in the [`apps-script/`](apps-script/) directory:

- **Built-in Authorization:** Leverages Apps Script's native `ScriptApp.getOAuthToken()` and `UrlFetchApp` to execute Google Ads API REST requests without managing OAuth refresh flows manually.
- **Direct Google Sheets Output:** Includes `SheetsWriterAppsScript` to write query results directly into spreadsheet sheets in batches.
- **Interactive Sidebar:** Includes an Apps Script sidebar UI that lets spreadsheet users paste GAQL queries, select accounts, and run reports into sheets on demand.
- **Deployable via Clasp:** Easily built with Rollup and deployed using [`@google/clasp`](https://github.com/google/clasp).

You can create your own library in AppsScript and just reference our library with name 'GaarfLibrary'.

### Web / Browser (`gaarf-web`)

[`gaarf-web`](web/) is a standalone, browser-compatible npm package (`gaarf-web`) that brings Gaarf's full query parsing, customizers, and REST execution capabilities directly to client-side web applications:

- **Zero Node Dependencies:** Bundled for standard ESM in browsers without `fs`, `path`, or Node-specific libraries.
- **Browser-Based Auth:** Works seamlessly with OAuth 2.0 access tokens obtained in the frontend (e.g., via Google Identity Services / GIS `google.accounts.oauth2`).
- **Dynamic Schema Discovery & Caching:** Uses `WebSchemaLoader` to dynamically load and cache Google Ads API schemas from Google's Discovery REST API into browser `localStorage`, with bundled fallback schemas.
- **Client-Side Data Processing:** Perform complex GAQL querying, virtual column calculations (e.g. `clicks / impressions`), field customizers, and macro substitutions directly in client apps (React, Angular, Vue, etc.) without requiring a backend server.

#### Browser Example

```ts
import {
  GoogleAdsApiClient,
  AdsQueryExecutor,
} from 'gaarf-web';

// Initialize with an OAuth access token obtained in the browser
const client = new GoogleAdsApiClient({
  access_token: tokenFromGoogleIdentityServices,
  login_customer_id: '1234567890', // optional manager account CID
});

const executor = new AdsQueryExecutor(client);
const queryText = `
  SELECT
    campaign.id,
    campaign.name,
    metrics.clicks,
    metrics.impressions,
    metrics.clicks / metrics.impressions AS ctr
  FROM campaign
  WHERE segments.date DURING LAST_30_DAYS
`;

const query = await executor.parseQuery(queryText);
const result = await executor.executeQueryAndParseToObjects(query, '1234567890');

console.log(result.rows);
// Output: [{ campaign_id: '123', campaign_name: 'Summer Sale', ctr: 0.045, ... }]
```

## Development

### Run TypeScript CLI directly

```shell
npm start -- <files> [options]
```

or using `ts-node`:

```shell
node -r ts-node/register src/cli.ts <files> [options]
```

### Build

```shell
npm run build
```

### Run Tests

```shell
npm test
```

## License

[Apache-2.0](LICENSE)

## Disclaimer

This is not an officially supported Google product.
