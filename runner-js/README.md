
# Usage
>You need [Node.js](https://nodejs.org/) to run the tool.
v16 should be sufficient.

## Command Line
### Install globally
```shell
npm i ads-api-report-fetcher -g
```
then you can run the tool with `ads-fetcher` command.

### Running from folder
if you cloned the repo into "ads-api-fetcher" folder:
```shell
node ads-api-fetcher/runner-js/dist/cli.js <files> [options]
```
or
```shell
ads-api-fetcher/runner-js/cli-bin.js <file> [options]
```

The required positional argument is a file mask for files with queries (GAQL).
Glob patterns are supported. E.g. `./ads-queries/**/*.sql`.

All options are passed as: --option_name=options_value or --option_name (for flags).

Options:
* ads-config - a path to yaml file with config for Google Ads,
               by default assuming 'google-ads.yaml' in the current folder
* customer - customer id, also can be specified in google-ads.yaml as 'customer-id'
             customer-id and customer_id are also supported
* output - output type,
           values:
           * csv - write data to CSV files
           * bg or bigquery - write data to BigQuery

Options specific for CSV writer:
* destination-folder - output folder where csv files will be created

Options specific for BigQuery writer:
* bq-project - GCP project id
* bq-dataset - BigQuery dataset id where tables with output data will be created
* dump-schema - flags that enable dumping json with schema for tables

All other parameters are treated as parameters for queries and can be used inside
query as `{param}`.
For example if we pass parameters: `--start_date=2021-12-01 --end_date=2022-02-28`
then inside sql we can use them:
```sql
    AND segments.date >= "{start_date}"
    AND segments.date <= "{end_date}"
```

Full example:
```
ads-api-fetcher/runner-js/cli-bin.js google_ads_queries/*.sql --ads-config=google-ads.yaml --customer=6368728866 --output=bq --start_date=2021-12-01 --end_date=2022-02-28 --bq-project=my_project --bq-dataset=my_dataset
```

## Library

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

# Development
## Run typescript directly
```
node -r ./node_modules/ts-node/register src/cli.ts ...
```

