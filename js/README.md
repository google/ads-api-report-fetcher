
# Usage
>You need [Node.js](https://nodejs.org/) to run the tool.
v16 should be sufficient.

## Command Line
### Install globally
```shell
npm i ads-api-report-fetcher -g
```
then you can run the tool with `gaarf` command:
```shell
gaarf <files> [options]
```

### Running from folder
if you cloned the repo into "ads-api-fetcher" folder:
```shell
ads-api-fetcher/runner-js/cli-bin <files> [options]
```
or
```shell
node ads-api-fetcher/runner-js/dist/cli.js <files> [options]
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
* `bq.table-template`  - template for tables names, `{script}` references script name
* `bq.dump-schema` - flag that enable dumping json files with schemas for tables

All parameters whose names start with the `sql.` prefix are passed to queries as params object.
For example if we pass parameters: `--sql.start_date=2021-12-01 --sql.end_date=2022-02-28`
then inside sql we can use `start_date` and `end_date`:
```sql
    AND segments.date >= "{start_date}"
    AND segments.date <= "{end_date}"
```

Full example:
```
ads-api-fetcher/runner-js/cli-bin google_ads_queries/*.sql --ads-config=google-ads.yaml --account=6368728866 --output=bq --sql.start_date=2021-12-01 --sql.end_date=2022-02-28 --bq.project=my_project --bq.dataset=my_dataset
```

#### Config files
Besides passing options explicitly you can use config files.
By default the tool will try to find `.gaarfrc` starting from the current folder
up to the root. If found, options from that file will be used if they weren't
supplied via command line.

Example of `.gaarfrc`:
```json
{
 "ads-config": ".config/google-ads.yaml",
 "output": "bq",
 "csv.destination-folder": "output",
 "sql": {
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
either as objects (see "sql") or as flatten names ("bq.project").

Besides an implicitly used .rc-file you can specify a config file explicitly
via `--config` option. In that case options from --config file will be merge
with .rc file.


See more help with `--help` option.


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

If you need to process results from queries (and not just rely on a writer) then
use `executeGen` method (it's a async generator):
```ts
  let results = await executor
    .executeGen(scriptName, queryText, customers, params, writer);
  for await (let res of results) {
    //res.rows - array of rows for one customer
  }
```

# Development
## Run typescript directly
```
node -r ./node_modules/ts-node/register src/cli.ts ...
```

