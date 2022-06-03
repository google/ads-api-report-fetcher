
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

Documentation on available options see in the root [README.md](../README.md).

### Running from folder
if you cloned the repo into "ads-api-fetcher" folder:
```shell
ads-api-fetcher/runner-js/cli-bin <files> [options]
```
or
```shell
node ads-api-fetcher/runner-js/dist/cli.js <files> [options]
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
via `--config` option. In that case options from `--config` file will be merge
with .rc file.


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

# Development
## Run typescript directly
```
node -r ./node_modules/ts-node/register src/cli.ts ...
```

