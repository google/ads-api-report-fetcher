# Google Ads API Report Fetcher (gaarf)
Node.js version of Google Ads API Report Fetcher tool a.k.a. `gaarf`.
Please see the full documentation in the root [README](../README.md).


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
if you cloned the repo into "ads-api-fetcher" folder (don't forget to run `npm i` first):
```shell
ads-api-fetcher/js/gaarf <files> [options]
```
or
```shell
node ads-api-fetcher/js/dist/cli.js <files> [options]
```


#### Config files
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

Besides an implicitly used .rc-file you can specify a config file explicitly
via `--config` option. In that case options from `--config` file will be merge
with .rc file.


##### Ads API config
There are two mechanisms for supplying Ads API configuration (developer token, etc ).
Either via a separated yaml-file whose name is set in `ads-config` argument or
via separated CLI arguments starting `ads.*` (e.g. `--ads.client_id`) or
via `.gaarfrc` file (`ads` object):
```json
{
 "ads": {
   "client_id": "...",
   "developer_token": ".."
 },
 "output": "bq",
}
```

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

## Docker
You can run Gaarf as a Docker container. At the moment we don't publish container images so you'll need to build it on you own.

### Build a container image
If you cloned the repo then you can just run `docker build` (see below) inside it (in js folder) with the local [Dockerfile](Dockerfile). Otherwise you can just download [Dockerfile](Dockerfile) into an empty folder:
```
curl -L https://raw.githubusercontent.com/google/ads-api-report-fetcher/main/js/Dockerfile > Dockerfile
```

To build an image with name 'gaarf' (the name is up to you but it'll use to run later) run the following command in a folder with `Dockerfile`:
```
sudo docker build . -t gaarf
```
Now you can run a container from this image.

### Run a container
For running a container you'll need: a list of ads scripts, a config with Ads API and other parameters, a service account key file to authenticate in Google Cloud APIs. You can either embed them into the image on build or supply in runtime when you run a container.

The aforementioned `Dockerfile` assumes the following:
* Ads scripts are placed in the `ads-scripts/` folder
* an Application Default Credentials is set with a service account key file as `/app/service_account.json`

So you can map your local files onto these pathes so that Gaarf inside a container will find them. Or copy them before building, so they will be embeded into the image.

This is an example of mapping local files into container, assuming you have `.gaarfrc` and `service_account.json` in the current folder:
```
sudo docker run --mount type=bind,source="$(pwd)/.gaarfrc",target=/app/.gaarfrc \
  --mount type=bind,source="$(pwd)/ads-scripts",target=/app/ads-scripts \
  --mount type=bind,source="$(pwd)/service_account.json",target=/app/service_account.json \
  gaarf ./ads-scripts/*.sql
```


# Development
## Run typescript directly
```
node -r ./node_modules/ts-node/register src/cli.ts ...
```

