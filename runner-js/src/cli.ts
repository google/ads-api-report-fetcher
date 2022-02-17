import fs from 'fs';
import _ from 'lodash';
import path from 'path';
import {AdsQueryExecutor} from './ads-query-executor';
import {GoogleAdsApiClient} from './api-client';
import {CsvWriter} from './csv-writer';
import {BigQueryWriteOptions, BigQueryWriter} from './bq-writer';
import {IResultWriter, QueryElements} from './types';
const argv = require('yargs/yargs')(process.argv.slice(2)).argv;
// TODO: describe cli arguments for help

function getWriter(argv: any): IResultWriter {
  let output = (argv['output'] || '').toString();
  if (output === '') {
    return new NullWriter();
  }
  if (output === 'csv') {
    return new CsvWriter(argv['destination-folder']);
  }
  if (output === 'bq' || output === 'bigquery') {
    let projectId = argv['bq_project'] || argv['bq-project'];
    let dataset = argv['bq_dataset'] || argv['bq-dataset'];
    let opts: BigQueryWriteOptions = {};
    opts.tableSuffix = argv['table-suffix'];
    opts.dumpSchema = argv['dump-schema'];
    return new BigQueryWriter(projectId, dataset, opts);
  }

  throw new Error(`Unknown output format: '${output}'`);
}

export class NullWriter implements IResultWriter {
  beginScript(scriptName: string, query: QueryElements): void | Promise<void> {
  }
  endScript(): void | Promise<void> {
  }
  beginCustomer(customerId: string): void | Promise<void> {
  }
  endCustomer(): void | Promise<void> {
  }
  addRow(parsedRow: any[]): void {
  }
}

async function main() {
  let configFilePath = argv['ads-config'];
  if (!configFilePath) {
    if (fs.existsSync('google-ads.yaml')) {
      configFilePath = 'google-ads.yaml';
    } else {
      throw new Error(
          `Config file was not specified, pass 'ads-config' agrument`);
    }
  }

  let customerId =
      argv['customer'] || argv['customer-id'] || argv['customer_id'];
  if (!argv['_'] || !argv['_'].length) {
    console.log(
        `Please specify a positional argument with a file path mask for queries (e.g. ./ads-queries/**/*.sql)`);
    return;
  }
  let scriptPaths = argv['_'];
  let client = new GoogleAdsApiClient(configFilePath, customerId);
  console.log('Fetching customer ids');
  let customers = await client.getCustomerIds();
  console.log(`Customers to process:`);
  console.log(customers);

  let params = _.omit(
      argv, ['_', 'customer-id', 'ads-config', 'format', 'destination-folder']);
  let writer = getWriter(argv);
  let executor = new AdsQueryExecutor(client);

  console.log(`Found ${scriptPaths.length} script to process`);
  for (let scriptPath of scriptPaths) {
    let queryText = fs.readFileSync(scriptPath.trim(), 'utf-8');
    console.log(`Processing query from ${scriptPath}`);

    let scriptName = path.basename(scriptPath).split('.sql')[0];
    // if script has 'constant' in its name, break the loop over customers (it
    // shouldn't depend on one)
    if (path.basename(scriptPath).indexOf('constant') >= 0) {
      await executor.execute(scriptName, queryText, [customers[0]], params, writer);
    } else {
      await executor.execute(scriptName, queryText, customers, params, writer);
    }
    console.log();
  }
  console.log('All done!');
}

main().catch(console.error);
