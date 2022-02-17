"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const fs_1 = __importDefault(require("fs"));
const lodash_1 = __importDefault(require("lodash"));
const path_1 = __importDefault(require("path"));
const ads_query_executor_1 = require("./ads-query-executor");
const api_client_1 = require("./api-client");
const csv_writer_1 = require("./csv-writer");
const bq_writer_1 = require("./bq-writer");
const argv = require('yargs/yargs')(process.argv.slice(2)).argv;
function getWriter(argv) {
    let output = (argv['output'] || '').toString();
    if (output === '') {
        // return new ConsoleWriter();
        throw new Error('Not implemented');
    }
    if (output === 'csv') {
        return new csv_writer_1.CsvWriter(argv['destination-folder']);
    }
    if (output === 'bq' || output === 'bigquery') {
        let projectId = argv['bq_project'] || argv['bq-project'];
        let dataset = argv['bq_dataset'] || argv['bq-dataset'];
        let opts = {};
        opts.tableSuffix = argv['table-suffix'];
        opts.dumpSchema = argv['dump-schema'];
        return new bq_writer_1.BigQueryWriter(projectId, dataset, opts);
    }
    throw new Error(`Unknown output format: '${output}'`);
}
async function main() {
    let configFilePath = argv['ads-config'];
    if (!configFilePath) {
        if (fs_1.default.existsSync('google-ads.yaml')) {
            configFilePath = 'google-ads.yaml';
        }
        else {
            throw new Error(`Config file was not specified, pass 'ads-config' agrument`);
        }
    }
    let customerId = argv['customer'] || argv['customer-id'] || argv['customer_id'];
    if (!argv['_'] || !argv['_'].length) {
        console.log(`Please specify a positional argument with a file path mask for queries (e.g. ./ads-queries/**/*.sql)`);
        return;
    }
    let scriptPaths = argv['_'];
    let client = new api_client_1.GoogleAdsApiClient(configFilePath, customerId);
    console.log('Fetching customer ids');
    let customers = await client.getCustomerIds();
    console.log(`Customers to process:`);
    console.log(customers);
    let params = lodash_1.default.omit(argv, ['_', 'customer-id', 'ads-config', 'format', 'destination-folder']);
    let writer = getWriter(argv);
    let executor = new ads_query_executor_1.AdsQueryExecutor(client);
    for (let scriptPath of scriptPaths) {
        let queryText = fs_1.default.readFileSync(scriptPath.trim(), 'utf-8');
        console.log(`Processing query from ${scriptPath}`);
        let scriptName = path_1.default.basename(scriptPath).split('.sql')[0];
        // TODO: parallelirize
        //await writer.beginScript(scriptPath);
        // if script has 'constant' in its name, break the loop over customers (it
        // shouldn't depend on one)
        if (path_1.default.basename(scriptPath).indexOf('constant') >= 0) {
            await executor.execute(scriptName, queryText, [customers[0]], params, writer);
        }
        else {
            await executor.execute(scriptName, queryText, customers, params, writer);
        }
        //await writer.endScript();
    }
}
main().catch(console.error);
//# sourceMappingURL=cli.js.map