"use strict";
/**
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const chalk_1 = __importDefault(require("chalk"));
const find_up_1 = __importDefault(require("find-up"));
const fs_1 = __importDefault(require("fs"));
const js_yaml_1 = __importDefault(require("js-yaml"));
const path_1 = __importDefault(require("path"));
const yargs_1 = __importDefault(require("yargs"));
const helpers_1 = require("yargs/helpers");
const ads_query_executor_1 = require("./lib/ads-query-executor");
const api_client_1 = require("./lib/api-client");
const bq_writer_1 = require("./lib/bq-writer");
const console_writer_1 = require("./lib/console-writer");
const csv_writer_1 = require("./lib/csv-writer");
const configPath = find_up_1.default.sync(['.gaarfrc', '.gaarfrc.json']);
const configObj = configPath ? JSON.parse(fs_1.default.readFileSync(configPath, 'utf-8')) : {};
const argv = (0, yargs_1.default)((0, helpers_1.hideBin)(process.argv))
    .scriptName('gaarf')
    .command('$0 <files..>', 'Execute ads queries (GAQL)', {})
    .positional('files', {
    array: true,
    type: 'string',
    description: 'list of files with Ads queries'
})
    // .command(
    //     'bigquery <files>', 'Execute BigQuery queries',
    //     {'bq.project': {type: 'string', description: 'GCP project'}})
    // NOTE: when/if we introduce another command, then all options will
    //       move to the defaul command's suboptions
    //       But having them at root level is better for TS typings
    .option('ads-config', { type: 'string', description: 'path to yaml config for Google Ads' })
    .option('account', {
    alias: ['customer', 'customer-id', 'customer_id'],
    type: 'string',
    description: 'Google Ads account id (w/o dashes), a.k.a customer id'
})
    .option('output', {
    choices: ['csv', 'bq', 'bigquery', 'console'],
    alias: 'o',
    description: 'output writer to use'
})
    .option('csv.destination-folder', {
    type: 'string',
    description: 'output folder for generated CSV files'
})
    .option('bq', { hidden: true })
    .option('csv', { hidden: true })
    .option('console', { hidden: true })
    .option('bq.project', { type: 'string', description: 'GCP project id for BigQuery' })
    .option('bq.dataset', {
    type: 'string',
    description: 'BigQuery dataset id where tables will be created'
})
    .option('bq.location', { type: 'string', description: 'BigQuery dataset location' })
    .option('bq.table-template', {
    type: 'string',
    description: 'template for tables names, you can use {script} macro inside'
})
    .option('bq.dump-schema', {
    type: 'boolean',
    description: 'flag that enables dumping json files with schemas for tables'
})
    .group([
    'bq.project', 'bq.dataset', 'bq.dump-schema', 'bq.table-template',
    'bq.location'
], 'BigQuery writer options:')
    .group('csv.destination-folder', 'CSV writer options:')
    .option('skip-constants', {
    type: 'boolean',
    description: 'do not execute scripts for constant resources'
})
    .config(configObj)
    .config()
    .help()
    .example('$0 queries/**/*.sql --output=bq --bq.project=myproject --bq.dataset=myds', 'Execute ads queries and upload results to BigQuery, table per script')
    .example('$0 queries/**/*.sql --output=csv --csv.destination-folder=output', 'Execute ads queries and output results to csv files, one per script')
    .epilog('(c) Google 2022. Not officially supported product.')
    .parseSync();
function getWriter() {
    let output = (argv.output || '').toString();
    if (output === '') {
        return new csv_writer_1.NullWriter();
    }
    if (output === 'console') {
        // TODO:
        return new console_writer_1.ConsoleWriter(argv.console);
    }
    if (output === 'csv') {
        return new csv_writer_1.CsvWriter(argv.csv);
    }
    if (output === 'bq' || output === 'bigquery') {
        // TODO: move all options to BigQueryWriterOptions
        if (!argv.bq) {
            throw new Error(`For BigQuery writer (---output=bq) we should specify at least a project and a dataset id`);
        }
        let projectId = argv.bq.project;
        let dataset = argv.bq.dataset;
        if (!projectId) {
            console.warn(`bq.project option should be specified (GCP project id)`);
            process.exit(-1);
        }
        if (!dataset) {
            console.warn(`bq.dataset option should be specified (BigQuery dataset id)`);
            process.exit(-1);
        }
        let opts = {};
        opts.datasetLocation = argv.bq.location;
        opts.tableTemplate = argv.bq['table-template'];
        opts.dumpSchema = argv.bq['dump-schema'];
        return new bq_writer_1.BigQueryWriter(projectId, dataset, opts);
    }
    throw new Error(`Unknown output format: '${output}'`);
}
async function main() {
    if (argv.account) {
        argv.account = argv.account.toString();
    }
    console.log(chalk_1.default.gray(JSON.stringify(argv, null, 2)));
    // TODO: support ads api settings in main config and as cli arguments
    let configFilePath = argv.adsConfig;
    if (!configFilePath) {
        if (fs_1.default.existsSync('google-ads.yaml')) {
            configFilePath = 'google-ads.yaml';
        }
        else {
            console.log(chalk_1.default.red(`Ads API config file was not specified (use 'ads-config' agrument) and hasn't found in the current folder`));
            process.exit(-1);
        }
        // TODO: support searching google-ads.yaml in user home folder (?)
    }
    let adsConfig = loadAdsConfig(configFilePath, argv.account);
    let client = new api_client_1.GoogleAdsApiClient(adsConfig, argv.account);
    // NOTE: a note regarding the 'files' argument
    // normaly on *nix OSes (at least in bash and zsh) passing an argument
    // with mask like *.sql will expand it to a list of files (see
    // https://zsh.sourceforge.io/Doc/Release/Expansion.html, 14.8 Filename
    // Generation,
    // https://www.gnu.org/software/bash/manual/html_node/Filename-Expansion.html)
    // So, actually the tool accepts already expanding list of files, and
    // if we want to support blog patterns as parameter (for example for calling
    // from outside zsh/bash) then we have to handle items in `files` argument and
    // expand them using glob rules
    if (!argv.files || !argv.files.length) {
        console.log(chalk_1.default.redBright(`Please specify a positional argument with a file path mask for queries (e.g. ./ads-queries/**/*.sql)`));
        return;
    }
    let scriptPaths = argv.files;
    console.log('Fetching customer ids');
    let customers = await client.getCustomerIds();
    console.log(`Customers to process:`);
    console.log(customers);
    let params = argv['sql'] || {};
    let writer = getWriter(); // NOTE: create writer from argv
    let executor = new ads_query_executor_1.AdsQueryExecutor(client);
    let options = {
        skipConstants: argv.skipConstants
    };
    console.log(`Found ${scriptPaths.length} script to process`);
    for (let scriptPath of scriptPaths) {
        let queryText = fs_1.default.readFileSync(scriptPath.trim(), 'utf-8');
        console.log(`Processing query from ${scriptPath}`);
        let scriptName = path_1.default.basename(scriptPath).split('.sql')[0];
        await executor.execute(scriptName, queryText, customers, params, writer, options);
        console.log();
    }
    console.log(chalk_1.default.green('All done!'));
}
function loadAdsConfig(configFilepath, customerId) {
    var _a, _b;
    if (!fs_1.default.existsSync(configFilepath)) {
        console.log(chalk_1.default.red(`Config file ${configFilepath} does not exist`));
        process.exit(-1);
    }
    try {
        const doc = js_yaml_1.default.load(fs_1.default.readFileSync(configFilepath, 'utf8'));
        console.log(chalk_1.default.gray('Using ads config:'));
        console.log(doc);
        return {
            developer_token: doc['developer_token'],
            client_id: doc['client_id'],
            client_secret: doc['client_secret'],
            refresh_token: doc['refresh_token'],
            login_customer_id: (_a = doc['login_customer_id']) === null || _a === void 0 ? void 0 : _a.toString(),
            customer_id: (_b = (customerId || doc['customer_id'] || doc['login_customer_id'])) === null || _b === void 0 ? void 0 : _b.toString()
        };
    }
    catch (e) {
        console.log(chalk_1.default.red(`Failed to load Ads API configuration from ${configFilepath}: ${e}`));
        process.exit(-1);
    }
}
main().catch(console.error);
//# sourceMappingURL=cli.js.map