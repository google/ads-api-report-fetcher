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
const path_1 = __importDefault(require("path"));
const yargs_1 = __importDefault(require("yargs"));
const helpers_1 = require("yargs/helpers");
const ads_api_client_1 = require("./lib/ads-api-client");
const ads_query_executor_1 = require("./lib/ads-query-executor");
const bq_writer_1 = require("./lib/bq-writer");
const console_writer_1 = require("./lib/console-writer");
const csv_writer_1 = require("./lib/csv-writer");
const file_utils_1 = require("./lib/file-utils");
const utils_1 = require("./lib/utils");
const configPath = find_up_1.default.sync(['.gaarfrc', '.gaarfrc.json']);
const configObj = configPath ? JSON.parse(fs_1.default.readFileSync(configPath, 'utf-8')) : {};
const argv = (0, yargs_1.default)((0, helpers_1.hideBin)(process.argv))
    .scriptName('gaarf')
    .command('$0 <files..>', 'Execute ads queries (GAQL)', {})
    .positional('files', {
    array: true,
    type: 'string',
    description: 'List of files with Ads queries (can be gcs:// resources)'
})
    // .command(
    //     'bigquery <files>', 'Execute BigQuery queries',
    //     {'bq.project': {type: 'string', description: 'GCP project'}})
    // NOTE: when/if we introduce another command, then all options will
    //       move to the defaul command's suboptions
    //       But having them at root level is better for TS typings
    .option('ads-config', { type: 'string', description: 'path to yaml config for Google Ads' })
    .option('ads', { hidden: true })
    .option('ads.developer_token', { type: 'string', description: 'Ads API developer token' })
    .option('ads.client_id', { type: 'string', description: 'OAuth client_id' })
    .option('ads.client_secret', { type: 'string', description: 'OAuth client_secret' })
    .option('ads.refresh_token', { type: 'string', description: 'OAuth refresh token' })
    .option('ads.login_customer_id', {
    type: 'string',
    description: 'Ads API login account (can be the same as account argument)'
})
    .option('account', {
    alias: ['customer', 'customer-id', 'customer_id'],
    type: 'string',
    description: 'Google Ads account id (w/o dashes), a.k.a customer id'
})
    .option('customer-ids-query', {
    alias: ['customer_ids_query'],
    type: 'string',
    description: 'GAQL query that refines for which accounts to execute scripts'
})
    .option('customer-ids-query-file', {
    alias: ['customer_ids_query_file'],
    type: 'string',
    description: 'Same as customer-ids-query but a file path to a query script'
})
    .option('output', {
    choices: ['csv', 'bq', 'bigquery', 'console'],
    alias: 'o',
    description: 'output writer to use'
})
    .option('sync', {
    type: 'boolean',
    description: 'Queries will be executed for each customer synchronously (otherwise in parallel)'
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
    .option('bq.no-union-view', {
    type: 'boolean',
    description: 'disable creation of union views (combining data from customer\'s table'
})
    .group([
    'bq.project', 'bq.dataset', 'bq.dump-schema', 'bq.table-template',
    'bq.location', 'bq.no-union-view'
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
        opts.noUnionView = argv.bq['no-union-view'];
        return new bq_writer_1.BigQueryWriter(projectId, dataset, opts);
    }
    throw new Error(`Unknown output format: '${output}'`);
}
async function main() {
    var _a, _b;
    if (argv.account) {
        argv.account = argv.account.toString();
    }
    console.log(chalk_1.default.gray(JSON.stringify(argv, null, 2)));
    let adsConfig;
    let configFilePath = argv.adsConfig;
    if (configFilePath) {
        // try to use ads config from extenral file (ads-config arg)
        adsConfig = loadAdsConfig(configFilePath, argv.account);
    }
    else {
        // try to use ads config from explicit cli arguments
        if (argv.ads) {
            let ads_cfg = argv.ads;
            adsConfig = {
                client_id: ads_cfg.client_id || '',
                client_secret: ads_cfg.client_secret || '',
                developer_token: ads_cfg.developer_token || '',
                refresh_token: ads_cfg.refresh_token || '',
                login_customer_id: (_a = (ads_cfg.login_customer_id || argv.account || '')) === null || _a === void 0 ? void 0 : _a.toString(),
                customer_id: (_b = (argv.account || ads_cfg.login_customer_id || '')) === null || _b === void 0 ? void 0 : _b.toString(),
            };
        }
        else if (fs_1.default.existsSync('google-ads.yaml')) {
            adsConfig = loadAdsConfig('google-ads.yaml', argv.account);
        }
        else {
            // TODO: support searching google-ads.yaml in user home folder (?)
            console.log(chalk_1.default.red(`Neither Ads API config file was not specified ('ads-config' agrument) nor ads.* arguments (either explicitly or via .gaarfrc config)`));
            process.exit(-1);
        }
    }
    console.log(chalk_1.default.gray('Using ads config:'));
    console.log(adsConfig);
    let client = new ads_api_client_1.GoogleAdsApiClient(adsConfig, argv.account);
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
    let customer_ids_query = "";
    if (argv.customer_ids_query) {
        customer_ids_query = argv.customer_ids_query;
    }
    else if (argv.customer_ids_query_file) {
        customer_ids_query = await (0, file_utils_1.getFileContent)(argv.customer_ids_query_file);
    }
    console.log(`Fetching customer ids ${customer_ids_query ? ' (using custom query)' : ''}`);
    let customers = await client.getCustomerIds(customer_ids_query);
    console.log(`Customers to process (${customers.length}):`);
    console.log(customers);
    let macros = argv['macro'] || {};
    let writer = getWriter(); // NOTE: create writer from argv
    let executor = new ads_query_executor_1.AdsQueryExecutor(client);
    let options = {
        skipConstants: argv.skipConstants,
        sync: argv.sync
    };
    console.log(`Found ${scriptPaths.length} script to process`);
    let started = new Date();
    for (let scriptPath of scriptPaths) {
        let queryText = await (0, file_utils_1.getFileContent)(scriptPath);
        console.log(`Processing query from ${scriptPath}`);
        let scriptName = path_1.default.basename(scriptPath).split('.sql')[0];
        await executor.execute(scriptName, queryText, customers, macros, writer, options);
        console.log();
    }
    let elapsed = (0, utils_1.getElapsed)(started);
    console.log(chalk_1.default.green('All done!') + ' ' + chalk_1.default.gray(`Elapsed: ${elapsed}`));
}
function loadAdsConfig(configFilepath, customerId) {
    if (!fs_1.default.existsSync(configFilepath)) {
        console.log(chalk_1.default.red(`Config file ${configFilepath} does not exist`));
        process.exit(-1);
    }
    try {
        return (0, ads_api_client_1.loadAdsConfigYaml)(configFilepath, customerId);
    }
    catch (e) {
        console.log(chalk_1.default.red(`Failed to load Ads API configuration from ${configFilepath}: ${e}`));
        process.exit(-1);
    }
}
main().catch(console.error);
//# sourceMappingURL=cli.js.map