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
const ads_api_client_1 = require("./lib/ads-api-client");
const ads_query_executor_1 = require("./lib/ads-query-executor");
const bq_writer_1 = require("./lib/bq-writer");
const console_writer_1 = require("./lib/console-writer");
const csv_writer_1 = require("./lib/csv-writer");
const file_utils_1 = require("./lib/file-utils");
const logger_1 = __importDefault(require("./lib/logger"));
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
    .option('ads-config', {
    type: 'string',
    description: 'path to yaml config for Google Ads (google-ads.yaml)'
})
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
    .conflicts('customer-ids-query', 'customer-ids-query-file')
    .option('output', {
    choices: ['csv', 'bq', 'bigquery', 'console'],
    alias: 'o',
    description: 'output writer to use'
})
    .option('loglevel', {
    alias: ['log-level', 'll', 'log_level'],
    choises: ['debug', 'verbose', 'info', 'warn', 'error'],
    description: 'Logging level. By default - \'info\', for output=console - \'warn\''
})
    // TODO: support parallel query execution (to catch up with Python)
    // .option('parallel-queries', {
    //   type: 'boolean',
    //   description: 'How queries are being processed: in parallel (true) or sequentially (false, default)',
    //   default: false
    // })
    .option('parallel-accounts', {
    type: 'boolean',
    description: 'How one query is being processed for multiple accounts: in parallel (true, default) or sequentially (false)',
    default: true
})
    .option('csv.destination-folder', {
    type: 'string',
    alias: 'csv.destination',
    description: 'output folder for generated CSV files'
})
    .option('console.transpose', {
    choices: ['auto', 'never', 'always'],
    default: 'auto',
    description: 'transposing tables: auto - transponse only if table does not fit in terminal window (default), always - transpose all the time, never - never transpose'
})
    .option('console.page_size', {
    type: 'number',
    description: 'Maximum row count to output per each script'
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
    .option('bq.dump-data', {
    type: 'boolean',
    description: 'flag that enables dumping json files with tables data'
})
    .option('bq.no-union-view', {
    type: 'boolean',
    description: 'disable creation of union views (combining data from customer\'s table'
})
    .option('bq.insert-method', {
    type: 'string',
    choices: ['insert-all', 'load-table'],
    hidden: true
})
    .option('skip-constants', {
    type: 'boolean',
    description: 'do not execute scripts for constant resources'
})
    .option('dump-query', { type: 'boolean' })
    .group([
    'bq.project', 'bq.dataset', 'bq.dump-schema', 'bq.table-template',
    'bq.location', 'bq.no-union-view', 'bq.dump-data', 'bq.insert-method'
], 'BigQuery writer options:')
    .group('csv.destination-folder', 'CSV writer options:')
    .group(['console.transpose', 'console.page_size'], 'Console writer options:')
    .env('GAARF')
    .config(configObj)
    .config('config', 'Path to JSON or YAML config file', function (configPath) {
    let content = fs_1.default.readFileSync(configPath, 'utf-8');
    if (configPath.endsWith('.yaml')) {
        return js_yaml_1.default.load(content);
    }
    return JSON.parse(content);
})
    .help()
    .example('$0 queries/**/*.sql --output=bq --bq.project=myproject --bq.dataset=myds', 'Execute ads queries and upload results to BigQuery, table per script')
    .example('$0 queries/**/*.sql --output=csv --csv.destination-folder=output', 'Execute ads queries and output results to csv files, one per script')
    .example('$0 queries/**/*.sql --config=gaarf.json', 'Execute ads queries with passing arguments via config file (can be json or yaml)')
    .epilog('(c) Google 2022. Not officially supported product.')
    .parseSync();
function getWriter() {
    let output = (argv.output || '').toString();
    if (output === '') {
        return new csv_writer_1.NullWriter();
    }
    if (output === 'console') {
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
        let bq_opts = argv.bq;
        opts.datasetLocation = bq_opts.location;
        opts.tableTemplate = bq_opts['table-template'];
        opts.dumpSchema = bq_opts['dump-schema'];
        opts.dumpData = bq_opts['dump-data'];
        opts.noUnionView = bq_opts['no-union-view'];
        opts.insertMethod = (bq_opts['insert-method'] || '').toLowerCase() === 'insert-all'
            ? bq_writer_1.BigQueryInsertMethod.insertAll : bq_writer_1.BigQueryInsertMethod.loadTable;
        logger_1.default.debug('BigQueryWriterOptions:');
        logger_1.default.debug(opts);
        return new bq_writer_1.BigQueryWriter(projectId, dataset, opts);
    }
    throw new Error(`Unknown output format: '${output}'`);
}
async function main() {
    var _a, _b;
    if (argv.account) {
        argv.account = argv.account.toString();
    }
    logger_1.default.verbose(JSON.stringify(argv, null, 2));
    let adsConfig = undefined;
    let adConfigFilePath = argv.adsConfig;
    if (adConfigFilePath) {
        // try to use ads config from extenral file (ads-config arg)
        adsConfig = await loadAdsConfig(adConfigFilePath, argv.account);
    }
    // try to use ads config from explicit cli arguments
    if (argv.ads) {
        let ads_cfg = argv.ads;
        adsConfig = Object.assign(adsConfig || {}, {
            client_id: ads_cfg.client_id || '',
            client_secret: ads_cfg.client_secret || '',
            developer_token: ads_cfg.developer_token || '',
            refresh_token: ads_cfg.refresh_token || '',
            login_customer_id: (_a = (ads_cfg.login_customer_id || argv.account || '')) === null || _a === void 0 ? void 0 : _a.toString(),
            customer_id: (_b = (argv.account || ads_cfg.login_customer_id || '')) === null || _b === void 0 ? void 0 : _b.toString(),
        });
    }
    else if (!adConfigFilePath && fs_1.default.existsSync('google-ads.yaml')) {
        // load a default google-ads if it wasn't explicitly specified
        // TODO: support searching google-ads.yaml in user home folder (?)
        adsConfig = await loadAdsConfig('google-ads.yaml', argv.account);
    }
    if (!adsConfig) {
        console.log(chalk_1.default.red(`Neither Ads API config file was specified ('ads-config' agrument) nor ads.* arguments (either explicitly or via config files) nor google-ads.yaml found. Exiting`));
        process.exit(-1);
    }
    logger_1.default.verbose('Using ads config:');
    logger_1.default.verbose(JSON.stringify(adsConfig, null, 2));
    let client = new ads_api_client_1.GoogleAdsApiClient(adsConfig, argv.account);
    let executor = new ads_query_executor_1.AdsQueryExecutor(client);
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
        process.exit(-1);
    }
    let scriptPaths = argv.files;
    if (argv.output === 'console') {
        // for console writer by default increase default log level to 'warn' (to
        // hide all auxillary info)
        logger_1.default.transports.forEach((transport) => {
            if (transport.name === 'console' && !argv.loglevel) {
                transport.level = 'warn';
            }
        });
    }
    let customer_ids_query = '';
    if (argv.customer_ids_query) {
        customer_ids_query = argv.customer_ids_query;
    }
    else if (argv.customer_ids_query_file) {
        customer_ids_query =
            await (0, file_utils_1.getFileContent)(argv.customer_ids_query_file);
    }
    logger_1.default.info(`Fetching customer ids ${customer_ids_query ? '(using custom query)' : ''}`);
    let customers = await client.getCustomerIds();
    logger_1.default.verbose(`Customer ids from the root account ${client.root_cid} (${customers.length}):`);
    logger_1.default.verbose(customers);
    if (customer_ids_query) {
        logger_1.default.verbose(`Fetching customer ids with custom query`);
        logger_1.default.debug(customer_ids_query);
        customers = await executor.getCustomerIds(customers, customer_ids_query);
    }
    if (customers.length === 0) {
        console.log(chalk_1.default.redBright(`No customers found for processing`));
        process.exit(-1);
    }
    logger_1.default.info(`Customers to process (${customers.length}):`);
    logger_1.default.info(customers);
    let macros = argv["macro"] || {};
    let writer = getWriter(); // NOTE: create writer from argv
    let options = {
        skipConstants: argv.skipConstants,
        parallelAccounts: argv.parallelAccounts,
        dumpQuery: argv.dumpQuery,
    };
    logger_1.default.info(`Found ${scriptPaths.length} script to process`);
    logger_1.default.debug(JSON.stringify(scriptPaths, null, 2));
    let started = new Date();
    for (let scriptPath of scriptPaths) {
        let queryText = await (0, file_utils_1.getFileContent)(scriptPath);
        logger_1.default.info(`Processing query from ${chalk_1.default.gray(scriptPath)}`);
        let scriptName = path_1.default.basename(scriptPath).split('.sql')[0];
        let started_script = new Date();
        await executor.execute(scriptName, queryText, customers, macros, writer, options);
        let elapsed_script = (0, utils_1.getElapsed)(started_script);
        logger_1.default.info(`Query from ${chalk_1.default.gray(scriptPath)} processing for all customers completed. Elapsed: ${elapsed_script}`);
    }
    let elapsed = (0, utils_1.getElapsed)(started);
    logger_1.default.info(chalk_1.default.green('All done!') + ' ' + chalk_1.default.gray(`Elapsed: ${elapsed}`));
}
async function loadAdsConfig(configFilepath, customerId) {
    try {
        return (0, ads_api_client_1.loadAdsConfigFromFile)(configFilepath, customerId);
    }
    catch (e) {
        console.log(chalk_1.default.red(`Failed to load Ads API configuration from ${configFilepath}: ${e}`));
        process.exit(-1);
    }
}
main().catch(console.error);
//# sourceMappingURL=cli.js.map