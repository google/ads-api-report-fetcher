/* eslint-disable n/no-process-exit */
/**
 * Copyright 2025 Google LLC
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
import chalk from 'chalk';
import { findUpSync } from 'find-up';
import fs from 'fs';
import yaml from 'js-yaml';
import yargs from 'yargs';
import { hideBin } from 'yargs/helpers';
import { GoogleAdsRpcApiClient, GoogleAdsRestApiClient, } from './lib/ads-api-client.js';
import { AdsQueryExecutor, AdsApiVersion, } from './lib/ads-query-executor.js';
import { BigQueryInsertMethod, BigQueryWriter, } from './lib/bq-writer.js';
import { ConsoleWriter } from './lib/console-writer.js';
import { CsvWriter, JsonWriter, NullWriter, } from './lib/file-writers.js';
import { getFileContent } from './lib/file-utils.js';
import { getLogger } from './lib/logger.js';
import { getElapsed } from './lib/utils.js';
import { ConsoleQueryReader, FileQueryReader } from './lib/query-reader.js';
import { filterCustomerIds, getCustomerIds, getCustomerInfo, loadAdsConfigFromFile, parseCustomerIds, } from './lib/ads-utils.js';
const configPath = findUpSync(['.gaarfrc', '.gaarfrc.json']);
const configObj = configPath
    ? JSON.parse(fs.readFileSync(configPath, 'utf-8'))
    : {};
const logger = getLogger();
const argv = yargs(hideBin(process.argv))
    .scriptName('gaarf')
    .wrap(yargs().terminalWidth())
    .version()
    .alias('v', 'version')
    .command('validate', 'Validate Ads configuration')
    .command('account-tree', 'Display info about a customer account')
    .command('$0 <files..>', 'Execute ads queries (GAQL)', {})
    .positional('files', {
    array: true,
    type: 'string',
    description: 'List of files (or wildcards) with Ads queries (can be gs:// resources)',
})
    // .command(
    //     'bigquery <files>', 'Execute BigQuery queries',
    //     {'bq.project': {type: 'string', description: 'GCP project'}})
    // NOTE: when/if we introduce another command, then all options will
    //       move to the defaul command's suboptions
    //       But having them at root level is better for TS typings
    .option('ads-config', {
    type: 'string',
    description: 'path to yaml config for Google Ads (google-ads.yaml)',
})
    .option('ads', { hidden: true })
    .option('ads.developer_token', {
    type: 'string',
    description: 'Ads API developer token',
})
    .option('ads.client_id', { type: 'string', description: 'OAuth client_id' })
    .option('ads.client_secret', {
    type: 'string',
    description: 'OAuth client_secret',
})
    .option('ads.refresh_token', {
    type: 'string',
    description: 'OAuth refresh token',
})
    .option('ads.login_customer_id', {
    type: 'string',
    description: 'Ads API login account (can be the same as account argument)',
})
    .option('account', {
    alias: ['customer', 'customer-id', 'customer_id'],
    type: 'string',
    description: 'Google Ads account id (w/o dashes), a.k.a customer id or multiple accounts separeted with comma',
})
    .option('customer-ids-query', {
    alias: ['customer_ids_query'],
    type: 'string',
    description: 'GAQL query that refines for which accounts to execute scripts',
})
    .option('customer-ids-query-file', {
    alias: ['customer_ids_query_file'],
    type: 'string',
    description: 'Same as customer-ids-query but a file path to a query script',
})
    .conflicts('customer-ids-query', 'customer-ids-query-file')
    .option('disable-account-expansion', {
    alias: ['disable_account_expansion'],
    type: 'boolean',
    descriptions: 'Disable MCC account expansion',
})
    .option('input', {
    choices: ['console', 'file'],
    description: 'Different types of input besides the default file input',
})
    .option('output', {
    choices: ['csv', 'bq', 'bigquery', 'console', 'json'],
    alias: 'o',
    description: 'Output writer to use',
})
    .option('loglevel', {
    alias: ['log-level', 'll', 'log_level'],
    choises: ['off', 'debug', 'verbose', 'info', 'warn', 'error'],
    description: "Logging level. By default - 'info', for output=console - 'warn'",
})
    // TODO: support parallel query execution (to catch up with Python)
    // .option('parallel-queries', {
    //   type: 'boolean',
    //   description: 'How queries are being processed: in parallel (true) or sequentially (false, default)',
    //   default: false
    // })
    .option('parallel-accounts', {
    type: 'boolean',
    description: 'How one query is being processed for multiple accounts: in parallel (true) or sequentially (false). By default - in parallel',
    default: true,
})
    .option('parallel-threshold', {
    type: 'number',
    description: 'The maximum number of parallel queries',
})
    .option('csv.output-path', {
    type: 'string',
    alias: ['csv.destination', 'csv.destination-folder'],
    description: 'Output folder for generated CSV files (can be gs://)',
})
    .option('csv.file-per-customer', {
    type: 'boolean',
})
    .option('csv.array-separator', {
    type: 'string',
    description: 'Arrays separator symbol',
})
    .option('csv.quoted', {
    type: 'boolean',
    description: 'Wrap values in quotes',
})
    .option('json.format', {
    type: 'string',
    description: 'output format: json or jsonl (JSON Lines)',
})
    .option('json.value-format', {
    type: 'string',
    description: 'value format: arrays (values as arrays), objects (values as objects), raw (raw output)',
})
    .option('json.output-path', {
    type: 'string',
    alias: ['json.destination', 'json.destination-folder'],
    description: 'Output folder for generated JSON files (can be gs://)',
})
    .option('json.file-per-customer', {
    type: 'boolean',
})
    .option('console.transpose', {
    choices: ['auto', 'never', 'always'],
    default: 'auto',
    description: 'Transposing tables: auto - transponse only if table does not fit in terminal window (default), always - transpose all the time, never - never transpose',
})
    .option('console.page-size', {
    type: 'number',
    alias: ['maxrows', 'max-rows', 'page_size'],
    description: 'Maximum rows count to output per each script',
})
    .option('bq', { hidden: true })
    .option('csv', { hidden: true })
    .option('console', { hidden: true })
    .option('bq.project', {
    type: 'string',
    description: 'GCP project id for BigQuery',
})
    .option('bq.dataset', {
    type: 'string',
    description: 'BigQuery dataset id where tables will be created',
})
    .option('bq.location', {
    type: 'string',
    description: 'BigQuery dataset location',
})
    .option('bq.table-template', {
    type: 'string',
    description: 'Template for tables names, you can use {script} macro inside',
})
    .option('bq.dump-schema', {
    type: 'boolean',
    description: 'Flag that enables dumping json files with schemas for tables',
})
    .option('bq.dump-data', {
    type: 'boolean',
    description: 'Flag that enables dumping json files with tables data',
})
    .option('bq.no-union-view', {
    type: 'boolean',
    description: "Disable creation of union views (combining data from customer's tables)",
})
    .option('bq.insert-method', {
    type: 'string',
    choices: ['insert', 'load'],
    hidden: true,
})
    .option('bq.array-handling', {
    type: 'string',
    choices: ['arrays', 'strings'],
    description: 'Arrays handling (as arrays or as strings)',
})
    .option('bq.array-separator', {
    type: 'string',
    description: 'Arrays separator symbol (for array-handling=strings)',
})
    .option('bq.key-file-path', {
    type: 'string',
    description: 'A path to a service account key file for BigQuery authentication',
})
    .option('skip-constants', {
    type: 'boolean',
    description: 'Do not execute scripts for constant resources',
})
    .option('dump-query', {
    type: 'boolean',
    description: 'Output GAQL quesries to console before execution',
})
    .option('api', {
    type: 'string',
    choices: ['grpc', 'rest'],
    description: 'API to use: gRPC or REST',
})
    .option('api-version', {
    type: 'string',
    description: 'API versin (supported only for REST API)',
})
    .group([
    'bq.project',
    'bq.dataset',
    'bq.dump-schema',
    'bq.table-template',
    'bq.location',
    'bq.no-union-view',
    'bq.dump-data',
    'bq.insert-method',
    'bq.array-handling',
    'bq.array-separator',
    'bq.key-file-path',
], 'BigQuery writer options:')
    .group([
    'csv.output-path',
    'csv.file-per-customer',
    'csv.array-separator',
    'csv.quoted',
], 'CSV writer options:')
    .group([
    'json.output-path',
    'json.file-per-customer',
    'json.format',
    'json.value-format',
], 'JSON writer options:')
    .group(['console.transpose', 'console.page_size'], 'Console writer options:')
    .env('GAARF')
    .config(configObj)
    .config('config', 'Path to JSON or YAML config file', async (configPath) => {
    const content = await getFileContent(configPath);
    if (configPath.endsWith('.yaml')) {
        return yaml.load(content);
    }
    return JSON.parse(content);
})
    .usage(`Google Ads API Report Fetcher (gaarf) - a tool for executing Google Ads queries (aka reports, GAQL) with optional exporting to different targets (e.g. BigQuery, CSV) or dumping to the console.\n Built for Ads API ${AdsApiVersion}.`)
    .example('$0 queries/**/*.sql --output=bq --bq.project=myproject --bq.dataset=myds', 'Execute ads queries and upload results to BigQuery, table per script')
    .example('$0 queries/**/*.sql --output=csv --csv.destination-folder=output', 'Execute ads queries and output results to csv files, one per script')
    .example('$0 queries/**/*.sql --config=gaarf.json', 'Execute ads queries with passing arguments via config file')
    .epilog(`(c) Google 2022-${new Date().getFullYear()}. Not officially supported product.`)
    // TODO: .completion()
    .parseSync();
function getWriter() {
    const output = (argv.output || '').toString();
    if (output === '') {
        return new NullWriter();
    }
    if (output === 'console') {
        return new ConsoleWriter(argv.console);
    }
    if (output === 'csv') {
        return new CsvWriter(argv.csv);
    }
    if (output === 'json') {
        return new JsonWriter(argv.json);
    }
    if (output === 'bq' || output === 'bigquery') {
        // TODO: move all options to BigQueryWriterOptions
        if (!argv.bq) {
            console.warn('For BigQuery writer (---output=bq) you should specify at least a dataset id (--bq.dataset)');
            process.exit(-1);
        }
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const bq_opts = argv.bq;
        const dataset = bq_opts.dataset;
        if (!dataset) {
            console.warn('bq.dataset option should be specified (BigQuery dataset id)');
            process.exit(-1);
        }
        const projectId = bq_opts.project;
        if (!projectId) {
            console.warn("GCP project id was not specified explicitly (bq.project option), so we're using the current default project");
        }
        const opts = {};
        opts.datasetLocation = bq_opts.location;
        opts.tableTemplate = bq_opts['table-template'];
        opts.dumpSchema = bq_opts['dump-schema'];
        opts.dumpData = bq_opts['dump-data'];
        opts.noUnionView = bq_opts['no-union-view'];
        opts.insertMethod =
            (bq_opts['insert-method'] || '').toLowerCase() === 'insert'
                ? BigQueryInsertMethod.insertAll
                : BigQueryInsertMethod.loadTable;
        opts.arrayHandling = bq_opts['array-handling'];
        opts.arraySeparator = bq_opts['array-separator'];
        opts.keyFilePath = bq_opts['key-file-path'];
        opts.outputPath = bq_opts['output-path'];
        logger.debug('BigQueryWriterOptions:');
        logger.debug(opts);
        return new BigQueryWriter(projectId, dataset, opts);
    }
    // TODO: if (output === 'sqldb')
    throw new Error(`Unknown output format: '${output}'`);
}
function getReader() {
    const input = (argv.input || '').toString();
    if (input === 'console') {
        return new ConsoleQueryReader(argv.files);
    }
    return new FileQueryReader(argv.files);
}
function getApiClient(adsConfig) {
    let client;
    if (argv.api === 'rest') {
        client = new GoogleAdsRestApiClient(adsConfig, argv.apiVersion);
    }
    else {
        client = new GoogleAdsRpcApiClient(adsConfig);
        if (argv.apiVersion) {
            console.warn('api-version is not supported for gRPC API (api=grpc)');
        }
    }
    return client;
}
async function main() {
    logger.verbose(JSON.stringify(argv, null, 2));
    let adsConfig = undefined;
    const adConfigFilePath = argv.adsConfig;
    if (adConfigFilePath) {
        // try to use ads config from extenral file (ads-config arg)
        adsConfig = await loadAdsConfig(adConfigFilePath);
    }
    // try to use ads config from explicit cli arguments
    if (argv.ads) {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const ads_cfg = argv.ads;
        adsConfig = Object.assign(adsConfig || {}, {
            client_id: ads_cfg.client_id || '',
            client_secret: ads_cfg.client_secret || '',
            developer_token: ads_cfg.developer_token || '',
            refresh_token: ads_cfg.refresh_token || '',
            login_customer_id: ads_cfg.login_customer_id || '',
        });
    }
    else if (!adConfigFilePath && fs.existsSync('google-ads.yaml')) {
        // load a default google-ads if it wasn't explicitly specified
        // TODO: support searching google-ads.yaml in user home folder (?)
        adsConfig = await loadAdsConfig('google-ads.yaml');
    }
    if (!adsConfig) {
        if (argv.loglevel !== 'off') {
            console.log(chalk.red("Neither Ads API config file was specified ('ads-config' agrument) nor ads.* arguments (either explicitly or via config files) nor google-ads.yaml found. Exiting"));
        }
        process.exit(-1);
    }
    logger.verbose('Using ads config:');
    logger.verbose(JSON.stringify(Object.assign({}, adsConfig, {
        refresh_token: '<hidden>',
        developer_token: '<hidden>',
    }), null, 2));
    const customerIds = parseCustomerIds(argv.account, adsConfig);
    if (!customerIds || customerIds.length === 0) {
        if (argv.loglevel !== 'off') {
            console.log(chalk.red('No customer id/ids were provided. Exiting'));
        }
        process.exit(-1);
    }
    if (!adsConfig.login_customer_id && customerIds && customerIds.length === 1) {
        adsConfig.login_customer_id = customerIds[0];
    }
    const client = getApiClient(adsConfig);
    logger.info(`Using ${client.apiType} API (${client.apiVersion})`);
    const executor = new AdsQueryExecutor(client);
    if (argv._ && argv._[0] === 'validate') {
        try {
            await getCustomerIds(client, customerIds);
            if (argv.loglevel !== 'off') {
                console.log(chalk.green('Ads configuration has been validated'));
            }
            process.exit(0);
        }
        catch (e) {
            if (argv.loglevel !== 'off') {
                console.log('Validation of ads config has failed:');
                console.log(chalk.red(e));
            }
            process.exit(-1);
        }
    }
    else if (argv._ && argv._[0] === 'account-tree') {
        for (const cid of customerIds) {
            const info = await getCustomerInfo(client, cid);
            dumpCustomerInfo(info);
        }
        process.exit(0);
    }
    if (!argv.files || !argv.files.length) {
        // Besides two commands ("validate" and "account-tree") we treat all
        // positional args as file names
        if (argv.loglevel !== 'off') {
            console.log(chalk.redBright('Please specify a positional argument with a file path mask for queries (e.g. ./ads-queries/**/*.sql)'));
        }
        process.exit(-1);
    }
    if (argv.output === 'console') {
        // for console writer by default increase default log level to 'warn' (to
        // hide all auxillary info)
        logger.transports.forEach(transport => {
            // no `name` field in Transport's typeing but it does exist
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
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
        customer_ids_query = await getFileContent(argv.customer_ids_query_file);
    }
    let customers;
    if (argv.disable_account_expansion) {
        logger.info('Skipping account expansion because of disable_account_expansion flag');
        customers = customerIds;
    }
    else {
        // expand the provided accounts to leaf ones as they could be MMC accounts
        logger.info(`Expanding customer ids ${customer_ids_query ? '(using custom query)' : ''}`);
        customers = await getCustomerIds(client, customerIds);
        logger.verbose(`Customer ids from the root account(s) ${customerIds.join(',')} (${customers.length}):`);
        logger.verbose(customers);
        if (customer_ids_query) {
            logger.verbose('Filtering customer ids with custom query');
            logger.debug(customer_ids_query);
            try {
                customers = await filterCustomerIds(client, customers, customer_ids_query);
            }
            catch (e) {
                logger.error('Fetching customer ids using customer_ids_query failed: ' + e);
                process.exit(-1);
            }
        }
    }
    if (customers.length === 0) {
        if (argv.loglevel !== 'off') {
            console.log(chalk.redBright('No customers found for processing'));
        }
        process.exit(-1);
    }
    logger.info(`Customers to process (${customers.length}):`);
    logger.info(customers);
    const macros = (argv['macro'] || {});
    const templateParams = argv['template'];
    const writer = getWriter(); // NOTE: create writer from argv
    const reader = getReader(); // NOTE: create reader from argv
    const options = {
        skipConstants: argv.skipConstants,
        parallelAccounts: argv.parallelAccounts,
        parallelThreshold: argv.parallelThreshold,
        dumpQuery: argv.dumpQuery,
    };
    const started = new Date();
    for await (const query of reader) {
        const started_script = new Date();
        await executor.execute(query.name, query.text, customers, { macros, templateParams }, writer, options);
        const elapsed_script = getElapsed(started_script);
        logger.info(`Query from ${chalk.gray(query.name)} processing for all customers completed. Elapsed: ${elapsed_script}`);
    }
    const elapsed = getElapsed(started);
    logger.info(chalk.green('All done!') + ' ' + chalk.gray(`Elapsed: ${elapsed}`));
}
async function loadAdsConfig(configFilepath) {
    try {
        return loadAdsConfigFromFile(configFilepath);
    }
    catch (e) {
        if (argv.loglevel !== 'off') {
            console.log(chalk.red(`Failed to load Ads API configuration from ${configFilepath}: ${e}`));
        }
        process.exit(-1);
    }
}
function dumpCustomerInfo(info, level = 0) {
    const txt = `${info.id} - ${info.name} ${info.is_mcc ? ' - MCC' : ''}`;
    console.log('  '.repeat(level) + txt);
    if (info.children && info.children.length) {
        level += 1;
        for (const child of info.children) {
            dumpCustomerInfo(child, level);
        }
    }
}
main().catch(console.error);
//# sourceMappingURL=cli.js.map