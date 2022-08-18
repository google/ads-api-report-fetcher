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

import chalk from 'chalk';
import findUp from 'find-up';
import fs from 'fs';
import yaml from 'js-yaml';
import _ from 'lodash';
import path from 'path';
import yargs from 'yargs'
import {hideBin} from 'yargs/helpers'

import {GoogleAdsApiClient, GoogleAdsApiConfig, loadAdsConfigYaml} from './lib/ads-api-client';
import {AdsQueryExecutor, AdsQueryExecutorOptions} from './lib/ads-query-executor';
import {BigQueryWriter, BigQueryWriterOptions} from './lib/bq-writer';
import {ConsoleWriter, ConsoleWriterOptions} from './lib/console-writer';
import {CsvWriter, CsvWriterOptions, NullWriter} from './lib/csv-writer';
import {getFileContent} from './lib/file-utils';
import logger from './lib/logger';
import {IResultWriter, QueryElements} from './lib/types';
import {getElapsed} from './lib/utils';

const configPath = findUp.sync(['.gaarfrc', '.gaarfrc.json'])
const configObj =
    configPath ? JSON.parse(fs.readFileSync(configPath, 'utf-8')) : {};

const argv =
    yargs(hideBin(process.argv))
        .scriptName('gaarf')
        .command('$0 <files..>', 'Execute ads queries (GAQL)', {})
        .positional('files', {
          array: true,
          type: 'string',
          description:
              'List of files with Ads queries (can be gcs:// resources)'
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
        .option('ads', {hidden: true})
        .option(
            'ads.developer_token',
            {type: 'string', description: 'Ads API developer token'})
        .option(
            'ads.client_id', {type: 'string', description: 'OAuth client_id'})
        .option(
            'ads.client_secret',
            {type: 'string', description: 'OAuth client_secret'})
        .option(
            'ads.refresh_token',
            {type: 'string', description: 'OAuth refresh token'})
        .option('ads.login_customer_id', {
          type: 'string',
          description:
              'Ads API login account (can be the same as account argument)'
        })
        .option('account', {
          alias: ['customer', 'customer-id', 'customer_id'],
          type: 'string',
          description: 'Google Ads account id (w/o dashes), a.k.a customer id'
        })
        .option('customer-ids-query', {
          alias: ['customer_ids_query'],
          type: 'string',
          description:
              'GAQL query that refines for which accounts to execute scripts'
        })
        .option('customer-ids-query-file', {
          alias: ['customer_ids_query_file'],
          type: 'string',
          description:
              'Same as customer-ids-query but a file path to a query script'
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
          description:
              'Logging level. By default - \'info\', for output=console - \'warn\''
        })
        .option('sync', {
          type: 'boolean',
          description:
              'Queries will be executed for each customer synchronously (otherwise in parallel)'
        })
        .option('csv.destination-folder', {
          type: 'string',
          description: 'output folder for generated CSV files'
        })
        .option('console.transpose', {
          choices: ['auto', 'never', 'always'],
          default: 'auto',
          description:
              'transposing tables: auto - transponse only if table does not fit in terminal window (default), always - transpose all the time, never - never transpose'
        })
        .option('bq', {hidden: true})
        .option('csv', {hidden: true})
        .option('console', {hidden: true})
        .option(
            'bq.project',
            {type: 'string', description: 'GCP project id for BigQuery'})
        .option('bq.dataset', {
          type: 'string',
          description: 'BigQuery dataset id where tables will be created'
        })
        .option(
            'bq.location',
            {type: 'string', description: 'BigQuery dataset location'})
        .option('bq.table-template', {
          type: 'string',
          description:
              'template for tables names, you can use {script} macro inside'
        })
        .option('bq.dump-schema', {
          type: 'boolean',
          description:
              'flag that enables dumping json files with schemas for tables'
        })
        .option('bq.no-union-view', {
          type: 'boolean',
          description:
              'disable creation of union views (combining data from customer\'s table'
        })
        .option('skip-constants', {
          type: 'boolean',
          description: 'do not execute scripts for constant resources'
        })
        .option('dump-query', {type: 'boolean'})
        .group(
            [
              'bq.project', 'bq.dataset', 'bq.dump-schema', 'bq.table-template',
              'bq.location', 'bq.no-union-view'
            ],
            'BigQuery writer options:')
        .group('csv.destination-folder', 'CSV writer options:')
        .group('console.transpose', 'Console writer options:')
        .env('GAARF')
        .config(configObj)
        .config(
            'config', 'Path to JSON or YAML config file',
            function(configPath) {
              let content = fs.readFileSync(configPath, 'utf-8');
              if (configPath.endsWith('.yaml')) {
                return yaml.load(content)
              }
              return JSON.parse(content);
            })
        .help()
        .example(
            '$0 queries/**/*.sql --output=bq --bq.project=myproject --bq.dataset=myds',
            'Execute ads queries and upload results to BigQuery, table per script')
        .example(
            '$0 queries/**/*.sql --output=csv --csv.destination-folder=output',
            'Execute ads queries and output results to csv files, one per script')
        .example(
            '$0 queries/**/*.sql --config=gaarf.json',
            'Execute ads queries with passing arguments via config file (can be json or yaml)')
        .epilog('(c) Google 2022. Not officially supported product.')
        .parseSync();


function getWriter(): IResultWriter {
  let output = (argv.output || '').toString();
  if (output === '') {
    return new NullWriter();
  }
  if (output === 'console') {
    return new ConsoleWriter(<ConsoleWriterOptions>argv.console);
  }
  if (output === 'csv') {
    return new CsvWriter(<CsvWriterOptions>argv.csv);
  }
  if (output === 'bq' || output === 'bigquery') {
    // TODO: move all options to BigQueryWriterOptions
    if (!argv.bq) {
      throw new Error(
          `For BigQuery writer (---output=bq) we should specify at least a project and a dataset id`);
    }
    let projectId = (<any>argv.bq).project
    let dataset = (<any>argv.bq).dataset;
    if (!projectId) {
      console.warn(`bq.project option should be specified (GCP project id)`);
      process.exit(-1);
    }
    if (!dataset) {
      console.warn(
          `bq.dataset option should be specified (BigQuery dataset id)`);
      process.exit(-1);
    }
    let opts: BigQueryWriterOptions = {};
    opts.datasetLocation = (<any>argv.bq).location;
    opts.tableTemplate = (<any>argv.bq)['table-template'];
    opts.dumpSchema = (<any>argv.bq)['dump-schema'];
    opts.noUnionView = ((<any>argv.bq))['no-union-view'];
    return new BigQueryWriter(projectId, dataset, opts);
  }
  throw new Error(`Unknown output format: '${output}'`);
}

async function main() {
  if (argv.account) {
    argv.account = argv.account.toString();
  }
  logger.verbose(JSON.stringify(argv, null, 2));

  let adsConfig: GoogleAdsApiConfig|undefined = undefined;
  let configFilePath = <string>argv.adsConfig;
  if (configFilePath) {
    // try to use ads config from extenral file (ads-config arg)
    adsConfig = loadAdsConfig(configFilePath, argv.account);
  }
  // try to use ads config from explicit cli arguments
  if (argv.ads) {
    let ads_cfg = <any>argv.ads;
    adsConfig = Object.assign(adsConfig || {}, {
      client_id: ads_cfg.client_id || '',
      client_secret: ads_cfg.client_secret || '',
      developer_token: ads_cfg.developer_token || '',
      refresh_token: ads_cfg.refresh_token || '',
      login_customer_id:
          (ads_cfg.login_customer_id || argv.account || '')?.toString(),
      customer_id:
          (argv.account || ads_cfg.login_customer_id || '')?.toString(),
    })
  } else if (fs.existsSync('google-ads.yaml')) {
    adsConfig = loadAdsConfig('google-ads.yaml', argv.account);
  } else {
    // TODO: support searching google-ads.yaml in user home folder (?)
    console.log(chalk.red(
        `Neither Ads API config file was specified ('ads-config' agrument) nor ads.* arguments (either explicitly or config files) nor google-ads.yaml found. Exiting`));
    process.exit(-1);
  }


  logger.verbose('Using ads config:');
  logger.verbose(JSON.stringify(adsConfig, null, 2));

  let client = new GoogleAdsApiClient(adsConfig, argv.account);

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
    console.log(chalk.redBright(
        `Please specify a positional argument with a file path mask for queries (e.g. ./ads-queries/**/*.sql)`));
    process.exit(-1);
  }
  let scriptPaths = argv.files;

  if (argv.output === 'console') {
    // for console writer by default increase default log level to 'warn' (to
    // hide all auxillary info)
    logger.transports.forEach((transport) => {
      if ((<any>transport).name === 'console' && !argv.loglevel) {
        transport.level = 'warn';
      }
    });
  }

  let customer_ids_query = '';
  if (argv.customer_ids_query) {
    customer_ids_query = <string>argv.customer_ids_query;
  } else if (argv.customer_ids_query_file) {
    customer_ids_query =
        await getFileContent(<string>argv.customer_ids_query_file);
  }
  logger.info(`Fetching customer ids ${
      customer_ids_query ? ' (using custom query)' : ''}`);
  let customers = await client.getCustomerIds(customer_ids_query);
  logger.info(`Customers to process (${customers.length}):`);
  logger.info(customers);

  let macros = <Record<string, any>>argv['macro'] || {};
  let writer = getWriter();  // NOTE: create writer from argv
  let executor = new AdsQueryExecutor(client);
  let options: AdsQueryExecutorOptions = {
    skipConstants: argv.skipConstants,
    sync: argv.sync,
    dumpQuery: argv.dumpQuery
  };
  logger.info(`Found ${scriptPaths.length} script to process`);
  logger.debug(JSON.stringify(scriptPaths, null, 2));

  let started = new Date();
  for (let scriptPath of scriptPaths) {
    let queryText = await getFileContent(scriptPath);
    logger.info(`Processing query from ${chalk.gray(scriptPath)}`);

    let scriptName = path.basename(scriptPath).split('.sql')[0];
    await executor.execute(
        scriptName, queryText, customers, macros, writer, options);
  }
  let elapsed = getElapsed(started);
  logger.info(
      chalk.green('All done!') + ' ' + chalk.gray(`Elapsed: ${elapsed}`));
}

function loadAdsConfig(configFilepath: string, customerId?: string|undefined) {
  if (!fs.existsSync(configFilepath)) {
    console.log(chalk.red(`Config file ${configFilepath} does not exist`));
    process.exit(-1);
  }
  try {
    return loadAdsConfigYaml(configFilepath, customerId);
  } catch (e) {
    console.log(chalk.red(
        `Failed to load Ads API configuration from ${configFilepath}: ${e}`));
    process.exit(-1);
  }
}

main().catch(console.error);
