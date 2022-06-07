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

import {AdsQueryExecutor} from './lib/ads-query-executor';
import {GoogleAdsApiClient, GoogleAdsApiConfig} from './lib/api-client';
import {BigQueryWriter, BigQueryWriterOptions} from './lib/bq-writer';
import {ConsoleWriter, ConsoleWriterOptions} from './lib/console-writer';
import {CsvWriter, CsvWriterOptions, NullWriter} from './lib/csv-writer';
import {getFileContent} from './lib/file-utils';
import logger from './lib/logger';
import {IResultWriter, QueryElements} from './lib/types';

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
        .option(
            'ads-config',
            {type: 'string', description: 'path to yaml config for Google Ads'})
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
        .option('output', {
          choices: ['csv', 'bq', 'bigquery', 'console'],
          alias: 'o',
          description: 'output writer to use'
        })
        .option('csv.destination-folder', {
          type: 'string',
          description: 'output folder for generated CSV files'
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
        .group(
            [
              'bq.project', 'bq.dataset', 'bq.dump-schema', 'bq.table-template',
              'bq.location'
            ],
            'BigQuery writer options:')
        .group('csv.destination-folder', 'CSV writer options:')
        .option('skip-constants', {
          type: 'boolean',
          description: 'do not execute scripts for constant resources'
        })
        .config(configObj)
        .config()
        .help()
        .example(
            '$0 queries/**/*.sql --output=bq --bq.project=myproject --bq.dataset=myds',
            'Execute ads queries and upload results to BigQuery, table per script')
        .example(
            '$0 queries/**/*.sql --output=csv --csv.destination-folder=output',
            'Execute ads queries and output results to csv files, one per script')
        .epilog('(c) Google 2022. Not officially supported product.')
        .parseSync()


function getWriter(): IResultWriter {
  let output = (argv.output || '').toString();
  if (output === '') {
    return new NullWriter();
  }
  if (output === 'console') {
    // TODO:
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
    return new BigQueryWriter(projectId, dataset, opts);
  }
  throw new Error(`Unknown output format: '${output}'`);
}

async function main() {
  if (argv.account) {
    argv.account = argv.account.toString();
  }
  console.log(chalk.gray(JSON.stringify(argv, null, 2)));

  let adsConfig: GoogleAdsApiConfig;
  let configFilePath = <string>argv.adsConfig;
  if (configFilePath) {
    // try to use ads config from extenral file (ads-config arg)
    adsConfig = loadAdsConfig(configFilePath, argv.account);
  } else {
    // try to use ads config from explicit cli arguments
    if (argv.ads) {
      let ads_cfg = <any>argv.ads;
      adsConfig = {
        client_id: ads_cfg.client_id || '',
        client_secret: ads_cfg.client_secret || '',
        developer_token: ads_cfg.developer_token || '',
        refresh_token: ads_cfg.refresh_token || '',
        login_customer_id:
            (ads_cfg.login_customer_id || argv.account || '')?.toString(),
        customer_id:
            (argv.account || ads_cfg.login_customer_id || '')?.toString(),
      }
    } else if (fs.existsSync('google-ads.yaml')) {
      adsConfig = loadAdsConfig('google-ads.yaml', argv.account);
    } else {
      // TODO: support searching google-ads.yaml in user home folder (?)
      console.log(chalk.red(
          `Neither Ads API config file was not specified ('ads-config' agrument) nor ads.* arguments (either explicitly or via .gaarfrc config)`));
      process.exit(-1);
    }
  }
  console.log(chalk.gray('Using ads config:'));
  console.log(adsConfig);

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
    return;
  }
  let scriptPaths = argv.files;

  console.log('Fetching customer ids');
  let customers = await client.getCustomerIds();
  console.log(`Customers to process:`);
  console.log(customers);

  let params = <Record<string, any>>argv['sql'] || {};
  let writer = getWriter();  // NOTE: create writer from argv
  let executor = new AdsQueryExecutor(client);
  let options = {skipConstants: argv.skipConstants};
  console.log(`Found ${scriptPaths.length} script to process`);
  for (let scriptPath of scriptPaths) {
    let queryText = await getFileContent(scriptPath);
    console.log(`Processing query from ${scriptPath}`);

    let scriptName = path.basename(scriptPath).split('.sql')[0];
    await executor.execute(
        scriptName, queryText, customers, params, writer, options);
    console.log();
  }

  console.log(chalk.green('All done!'));
}


function loadAdsConfig(
    configFilepath: string, customerId?: string|undefined): GoogleAdsApiConfig {
  if (!fs.existsSync(configFilepath)) {
    console.log(chalk.red(`Config file ${configFilepath} does not exist`));
    process.exit(-1);
  }
  try {
    const doc = <any>yaml.load(fs.readFileSync(configFilepath, 'utf8'));
    return {
      developer_token: doc['developer_token'],
      client_id: doc['client_id'],
      client_secret: doc['client_secret'],
      refresh_token: doc['refresh_token'],
      login_customer_id: doc['login_customer_id']?.toString(),
      customer_id:
          (customerId || doc['customer_id'] || doc['login_customer_id'])
              ?.toString()
    };
  } catch (e) {
    console.log(chalk.red(
        `Failed to load Ads API configuration from ${configFilepath}: ${e}`));
    process.exit(-1);
  }
}

main().catch(console.error);
