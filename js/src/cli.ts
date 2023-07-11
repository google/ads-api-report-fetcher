/**
 * Copyright 2023 Google LLC
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

import {GoogleAdsApiClient, GoogleAdsApiConfig, loadAdsConfigFromFile, parseCustomerIds} from './lib/ads-api-client';
import {AdsQueryExecutor, AdsQueryExecutorOptions, AdsApiVersion} from './lib/ads-query-executor';
import {BigQueryInsertMethod, BigQueryWriter, BigQueryWriterOptions} from './lib/bq-writer';
import {ConsoleWriter, ConsoleWriterOptions} from './lib/console-writer';
import {CsvWriter, CsvWriterOptions, NullWriter} from './lib/csv-writer';
import {getFileContent} from './lib/file-utils';
import {getLogger} from './lib/logger';
import {IResultWriter, QueryElements} from './lib/types';
import {getElapsed} from './lib/utils';

const configPath = findUp.sync(['.gaarfrc', '.gaarfrc.json'])
const configObj =
    configPath ? JSON.parse(fs.readFileSync(configPath, 'utf-8')) : {};

const logger = getLogger();

const argv = yargs(hideBin(process.argv))
  .scriptName("gaarf")
  .wrap(yargs.terminalWidth())
  .version()
  .alias("v", "version")
  .command("$0 <files..>", "Execute ads queries (GAQL)", {})
  .positional("files", {
    array: true,
    type: "string",
    description: "List of files with Ads queries (can be gcs:// resources)",
  })
  // .command(
  //     'bigquery <files>', 'Execute BigQuery queries',
  //     {'bq.project': {type: 'string', description: 'GCP project'}})
  // NOTE: when/if we introduce another command, then all options will
  //       move to the defaul command's suboptions
  //       But having them at root level is better for TS typings
  .option("ads-config", {
    type: "string",
    description: "path to yaml config for Google Ads (google-ads.yaml)",
  })
  .option("ads", { hidden: true })
  .option("ads.developer_token", {
    type: "string",
    description: "Ads API developer token",
  })
  .option("ads.client_id", { type: "string", description: "OAuth client_id" })
  .option("ads.client_secret", {
    type: "string",
    description: "OAuth client_secret",
  })
  .option("ads.refresh_token", {
    type: "string",
    description: "OAuth refresh token",
  })
  .option("ads.login_customer_id", {
    type: "string",
    description: "Ads API login account (can be the same as account argument)",
  })
  .option("account", {
    alias: ["customer", "customer-id", "customer_id"],
    type: "string",
    description: "Google Ads account id (w/o dashes), a.k.a customer id or multiple accounts separeted with comma",
  })
  .option("customer-ids-query", {
    alias: ["customer_ids_query"],
    type: "string",
    description:
      "GAQL query that refines for which accounts to execute scripts",
  })
  .option("customer-ids-query-file", {
    alias: ["customer_ids_query_file"],
    type: "string",
    description: "Same as customer-ids-query but a file path to a query script",
  })
  .conflicts("customer-ids-query", "customer-ids-query-file")
  .option("disable-account-expansion", {
    alias: ["disable_account_expansion"],
    type: "boolean",
    descriptions: "Disable MCC account expansion",
  })
  .option("output", {
    choices: ["csv", "bq", "bigquery", "console"],
    alias: "o",
    description: "Output writer to use",
  })
  .option("loglevel", {
    alias: ["log-level", "ll", "log_level"],
    choises: ["debug", "verbose", "info", "warn", "error"],
    description:
      "Logging level. By default - 'info', for output=console - 'warn'",
  })
  // TODO: support parallel query execution (to catch up with Python)
  // .option('parallel-queries', {
  //   type: 'boolean',
  //   description: 'How queries are being processed: in parallel (true) or sequentially (false, default)',
  //   default: false
  // })
  .option("parallel-accounts", {
    type: "boolean",
    description:
      "How one query is being processed for multiple accounts: in parallel (true) or sequentially (false). By default - in parallel",
    default: true,
  })
  .option("parallel-threshold", {
    type: "number",
    description: "The maximum number of parallel queries"
  })
  .option("csv.destination-folder", {
    type: "string",
    alias: "csv.destination",
    description: "Output folder for generated CSV files",
  })
  .option("csv.array-separator", {
    type: "string",
    description: "Arrays separator symbol",
  })
  .option("csv.file-per-customer", {
    type: "boolean"
  })
  .option("console.transpose", {
    choices: ["auto", "never", "always"],
    default: "auto",
    description:
      "Transposing tables: auto - transponse only if table does not fit in terminal window (default), always - transpose all the time, never - never transpose",
  })
  .option("console.page_size", {
    type: "number",
    description: "Maximum row count to output per each script",
  })
  .option("bq", { hidden: true })
  .option("csv", { hidden: true })
  .option("console", { hidden: true })
  .option("bq.project", {
    type: "string",
    description: "GCP project id for BigQuery",
  })
  .option("bq.dataset", {
    type: "string",
    description: "BigQuery dataset id where tables will be created",
  })
  .option("bq.location", {
    type: "string",
    description: "BigQuery dataset location",
  })
  .option("bq.table-template", {
    type: "string",
    description: "Template for tables names, you can use {script} macro inside",
  })
  .option("bq.dump-schema", {
    type: "boolean",
    description: "Flag that enables dumping json files with schemas for tables",
  })
  .option("bq.dump-data", {
    type: "boolean",
    description: "Flag that enables dumping json files with tables data",
  })
  .option("bq.no-union-view", {
    type: "boolean",
    description:
      "Disable creation of union views (combining data from customer's tables)",
  })
  .option("bq.insert-method", {
    type: "string",
    choices: ["insert-all", "load-table"],
    hidden: true,
  })
  .option("bq.array-handling", {
    type: "string",
    choices: ["arrays", "strings"],
    description: "Arrays handling (as arrays or as strings)",
  })
  .option("bq.array-separator", {
    type: "string",
    description: "Arrays separator symbol (for array-handling=strings)",
  })
  .option("skip-constants", {
    type: "boolean",
    description: "Do not execute scripts for constant resources",
  })
  .option("dump-query", {
    type: "boolean",
    description: "Output GAQL quesries to console before execution",
  })
  .group(
    [
      "bq.project",
      "bq.dataset",
      "bq.dump-schema",
      "bq.table-template",
      "bq.location",
      "bq.no-union-view",
      "bq.dump-data",
      "bq.insert-method",
      "bq.array-handling",
      "bq.array-separator",
    ],
    "BigQuery writer options:"
  )
  .group(
    ["csv.destination-folder", "csv.array-separator"],
    "CSV writer options:"
  )
  .group(["console.transpose", "console.page_size"], "Console writer options:")
  .env("GAARF")
  .config(configObj)
  .config(
    "config",
    "Path to JSON or YAML config file",
    async function (configPath) {
      let content = await getFileContent(configPath);
      if (configPath.endsWith(".yaml")) {
        return yaml.load(content);
      }
      return JSON.parse(content);
    }
  )
  .usage(
    `Google Ads API Report Fetcher (gaarf) - a tool for executing Google Ads queries (aka reports, GAQL) with optional exporting to different targets (e.g. BigQuery, CSV) or dumping to the console.\n Built for Ads API ${AdsApiVersion}.`
  )
  .example(
    "$0 queries/**/*.sql --output=bq --bq.project=myproject --bq.dataset=myds",
    "Execute ads queries and upload results to BigQuery, table per script"
  )
  .example(
    "$0 queries/**/*.sql --output=csv --csv.destination-folder=output",
    "Execute ads queries and output results to csv files, one per script"
  )
  .example(
    "$0 queries/**/*.sql --config=gaarf.json",
    "Execute ads queries with passing arguments via config file"
  )
  .epilog(
    `(c) Google 2022-${new Date().getFullYear()}. Not officially supported product.`
  )
  // TODO: .completion()
  .parseSync();


function getWriter(): IResultWriter {
  let output = (argv.output || "").toString();
  if (output === "") {
    return new NullWriter();
  }
  if (output === "console") {
    return new ConsoleWriter(<ConsoleWriterOptions>argv.console);
  }
  if (output === "csv") {
    return new CsvWriter(<CsvWriterOptions>argv.csv);
  }
  if (output === "bq" || output === "bigquery") {
    // TODO: move all options to BigQueryWriterOptions
    if (!argv.bq) {
      throw new Error(
        `For BigQuery writer (---output=bq) we should specify at least a dataset id`
      );
    }
    const dataset = (<any>argv.bq).dataset;
    if (!dataset) {
      console.warn(
        `bq.dataset option should be specified (BigQuery dataset id)`
      );
      process.exit(-1);
    }
    const projectId = (<any>argv.bq).project;
    if (!projectId) {
      console.warn(
        `GCP project id was not specified explicitly (bq.project option), so we're using the current default project`
      );
    }
    let opts: BigQueryWriterOptions = {};
    let bq_opts = <any>argv.bq;
    opts.datasetLocation = bq_opts.location;
    opts.tableTemplate = bq_opts["table-template"];
    opts.dumpSchema = bq_opts["dump-schema"];
    opts.dumpData = bq_opts["dump-data"];
    opts.noUnionView = bq_opts["no-union-view"];
    opts.insertMethod =
      (bq_opts["insert-method"] || "").toLowerCase() === "insert-all"
        ? BigQueryInsertMethod.insertAll
        : BigQueryInsertMethod.loadTable;
    opts.arrayHandling = bq_opts["array-handling"];
    opts.arraySeparator = bq_opts["array-separator"];
    logger.debug("BigQueryWriterOptions:");
    logger.debug(opts);
    return new BigQueryWriter(projectId, dataset, opts);
  }
  // TODO: if (output === 'sqldb')

  throw new Error(`Unknown output format: '${output}'`);
}

async function main() {
  logger.verbose(JSON.stringify(argv, null, 2));

  let adsConfig: GoogleAdsApiConfig|undefined = undefined;
  let adConfigFilePath = <string>argv.adsConfig;
  if (adConfigFilePath) {
    // try to use ads config from extenral file (ads-config arg)
    adsConfig = await loadAdsConfig(adConfigFilePath);
  }
  // try to use ads config from explicit cli arguments
  if (argv.ads) {
    let ads_cfg = <any>argv.ads;
    adsConfig = Object.assign(adsConfig || {}, {
      client_id: ads_cfg.client_id || '',
      client_secret: ads_cfg.client_secret || '',
      developer_token: ads_cfg.developer_token || '',
      refresh_token: ads_cfg.refresh_token || '',
      login_customer_id: ads_cfg.login_customer_id || '',
    });
  } else if (!adConfigFilePath && fs.existsSync('google-ads.yaml')) {
    // load a default google-ads if it wasn't explicitly specified
    // TODO: support searching google-ads.yaml in user home folder (?)
    adsConfig = await loadAdsConfig('google-ads.yaml');
  }
  if (!adsConfig) {
    console.log(chalk.red(
        `Neither Ads API config file was specified ('ads-config' agrument) nor ads.* arguments (either explicitly or via config files) nor google-ads.yaml found. Exiting`));
    process.exit(-1);
  }

  logger.verbose('Using ads config:');
  logger.verbose(
    JSON.stringify(
      Object.assign({}, adsConfig, {
        refresh_token: "<hidden>",
        developer_token: "<hidden>",
      }),
      null,
      2
    )
  );

  let customerIds = parseCustomerIds(argv.account, adsConfig);

  if (!customerIds || customerIds.length === 0) {
    console.log(chalk.red(`No customer id/ids were provided. Exiting`));
    process.exit(-1);
  }
  if (
      !adsConfig.login_customer_id &&
      customerIds &&
      customerIds.length === 1
    ) {
      adsConfig.login_customer_id = customerIds[0];
    }

  let client = new GoogleAdsApiClient(adsConfig);
  let executor = new AdsQueryExecutor(client);

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

  let customers: string[];
  if (argv.disable_account_expansion) {
    logger.info(
      "Skipping account expansion because of disable_account_expansion flag"
     );
    customers = customerIds;
  } else {
    // expand the provided accounts to leaf ones as they could be MMC accounts
    logger.info(`Expanding customer ids ${
      customer_ids_query ? '(using custom query)' : ''}`);
    customers = await client.getCustomerIds(customerIds);
    logger.verbose(
      `Customer ids from the root account(s) ${customerIds.join(',')} (${customers.length}):`
    );
    logger.verbose(customers);
    if (customer_ids_query) {
      logger.verbose(`Filtering customer ids with custom query`);
      logger.debug(customer_ids_query);
      try {
        customers = await executor.getCustomerIds(customers, customer_ids_query);
      } catch (e) {
        logger.error(`Fetching customer ids using customer_ids_query failed: ` + e);
        process.exit(-1);
      }
    }
  }
  if (customers.length === 0) {
    console.log(chalk.redBright(`No customers found for processing`));
    process.exit(-1);
  }
  logger.info(`Customers to process (${customers.length}):`);
  logger.info(customers);

  let macros = <Record<string, any>>argv["macro"] || {};
  let writer = getWriter();  // NOTE: create writer from argv
  let options: AdsQueryExecutorOptions = {
    skipConstants: argv.skipConstants,
    parallelAccounts: argv.parallelAccounts,
    parallelThreshold: argv.parallelThreshold,
    dumpQuery: argv.dumpQuery,
  };
  logger.info(`Found ${scriptPaths.length} script to process`);
  logger.debug(JSON.stringify(scriptPaths, null, 2));

  let started = new Date();
  for (let scriptPath of scriptPaths) {
    let queryText = await getFileContent(scriptPath);
    logger.info(`Processing query from ${chalk.gray(scriptPath)}`);

    let scriptName = path.basename(scriptPath).split('.sql')[0];
    let started_script = new Date();
    await executor.execute(
      scriptName, queryText, customers, macros, writer, options);
    let elapsed_script = getElapsed(started_script);
    logger.info(
      `Query from ${chalk.gray(
        scriptPath
      )} processing for all customers completed. Elapsed: ${elapsed_script}`
    );
  }
  let elapsed = getElapsed(started);
  logger.info(
      chalk.green('All done!') + ' ' + chalk.gray(`Elapsed: ${elapsed}`));
}

async function loadAdsConfig(configFilepath: string) {
  try {
    return loadAdsConfigFromFile(configFilepath);
  } catch (e) {
    console.log(chalk.red(
        `Failed to load Ads API configuration from ${configFilepath}: ${e}`));
    process.exit(-1);
  }
}

main().catch(console.error);
