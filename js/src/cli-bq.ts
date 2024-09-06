/* eslint-disable n/no-process-exit */
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
import fs from 'fs';
import yaml from 'js-yaml';
import path from 'path';
import yargs from 'yargs';
import {hideBin} from 'yargs/helpers';

import {BigQueryExecutor, BigQueryExecutorOptions} from './lib/bq-executor';
import {getFileContent} from './lib/file-utils';
import {getLogger} from './lib/logger';

const argv = yargs(hideBin(process.argv))
  .scriptName('gaarf-bq')
  .wrap(yargs.terminalWidth())
  .version()
  .alias('v', 'version')
  .command('$0 <files..>', 'Execute BigQuery queries', {})
  .positional('files', {
    array: true,
    type: 'string',
    description: 'List of files with BigQuery queries (can be gs:// resources)',
  })
  .option('project', {
    type: 'string',
    description: 'GCP project id for BigQuery',
  })
  .option('dataset-location', {
    type: 'string',
    description: 'BigQuery dataset location',
  })
  .option('dump-query', {
    type: 'boolean',
    description: 'Output quesries to console before execution',
  })
  .option('loglevel', {
    alias: ['log-level', 'll', 'log_level'],
    choises: ['debug', 'verbose', 'info', 'warn', 'error'],
    description:
      "Logging level. By default - 'info', for output=console - 'warn'",
  })
  .env('GAARF_BQ')
  .config('config', 'Path to JSON or YAML config file', configPath => {
    const content = fs.readFileSync(configPath, 'utf-8');
    if (configPath.endsWith('.yaml')) {
      return yaml.load(content);
    }
    return JSON.parse(content);
  })
  .help()
  .usage(
    'gaarf-bq - a tool for executing BigQuery queries, a companion tool for Google Ads API Report Fetcher (gaarf).'
  )
  .example(
    '$0 bq-queries/**/*.sql --project=myproject --macro.dataset=mydata',
    'Execute BigQuery queries w/o creating tables (assuming they are DDL queries, e.g. create views)'
  )
  .example(
    '$0 bq-queries/**/*.sql --config=gaarf_bq.json',
    'Execute BigQuery queries with passing arguments via config file'
  )
  .epilog(
    `(c) Google 2022-${new Date().getFullYear()}. Not officially supported product.`
  )
  .parseSync();

const logger = getLogger();

async function main() {
  logger.verbose(JSON.stringify(argv, null, 2));
  if (!argv.files || !argv.files.length) {
    console.log(
      chalk.redBright(
        'Please specify a positional argument with a file path mask for queries (e.g. ./ads-queries/**/*.sql)'
      )
    );
    process.exit(-1);
  }
  const scriptPaths = argv.files;
  const projectId = argv.project || '';
  const sqlParams = <Record<string, unknown>>argv['sql'];
  const macroParams = <Record<string, string>>argv['macro'];
  const templateParams = <Record<string, string>>argv['template'];
  const options: BigQueryExecutorOptions = {
    datasetLocation: argv['dataset-location'],
    dumpQuery: argv['dump-query'],
  };
  const executor = new BigQueryExecutor(projectId, options);
  for (const scriptPath of scriptPaths) {
    const queryText = await getFileContent(scriptPath);
    logger.info(`Processing query from ${scriptPath}`);

    const scriptName = path.basename(scriptPath).split('.sql')[0];
    await executor.execute(scriptName, queryText, {
      sqlParams,
      macroParams,
      templateParams,
    });
  }
}

main().catch(console.error);
