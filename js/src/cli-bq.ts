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
import yargs from 'yargs'
import {hideBin} from 'yargs/helpers'

import {BigQueryExecutor, BigQueryExecutorOptions} from './lib/bq-executor';
import {getFileContent} from './lib/file-utils';
import {getLogger} from './lib/logger';

const argv =
    yargs(hideBin(process.argv))
        .scriptName('gaarf-bq')
        .command('$0 <files..>', 'Execute BigQuery queries', {})
        .positional('files', {
          array: true,
          type: 'string',
          description:
              'List of files with BigQuery queries (can be gcs:// resources)'
        })
        .option(
            'project',
            {type: 'string', description: 'GCP project id for BigQuery'})
        .option(
            'dataset-location',
            {type: 'string', description: 'BigQuery dataset location'})
        .option('loglevel', {
          alias: ['log-level', 'll', 'log_level'],
          choises: ['debug', 'verbose', 'info', 'warn', 'error'],
          description:
              'Logging level. By default - \'info\', for output=console - \'warn\''
        })
        .env('GAARF_BQ')
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
            '$0 bq-queries/**/*.sql --project=myproject --macro.dataset=mydata',
            'Execute BigQuery queries w/o creating tables (assuming they are DDL queries, e.g. create views)')
        .example(
            '$0 bq-queries/**/*.sql --config=gaarf_bq.json',
            'Execute BigQuery queries with passing arguments via config file (can be json or yaml)')
        .epilog('(c) Google 2022. Not officially supported product.')
        .parseSync();

const logger = getLogger();

async function main() {
  logger.verbose(JSON.stringify(argv, null, 2));
  if (!argv.files || !argv.files.length) {
    console.log(chalk.redBright(
        `Please specify a positional argument with a file path mask for queries (e.g. ./ads-queries/**/*.sql)`));
    process.exit(-1);
  }
  let scriptPaths = argv.files;
  let projectId = argv.project || '';
  let sqlParams = <Record<string, any>>argv['sql'] || {};
  let macroParams = <Record<string, any>>argv['macro'] || {};
  let options: BigQueryExecutorOptions = {
    datasetLocation: argv['dataset-location']
  };
  let executor = new BigQueryExecutor(projectId, options);
  for (let scriptPath of scriptPaths) {
    let queryText = await getFileContent(scriptPath);
    logger.info(`Processing query from ${scriptPath}`);

    let scriptName = path.basename(scriptPath).split('.sql')[0];
    await executor.execute(
        scriptName, queryText, {sqlParams, macroParams});
  }
}

main().catch(console.error);
