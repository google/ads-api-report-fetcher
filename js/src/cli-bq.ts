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
import path from 'path';
import yargs from 'yargs'
import {hideBin} from 'yargs/helpers'

import {BigQueryExecutor} from './lib/bq-executor';
import {getFileContent} from './lib/file-utils';

const argv =
    yargs(hideBin(process.argv))
        .scriptName('gaarf')
        .command('$0 <files..>', 'Execute BigQuery queries', {})
        .positional('files', {
          array: true,
          type: 'string',
          description: 'List of files with BigQuery queries (can be gcs:// resources)'
        })
        .option(
            'project',
            {type: 'string', description: 'GCP project id for BigQuery'})
        .option('target', {
          type: 'string',
          description:
              'BigQuery dataset or dataset.table to put query result into'
        })
        // .option('dataset-dst', {
        //   type: 'string',
        //   description:
        //       'Destination BigQuery dataset id where output tables will be created'
        // })
        // .option(
        //     'location',
        //     {type: 'string', description: 'BigQuery dataset location'})
        // .option('table-template', {
        //   type: 'string',
        //   description:
        //       'Template for tables names, you can use {script} macro inside'
        // })
        .group(['project', 'dataset', 'dataset-dst', 'location'], 'BigQuery options:')
        .help()
        .example(
            '$0 queries/**/*.sql --project=myproject --dataset=myds',
            'Execute BigQuery queries and create table for each script\'s result (table per script)')
        .epilog('(c) Google 2022. Not officially supported product.')
        .parseSync()

async function main() {
  console.log(chalk.gray(JSON.stringify(argv, null, 2)));
  if (!argv.files || !argv.files.length) {
    console.log(chalk.redBright(
        `Please specify a positional argument with a file path mask for queries (e.g. ./ads-queries/**/*.sql)`));
    return;
  }
  let scriptPaths = argv.files;
  let projectId = argv.project || '';
  let target = argv.target;
  //let dataset = (<any>argv.bq).dataset;
  let sqlParams = <Record<string, any>>argv['sql'] || {};
  let macroParams = <Record<string, any>>argv['macro'] || {};
  let executor = new BigQueryExecutor(projectId);
  for (let scriptPath of scriptPaths) {
    let queryText = await getFileContent(scriptPath);
    console.log(`Processing query from ${scriptPath}`);

    let scriptName = path.basename(scriptPath).split('.sql')[0];
    executor.execute(scriptName, queryText, {sqlParams, macroParams, target})
  }
}

main().catch(console.error);
