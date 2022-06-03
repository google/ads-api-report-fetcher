"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
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
const chalk_1 = __importDefault(require("chalk"));
const path_1 = __importDefault(require("path"));
const yargs_1 = __importDefault(require("yargs"));
const helpers_1 = require("yargs/helpers");
const bq_executor_1 = require("./lib/bq-executor");
const file_utils_1 = require("./lib/file-utils");
const argv = (0, yargs_1.default)((0, helpers_1.hideBin)(process.argv))
    .scriptName('gaarf')
    .command('$0 <files..>', 'Execute BigQuery queries', {})
    .positional('files', {
    array: true,
    type: 'string',
    description: 'List of files with BigQuery queries (can be gcs:// resources)'
})
    .option('project', { type: 'string', description: 'GCP project id for BigQuery' })
    .option('target', {
    type: 'string',
    description: 'BigQuery dataset or dataset.table to put query result into'
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
    .example('$0 queries/**/*.sql --project=myproject --dataset=myds', 'Execute BigQuery queries and create table for each script\'s result (table per script)')
    .epilog('(c) Google 2022. Not officially supported product.')
    .parseSync();
async function main() {
    console.log(chalk_1.default.gray(JSON.stringify(argv, null, 2)));
    if (!argv.files || !argv.files.length) {
        console.log(chalk_1.default.redBright(`Please specify a positional argument with a file path mask for queries (e.g. ./ads-queries/**/*.sql)`));
        return;
    }
    let scriptPaths = argv.files;
    let projectId = argv.project || '';
    let target = argv.target;
    //let dataset = (<any>argv.bq).dataset;
    let sqlParams = argv['sql'] || {};
    let macroParams = argv['macro'] || {};
    let executor = new bq_executor_1.BigQueryExecutor(projectId);
    for (let scriptPath of scriptPaths) {
        let queryText = await (0, file_utils_1.getFileContent)(scriptPath);
        console.log(`Processing query from ${scriptPath}`);
        let scriptName = path_1.default.basename(scriptPath).split('.sql')[0];
        executor.execute(scriptName, queryText, { sqlParams, macroParams, target });
    }
}
main().catch(console.error);
//# sourceMappingURL=cli-bq.js.map