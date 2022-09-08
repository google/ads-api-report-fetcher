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
const fs_1 = __importDefault(require("fs"));
const js_yaml_1 = __importDefault(require("js-yaml"));
const path_1 = __importDefault(require("path"));
const yargs_1 = __importDefault(require("yargs"));
const helpers_1 = require("yargs/helpers");
const bq_executor_1 = require("./lib/bq-executor");
const file_utils_1 = require("./lib/file-utils");
const logger_1 = __importDefault(require("./lib/logger"));
const argv = (0, yargs_1.default)((0, helpers_1.hideBin)(process.argv))
    .scriptName('gaarf-bq')
    .command('$0 <files..>', 'Execute BigQuery queries', {})
    .positional('files', {
    array: true,
    type: 'string',
    description: 'List of files with BigQuery queries (can be gcs:// resources)'
})
    .option('project', { type: 'string', description: 'GCP project id for BigQuery' })
    .option('dataset-location', { type: 'string', description: 'BigQuery dataset location' })
    .option('loglevel', {
    alias: ['log-level', 'll', 'log_level'],
    choises: ['debug', 'verbose', 'info', 'warn', 'error'],
    description: 'Logging level. By default - \'info\', for output=console - \'warn\''
})
    .env('GAARF_BQ')
    .config('config', 'Path to JSON or YAML config file', function (configPath) {
    let content = fs_1.default.readFileSync(configPath, 'utf-8');
    if (configPath.endsWith('.yaml')) {
        return js_yaml_1.default.load(content);
    }
    return JSON.parse(content);
})
    .help()
    .example('$0 bq-queries/**/*.sql --project=myproject --macro.dataset=mydata', 'Execute BigQuery queries w/o creating tables (assuming they are DDL queries, e.g. create views)')
    .example('$0 bq-queries/**/*.sql --config=gaarf_bq.json', 'Execute BigQuery queries with passing arguments via config file (can be json or yaml)')
    .epilog('(c) Google 2022. Not officially supported product.')
    .parseSync();
async function main() {
    logger_1.default.verbose(JSON.stringify(argv, null, 2));
    if (!argv.files || !argv.files.length) {
        console.log(chalk_1.default.redBright(`Please specify a positional argument with a file path mask for queries (e.g. ./ads-queries/**/*.sql)`));
        process.exit(-1);
    }
    let scriptPaths = argv.files;
    let projectId = argv.project || '';
    let sqlParams = argv['sql'] || {};
    let macroParams = argv['macro'] || {};
    let options = {
        datasetLocation: argv['dataset-location']
    };
    let executor = new bq_executor_1.BigQueryExecutor(projectId, options);
    for (let scriptPath of scriptPaths) {
        let queryText = await (0, file_utils_1.getFileContent)(scriptPath);
        logger_1.default.info(`Processing query from ${scriptPath}`);
        let scriptName = path_1.default.basename(scriptPath).split('.sql')[0];
        await executor.execute(scriptName, queryText, { sqlParams, macroParams });
    }
}
main().catch(console.error);
//# sourceMappingURL=cli-bq.js.map