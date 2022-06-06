"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.BigQueryExecutor = exports.OAUTH_SCOPES = void 0;
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
const bigquery_1 = require("@google-cloud/bigquery");
exports.OAUTH_SCOPES = [
    'https://www.googleapis.com/auth/cloud-platform',
    'https://www.googleapis.com/auth/cloud-platform.read-only',
    'https://www.googleapis.com/auth/bigquery',
    'https://www.googleapis.com/auth/bigquery.readonly',
];
class BigQueryExecutor {
    //tableId: string|undefined;
    //dataset: Dataset|undefined;
    constructor(projectId, options) {
        this.bigquery = new bigquery_1.BigQuery({
            projectId: projectId,
            scopes: exports.OAUTH_SCOPES,
            // TODO: keyFilename: argv.keyFile
        });
        this.datasetLocation = options === null || options === void 0 ? void 0 : options.datasetLocation;
    }
    substituteMacros(queryText, macros) {
        // replace(/["']/g, "")
        for (let pair of Object.entries(macros)) {
            queryText = queryText.replaceAll(`{${pair[0]}}`, pair[1]);
            //queryText.replace(/{${pair[0]}}/g, pair[1])
        }
        return queryText;
    }
    async execute(scriptName, queryText, params) {
        let dataset;
        if (params === null || params === void 0 ? void 0 : params.target) {
            if (params.target.includes('.')) {
                let idx = params.target.indexOf('.');
                if (idx > 0)
                    throw new Error('Not yet supported');
            }
            dataset = await this.getDataset(params.target);
        }
        let query = {
            query: (params === null || params === void 0 ? void 0 : params.macroParams) ?
                this.substituteMacros(queryText, params === null || params === void 0 ? void 0 : params.macroParams) :
                queryText,
        };
        if (dataset) {
            //query.defaultDataset = dataset;
            query.destination = dataset.table(scriptName);
            query.createDisposition = 'CREATE_IF_NEEDED';
            // TODO: support WRITE_APPEND (if target='dataset.table' or specify
            // disposition explicitly)
            query.writeDisposition = 'WRITE_TRUNCATE';
            //query.location = 'US';
        }
        try {
            let [values] = await this.bigquery.query(query);
            console.log(`Query '${scriptName}' executed successfully (${values.length} rows)`);
            if (dataset && values.length) {
                // write down query's results into a table in BQ
                let table = query.destination;
                const MAX_ROWS = 50000;
                for (let i = 0, j = values.length; i < j; i += MAX_ROWS) {
                    let rowsChunk = values.slice(i, i + MAX_ROWS);
                    await table.insert(rowsChunk, {});
                    console.log(`\tInserted ${rowsChunk.length} rows`);
                }
            }
            return values;
        }
        catch (e) {
            console.log(`Query '${scriptName}' failed to execute: ${e}`);
            throw e;
        }
    }
    async getDataset(datasetId) {
        let dataset;
        const options = {
            location: this.datasetLocation,
        };
        try {
            dataset = this.bigquery.dataset(datasetId, options);
            await dataset.get({ autoCreate: true });
        }
        catch (e) {
            console.log(`Failed to get or create the dataset '${datasetId}'`);
            throw e;
        }
        return dataset;
    }
}
exports.BigQueryExecutor = BigQueryExecutor;
//# sourceMappingURL=bq-executor.js.map