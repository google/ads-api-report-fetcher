"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.BigQueryExecutor = void 0;
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
const logger_1 = __importDefault(require("./logger"));
const utils_1 = require("./utils");
const bq_common_1 = require("./bq-common");
class BigQueryExecutor {
    constructor(projectId, options) {
        this.bigquery = new bigquery_1.BigQuery({
            projectId: projectId,
            scopes: bq_common_1.OAUTH_SCOPES,
            keyFilename: options === null || options === void 0 ? void 0 : options.keyFilePath
        });
        this.datasetLocation = options === null || options === void 0 ? void 0 : options.datasetLocation;
    }
    async execute(scriptName, queryText, params) {
        if (params === null || params === void 0 ? void 0 : params.macroParams) {
            for (const macro of Object.keys(params.macroParams)) {
                if (macro.includes('dataset')) {
                    // all macros containing the word 'dataset' we treat as a dataset's name
                    const value = params.macroParams[macro];
                    if (value) {
                        await this.getDataset(value);
                    }
                }
            }
        }
        let res = (0, utils_1.substituteMacros)(queryText, params === null || params === void 0 ? void 0 : params.macroParams);
        if (res.unknown_params.length) {
            throw new Error(`The following parameters used in '${scriptName}' query were not specified: ${res.unknown_params}`);
        }
        let query = {
            query: res.text
        };
        // NOTE: we can support DML scripts as well, but there is no clear reason for this
        // but if we do then it can be like this:
        //if (dataset && !meta.ddl) {
        // query.destination = dataset.table(meta.table || scriptName);
        // query.createDisposition = 'CREATE_IF_NEEDED';
        // query.writeDisposition = params?.writeDisposition || 'WRITE_TRUNCATE';
        //}
        try {
            let [values] = await this.bigquery.query(query);
            logger_1.default.info(`Query '${scriptName}' executed successfully`);
            return values;
        }
        catch (e) {
            logger_1.default.error(`Query '${scriptName}' failed to execute: ${e}`);
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
            logger_1.default.error(`Failed to get or create the dataset '${datasetId}'`);
            throw e;
        }
        return dataset;
    }
}
exports.BigQueryExecutor = BigQueryExecutor;
//# sourceMappingURL=bq-executor.js.map