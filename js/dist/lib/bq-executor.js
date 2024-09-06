"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.BigQueryExecutor = void 0;
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
const bigquery_1 = require("@google-cloud/bigquery");
const logger_1 = require("./logger");
const utils_1 = require("./utils");
const bq_common_1 = require("./bq-common");
class BigQueryExecutor {
    constructor(projectId, options) {
        const datasetLocation = (options === null || options === void 0 ? void 0 : options.datasetLocation) || 'us';
        this.bigquery =
            (options === null || options === void 0 ? void 0 : options.bigqueryClient) ||
                new bigquery_1.BigQuery({
                    projectId: projectId,
                    scopes: bq_common_1.OAUTH_SCOPES,
                    keyFilename: options === null || options === void 0 ? void 0 : options.keyFilePath,
                    location: datasetLocation,
                });
        this.datasetLocation = datasetLocation;
        this.dumpQuery = options === null || options === void 0 ? void 0 : options.dumpQuery;
        this.logger = (0, logger_1.getLogger)();
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
        if (params === null || params === void 0 ? void 0 : params.templateParams) {
            queryText = (0, utils_1.renderTemplate)(queryText, params.templateParams);
        }
        const res = (0, utils_1.substituteMacros)(queryText, params === null || params === void 0 ? void 0 : params.macroParams);
        if (res.unknown_params.length) {
            throw new Error(`The following parameters used in '${scriptName}' query were not specified: ${res.unknown_params}`);
        }
        const query = {
            query: res.text,
            params: params === null || params === void 0 ? void 0 : params.sqlParams,
        };
        // NOTE: we can support DML scripts as well, but there is no clear reason for this
        // but if we do then it can be like this:
        //if (dataset && !meta.ddl) {
        // query.destination = dataset.table(meta.table || scriptName);
        // query.createDisposition = 'CREATE_IF_NEEDED';
        // query.writeDisposition = params?.writeDisposition || 'WRITE_TRUNCATE';
        //}
        if (this.dumpQuery) {
            this.logger.info('Query text to execute:\n' + query.query);
        }
        try {
            const [values] = await this.bigquery.query(query);
            this.logger.info(`Query '${scriptName}' executed successfully`);
            return values;
        }
        catch (e) {
            this.logger.error(`Query '${scriptName}' failed to execute: ${e}`);
            throw e;
        }
    }
    async createUnifiedView(dataset, tableId, customers) {
        if (typeof dataset === 'string') {
            dataset = await (0, bq_common_1.getDataset)(this.bigquery, dataset, this.datasetLocation);
        }
        const datasetId = dataset.id;
        // Unfortunately BQ always creates a based empty table for templated
        // (customer) table, so we have to drop it first.
        await dataset.table(tableId).delete({ ignoreNotFound: true });
        const table_fq = `${datasetId}.${tableId}`;
        try {
            // here there's a potential problem. If wildcard expression (resource_*)
            // catches another view the DML-query will fail with error:
            // 'Views cannot be queried through prefix. First view projectid:datasetid.viewname.'
            let query = `CREATE OR REPLACE VIEW \`${table_fq}\` AS SELECT * FROM \`${table_fq}_*\``;
            if (customers && customers.length) {
                query += ` WHERE _TABLE_SUFFIX in (${customers
                    .map(s => "'" + s + "'")
                    .join(',')})`;
            }
            this.logger.debug(query);
            await dataset.query({
                query: query,
            });
            return table_fq;
        }
        catch (e) {
            this.logger.error(`An error occured during creating the unified view (${table_fq}): ${e.message}`);
            if (e.message.includes('Views cannot be queried through prefix')) {
                this.logger.warn(`You have to rename the script ${tableId} to a name so the wildcard expression ${tableId}_* would not catch other views`);
            }
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
            this.logger.error(`Failed to get or create the dataset '${datasetId}'`);
            throw e;
        }
        return dataset;
    }
}
exports.BigQueryExecutor = BigQueryExecutor;
//# sourceMappingURL=bq-executor.js.map