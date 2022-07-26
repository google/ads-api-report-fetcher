"use strict";
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
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.AdsQueryExecutor = void 0;
const ads_query_editor_1 = require("./ads-query-editor");
const ads_row_parser_1 = require("./ads-row-parser");
const csv_writer_1 = require("./csv-writer");
const logger_1 = __importDefault(require("./logger"));
class AdsQueryExecutor {
    constructor(client) {
        this.client = client;
        this.editor = new ads_query_editor_1.AdsQueryEditor();
        this.parser = new ads_row_parser_1.AdsRowParser();
    }
    parseQuery(queryText, macros) {
        return this.editor.parseQuery(queryText, macros);
    }
    /**
     * Executes a query for a list of customers.
     * Please note that if you use the method directly you should call methods
     * `beginScript` and `endScript` on your writer instance.
     * @param scriptName name of a script (can be use as target table name)
     * @param queryText Ads query text (GAQL)
     * @param customers customer ids
     * @param macros macro values to substritute into the query
     * @param writer output writer, can be ommited
     * @param options additional execution options
     */
    async execute(scriptName, queryText, customers, macros, writer, options) {
        let skipConstants = !!(options === null || options === void 0 ? void 0 : options.skipConstants);
        let sync = !!(options === null || options === void 0 ? void 0 : options.sync) || customers.length === 1;
        if (sync)
            logger_1.default.verbose(`Running in synchronous mode`, { scriptName: scriptName });
        let query = this.parseQuery(queryText, macros);
        let isConstResource = query.resource.isConstant;
        if (skipConstants && isConstResource) {
            logger_1.default.verbose(`Skipping constant resource '${query.resource.name}'`, { scriptName: scriptName });
            return;
        }
        if (writer)
            await writer.beginScript(scriptName, query);
        let tasks = [];
        for (let customerId of customers) {
            try {
                if (sync) {
                    await this.executeOne(query, customerId, writer);
                }
                else {
                    let task = this.executeOne(query, customerId, writer);
                    tasks.push(task);
                }
            }
            catch (e) {
                logger_1.default.error(`An error occured during executing script '${scriptName}' for ${customerId} customer:`);
                logger_1.default.error(e);
                // we're swallowing the exception
            }
            // if resource has '_constant' in its name, break the loop over customers
            // (it doesn't depend on them)
            if (isConstResource) {
                logger_1.default.debug('Detected constant resource script (breaking loop over customers)', { scriptName: scriptName, customerId: customerId });
                break;
            }
        }
        if (!sync) {
            let results = await Promise.allSettled(tasks);
            for (let result of results) {
                if (result.status == 'rejected') {
                    let customerId = result.reason.customerId;
                    logger_1.default.error(`An error occured during executing script '${scriptName}' for ${customerId} customer:`);
                    logger_1.default.error(result.reason);
                }
            }
        }
        if (writer)
            await writer.endScript();
    }
    /**
     * Analogue to `execute` method but with an ability to get result for each
     * customer
     * (`execute` can only be used with a writer)
     * @example
     *
     * @param scriptName name of the script
     * @param queryText parsed Ads query
     * @param customers a list of customers to process
     * @param macros macros (arbitrary key-value pairs to substitute into query)
     * @param options execution options
     * @returns an async generator to iterate through to get results for each
     *     customer
     */
    async *executeGen(scriptName, queryText, customers, macros, options) {
        let skipConstants = !!(options === null || options === void 0 ? void 0 : options.skipConstants);
        let query = this.parseQuery(queryText, macros);
        let isConstResource = query.resource.isConstant;
        if (skipConstants && isConstResource) {
            logger_1.default.verbose(`Skipping constant resource '${query.resource.name}'`, { scriptName: scriptName });
            return;
        }
        for (let customerId of customers) {
            logger_1.default.info(`Processing customer ${customerId}`, { scriptName: scriptName });
            let result = await this.executeOne(query, customerId);
            yield result;
            // if resource has '_constant' in its name, break the loop over customers
            // (it doesn't depend on them)
            if (skipConstants) {
                logger_1.default.debug('Detected constant resource script (breaking loop over customers)', { scriptName: scriptName, customerId: customerId });
                break;
            }
        }
    }
    /**
     * Executes a query for a customer.
     * Please note that if you use the method directly you should call methods
     * `beginScript` and `endScript` on your writer instance.
     * @param query parsed Ads query (GAQL)
     * @param customerId customer id
     * @param writer output writer, can be ommited (if you need QueryResult)
     * @returns void if you supplied a writer, otherwise (no writer) a QueryResult
     */
    async executeOne(query, customerId, writer) {
        if (!customerId)
            throw new Error(`customerId should be specified`);
        let empty_result = !!writer;
        if (!writer) {
            writer = new csv_writer_1.NullWriter();
        }
        logger_1.default.verbose(`Starting processing customer ${customerId}`, { customerId: customerId });
        try {
            await writer.beginCustomer(customerId);
            let parsedRows = [];
            logger_1.default.debug(`Executing query: ${query.queryText}`, { customerId: customerId });
            let rows = await this.client.executeQuery(query.queryText, customerId);
            for (let row of rows) {
                if (logger_1.default.isLevelEnabled('debug')) {
                    logger_1.default.debug('row row:', { customerId: customerId });
                    logger_1.default.debug(JSON.stringify(row, null, 2));
                }
                let parsedRow = this.parser.parseRow(row, query);
                if (logger_1.default.isLevelEnabled('debug')) {
                    logger_1.default.debug('parsed row:', { customerId: customerId });
                    logger_1.default.debug(JSON.stringify(parsedRow, null, 2));
                }
                if (!empty_result) {
                    parsedRows.push(parsedRow);
                }
                writer.addRow(customerId, parsedRow, row);
            }
            logger_1.default.info(`Query executed and resulted in ${rows.length} rows`, { customerId: customerId });
            await writer.endCustomer(customerId);
            if (empty_result)
                return;
            return { rawRows: rows, rows: parsedRows, query };
        }
        catch (e) {
            e.customerId = customerId;
            throw e;
        }
    }
}
exports.AdsQueryExecutor = AdsQueryExecutor;
//# sourceMappingURL=ads-query-executor.js.map