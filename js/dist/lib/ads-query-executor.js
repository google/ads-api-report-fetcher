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
const logger_1 = __importDefault(require("./logger"));
const utils_1 = require("./utils");
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
     * @returns a map from customer-id to row counts
     */
    async execute(scriptName, queryText, customers, macros, writer, options) {
        let skipConstants = !!(options === null || options === void 0 ? void 0 : options.skipConstants);
        let sync = !!(options === null || options === void 0 ? void 0 : options.sync) || customers.length === 1;
        if (sync)
            logger_1.default.verbose(`Running in synchronous mode`, { scriptName: scriptName });
        let query = this.parseQuery(queryText, macros);
        let isConstResource = query.resource.isConstant;
        if (skipConstants && isConstResource) {
            logger_1.default.verbose(`Skipping constant resource '${query.resource.name}'`, {
                scriptName: scriptName,
            });
            return {};
        }
        if (options === null || options === void 0 ? void 0 : options.dumpQuery) {
            logger_1.default.verbose(`Script text to execute:\n` + query.queryText);
        }
        if (writer)
            await writer.beginScript(scriptName, query);
        let tasks = [];
        let result_map = {}; // customer-id to row count mapping for return
        for (let customerId of customers) {
            try {
                if (sync) {
                    let res = await this.executeOne(query, customerId, writer);
                    result_map[customerId] = res.rowCount;
                }
                else {
                    let task = this.executeOne(query, customerId, writer);
                    tasks.push(task);
                }
            }
            catch (e) {
                logger_1.default.error(`An error occured during executing script '${scriptName}' for ${customerId} customer:`);
                logger_1.default.error(e);
                // there could be legit reasons for the query to fail (e.g. customer is disabled),
                // but swalling the exception here will possible cause other issue in writer,
                // particularly in BigQueryWriter.endScript we'll trying to create a view for customer-based tables,
                // and if query failed for all customers the view creation will also fail.
                throw e;
            }
            // if resource has '_constant' in its name, break the loop over customers
            // (it doesn't depend on them)
            if (isConstResource) {
                logger_1.default.debug("Detected constant resource script (breaking loop over customers)", { scriptName: scriptName, customerId: customerId });
                break;
            }
        }
        if (!sync) {
            let results = await Promise.allSettled(tasks);
            for (let result of results) {
                if (result.status == "rejected") {
                    let customerId = result.reason.customerId;
                    logger_1.default.error(`An error occured during executing script '${scriptName}' for ${customerId} customer:`);
                    logger_1.default.error(result.reason);
                    throw result.reason;
                }
                else {
                    let customerId = result.value.customerId;
                    result_map[customerId] = result.value.rowCount;
                }
            }
        }
        if (writer)
            await writer.endScript();
        logger_1.default.debug(`[${scriptName}] Memory (script completed):\n` + (0, utils_1.dumpMemory)());
        return result_map;
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
            logger_1.default.verbose(`Skipping constant resource '${query.resource.name}'`, {
                scriptName: scriptName,
            });
            return;
        }
        for (let customerId of customers) {
            logger_1.default.info(`Processing customer ${customerId}`, {
                scriptName: scriptName,
            });
            let result = await this.executeOne(query, customerId);
            yield result;
            // if resource has '_constant' in its name, break the loop over customers
            // (it doesn't depend on them)
            if (skipConstants) {
                logger_1.default.debug("Detected constant resource script (breaking loop over customers)", { scriptName: scriptName, customerId: customerId });
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
     * @param writer output writer, can be ommited (if you need QueryResult with data)
     * @returns QueryResult, but `rows` and `rawRows` fields will be empty if you supplied a writer
     */
    async executeOne(query, customerId, writer) {
        if (!customerId)
            throw new Error(`customerId should be specified`);
        let accumulate_data = !writer;
        logger_1.default.verbose(`Starting processing customer ${customerId}`, {
            customerId: customerId,
        });
        if (logger_1.default.isLevelEnabled("debug")) {
            logger_1.default.debug(`[${customerId}] Memory (before customer):\n` + (0, utils_1.dumpMemory)());
        }
        let started = new Date();
        try {
            if (writer)
                await writer.beginCustomer(customerId);
            let rawRows = [];
            let parsedRows = [];
            logger_1.default.debug(`Executing query: ${query.queryText}`, {
                customerId: customerId,
            });
            let stream = this.client.executeQueryStream(query.queryText, customerId);
            let rowCount = 0;
            for await (const row of stream) {
                let parsedRow = this.parser.parseRow(row, query);
                rowCount++;
                if (accumulate_data) {
                    // NOTE: to descrease memory consumption we won't accumulate data if a writer was supplied
                    rawRows.push(row);
                    parsedRows.push(parsedRow);
                }
                if (writer)
                    await writer.addRow(customerId, parsedRow, row);
            }
            logger_1.default.info(`Query executed and parsed. ${rowCount} rows. Elapsed: ${(0, utils_1.getElapsed)(started)}`, {
                customerId: customerId,
            });
            if (writer)
                await writer.endCustomer(customerId);
            if (logger_1.default.isDebugEnabled()) {
                logger_1.default.debug(`[${customerId}] Memory (customer completed):\n` + (0, utils_1.dumpMemory)());
            }
            logger_1.default.info(`Customer processing completed. Elapsed: ${(0, utils_1.getElapsed)(started)}`, {
                customerId: customerId,
            });
            return { rawRows, rows: parsedRows, query, customerId, rowCount };
        }
        catch (e) {
            e.customerId = customerId;
            throw e;
        }
    }
}
exports.AdsQueryExecutor = AdsQueryExecutor;
//# sourceMappingURL=ads-query-executor.js.map