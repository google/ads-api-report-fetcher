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
Object.defineProperty(exports, "__esModule", { value: true });
exports.AdsQueryExecutor = void 0;
const ads_query_editor_1 = require("./ads-query-editor");
const ads_row_parser_1 = require("./ads-row-parser");
const csv_writer_1 = require("./csv-writer");
class AdsQueryExecutor {
    constructor(client) {
        this.client = client;
        this.editor = new ads_query_editor_1.AdsQueryEditor();
        this.parser = new ads_row_parser_1.AdsRowParser();
    }
    parseQuery(queryText, macros) {
        return this.editor.parseQuery(queryText, macros);
    }
    async execute(scriptName, queryText, customers, macros, writer, options) {
        let skipConstants = !!(options === null || options === void 0 ? void 0 : options.skipConstants);
        let sync = !!(options === null || options === void 0 ? void 0 : options.sync);
        if (sync)
            console.log(`Running in synchronous mode`);
        let query = this.parseQuery(queryText, macros);
        let isConstResource = query.resource.isConstant;
        if (skipConstants && isConstResource) {
            console.log(`Skipping constant resource ${query.resource.name}`);
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
                console.log(`An error occured during executing script '${scriptName}' for ${customerId} customer:`);
                console.log(e);
                // we're swallowing the exception
            }
            // if resource has '_constant' in its name, break the loop over customers
            // (it doesn't depend on them)
            if (isConstResource) {
                console.log('Detected constant resource script (breaking loop over customers)');
                break;
            }
        }
        if (!sync) {
            let results = await Promise.allSettled(tasks);
            for (let result of results) {
                if (result.status == 'rejected') {
                    let customerId = result.reason.customerId;
                    console.log(`An error occured during executing script '${scriptName}' for ${customerId} customer:`);
                    console.log(result.reason);
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
            console.log(`Skipping constant resource ${query.resource.name}`);
            return;
        }
        for (let customerId of customers) {
            console.log(`Processing customer ${customerId}`);
            let result = await this.executeOne(query, customerId);
            yield result;
            // if resource has '_constant' in its name, break the loop over customers
            // (it doesn't depend on them)
            if (skipConstants) {
                console.log('Detected constant resource script (breaking loop over customers)');
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
        console.log(`Processing customer ${customerId}`);
        try {
            await writer.beginCustomer(customerId);
            let parsedRows = [];
            let rows = await this.client.executeQuery(query.queryText, customerId);
            for (let row of rows) {
                // TODO: use logging instead
                // console.log('raw row:');
                // console.log(row);
                let parsedRow = this.parser.parseRow(row, query);
                // console.log('parsed row:');
                // console.log(parsedRow);
                if (!empty_result) {
                    parsedRows.push(parsedRow);
                }
                writer.addRow(customerId, parsedRow, row);
            }
            console.log(`\t[${customerId}] got ${rows.length} rows`);
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