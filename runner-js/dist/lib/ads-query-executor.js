"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.AdsQueryExecutor = void 0;
const ads_query_editor_1 = require("./ads-query-editor");
const ads_row_parser_1 = require("./ads-row-parser");
class AdsQueryExecutor {
    constructor(client) {
        this.client = client;
        this.editor = new ads_query_editor_1.AdsQueryEditor();
        this.parser = new ads_row_parser_1.AdsRowParser();
    }
    async execute(scriptName, queryText, customers, params, writer) {
        let query = this.editor.parseQuery(queryText, params);
        await writer.beginScript(scriptName, query);
        for (let customerId of customers) {
            console.log(`Processing customer ${customerId}`);
            // TODO: should we parallelirize?
            let result = await this.executeOne(query, customerId, writer);
        }
        await writer.endScript();
    }
    async *executeGen(scriptName, queryText, customers, params, writer) {
        let query = this.editor.parseQuery(queryText, params);
        await writer.beginScript(scriptName, query);
        for (let customerId of customers) {
            console.log(`Processing customer ${customerId}`);
            let result = await this.executeOne(query, customerId, writer);
            yield result;
        }
        await writer.endScript();
    }
    async executeOne(query, customerId, writer) {
        await writer.beginCustomer(customerId);
        let parsedRows = [];
        let rows = await this.client.executeQuery(query.queryText, customerId);
        for (let row of rows) {
            console.log('raw row:');
            console.log(row);
            let parsedRow = this.parser.parseRow(row, query);
            // console.log('parsed row:');
            // console.log(parsedRow);
            parsedRows.push(parsedRow);
            writer.addRow(parsedRow);
        }
        console.log(`\tgot ${rows.length} rows`);
        await writer.endCustomer();
        return { rawRows: rows, rows: parsedRows, query };
    }
}
exports.AdsQueryExecutor = AdsQueryExecutor;
//# sourceMappingURL=ads-query-executor.js.map