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
exports.ConsoleWriter = void 0;
const table_1 = require("table");
// TODO:
class ConsoleWriter {
    constructor(options) {
        this.rowsByCustomer = {};
    }
    beginScript(scriptName, query) {
        this.query = query;
    }
    endScript() {
        this.query = undefined;
    }
    beginCustomer(customerId) {
        this.rowsByCustomer[customerId] = [];
    }
    addRow(customerId, parsedRow, rawRow) {
        this.rowsByCustomer[customerId].push(parsedRow);
    }
    endCustomer(customerId) {
        // TODO:
        let cc = { wrapWord: true, alignment: 'center' };
        let rows = this.rowsByCustomer[customerId];
        let text = (0, table_1.table)(rows, {
            border: (0, table_1.getBorderCharacters)('void'),
            columnDefault: { paddingLeft: 0, paddingRight: 1 },
            drawHorizontalLine: () => false
            // border: getBorderCharacters('ramac'),
            // columns: this.query!.columnNames.map(c => cc),
            // singleLine: true
        });
        console.log(text);
        this.rowsByCustomer[customerId] = [];
    }
}
exports.ConsoleWriter = ConsoleWriter;
//# sourceMappingURL=console-writer.js.map