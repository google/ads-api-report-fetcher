"use strict";
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
Object.defineProperty(exports, "__esModule", { value: true });
exports.ConsoleWriter = exports.TransposeModes = void 0;
const table_1 = require("table");
var TransposeModes;
(function (TransposeModes) {
    TransposeModes["auto"] = "auto";
    TransposeModes["never"] = "never";
    TransposeModes["always"] = "always";
})(TransposeModes = exports.TransposeModes || (exports.TransposeModes = {}));
class ConsoleWriter {
    constructor(options) {
        this.rowsByCustomer = {};
        options = options || {};
        this.transpose =
            TransposeModes[options.transpose || "auto"];
        this.pageSize = options.page_size || 0;
    }
    beginScript(scriptName, query) {
        this.scriptName = scriptName;
        this.query = query;
    }
    endScript() {
        this.query = undefined;
    }
    beginCustomer(customerId) {
        this.rowsByCustomer[customerId] = [];
    }
    addRow(customerId, parsedRow, rawRow) {
        if (this.pageSize > 0 &&
            this.rowsByCustomer[customerId].length >= this.pageSize)
            return;
        this.rowsByCustomer[customerId].push(parsedRow);
    }
    endCustomer(customerId) {
        let cc = {
            wrapWord: true,
            alignment: "right",
            truncate: 200,
        };
        let rows = this.rowsByCustomer[customerId];
        console.log(`${this.scriptName} (${customerId})`);
        rows = rows.map((row) => {
            return row.map((col) => {
                if (col === undefined)
                    return "";
                return col;
            });
        });
        // original table plus a row (first) with headers (columns names)
        let data = [this.query.columnNames].concat(rows);
        // transpose table (rows become columns)
        let data_trans = data[0].map((_, colIndex) => data.map((row) => row[colIndex]));
        // and a row with indexes
        data_trans.splice(0, 0, [
            "index",
            ...[...Array(rows.length).keys()].map((i) => (++i).toString()),
        ]);
        let tableConfig = {
            border: (0, table_1.getBorderCharacters)("norc"),
            columnDefault: {
                paddingLeft: 0,
                paddingRight: 1,
                truncate: 200,
                wrapWord: true,
            },
            drawVerticalLine: () => true,
            drawHorizontalLine: (lineIndex, rowCount) => {
                return lineIndex === 0 || lineIndex === 1 || lineIndex === rowCount;
            },
            columns: this.query.columnNames.map((c) => cc),
            // singleLine: true
        };
        let data_formatted_orig = (0, table_1.table)(data, tableConfig);
        let data_formatted_trans = (0, table_1.table)(data_trans, tableConfig);
        let use_trans = this.transpose == TransposeModes.always;
        let data_formatted = "";
        if (process.stdout.columns && this.transpose != TransposeModes.never) {
            // we're in Terminal (not streaming to a file)
            if (!use_trans) {
                let first_line = data_formatted_orig.slice(0, data_formatted_orig.indexOf("\n"));
                if (first_line.length > process.stdout.columns) {
                    // table isn't fitting into terminal window, transpose it
                    use_trans = true;
                }
            }
            if (use_trans) {
                let first_line_trans = data_formatted_trans.slice(0, data_formatted_trans.indexOf("\n"));
                if (first_line_trans.length > process.stdout.columns) {
                    // transposed table also isn't fitting, split it onto several tables
                    data_formatted = this.processTransposedTable(data_trans, this.query.columnNames);
                }
            }
        }
        if (!data_formatted) {
            data_formatted = use_trans ? data_formatted_trans : data_formatted_orig;
        }
        console.log(data_formatted);
        this.rowsByCustomer[customerId] = [];
    }
    processTransposedTable(data_trans, headers) {
        let tableConfig = {
            border: (0, table_1.getBorderCharacters)("norc"),
            columnDefault: {
                paddingLeft: 0,
                paddingRight: 1,
                truncate: 200,
                wrapWord: true,
            },
            drawVerticalLine: () => true,
            drawHorizontalLine: () => false,
            columns: this.query.columnNames.map((c) => {
                return {
                    wrapWord: true,
                    alignment: "left",
                    truncate: 200,
                };
            }),
            // singleLine: true
        };
        let output = "";
        let part = 1;
        let done = false;
        while (!done) {
            let first_line = data_trans[0];
            let column_count = first_line.length;
            let row_count = data_trans.length;
            // note: we're starting from 1 because there's always a header columns coming first
            for (let i = 1; i < column_count; i++) {
                // slice matrix up to i-th column
                let submatrix = data_trans
                    .slice(0, row_count + 1)
                    .map((row) => row.slice(0, i + 1));
                let submatrix_formatted = (0, table_1.table)(submatrix, tableConfig);
                let first_line = submatrix_formatted.slice(0, submatrix_formatted.indexOf("\n"));
                if (i === column_count - 1) {
                    // it's the last column, and the matrix being dumped fitted into the window
                    done = true;
                }
                else if (first_line.length > process.stdout.columns) {
                    // we have to break at this column - dump sub-matrix from 0 to (i-1)th column
                    submatrix = data_trans
                        .slice(0, row_count + 1)
                        .map((row) => row.slice(0, i));
                    submatrix_formatted = (0, table_1.table)(submatrix, tableConfig);
                    if (output)
                        output += "\n";
                    output = output + "#" + part + "\n" + submatrix_formatted;
                    part++;
                    // now remove the columns that have been dumped,
                    data_trans = data_trans
                        .slice(0, row_count + 1)
                        .map((row) => row.slice(i, column_count + 1));
                    // append headers at matrix first column (for each row)
                    data_trans[0].splice(0, 0, "index");
                    for (let j = 0; j < headers.length; j++) {
                        data_trans[j + 1].splice(0, 0, headers[j]);
                    }
                    break;
                }
            }
            if (done || column_count <= 2) {
                if (part > 1) {
                    output = output + "\n#" + part;
                }
                output = output + "\n" + (0, table_1.table)(data_trans, tableConfig);
            }
        }
        return output;
    }
}
exports.ConsoleWriter = ConsoleWriter;
//# sourceMappingURL=console-writer.js.map