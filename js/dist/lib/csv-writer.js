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
exports.NullWriter = exports.CsvWriter = void 0;
const sync_1 = require("csv-stringify/sync");
const fs_1 = __importDefault(require("fs"));
const path_1 = __importDefault(require("path"));
const logger_1 = __importDefault(require("./logger"));
class CsvWriter {
    constructor(options) {
        this.appending = false;
        this.customerRows = 0;
        // rows: any[][] = [];
        this.rowsByCustomer = {};
        this.destination = options === null || options === void 0 ? void 0 : options.destinationFolder;
    }
    beginScript(scriptName, query) {
        this.appending = false;
        this.query = query;
        let filename = scriptName + '.csv';
        if (this.destination) {
            if (!fs_1.default.existsSync(this.destination)) {
                fs_1.default.mkdirSync(this.destination, { recursive: true });
            }
            filename = path_1.default.join(this.destination, filename);
        }
        this.filename = filename;
        if (fs_1.default.existsSync(this.filename)) {
            fs_1.default.rmSync(this.filename);
        }
    }
    endScript() {
        this.filename = undefined;
    }
    beginCustomer(customerId) {
        this.rowsByCustomer[customerId] = [];
    }
    addRow(customerId, parsedRow, rawRow) {
        if (!parsedRow || parsedRow.length == 0)
            return;
        this.rowsByCustomer[customerId].push(parsedRow);
    }
    endCustomer(customerId) {
        let rows = this.rowsByCustomer[customerId];
        if (!rows.length) {
            return;
        }
        let csvOptions = {
            header: !this.appending,
            quoted: false,
            columns: this.query.columnNames,
            cast: {
                boolean: (value, context) => value ? 'true' : 'false'
            }
        };
        let csv = (0, sync_1.stringify)(rows, csvOptions);
        fs_1.default.writeFileSync(this.filename, csv, { encoding: 'utf-8', flag: this.appending ? 'a' : 'w' });
        if (rows.length > 0) {
            logger_1.default.info((this.appending ? 'Updated ' : 'Created ') + this.filename +
                ` with ${rows.length} rows`, { customerId: customerId, scriptName: this.filename });
        }
        this.appending = true;
        this.rowsByCustomer[customerId] = [];
    }
}
exports.CsvWriter = CsvWriter;
class NullWriter {
    beginScript(scriptName, query) { }
    beginCustomer(customerId) { }
    addRow(customerId, parsedRow, rawRow) { }
    endCustomer(customerId) { }
    endScript() { }
}
exports.NullWriter = NullWriter;
//# sourceMappingURL=csv-writer.js.map