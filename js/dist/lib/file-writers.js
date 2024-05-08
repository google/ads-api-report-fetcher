"use strict";
/**
 * Copyright 2024 Google LLC
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
exports.NullWriter = exports.CsvWriter = exports.JsonWriter = exports.JsonValueFormat = exports.JsonOutputFormat = void 0;
const sync_1 = require("csv-stringify/sync");
const fs_1 = __importDefault(require("fs"));
const path_1 = __importDefault(require("path"));
const logger_1 = require("./logger");
var JsonOutputFormat;
(function (JsonOutputFormat) {
    JsonOutputFormat["json"] = "json";
    JsonOutputFormat["jsonl"] = "jsonl";
})(JsonOutputFormat = exports.JsonOutputFormat || (exports.JsonOutputFormat = {}));
var JsonValueFormat;
(function (JsonValueFormat) {
    JsonValueFormat["raw"] = "raw";
    JsonValueFormat["arrays"] = "arrays";
    JsonValueFormat["objects"] = "objects";
})(JsonValueFormat = exports.JsonValueFormat || (exports.JsonValueFormat = {}));
class FileWriterBase {
    constructor(options) {
        this.appending = false;
        this.customerRows = 0;
        this.rowsByCustomer = {};
        this.destination = options === null || options === void 0 ? void 0 : options.destinationFolder;
        this.filePerCustomer = !!(options === null || options === void 0 ? void 0 : options.filePerCustomer);
        this.logger = (0, logger_1.getLogger)();
    }
    beginScript(scriptName, query) {
        this.appending = false;
        this.query = query;
        this.scriptName = scriptName;
        if (this.destination) {
            if (!fs_1.default.existsSync(this.destination)) {
                fs_1.default.mkdirSync(this.destination, { recursive: true });
            }
        }
    }
    beginCustomer(customerId) {
        this.rowsByCustomer[customerId] = [];
    }
    addRow(customerId, parsedRow, rawRow) {
        if (!parsedRow || parsedRow.length == 0)
            return;
        this.rowsByCustomer[customerId].push(parsedRow);
    }
    endScript() {
        this.scriptName = undefined;
    }
    _getFileName(customerId) {
        let filename = "";
        if (this.filePerCustomer) {
            filename = `${this.scriptName}_${customerId}.${this.fileExtension}`;
        }
        else {
            filename = `${this.scriptName}.${this.fileExtension}`;
        }
        if (this.destination) {
            filename = path_1.default.join(this.destination, filename);
        }
        return filename;
    }
}
class JsonWriter extends FileWriterBase {
    constructor(options) {
        super(options);
        this.fileExtension = "json";
        this.format = (options === null || options === void 0 ? void 0 : options.format) || JsonOutputFormat.json;
        this.formatted =
            this.format === JsonOutputFormat.json ? !!(options === null || options === void 0 ? void 0 : options.formatted) : false;
        this.valueFormat = (options === null || options === void 0 ? void 0 : options.valueFormat) || JsonValueFormat.objects;
    }
    addRow(customerId, parsedRow, rawRow) {
        if (!parsedRow || parsedRow.length == 0)
            return;
        if (this.valueFormat === JsonValueFormat.raw) {
            this.rowsByCustomer[customerId].push(rawRow);
        }
        else if (this.valueFormat === JsonValueFormat.objects) {
            let obj = this.query.columnNames.reduce((obj, key, index) => ({ ...obj, [key]: parsedRow[index] }), {});
            this.rowsByCustomer[customerId].push(obj);
        }
        else {
            // i.e. JsonValueFormat.arrays
            this.rowsByCustomer[customerId].push(parsedRow);
        }
    }
    endCustomer(customerId) {
        let rows = this.rowsByCustomer[customerId];
        if (!rows.length) {
            return;
        }
        let appending = this.appending && !this.filePerCustomer;
        let filename = this._getFileName(customerId);
        let content = "";
        if (this.valueFormat === JsonValueFormat.arrays && !appending) {
            rows.unshift(this.query.columnNames);
        }
        if (this.format === JsonOutputFormat.jsonl) {
            if (appending) {
                content += "\n";
            }
            content += rows.map((val) => JSON.stringify(val)).join("\n");
        }
        else {
            if (!appending) {
                content = "[\n";
            }
            else {
                content += ",\n";
            }
            content += rows
                .map((val) => JSON.stringify(val, null, this.formatted ? 2 : undefined))
                .join(",\n");
            if (this.filePerCustomer) {
                content += "\n]";
            }
        }
        fs_1.default.writeFileSync(filename, content, {
            encoding: "utf-8",
            flag: appending ? "a" : "w",
        });
        if (rows.length > 0) {
            this.logger.info((appending ? "Updated " : "Created ") +
                filename +
                ` with ${rows.length} rows`, { customerId: customerId, scriptName: filename });
        }
        this.appending = true;
        this.rowsByCustomer[customerId] = [];
    }
    endScript() {
        if (this.format === JsonOutputFormat.json && this.appending &&
            !this.filePerCustomer) {
            let filename = this._getFileName("");
            fs_1.default.writeFileSync(filename, "\n]", {
                encoding: "utf-8",
                flag: "a",
            });
        }
        this.appending = false;
        this.scriptName = undefined;
    }
}
exports.JsonWriter = JsonWriter;
class CsvWriter extends FileWriterBase {
    constructor(options) {
        super(options);
        this.quoted = !!(options === null || options === void 0 ? void 0 : options.quoted);
        this.arraySeparator = (options === null || options === void 0 ? void 0 : options.arraySeparator) || "|";
        this.fileExtension = "csv";
    }
    endCustomer(customerId) {
        let rows = this.rowsByCustomer[customerId];
        if (!rows.length) {
            return;
        }
        let appending = this.appending && !this.filePerCustomer;
        let filename = this._getFileName(customerId);
        let csvOptions = {
            header: !appending,
            quoted: this.quoted,
            columns: this.query.columns.map((col) => col.name),
            cast: {
                boolean: (value, context) => value ? "true" : "false",
                object: (value, context) => Array.isArray(value)
                    ? value.join(this.arraySeparator)
                    : JSON.stringify(value),
            },
        };
        let csvText = (0, sync_1.stringify)(rows, csvOptions);
        fs_1.default.writeFileSync(filename, csvText, {
            encoding: "utf-8",
            flag: appending ? "a" : "w",
        });
        if (rows.length > 0) {
            this.logger.info((appending ? "Updated " : "Created ") +
                filename +
                ` with ${rows.length} rows`, { customerId: customerId, scriptName: filename });
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
//# sourceMappingURL=file-writers.js.map