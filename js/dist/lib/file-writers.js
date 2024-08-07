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
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (k !== "default" && Object.prototype.hasOwnProperty.call(mod, k)) __createBinding(result, mod, k);
    __setModuleDefault(result, mod);
    return result;
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.NullWriter = exports.CsvWriter = exports.JsonWriter = exports.FileWriterBase = exports.JsonValueFormat = exports.JsonOutputFormat = void 0;
const sync_1 = require("csv-stringify/sync");
const fs_1 = __importDefault(require("fs"));
const fs_async = __importStar(require("node:fs/promises"));
const path_1 = __importDefault(require("path"));
const storage_1 = require("@google-cloud/storage");
const logger_1 = require("./logger");
/**
 * File format mode for JSON
 */
var JsonOutputFormat;
(function (JsonOutputFormat) {
    /**
     * Array at the root with all rows as items.
     */
    JsonOutputFormat["json"] = "json";
    /**
     * Every row is a line
     */
    JsonOutputFormat["jsonl"] = "jsonl";
})(JsonOutputFormat = exports.JsonOutputFormat || (exports.JsonOutputFormat = {}));
/**
 * Formatting modes for values.
 */
var JsonValueFormat;
(function (JsonValueFormat) {
    /**
     * Output rows as they received from the API (hierarchical objects)
     */
    JsonValueFormat["raw"] = "raw";
    /**
     * Output rows as arrays (every query's column is an array's value).
     */
    JsonValueFormat["arrays"] = "arrays";
    /**
     * Output rows as objects (compared to raw an object is flatten
     * where each query's column correspondes to a field).
     */
    JsonValueFormat["objects"] = "objects";
})(JsonValueFormat = exports.JsonValueFormat || (exports.JsonValueFormat = {}));
class Output {
    constructor(path, stream, getStorageFile) {
        this.path = path;
        this.stream = stream;
        this.isGCS = this.path.startsWith("gs://");
        this.getStorageFile = getStorageFile;
    }
    async deleteFile() {
        if (this.isGCS) {
            await this.getStorageFile().delete({ ignoreNotFound: true });
        }
        else if (fs_1.default.existsSync(this.path)) {
            await fs_async.rm(this.path);
        }
    }
}
/**
 * Base class for all file-based writers.
 */
class FileWriterBase {
    constructor(options) {
        this.fileExtension = "";
        this.appending = false;
        this.rowWritten = false;
        this.destination = (options === null || options === void 0 ? void 0 : options.outputPath) || (options === null || options === void 0 ? void 0 : options.destinationFolder);
        this.filePerCustomer = !!(options === null || options === void 0 ? void 0 : options.filePerCustomer);
        this.streamsByCustomer = {};
        this.rowCountsByCustomer = {};
        this.logger = (0, logger_1.getLogger)();
    }
    beginScript(scriptName, query) {
        this.appending = false;
        this.query = query;
        this.scriptName = scriptName;
        this.streamsByCustomer = {};
        if (this.destination) {
            if (!fs_1.default.existsSync(this.destination)) {
                fs_1.default.mkdirSync(this.destination, { recursive: true });
            }
        }
        this.onBeginScript(scriptName, query);
    }
    onBeginScript(scriptName, query) { }
    async beginCustomer(customerId) {
        this.rowCountsByCustomer[customerId] = 0;
        const filePath = this.getDataFilepath(this.getFileName(customerId));
        let output;
        if (this.useFilePerCustomer()) {
            output = this.createOutput(filePath);
            this.streamsByCustomer[customerId] = output;
        }
        else {
            // all customers into one file
            if (!this.streamsByCustomer[""]) {
                output = this.createOutput(filePath);
                this.streamsByCustomer[""] = output;
            }
        }
        if (!output) {
            output = this.streamsByCustomer[""];
        }
        await this.onBeginCustomer(customerId, output);
        if (!this.useFilePerCustomer()) {
            this.appending = true;
        }
    }
    onBeginCustomer(customerId, output) { }
    useFilePerCustomer() {
        var _a;
        if ((_a = this.query) === null || _a === void 0 ? void 0 : _a.resource.isConstant)
            return false;
        return this.filePerCustomer;
    }
    getFileName(customerId) {
        let filename = "";
        if (this.useFilePerCustomer()) {
            filename = `${this.scriptName}_${customerId}.${this.fileExtension}`;
        }
        else {
            filename = `${this.scriptName}.${this.fileExtension}`;
        }
        return filename;
    }
    getDataFilepath(filename) {
        let filepath = filename;
        if (this.destination) {
            filepath = this.destination;
            if (!this.destination.endsWith("/"))
                filepath += "/";
            filepath += filename;
        }
        else if (process.env.K_SERVICE) {
            // we're in GCloud - file system is readonly, the only writable place is /tmp
            filepath = path_1.default.join("/tmp", filepath);
        }
        return filepath;
    }
    createOutput(filePath) {
        let writeStream;
        let getStorageFile;
        if (filePath.startsWith("gs://")) {
            let parsed = new URL(filePath);
            let bucketName = parsed.hostname;
            let destFileName = parsed.pathname.substring(1);
            const storage = new storage_1.Storage();
            const bucket = storage.bucket(bucketName);
            const file = bucket.file(destFileName);
            writeStream = file.createWriteStream({
                // surprisingly setting highWaterMark is crucial,
                // w/ o it we'll get unlimited memory growth
                highWaterMark: 1024 * 1024,
            });
            getStorageFile = () => {
                const storage = new storage_1.Storage();
                return storage.bucket(bucketName).file(destFileName);
            };
        }
        else {
            // local files
            writeStream = fs_1.default.createWriteStream(filePath);
        }
        return new Output(filePath, writeStream, getStorageFile);
    }
    getOutput(customerId) {
        let output;
        if (this.useFilePerCustomer()) {
            output = this.streamsByCustomer[customerId];
        }
        else {
            // all customers into one file
            output = this.streamsByCustomer[""];
        }
        return output;
    }
    async addRow(customerId, parsedRow, rawRow) {
        let firstRow;
        if (!parsedRow || parsedRow.length == 0)
            return;
        if (this.useFilePerCustomer()) {
            const count = this.rowCountsByCustomer[customerId];
            firstRow = count === 0;
        }
        else {
            firstRow = !this.rowWritten;
        }
        this.rowWritten = true;
        await this.onAddRow(customerId, parsedRow, rawRow, firstRow);
        this.rowCountsByCustomer[customerId] += 1;
    }
    async onAddRow(customerId, parsedRow, rawRow, firstRow) { }
    async endCustomer(customerId) {
        let output = this.getOutput(customerId);
        await this.onEndCustomer(customerId, output);
        // finalize the output stream
        if (this.useFilePerCustomer()) {
            await this.closeStream(output);
            delete this.streamsByCustomer[customerId];
        }
    }
    onEndCustomer(customerId, output) { }
    async endScript() {
        if (!this.useFilePerCustomer()) {
            // single file for all customer
            const output = this.streamsByCustomer[""];
            await this.closeStream(output);
        }
        this.streamsByCustomer = {};
        this.scriptName = undefined;
        this.rowWritten = false;
    }
    async closeStream(output) {
        await this.onClosingStream(output);
        const stream = output.stream;
        this.logger.debug(`Closing stream ${output.path}`);
        await new Promise((resolve, reject) => {
            stream.once("close", () => {
                this.logger.debug(`Closed stream ${output.path}, exists: ${fs_1.default.existsSync(output.path)}`);
                resolve(null);
            });
            stream.once("error", reject);
            stream.end((err) => {
                if (err) {
                    reject(err);
                }
            });
        });
    }
    async onClosingStream(output) { }
    async writeToStream(output, content) {
        const writeStream = output.stream;
        await new Promise((resolve, reject) => {
            const cb = (error) => {
                if (error) {
                    reject(error);
                }
                else {
                    resolve(null);
                }
            };
            const success = writeStream.write(content, cb);
            if (!success) {
                writeStream.once("drain", cb);
            }
            else {
                process.nextTick(cb);
            }
        });
        // const success = writeStream.write(content);
        // // Handle backpressure (if stream is overwhelmed)
        // if (!success) {
        //   await new Promise((resolve) => writeStream.once("drain", resolve));
        // }
    }
    async writeContent(customerId, content) {
        let output = this.getOutput(customerId);
        await this.writeToStream(output, content);
    }
}
exports.FileWriterBase = FileWriterBase;
class JsonWriter extends FileWriterBase {
    constructor(options) {
        super(options);
        this.fileExtension = "json";
        this.format = (options === null || options === void 0 ? void 0 : options.format) || JsonOutputFormat.jsonl;
        this.formatted =
            this.format === JsonOutputFormat.json ? !!(options === null || options === void 0 ? void 0 : options.formatted) : false;
        this.valueFormat = (options === null || options === void 0 ? void 0 : options.valueFormat) || JsonValueFormat.objects;
    }
    // override async onBeginCustomer(
    //   customerId: string,
    //   output: Output
    // ): Promise<void> {
    //   let content = "";
    //   if (!this.appending) {
    //     // starting a new file
    //     if (this.format === JsonOutputFormat.json) {
    //       content = "[\n";
    //       await this.writeToStream(output, content);
    //     }
    //     if (this.valueFormat === JsonValueFormat.arrays) {
    //       let content = JSON.stringify(this.query!.columnNames);
    //       if (this.format === JsonOutputFormat.json) {
    //         content += ",\n";
    //       } else {
    //         content += "\n";
    //       }
    //       await this.writeToStream(output, content);
    //     }
    //   }
    // }
    serializeRow(parsedRow, rawRow) {
        let rowObj;
        if (this.valueFormat === JsonValueFormat.raw) {
            rowObj = rawRow;
        }
        else if (this.valueFormat === JsonValueFormat.objects) {
            let obj = this.query.columnNames.reduce((obj, key, index) => ({ ...obj, [key]: parsedRow[index] }), {});
            rowObj = obj;
        }
        else {
            // i.e. JsonValueFormat.arrays
            rowObj = parsedRow;
        }
        let content = JSON.stringify(rowObj, null, this.formatted ? 2 : undefined);
        return content;
    }
    async onAddRow(customerId, parsedRow, rawRow, firstRow) {
        let content = "";
        if (firstRow) {
            // starting a new file
            if (this.format === JsonOutputFormat.json) {
                content += "[\n";
            }
            if (this.valueFormat === JsonValueFormat.arrays) {
                content += JSON.stringify(this.query.columnNames);
                if (this.format === JsonOutputFormat.json) {
                    content += ",\n";
                }
                else {
                    content += "\n";
                }
            }
        }
        content += this.serializeRow(parsedRow, rawRow);
        if (this.format === JsonOutputFormat.json) {
            if (!firstRow) {
                content = ",\n" + content;
            }
        }
        else {
            content += "\n";
        }
        await this.writeContent(customerId, content);
        this.rowCountsByCustomer[customerId] += 1;
    }
    async onClosingStream(output) {
        if (this.format === JsonOutputFormat.json) {
            const content = "\n]";
            await this.writeToStream(output, content);
        }
    }
}
exports.JsonWriter = JsonWriter;
class CsvWriter extends FileWriterBase {
    constructor(options) {
        super(options);
        this.fileExtension = "csv";
        this.quoted = !!(options === null || options === void 0 ? void 0 : options.quoted);
        this.arraySeparator = (options === null || options === void 0 ? void 0 : options.arraySeparator) || "|";
    }
    onBeginScript(scriptName, query) {
        this.csvOptions = {
            header: false,
            quoted: this.quoted,
            columns: query.columns.map((col) => col.name),
            cast: {
                boolean: (value, context) => value ? "true" : "false",
                object: (value, context) => Array.isArray(value)
                    ? value.join(this.arraySeparator)
                    : JSON.stringify(value),
            },
        };
    }
    async onAddRow(customerId, parsedRow, rawRow, firstRow) {
        let opts = this.csvOptions;
        if (firstRow) {
            opts = Object.assign({}, this.csvOptions, { header: true });
        }
        let csvText = (0, sync_1.stringify)([parsedRow], opts);
        await this.writeContent(customerId, csvText);
        this.rowCountsByCustomer[customerId] += 1;
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