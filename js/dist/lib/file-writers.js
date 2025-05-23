/**
 * Copyright 2025 Google LLC
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
import { stringify } from 'csv-stringify/sync';
import fs from 'fs';
import * as fs_async from 'node:fs/promises';
import path from 'path';
import { Storage } from '@google-cloud/storage';
import { getLogger } from './logger.js';
/**
 * File format mode for JSON
 */
export var JsonOutputFormat;
(function (JsonOutputFormat) {
    /**
     * Array at the root with all rows as items.
     */
    JsonOutputFormat["json"] = "json";
    /**
     * Every row is a line
     */
    JsonOutputFormat["jsonl"] = "jsonl";
})(JsonOutputFormat || (JsonOutputFormat = {}));
/**
 * Formatting modes for values.
 */
export var JsonValueFormat;
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
     * where each query's column corresponds to a field).
     */
    JsonValueFormat["objects"] = "objects";
})(JsonValueFormat || (JsonValueFormat = {}));
class Output {
    constructor(path, stream, getStorageFile) {
        this.path = path;
        this.stream = stream;
        this.isGCS = this.path.startsWith('gs://');
        this.getStorageFile = getStorageFile;
    }
    async deleteFile() {
        if (this.isGCS) {
            await this.getStorageFile().delete({ ignoreNotFound: true });
        }
        else if (fs.existsSync(this.path)) {
            await fs_async.rm(this.path);
        }
    }
}
/**
 * Base class for all file-based writers.
 */
export class FileWriterBase {
    constructor(options) {
        this.fileExtension = '';
        this.rowWritten = false;
        this.destination = (options === null || options === void 0 ? void 0 : options.outputPath) || (options === null || options === void 0 ? void 0 : options.destinationFolder);
        if (this.destination && !URL.canParse(this.destination)) {
            // it's a folder
            this.destination = path.resolve(this.destination);
        }
        this.filePerCustomer = !!(options === null || options === void 0 ? void 0 : options.filePerCustomer);
        this.streamsByCustomer = {};
        this.rowCountsByCustomer = {};
        this.logger = getLogger();
    }
    beginScript(scriptName, query) {
        this.query = query;
        this.scriptName = scriptName;
        this.streamsByCustomer = {};
        if (this.destination && !URL.canParse(this.destination)) {
            if (!fs.existsSync(this.destination)) {
                fs.mkdirSync(this.destination, { recursive: true });
            }
        }
        this.onBeginScript(scriptName, query);
    }
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    onBeginScript(scriptName, query) { }
    async beginCustomer(customerId) {
        this.rowCountsByCustomer[customerId] = 0;
        const filePath = this.getDataFilePath(this.getDataFileName(customerId));
        let output;
        if (this.useFilePerCustomer()) {
            output = this.createOutput(filePath);
            this.streamsByCustomer[customerId] = output;
        }
        else {
            // all customers into one file
            if (!this.streamsByCustomer['']) {
                output = this.createOutput(filePath);
                this.streamsByCustomer[''] = output;
            }
        }
        if (!output) {
            output = this.streamsByCustomer[''];
        }
        await this.onBeginCustomer(customerId, output);
    }
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    onBeginCustomer(customerId, output) { }
    useFilePerCustomer() {
        var _a;
        if ((_a = this.query) === null || _a === void 0 ? void 0 : _a.resource.isConstant)
            return false;
        return this.filePerCustomer;
    }
    getDataFileName(customerId) {
        let filename = '';
        if (this.useFilePerCustomer()) {
            filename = `${this.scriptName}_${customerId}.${this.fileExtension}`;
        }
        else {
            filename = `${this.scriptName}.${this.fileExtension}`;
        }
        return filename;
    }
    getDataFilePath(filename) {
        let filepath = filename;
        if (this.destination) {
            filepath = this.destination;
            if (!this.destination.endsWith('/'))
                filepath += '/';
            filepath += filename;
        }
        else if (process.env.K_SERVICE) {
            // we're in GCloud - file system is readonly, the only writable place is /tmp
            filepath = path.join('/tmp', filepath);
        }
        return filepath;
    }
    createOutput(filePath) {
        let writeStream;
        let getStorageFile;
        if (filePath.startsWith('gs://')) {
            const parsed = new URL(filePath);
            const bucketName = parsed.hostname;
            const destFileName = parsed.pathname.substring(1);
            const storage = new Storage({
                retryOptions: { autoRetry: true, maxRetries: 10 },
            });
            const bucket = storage.bucket(bucketName);
            const file = bucket.file(destFileName);
            writeStream = file.createWriteStream({
                // surprisingly setting highWaterMark is crucial,
                // w/ o it we'll get unlimited memory growth
                highWaterMark: 1024 * 1024,
                // setting for preventing sporadic errors 'Retry limit exceeded'
                resumable: false,
            });
            getStorageFile = () => {
                const storage = new Storage();
                return storage.bucket(bucketName).file(destFileName);
            };
            writeStream.on('error', e => {
                this.logger.error(`Error on writing to remote stream ${filePath}: ${e}`);
            });
        }
        else {
            // local files
            writeStream = fs.createWriteStream(filePath);
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
            output = this.streamsByCustomer[''];
        }
        return output;
    }
    async addRow(customerId, parsedRow, rawRow) {
        let firstRow;
        if (!parsedRow || parsedRow.length === 0)
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
    async onAddRow(
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    customerId, 
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    parsedRow, 
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    rawRow, 
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    firstRow) { }
    async endCustomer(customerId) {
        const output = this.getOutput(customerId);
        await this.onEndCustomer(customerId, output);
        // finalize the output stream
        if (this.useFilePerCustomer()) {
            await this.closeStream(output);
            delete this.streamsByCustomer[customerId];
        }
    }
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    onEndCustomer(customerId, output) { }
    async endScript() {
        if (!this.useFilePerCustomer()) {
            // single file for all customer
            const output = this.streamsByCustomer[''];
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
            stream.once('close', () => {
                this.logger.debug(`Closed stream ${output.path}, exists: ${fs.existsSync(output.path)}`);
                stream.removeAllListeners('error');
                resolve(null);
            });
            stream.once('error', reject);
            stream.end((err) => {
                if (err) {
                    reject(err);
                }
            });
        });
    }
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
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
                writeStream.once('drain', cb);
            }
            else {
                process.nextTick(cb);
            }
        });
    }
    async writeContent(customerId, content) {
        const output = this.getOutput(customerId);
        await this.writeToStream(output, content);
    }
}
export class JsonWriter extends FileWriterBase {
    constructor(options) {
        super(options);
        this.fileExtension = 'json';
        this.format = (options === null || options === void 0 ? void 0 : options.format) || JsonOutputFormat.jsonl;
        this.formatted =
            this.format === JsonOutputFormat.json ? !!(options === null || options === void 0 ? void 0 : options.formatted) : false;
        this.valueFormat = (options === null || options === void 0 ? void 0 : options.valueFormat) || JsonValueFormat.objects;
    }
    serializeRow(parsedRow, rawRow) {
        let rowObj;
        if (this.valueFormat === JsonValueFormat.raw) {
            rowObj = rawRow;
        }
        else if (this.valueFormat === JsonValueFormat.objects) {
            const obj = this.query.columnNames.reduce((obj, key, index) => ({ ...obj, [key]: parsedRow[index] }), {});
            rowObj = obj;
        }
        else {
            // i.e. JsonValueFormat.arrays
            rowObj = parsedRow;
        }
        const content = JSON.stringify(rowObj, null, this.formatted ? 2 : undefined);
        return content;
    }
    async onAddRow(customerId, parsedRow, rawRow, firstRow) {
        let content = '';
        if (firstRow) {
            // starting a new file
            if (this.format === JsonOutputFormat.json) {
                content += '[\n';
            }
            if (this.valueFormat === JsonValueFormat.arrays) {
                content += JSON.stringify(this.query.columnNames);
                if (this.format === JsonOutputFormat.json) {
                    content += ',\n';
                }
                else {
                    content += '\n';
                }
            }
        }
        content += this.serializeRow(parsedRow, rawRow);
        if (this.format === JsonOutputFormat.json) {
            if (!firstRow) {
                content = ',\n' + content;
            }
        }
        else {
            content += '\n';
        }
        await this.writeContent(customerId, content);
        this.rowCountsByCustomer[customerId] += 1;
    }
    async onClosingStream(output) {
        if (this.format === JsonOutputFormat.json) {
            const content = '\n]';
            await this.writeToStream(output, content);
        }
    }
}
export class CsvWriter extends FileWriterBase {
    constructor(options) {
        super(options);
        this.fileExtension = 'csv';
        this.quoted = !!(options === null || options === void 0 ? void 0 : options.quoted);
        this.arraySeparator = (options === null || options === void 0 ? void 0 : options.arraySeparator) || '|';
    }
    onBeginScript(scriptName, query) {
        this.csvOptions = {
            header: false,
            quoted: this.quoted,
            columns: query.columns.map(col => col.name),
            cast: {
                // eslint-disable-next-line @typescript-eslint/no-unused-vars
                boolean: (value, context) => value ? 'true' : 'false',
                // eslint-disable-next-line @typescript-eslint/no-unused-vars
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
        const csvText = stringify([parsedRow], opts);
        await this.writeContent(customerId, csvText);
        this.rowCountsByCustomer[customerId] += 1;
    }
}
export class NullWriter {
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    beginScript(scriptName, query) { }
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    beginCustomer(customerId) { }
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    addRow(customerId, parsedRow, rawRow) { }
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    endCustomer(customerId) { }
    endScript() { }
}
//# sourceMappingURL=file-writers.js.map