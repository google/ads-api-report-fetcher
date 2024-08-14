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

import csvStringify from "csv-stringify";
import { stringify } from "csv-stringify/sync";
import fs from "fs";
import * as fs_async from "node:fs/promises";
import * as stream from "node:stream";
import path from "path";
import { Storage, File } from "@google-cloud/storage";

import { getLogger } from "./logger";
import {
  ArrayHandling,
  IResultWriter,
  QueryElements,
  QueryResult,
} from "./types";

/**
 * Base options for all file-based writers.
 */
export interface FileWriterOptions {
  /**
   * @deprecated use outputPath.
   */
  destinationFolder?: string | undefined;
  /**
   * Folder or GCS path for output files.
   */
  outputPath?: string | undefined;
  /**
   * Create file per customer (true) or put all customers into one file (false).
   */
  filePerCustomer?: boolean | undefined;
}
/**
 * Options for CsvWriter.
 */
export interface CsvWriterOptions extends FileWriterOptions {
  /**
   * Separator symbol for arrays' values. By default it's "|".
   */
  arraySeparator?: string | undefined;
  /**
   * Wrap values in quotes.
   */
  quoted?: boolean;
}
/**
 * File format mode for JSON
 */
export enum JsonOutputFormat {
  /**
   * Array at the root with all rows as items.
   */
  json = "json",
  /**
   * Every row is a line
   */
  jsonl = "jsonl",
}
/**
 * Formatting modes for values.
 */
export enum JsonValueFormat {
  /**
   * Output rows as they received from the API (hierarchical objects)
   */
  raw = "raw",
  /**
   * Output rows as arrays (every query's column is an array's value).
   */
  arrays = "arrays",
  /**
   * Output rows as objects (compared to raw an object is flatten
   * where each query's column correspondes to a field).
   */
  objects = "objects",
}
/**
 * Options for JsonWriter.
 */
export interface JsonWriterOptions extends FileWriterOptions {
  /**
   * File formats: json or json new line
   */
  format?: JsonOutputFormat;
  /**
   * How to format values.
   */
  valueFormat?: JsonValueFormat;
  /**
   * True to do nice formatting with indents if format is json
   * (i.e. it's not applicable for jsonl).
   */
  formatted?: boolean;
}
/**
 * Output destination for writing serialized data.
 */
export interface IOutput {
  /**
   * Output stream to write to.
   */
  stream: stream.Writable;
  /**
   * Full output path (local file or GCS path)
   */
  path: string;
  /**
   * True if the output is a GCS destination.
   */
  isGCS: boolean;
  /**
   * Return a GCS File descriptor.
   */
  getStorageFile: (() => File) | undefined;
  /**
   * Delete the file regardless of destination.
   */
  deleteFile(): Promise<void>;
}

class Output implements IOutput {
  stream: stream.Writable;
  path: string;
  isGCS: boolean;
  getStorageFile: (() => File) | undefined;

  constructor(
    path: string,
    stream: stream.Writable,
    getStorageFile: (() => File) | undefined
  ) {
    this.path = path;
    this.stream = stream;
    this.isGCS = this.path.startsWith("gs://");
    this.getStorageFile = getStorageFile;
  }

  async deleteFile() {
    if (this.isGCS) {
      await this.getStorageFile!().delete({ ignoreNotFound: true });
    } else if (fs.existsSync(this.path)) {
      await fs_async.rm(this.path);
    }
  }
}

/**
 * Base class for all file-based writers.
 */
export abstract class FileWriterBase implements IResultWriter {
  destination: string | undefined;
  filePerCustomer: boolean;
  logger;
  fileExtension: string = "";
  scriptName: string | undefined;
  streamsByCustomer: Record<string, IOutput>;
  query: QueryElements | undefined;
  rowCountsByCustomer: Record<string, number>;
  rowWritten = false;

  constructor(options?: FileWriterOptions) {
    this.destination = options?.outputPath || options?.destinationFolder;
    if (this.destination && !URL.canParse(this.destination)) {
      // it's a folder
      this.destination = path.resolve(this.destination);
    }
    this.filePerCustomer = !!options?.filePerCustomer;
    this.streamsByCustomer = {};
    this.rowCountsByCustomer = {};
    this.logger = getLogger();
  }

  beginScript(scriptName: string, query: QueryElements) {
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

  protected onBeginScript(scriptName: string, query: QueryElements): void {}

  async beginCustomer(customerId: string) {
    this.rowCountsByCustomer[customerId] = 0;
    const filePath = this.getDataFilePath(this.getDataFileName(customerId));
    let output: Output | undefined;
    if (this.useFilePerCustomer()) {
      output = this.createOutput(filePath);
      this.streamsByCustomer[customerId] = output;
    } else {
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
  }

  protected onBeginCustomer(customerId: string, output: Output): void {}

  protected useFilePerCustomer() {
    if (this.query?.resource.isConstant) return false;
    return this.filePerCustomer;
  }

  protected getDataFileName(customerId: string) {
    let filename = "";
    if (this.useFilePerCustomer()) {
      filename = `${this.scriptName}_${customerId}.${this.fileExtension}`;
    } else {
      filename = `${this.scriptName}.${this.fileExtension}`;
    }
    return filename;
  }

  protected getDataFilePath(filename: string) {
    let filepath = filename;
    if (this.destination) {
      filepath = this.destination;
      if (!this.destination.endsWith("/")) filepath += "/";
      filepath += filename;
    } else if (process.env.K_SERVICE) {
      // we're in GCloud - file system is readonly, the only writable place is /tmp
      filepath = path.join("/tmp", filepath);
    }
    return filepath;
  }

  protected createOutput(filePath: string) {
    let writeStream: stream.Writable;
    let getStorageFile;
    if (filePath.startsWith("gs://")) {
      let parsed = new URL(filePath);
      let bucketName = parsed.hostname;
      let destFileName = parsed.pathname.substring(1);
      const storage = new Storage({
        retryOptions: { autoRetry: true, maxRetries: 10 },
      });
      const bucket = storage.bucket(bucketName);
      const file = bucket.file(destFileName);
      writeStream = file.createWriteStream({
        // surprisingly setting highWaterMark is crucial,
        // w/ o it we'll get unlimited memory growth
        highWaterMark: 1024 * 1024,
        // setting for preventing sparodic errors 'Retry limit exceeded'
        resumable: false,
      });
      getStorageFile = () => {
        const storage = new Storage();
        return storage.bucket(bucketName).file(destFileName);
      };
      writeStream.on("error", (e) => {
        this.logger.error(
          `Error on writing to remote stream ${filePath}: ${e}`
        );
      });
    } else {
      // local files
      writeStream = fs.createWriteStream(filePath);
    }
    return new Output(filePath, writeStream, getStorageFile);
  }

  protected getOutput(customerId: string) {
    let output;
    if (this.useFilePerCustomer()) {
      output = this.streamsByCustomer[customerId];
    } else {
      // all customers into one file
      output = this.streamsByCustomer[""];
    }
    return output;
  }

  async addRow(
    customerId: string,
    parsedRow: any[],
    rawRow: any[]
  ): Promise<void> {
    let firstRow;
    if (!parsedRow || parsedRow.length == 0) return;
    if (this.useFilePerCustomer()) {
      const count = this.rowCountsByCustomer[customerId];
      firstRow = count === 0;
    } else {
      firstRow = !this.rowWritten;
    }
    this.rowWritten = true;
    await this.onAddRow(customerId, parsedRow, rawRow, firstRow);
    this.rowCountsByCustomer[customerId] += 1;
  }

  protected async onAddRow(
    customerId: string,
    parsedRow: any[],
    rawRow: any[],
    firstRow: boolean
  ): Promise<void> {}

  async endCustomer(customerId: string): Promise<void> {
    let output = this.getOutput(customerId);
    await this.onEndCustomer(customerId, output);
    // finalize the output stream
    if (this.useFilePerCustomer()) {
      await this.closeStream(output);
      delete this.streamsByCustomer[customerId];
    }
  }

  protected onEndCustomer(customerId: string, output: Output): void {}

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

  protected async closeStream(output: Output) {
    await this.onClosingStream(output);
    const stream = output.stream;
    this.logger.debug(`Closing stream ${output.path}`);
    await new Promise((resolve, reject) => {
      stream.once("close", () => {
        this.logger.debug(
          `Closed stream ${output.path}, exists: ${fs.existsSync(output.path)}`
        );
        stream.removeAllListeners("error");
        resolve(null);
      });
      stream.once("error", reject);
      stream.end((err: any) => {
        if (err) {
          reject(err);
        }
      });
    });
  }

  protected async onClosingStream(output: Output): Promise<void> {}

  protected async writeToStream(output: Output, content: string) {
    const writeStream = output.stream;
    await new Promise((resolve, reject) => {
      const cb = (error: Error | null | undefined) => {
        if (error) {
          reject(error);
        } else {
          resolve(null);
        }
      };
      const success = writeStream.write(content, cb);
      if (!success) {
        writeStream.once("drain", cb);
      } else {
        process.nextTick(cb);
      }
    });
  }

  protected async writeContent(customerId: string, content: string) {
    let output = this.getOutput(customerId);
    await this.writeToStream(output, content);
  }
}

export class JsonWriter extends FileWriterBase {
  format: JsonOutputFormat;
  formatted: boolean;
  valueFormat: JsonValueFormat;

  constructor(options?: JsonWriterOptions) {
    super(options);
    this.fileExtension = "json";
    this.format = options?.format || JsonOutputFormat.jsonl;
    this.formatted =
      this.format === JsonOutputFormat.json ? !!options?.formatted : false;
    this.valueFormat = options?.valueFormat || JsonValueFormat.objects;
  }

  protected serializeRow(parsedRow: any[], rawRow: any[]) {
    let rowObj: any;
    if (this.valueFormat === JsonValueFormat.raw) {
      rowObj = rawRow;
    } else if (this.valueFormat === JsonValueFormat.objects) {
      let obj = this.query!.columnNames.reduce(
        (obj, key, index) => ({ ...obj, [key]: parsedRow[index] }),
        {}
      );
      rowObj = <any>obj;
    } else {
      // i.e. JsonValueFormat.arrays
      rowObj = parsedRow;
    }
    let content = JSON.stringify(rowObj, null, this.formatted ? 2 : undefined);
    return content;
  }

  override async onAddRow(
    customerId: string,
    parsedRow: any[],
    rawRow: any[],
    firstRow: boolean
  ) {
    let content = "";
    if (firstRow) {
      // starting a new file
      if (this.format === JsonOutputFormat.json) {
        content += "[\n";
      }
      if (this.valueFormat === JsonValueFormat.arrays) {
        content += JSON.stringify(this.query!.columnNames);
        if (this.format === JsonOutputFormat.json) {
          content += ",\n";
        } else {
          content += "\n";
        }
      }
    }
    content += this.serializeRow(parsedRow, rawRow);
    if (this.format === JsonOutputFormat.json) {
      if (!firstRow) {
        content = ",\n" + content;
      }
    } else {
      content += "\n";
    }
    await this.writeContent(customerId, content);
    this.rowCountsByCustomer[customerId] += 1;
  }

  override async onClosingStream(output: Output): Promise<void> {
    if (this.format === JsonOutputFormat.json) {
      const content = "\n]";
      await this.writeToStream(output, content);
    }
  }
}

export class CsvWriter extends FileWriterBase {
  quoted: boolean;
  arraySeparator: string;
  csvOptions: csvStringify.Options | undefined;

  constructor(options?: CsvWriterOptions) {
    super(options);
    this.fileExtension = "csv";
    this.quoted = !!options?.quoted;
    this.arraySeparator = options?.arraySeparator || "|";
  }

  protected onBeginScript(scriptName: string, query: QueryElements): void {
    this.csvOptions = {
      header: false,
      quoted: this.quoted,
      columns: query!.columns.map((col) => col.name),
      cast: {
        boolean: (value: boolean, context: csvStringify.CastingContext) =>
          value ? "true" : "false",
        object: (value: object, context: csvStringify.CastingContext) =>
          Array.isArray(value)
            ? value.join(this.arraySeparator)
            : JSON.stringify(value),
      },
    };
  }

  override async onAddRow(
    customerId: string,
    parsedRow: any[],
    rawRow: any[],
    firstRow: boolean
  ) {
    let opts = this.csvOptions;
    if (firstRow) {
      opts = Object.assign({}, this.csvOptions, { header: true });
    }
    let csvText = stringify([parsedRow], opts);
    await this.writeContent(customerId, csvText);
    this.rowCountsByCustomer[customerId] += 1;
  }
}

export class NullWriter implements IResultWriter {
  beginScript(scriptName: string, query: QueryElements): void | Promise<void> {}
  beginCustomer(customerId: string): void | Promise<void> {}
  addRow(customerId: string, parsedRow: any[], rawRow: any[]): void {}
  endCustomer(customerId: string): void | Promise<void> {}
  endScript(): void | Promise<void> {}
}
