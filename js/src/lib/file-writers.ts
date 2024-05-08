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

import csvStringify from 'csv-stringify';
import {stringify} from 'csv-stringify/sync';
import fs from 'fs';
import path from 'path';

import {getLogger} from './logger';
import {ArrayHandling, IResultWriter, QueryElements, QueryResult} from './types';

export interface FileWriterOptions {
  destinationFolder?: string | undefined;
  filePerCustomer?: boolean | undefined;
}
export interface CsvWriterOptions extends FileWriterOptions {
  arraySeparator?: string | undefined;
  quoted?: boolean;
}
export enum JsonOutputFormat {
  json = 'json',
  jsonl = 'jsonl',
}
export enum JsonValueFormat {
  raw = "raw",
  arrays = "arrays",
  objects = "objects",
}
export interface JsonWriterOptions extends FileWriterOptions {
  format?: JsonOutputFormat;
  valueFormat?: JsonValueFormat;
  formatted?: boolean;
}

abstract class FileWriterBase implements IResultWriter {
  destination: string | undefined;
  filePerCustomer: boolean;
  logger;
  abstract fileExtension: string;
  scriptName: string | undefined;
  appending = false;
  customerRows = 0;
  rowsByCustomer: Record<string, any[][]> = {};
  query: QueryElements | undefined;

  constructor(options?: FileWriterOptions) {
    this.destination = options?.destinationFolder;
    this.filePerCustomer = !!options?.filePerCustomer;
    this.logger = getLogger();
  }

  beginScript(scriptName: string, query: QueryElements) {
    this.appending = false;
    this.query = query;
    this.scriptName = scriptName;

    if (this.destination) {
      if (!fs.existsSync(this.destination)) {
        fs.mkdirSync(this.destination, { recursive: true });
      }
    }
  }

  beginCustomer(customerId: string) {
    this.rowsByCustomer[customerId] = [];
  }

  addRow(customerId: string, parsedRow: any[], rawRow: any[]) {
    if (!parsedRow || parsedRow.length == 0) return;
    this.rowsByCustomer[customerId].push(parsedRow);
  }

  abstract endCustomer(customerId: string): void | Promise<void>;

  endScript() {
    this.scriptName = undefined;
  }

  _getFileName(customerId: string) {
    let filename = "";
    if (this.filePerCustomer) {
      filename = `${this.scriptName}_${customerId}.${this.fileExtension}`;
    } else {
      filename = `${this.scriptName}.${this.fileExtension}`;
    }
    if (this.destination) {
      filename = path.join(this.destination, filename);
    }
    return filename;
  }
}

export class JsonWriter extends FileWriterBase {
  fileExtension: string;
  format: JsonOutputFormat;
  formatted: boolean;
  valueFormat: JsonValueFormat;

  constructor(options?: JsonWriterOptions) {
    super(options);
    this.fileExtension = "json";
    this.format = options?.format || JsonOutputFormat.json;
    this.formatted =
      this.format === JsonOutputFormat.json ? !!options?.formatted : false;
    this.valueFormat = options?.valueFormat || JsonValueFormat.objects;
  }

  addRow(customerId: string, parsedRow: any[], rawRow: any[]) {
    if (!parsedRow || parsedRow.length == 0) return;
    if (this.valueFormat === JsonValueFormat.raw) {
      this.rowsByCustomer[customerId].push(rawRow);
    } else if (this.valueFormat === JsonValueFormat.objects) {
      let obj = this.query!.columnNames.reduce(
        (obj, key, index) => ({ ...obj, [key]: parsedRow[index] }),
        {}
      );
      this.rowsByCustomer[customerId].push(<any>obj);
    } else {
      // i.e. JsonValueFormat.arrays
      this.rowsByCustomer[customerId].push(parsedRow);
    }
  }

  endCustomer(customerId: string) {
    let rows = this.rowsByCustomer[customerId];
    if (!rows.length) {
      return;
    }
    let appending = this.appending && !this.filePerCustomer;
    let filename = this._getFileName(customerId);

    let content = "";
    if (this.valueFormat === JsonValueFormat.arrays && !appending) {
      rows.unshift(this.query!.columnNames);
    }
    if (this.format === JsonOutputFormat.jsonl) {
      if (appending) {
        content += "\n";
      }
      content += rows.map((val) => JSON.stringify(val)).join("\n");
    } else {
      if (!appending) {
        content = "[\n";
      } else {
        content += ",\n";
      }
      content += rows
        .map((val) => JSON.stringify(val, null, this.formatted ? 2 : undefined))
        .join(",\n");
      if (this.filePerCustomer) {
        content += "\n]";
      }
    }

    fs.writeFileSync(filename, content, {
      encoding: "utf-8",
      flag: appending ? "a" : "w",
    });

    if (rows.length > 0) {
      this.logger.info(
        (appending ? "Updated " : "Created ") +
          filename +
          ` with ${rows.length} rows`,
        { customerId: customerId, scriptName: filename }
      );
    }

    this.appending = true;
    this.rowsByCustomer[customerId] = [];
  }

  endScript() {
    if (
      this.format === JsonOutputFormat.json && this.appending &&
      !this.filePerCustomer
    ) {
      let filename = this._getFileName("");
      fs.writeFileSync(filename, "\n]", {
        encoding: "utf-8",
        flag: "a",
      });
    }
    this.appending = false;
    this.scriptName = undefined;
  }
}

export class CsvWriter extends FileWriterBase {
  quoted: boolean;
  arraySeparator: string;
  fileExtension: string;

  constructor(options?: CsvWriterOptions) {
    super(options);
    this.quoted = !!options?.quoted;
    this.arraySeparator = options?.arraySeparator || "|";
    this.fileExtension = "csv";
  }

  endCustomer(customerId: string) {
    let rows = this.rowsByCustomer[customerId];
    if (!rows.length) {
      return;
    }
    let appending = this.appending && !this.filePerCustomer;
    let filename = this._getFileName(customerId);

    let csvOptions: csvStringify.Options = {
      header: !appending,
      quoted: this.quoted,
      columns: this.query!.columns.map((col) => col.name),
      cast: {
        boolean: (value: boolean, context: csvStringify.CastingContext) =>
          value ? "true" : "false",
        object: (value: object, context: csvStringify.CastingContext) =>
          Array.isArray(value)
            ? value.join(this.arraySeparator)
            : JSON.stringify(value),
      },
    };
    let csvText = stringify(rows, csvOptions);
    fs.writeFileSync(filename, csvText, {
      encoding: "utf-8",
      flag: appending ? "a" : "w",
    });

    if (rows.length > 0) {
      this.logger.info(
        (appending ? "Updated " : "Created ") +
          filename +
          ` with ${rows.length} rows`,
        { customerId: customerId, scriptName: filename }
      );
    }
    this.appending = true;
    this.rowsByCustomer[customerId] = [];
  }
}

export class NullWriter implements IResultWriter {
  beginScript(scriptName: string, query: QueryElements): void|Promise<void> {}
  beginCustomer(customerId: string): void|Promise<void> {}
  addRow(customerId: string, parsedRow: any[], rawRow: any[]): void {}
  endCustomer(customerId: string): void|Promise<void> {}
  endScript(): void|Promise<void> {}
}
