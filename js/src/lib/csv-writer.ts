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

import csvStringify from 'csv-stringify';
import {stringify} from 'csv-stringify/sync';
import fs from 'fs';
import path from 'path';

import logger from './logger';
import {IResultWriter, QueryElements, QueryResult} from './types';

export interface CsvWriterOptions {
  destinationFolder?: string|undefined;
}
export class CsvWriter implements IResultWriter {
  destination: string|undefined;
  filename: string|undefined;
  appending = false;
  customerRows = 0;
  rowsByCustomer: Record<string, any[][]> = {};
  query: QueryElements|undefined;

  constructor(options?: CsvWriterOptions) {
    this.destination = options?.destinationFolder;
  }

  beginScript(scriptName: string, query: QueryElements) {
    this.appending = false;
    this.query = query;

    let filename = scriptName + '.csv';
    if (this.destination) {
      if (!fs.existsSync(this.destination)) {
        fs.mkdirSync(this.destination, {recursive: true});
      }
      filename = path.join(this.destination, filename);
    }
    this.filename = filename;
    if (fs.existsSync(this.filename)) {
      fs.rmSync(this.filename);
    }
  }

  endScript() {
    this.filename = undefined;
  }

  beginCustomer(customerId: string) {
    this.rowsByCustomer[customerId] = [];
  }

  addRow(customerId: string, parsedRow: any[], rawRow: any[]) {
    if (!parsedRow || parsedRow.length == 0) return;
    this.rowsByCustomer[customerId].push(parsedRow);
  }

  endCustomer(customerId: string) {
    let rows = this.rowsByCustomer[customerId];
    if (!rows.length) {
      return;
    }
    let csvOptions: csvStringify.Options = {
      header: !this.appending,
      quoted: false,
      columns: this.query!.columnNames,
      cast: {
        boolean: (value: boolean, context: csvStringify.CastingContext) =>
            value ? 'true' : 'false'
      }
    };
    let csv = stringify(rows, csvOptions);
    fs.writeFileSync(
        this.filename!, csv,
        {encoding: 'utf-8', flag: this.appending ? 'a' : 'w'});

    if (rows.length > 0) {
      logger.info(
          (this.appending ? 'Updated ' : 'Created ') + this.filename +
              ` with ${rows.length} rows`,
          {customerId: customerId, scriptName: this.filename});
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
