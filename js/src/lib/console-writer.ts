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

import {ColumnUserConfig, getBorderCharacters, table} from 'table'

import {IResultWriter, QueryElements} from './types';

export interface ConsoleWriterOptions {}

// TODO:
export class ConsoleWriter implements IResultWriter {
  query: QueryElements|undefined;
  rows: any[]|undefined;

  constructor(options?: ConsoleWriterOptions) {}
  beginScript(scriptName: string, query: QueryElements): void|Promise<void> {
    this.query = query;
  }
  endScript(customers: string[]): void|Promise<void> {
    this.query = undefined;
  }
  beginCustomer(customerId: string): void|Promise<void> {
    this.rows = [];
  }
  endCustomer(): void | Promise<void> {
    // TODO:
    let cc: ColumnUserConfig = {wrapWord: true, alignment: 'center'};

    let text = table(this.rows!, {
      border: getBorderCharacters('void'),
      columnDefault: {paddingLeft: 0, paddingRight: 1},
      drawHorizontalLine: () => false

      // border: getBorderCharacters('ramac'),
      // columns: this.query!.columnNames.map(c => cc),
      // singleLine: true
    });
    console.log(text);

    this.rows = [];
  }
  addRow(parsedRow: any[]): void {
    this.rows!.push(parsedRow);
  }
}
