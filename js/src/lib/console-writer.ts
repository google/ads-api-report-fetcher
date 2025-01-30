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

import {
  ColumnUserConfig,
  getBorderCharacters,
  table,
  TableUserConfig,
} from 'table';
import {isNumber, isBoolean, isString, isArray, max} from 'lodash-es';

import {IResultWriter, QueryElements} from './types.js';

export interface ConsoleWriterOptions {
  transpose?: string;
  pageSize?: number;
}

export enum TransposeModes {
  auto = 'auto',
  never = 'never',
  always = 'always',
}

export class ConsoleWriter implements IResultWriter {
  static DEFAULT_MAX_ROWS = 1000;
  scriptName: string | undefined;
  query: QueryElements | undefined;
  transpose: TransposeModes;
  rowsByCustomer: Record<string, unknown[][]> = {};
  pageSize: number;
  hasMoreRows: boolean;

  constructor(options?: ConsoleWriterOptions) {
    options = options || {};
    this.transpose =
      TransposeModes[
        (options.transpose as keyof typeof TransposeModes) || 'auto'
      ];
    this.pageSize = options.pageSize || ConsoleWriter.DEFAULT_MAX_ROWS;
    this.hasMoreRows = false;
  }

  beginScript(scriptName: string, query: QueryElements): void | Promise<void> {
    this.scriptName = scriptName;
    this.query = query;
  }

  endScript(): void | Promise<void> {
    this.query = undefined;
  }

  beginCustomer(customerId: string): void | Promise<void> {
    this.rowsByCustomer[customerId] = [];
  }

  addRow(
    customerId: string,
    parsedRow: unknown[],
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    rawRow: Record<string, unknown>
  ): void {
    if (
      this.pageSize > 0 &&
      this.rowsByCustomer[customerId].length >= this.pageSize
    ) {
      this.hasMoreRows = true;
      return;
    }
    this.rowsByCustomer[customerId].push(parsedRow);
  }

  _formatValue(val: unknown): unknown {
    if (!val) return val;
    if (isNumber(val) || isString(val) || isBoolean(val)) return val;
    if (isArray(val)) {
      return val.map((v: unknown) => this._formatValue(v)).join('\n');
    }
    return JSON.stringify(val, null, 2);
  }

  endCustomer(customerId: string): void | Promise<void> {
    const cc: ColumnUserConfig = {
      wrapWord: true,
      alignment: 'right',
      truncate: 200,
    };
    let rows = this.rowsByCustomer[customerId];

    if (!rows || !rows.length) {
      this.rowsByCustomer[customerId] = [];
      this.hasMoreRows = false;
      return;
    }
    console.log(
      `${this.scriptName} (${customerId}), ${this.hasMoreRows ? 'first ' : ''}${rows.length} rows`
    );

    rows = rows.map(row => {
      return row.map(val => {
        if (val === undefined) return '';
        if (
          isArray(val) &&
          val.length > 0 &&
          max(val.map(v => (v ? v.length : 0))) > 20
        ) {
          return val.map(i => (i ? this._formatValue(i) + '\n' : '')).join('');
        }
        return this._formatValue(val);
      });
    });
    // original table plus a row (first) with headers (columns names)
    const data = [this.query!.columnNames as unknown[]].concat(rows);
    // transpose table (rows become columns)
    const data_trans = data[0].map((_, colIndex) =>
      data.map(row => row[colIndex])
    );
    // and a row with indexes
    data_trans.splice(0, 0, [
      'index',
      ...[...Array(rows.length).keys()].map(i => (++i).toString()),
    ]);
    const tableConfig: TableUserConfig = {
      border: getBorderCharacters('norc'),
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
      columns: this.query!.columnNames.map(_ => cc),
      // singleLine: true
    };
    const data_formatted_orig = table(data, tableConfig);
    const data_formatted_trans = table(data_trans, tableConfig);
    let use_trans = this.transpose === TransposeModes.always;
    let data_formatted = '';
    if (process.stdout.columns && this.transpose !== TransposeModes.never) {
      // we're in Terminal (not streaming to a file)
      if (!use_trans) {
        const first_line = data_formatted_orig.slice(
          0,
          data_formatted_orig.indexOf('\n')
        );
        if (first_line.length > process.stdout.columns) {
          // table isn't fitting into terminal window, transpose it
          use_trans = true;
        }
      }
      if (use_trans) {
        const first_line_trans = data_formatted_trans.slice(
          0,
          data_formatted_trans.indexOf('\n')
        );
        if (first_line_trans.length > process.stdout.columns) {
          // transposed table also isn't fitting, split it onto several tables
          data_formatted = this.processTransposedTable(
            data_trans,
            this.query!.columnNames
          );
        }
      }
    }
    if (!data_formatted) {
      data_formatted = use_trans ? data_formatted_trans : data_formatted_orig;
    }

    console.log(data_formatted);
    this.rowsByCustomer[customerId] = [];
    this.hasMoreRows = false;
  }

  processTransposedTable(data_trans: unknown[][], headers: string[]) {
    const tableConfig: TableUserConfig = {
      border: getBorderCharacters('norc'),
      columnDefault: {
        paddingLeft: 0,
        paddingRight: 1,
        truncate: 200,
        wrapWord: true,
      },
      drawVerticalLine: () => true,
      drawHorizontalLine: () => false,
      columns: this.query!.columnNames.map(_ => {
        return {
          wrapWord: true,
          alignment: 'left',
          truncate: 200,
        };
      }),
      // singleLine: true
    };

    let output = '';
    let part = 1;
    let done = false;
    while (!done) {
      const first_line = data_trans[0];
      const column_count = first_line.length;
      const row_count = data_trans.length;
      // note: we're starting from 1 because there's always a header columns coming first
      if (column_count <= 2) {
        // if we have only 2 columns (headers+data) there's no way to shrink the matrix
        done = true;
      } else {
        for (let i = 2; i < column_count; i++) {
          // slice matrix up to i-th column (included)
          let submatrix = data_trans
            .slice(0, row_count + 1)
            .map(row => row.slice(0, i + 1));
          let submatrix_formatted = table(submatrix, tableConfig);
          const first_line = submatrix_formatted.slice(
            0,
            submatrix_formatted.indexOf('\n')
          );
          if (first_line.length >= process.stdout.columns) {
            // currently accumulated matrix has come too long horizontally,
            // we have to break at this column - i.e. dump sub-matrix from 0 to previous, (i - 1)th column
            submatrix = data_trans
              .slice(0, row_count + 1)
              .map(row => row.slice(0, i));
            submatrix_formatted = table(submatrix, tableConfig);
            if (output) output += '\n';
            output = output + '#' + part + '\n' + submatrix_formatted;
            part++;
            // now remove the columns that have been dumped,
            data_trans = data_trans
              .slice(0, row_count + 1)
              .map(row => row.slice(i));
            // append headers at matrix first column (for each row)
            data_trans[0].splice(0, 0, 'index');
            for (let j = 0; j < headers.length; j++) {
              data_trans[j + 1].splice(0, 0, headers[j]);
            }
            break;
          } else if (i === column_count - 1) {
            // it's the last column, and the matrix being dumped fitted into the window
            done = true;
          }
        }
      }

      if (done || column_count <= 2) {
        if (part > 1) {
          output = output + '\n#' + part;
        }
        output = output + '\n' + table(data_trans, tableConfig);
      }
    }
    return output;
  }
}
