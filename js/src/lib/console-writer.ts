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

import {ColumnUserConfig, getBorderCharacters, table, TableUserConfig} from 'table'

import {IResultWriter, QueryElements} from './types';

export interface ConsoleWriterOptions {
  transpose?: string;
}
export enum TransposeModes {
  auto = 'auto', never = 'never', always = 'always'
}
export class ConsoleWriter implements IResultWriter {
  scriptName: string|undefined;
  query: QueryElements | undefined;
  transpose: TransposeModes;
  rowsByCustomer: Record<string, any[][]> = {};

  constructor(options?: ConsoleWriterOptions) {
    options = options || {};
    this.transpose = TransposeModes[options.transpose as keyof typeof TransposeModes || 'auto']
  }

  beginScript(scriptName: string, query: QueryElements): void | Promise<void> {
    this.scriptName = scriptName;
    this.query = query;
  }

  endScript(): void|Promise<void> {
    this.query = undefined;
  }

  beginCustomer(customerId: string): void|Promise<void> {
    this.rowsByCustomer[customerId] = [];
  }

  addRow(customerId: string, parsedRow: any[], rawRow: any[]): void {
    this.rowsByCustomer[customerId].push(parsedRow);
  }

  endCustomer(customerId: string): void|Promise<void> {
    let cc:
        ColumnUserConfig = {wrapWord: true, alignment: 'right', truncate: 200, };
    let rows = this.rowsByCustomer[customerId];

    console.log(this.scriptName);

    rows = rows.map(row => {
      return row.map(col => {
        if (col === undefined) return '';
        return col;
      })
    });
    // original table plus a row (first) with headers (columns names)
    let data = [this.query!.columnNames].concat(rows);
    // transpose table (rows become columns)
    let data_trans = data[0].map((_, colIndex) => data.map(row => row[colIndex]));
    // and a row with indexes
    data_trans.splice(
      0, 0, ['index', ...[...Array(rows.length).keys()].map(i => (++i).toString())]);
    let tableConfig: TableUserConfig = {
      border: getBorderCharacters('norc'),
      columnDefault:
          {paddingLeft: 0, paddingRight: 1, truncate: 200, wrapWord: true},
      drawVerticalLine: () => true,
      drawHorizontalLine: (lineIndex, rowCount) => {
        return lineIndex === 0 || lineIndex === 1 || lineIndex === rowCount;
      },
      columns: this.query!.columnNames.map(c => cc),
      // singleLine: true
    };
    let data_formatted_orig = table(data, tableConfig);
    let data_formatted_trans = table(data_trans, tableConfig);
    let data_formatted = this.transpose == TransposeModes.never ?
        data_formatted_orig :
        data_formatted_trans;
    if (process.stdout.columns && this.transpose != TransposeModes.never) {
      // we're in Terminal (not streaming to a file)
      let first_line =
          data_formatted_orig.slice(0, data_formatted_orig.indexOf('\n'));
      if (first_line.length > process.stdout.columns) {
        // table isn't fitting into terminal window, transpose it
        let first_line_trans =
          data_formatted_trans.slice(0, data_formatted_trans.indexOf('\n'));
        if (first_line_trans.length > process.stdout.columns) {
          // transposed table also isn't fitting, split it onto several tables
          data_formatted =
              this.processTransposedTable(data_trans, this.query!.columnNames)
        }
        else {
          data_formatted = data_formatted_trans;
        }
      }
    }

    console.log(data_formatted);
    //console.table(data);
    this.rowsByCustomer[customerId] = [];
  }

  processTransposedTable(data_trans: any[][], headers: string[]) {
    let tableConfig: TableUserConfig = {
      border: getBorderCharacters('norc'),
      columnDefault:
          {paddingLeft: 0, paddingRight: 1, truncate: 200, wrapWord: true},
      drawVerticalLine: () => true,
      drawHorizontalLine: () => false,
      columns: this.query!.columnNames.map(c => {
        return {
          wrapWord: true,
          alignment: 'right',
          truncate: 200,
        }
      }),
      // singleLine: true
    };

    let output = '';
    let part = 1;
    let done = false;
    while (!done) {
      let first_line = data_trans[0];
      let column_count = first_line.length;
      let row_count = data_trans.length;
      // note: we're starting from 1 because there's always a header columns coming first
      for (let i = 1; i < column_count; i++) {
        // slice matrix up to i-th column
        let submatrix = data_trans.slice(0, row_count + 1)
          .map(row => row.slice(0, i + 1));
        let submatrix_formatted = table(submatrix, tableConfig);
        let first_line =
          submatrix_formatted.slice(0, submatrix_formatted.indexOf('\n'));
        if (first_line.length > process.stdout.columns) {
          // we have to break at this column - dump sub-matrix from 0 to (i-1)th column
          submatrix = data_trans.slice(0, row_count + 1)
            .map(row => row.slice(0, i));
          submatrix_formatted = table(submatrix, tableConfig); if (output) output += '\n';
          output = output + '#' + part + '\n' + submatrix_formatted;
          part++;
          // now remove the columns that have been dumped,
          data_trans = data_trans.slice(0, row_count + 1)
            .map(row => row.slice(i, column_count + 1));
          // append headers at matrix first column (for each row)
          data_trans[0].splice(0, 0, 'index');
          for (let j = 1; j < data_trans.length; j++) {
            data_trans[j].splice(0, 0, headers[j])
          }
          break;
        }
        else if (i === column_count - 1) {
          // it's the last column, and the matrix being dumped fitted into the window
          done = true;
        }
      }

      if (done || column_count <= 2) {
        if (part > 1) {
          output = output  + '\n#' + part;
        }
        output = output  + '\n' + table(data_trans, tableConfig);
      }
    }
    return output;
  }
}
