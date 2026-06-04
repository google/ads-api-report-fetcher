/**
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * Copyright 2026 Google LLC
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

import {IResultWriter, QueryElements} from '../../../src/lib/types';

export class SheetsWriterAppsScript implements IResultWriter {
  private sheetName: string = '';
  private data: unknown[][] = [];
  private headers: string[] = [];
  private includeHeaders: boolean;
  private batchSize: number;
  private currentRow: number = 1;
  private sheet: GoogleAppsScript.Spreadsheet.Sheet | null = null;

  constructor(includeHeaders = true, batchSize = 100) {
    this.includeHeaders = includeHeaders;
    this.batchSize = batchSize;
  }

  beginScript(scriptName: string, query: QueryElements): void {
    this.sheetName = scriptName;
    this.headers = query.columnNames;
    this.data = [];
    this.currentRow = 1;
    if (this.includeHeaders) {
      this.data.push(this.headers);
    }

    const ss = SpreadsheetApp.getActiveSpreadsheet();
    this.sheet = ss.getSheetByName(this.sheetName);

    if (!this.sheet) {
      this.sheet = ss.insertSheet(this.sheetName);
    } else {
      this.sheet.clear();
    }
  }

  beginCustomer(customerId: string): void {
    // No-op for now. We could add a customer_id column if needed.
  }

  addRow(
    customerId: string,
    parsedRow: unknown[],
    rawRow: Record<string, unknown>,
  ): void {
    this.data.push(parsedRow);
    if (this.data.length >= this.batchSize) {
      this.flush();
    }
  }

  private flush(): void {
    if (this.data.length === 0) {
      return;
    }
    console.log(`SheetsWriterAppsScript: flushing ${this.data.length} rows`);
    if (!this.sheet) {
      throw new Error(`Sheet ${this.sheetName} not found during flush`);
    }
    const range = this.sheet.getRange(this.currentRow, 1, this.data.length, this.data[0].length);
    range.setValues(this.data);
    this.currentRow += this.data.length;
    this.data = [];
  }

  endCustomer(customerId: string): void {
    // No-op.
  }

  endScript(): void {
    console.log(
      `SheetsWriterAppsScript: endScript. Remaining rows ${this.data.length}`,
    );
    this.flush();
  }
}
