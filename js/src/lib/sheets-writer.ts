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

import {GoogleAuth} from 'google-auth-library';
import axios from 'axios';
import {IResultWriter, QueryElements} from './types.js';
import {getLogger} from './logger.js';

export interface SheetsWriterOptions {
  spreadsheetId?: string;
  sheetName?: string;
  includeHeaders?: boolean;
  arraySeparator?: string;
}

export class SheetsWriter implements IResultWriter {
  private spreadsheetId?: string;
  private sheetName?: string;
  private currentSheetName = '';
  private includeHeaders: boolean;
  private arraySeparator: string;
  private data: unknown[][] = [];
  private headers: string[] = [];
  private authClient: GoogleAuth;
  private logger = getLogger();

  constructor(options?: SheetsWriterOptions) {
    this.spreadsheetId = options?.spreadsheetId;
    this.sheetName = options?.sheetName;
    this.includeHeaders = options?.includeHeaders !== false;
    this.arraySeparator = options?.arraySeparator || '\n';
    this.authClient = new GoogleAuth({
      scopes: 'https://www.googleapis.com/auth/spreadsheets',
    });
  }

  beginScript(scriptName: string, query: QueryElements): void {
    this.currentSheetName = this.sheetName || scriptName;
    this.headers = query.columnNames;
    this.data = [];
    if (this.includeHeaders) {
      this.data.push(this.headers);
    }
  }

  beginCustomer(customerId: string): void {
    // No-op.
  }

  addRow(
    customerId: string,
    parsedRow: unknown[],
    rawRow: Record<string, unknown>,
  ): void {
    const formattedRow = parsedRow.map(val => this.formatValue(val));
    this.data.push(formattedRow);
  }

  private formatValue(val: unknown): unknown {
    if (val === null || val === undefined) {
      return val;
    }
    if (Array.isArray(val)) {
      return val.map(v => this.formatValue(v)).join(this.arraySeparator);
    }
    if (typeof val === 'object') {
      if (typeof (val as any).toJSON === 'function') {
        return (val as any).toJSON();
      }
      if (
        typeof (val as any).toString === 'function' &&
        (val as any).toString !== Object.prototype.toString
      ) {
        return (val as any).toString();
      }
      return JSON.stringify(val);
    }
    return val;
  }

  endCustomer(customerId: string): void {
    // No-op.
  }

  async endScript(): Promise<void> {
    if (this.data.length === 0) {
      return;
    }

    const token = await this.authClient.getAccessToken();
    const projectId = await this.authClient.getProjectId();

    const headers: Record<string, string> = {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json',
    };

    if (projectId) {
      headers['x-goog-user-project'] = projectId;
    }

    if (!this.spreadsheetId) {
      this.logger.info(
        'spreadsheetId is not provided. Creating a new spreadsheet...',
      );
      const createSpreadsheetUrl =
        'https://sheets.googleapis.com/v4/spreadsheets';
      const payload = {
        properties: {
          title: this.currentSheetName || 'Gaarf Report',
        },
        sheets: [
          {
            properties: {
              title: this.currentSheetName || 'Sheet1',
            },
          },
        ],
      };
      try {
        const response = await axios.post(createSpreadsheetUrl, payload, {
          headers,
        });
        this.spreadsheetId = response.data.spreadsheetId;
        this.logger.info(
          `Created new spreadsheet with ID: ${this.spreadsheetId}`,
        );
        console.log(
          `Created new spreadsheet: https://docs.google.com/spreadsheets/d/${this.spreadsheetId}/edit`,
        );
      } catch (error) {
        this.logger.error(`Failed to create spreadsheet: ${error}`);
        throw error;
      }
    }

    this.logger.debug(
      `Writing ${this.data.length} rows to sheet ${this.currentSheetName} in spreadsheet ${this.spreadsheetId}`,
    );

    // Check if sheet exists and create it if not
    try {
      const metadataUrl = `https://sheets.googleapis.com/v4/spreadsheets/${this.spreadsheetId}?fields=sheets.properties.title`;
      const metadataResponse = await axios.get(metadataUrl, {headers});
      const sheets = metadataResponse.data.sheets || [];
      const sheetExists = sheets.some(
        (s: any) => s.properties.title === this.currentSheetName,
      );

      if (!sheetExists) {
        this.logger.debug(
          `Sheet ${this.currentSheetName} does not exist. Creating it.`,
        );
        const createUrl = `https://sheets.googleapis.com/v4/spreadsheets/${this.spreadsheetId}:batchUpdate`;
        const createPayload = {
          requests: [
            {
              addSheet: {
                properties: {
                  title: this.currentSheetName,
                },
              },
            },
          ],
        };
        await axios.post(createUrl, createPayload, {headers});
        this.logger.debug(`Sheet ${this.currentSheetName} created.`);
      }
    } catch (error) {
      this.logger.error(`Failed to check/create sheet: ${error}`);
      throw error;
    }

    const range = `${this.currentSheetName}!A1`;
    const url = `https://sheets.googleapis.com/v4/spreadsheets/${this.spreadsheetId}/values/${encodeURIComponent(range)}:append?valueInputOption=USER_ENTERED`;

    const payload = {
      values: this.data,
    };

    try {
      const response = await axios.post(url, payload, {headers});

      this.logger.debug(
        `Successfully wrote to sheet: ${JSON.stringify(response.data)}`,
      );
    } catch (error) {
      if (axios.isAxiosError(error)) {
        this.logger.error(
          `Failed to write to sheet: ${error.response?.status}, ${JSON.stringify(error.response?.data)}`,
        );
        throw new Error(
          `Failed to write to sheet: ${error.response?.status} ${error.message}`,
        );
      }
      throw error;
    }
  }
}
