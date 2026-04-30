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
import { GoogleAuth } from 'google-auth-library';
import axios from 'axios';
import { getLogger } from './logger.js';
export class SheetsWriter {
    constructor(options) {
        this.currentSheetName = '';
        this.data = [];
        this.headers = [];
        this.logger = getLogger();
        this.spreadsheetId = options === null || options === void 0 ? void 0 : options.spreadsheetId;
        this.sheetName = options === null || options === void 0 ? void 0 : options.sheetName;
        this.includeHeaders = (options === null || options === void 0 ? void 0 : options.includeHeaders) !== false;
        this.arraySeparator = (options === null || options === void 0 ? void 0 : options.arraySeparator) || '\n';
        this.authClient = new GoogleAuth({
            scopes: 'https://www.googleapis.com/auth/spreadsheets',
        });
    }
    beginScript(scriptName, query) {
        this.currentSheetName = this.sheetName || scriptName;
        this.headers = query.columnNames;
        this.data = [];
        if (this.includeHeaders) {
            this.data.push(this.headers);
        }
    }
    beginCustomer(customerId) {
        // No-op.
    }
    addRow(customerId, parsedRow, rawRow) {
        const formattedRow = parsedRow.map(val => this.formatValue(val));
        this.data.push(formattedRow);
    }
    formatValue(val) {
        if (val === null || val === undefined) {
            return val;
        }
        if (Array.isArray(val)) {
            return val.map(v => this.formatValue(v)).join(this.arraySeparator);
        }
        if (typeof val === 'object') {
            if (typeof val.toJSON === 'function') {
                return val.toJSON();
            }
            if (typeof val.toString === 'function' &&
                val.toString !== Object.prototype.toString) {
                return val.toString();
            }
            return JSON.stringify(val);
        }
        return val;
    }
    endCustomer(customerId) {
        // No-op.
    }
    async endScript() {
        var _a, _b, _c;
        if (this.data.length === 0) {
            return;
        }
        const token = await this.authClient.getAccessToken();
        const projectId = await this.authClient.getProjectId();
        const headers = {
            Authorization: `Bearer ${token}`,
            'Content-Type': 'application/json',
        };
        if (projectId) {
            headers['x-goog-user-project'] = projectId;
        }
        if (!this.spreadsheetId) {
            this.logger.info('spreadsheetId is not provided. Creating a new spreadsheet...');
            const createSpreadsheetUrl = 'https://sheets.googleapis.com/v4/spreadsheets';
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
                this.logger.info(`Created new spreadsheet with ID: ${this.spreadsheetId}`);
                console.log(`Created new spreadsheet: https://docs.google.com/spreadsheets/d/${this.spreadsheetId}/edit`);
            }
            catch (error) {
                this.logger.error(`Failed to create spreadsheet: ${error}`);
                throw error;
            }
        }
        this.logger.debug(`Writing ${this.data.length} rows to sheet ${this.currentSheetName} in spreadsheet ${this.spreadsheetId}`);
        // Check if sheet exists and create it if not
        try {
            const metadataUrl = `https://sheets.googleapis.com/v4/spreadsheets/${this.spreadsheetId}?fields=sheets.properties.title`;
            const metadataResponse = await axios.get(metadataUrl, { headers });
            const sheets = metadataResponse.data.sheets || [];
            const sheetExists = sheets.some((s) => s.properties.title === this.currentSheetName);
            if (!sheetExists) {
                this.logger.debug(`Sheet ${this.currentSheetName} does not exist. Creating it.`);
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
                await axios.post(createUrl, createPayload, { headers });
                this.logger.debug(`Sheet ${this.currentSheetName} created.`);
            }
        }
        catch (error) {
            this.logger.error(`Failed to check/create sheet: ${error}`);
            throw error;
        }
        const range = `${this.currentSheetName}!A1`;
        const url = `https://sheets.googleapis.com/v4/spreadsheets/${this.spreadsheetId}/values/${encodeURIComponent(range)}:append?valueInputOption=USER_ENTERED`;
        const payload = {
            values: this.data,
        };
        try {
            const response = await axios.post(url, payload, { headers });
            this.logger.debug(`Successfully wrote to sheet: ${JSON.stringify(response.data)}`);
        }
        catch (error) {
            if (axios.isAxiosError(error)) {
                this.logger.error(`Failed to write to sheet: ${(_a = error.response) === null || _a === void 0 ? void 0 : _a.status}, ${JSON.stringify((_b = error.response) === null || _b === void 0 ? void 0 : _b.data)}`);
                throw new Error(`Failed to write to sheet: ${(_c = error.response) === null || _c === void 0 ? void 0 : _c.status} ${error.message}`);
            }
            throw error;
        }
    }
}
//# sourceMappingURL=sheets-writer.js.map