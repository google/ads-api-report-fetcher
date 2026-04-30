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
/* eslint-disable @typescript-eslint/no-explicit-any */

import assert from 'assert';
import axios from 'axios';
import {GoogleAuth} from 'google-auth-library';
import {SheetsWriter} from '../lib/sheets-writer.js';

suite('SheetsWriter', () => {
  let originalPost: any;
  let originalGet: any;
  let originalGetAccessToken: any;
  let originalGetProjectId: any;

  let postCalls: {url: string; data?: any; config?: any}[] = [];
  let getCalls: {url: string; config?: any}[] = [];

  setup(() => {
    originalPost = axios.post;
    originalGet = axios.get;
    originalGetAccessToken = GoogleAuth.prototype.getAccessToken;
    originalGetProjectId = GoogleAuth.prototype.getProjectId;

    postCalls = [];
    getCalls = [];

    // Mock axios.post
    (axios as any).post = async (url: string, data?: any, config?: any) => {
      postCalls.push({url, data, config});
      if (url === 'https://sheets.googleapis.com/v4/spreadsheets') {
        return {data: {spreadsheetId: 'new-spreadsheet-123'}};
      }
      if (url.includes(':batchUpdate')) {
        return {data: {}};
      }
      if (url.includes(':append')) {
        return {data: {}};
      }
      throw new Error(`Unexpected POST url: ${url}`);
    };

    // Mock axios.get
    (axios as any).get = async (url: string, config?: any) => {
      getCalls.push({url, config});
      if (url.includes('new-spreadsheet-123')) {
        // Mock metadata response for sheet existence check
        return {data: {sheets: [{properties: {title: 'test-query'}}]}};
      }
      throw new Error(`Unexpected GET url: ${url}`);
    };

    // Mock GoogleAuth
    GoogleAuth.prototype.getAccessToken = async () => 'mock-token';
    GoogleAuth.prototype.getProjectId = async () => 'mock-project-id';
  });

  teardown(() => {
    axios.post = originalPost;
    axios.get = originalGet;
    GoogleAuth.prototype.getAccessToken = originalGetAccessToken;
    GoogleAuth.prototype.getProjectId = originalGetProjectId;
  });

  test('spreadsheet creation on first endScript', async () => {
    const writer = new SheetsWriter({
      sheetName: 'test-query',
    });

    writer.beginScript('test-query', {
      queryText: 'SELECT campaign.id FROM campaign',
      columnNames: ['campaign.id'],
    } as any);

    writer.addRow('1234567890', [123], {});

    await writer.endScript();

    // Verify spreadsheet was created
    const createCall = postCalls.find(
      c => c.url === 'https://sheets.googleapis.com/v4/spreadsheets',
    );
    assert(createCall);
    assert.deepStrictEqual(createCall.data, {
      properties: {
        title: 'test-query',
      },
      sheets: [
        {
          properties: {
            title: 'test-query',
          },
        },
      ],
    });

    // Verify data was written to the created spreadsheet
    const appendCall = postCalls.find(c => c.url.includes(':append'));
    assert(appendCall);
    assert(appendCall.url.includes('new-spreadsheet-123'));
    assert.deepStrictEqual(appendCall.data, {
      values: [['campaign.id'], [123]],
    });
  });

  test('handles objects and arrays in rows', async () => {
    const writer = new SheetsWriter({
      sheetName: 'test-query',
    });

    writer.beginScript('test-query', {
      queryText:
        'SELECT campaign.id, campaign.policy_summary, campaign.labels FROM campaign',
      columnNames: [
        'campaign.id',
        'campaign.policy_summary',
        'campaign.labels',
      ],
    } as any);

    writer.addRow(
      '1234567890',
      [
        123,
        {reviewStatus: 'REVIEWED', approvalStatus: 'APPROVED'},
        ['value1', 'value2'],
      ],
      {},
    );

    await writer.endScript();

    // Verify data was written to the created spreadsheet with correctly formatted values
    const appendCall = postCalls.find(c => c.url.includes(':append'));
    assert(appendCall);
    assert.deepStrictEqual(appendCall.data, {
      values: [
        ['campaign.id', 'campaign.policy_summary', 'campaign.labels'],
        [
          123,
          '{"reviewStatus":"REVIEWED","approvalStatus":"APPROVED"}',
          'value1\nvalue2',
        ],
      ],
    });
  });

  test('subsequent endScript calls reuse the created spreadsheet', async () => {
    const writer = new SheetsWriter({}); // no spreadsheet ID, no sheet name

    // First query
    writer.beginScript('query-one', {
      queryText: 'SELECT campaign.id FROM campaign',
      columnNames: ['campaign.id'],
    } as any);
    writer.addRow('123', [123], {});
    await writer.endScript();

    // Verify creation call happened
    const createCall = postCalls.find(
      c => c.url === 'https://sheets.googleapis.com/v4/spreadsheets',
    );
    assert(createCall);
    assert.equal(createCall.data.properties.title, 'query-one');

    // Reset postCalls to track second query easily
    postCalls = [];
    getCalls = [];

    // Mock get metadata to say 'query-two' doesn't exist yet in 'new-spreadsheet-123'
    (axios as any).get = async (url: string, config?: any) => {
      getCalls.push({url, config});
      if (url.includes('new-spreadsheet-123')) {
        return {data: {sheets: [{properties: {title: 'query-one'}}]}};
      }
      throw new Error(`Unexpected GET url: ${url}`);
    };

    // Second query
    writer.beginScript('query-two', {
      queryText: 'SELECT campaign.name FROM campaign',
      columnNames: ['campaign.name'],
    } as any);
    writer.addRow('123', ['camp-name'], {});
    await writer.endScript();

    // Verify NO new spreadsheet creation call happened
    const createCall2 = postCalls.find(
      c => c.url === 'https://sheets.googleapis.com/v4/spreadsheets',
    );
    assert(!createCall2);

    // Verify batchUpdate was called to create the new sheet 'query-two'
    const batchUpdateCall = postCalls.find(c => c.url.includes(':batchUpdate'));
    assert(batchUpdateCall);
    assert(batchUpdateCall.url.includes('new-spreadsheet-123'));
    assert.deepStrictEqual(batchUpdateCall.data, {
      requests: [
        {
          addSheet: {
            properties: {
              title: 'query-two',
            },
          },
        },
      ],
    });

    // Verify append was called on the same spreadsheet for query-two
    const appendCall2 = postCalls.find(c => c.url.includes(':append'));
    assert(appendCall2);
    assert(appendCall2.url.includes('new-spreadsheet-123'));
  });
});
