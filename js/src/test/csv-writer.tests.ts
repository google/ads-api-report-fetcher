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
/* eslint-disable @typescript-eslint/no-explicit-any */

import assert from 'assert';
import {parse} from 'csv/sync';
import fs from 'fs';
import path from 'path';

import {AdsQueryExecutor} from '../lib/ads-query-executor.js';
import {CsvWriter} from '../lib/file-writers.js';
import {MockGoogleAdsApiClient} from './helpers.js';

suite('CsvWriter', () => {
  const OUTPUT_DIR = '.tmp';
  const SCRIPT_NAME = 'test';

  function assertCsvEqual(customerId: string, mockResults: any[]) {
    const csvText = fs.readFileSync(
      path.join(
        OUTPUT_DIR,
        customerId
          ? SCRIPT_NAME + '_' + customerId + '.csv'
          : SCRIPT_NAME + '.csv'
      ),
      'utf-8'
    );
    const csvData = parse(csvText, {columns: true, skipEmptyLines: true});
    assert.equal(csvData.length, mockResults.length);
    for (let i = 0; i < mockResults.length; i++) {
      const expected = mockResults[i];
      const actual = csvData[i];
      console.log('Serialized CSV: ');
      console.log(actual);
      assert.deepStrictEqual(Object.keys(actual), Object.keys(expected));
      const keys = Object.keys(expected);
      for (let j = 0; j < keys.length; j++) {
        assert.deepEqual(actual[keys[j]], expected[keys[j]]);
      }
    }
  }

  test('file handling', async () => {
    // arrange
    const mock_result = {
      customer1: [
        {
          campaign: {
            id: 123,
            resource_name: 'customers/customer1/campaigns/123',
          },
        },
        {
          campaign: {
            id: 125,
            resource_name: 'customers/customer1/campaigns/125',
          },
        },
      ],
      customer2: [
        {
          campaign: {
            id: 124,
            resource_name: 'customers/customer2/campaigns/124',
          },
        },
        {
          campaign: {
            id: 126,
            resource_name: 'customers/customer2/campaigns/126',
          },
        },
      ],
    };
    const customers = ['customer1', 'customer2'];
    const client = new MockGoogleAdsApiClient();
    client.setupResult(mock_result);
    const executor = new AdsQueryExecutor(client);
    const queryText = `
      SELECT
        campaign.id,
        campaign.resource_name
      FROM campaign
    `;

    // act: file per customer
    let writer = new CsvWriter({
      outputPath: OUTPUT_DIR,
      filePerCustomer: true,
    });
    await executor.execute(SCRIPT_NAME, queryText, customers, {}, writer);

    // assert
    assertCsvEqual(
      customers[0],
      mock_result['customer1'].map(o => {
        return {id: o.campaign.id, resource_name: o.campaign.resource_name};
      })
    );
    assertCsvEqual(
      customers[1],
      mock_result['customer2'].map(o => {
        return {id: o.campaign.id, resource_name: o.campaign.resource_name};
      })
    );

    // act #2: all customers in the same file
    writer = new CsvWriter({
      outputPath: OUTPUT_DIR,
      filePerCustomer: false,
    });
    await executor.execute(SCRIPT_NAME, queryText, customers, {}, writer);

    // assert
    assertCsvEqual(
      '',
      Object.values(mock_result)
        .flat()
        .map(o => {
          return {id: o.campaign.id, resource_name: o.campaign.resource_name};
        })
        .sort((a, b) => {
          return a.id > b.id ? 1 : -1;
        })
    );
  });

  test('writing', async () => {
    // arrange
    const mock_result = [
      {
        campaign: {
          id: 1767375787,
          resource_name: 'customers/9489090398/campaigns/1767375787',
        },
        ad_group_ad: {
          ad: {
            id: 563386468726,
            final_urls: ['url1', 'url2'],
            type: 7,
            resource_name: 'customers/9489090398/ads/563386468726',
          },
          ad_group: 'customers/9489090398/adGroups/132594495320',
          policy_summary: {
            policy_topic_entries: [
              {
                evidences: [],
                constraints: [{}],
                topic: 'COPYRIGHTED_CONTENT',
                type: 8,
              },
            ],
          },
          resource_name:
            'customers/9489090398/adGroupAds/132594495320~563386468726',
        },
      },
    ];
    const queryText = `
      SELECT
        ad_group_ad.ad.id AS ad_id,  --number
        ad_group_ad.ad.final_urls AS final_urls,  -- array<string>
        ad_group_ad.ad.type AS ad_type,  -- enum
        ad_group_ad.ad_group AS ad_group,  -- resource_name
        ad_group_ad.policy_summary.policy_topic_entries AS policy_topic_entries  -- array
      FROM ad_group_ad
    `;
    const customers = ['cust_with_no_data', 'cust_with_data'];
    const client = new MockGoogleAdsApiClient();
    client.setupResult({cust_with_data: mock_result});

    const writer = new CsvWriter({
      outputPath: OUTPUT_DIR,
    });
    const executor = new AdsQueryExecutor(client);

    // act
    const SCRIPT_NAME = 'test';
    await executor.execute(SCRIPT_NAME, queryText, customers, {}, writer);

    // assert
    const csvText = fs.readFileSync(
      path.join(OUTPUT_DIR, SCRIPT_NAME + '.csv'),
      'utf-8'
    );
    const csvData = parse(csvText, {columns: true, skipEmptyLines: true});
    console.log(csvData);
    assert(csvData);
    assert.equal(csvData.length, 1);
    const row = csvData[0];
    assert.equal(row.ad_id, mock_result[0].ad_group_ad.ad.id);
    assert.deepStrictEqual(
      row.final_urls,
      mock_result[0].ad_group_ad.ad.final_urls.join('|')
    );
  });
});
