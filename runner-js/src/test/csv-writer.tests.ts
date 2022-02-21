import assert from 'assert';
import {parse} from 'csv-parse/sync';
import fs from 'fs';
import path from 'path';

import {AdsQueryExecutor} from '../lib/ads-query-executor';
import {CsvWriter} from '../lib/csv-writer';

import {MockGoogleAdsApiClient} from './helpers';

suite('CsvWriter', () => {
  test('writing', async function() {
    // arrange
    let mock_result = [{
      campaign: {
        id: 1767375787,
        resource_name: 'customers/9489090398/campaigns/1767375787'
      },
      ad_group_ad: {
        ad: {
          id: 563386468726,
          final_urls: ['url1', 'url2'],
          type: 7,
          resource_name: 'customers/9489090398/ads/563386468726'
        },
        ad_group: 'customers/9489090398/adGroups/132594495320',
        policy_summary: {
          policy_topic_entries: [{
            evidences: [],
            constraints: [{}],
            topic: 'COPYRIGHTED_CONTENT',
            type: 8
          }]
        },
        resource_name:
            'customers/9489090398/adGroupAds/132594495320~563386468726'
      }
    }];
    let queryText = `
      SELECT
        ad_group_ad.ad.id AS ad_id,  --number
        ad_group_ad.ad.final_urls AS final_urls,  -- array<string>
        ad_group_ad.ad.type AS ad_type,  -- enum
        ad_group_ad.ad_group AS ad_group,  -- resource_name
        ad_group_ad.policy_summary.policy_topic_entries AS policy_topic_entries  -- array
      FROM ad_group_ad
    `;
    let customers = ['cust_with_no_data', 'cust_with_data'];
    let client = new MockGoogleAdsApiClient(customers);
    client.setupResult({'cust_with_data': mock_result});
    const OUTPUT_DIR = '.tmp'
    let writer = new CsvWriter({destinationFolder: OUTPUT_DIR});
    let executor = new AdsQueryExecutor(client);

    // act
    const SCRIPT_NAME = 'test';
    await executor.execute(SCRIPT_NAME, queryText, customers, {}, writer);

    // assert
    let csvText =
        fs.readFileSync(path.join(OUTPUT_DIR, SCRIPT_NAME + '.csv'), 'utf-8');
    let csvData = parse(csvText, {columns: true, skipEmptyLines: true});
    console.log(csvData);
    assert(csvData);
    assert.equal(csvData.length, 1);
    let row = csvData[0];
    assert.equal(row.ad_id, mock_result[0].ad_group_ad.ad.id);
    assert.deepStrictEqual(
        row.final_urls,
        JSON.stringify(mock_result[0].ad_group_ad.ad.final_urls));
  });
})
