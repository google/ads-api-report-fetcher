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

import assert from 'assert';

import {AdsQueryExecutor} from '../lib/ads-query-executor';
import {QueryResult} from '../lib/types';

import {MockGoogleAdsApiClient} from './helpers';


suite('AdsQueryExecutor', () => {
  test('enums', async function() {
    // arrange
    let mockResult = [{
      campaign: {id: 2, resource_name: 'customers/1/campaigns/2'},
      campaign_criterion: {
        ad_schedule: {day_of_week: 2},  // DayOfWeekEnum
        resource_name: 'customers/1/campaignCriteria/2~340096'
      }
    }];
    let queryText = `
      SELECT
        campaign.id AS campaign_id,
        campaign_criterion.ad_schedule.day_of_week AS ad_schedule_day_of_week
      FROM campaign_criterion
    `;
    let customerId = '1';
    let client = new MockGoogleAdsApiClient([customerId]);
    client.setupResult(mockResult);
    let executor = new AdsQueryExecutor(client);

    // act (using executeGen)
    let result =
        <QueryResult>(
            await executor.executeGen('test', queryText, [customerId]).next())
            .value;

    // assert
    assert(result.rows.length);
    assert.deepEqual(result.rows[0][1], 'MONDAY');
  });

  test('nested values', async function() {
    // arrange
    let mockResult = [{
      campaign: {
        id: 2,
        resource_name: 'customers/1/campaigns/2',
        frequency_caps: [
          {
            key: {
              level: 2,       // FrequencyCapLevel.AD_GROUP_AD
              event_type: 2,  // FrequencyCapEventType.IMPRESSION
              time_unit: 2,   // FrequencyCapTimeUnit.DAY
              time_length: 10
            },
            cap: 100
          },
          {
            key: {
              level: 3,       // FrequencyCapLevel.AD_GROUP
              event_type: 3,  // FrequencyCapEventType.VIDEO_VIEW
              time_unit: 3,   // FrequencyCapTimeUnit.WEEK
              time_length: 20
            },
            cap: 200
          },

        ]
      }
    }];
    let queryText = `
      SELECT
        campaign.frequency_caps:key.level AS frequency_caps_level,
      FROM campaign
    `;
    let customerId = '1';
    let client = new MockGoogleAdsApiClient([customerId]);
    client.setupResult(mockResult);
    let executor = new AdsQueryExecutor(client);

    // act (using executeOne)
    let query = executor.parseQuery(queryText);
    let result = await executor.executeOne(query, <string>customerId);

    // assert
    assert.deepEqual(result.rows[0][0], ['AD_GROUP_AD', 'AD_GROUP'])
  });

  test('functions', async function() {
    // arrange
    let mock_result = [{
      campaign: {id: 2, resource_name: 'customers/1/campaigns/2'},
      campaign_criterion: {
        ad_schedule: {day_of_week: 7},  // DayOfWeekEnum
        resource_name: 'customers/1/campaignCriteria/2~340096'
      }
    }];
    let queryText = `
      SELECT
        campaign.id AS campaign_id,
        campaign_criterion.ad_schedule.day_of_week:$format AS ad_schedule_day_of_week
      FROM campaign_criterion
      FUNCTIONS
      function format(val) {
        let days = ['пн', 'вт', 'ср', 'чт', 'пт', 'сб', 'вс'];
        if (!val) return '';
        return val === 8 ? days[6] : days[val-2];
      }
    `;
    let customerId = '1';
    let client = new MockGoogleAdsApiClient([customerId]);
    client.setupResult(mock_result);
    let executor = new AdsQueryExecutor(client);

    // act (using executeOne)
    let query = executor.parseQuery(queryText);
    let result = await executor.executeOne(query, <string>customerId);

    // assert
    assert(result.rows.length);
    assert.equal(result.rows[0][1], 'сб');
  });

  test('indices', async function() {
    const campaignId = 2;
    const resId = 853097294612;
    const resName = `customers/1/campaignAudienceViews/${campaignId}~${resId}`;
    let mockResult = [{campaign_audience_view: {resource_name: resName}}];
    let queryText = `
      SELECT
        campaign_audience_view.resource_name as res_name,
        campaign_audience_view.resource_name~0 AS res_base,
        campaign_audience_view.resource_name~1 AS res_id,
      FROM campaign_audience_view
    `;

    let customerId = '1';
    let client = new MockGoogleAdsApiClient([customerId]);
    client.setupResult(mockResult);
    let executor = new AdsQueryExecutor(client);

    // act (using executeGen with for-await)
    for await (const result of executor.executeGen(
        'campaign_audience_view', queryText, [customerId])) {
      // assert
      let row = result.rows[0];
      assert.deepEqual(
          result.query.columnNames, ['res_name', 'res_base', 'res_id']);
      assert.deepStrictEqual(row, [resName, campaignId, resId]);
      break;
    }
  })
});
