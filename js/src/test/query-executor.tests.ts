/**
 * Copyright 2023 Google LLC
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
import {LocalDate} from '@js-joda/core';
import {AdsQueryExecutor} from '../lib/ads-query-executor';
import {QueryResult} from '../lib/types';

import {MockGoogleAdsApiClient} from './helpers';

suite('AdsQueryExecutor', () => {
  test('enums', async () => {
    // arrange
    const mockResult = [
      {
        campaign: {id: 2, resource_name: 'customers/1/campaigns/2'},
        campaign_criterion: {
          ad_schedule: {day_of_week: 2}, // DayOfWeekEnum
        },
      },
    ];
    const queryText = `
      SELECT
        campaign.id AS campaign_id,
        campaign_criterion.ad_schedule.day_of_week AS ad_schedule_day_of_week
      FROM campaign_criterion
    `;
    const customerId = '1';
    const client = new MockGoogleAdsApiClient();
    client.setupResult(mockResult);
    const executor = new AdsQueryExecutor(client);

    // act (using executeGen)
    const result = <QueryResult>(
      (await executor.executeGen('test', queryText, [customerId]).next()).value
    );

    // assert
    assert(result.rows!.length);
    assert.deepEqual(result.rows![0][1], 'MONDAY');
  });

  test.skip('nested arrays', async () => {
    /* TODO: nested values for arrays
      SELECT
        audience.dimensions.audience_segments.segments.life_event.life_event
      FROM audience
    `dimensions` and `segments` are both arrays.
    We need to iterate over dimensions first, then over segments.
    How should a select expression look like?
    `audience.dimensions:audience_segments.segments:life_event.life_event` ?
    But why not to parse it automatically, without ":" selector?
    */

    // arrange
    const customerId = '1';
    const mockResult = [
      {
        audience: {
          dimensions: [
            {
              audience_segments: {
                segments: [
                  {
                    custom_audience: {
                      custom_audience: `customers/${customerId}/customAudiences/731504962`,
                    },
                  },
                  {
                    user_interest: {
                      user_interest_category: `customers/${customerId}/userInterests/80237`,
                    },
                  },
                ],
              },
            },
          ],
        },
      },
    ];
    const queryText = `
      SELECT
        audience.dimensions.audience_segments.segments.user_interest.user_interest_category,
        audience.dimensions.audience_segments.segments.custom_audience.custom_audience
      FROM audience`;
    const client = new MockGoogleAdsApiClient();
    client.setupResult(mockResult);
    const executor = new AdsQueryExecutor(client);

    // act (using executeOne)
    const query = executor.parseQuery(queryText);
    const result = await executor.executeOne(query, <string>customerId);

    // assert
    assert.deepEqual(result.rows![0][0], ['AD_GROUP_AD', 'AD_GROUP']);
  });

  test('nested values', async () => {
    // arrange
    const mockResult = [
      {
        campaign: {
          id: 2,
          resource_name: 'customers/1/campaigns/2',
          frequency_caps: [
            {
              key: {
                level: 2, // FrequencyCapLevel.AD_GROUP_AD
                event_type: 2, // FrequencyCapEventType.IMPRESSION
                time_unit: 2, // FrequencyCapTimeUnit.DAY
                time_length: 10,
              },
              cap: 100,
            },
            {
              key: {
                level: 3, // FrequencyCapLevel.AD_GROUP
                event_type: 3, // FrequencyCapEventType.VIDEO_VIEW
                time_unit: 3, // FrequencyCapTimeUnit.WEEK
                time_length: 20,
              },
              cap: 200,
            },
          ],
        },
      },
    ];
    const queryText = `
      SELECT
        campaign.frequency_caps:key.level AS frequency_caps_level,
      FROM campaign
    `;
    const customerId = '1';
    const client = new MockGoogleAdsApiClient();
    client.setupResult(mockResult);
    const executor = new AdsQueryExecutor(client);

    // act (using executeOne)
    const query = executor.parseQuery(queryText);
    const result = await executor.executeOne(query, <string>customerId);

    // assert
    assert.deepEqual(result.rows![0][0], ['AD_GROUP_AD', 'AD_GROUP']);
  });

  test('functions', async () => {
    // arrange
    const mock_result = [
      {
        campaign: {id: 2, resource_name: 'customers/1/campaigns/2'},
        campaign_criterion: {
          ad_schedule: {day_of_week: 7}, // DayOfWeekEnum
          resource_name: 'customers/1/campaignCriteria/2~340096',
        },
      },
    ];
    const queryText = `
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
    const customerId = '1';
    const client = new MockGoogleAdsApiClient();
    client.setupResult(mock_result);
    const executor = new AdsQueryExecutor(client);

    // act (using executeOne)
    const query = executor.parseQuery(queryText);
    const result = await executor.executeOne(query, <string>customerId);

    // assert
    assert(result.rows!.length);
    assert.equal(result.rows![0][1], 'сб');
  });

  test('resource indices', async () => {
    const customerId = '1';
    const campaignId = 2;
    const resId = 853097294612;
    const resName = `customers/${customerId}/campaignAudienceViews/${campaignId}~${resId}`;
    const mockResult = [{campaign_audience_view: {resource_name: resName}}];
    const queryText = `
      SELECT
        campaign_audience_view.resource_name as res_name,
        campaign_audience_view.resource_name~0 AS res_base,
        campaign_audience_view.resource_name~1 AS res_id,
      FROM campaign_audience_view
    `;

    const client = new MockGoogleAdsApiClient();
    client.setupResult(mockResult);
    const executor = new AdsQueryExecutor(client);

    // act (using executeOne)
    const query = executor.parseQuery(queryText);
    const res = await executor.executeOne(query, customerId);

    // assert
    assert.ok(res.rows);
    assert.deepStrictEqual(res.rows[0], [resName, campaignId, resId]);
  });

  test('virtual columns', async () => {
    const queryText = `
      SELECT
        '$\{today()}' as date, -- #1
        1 AS counter,           -- #2
        metrics.clicks / metrics.impressions AS ctr, -- #3
        metrics.cost_micros * 1000 AS cost,          -- #4
        campaign.target_cpa.target_cpa_micros / 1000000 AS target_cpa, -- #5
        campaign.app_campaign_setting.bidding_strategy_goal_type AS bidding_type
      FROM campaign
    `;
    // NOTE for the query:
    //  #1: virtual column with a constant string (value executed as expression)
    //  #2: virtual column with a constant number
    //  #3: virtual column with a ariphmetic operation against real columns
    //      (they will be fetched automatically)
    //  #4: virtual column with a ariphmetic operation between a column and const
    //  #5: virtual column with a ariphmetic operation between a column and const
    //      but the column is 2 level nested
    const mockResult = [
      {
        campaign: {
          app_campaign_setting: {
            bidding_strategy_goal_type: 2, // OPTIMIZE_INSTALLS_TARGET_INSTALL_COST
          },
          target_cpa: {
            target_cpa_micros: 1000000,
          },
        },
        metrics: {
          clicks: 10,
          impressions: 2,
          cost_micros: 3,
        },
      },
    ];
    const customerId = '1';
    const client = new MockGoogleAdsApiClient();
    client.setupResult(mockResult);
    const executor = new AdsQueryExecutor(client);
    const query = executor.parseQuery(queryText);
    const res = await executor.executeOne(query, customerId);
    assert.ok(res.rows);
    assert.deepStrictEqual(res.rows[0], [
      LocalDate.now().toString(), // date
      1, // counter
      10 / 2, // ctr
      3 * 1000, // cost
      1, //target_cpa
      'OPTIMIZE_INSTALLS_TARGET_INSTALL_COST', // bidding_type
    ]);
  });

  test('builtin query', async () => {
    const queryText = 'SELECT * FROM builtin.ocid_mapping';
    const ocid = '567';
    const url = `https://adwords.google.com/aw_prime/recommendations/deeplink?ocid=${ocid}&campaignId=3&utm_source=astore&src=%SRC%&%ADDITIONAL_PARAMS%`;
    const customerId = '1';
    const mockResult = [
      {
        customer: {
          id: customerId,
        },
        metrics: {
          optimization_score_url: url,
        },
      },
    ];
    const client = new MockGoogleAdsApiClient();
    client.setupResult(mockResult);
    const executor = new AdsQueryExecutor(client);
    const query = executor.parseQuery(queryText);
    const res = await executor.executeOne(query, customerId);
    assert.ok(res.rows);
    assert.deepStrictEqual(res.rows[0], [customerId, ocid]);
  });

  test('wildcard in query', async () => {
    // arrange
    const customerId = '1';
    const mockResult = [
      {
        customer_client: {
          applied_labels: ['customers/1/labels/1'],
          client_customer: 'customerClients/2',
          currency_code: 'USD',
          descriptive_name: 'test customer',
          hidden: false,
          id: 1,
          level: 0,
          manager: false,
          resource_name: 'customers/1/customerClients/2',
          status: 3, // CustomerStatus
          test_account: false,
          time_zone: 'UTC',
        },
      },
    ];
    const queryText = `
      SELECT
        customer_client.id,
        *,
        customer_client.manager as is_manager
      FROM customer_client
    `;

    const client = new MockGoogleAdsApiClient();
    client.setupResult(mockResult);
    const executor = new AdsQueryExecutor(client);

    // act
    const query = executor.parseQuery(queryText);
    assert.deepStrictEqual(query.columnNames, [
      'id',
      'resource_name',
      'client_customer',
      'hidden',
      'level',
      'time_zone',
      'test_account',
      'manager',
      'descriptive_name',
      'currency_code',
      'status',
      'is_manager',
    ]);

    const result = await executor.executeOne(query, customerId);

    // assert
    assert.ok(result.rows);
    const status = result.rows[0][result.query.columnNames.indexOf('status')];
    assert.strictEqual(status, 'CANCELED');
    assert.deepStrictEqual(result.rows[0], [
      1, // id
      'customers/1/customerClients/2', //resource_name
      'customerClients/2', // client_customer
      false, // hidden
      0, // level
      'UTC', // time_zone
      false, // test_account
      false, // manager
      'test customer', // descriptive_name
      'USD', // currency_code
      'CANCELED', // status
      false, // is_manager
    ]);
  });
});
