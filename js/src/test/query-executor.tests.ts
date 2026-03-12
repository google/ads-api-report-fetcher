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

import assert from 'assert';
import {LocalDate} from '@js-joda/core';
import {AdsQueryExecutor} from '../lib/ads-query-executor.js';
import {QueryResult} from '../lib/types.js';
import {MockGoogleAdsApiClient} from './helpers.js';

suite('AdsQueryExecutor', () => {
  test('enums', async () => {
    // arrange
    const mockResult = [
      {
        campaign: {id: 2, resourceName: 'customers/1/campaigns/2'},
        campaignCriterion: {
          adSchedule: {dayOfWeek: 'MONDAY'}, // DayOfWeekEnum
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
        --audience.dimensions:audience_segments.segments:user_interest.user_interest_category,
        (audience.dimensions:audience_segments.segments) --:custom_audience.custom_audience
      FROM audience`;
    const client = new MockGoogleAdsApiClient();
    client.setupResult(mockResult);
    const executor = new AdsQueryExecutor(client);

    // act (using executeOne)
    const query = await executor.parseQuery(queryText);
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
          resourceName: 'customers/1/campaigns/2',
          frequencyCaps: [
            {
              key: {
                level: 'AD_GROUP_AD', // FrequencyCapLevel.AD_GROUP_AD
                eventType: 'IMPRESSION', // FrequencyCapEventType.IMPRESSION
                timeUnit: 'DAY', // FrequencyCapTimeUnit.DAY
                timeLength: 10,
              },
              cap: 100,
            },
            {
              key: {
                level: 'AD_GROUP', // FrequencyCapLevel.AD_GROUP
                eventType: 'VIDEO_VIEW', // FrequencyCapEventType.VIDEO_VIEW
                timeUnit: 'WEEK', // FrequencyCapTimeUnit.WEEK
                timeLength: 20,
              },
              cap: 200,
            },
          ],
          manualCpc: {
            enhancedCpcEnabled: true,
          },
        },
      },
    ];
    const queryText = `
      SELECT
        campaign.frequency_caps:key.level AS frequency_caps_level, --array
        campaign.manual_cpc:enhanced_cpc_enabled                   --scalar
      FROM campaign
    `;
    const customerId = '1';
    const client = new MockGoogleAdsApiClient();
    client.setupResult(mockResult);
    const executor = new AdsQueryExecutor(client);

    // act (using executeOne)
    const query = await executor.parseQuery(queryText);
    const result = await executor.executeOne(query, <string>customerId);

    // assert
    assert.deepEqual(result.rows![0][0], ['AD_GROUP_AD', 'AD_GROUP']);
    assert.deepEqual(result.rows![0][1], true);
  });

  test('functions', async () => {
    // arrange
    const mock_result = [
      {
        campaign: {id: 2, resourceName: 'customers/1/campaigns/2'},
        campaignCriterion: {
          adSchedule: {dayOfWeek: 'SATURDAY'}, // DayOfWeekEnum
          resourceName: 'customers/1/campaignCriteria/2~340096',
        },
      },
    ];
    const queryText = `
      SELECT
        campaign.id AS campaign_id,
        campaign_criterion.ad_schedule.day_of_week:$formatDay
          AS ad_schedule_day_of_week, --pre v3 format
        formatDay(campaign_criterion.ad_schedule.day_of_week)
          AS ad_schedule_day_of_week2
      FROM campaign_criterion
      FUNCTIONS
      function formatDay(val) {
        let days = {'MONDAY': 'пн', 'TUESDAY': 'вт', 'WEDNESDAY': 'ср', 'THURSDAY': 'чт', 'FRIDAY': 'пт', 'SATURDAY': 'сб', 'SUNDAY': 'вс'};
        if (!val || val === 'UNSPECIFIED') return '';
        return days[val];
      }
    `;
    const customerId = '1';
    const client = new MockGoogleAdsApiClient();
    client.setupResult(mock_result);
    const executor = new AdsQueryExecutor(client);

    // act (using executeOne)
    const query = await executor.parseQuery(queryText);
    const result = await executor.executeOne(query, <string>customerId);

    // assert
    assert(result.rows!.length);
    assert.equal(result.rows![0][1], 'сб');
    assert.equal(result.rows![0][2], 'сб');
  });

  test('resource indices', async () => {
    const customerId = '1';
    const campaignId = 2;
    const resId = 853097294612;
    const resName = `customers/${customerId}/campaignAudienceViews/${campaignId}~${resId}`;
    const mockResult = [{campaignAudienceView: {resourceName: resName}}];
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
    const query = await executor.parseQuery(queryText);
    const res = await executor.executeOne(query, customerId);

    // assert
    assert.ok(res.rows);
    assert.deepStrictEqual(res.rows[0], [resName, campaignId, resId]);
  });

  test('virtual columns', async () => {
    const queryText = `
      SELECT
        '$\{today()}' as date, #1 constant string substituted as expression
        1 AS counter,          #2 constant number
        100+10 AS counter2,    #3 constant expression
        #4 arithmetic operation against real columns:
        metrics.clicks / metrics.impressions AS ctr,
        #5 arithmetic operation between a column and constant:
        metrics.cost_micros * 1000 AS cost,
        #6 arithmetic operation between a column and const but the column is 2-level nested:
        campaign.target_cpa.target_cpa_micros / 1000000 AS target_cpa,
        #7 function call on a enum column's value:
        campaign.name.split('.').pop() as last_name,
        #8 indexing on function's result
        campaign.tracking_url_template.split(':')[1] as protocol,
        #9 enum:
        campaign.app_campaign_setting.bidding_strategy_goal_type AS bidding_type,
        #10 indexing and nesting on arrays (repeatable fields)
        (campaign.final_urls[1].key).toString() + '!' as url_key,
      FROM campaign
    `;
    const mockResult = [
      {
        campaign: {
          name: 'part1.part2.part3',
          trackingUrlTemplate: 'https://example.org',
          appCampaignSetting: {
            biddingStrategyGoalType: 'OPTIMIZE_INSTALLS_TARGET_INSTALL_COST', // OPTIMIZE_INSTALLS_TARGET_INSTALL_COST
          },
          targetCpa: {
            targetCpaMicros: 1000000,
          },
          finalUrls: [
            {key: 'key1', value: 'value1'},
            {key: 'key2', value: 'value2'},
          ],
        },
        metrics: {
          clicks: 10,
          impressions: 2,
          costMicros: 3,
        },
      },
    ];
    const customerId = '1';
    const client = new MockGoogleAdsApiClient();
    client.setupResult(mockResult);
    const executor = new AdsQueryExecutor(client);
    const query = await executor.parseQuery(queryText);
    const res = await executor.executeOne(query, customerId);
    assert.ok(res.rows);
    assert.deepStrictEqual(res.rows[0], [
      LocalDate.now().toString(), // date
      1, // counter
      100 + 10, // counter2
      10 / 2, // ctr
      3 * 1000, // cost
      1, //target_cpa
      'part3', // last_name
      'https', // protocol
      'OPTIMIZE_INSTALLS_TARGET_INSTALL_COST', // bidding_type
      'key1!', // url_key
    ]);
  });

  test('virtual columns: integration with customizers', async () => {
    const queryText = `
SELECT
    (change_event.new_resource:campaign.target_cpa.target_cpa_micros) / 1000000 AS new_target_cpa
FROM change_event
    `;
    const mockResult = [
      {
        changeEvent: {
          newResource: {
            campaign: {
              targetCpa: {
                targetCpaMicros: 1000000,
              },
            },
          },
        },
      },
    ];

    const customerId = '1';
    const client = new MockGoogleAdsApiClient();
    client.setupResult(mockResult);
    const executor = new AdsQueryExecutor(client);
    const query = await executor.parseQuery(queryText);
    const res = await executor.executeOne(query, customerId);
    assert.ok(res.rows);
    assert.deepStrictEqual(res.rows[0], [1]);
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
          optimizationScoreUrl: url,
        },
      },
    ];
    const client = new MockGoogleAdsApiClient();
    client.setupResult(mockResult);
    const executor = new AdsQueryExecutor(client);
    const query = await executor.parseQuery(queryText);
    const res = await executor.executeOne(query, customerId);
    assert.ok(res.rows);
    assert.deepStrictEqual(res.rows[0], [customerId, ocid]);
  });

  test('wildcard in query', async () => {
    // arrange
    const customerId = '1';
    const mockResult = [
      {
        customerClient: {
          appliedLabels: ['customers/1/labels/1'],
          clientCustomer: 'customerClients/2',
          currencyCode: 'USD',
          descriptiveName: 'test customer',
          hidden: false,
          id: 1,
          level: 0,
          manager: false,
          resourceName: 'customers/1/customerClients/2',
          status: 'CANCELED', // CustomerStatus
          testAccount: false,
          timeZone: 'UTC',
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
    const query = await executor.parseQuery(queryText);
    assert.deepStrictEqual(query.columnNames, [
      'id',
      'currency_code',
      'test_account',
      'level',
      'time_zone',
      'descriptive_name',
      'hidden',
      'resource_name',
      'client_customer',
      'status',
      'manager',
      'is_manager',
    ]);

    const result = await executor.executeOne(query, customerId);

    // assert
    assert.ok(result.rows);
    const status = result.rows[0][result.query.columnNames.indexOf('status')];
    assert.strictEqual(status, 'CANCELED');
    assert.deepStrictEqual(result.rows[0], [
      1, // id
      'USD', // currency_code
      false, // test_account
      0, // level
      'UTC', // time_zone
      'test customer', // descriptive_name
      false, // hidden
      'customers/1/customerClients/2', // resource_name
      'customerClients/2', // client_customer
      'CANCELED', // status
      false, // manager
      false, // is_manager
    ]);
  });
});
