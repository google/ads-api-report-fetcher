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
import {LocalDate} from "@js-joda/core";
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
    assert(result.rows!.length);
    assert.deepEqual(result.rows![0][1], 'MONDAY');
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
    assert.deepEqual(result.rows![0][0], ['AD_GROUP_AD', 'AD_GROUP'])
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
    assert(result.rows!.length);
    assert.equal(result.rows![0][1], 'сб');
  });

  test('indices', async function () {
    const campaignId = 2;
    const resId = 853097294612;
    const resName = `customers/1/campaignAudienceViews/${campaignId}~${resId}`;
    let mockResult = [{ campaign_audience_view: { resource_name: resName } }];
    let queryText = `
      SELECT
        campaign_audience_view.resource_name as res_name,
        campaign_audience_view.resource_name~0 AS res_base,
        campaign_audience_view.resource_name~1 AS res_id,
      FROM campaign_audience_view
    `;

    let customerId = "1";
    let client = new MockGoogleAdsApiClient([customerId]);
    client.setupResult(mockResult);
    let executor = new AdsQueryExecutor(client);

    // act (using executeOne)
    let query = executor.parseQuery(queryText);
    let res = await executor.executeOne(query, customerId);

    // assert
    assert.ok(res.rows);
    assert.deepStrictEqual(res.rows[0], [resName, campaignId, resId]);
  });

  test('virtual columns', async function () {
    let queryText = `
      SELECT
        '$\{today()\}' as date,
        1 AS counter,
        metrics.clicks / metrics.impressions AS ctr,
        metrics.cost_micros * 1000 AS cost,
        campaign.app_campaign_setting.bidding_strategy_goal_type AS bidding_type
      FROM campaign
    `;
    let mockResult = [
      {
        campaign: {
          app_campaign_setting: {
            bidding_strategy_goal_type: 2, // OPTIMIZE_INSTALLS_TARGET_INSTALL_COST
          },
        },
        metrics: {
          clicks: 10,
          impressions: 2,
          cost_micros: 3,
        },
      },
    ];
    let customerId = "1";
    let client = new MockGoogleAdsApiClient([customerId]);
    client.setupResult(mockResult);
    let executor = new AdsQueryExecutor(client);
    let query = executor.parseQuery(queryText);
    let res = await executor.executeOne(query, customerId);
    assert.ok(res.rows);
    assert.deepStrictEqual(res.rows[0], [
      LocalDate.now().toString(),
      1,
      10 / 2,
      3 * 1000,
      "OPTIMIZE_INSTALLS_TARGET_INSTALL_COST",
    ]);
  });

  test('builtin query', async function () {
    const queryText = `SELECT * FROM builtin.ocid_mapping`;
    const ocid = "567";
    const url = `https://adwords.google.com/aw_prime/recommendations/deeplink?ocid=${ocid}&campaignId=3&utm_source=astore&src=%SRC%&%ADDITIONAL_PARAMS%`;
    const customerId = "1";
    let mockResult = [
      {
        customer: {
          id: customerId,
        },
        metrics: {
          optimization_score_url: url,
        },
      },
    ];
    let client = new MockGoogleAdsApiClient([customerId]);
    client.setupResult(mockResult);
    let executor = new AdsQueryExecutor(client);
    let query = executor.parseQuery(queryText);
    let res = await executor.executeOne(query, customerId);
    assert.ok(res.rows);
    assert.deepStrictEqual(res.rows[0], [customerId, ocid]);
  });

  test('wildcard in query', async function () {
    // arrange
    let customerId = "1";
    let mockResult = [
      {
        customer_client: {
          applied_labels: ["customers/1/labels/1"],
          client_customer: "customerClients/2",
          currency_code: "USD",
          descriptive_name: "test customer",
          hidden: false,
          id: 1,
          level: 0,
          manager: false,
          resource_name: "customers/1/customerClients/2",
          status: 3, // CustomerStatus
          test_account: false,
          time_zone: "UTC",
        },
      },
    ];
    let queryText = `
      SELECT
        customer_client.id,
        *,
        customer_client.manager as is_manager
      FROM customer_client
    `;

    let client = new MockGoogleAdsApiClient([customerId]);
    client.setupResult(mockResult);
    let executor = new AdsQueryExecutor(client);

    // act
    let query = executor.parseQuery(queryText);
    assert.deepStrictEqual(query.columnNames, [
      "id",
      "resource_name",
      "client_customer",
      "hidden",
      "level",
      "time_zone",
      "test_account",
      "manager",
      "descriptive_name",
      "currency_code",
      "status",
      "is_manager",
    ]);

    let result = await executor.executeOne(query, customerId);
    assert.ok(result.rows);
    let status = result.rows[0][result.query.columnNames.indexOf("status")];
    assert.strictEqual(status, "CANCELED");

    // // assert
    // assert.ok(result.rows);
    // assert.deepStrictEqual(result.rows[0], [
    //   1,
    //   "customerClients/2",
    //   "USD",
    //   "test customer",
    //   false,
    //   0,
    //   false,
    //   "customers/1/customerClients/2",
    //   3,
    //  false, "UTC"
    // ]);
  });
});
