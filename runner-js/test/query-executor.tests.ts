import assert from 'assert';

import {AdsQueryExecutor} from '../src/ads-query-executor';
import {IGoogleAdsApiClient} from '../src/api-client';
import {CsvWriter} from '../src/csv-writer';

class MockGoogleAdsApiClient implements IGoogleAdsApiClient {
  customerIds: string[];
  results: any[] = [];

  constructor(customerIds: string[]) {
    this.customerIds = customerIds;
  }

  async getCustomerIds(): Promise<string[]> {
    return new Promise((resolve, reject) => {
      resolve(this.customerIds);
    });
  }

  setupResult(result: any[]) {
    this.results = result;
  }

  async executeQuery(query: string, customerId?: string|undefined|null):
      Promise<any[]> {
    return new Promise((resolve, reject) => {
      resolve(this.results);
    });
  }
}

suite('AdsQueryExecutor', () => {
  test('enums', async function() {
    // arrange
    let mock_result = [{
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
    client.setupResult(mock_result);
    let writer = new CsvWriter('.tmp');
    let executor = new AdsQueryExecutor(client);

    // act
    let result =
        <any[]>(await executor
                    .executeGen('test', queryText, [customerId], {}, writer)
                    .next())
            .value;
    // let result = await executor.execute("test", queryText, [customerId], {},
    // writer);

    // assert
    assert(result.length);
    assert.deepEqual(result[0][1], 'MONDAY');
  });

  test('nested values', async function() {
    // arrange
    let mock_result = [{
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
    client.setupResult(mock_result);
    let writer = new CsvWriter('.tmp');
    let executor = new AdsQueryExecutor(client);

    // act
    let result =
        <any[]>(await executor
                    .executeGen('test', queryText, [customerId], {}, writer)
                    .next())
            .value;

    // assert
    assert.deepEqual(result[0][0], ['AD_GROUP_AD', 'AD_GROUP'])
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
    let writer = new CsvWriter('.tmp');
    let executor = new AdsQueryExecutor(client);

    // act
    let result =
        <any[]>(await executor
                    .executeGen('test', queryText, [customerId], {}, writer)
                    .next())
            .value;

    // assert
    assert(result.length);
    assert.equal(result[0][1], 'сб');
  });
});
