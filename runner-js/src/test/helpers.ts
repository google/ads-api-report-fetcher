import _ from 'lodash';

import {IGoogleAdsApiClient} from '../lib/api-client';

export class MockGoogleAdsApiClient implements IGoogleAdsApiClient {
  customerIds: string[];
  results: Record<string, any[]> = {};

  constructor(customerIds: string[]) {
    this.customerIds = customerIds;
  }

  async getCustomerIds(): Promise<string[]> {
    return new Promise((resolve, reject) => {
      resolve(this.customerIds);
    });
  }

  setupResult(result: any[]|Record<string, any[]>) {
    if (_.isArray(result)) {
      this.results[''] = result;
    } else {
      this.results = result;
    }
  }

  async executeQuery(query: string, customerId: string): Promise<any[]> {
    let result = this.results[customerId] || this.results[''] || [];
    return new Promise((resolve, reject) => {
      resolve(result);
    });
  }
}
