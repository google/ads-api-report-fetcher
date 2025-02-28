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

import {isArray} from 'lodash-es';

import {GoogleAdsApiClientBase} from '../lib/ads-api-client.js';
import {ApiType} from '../lib/types.js';

export class MockGoogleAdsApiClient extends GoogleAdsApiClientBase {
  results: Record<string, any[]> = {};

  constructor() {
    super(
      {
        client_id: '',
        client_secret: '',
        developer_token: '',
        refresh_token: '',
      },
      ApiType.gRPC
    );
  }

  setupResult(result: any[] | Record<string, any[]>) {
    if (isArray(result)) {
      this.results[''] = result;
    } else {
      this.results = result;
    }
  }

  async executeQuery(query: string, customerId: string): Promise<any[]> {
    const result = this.results[customerId] || this.results[''] || [];
    return new Promise(resolve => {
      resolve(result);
    });
  }

  async *executeQueryStream(query: string, customerId: string) {
    const result = this.results[customerId] || this.results[''] || [];
    for (const row of result) {
      yield row;
    }
  }
}
