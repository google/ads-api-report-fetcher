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
import {GoogleAdsApiClient} from '../lib/ads-api-client-rest.js';
import {GoogleAdsError} from '../lib/ads-api-client-base.js';
import {AdsApiDefaultVersion} from '../lib/ads-api-schema-base.js';

suite('GoogleAdsRestApiClient', () => {
  test('init API version', () => {
    const client = new GoogleAdsApiClient({}, AdsApiDefaultVersion);
    assert.equal(client.apiVersion, AdsApiDefaultVersion);
  });

  test('handleGoogleAdsError with RPC ErrorInfo details (no failure.errors)', () => {
    const client = new GoogleAdsApiClient({}, AdsApiDefaultVersion);
    const rpcError = {
      code: 403,
      message:
        'Google Ads API has not been used in project 123 before or it is disabled.',
      status: 'PERMISSION_DENIED',
      details: [
        {
          '@type': 'type.googleapis.com/google.rpc.ErrorInfo',
          reason: 'SERVICE_DISABLED',
          domain: 'googleapis.com',
        },
      ],
    };
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const handled = (client as any).handleGoogleAdsError(
      rpcError,
      '1234567890',
      'SELECT campaign.id FROM campaign',
    );
    assert.ok(handled instanceof GoogleAdsError);
    assert.equal(
      handled.message,
      'PERMISSION_DENIED: Google Ads API has not been used in project 123 before or it is disabled.',
    );
    assert.equal(handled.retryable, false);
  });
});
