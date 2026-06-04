/**
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * Copyright 2026 Google LLC
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

import {
  GoogleAdsApiClientBase,
  GoogleAdsApiConfig,
  IGoogleAdsApiClient,
} from '../../../src/lib/ads-api-client-base';
import {BundledSchemaLoader} from './ads-api-schema-loader-bundled';
import {AdsApiSchemaRest} from '../../../src/lib/ads-api-schema-base.js';

type InternalAdsAppType = {
  search: (request: string, opts: unknown) => string;
};
declare const InternalAdsApp: InternalAdsAppType;

export class GoogleAdsAppsScriptClient
  extends GoogleAdsApiClientBase
  implements IGoogleAdsApiClient
{
  private useInternalProxy: boolean;

  constructor(adsConfig: GoogleAdsApiConfig, useInternalProxy = false) {
    const loader = new BundledSchemaLoader();
    const schema = new AdsApiSchemaRest(loader);
    super(adsConfig, schema);
    this.useInternalProxy = useInternalProxy;
  }

  async executeQuery(
    query: string,
    customerId: string,
  ): Promise<Array<Record<string, unknown>>> {
    this.logger.debug(`Executing GAQL query: ${query}`);
    const url = `https://googleads.googleapis.com/${this.apiVersion}/customers/${customerId}/googleAds:search`;
    const payload: Record<string, unknown> = {
      query,
    };

    let results: Array<Record<string, unknown>> = [];
    let nextPageToken: string | undefined = undefined;

    do {
      if (nextPageToken) {
        payload.pageToken = nextPageToken;
      }

      const response = this.sendApiRequest(url, payload, customerId);
      if (response.results) {
        results = results.concat(response.results);
      }
      nextPageToken = response.nextPageToken;
    } while (nextPageToken);

    return results;
  }

  async *executeQueryStream(
    query: string,
    customerId: string,
  ): AsyncGenerator<Record<string, unknown>> {
    this.logger.debug(`Executing GAQL query (stream): ${query}`);
    const url = `https://googleads.googleapis.com/${this.apiVersion}/customers/${customerId}/googleAds:search`;
    const payload: Record<string, unknown> = {
      query,
    };

    let nextPageToken: string | undefined = undefined;

    do {
      if (nextPageToken) {
        payload.pageToken = nextPageToken;
      }

      const response = this.sendApiRequest(url, payload, customerId);
      if (response.results) {
        for (const row of response.results) {
          yield row;
        }
      }
      nextPageToken = response.nextPageToken;
    } while (nextPageToken);
  }

  private sendApiRequest(
    url: string,
    payload: Record<string, unknown>,
    customerId: string,
  ): any {
    if (this.useInternalProxy) {
      this.logger.debug(`Using InternalAdsApp for customer ${customerId}`);
      payload['customer_id'] = customerId;
      const responseText = InternalAdsApp.search(JSON.stringify(payload), {
        version: this.apiVersion,
      });
      return JSON.parse(responseText);
    } else {
      const OAUTH_TOKEN = ScriptApp.getOAuthToken();
      const request: GoogleAppsScript.URL_Fetch.URLFetchRequestOptions = {
        method: 'post',
        headers: {
          Authorization: 'Bearer ' + OAUTH_TOKEN,
          'developer-token': this.adsConfig.developer_token,
          'Content-Type': 'application/json',
        },
        contentType: 'application/json',
        payload: JSON.stringify(payload),
        muteHttpExceptions: true,
      };

      if (this.adsConfig.login_customer_id) {
        request.headers!['login-customer-id'] =
          this.adsConfig.login_customer_id.toString();
      }

      const response = UrlFetchApp.fetch(url, request);
      const code = response.getResponseCode();
      const responseText = response.getContentText();

      if (code !== 200) {
        throw new Error(`API call failed with code ${code}: ${responseText}`);
      }

      return JSON.parse(responseText);
    }
  }
}
